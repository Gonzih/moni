use std::{path::PathBuf, process::Stdio};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
};

use crate::engine::{AgentEngine, EngineConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StopReason {
    Reset,
    Clear,
    Shutdown,
    Replace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EventStreamKind {
    Stdout,
    Stderr,
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEvent {
    pub namespace: String,
    pub engine: AgentEngine,
    pub stream: EventStreamKind,
    pub line: String,
}

pub type AgentEventStream = mpsc::Receiver<AgentEvent>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHarnessStatus {
    pub namespace: String,
    pub engine: AgentEngine,
    pub running: bool,
    pub pid: Option<u32>,
}

#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("agent process is already running for namespace {0}")]
    AlreadyRunning(String),
    #[error("agent process is not running for namespace {0}")]
    NotRunning(String),
    #[error("agent process stdin is unavailable for namespace {0}")]
    MissingStdin(String),
}

#[async_trait]
pub trait AgentHarness: Send {
    fn engine(&self) -> AgentEngine;
    fn namespace(&self) -> &str;
    async fn start(&mut self) -> anyhow::Result<()>;
    async fn send(&mut self, prompt: &str) -> anyhow::Result<()>;
    async fn stop(&mut self, reason: StopReason) -> anyhow::Result<()>;
    fn status(&self) -> AgentHarnessStatus;
}

pub struct ProcessAgentHarness {
    namespace: String,
    workspace_path: PathBuf,
    config: EngineConfig,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    events: mpsc::Sender<AgentEvent>,
}

impl ProcessAgentHarness {
    pub fn new(
        namespace: impl Into<String>,
        workspace_path: impl Into<PathBuf>,
        config: EngineConfig,
    ) -> (Self, AgentEventStream) {
        let (events, rx) = mpsc::channel(256);
        (
            Self {
                namespace: namespace.into(),
                workspace_path: workspace_path.into(),
                config,
                child: None,
                stdin: None,
                events,
            },
            rx,
        )
    }

    fn emit_output_task<R>(&self, reader: R, stream: EventStreamKind)
    where
        R: tokio::io::AsyncRead + Send + Unpin + 'static,
    {
        let namespace = self.namespace.clone();
        let engine = self.config.engine;
        let events = self.events.clone();

        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        let _ = events
                            .send(AgentEvent {
                                namespace: namespace.clone(),
                                engine,
                                stream,
                                line,
                            })
                            .await;
                    }
                    Ok(None) => break,
                    Err(err) => {
                        let _ = events
                            .send(AgentEvent {
                                namespace: namespace.clone(),
                                engine,
                                stream: EventStreamKind::Status,
                                line: format!("read-error:{err}"),
                            })
                            .await;
                        break;
                    }
                }
            }
        });
    }
}

#[async_trait]
impl AgentHarness for ProcessAgentHarness {
    fn engine(&self) -> AgentEngine {
        self.config.engine
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    async fn start(&mut self) -> anyhow::Result<()> {
        if self.child.is_some() {
            return Err(HarnessError::AlreadyRunning(self.namespace.clone()).into());
        }

        let mut command = Command::new(&self.config.command);
        command
            .args(&self.config.args)
            .current_dir(&self.workspace_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn()?;
        self.stdin = child.stdin.take();

        if let Some(stdout) = child.stdout.take() {
            self.emit_output_task(stdout, EventStreamKind::Stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            self.emit_output_task(stderr, EventStreamKind::Stderr);
        }

        let _ = self
            .events
            .send(AgentEvent {
                namespace: self.namespace.clone(),
                engine: self.config.engine,
                stream: EventStreamKind::Status,
                line: "started".to_string(),
            })
            .await;

        self.child = Some(child);
        Ok(())
    }

    async fn send(&mut self, prompt: &str) -> anyhow::Result<()> {
        if self.child.is_none() {
            return Err(HarnessError::NotRunning(self.namespace.clone()).into());
        }

        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| HarnessError::MissingStdin(self.namespace.clone()))?;
        stdin.write_all(prompt.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn stop(&mut self, reason: StopReason) -> anyhow::Result<()> {
        drop(self.stdin.take());

        if let Some(mut child) = self.child.take() {
            if child.try_wait()?.is_none() {
                child.kill().await?;
            }
            let _ = child.wait().await;
        }

        let _ = self
            .events
            .send(AgentEvent {
                namespace: self.namespace.clone(),
                engine: self.config.engine,
                stream: EventStreamKind::Status,
                line: format!("stopped:{reason:?}"),
            })
            .await;

        Ok(())
    }

    fn status(&self) -> AgentHarnessStatus {
        AgentHarnessStatus {
            namespace: self.namespace.clone(),
            engine: self.config.engine,
            running: self.child.is_some(),
            pid: self.child.as_ref().and_then(Child::id),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt, path::Path};

    use tempfile::TempDir;
    use tokio::time::{Duration, timeout};

    use super::*;

    fn write_mock_agent(path: &Path) {
        fs::write(
            path,
            r#"#!/usr/bin/env bash
echo "ready:$MOCK_AGENT_NAME"
while IFS= read -r line; do
  if [ -n "$MOCK_AGENT_STDERR" ]; then
    echo "err:$line" >&2
  fi
  echo "${MOCK_AGENT_RESPONSE:-ok}:$line"
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    async fn next_event(rx: &mut AgentEventStream) -> AgentEvent {
        timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn process_harness_sends_prompts_over_stdin_and_streams_output() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);

        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        harness.send("hello").await.unwrap();

        let mut lines = Vec::new();
        for _ in 0..3 {
            lines.push(next_event(&mut events).await.line);
        }

        assert!(lines.iter().any(|line| line == "started"));
        assert!(lines.iter().any(|line| line == "ready:"));
        assert!(lines.iter().any(|line| line == "ok:hello"));

        assert!(harness.status().running);
        harness.stop(StopReason::Reset).await.unwrap();
        assert!(!harness.status().running);
    }
}
