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

    fn write_script(path: &Path, body: &str) {
        fs::write(path, body).unwrap();
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

    async fn collect_lines(rx: &mut AgentEventStream, count: usize) -> Vec<String> {
        let mut lines = Vec::new();
        for _ in 0..count {
            lines.push(next_event(rx).await.line);
        }
        lines
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

    #[test]
    fn new_harness_reports_initial_status() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(AgentEngine::Codex, "/bin/cat");
        let (harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        let status = harness.status();
        assert_eq!(status.namespace, "moni");
        assert_eq!(status.engine, AgentEngine::Codex);
        assert!(!status.running);
        assert!(status.pid.is_none());
    }

    #[test]
    fn harness_exposes_engine_and_namespace() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(AgentEngine::Claude, "/bin/cat");
        let (harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        assert_eq!(harness.engine(), AgentEngine::Claude);
        assert_eq!(harness.namespace(), "moni");
    }

    #[tokio::test]
    async fn send_before_start_returns_not_running() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(AgentEngine::Claude, "/bin/cat");
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        let err = harness.send("hello").await.unwrap_err();

        assert!(err.to_string().contains("not running"));
    }

    #[tokio::test]
    async fn start_rejects_already_running_process() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        let err = harness.start().await.unwrap_err();
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(err.to_string().contains("already running"));
    }

    #[tokio::test]
    async fn stop_before_start_is_noop_with_status_event() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(AgentEngine::Claude, "/bin/cat");
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.stop(StopReason::Shutdown).await.unwrap();

        let event = next_event(&mut events).await;
        assert_eq!(event.stream, EventStreamKind::Status);
        assert_eq!(event.line, "stopped:Shutdown");
    }

    #[tokio::test]
    async fn process_harness_handles_multiple_prompts() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        harness.send("one").await.unwrap();
        harness.send("two").await.unwrap();

        let lines = collect_lines(&mut events, 4).await;
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(lines.iter().any(|line| line == "ok:one"));
        assert!(lines.iter().any(|line| line == "ok:two"));
    }

    #[tokio::test]
    async fn process_harness_streams_stderr_lines() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("stderr-agent");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  echo "stderr:$line" >&2
done
"#,
        );
        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        harness.send("boom").await.unwrap();

        let mut seen_stderr = false;
        for _ in 0..3 {
            let event = next_event(&mut events).await;
            if event.stream == EventStreamKind::Stderr && event.line == "stderr:boom" {
                seen_stderr = true;
                break;
            }
        }
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(seen_stderr);
    }

    #[tokio::test]
    async fn process_harness_passes_configured_args() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("args-agent");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
echo "args:$*"
while IFS= read -r line; do
  echo "ok:$line"
done
"#,
        );
        let config = EngineConfig::new(AgentEngine::Claude, bin).with_args(["--alpha", "beta"]);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();

        let lines = collect_lines(&mut events, 2).await;
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(lines.iter().any(|line| line == "args:--alpha beta"));
    }

    #[tokio::test]
    async fn process_harness_runs_in_workspace_path() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("pwd-agent");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
pwd
while IFS= read -r line; do
  echo "$line"
done
"#,
        );
        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();

        let lines = collect_lines(&mut events, 2).await;
        harness.stop(StopReason::Shutdown).await.unwrap();

        let expected = fs::canonicalize(dir.path()).unwrap();
        assert!(
            lines
                .iter()
                .filter_map(|line| fs::canonicalize(line).ok())
                .any(|line| line == expected)
        );
    }

    #[tokio::test]
    async fn stop_emits_stop_reason() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        let _ = next_event(&mut events).await;
        harness.stop(StopReason::Clear).await.unwrap();

        let mut saw_reason = false;
        for _ in 0..3 {
            let event = next_event(&mut events).await;
            if event.line == "stopped:Clear" {
                saw_reason = true;
                break;
            }
        }
        assert!(saw_reason);
    }

    #[tokio::test]
    async fn events_include_codex_engine_metadata() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let config = EngineConfig::new(AgentEngine::Codex, bin);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();

        let event = next_event(&mut events).await;
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert_eq!(event.engine, AgentEngine::Codex);
        assert_eq!(event.namespace, "moni");
    }

    #[tokio::test]
    async fn start_missing_binary_returns_error() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(AgentEngine::Claude, dir.path().join("missing"));
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        let err = harness.start().await.unwrap_err();

        assert!(err.to_string().contains("No such file") || err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn status_has_pid_after_start() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        let status = harness.status();
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(status.running);
        assert!(status.pid.is_some());
    }

    #[test]
    fn stop_reason_serializes_as_kebab_case() {
        assert_eq!(
            serde_json::to_string(&StopReason::Reset).unwrap(),
            "\"reset\""
        );
        assert_eq!(
            serde_json::to_string(&StopReason::Clear).unwrap(),
            "\"clear\""
        );
    }

    #[test]
    fn event_stream_kind_serializes_as_kebab_case() {
        assert_eq!(
            serde_json::to_string(&EventStreamKind::Stdout).unwrap(),
            "\"stdout\""
        );
        assert_eq!(
            serde_json::to_string(&EventStreamKind::Stderr).unwrap(),
            "\"stderr\""
        );
    }

    #[test]
    fn agent_event_round_trips_json() {
        let event = AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Claude,
            stream: EventStreamKind::Stdout,
            line: "hello".to_string(),
        };
        let encoded = serde_json::to_string(&event).unwrap();
        let decoded: AgentEvent = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, event);
    }

    #[test]
    fn harness_status_round_trips_json() {
        let status = AgentHarnessStatus {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            running: true,
            pid: Some(42),
        };
        let encoded = serde_json::to_string(&status).unwrap();
        let decoded: AgentHarnessStatus = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, status);
    }
}
