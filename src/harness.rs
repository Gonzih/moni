use std::{path::PathBuf, process::Stdio, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, mpsc},
};

use crate::engine::{AgentEngine, AgentProtocol, EngineConfig};

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
    Delta,
    Final,
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
    codex_state: Arc<Mutex<CodexAppServerState>>,
    next_request_id: u64,
}

#[derive(Debug, Default)]
struct CodexAppServerState {
    thread_id: Option<String>,
    pending_message: String,
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
                codex_state: Arc::new(Mutex::new(CodexAppServerState::default())),
                next_request_id: 1,
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
        let protocol = self.config.protocol;
        let events = self.events.clone();
        let codex_state = self.codex_state.clone();

        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        if protocol == AgentProtocol::CodexAppServer {
                            handle_codex_app_server_line(
                                &namespace,
                                engine,
                                &events,
                                codex_state.clone(),
                                line,
                            )
                            .await;
                            continue;
                        }
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

    fn next_request_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        id
    }

    async fn write_jsonrpc(&mut self, message: Value) -> anyhow::Result<()> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| HarnessError::MissingStdin(self.namespace.clone()))?;
        let bytes = serde_json::to_vec(&message)?;
        stdin.write_all(&bytes).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn bootstrap_codex_app_server(&mut self) -> anyhow::Result<()> {
        let initialize_id = self.next_request_id();
        let cwd = self.workspace_path.to_string_lossy().to_string();
        self.write_jsonrpc(json!({
            "jsonrpc": "2.0",
            "id": initialize_id,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "moni",
                    "title": "moni",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "experimentalApi": true,
                    "requestAttestation": false
                }
            }
        }))
        .await?;

        let thread_start_id = self.next_request_id();
        self.write_jsonrpc(json!({
            "jsonrpc": "2.0",
            "id": thread_start_id,
            "method": "thread/start",
            "params": {
                "cwd": cwd,
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
                "model": null
            }
        }))
        .await
    }

    async fn wait_for_codex_thread_id(&self) -> anyhow::Result<String> {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
        loop {
            if let Some(thread_id) = self.codex_state.lock().await.thread_id.clone() {
                return Ok(thread_id);
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for Codex app-server thread id for namespace {}",
                    self.namespace
                );
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        }
    }

    async fn send_codex_turn(&mut self, prompt: &str) -> anyhow::Result<()> {
        let thread_id = self.wait_for_codex_thread_id().await?;
        let turn_start_id = self.next_request_id();
        let cwd = self.workspace_path.to_string_lossy().to_string();
        self.write_jsonrpc(json!({
            "jsonrpc": "2.0",
            "id": turn_start_id,
            "method": "turn/start",
            "params": {
                "threadId": thread_id,
                "cwd": cwd,
                "input": [{
                    "type": "text",
                    "text": prompt,
                    "text_elements": []
                }]
            }
        }))
        .await
    }

    pub async fn reap_if_exited(&mut self) -> anyhow::Result<bool> {
        let Some(child) = self.child.as_mut() else {
            return Ok(false);
        };

        if let Some(status) = child.try_wait()? {
            self.child.take();
            self.stdin.take();
            let _ = self
                .events
                .send(AgentEvent {
                    namespace: self.namespace.clone(),
                    engine: self.config.engine,
                    stream: EventStreamKind::Status,
                    line: format!("exited:{status}"),
                })
                .await;
            return Ok(true);
        }

        Ok(false)
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

        self.child = Some(child);
        if self.config.protocol == AgentProtocol::CodexAppServer {
            self.bootstrap_codex_app_server().await?;
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

        Ok(())
    }

    async fn send(&mut self, prompt: &str) -> anyhow::Result<()> {
        if self.child.is_none() {
            return Err(HarnessError::NotRunning(self.namespace.clone()).into());
        }
        if self.config.protocol == AgentProtocol::CodexAppServer {
            return self.send_codex_turn(prompt).await;
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

async fn handle_codex_app_server_line(
    namespace: &str,
    engine: AgentEngine,
    events: &mpsc::Sender<AgentEvent>,
    state: Arc<Mutex<CodexAppServerState>>,
    line: String,
) {
    let Ok(message) = serde_json::from_str::<Value>(&line) else {
        emit_agent_event(events, namespace, engine, EventStreamKind::Stderr, line).await;
        return;
    };

    if let Some(error) = message.get("error") {
        emit_agent_event(
            events,
            namespace,
            engine,
            EventStreamKind::Status,
            format!("codex-error:{error}"),
        )
        .await;
        return;
    }

    if let Some(thread_id) = codex_thread_id(&message, "/result") {
        state.lock().await.thread_id = Some(thread_id);
        return;
    }

    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return;
    };

    match method {
        "thread/started" => {
            if let Some(thread_id) = codex_thread_id(&message, "/params") {
                state.lock().await.thread_id = Some(thread_id);
            }
        }
        "agentMessageDelta" | "item/agentMessage/delta" => {
            if let Some(delta) = message.pointer("/params/delta").and_then(Value::as_str) {
                state.lock().await.pending_message.push_str(delta);
                emit_agent_event(events, namespace, engine, EventStreamKind::Delta, delta).await;
            }
        }
        "turnCompleted" | "turn/completed" => {
            let mut state = state.lock().await;
            let body = state.pending_message.trim().to_string();
            state.pending_message.clear();
            drop(state);

            if !body.is_empty() {
                emit_agent_event(events, namespace, engine, EventStreamKind::Final, body).await;
            }
        }
        "error" => {
            emit_agent_event(
                events,
                namespace,
                engine,
                EventStreamKind::Status,
                format!("codex-error:{message}"),
            )
            .await;
        }
        _ => {}
    }
}

fn codex_thread_id(message: &Value, root: &str) -> Option<String> {
    ["/thread/id", "/threadId"].into_iter().find_map(|path| {
        message
            .pointer(&format!("{root}{path}"))
            .and_then(Value::as_str)
            .map(str::to_string)
    })
}

async fn emit_agent_event(
    events: &mpsc::Sender<AgentEvent>,
    namespace: &str,
    engine: AgentEngine,
    stream: EventStreamKind,
    line: impl Into<String>,
) {
    let _ = events
        .send(AgentEvent {
            namespace: namespace.to_string(),
            engine,
            stream,
            line: line.into(),
        })
        .await;
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
        timeout(Duration::from_secs(10), rx.recv())
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

    #[tokio::test]
    async fn reap_if_exited_clears_finished_child() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("exit-agent");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
echo "done"
exit 0
"#,
        );
        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        let _ = next_event(&mut events).await;
        let exited = timeout(Duration::from_secs(10), async {
            loop {
                if harness.reap_if_exited().await.unwrap() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert!(exited);
        assert!(!harness.status().running);
    }

    #[tokio::test]
    async fn codex_app_server_protocol_submits_turn_and_flushes_delta() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("codex-app-server");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  if [[ "$line" == *'"method":"thread/start"'* ]]; then
    echo '{"id":2,"result":{"thread":{"id":"thread-1"}}}'
  elif [[ "$line" == *'"method":"turn/start"'* ]]; then
    echo '{"method":"item/agentMessage/delta","params":{"delta":"hello ","itemId":"item-1","threadId":"thread-1","turnId":"turn-1"}}'
    echo '{"method":"item/agentMessage/delta","params":{"delta":"codex","itemId":"item-1","threadId":"thread-1","turnId":"turn-1"}}'
    echo '{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1"}}}'
  fi
done
"#,
        );
        let config =
            EngineConfig::new(AgentEngine::Codex, bin).with_protocol(AgentProtocol::CodexAppServer);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        harness.send("run").await.unwrap();

        let mut saw_delta = false;
        let mut saw_final = false;
        for _ in 0..5 {
            let event = next_event(&mut events).await;
            if event.stream == EventStreamKind::Delta && event.line == "hello " {
                saw_delta = true;
            }
            if event.stream == EventStreamKind::Final && event.line == "hello codex" {
                saw_final = true;
                break;
            }
        }
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(saw_delta);
        assert!(saw_final);
    }

    #[tokio::test]
    async fn codex_app_server_protocol_accepts_legacy_delta_names() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("codex-app-server");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  if [[ "$line" == *'"method":"thread/start"'* ]]; then
    echo '{"method":"thread/started","params":{"threadId":"thread-1"}}'
  elif [[ "$line" == *'"method":"turn/start"'* ]]; then
    echo '{"method":"agentMessageDelta","params":{"delta":"legacy"}}'
    echo '{"method":"turnCompleted","params":{"threadId":"thread-1","turn":{"id":"turn-1"}}}'
  fi
done
"#,
        );
        let config =
            EngineConfig::new(AgentEngine::Codex, bin).with_protocol(AgentProtocol::CodexAppServer);
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        harness.send("run").await.unwrap();

        let mut saw_delta = false;
        let mut saw_final = false;
        for _ in 0..5 {
            let event = next_event(&mut events).await;
            if event.stream == EventStreamKind::Delta && event.line == "legacy" {
                saw_delta = true;
            }
            if event.stream == EventStreamKind::Final && event.line == "legacy" {
                saw_final = true;
                break;
            }
        }
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(saw_delta);
        assert!(saw_final);
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
