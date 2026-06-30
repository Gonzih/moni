use std::{collections::HashSet, path::PathBuf, process::Stdio, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    io::AsyncRead,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<AgentEventPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum AgentEventPayload {
    Text {
        text: String,
    },
    ToolStarted {
        id: Option<String>,
        label: String,
        kind: String,
    },
    ToolCompleted {
        id: Option<String>,
        label: String,
        kind: String,
        status: Option<String>,
        exit_code: Option<i64>,
        stdout: Option<String>,
        stderr: Option<String>,
        error: Option<String>,
    },
    TurnCompleted {
        final_text: String,
        model: Option<String>,
        duration_ms: Option<u64>,
        usage: Option<TokenUsage>,
        exit_status: Option<String>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

impl AgentEvent {
    fn new(namespace: String, engine: AgentEngine, stream: EventStreamKind, line: String) -> Self {
        Self {
            namespace,
            engine,
            stream,
            line,
            payload: None,
        }
    }

    fn with_payload(
        namespace: String,
        engine: AgentEngine,
        stream: EventStreamKind,
        line: String,
        payload: AgentEventPayload,
    ) -> Self {
        Self {
            namespace,
            engine,
            stream,
            line,
            payload: Some(payload),
        }
    }
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
    #[cfg(test)]
    fail_next_reap: bool,
    #[cfg(test)]
    fail_next_try_wait: bool,
}

#[derive(Debug, Default)]
struct CodexAppServerState {
    thread_id: Option<String>,
    pending_message: String,
    delta_item_ids: HashSet<String>,
}

impl ProcessAgentHarness {
    pub fn new(
        namespace: &str,
        workspace_path: &std::path::Path,
        config: EngineConfig,
    ) -> (Self, AgentEventStream) {
        let (events, rx) = mpsc::channel(256);
        (
            Self {
                namespace: namespace.to_string(),
                workspace_path: workspace_path.to_path_buf(),
                config,
                child: None,
                stdin: None,
                events,
                codex_state: Arc::new(Mutex::new(CodexAppServerState::default())),
                next_request_id: 1,
                #[cfg(test)]
                fail_next_reap: false,
                #[cfg(test)]
                fail_next_try_wait: false,
            },
            rx,
        )
    }

    fn emit_output_task(
        &self,
        reader: Box<dyn AsyncRead + Send + Unpin + 'static>,
        stream: EventStreamKind,
    ) {
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
                            .send(AgentEvent::new(namespace.clone(), engine, stream, line))
                            .await;
                    }
                    Ok(None) => break,
                    Err(err) => {
                        let _ = events
                            .send(AgentEvent::new(
                                namespace.clone(),
                                engine,
                                EventStreamKind::Status,
                                format!("read-error:{err}"),
                            ))
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

    #[cfg(test)]
    pub(crate) fn drop_stdin_for_test(&mut self) {
        self.stdin.take();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_reap_for_test(&mut self) {
        self.fail_next_reap = true;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_try_wait_for_test(&mut self) {
        self.fail_next_try_wait = true;
    }

    async fn write_jsonrpc(&mut self, message: Value) -> anyhow::Result<()> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| HarnessError::MissingStdin(self.namespace.clone()))?;
        let mut bytes =
            serde_json::to_vec(&message).expect("serializing a serde_json::Value cannot fail");
        bytes.push(b'\n');
        stdin.write_all(&bytes).await?;
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
                "model": self.config.model.clone()
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
        #[cfg(test)]
        if self.fail_next_reap {
            self.fail_next_reap = false;
            anyhow::bail!("forced reap failure");
        }

        let Some(child) = self.child.as_mut() else {
            return Ok(false);
        };

        let status = try_wait_child(
            child,
            #[cfg(test)]
            &mut self.fail_next_try_wait,
        )?;

        if let Some(status) = status {
            self.child.take();
            self.stdin.take();
            let _ = self
                .events
                .send(AgentEvent::new(
                    self.namespace.clone(),
                    self.config.engine,
                    EventStreamKind::Status,
                    format!("exited:{status}"),
                ))
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
        let mut args = self.config.args.clone();
        if self.config.protocol != AgentProtocol::CodexAppServer {
            if let Some(model) = &self.config.model {
                args.extend(["--model".to_string(), model.clone()]);
            }
        }
        command
            .args(&args)
            .current_dir(&self.workspace_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn()?;
        self.stdin = child.stdin.take();

        if let Some(stdout) = child.stdout.take() {
            self.emit_output_task(Box::new(stdout), EventStreamKind::Stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            self.emit_output_task(Box::new(stderr), EventStreamKind::Stderr);
        }

        self.child = Some(child);
        if self.config.protocol == AgentProtocol::CodexAppServer {
            self.bootstrap_codex_app_server().await?;
        }

        let _ = self
            .events
            .send(AgentEvent::new(
                self.namespace.clone(),
                self.config.engine,
                EventStreamKind::Status,
                "started".to_string(),
            ))
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
            let status = try_wait_child(
                &mut child,
                #[cfg(test)]
                &mut self.fail_next_try_wait,
            )?;

            if status.is_none() {
                child.kill().await?;
            }
            let _ = child.wait().await;
        }

        let _ = self
            .events
            .send(AgentEvent::new(
                self.namespace.clone(),
                self.config.engine,
                EventStreamKind::Status,
                format!("stopped:{reason:?}"),
            ))
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

fn try_wait_child(
    child: &mut Child,
    #[cfg(test)] fail_next_try_wait: &mut bool,
) -> std::io::Result<Option<std::process::ExitStatus>> {
    #[cfg(test)]
    if *fail_next_try_wait {
        *fail_next_try_wait = false;
        return Err(std::io::Error::other("forced try_wait failure"));
    }

    child.try_wait()
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
        let error_message = codex_error_message(error);
        emit_structured_agent_event(
            events,
            namespace,
            engine,
            EventStreamKind::Status,
            format!("codex-error:{error_message}"),
            AgentEventPayload::Error {
                message: error_message,
            },
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
                let mut state = state.lock().await;
                state.pending_message.push_str(delta);
                if let Some(item_id) = message.pointer("/params/itemId").and_then(Value::as_str) {
                    state.delta_item_ids.insert(item_id.to_string());
                }
                drop(state);
                emit_structured_agent_event(
                    events,
                    namespace,
                    engine,
                    EventStreamKind::Delta,
                    delta.to_string(),
                    AgentEventPayload::Text {
                        text: delta.to_string(),
                    },
                )
                .await;
            }
        }
        "item/started" => {
            if let Some(item) = message.pointer("/params/item") {
                if let Some(tool) = parse_tool_started(item) {
                    emit_structured_agent_event(
                        events,
                        namespace,
                        engine,
                        EventStreamKind::Status,
                        format!("tool-start:{}", tool.label),
                        AgentEventPayload::ToolStarted {
                            id: tool.id,
                            label: tool.label,
                            kind: tool.kind,
                        },
                    )
                    .await;
                }
            }
        }
        "item/completed" => {
            if let Some(item) = message.pointer("/params/item") {
                let item_type = item_type(item);
                if is_agent_message_item(&item_type) {
                    let item_id = item_id(item);
                    let already_streamed = if let Some(item_id) = &item_id {
                        state.lock().await.delta_item_ids.remove(item_id)
                    } else {
                        false
                    };
                    if !already_streamed {
                        if let Some(text) = extract_codex_item_text(item) {
                            state.lock().await.pending_message.push_str(&text);
                            emit_structured_agent_event(
                                events,
                                namespace,
                                engine,
                                EventStreamKind::Delta,
                                text.clone(),
                                AgentEventPayload::Text { text },
                            )
                            .await;
                        }
                    }
                } else if let Some(tool) = parse_tool_completed(item) {
                    emit_structured_agent_event(
                        events,
                        namespace,
                        engine,
                        EventStreamKind::Status,
                        format!("tool-complete:{}", tool.label),
                        AgentEventPayload::ToolCompleted {
                            id: tool.id,
                            label: tool.label,
                            kind: tool.kind,
                            status: tool.status,
                            exit_code: tool.exit_code,
                            stdout: tool.stdout,
                            stderr: tool.stderr,
                            error: tool.error,
                        },
                    )
                    .await;
                }
            }
        }
        "turnCompleted" | "turn/completed" => {
            let mut state = state.lock().await;
            let body = state.pending_message.trim().to_string();
            state.pending_message.clear();
            state.delta_item_ids.clear();
            drop(state);

            let turn = message.pointer("/params/turn").unwrap_or(&Value::Null);
            let model = find_string(&message, &["/params/model", "/params/turn/model"]);
            let duration_ms = find_u64(
                &message,
                &[
                    "/params/duration_ms",
                    "/params/durationMs",
                    "/params/turn/duration_ms",
                    "/params/turn/durationMs",
                ],
            );
            let usage = parse_token_usage(
                message
                    .pointer("/params/usage")
                    .or_else(|| message.pointer("/params/turn/usage")),
            );
            let exit_status = find_string(turn, &["/status", "/exit_status", "/exitStatus"]);

            if !body.is_empty()
                || model.is_some()
                || duration_ms.is_some()
                || usage.is_some()
                || exit_status.is_some()
            {
                emit_structured_agent_event(
                    events,
                    namespace,
                    engine,
                    EventStreamKind::Final,
                    body.clone(),
                    AgentEventPayload::TurnCompleted {
                        final_text: body,
                        model,
                        duration_ms,
                        usage,
                        exit_status,
                    },
                )
                .await;
            }
        }
        "error" => {
            let error_message = message
                .pointer("/params/message")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| message.to_string());
            emit_structured_agent_event(
                events,
                namespace,
                engine,
                EventStreamKind::Status,
                format!("codex-error:{error_message}"),
                AgentEventPayload::Error {
                    message: error_message,
                },
            )
            .await;
        }
        _ => {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedToolEvent {
    id: Option<String>,
    label: String,
    kind: String,
    status: Option<String>,
    exit_code: Option<i64>,
    stdout: Option<String>,
    stderr: Option<String>,
    error: Option<String>,
}

fn parse_tool_started(item: &Value) -> Option<ParsedToolEvent> {
    let kind = item_type(item);
    if !is_tool_item(&kind) {
        return None;
    }
    Some(ParsedToolEvent {
        id: item_id(item),
        label: tool_label(item, &kind),
        kind,
        status: None,
        exit_code: None,
        stdout: None,
        stderr: None,
        error: None,
    })
}

fn parse_tool_completed(item: &Value) -> Option<ParsedToolEvent> {
    let kind = item_type(item);
    if !is_tool_item(&kind) {
        return None;
    }
    Some(ParsedToolEvent {
        id: item_id(item),
        label: tool_label(item, &kind),
        kind,
        status: find_string(item, &["/status", "/state"]),
        exit_code: find_i64(
            item,
            &[
                "/exitCode",
                "/exit_code",
                "/output/exitCode",
                "/output/exit_code",
            ],
        ),
        stdout: find_string(
            item,
            &[
                "/stdout",
                "/output/stdout",
                "/formattedOutput",
                "/formatted_output",
            ],
        ),
        stderr: find_string(item, &["/stderr", "/output/stderr"]),
        error: find_string(
            item,
            &[
                "/error",
                "/error/message",
                "/output/error",
                "/output/error/message",
            ],
        ),
    })
}

fn item_type(item: &Value) -> String {
    find_string(item, &["/type"]).unwrap_or_default()
}

fn item_id(item: &Value) -> Option<String> {
    find_string(item, &["/id", "/itemId", "/item_id"])
}

fn is_agent_message_item(kind: &str) -> bool {
    normalized_kind(kind) == "agentmessage"
}

fn is_tool_item(kind: &str) -> bool {
    matches!(
        normalized_kind(kind).as_str(),
        "commandexecution" | "mcptoolcall" | "dynamictoolcall" | "tooluse"
    )
}

fn normalized_kind(kind: &str) -> String {
    kind.chars()
        .filter(|ch| *ch != '_' && *ch != '-' && !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

fn tool_label(item: &Value, fallback: &str) -> String {
    find_string(
        item,
        &[
            "/command",
            "/tool",
            "/name",
            "/call/name",
            "/params/name",
            "/server",
        ],
    )
    .filter(|label| !label.trim().is_empty())
    .unwrap_or_else(|| fallback.to_string())
}

fn extract_codex_item_text(item: &Value) -> Option<String> {
    if let Some(text) = find_string(item, &["/text", "/content/text", "/message/content"]) {
        return Some(text);
    }
    item.pointer("/content")
        .and_then(Value::as_array)
        .and_then(|blocks| {
            blocks.iter().find_map(|block| {
                let is_text = block
                    .pointer("/type")
                    .and_then(Value::as_str)
                    .map(|kind| kind == "text" || kind == "output_text")
                    .unwrap_or(false);
                if is_text {
                    block
                        .pointer("/text")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                } else {
                    None
                }
            })
        })
}

fn parse_token_usage(value: Option<&Value>) -> Option<TokenUsage> {
    let usage = value?;
    let parsed = TokenUsage {
        input_tokens: find_u64(
            usage,
            &[
                "/input_tokens",
                "/inputTokens",
                "/prompt_tokens",
                "/promptTokens",
            ],
        ),
        output_tokens: find_u64(
            usage,
            &[
                "/output_tokens",
                "/outputTokens",
                "/completion_tokens",
                "/completionTokens",
            ],
        ),
        cached_input_tokens: find_u64(
            usage,
            &[
                "/cached_input_tokens",
                "/cachedInputTokens",
                "/cache_read_input_tokens",
                "/cacheReadInputTokens",
            ],
        ),
        total_tokens: find_u64(usage, &["/total_tokens", "/totalTokens"]),
    };
    if parsed == TokenUsage::default() {
        None
    } else {
        Some(parsed)
    }
}

fn find_string(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers.iter().find_map(|pointer| {
        value
            .pointer(pointer)
            .and_then(|candidate| match candidate {
                Value::String(text) => Some(text.clone()),
                Value::Number(number) => Some(number.to_string()),
                Value::Bool(flag) => Some(flag.to_string()),
                _ => None,
            })
    })
}

fn find_u64(value: &Value, pointers: &[&str]) -> Option<u64> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(value_as_u64))
}

fn find_i64(value: &Value, pointers: &[&str]) -> Option<i64> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(value_as_i64))
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_i64().and_then(|n| n.try_into().ok())),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn codex_error_message(error: &Value) -> String {
    error
        .pointer("/message")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| error.to_string())
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
    line: String,
) {
    let _ = events
        .send(AgentEvent::new(namespace.to_string(), engine, stream, line))
        .await;
}

async fn emit_structured_agent_event(
    events: &mpsc::Sender<AgentEvent>,
    namespace: &str,
    engine: AgentEngine,
    stream: EventStreamKind,
    line: String,
    payload: AgentEventPayload,
) {
    let _ = events
        .send(AgentEvent::with_payload(
            namespace.to_string(),
            engine,
            stream,
            line,
            payload,
        ))
        .await;
}

#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        os::unix::fs::PermissionsExt,
        path::Path,
        pin::Pin,
        task::{Context, Poll},
    };

    use tempfile::TempDir;
    use tokio::io::ReadBuf;
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

    struct ErrorReader;

    impl tokio::io::AsyncRead for ErrorReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("broken read")))
        }
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
    fn agent_event_constructors_accept_owned_strings() {
        let namespace = "moni".to_string();
        let line = "hello".to_string();

        let event = AgentEvent::new(
            namespace.clone(),
            AgentEngine::Codex,
            EventStreamKind::Stdout,
            line.clone(),
        );
        let structured = AgentEvent::with_payload(
            namespace,
            AgentEngine::Codex,
            EventStreamKind::Delta,
            line,
            AgentEventPayload::Text {
                text: "hello".to_string(),
            },
        );

        assert_eq!(event.namespace, "moni");
        assert_eq!(event.line, "hello");
        assert_eq!(
            structured.payload,
            Some(AgentEventPayload::Text {
                text: "hello".to_string()
            })
        );
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
    async fn output_task_reports_read_errors() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(AgentEngine::Claude, "/bin/cat");
        let (harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.emit_output_task(Box::new(ErrorReader), EventStreamKind::Stdout);

        let event = next_event(&mut events).await;
        assert_eq!(event.stream, EventStreamKind::Status);
        assert!(event.line.contains("read-error:broken read"));
    }

    #[tokio::test]
    async fn send_reports_missing_stdin_when_running_child_loses_pipe() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        harness.stdin.take();
        let err = harness.send("hello").await.unwrap_err();
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(err.to_string().contains("stdin is unavailable"));
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
    async fn process_harness_passes_model_arg_for_line_protocol() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("model-agent");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
echo "args:$*"
while IFS= read -r line; do
  echo "ok:$line"
done
"#,
        );
        let config = EngineConfig::new(AgentEngine::Codex, bin)
            .with_args(["--alpha"])
            .with_model("gpt-5-codex");
        let (mut harness, mut events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();

        let lines = collect_lines(&mut events, 2).await;
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(
            lines
                .iter()
                .any(|line| line == "args:--alpha --model gpt-5-codex")
        );
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
        let startup_lines = collect_lines(&mut events, 2).await;
        assert!(startup_lines.iter().any(|line| line == "started"));
        assert!(startup_lines.iter().any(|line| line == "ready:"));
        harness.stop(StopReason::Clear).await.unwrap();

        let event = next_event(&mut events).await;
        assert_eq!(event.line, "stopped:Clear");
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

        assert!(err.to_string().contains("No such file"));
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
    async fn reap_if_exited_reports_try_wait_error() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let config = EngineConfig::new(AgentEngine::Claude, bin);
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        harness.fail_next_try_wait_for_test();
        let err = harness.reap_if_exited().await.unwrap_err();
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(err.downcast_ref::<std::io::Error>().is_some());
        assert!(err.to_string().contains("forced try_wait failure"));
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
    async fn codex_app_server_bootstrap_reports_missing_stdin() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(AgentEngine::Codex, "/bin/cat")
            .with_protocol(AgentProtocol::CodexAppServer);
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        let err = harness.bootstrap_codex_app_server().await.unwrap_err();

        assert!(err.to_string().contains("stdin is unavailable"));
    }

    #[tokio::test]
    async fn codex_app_server_send_reports_missing_stdin() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("codex-app-server");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  if [[ "$line" == *'"method":"thread/start"'* ]]; then
    echo '{"id":2,"result":{"thread":{"id":"thread-1"}}}'
  fi
done
"#,
        );
        let config =
            EngineConfig::new(AgentEngine::Codex, bin).with_protocol(AgentProtocol::CodexAppServer);
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        harness.stdin.take();
        let err = harness.send("run").await.unwrap_err();
        harness.stop(StopReason::Shutdown).await.unwrap();

        assert!(err.to_string().contains("stdin is unavailable"));
    }

    #[tokio::test]
    async fn codex_app_server_send_reports_broken_stdin_pipe() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("codex-app-server");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  if [[ "$line" == *'"method":"thread/start"'* ]]; then
    echo '{"id":2,"result":{"thread":{"id":"thread-1"}}}'
    exit 0
  fi
done
"#,
        );
        let config =
            EngineConfig::new(AgentEngine::Codex, bin).with_protocol(AgentProtocol::CodexAppServer);
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        assert_eq!(
            harness.wait_for_codex_thread_id().await.unwrap(),
            "thread-1"
        );
        timeout(
            Duration::from_secs(5),
            harness.child.as_mut().expect("child is running").wait(),
        )
        .await
        .unwrap()
        .unwrap();
        let err = harness.send("run").await.unwrap_err();

        assert!(err.downcast_ref::<std::io::Error>().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn codex_app_server_send_reports_thread_wait_timeout() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(AgentEngine::Codex, "/bin/cat")
            .with_protocol(AgentProtocol::CodexAppServer);
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        let task = tokio::spawn(async move { harness.send_codex_turn("run").await });
        tokio::time::advance(Duration::from_secs(31)).await;
        let err = task.await.unwrap().unwrap_err();

        assert!(err.to_string().contains("timed out waiting for Codex"));
    }

    #[tokio::test]
    async fn codex_app_server_thread_start_uses_configured_model() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("codex-app-server");
        let log = dir.path().join("requests.log");
        write_script(
            &bin,
            r#"#!/usr/bin/env bash
log="$1"
while IFS= read -r line; do
  echo "$line" >> "$log"
  if [[ "$line" == *'"method":"thread/start"'* ]]; then
    echo '{"id":2,"result":{"thread":{"id":"thread-1"}}}'
  fi
done
"#,
        );
        let config = EngineConfig::new(AgentEngine::Codex, bin)
            .with_args([log.to_string_lossy().to_string()])
            .with_protocol(AgentProtocol::CodexAppServer)
            .with_model("gpt-5-codex");
        let (mut harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);

        harness.start().await.unwrap();
        timeout(Duration::from_secs(5), async {
            loop {
                let logged = fs::read_to_string(&log).unwrap_or_default();
                if logged.contains("\"method\":\"thread/start\"") {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        harness.stop(StopReason::Shutdown).await.unwrap();

        let logged = fs::read_to_string(&log).unwrap();
        assert!(logged.contains("\"model\":\"gpt-5-codex\""));
    }

    #[tokio::test(start_paused = true)]
    async fn codex_thread_id_wait_times_out() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(AgentEngine::Codex, "/bin/cat")
            .with_protocol(AgentProtocol::CodexAppServer);
        let (harness, _events) = ProcessAgentHarness::new("moni", dir.path(), config);
        let task = tokio::spawn(async move { harness.wait_for_codex_thread_id().await });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(30)).await;

        let err = task.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("timed out waiting"));
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

    #[tokio::test]
    async fn codex_app_server_line_routes_malformed_json_to_stderr() {
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(Mutex::new(CodexAppServerState::default()));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state,
            "not-json".to_string(),
        )
        .await;

        let event = next_event(&mut rx).await;
        assert_eq!(event.stream, EventStreamKind::Stderr);
        assert_eq!(event.line, "not-json");
    }

    #[tokio::test]
    async fn codex_app_server_line_reports_jsonrpc_error() {
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(Mutex::new(CodexAppServerState::default()));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"error":{"code":-32000,"message":"boom"}}"#.to_string(),
        )
        .await;

        let event = next_event(&mut rx).await;
        assert_eq!(event.stream, EventStreamKind::Status);
        assert!(event.line.contains("codex-error"));
        assert!(event.line.contains("boom"));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state,
            r#"{"error":{"code":-32000}}"#.to_string(),
        )
        .await;

        let fallback = next_event(&mut rx).await;
        assert_eq!(fallback.stream, EventStreamKind::Status);
        assert!(fallback.line.contains(r#""code":-32000"#));
    }

    #[tokio::test]
    async fn codex_app_server_line_ignores_json_without_method() {
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(Mutex::new(CodexAppServerState::default()));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state,
            r#"{"jsonrpc":"2.0","id":1,"result":{}}"#.to_string(),
        )
        .await;

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn codex_app_server_line_ignores_empty_completion_and_reports_error_method() {
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(Mutex::new(CodexAppServerState::default()));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"turn/completed","params":{"threadId":"thread-1"}}"#.to_string(),
        )
        .await;
        assert!(rx.try_recv().is_err());

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"error","params":{"message":"bad turn"}}"#.to_string(),
        )
        .await;

        let event = next_event(&mut rx).await;
        assert_eq!(event.stream, EventStreamKind::Status);
        assert!(event.line.contains("codex-error"));
        assert!(event.line.contains("bad turn"));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state,
            r#"{"method":"error","params":{"code":"missing-message"}}"#.to_string(),
        )
        .await;

        let fallback = next_event(&mut rx).await;
        assert_eq!(fallback.stream, EventStreamKind::Status);
        assert!(fallback.line.contains("missing-message"));
    }

    #[tokio::test]
    async fn codex_app_server_line_handles_thread_delta_and_unknown_methods() {
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(Mutex::new(CodexAppServerState::default()));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"thread/started","params":{"threadId":"thread-2"}}"#.to_string(),
        )
        .await;
        assert_eq!(state.lock().await.thread_id.as_deref(), Some("thread-2"));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"agentMessageDelta","params":{"delta":"chunk"}}"#.to_string(),
        )
        .await;
        let event = next_event(&mut rx).await;
        assert_eq!(event.stream, EventStreamKind::Delta);
        assert_eq!(event.line, "chunk");

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state,
            r#"{"method":"not/handled","params":{}}"#.to_string(),
        )
        .await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn codex_app_server_line_preserves_tool_and_turn_metadata() {
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(Mutex::new(CodexAppServerState::default()));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/started","params":{"item":{"id":"tool-1","type":"commandExecution","command":"cargo test"}}}"#.to_string(),
        )
        .await;
        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/completed","params":{"item":{"id":"tool-1","type":"commandExecution","command":"cargo test","exitCode":101,"stderr":"failed"}}}"#.to_string(),
        )
        .await;
        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/agentMessage/delta","params":{"delta":"done","itemId":"msg-1"}}"#
                .to_string(),
        )
        .await;
        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state,
            r#"{"method":"turn/completed","params":{"model":"gpt-5-codex","durationMs":1234,"usage":{"inputTokens":10,"outputTokens":3},"turn":{"status":"completed"}}}"#.to_string(),
        )
        .await;

        assert_eq!(
            next_event(&mut rx).await.payload,
            Some(AgentEventPayload::ToolStarted {
                id: Some("tool-1".to_string()),
                label: "cargo test".to_string(),
                kind: "commandExecution".to_string()
            })
        );
        assert_eq!(
            next_event(&mut rx).await.payload,
            Some(AgentEventPayload::ToolCompleted {
                id: Some("tool-1".to_string()),
                label: "cargo test".to_string(),
                kind: "commandExecution".to_string(),
                status: None,
                exit_code: Some(101),
                stdout: None,
                stderr: Some("failed".to_string()),
                error: None
            })
        );
        assert_eq!(
            next_event(&mut rx).await.payload,
            Some(AgentEventPayload::Text {
                text: "done".to_string()
            })
        );
        assert_eq!(
            next_event(&mut rx).await.payload,
            Some(AgentEventPayload::TurnCompleted {
                final_text: "done".to_string(),
                model: Some("gpt-5-codex".to_string()),
                duration_ms: Some(1234),
                usage: Some(TokenUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(3),
                    cached_input_tokens: None,
                    total_tokens: None,
                }),
                exit_status: Some("completed".to_string())
            })
        );
    }

    #[tokio::test]
    async fn codex_app_server_line_handles_non_streamed_message_items() {
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(Mutex::new(CodexAppServerState::default()));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/started","params":{"item":{"id":"msg-ignored","type":"agentMessage"}}}"#
                .to_string(),
        )
        .await;
        assert!(rx.try_recv().is_err());

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/started","params":{}}"#.to_string(),
        )
        .await;
        assert!(rx.try_recv().is_err());

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/completed","params":{"item":{"type":"agentMessage","content":[{"type":"image","text":"skip"},{"type":"output_text","text":"from block"}]}}}"#
                .to_string(),
        )
        .await;
        let event = next_event(&mut rx).await;
        assert_eq!(event.stream, EventStreamKind::Delta);
        assert_eq!(event.line, "from block");
        assert_eq!(
            event.payload,
            Some(AgentEventPayload::Text {
                text: "from block".to_string()
            })
        );

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/agentMessage/delta","params":{"delta":"streamed","itemId":"msg-1"}}"#
                .to_string(),
        )
        .await;
        assert_eq!(next_event(&mut rx).await.line, "streamed");

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/completed","params":{"item":{"id":"msg-1","type":"agentMessage","text":"duplicate"}}}"#
                .to_string(),
        )
        .await;
        assert!(rx.try_recv().is_err());

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/completed","params":{"item":{"id":"empty-msg","type":"agentMessage","content":[{"type":"image","text":"skip"}]}}}"#
                .to_string(),
        )
        .await;
        assert!(rx.try_recv().is_err());

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"item/completed","params":{"item":{"id":"unknown-1","type":"unknownItem"}}}"#
                .to_string(),
        )
        .await;
        assert!(rx.try_recv().is_err());

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state,
            r#"{"method":"item/completed","params":{}}"#.to_string(),
        )
        .await;
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn codex_app_server_parsers_cover_fallback_shapes() {
        let non_tool = serde_json::json!({
            "id": "msg-1",
            "type": "agentMessage"
        });
        assert!(parse_tool_started(&non_tool).is_none());
        assert!(parse_tool_completed(&non_tool).is_none());
        assert_eq!(parse_token_usage(Some(&serde_json::json!({}))), None);
        assert_eq!(
            extract_codex_item_text(&serde_json::json!({"content": {"text": "nested"}})),
            Some("nested".to_string())
        );
        assert_eq!(
            extract_codex_item_text(
                &serde_json::json!({"content": [{"type": "image", "text": "skip"}]})
            ),
            None
        );
        assert_eq!(
            extract_codex_item_text(
                &serde_json::json!({"content": [{"type": "text", "text": "plain"}]})
            ),
            Some("plain".to_string())
        );

        let usage = parse_token_usage(Some(&serde_json::json!({
            "promptTokens": "2",
            "completionTokens": "3",
            "cacheReadInputTokens": "1",
            "totalTokens": "6"
        })))
        .unwrap();
        assert_eq!(usage.input_tokens, Some(2));
        assert_eq!(usage.output_tokens, Some(3));
        assert_eq!(usage.cached_input_tokens, Some(1));
        assert_eq!(usage.total_tokens, Some(6));

        let scalars = serde_json::json!({
            "number": 7,
            "flag": true,
            "signed": "-9"
        });
        assert_eq!(find_string(&scalars, &["/number"]), Some("7".to_string()));
        assert_eq!(find_string(&scalars, &["/flag"]), Some("true".to_string()));
        assert_eq!(find_i64(&scalars, &["/signed"]), Some(-9));
        assert_eq!(value_as_u64(&serde_json::json!(-1)), None);
        assert_eq!(value_as_u64(&serde_json::json!(true)), None);
        assert_eq!(value_as_i64(&serde_json::json!(null)), None);

        let started_with_blank_label = parse_tool_started(&serde_json::json!({
            "type": "tool_use",
            "command": " "
        }))
        .unwrap();
        assert_eq!(started_with_blank_label.label, "tool_use");

        let completed = parse_tool_completed(&serde_json::json!({
            "id": "tool-2",
            "type": "mcp_tool_call",
            "server": "filesystem",
            "state": "errored",
            "output": {
                "exit_code": "-7",
                "stderr": true,
                "error": {
                    "message": "boom"
                }
            }
        }))
        .unwrap();
        assert_eq!(completed.id.as_deref(), Some("tool-2"));
        assert_eq!(completed.label, "filesystem");
        assert_eq!(completed.kind, "mcp_tool_call");
        assert_eq!(completed.status.as_deref(), Some("errored"));
        assert_eq!(completed.exit_code, Some(-7));
        assert_eq!(completed.stderr.as_deref(), Some("true"));
        assert_eq!(completed.error.as_deref(), Some("boom"));
        assert_eq!(
            codex_error_message(&serde_json::json!({"code": -32000})),
            "{\"code\":-32000}"
        );
    }

    #[tokio::test]
    async fn codex_app_server_line_ignores_thread_and_delta_without_payloads() {
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(Mutex::new(CodexAppServerState::default()));

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state.clone(),
            r#"{"method":"thread/started","params":{}}"#.to_string(),
        )
        .await;
        assert!(state.lock().await.thread_id.is_none());

        handle_codex_app_server_line(
            "moni",
            AgentEngine::Codex,
            &events,
            state,
            r#"{"method":"agentMessageDelta","params":{}}"#.to_string(),
        )
        .await;
        assert!(rx.try_recv().is_err());
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
            payload: Some(AgentEventPayload::Text {
                text: "hello".to_string(),
            }),
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
