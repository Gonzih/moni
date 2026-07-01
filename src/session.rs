use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{sync::Mutex, task::JoinHandle};

use crate::{
    engine::{AgentEngine, EngineConfig},
    harness::{AgentEventStream, AgentHarness, ProcessAgentHarness, StopReason},
    history::{RunHistory, RunRecord},
    output::{OutputSink, event_to_output_message},
    queue::QueuedPrompt,
};

pub enum EngineConfigResolver {
    Static(EngineConfig),
    Error(String),
}

impl EngineConfigResolver {
    pub fn error(message: String) -> Self {
        Self::Error(message)
    }

    fn config_for(&self, _namespace: &str) -> anyhow::Result<EngineConfig> {
        match self {
            Self::Static(config) => Ok(config.clone()),
            Self::Error(message) => Err(anyhow::anyhow!("{}", message)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StaticEngineConfigResolver;

impl StaticEngineConfigResolver {
    pub fn new(config: EngineConfig) -> EngineConfigResolver {
        EngineConfigResolver::Static(config)
    }
}

struct Session {
    harness: ProcessAgentHarness,
    forward_task: JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceSessionStatus {
    pub namespace: String,
    pub active: bool,
    pub engine: String,
    pub model: Option<String>,
}

pub struct SessionManager {
    workspace_root: PathBuf,
    resolver: Arc<EngineConfigResolver>,
    output: Arc<dyn OutputSink>,
    history: Option<Arc<RunHistory>>,
    sessions: Mutex<HashMap<String, Session>>,
    model_overrides: Mutex<HashMap<String, String>>,
}

impl SessionManager {
    pub fn new(
        workspace_root: PathBuf,
        resolver: Arc<EngineConfigResolver>,
        output: Arc<dyn OutputSink>,
    ) -> Self {
        Self {
            workspace_root,
            resolver,
            output,
            history: None,
            sessions: Mutex::new(HashMap::new()),
            model_overrides: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_run_history(mut self, history: Arc<RunHistory>) -> Self {
        self.history = Some(history);
        self
    }

    pub async fn handle_prompt(&self, prompt: QueuedPrompt) -> anyhow::Result<()> {
        let config = self.config_for_namespace(&prompt.namespace).await?;
        let run_started = if let Some(history) = &self.history {
            Some(
                history
                    .start_run(&prompt.namespace, &prompt.body, config.model.clone())
                    .await?,
            )
        } else {
            None
        };
        let mut sessions = self.sessions.lock().await;
        let session = self
            .ensure_session_locked(&mut sessions, &prompt.namespace, config)
            .await?;
        let result = async {
            session.harness.reap_if_exited().await?;
            if !session.harness.status().running {
                session.harness.start().await?;
            }
            session.harness.send(&prompt.body).await
        }
        .await;
        drop(sessions);
        if let Err(err) = result {
            if run_started.is_some()
                && let Some(history) = &self.history
            {
                let _ = history
                    .record_error(&prompt.namespace, err.to_string())
                    .await;
            }
            return Err(err);
        }
        Ok(())
    }

    pub async fn compact(&self, namespace: &str) -> anyhow::Result<()> {
        self.handle_prompt(QueuedPrompt::new(
            namespace,
            None,
            "/compact",
            "command:compact",
        ))
        .await
    }

    pub async fn goal(&self, namespace: &str, prompt: &str) -> anyhow::Result<&'static str> {
        let config = self.config_for_namespace(namespace).await?;
        let command = agent_goal_command(config.engine);
        self.handle_prompt(QueuedPrompt::new(
            namespace,
            None,
            agent_goal_prompt(config.engine, prompt)?,
            format!("command:{}", command.trim_start_matches('/')),
        ))
        .await?;
        Ok(command.trim_start_matches('/'))
    }

    pub async fn set_model(&self, namespace: &str, model: String) -> anyhow::Result<()> {
        let mut sessions = self.sessions.lock().await;
        self.model_overrides
            .lock()
            .await
            .insert(namespace.to_string(), model);

        if let Some(mut session) = sessions.remove(namespace) {
            match session.harness.stop(StopReason::Replace).await {
                Ok(()) => session.forward_task.abort(),
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub async fn stop_namespace(
        &self,
        namespace: &str,
        reason: StopReason,
    ) -> anyhow::Result<bool> {
        let mut sessions = self.sessions.lock().await;
        let Some(mut session) = sessions.remove(namespace) else {
            return Ok(false);
        };
        match session.harness.stop(reason).await {
            Ok(()) => session.forward_task.abort(),
            Err(err) => return Err(err),
        }
        Ok(true)
    }

    pub async fn active_namespaces(&self) -> Vec<String> {
        self.sessions.lock().await.keys().cloned().collect()
    }

    pub async fn namespace_status(
        &self,
        namespace: &str,
    ) -> anyhow::Result<NamespaceSessionStatus> {
        let active = self.sessions.lock().await.contains_key(namespace);
        let config = self.config_for_namespace(namespace).await?;
        Ok(NamespaceSessionStatus {
            namespace: namespace.to_string(),
            active,
            engine: config.engine.as_str().to_string(),
            model: config.model,
        })
    }

    pub async fn last_run(&self, namespace: &str) -> Option<RunRecord> {
        match &self.history {
            Some(history) => history.last_run(namespace).await,
            None => None,
        }
    }

    fn workspace_path(&self, namespace: &str) -> PathBuf {
        self.workspace_root.join(namespace)
    }

    async fn config_for_namespace(&self, namespace: &str) -> anyhow::Result<EngineConfig> {
        let mut config = self.resolver.config_for(namespace)?;
        if let Some(model) = self.model_overrides.lock().await.get(namespace).cloned() {
            config = config.with_model(model);
        }
        Ok(config)
    }

    async fn ensure_session_locked<'a>(
        &self,
        sessions: &'a mut HashMap<String, Session>,
        namespace: &str,
        config: EngineConfig,
    ) -> anyhow::Result<&'a mut Session> {
        if !sessions.contains_key(namespace) {
            let workspace_path = self.workspace_path(namespace);
            tokio::fs::create_dir_all(&workspace_path).await?;
            let (harness, events) = ProcessAgentHarness::new(namespace, &workspace_path, config);
            let forward_task =
                spawn_output_forwarder(events, self.output.clone(), self.history.clone());
            sessions.insert(
                namespace.to_string(),
                Session {
                    harness,
                    forward_task,
                },
            );
        }

        Ok(sessions.get_mut(namespace).expect("session inserted"))
    }
}

fn spawn_output_forwarder(
    mut events: AgentEventStream,
    output: Arc<dyn OutputSink>,
    history: Option<Arc<RunHistory>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let Some(history) = &history {
                let _ = history.record_agent_event(&event).await;
            }
            if let Some(message) = event_to_output_message(event) {
                let _ = output.send(message).await;
            }
        }
    })
}

pub fn namespace_workspace(root: &Path, namespace: &str) -> PathBuf {
    root.join(namespace)
}

fn agent_goal_command(engine: AgentEngine) -> &'static str {
    match engine {
        AgentEngine::Codex => "/goal",
        AgentEngine::Claude => "/loop",
    }
}

fn agent_goal_prompt(engine: AgentEngine, prompt: &str) -> anyhow::Result<String> {
    let prompt = prompt.trim();
    if prompt.is_empty() {
        anyhow::bail!("missing goal prompt");
    }
    Ok(format!("{} {}", agent_goal_command(engine), prompt))
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use tempfile::TempDir;
    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };

    use crate::{
        engine::{AgentEngine, EngineConfig},
        output::{InMemoryOutputSink, OutputMessage, OutputSink},
    };

    use super::*;

    fn write_mock_agent(path: &Path) {
        fs::write(
            path,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  echo "reply:$line"
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    fn write_arg_echo_agent(path: &Path) {
        fs::write(
            path,
            r#"#!/usr/bin/env bash
echo "args:$*"
while IFS= read -r line; do
  echo "reply:$line"
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    fn write_one_shot_agent(path: &Path) {
        fs::write(
            path,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  echo "reply:$line"
  exit 0
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    async fn wait_for_messages(
        sink: &InMemoryOutputSink,
        count: usize,
    ) -> Vec<crate::output::OutputMessage> {
        timeout(Duration::from_secs(5), async {
            loop {
                let messages = sink.messages().await;
                if messages.len() >= count {
                    return messages;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }

    fn manager(dir: &TempDir, bin: PathBuf, output: InMemoryOutputSink) -> SessionManager {
        let resolver = Arc::new(StaticEngineConfigResolver::new(EngineConfig::new(
            AgentEngine::Claude,
            bin,
        )));
        SessionManager::new(dir.path().join("workspaces"), resolver, Arc::new(output))
    }

    struct FailingOutputSink;

    #[async_trait::async_trait]
    impl OutputSink for FailingOutputSink {
        async fn send(&self, _message: OutputMessage) -> anyhow::Result<()> {
            anyhow::bail!("output failed")
        }

        async fn live_status(&self, _namespace: &str) -> String {
            "unavailable".to_string()
        }
    }

    #[test]
    fn namespace_workspace_joins_root_and_namespace() {
        assert_eq!(
            namespace_workspace(Path::new("/tmp/root"), "moni"),
            PathBuf::from("/tmp/root/moni")
        );
    }

    #[tokio::test]
    async fn handle_prompt_starts_session_and_forwards_output() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "hello", "test"))
            .await
            .unwrap();

        let messages = wait_for_messages(&output, 1).await;
        assert_eq!(messages[0].namespace, "moni");
        assert_eq!(messages[0].body, "reply:hello");
    }

    #[tokio::test]
    async fn handle_prompt_reuses_existing_session() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "one", "test"))
            .await
            .unwrap();
        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "two", "test"))
            .await
            .unwrap();

        let messages = wait_for_messages(&output, 2).await;
        assert!(messages.iter().any(|message| message.body == "reply:one"));
        assert!(messages.iter().any(|message| message.body == "reply:two"));
        assert_eq!(manager.active_namespaces().await, vec!["moni".to_string()]);
    }

    #[tokio::test]
    async fn stop_namespace_removes_active_session() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output);

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "one", "test"))
            .await
            .unwrap();

        assert!(
            manager
                .stop_namespace("moni", StopReason::Reset)
                .await
                .unwrap()
        );
        assert!(manager.active_namespaces().await.is_empty());
    }

    #[tokio::test]
    async fn stop_missing_namespace_returns_false() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output);

        assert!(
            !manager
                .stop_namespace("missing", StopReason::Reset)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn compact_sends_compact_prompt() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager.compact("moni").await.unwrap();

        let messages = wait_for_messages(&output, 1).await;
        assert_eq!(messages[0].body, "reply:/compact");
    }

    #[tokio::test]
    async fn output_forwarder_exits_when_event_stream_closes() {
        let (tx, rx) = mpsc::channel(1);
        let output = InMemoryOutputSink::default();
        drop(tx);

        timeout(
            Duration::from_secs(1),
            spawn_output_forwarder(rx, Arc::new(output), None),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn output_forwarder_records_final_history_before_output_failure() {
        let (tx, rx) = mpsc::channel(1);
        let history = Arc::new(RunHistory::in_memory());
        history
            .start_run("moni", "prompt", Some("gpt-5-codex".to_string()))
            .await
            .unwrap();
        let task = spawn_output_forwarder(rx, Arc::new(FailingOutputSink), Some(history.clone()));

        tx.send(crate::harness::AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: crate::harness::EventStreamKind::Final,
            line: "final".to_string(),
            payload: Some(crate::harness::AgentEventPayload::TurnCompleted {
                final_text: "final".to_string(),
                model: Some("gpt-5-codex".to_string()),
                duration_ms: Some(12),
                usage: None,
                exit_status: Some("completed".to_string()),
            }),
        })
        .await
        .unwrap();
        drop(tx);
        task.await.unwrap();

        let run = history.last_run("moni").await.unwrap();
        assert_eq!(run.final_result.as_deref(), Some("final"));
        assert_eq!(run.duration_ms, Some(12));
    }

    #[tokio::test]
    async fn handle_prompt_reports_workspace_creation_error() {
        let dir = TempDir::new().unwrap();
        let workspace_root = dir.path().join("not-a-directory");
        fs::write(&workspace_root, "file").unwrap();
        let output = InMemoryOutputSink::default();
        let resolver = Arc::new(StaticEngineConfigResolver::new(EngineConfig::new(
            AgentEngine::Claude,
            "/bin/cat",
        )));
        let manager = SessionManager::new(workspace_root, resolver, Arc::new(output));

        let err = manager
            .handle_prompt(QueuedPrompt::new("moni", None, "hello", "test"))
            .await
            .unwrap_err();

        assert!(err.downcast_ref::<std::io::Error>().is_some());
        assert!(manager.active_namespaces().await.is_empty());
    }

    #[tokio::test]
    async fn handle_prompt_reports_resolver_error() {
        let dir = TempDir::new().unwrap();
        let output = InMemoryOutputSink::default();
        let manager = SessionManager::new(
            dir.path().join("workspaces"),
            Arc::new(EngineConfigResolver::error("resolver failed".to_string())),
            Arc::new(output),
        );

        let err = manager
            .handle_prompt(QueuedPrompt::new("moni", None, "hello", "test"))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("resolver failed"));
        assert!(manager.active_namespaces().await.is_empty());
    }

    #[tokio::test]
    async fn handle_prompt_reports_send_error() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "first", "test"))
            .await
            .unwrap();
        {
            let mut sessions = manager.sessions.lock().await;
            sessions
                .get_mut("moni")
                .expect("session is active")
                .harness
                .drop_stdin_for_test();
        }

        let err = manager
            .handle_prompt(QueuedPrompt::new("moni", None, "second", "test"))
            .await
            .unwrap_err();
        manager
            .stop_namespace("moni", StopReason::Shutdown)
            .await
            .unwrap();

        assert!(err.to_string().contains("stdin is unavailable"));
    }

    #[tokio::test]
    async fn handle_prompt_records_history_error_after_run_start() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let history = Arc::new(RunHistory::in_memory());
        let manager = manager(&dir, bin, output.clone()).with_run_history(history.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "first", "test"))
            .await
            .unwrap();
        {
            let mut sessions = manager.sessions.lock().await;
            sessions
                .get_mut("moni")
                .expect("session is active")
                .harness
                .drop_stdin_for_test();
        }

        let err = manager
            .handle_prompt(QueuedPrompt::new("moni", None, "second", "test"))
            .await
            .unwrap_err();
        manager
            .stop_namespace("moni", StopReason::Shutdown)
            .await
            .unwrap();

        let run = history.last_run("moni").await.unwrap();
        assert!(err.to_string().contains("stdin is unavailable"));
        assert_eq!(run.prompt, "second");
        assert_eq!(run.exit_status.as_deref(), Some("error"));
        assert!(
            run.errors
                .iter()
                .any(|error| error.contains("stdin is unavailable"))
        );
    }

    #[tokio::test]
    async fn handle_prompt_reports_reap_error() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "first", "test"))
            .await
            .unwrap();
        {
            let mut sessions = manager.sessions.lock().await;
            sessions
                .get_mut("moni")
                .expect("session is active")
                .harness
                .fail_next_reap_for_test();
        }

        let err = manager
            .handle_prompt(QueuedPrompt::new("moni", None, "second", "test"))
            .await
            .unwrap_err();
        manager
            .stop_namespace("moni", StopReason::Shutdown)
            .await
            .unwrap();

        assert!(err.to_string().contains("forced reap failure"));
    }

    #[tokio::test]
    async fn failing_output_sink_reports_unavailable_live_status() {
        assert_eq!(FailingOutputSink.live_status("moni").await, "unavailable");
    }

    #[tokio::test]
    async fn set_model_replaces_active_session_and_applies_override() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("model-agent");
        write_arg_echo_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "before", "test"))
            .await
            .unwrap();
        wait_for_messages(&output, 2).await;
        manager
            .set_model("moni", "prompt".to_string())
            .await
            .unwrap();
        assert!(manager.active_namespaces().await.is_empty());
        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "after", "test"))
            .await
            .unwrap();

        let bodies = wait_for_messages(&output, 4)
            .await
            .into_iter()
            .map(|message| message.body)
            .collect::<Vec<_>>();
        assert!(bodies.iter().any(|body| body == "reply:before"));
        assert!(bodies.iter().any(|body| body == "args:--model prompt"));
        assert!(bodies.iter().any(|body| body == "reply:after"));
    }

    #[tokio::test]
    async fn namespace_status_reports_active_state_and_model() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("model-agent");
        write_arg_echo_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        let idle = manager.namespace_status("moni").await.unwrap();
        assert_eq!(
            idle,
            NamespaceSessionStatus {
                namespace: "moni".to_string(),
                active: false,
                engine: "claude".to_string(),
                model: None,
            }
        );

        manager
            .set_model("moni", "prompt".to_string())
            .await
            .unwrap();
        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "after", "test"))
            .await
            .unwrap();
        wait_for_messages(&output, 2).await;

        let active = manager.namespace_status("moni").await.unwrap();
        assert_eq!(active.active, true);
        assert_eq!(active.model.as_deref(), Some("prompt"));
    }

    #[tokio::test]
    async fn handle_prompt_records_run_history_from_structured_events() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("codex-app-server");
        fs::write(
            &bin,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  if [[ "$line" == *'"method":"thread/start"'* ]]; then
    echo '{"id":2,"result":{"thread":{"id":"thread-1"}}}'
  elif [[ "$line" == *'"method":"turn/start"'* ]]; then
    echo '{"method":"item/started","params":{"item":{"id":"tool-1","type":"commandExecution","command":"cargo test"}}}'
    echo '{"method":"item/completed","params":{"item":{"id":"tool-1","type":"commandExecution","command":"cargo test","exitCode":0}}}'
    echo '{"method":"item/agentMessage/delta","params":{"delta":"done","itemId":"msg-1"}}'
    echo '{"method":"turn/completed","params":{"model":"gpt-5-codex","durationMs":88,"usage":{"inputTokens":4,"outputTokens":2},"turn":{"status":"completed"}}}'
  fi
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&bin).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&bin, permissions).unwrap();
        let output = InMemoryOutputSink::default();
        let history = Arc::new(RunHistory::in_memory());
        let resolver = Arc::new(StaticEngineConfigResolver::new(
            EngineConfig::new(AgentEngine::Codex, bin)
                .with_protocol(crate::engine::AgentProtocol::CodexAppServer)
                .with_model("gpt-5-codex"),
        ));
        let manager = SessionManager::new(
            dir.path().join("workspaces"),
            resolver,
            Arc::new(output.clone()),
        )
        .with_run_history(history.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "run tests", "test"))
            .await
            .unwrap();

        let _ = wait_for_messages(&output, 3).await;
        let run = history.last_run("moni").await.unwrap();
        assert_eq!(run.prompt, "run tests");
        assert_eq!(run.model.as_deref(), Some("gpt-5-codex"));
        assert_eq!(run.tool_calls[0].label, "cargo test");
        assert_eq!(run.tool_calls[0].exit_code, Some(0));
        assert_eq!(run.final_result.as_deref(), Some("done"));
        assert_eq!(run.duration_ms, Some(88));
        assert_eq!(manager.last_run("moni").await.unwrap().id, run.id);
    }

    #[test]
    fn agent_goal_prompt_maps_to_engine_command() {
        assert_eq!(
            agent_goal_prompt(AgentEngine::Codex, "ship it").unwrap(),
            "/goal ship it"
        );
        assert_eq!(
            agent_goal_prompt(AgentEngine::Claude, "ship it").unwrap(),
            "/loop ship it"
        );
        assert!(agent_goal_prompt(AgentEngine::Codex, "   ").is_err());
    }

    #[tokio::test]
    async fn goal_command_reaches_codex_app_server_as_goal_turn() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("codex-app-server");
        fs::write(
            &bin,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  if [[ "$line" == *'"method":"thread/start"'* ]]; then
    echo '{"id":2,"result":{"thread":{"id":"thread-1"}}}'
  elif [[ "$line" == *'"method":"turn/start"'* ]]; then
    if [[ "$line" == *'/goal ship it'* ]]; then
      echo '{"method":"item/agentMessage/delta","params":{"delta":"goal received","itemId":"msg-1"}}'
    else
      echo '{"method":"item/agentMessage/delta","params":{"delta":"unexpected turn","itemId":"msg-1"}}'
    fi
  fi
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&bin).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&bin, permissions).unwrap();
        let output = InMemoryOutputSink::default();
        let resolver = Arc::new(StaticEngineConfigResolver::new(
            EngineConfig::new(AgentEngine::Codex, bin)
                .with_protocol(crate::engine::AgentProtocol::CodexAppServer),
        ));
        let manager = SessionManager::new(
            dir.path().join("workspaces"),
            resolver,
            Arc::new(output.clone()),
        );

        assert_eq!(manager.goal("moni", "ship it").await.unwrap(), "goal");

        let messages = wait_for_messages(&output, 1).await;
        assert_eq!(messages[0].body, "goal received");
    }

    #[tokio::test]
    async fn set_model_reports_stop_error_for_active_session() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("one-shot-agent");
        write_one_shot_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "before", "test"))
            .await
            .unwrap();
        wait_for_messages(&output, 1).await;
        {
            let mut sessions = manager.sessions.lock().await;
            sessions
                .get_mut("moni")
                .expect("session is active")
                .harness
                .fail_next_try_wait_for_test();
        }

        let err = manager
            .set_model("moni", "prompt".to_string())
            .await
            .unwrap_err();

        assert!(err.downcast_ref::<std::io::Error>().is_some());
        assert!(err.to_string().contains("forced try_wait failure"));
        assert!(manager.active_namespaces().await.is_empty());
    }

    #[tokio::test]
    async fn stop_namespace_reports_stop_error_for_active_session() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("one-shot-agent");
        write_one_shot_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "before", "test"))
            .await
            .unwrap();
        wait_for_messages(&output, 1).await;
        {
            let mut sessions = manager.sessions.lock().await;
            sessions
                .get_mut("moni")
                .expect("session is active")
                .harness
                .fail_next_try_wait_for_test();
        }

        let err = manager
            .stop_namespace("moni", StopReason::Shutdown)
            .await
            .unwrap_err();

        assert!(err.downcast_ref::<std::io::Error>().is_some());
        assert!(err.to_string().contains("forced try_wait failure"));
        assert!(manager.active_namespaces().await.is_empty());
    }

    #[tokio::test]
    async fn handle_prompt_creates_namespace_workspace() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output);

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "one", "test"))
            .await
            .unwrap();

        assert!(dir.path().join("workspaces/moni").is_dir());
    }
}
