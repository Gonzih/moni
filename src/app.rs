use std::{future::Future, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use crate::{
    commands::{CommandAction, parse_command},
    cron::{CronEngine, CronTask},
    discord::{ChannelBinding, DiscordInboundMessage, route_discord_message},
    harness::StopReason,
    output::{OutputMessage, OutputSink},
    queue::NamespaceQueue,
    registry::BindingRegistry,
    session::SessionManager,
    store::{MoniState, StateStore},
};

pub struct MoniAppConfig {
    pub queue: Arc<dyn NamespaceQueue>,
    pub sessions: Arc<SessionManager>,
    pub output: Arc<dyn OutputSink>,
    pub cron: CronEngine,
    pub registry: BindingRegistry,
    pub state_store: Option<Arc<dyn StateStore>>,
    pub voice_status: Option<String>,
}

pub struct MoniApp {
    queue: Arc<dyn NamespaceQueue>,
    sessions: Arc<SessionManager>,
    output: Arc<dyn OutputSink>,
    cron: Mutex<CronEngine>,
    registry: BindingRegistry,
    state_store: Option<Arc<dyn StateStore>>,
    voice_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutcome {
    pub namespace: String,
    pub body: String,
}

impl MoniApp {
    pub fn new(config: MoniAppConfig) -> Self {
        Self {
            queue: config.queue,
            sessions: config.sessions,
            output: config.output,
            cron: Mutex::new(config.cron),
            registry: config.registry,
            state_store: config.state_store,
            voice_status: config.voice_status,
        }
    }

    pub async fn handle_unbound_discord_message(
        &self,
        channel_id: String,
        message: DiscordInboundMessage,
    ) -> anyhow::Result<bool> {
        let Some(command) = parse_command("", &message.body)? else {
            return Ok(false);
        };
        let CommandAction::Register {
            namespace,
            repo_url,
        } = command.action
        else {
            return Ok(false);
        };

        let binding = ChannelBinding {
            channel_id,
            namespace,
            repo_url,
        };
        self.registry.upsert(binding.clone()).await?;
        self.persist_state().await?;
        self.ack(&binding.namespace, "registered".to_string())
            .await?;
        Ok(true)
    }

    pub async fn handle_discord_message(
        &self,
        binding: &ChannelBinding,
        message: DiscordInboundMessage,
    ) -> anyhow::Result<()> {
        if let Some(command) = parse_command(binding.namespace.clone(), &message.body)? {
            self.handle_command(binding, command.action).await?;
            return Ok(());
        }

        route_discord_message(self.queue.as_ref(), binding, message).await?;
        Ok(())
    }

    pub async fn register_binding(&self, binding: ChannelBinding) -> anyhow::Result<()> {
        self.registry.upsert(binding).await?;
        self.persist_state().await
    }

    pub async fn handle_queued_prompt(
        &self,
        prompt: crate::queue::QueuedPrompt,
    ) -> anyhow::Result<()> {
        self.sessions.handle_prompt(prompt).await
    }

    pub async fn tick_cron(&self, now: DateTime<Utc>) -> anyhow::Result<Vec<String>> {
        let mut cron = self.cron.lock().await;
        let fired = cron.tick(self.queue.as_ref(), now).await?;
        drop(cron);

        if fired.is_empty() {
            return Ok(fired);
        }

        self.persist_state().await?;
        Ok(fired)
    }

    pub async fn cron_count(&self) -> usize {
        self.cron.lock().await.tasks().len()
    }

    pub async fn handle_command_action(
        &self,
        binding: &ChannelBinding,
        action: CommandAction,
    ) -> anyhow::Result<CommandOutcome> {
        match action {
            CommandAction::Register {
                namespace,
                repo_url,
            } => {
                let binding = ChannelBinding {
                    channel_id: binding.channel_id.clone(),
                    namespace,
                    repo_url,
                };
                self.registry.upsert(binding.clone()).await?;
                self.persist_state().await?;
                Ok(command_outcome(
                    &binding.namespace,
                    "registered".to_string(),
                ))
            }
            CommandAction::Reset => {
                self.sessions
                    .stop_namespace(&binding.namespace, StopReason::Reset)
                    .await?;
                Ok(command_outcome(
                    &binding.namespace,
                    "reset complete".to_string(),
                ))
            }
            CommandAction::Clear => {
                self.sessions
                    .stop_namespace(&binding.namespace, StopReason::Clear)
                    .await?;
                Ok(command_outcome(
                    &binding.namespace,
                    "clear complete".to_string(),
                ))
            }
            CommandAction::Compact => {
                self.sessions.compact(&binding.namespace).await?;
                Ok(command_outcome(
                    &binding.namespace,
                    "compact queued".to_string(),
                ))
            }
            CommandAction::Status => {
                let session = self.sessions.namespace_status(&binding.namespace).await?;
                let cron_count = self
                    .cron
                    .lock()
                    .await
                    .tasks()
                    .iter()
                    .filter(|task| task.namespace == binding.namespace)
                    .count();
                let session_state = if session.active { "active" } else { "idle" };
                let model = session.model.as_deref().unwrap_or("default");
                let queue_depth = match self.queue.depth(&binding.namespace).await? {
                    Some(depth) => depth.to_string(),
                    None => "unavailable".to_string(),
                };
                let nats = if queue_depth == "unavailable" {
                    "configured"
                } else {
                    "not configured"
                };
                let last_run = self
                    .sessions
                    .last_run(&binding.namespace)
                    .await
                    .map(|run| {
                        let status = run.exit_status.unwrap_or_else(|| "running".to_string());
                        let tools = run.tool_calls.len();
                        format!("last run: {status}, tools: {tools}")
                    })
                    .unwrap_or_else(|| "last run: none".to_string());
                let live_status = self.output.live_status(&binding.namespace).await;
                let voice_status = self
                    .voice_status
                    .as_deref()
                    .map(first_status_line)
                    .unwrap_or("unavailable");
                Ok(command_outcome(
                    &binding.namespace,
                    format!(
                        "status\nnamespace: {}\nrepo: {}\nsession: {}\nengine: {}\nmodel: {}\ncrons: {}\nqueue depth: {}\nnats: {}\nlive output: {}\nvoice: {}\n{}",
                        session.namespace,
                        binding.repo_url,
                        session_state,
                        session.engine,
                        model,
                        cron_count,
                        queue_depth,
                        nats,
                        live_status,
                        voice_status,
                        last_run
                    ),
                ))
            }
            CommandAction::Goal { prompt } => {
                let command = self.sessions.goal(&binding.namespace, &prompt).await?;
                Ok(command_outcome(
                    &binding.namespace,
                    format!("{command} queued"),
                ))
            }
            CommandAction::SetModel { model } => {
                self.sessions
                    .set_model(&binding.namespace, model.clone())
                    .await?;
                Ok(command_outcome(
                    &binding.namespace,
                    format!("model set to {model}"),
                ))
            }
            CommandAction::VoiceStatus => Ok(command_outcome(
                &binding.namespace,
                "voice status is available through Discord `/voice status`".to_string(),
            )),
            CommandAction::CronAdd { schedule, message } => {
                let task = CronTask::new(
                    binding.namespace.clone(),
                    binding.repo_url.clone(),
                    schedule,
                    message,
                );
                let id = task.id.clone();
                self.cron.lock().await.add(task);
                self.persist_state().await?;
                Ok(command_outcome(
                    &binding.namespace,
                    format!("cron added {id}"),
                ))
            }
            CommandAction::CronList => {
                let cron = self.cron.lock().await;
                let rows = cron
                    .tasks()
                    .iter()
                    .filter(|task| task.namespace == binding.namespace)
                    .map(|task| format!("{} {} {}", task.id, task.schedule, task.status_string()))
                    .collect::<Vec<_>>();
                let body = if rows.is_empty() {
                    "no crons".to_string()
                } else {
                    rows.join("\n")
                };
                Ok(command_outcome(&binding.namespace, body))
            }
            CommandAction::CronPause { id } => {
                let changed = self.cron.lock().await.pause(&id);
                self.persist_state().await?;
                Ok(command_outcome(
                    &binding.namespace,
                    bool_body(changed, "cron paused", "cron not found"),
                ))
            }
            CommandAction::CronResume { id } => {
                let changed = self.cron.lock().await.resume(&id);
                self.persist_state().await?;
                Ok(command_outcome(
                    &binding.namespace,
                    bool_body(changed, "cron resumed", "cron not found"),
                ))
            }
            CommandAction::CronDelete { id } => {
                let changed = self.cron.lock().await.delete(&id);
                self.persist_state().await?;
                Ok(command_outcome(
                    &binding.namespace,
                    bool_body(changed, "cron deleted", "cron not found"),
                ))
            }
        }
    }

    async fn handle_command(
        &self,
        binding: &ChannelBinding,
        action: CommandAction,
    ) -> anyhow::Result<()> {
        let outcome = self.handle_command_action(binding, action).await?;
        self.ack(&outcome.namespace, outcome.body).await
    }

    async fn ack(&self, namespace: &str, body: String) -> anyhow::Result<()> {
        self.output
            .send(OutputMessage::complete(namespace, &body))
            .await
    }

    async fn persist_state(&self) -> anyhow::Result<()> {
        let Some(store) = &self.state_store else {
            return Ok(());
        };
        let cron = self.cron.lock().await;
        store
            .save(&MoniState {
                bindings: self.registry.all().await,
                cron_tasks: cron.tasks().to_vec(),
            })
            .await
    }
}

fn command_outcome(namespace: &str, body: String) -> CommandOutcome {
    CommandOutcome {
        namespace: namespace.to_string(),
        body,
    }
}

fn bool_body(changed: bool, ok: &str, missing: &str) -> String {
    if changed { ok } else { missing }.to_string()
}

fn first_status_line(status: &str) -> &str {
    status.lines().next().unwrap_or("unknown")
}

pub fn run_cron_loop(
    app: Arc<MoniApp>,
    tick_every: Duration,
) -> impl Future<Output = anyhow::Result<()>> + Send {
    run_cron_loop_until(app, tick_every, std::future::pending::<()>())
}

async fn run_cron_loop_until<S>(
    app: Arc<MoniApp>,
    tick_every: Duration,
    shutdown: S,
) -> anyhow::Result<()>
where
    S: Future<Output = ()>,
{
    let mut interval = tokio::time::interval(tick_every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            _ = interval.tick() => {
                let fired = app.tick_cron(Utc::now()).await?;
                if !fired.is_empty() {
                    tracing::info!(count = fired.len(), tasks = ?fired, "cron tasks fired");
                }
            }
        }
    }
}

trait CronStatusString {
    fn status_string(&self) -> &'static str;
}

impl CronStatusString for CronTask {
    fn status_string(&self) -> &'static str {
        match self.status {
            crate::cron::CronTaskStatus::Active => "active",
            crate::cron::CronTaskStatus::Paused => "paused",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt, path::Path, sync::Arc};

    use chrono::TimeZone;
    use tempfile::TempDir;
    use tokio::time::{Duration, timeout};

    use crate::{
        FileStateStore, RunHistory,
        engine::{AgentEngine, EngineConfig},
        harness::{AgentEvent, AgentEventPayload, EventStreamKind},
        nats::{NatsNamespaceQueue, run_nats_prompt_consumer},
        output::InMemoryOutputSink,
        queue::{InMemoryNamespaceQueue, NamespaceQueue, QueuedPrompt},
        session::StaticEngineConfigResolver,
    };

    use super::*;

    #[derive(Default)]
    struct DefaultDepthQueue {
        prompts: tokio::sync::Mutex<Vec<QueuedPrompt>>,
    }

    #[async_trait::async_trait]
    impl NamespaceQueue for DefaultDepthQueue {
        async fn enqueue(&self, prompt: QueuedPrompt) -> anyhow::Result<()> {
            self.prompts.lock().await.push(prompt);
            Ok(())
        }

        async fn drain_namespace(&self, namespace: &str) -> anyhow::Result<Vec<QueuedPrompt>> {
            let mut prompts = self.prompts.lock().await;
            let mut drained = Vec::new();
            let mut retained = Vec::new();
            for prompt in prompts.drain(..) {
                if prompt.namespace == namespace {
                    drained.push(prompt);
                } else {
                    retained.push(prompt);
                }
            }
            *prompts = retained;
            Ok(drained)
        }
    }

    fn binding() -> ChannelBinding {
        ChannelBinding {
            channel_id: "1".to_string(),
            namespace: "moni".to_string(),
            repo_url: "https://example.com/moni".to_string(),
        }
    }

    fn message(body: &str) -> DiscordInboundMessage {
        DiscordInboundMessage {
            channel_id: "1".to_string(),
            author_id: "u1".to_string(),
            body: body.to_string(),
        }
    }

    fn write_mock_agent(path: &Path) {
        fs::write(
            path,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  echo "agent:$line"
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    fn app(dir: &TempDir) -> (MoniApp, InMemoryNamespaceQueue, InMemoryOutputSink) {
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let queue = InMemoryNamespaceQueue::default();
        let output = InMemoryOutputSink::default();
        let resolver = Arc::new(StaticEngineConfigResolver::new(EngineConfig::new(
            AgentEngine::Claude,
            bin,
        )));
        let sessions = Arc::new(SessionManager::new(
            dir.path().join("workspaces"),
            resolver,
            Arc::new(output.clone()),
        ));
        let app = MoniApp::new(MoniAppConfig {
            queue: Arc::new(queue.clone()),
            sessions,
            output: Arc::new(output.clone()),
            cron: CronEngine::default(),
            registry: BindingRegistry::new([binding()]).unwrap(),
            state_store: None,
            voice_status: None,
        });
        (app, queue, output)
    }

    fn app_with_history(
        dir: &TempDir,
        history: Arc<RunHistory>,
    ) -> (MoniApp, InMemoryNamespaceQueue, InMemoryOutputSink) {
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let queue = InMemoryNamespaceQueue::default();
        let output = InMemoryOutputSink::default();
        let resolver = Arc::new(StaticEngineConfigResolver::new(EngineConfig::new(
            AgentEngine::Claude,
            bin,
        )));
        let sessions = Arc::new(
            SessionManager::new(
                dir.path().join("workspaces"),
                resolver,
                Arc::new(output.clone()),
            )
            .with_run_history(history),
        );
        let app = MoniApp::new(MoniAppConfig {
            queue: Arc::new(queue.clone()),
            sessions,
            output: Arc::new(output.clone()),
            cron: CronEngine::default(),
            registry: BindingRegistry::new([binding()]).unwrap(),
            state_store: None,
            voice_status: None,
        });
        (app, queue, output)
    }

    async fn wait_for_output(output: &InMemoryOutputSink, count: usize) -> Vec<OutputMessage> {
        timeout(Duration::from_secs(2), async {
            loop {
                let messages = output.messages().await;
                if messages.len() >= count {
                    return messages;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn normal_discord_message_is_enqueued() {
        let dir = TempDir::new().unwrap();
        let (app, queue, _) = app(&dir);

        app.handle_discord_message(&binding(), message("hello"))
            .await
            .unwrap();

        assert_eq!(
            queue.drain_namespace("moni").await.unwrap()[0].body,
            "hello"
        );
    }

    #[tokio::test]
    async fn reset_command_is_acknowledged() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/reset"))
            .await
            .unwrap();

        assert_eq!(output.messages().await[0].body, "reset complete");
    }

    #[tokio::test]
    async fn register_command_updates_registry() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/register ops https://example.com/ops"))
            .await
            .unwrap();

        assert_eq!(
            app.registry
                .get_by_channel(serenity::model::id::ChannelId::new(1))
                .await
                .unwrap()
                .namespace,
            "ops"
        );
        assert_eq!(output.messages().await[0].body, "registered");
    }

    #[tokio::test]
    async fn unbound_register_creates_binding() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        let handled = app
            .handle_unbound_discord_message(
                "55".to_string(),
                message("/register new https://example.com/new"),
            )
            .await
            .unwrap();

        assert!(handled);
        assert_eq!(
            app.registry
                .get_by_channel(serenity::model::id::ChannelId::new(55))
                .await
                .unwrap()
                .namespace,
            "new"
        );
        assert_eq!(output.messages().await[0].body, "registered");
    }

    #[tokio::test]
    async fn unbound_non_command_is_ignored() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        let handled = app
            .handle_unbound_discord_message("55".to_string(), message("hello"))
            .await
            .unwrap();

        assert!(!handled);
        assert!(output.messages().await.is_empty());
    }

    #[tokio::test]
    async fn unbound_non_register_command_is_ignored() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        let handled = app
            .handle_unbound_discord_message("55".to_string(), message("/reset"))
            .await
            .unwrap();

        assert!(!handled);
        assert!(output.messages().await.is_empty());
    }

    #[tokio::test]
    async fn register_binding_persists_state() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(FileStateStore::new(dir.path().join("state.json")));
        let (base_app, _, output) = app(&dir);
        let app = MoniApp::new(MoniAppConfig {
            queue: base_app.queue,
            sessions: base_app.sessions,
            output: Arc::new(output),
            cron: CronEngine::default(),
            registry: BindingRegistry::new(Vec::new()).unwrap(),
            state_store: Some(store.clone()),
            voice_status: None,
        });

        app.register_binding(ChannelBinding {
            channel_id: "77".to_string(),
            namespace: "ops".to_string(),
            repo_url: "https://example.com/ops".to_string(),
        })
        .await
        .unwrap();

        let state = store.load().await.unwrap();
        assert_eq!(state.bindings.len(), 1);
        assert_eq!(state.bindings[0].namespace, "ops");
    }

    #[tokio::test]
    async fn clear_command_is_acknowledged() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/clear"))
            .await
            .unwrap();

        assert_eq!(output.messages().await[0].body, "clear complete");
    }

    #[tokio::test]
    async fn compact_command_reaches_agent_and_acks() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/compact"))
            .await
            .unwrap();

        let messages = wait_for_output(&output, 2).await;
        assert!(
            messages
                .iter()
                .any(|message| message.body == "agent:/compact")
        );
        assert!(
            messages
                .iter()
                .any(|message| message.body == "compact queued")
        );
    }

    #[tokio::test]
    async fn goal_command_reaches_claude_as_loop_and_acks() {
        let dir = TempDir::new().unwrap();
        let (app, queue, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/goal keep going"))
            .await
            .unwrap();

        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
        let messages = wait_for_output(&output, 2).await;
        assert!(
            messages
                .iter()
                .any(|message| message.body == "agent:/loop keep going")
        );
        assert!(messages.iter().any(|message| message.body == "loop queued"));
    }

    #[tokio::test]
    async fn model_command_updates_session_model_and_acks() {
        let dir = TempDir::new().unwrap();
        let (app, queue, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/model prompt"))
            .await
            .unwrap();

        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
        assert_eq!(output.messages().await[0].body, "model set to prompt");
    }

    #[tokio::test]
    async fn status_command_reports_namespace_session_model_and_crons() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/model prompt"))
            .await
            .unwrap();
        app.handle_discord_message(&binding(), message("/cron add * * * * * run"))
            .await
            .unwrap();
        app.handle_discord_message(&binding(), message("/status"))
            .await
            .unwrap();

        let status = output.messages().await.last().unwrap().body.clone();
        assert!(status.contains("namespace: moni"));
        assert!(status.contains("repo: https://example.com/moni"));
        assert!(status.contains("session: idle"));
        assert!(status.contains("engine: claude"));
        assert!(status.contains("model: prompt"));
        assert!(status.contains("crons: 1"));
        assert!(status.contains("queue depth: 0"));
        assert!(status.contains("nats: not configured"));
        assert!(status.contains("live output: unavailable"));
        assert!(status.contains("voice: unavailable"));
        assert!(status.contains("last run: none"));
    }

    #[test]
    fn first_status_line_uses_first_nonempty_line_or_unknown() {
        assert_eq!(first_status_line("voice ok\nmore"), "voice ok");
        assert_eq!(first_status_line(""), "unknown");
    }

    #[test]
    fn cron_status_string_reports_active_and_paused() {
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");

        assert_eq!(task.status_string(), "active");
        task.status = crate::cron::CronTaskStatus::Paused;
        assert_eq!(task.status_string(), "paused");
    }

    #[tokio::test]
    async fn default_depth_queue_drains_only_requested_namespace() {
        let queue = DefaultDepthQueue::default();

        queue
            .enqueue(QueuedPrompt::new("moni", None, "one", "test"))
            .await
            .unwrap();
        queue
            .enqueue(QueuedPrompt::new("ops", None, "two", "test"))
            .await
            .unwrap();

        let drained = queue.drain_namespace("moni").await.unwrap();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].body, "one");
        assert_eq!(queue.drain_namespace("ops").await.unwrap().len(), 1);
        assert!(queue.drain_namespace("missing").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cron_count_reports_total_tasks() {
        let dir = TempDir::new().unwrap();
        let (app, _, _) = app(&dir);

        assert_eq!(app.cron_count().await, 0);
        app.handle_discord_message(&binding(), message("/cron add * * * * * run"))
            .await
            .unwrap();

        assert_eq!(app.cron_count().await, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn public_cron_loop_future_starts_until_cancelled() {
        let dir = TempDir::new().unwrap();
        let (app, _, _) = app(&dir);

        let handle = tokio::spawn(run_cron_loop(Arc::new(app), Duration::from_secs(60)));
        tokio::task::yield_now().await;
        handle.abort();

        assert!(handle.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn status_command_reports_configured_voice_health() {
        let dir = TempDir::new().unwrap();
        let (base_app, queue, output) = app(&dir);
        let app = MoniApp::new(MoniAppConfig {
            queue: Arc::new(queue),
            sessions: base_app.sessions,
            output: Arc::new(output.clone()),
            cron: CronEngine::default(),
            registry: BindingRegistry::new([binding()]).unwrap(),
            state_store: None,
            voice_status: Some("voice transcription configured\nwhisper.cpp: ok".to_string()),
        });

        app.handle_discord_message(&binding(), message("/status"))
            .await
            .unwrap();

        let status = output.messages().await.last().unwrap().body.clone();
        assert!(status.contains("voice: voice transcription configured"));
    }

    #[tokio::test]
    async fn status_command_reports_unavailable_queue_depth_as_configured_nats() {
        let dir = TempDir::new().unwrap();
        let (base_app, _, output) = app(&dir);
        let queue = Arc::new(DefaultDepthQueue::default());
        let app = MoniApp::new(MoniAppConfig {
            queue,
            sessions: base_app.sessions,
            output: Arc::new(output.clone()),
            cron: CronEngine::default(),
            registry: BindingRegistry::new([binding()]).unwrap(),
            state_store: None,
            voice_status: None,
        });

        app.handle_discord_message(&binding(), message("/status"))
            .await
            .unwrap();

        let status = output.messages().await.last().unwrap().body.clone();
        assert!(status.contains("queue depth: unavailable"));
        assert!(status.contains("nats: configured"));
    }

    #[tokio::test]
    async fn status_command_reports_last_run_history() {
        let dir = TempDir::new().unwrap();
        let history = Arc::new(RunHistory::in_memory());
        history
            .start_run("moni", "run tests", Some("prompt".to_string()))
            .await
            .unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "moni".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Final,
                line: "done".to_string(),
                payload: Some(AgentEventPayload::TurnCompleted {
                    final_text: "done".to_string(),
                    model: Some("prompt".to_string()),
                    duration_ms: Some(12),
                    usage: None,
                    exit_status: Some("completed".to_string()),
                }),
            })
            .await
            .unwrap();
        let (app, _, output) = app_with_history(&dir, history);

        app.handle_discord_message(&binding(), message("/status"))
            .await
            .unwrap();

        let status = output.messages().await.last().unwrap().body.clone();
        assert!(status.contains("last run: completed, tools: 0"));
    }

    #[tokio::test]
    async fn status_command_reports_running_last_run_history() {
        let dir = TempDir::new().unwrap();
        let history = Arc::new(RunHistory::in_memory());
        history.start_run("moni", "run tests", None).await.unwrap();
        let (app, _, output) = app_with_history(&dir, history);

        app.handle_discord_message(&binding(), message("/status"))
            .await
            .unwrap();

        let status = output.messages().await.last().unwrap().body.clone();
        assert!(status.contains("last run: running, tools: 0"));
    }

    #[tokio::test]
    async fn voice_status_command_has_app_level_fallback() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/voice status"))
            .await
            .unwrap();

        assert_eq!(
            output.messages().await[0].body,
            "voice status is available through Discord `/voice status`"
        );
    }

    #[tokio::test]
    async fn cron_add_command_adds_task() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/cron add * * * * * run"))
            .await
            .unwrap();

        assert_eq!(app.cron_count().await, 1);
        assert!(
            output.messages().await[0]
                .body
                .starts_with("cron added cron-")
        );
    }

    #[tokio::test]
    async fn cron_list_reports_empty() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/cron list"))
            .await
            .unwrap();

        assert_eq!(output.messages().await[0].body, "no crons");
    }

    #[tokio::test]
    async fn cron_list_reports_added_task() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/cron add * * * * * run"))
            .await
            .unwrap();
        app.handle_discord_message(&binding(), message("/cron list"))
            .await
            .unwrap();

        let messages = output.messages().await;
        assert!(messages[1].body.contains("* * * * *"));
        assert!(messages[1].body.contains("active"));
    }

    #[tokio::test]
    async fn cron_pause_resume_delete_acknowledge_success_and_missing() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/cron add * * * * * run"))
            .await
            .unwrap();
        let id = app.cron.lock().await.tasks()[0].id.clone();

        app.handle_discord_message(&binding(), message(&format!("/cron pause {id}")))
            .await
            .unwrap();
        app.handle_discord_message(&binding(), message(&format!("/cron resume {id}")))
            .await
            .unwrap();
        app.handle_discord_message(&binding(), message(&format!("/cron delete {id}")))
            .await
            .unwrap();
        app.handle_discord_message(&binding(), message("/cron pause missing"))
            .await
            .unwrap();
        app.handle_discord_message(&binding(), message("/cron resume missing"))
            .await
            .unwrap();
        app.handle_discord_message(&binding(), message("/cron delete missing"))
            .await
            .unwrap();

        let bodies = output
            .messages()
            .await
            .into_iter()
            .map(|message| message.body)
            .collect::<Vec<_>>();
        assert!(bodies.iter().any(|body| body == "cron paused"));
        assert!(bodies.iter().any(|body| body == "cron resumed"));
        assert!(bodies.iter().any(|body| body == "cron deleted"));
        assert_eq!(
            bodies
                .iter()
                .filter(|body| *body == "cron not found")
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn cron_list_reports_paused_status() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_discord_message(&binding(), message("/cron add * * * * * run"))
            .await
            .unwrap();
        let id = app.cron.lock().await.tasks()[0].id.clone();
        app.handle_discord_message(&binding(), message(&format!("/cron pause {id}")))
            .await
            .unwrap();
        app.handle_discord_message(&binding(), message("/cron list"))
            .await
            .unwrap();

        assert!(
            output
                .messages()
                .await
                .last()
                .unwrap()
                .body
                .contains("paused")
        );
    }

    #[tokio::test]
    async fn cron_tick_enqueues_due_task() {
        let dir = TempDir::new().unwrap();
        let (app, queue, _) = app(&dir);
        app.handle_discord_message(&binding(), message("/cron add * * * * * run"))
            .await
            .unwrap();

        let now = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        let fired = app.tick_cron(now).await.unwrap();

        assert_eq!(fired.len(), 1);
        assert_eq!(queue.drain_namespace("moni").await.unwrap()[0].body, "run");
    }

    #[tokio::test]
    async fn cron_tick_returns_empty_when_no_task_fires() {
        let dir = TempDir::new().unwrap();
        let (app, queue, _) = app(&dir);

        let now = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        let fired = app.tick_cron(now).await.unwrap();

        assert!(fired.is_empty());
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cron_tick_persists_when_task_fires() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(FileStateStore::new(dir.path().join("state.json")));
        let (base_app, queue, output) = app(&dir);
        let app = MoniApp::new(MoniAppConfig {
            queue: Arc::new(queue),
            sessions: base_app.sessions,
            output: Arc::new(output),
            cron: CronEngine::default(),
            registry: BindingRegistry::new([binding()]).unwrap(),
            state_store: Some(store.clone()),
            voice_status: None,
        });
        app.handle_discord_message(&binding(), message("/cron add * * * * * run"))
            .await
            .unwrap();

        let now = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        app.tick_cron(now).await.unwrap();

        assert_eq!(store.load().await.unwrap().cron_tasks[0].fire_count, 1);
    }

    #[tokio::test]
    async fn cron_loop_ticks_until_due_task_fires() {
        let dir = TempDir::new().unwrap();
        let (base_app, queue, output) = app(&dir);
        let mut task = CronTask::new(
            "moni",
            "https://example.com/moni",
            "* * * * * *",
            "loop-fired",
        );
        task.created_at = Utc::now() - chrono::Duration::seconds(2);
        let app = Arc::new(MoniApp::new(MoniAppConfig {
            queue: Arc::new(queue.clone()),
            sessions: base_app.sessions,
            output: Arc::new(output),
            cron: CronEngine::new(vec![task]),
            registry: BindingRegistry::new([binding()]).unwrap(),
            state_store: None,
            voice_status: None,
        }));
        let loop_task = tokio::spawn(run_cron_loop(app, Duration::from_millis(10)));

        timeout(Duration::from_secs(2), async {
            loop {
                let queued = queue.drain_namespace("moni").await.unwrap();
                if queued.iter().any(|prompt| prompt.body == "loop-fired") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        loop_task.abort();
    }

    #[tokio::test]
    async fn cron_loop_can_shutdown_cleanly() {
        let dir = TempDir::new().unwrap();
        let (base_app, queue, output) = app(&dir);
        let app = Arc::new(MoniApp::new(MoniAppConfig {
            queue: Arc::new(queue),
            sessions: base_app.sessions,
            output: Arc::new(output),
            cron: CronEngine::default(),
            registry: BindingRegistry::new([binding()]).unwrap(),
            state_store: None,
            voice_status: None,
        }));

        run_cron_loop_until(app, Duration::from_secs(30), async {})
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cron_loop_completes_fired_tick_before_shutdown() {
        let dir = TempDir::new().unwrap();
        let (base_app, queue, output) = app(&dir);
        let mut task = CronTask::new(
            "moni",
            "https://example.com/moni",
            "* * * * * *",
            "loop-fired",
        );
        task.created_at = Utc::now() - chrono::Duration::seconds(2);
        let app = Arc::new(MoniApp::new(MoniAppConfig {
            queue: Arc::new(queue.clone()),
            sessions: base_app.sessions,
            output: Arc::new(output),
            cron: CronEngine::new(vec![task]),
            registry: BindingRegistry::new([binding()]).unwrap(),
            state_store: None,
            voice_status: None,
        }));

        run_cron_loop_until(app, Duration::from_millis(5), async {
            tokio::time::sleep(Duration::from_millis(20)).await
        })
        .await
        .unwrap();

        assert!(
            queue
                .drain_namespace("moni")
                .await
                .unwrap()
                .iter()
                .any(|prompt| prompt.body == "loop-fired")
        );
    }

    #[tokio::test]
    async fn queued_prompt_reaches_session_output() {
        let dir = TempDir::new().unwrap();
        let (app, _, output) = app(&dir);

        app.handle_queued_prompt(QueuedPrompt::new("moni", None, "hello", "test"))
            .await
            .unwrap();

        assert_eq!(wait_for_output(&output, 1).await[0].body, "agent:hello");
    }

    #[tokio::test]
    async fn live_nats_publish_reaches_session_manager_when_configured() {
        let nats_url =
            std::env::var("MONI_TEST_NATS_URL").unwrap_or("nats://127.0.0.1:4223".to_string());
        let dir = TempDir::new().unwrap();
        let namespace = format!("moni-nats-{}", uuid::Uuid::new_v4());
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let client = async_nats::connect(&nats_url).await.unwrap();
        let queue = NatsNamespaceQueue::new(client.clone());
        let resolver = Arc::new(StaticEngineConfigResolver::new(EngineConfig::new(
            AgentEngine::Claude,
            bin,
        )));
        let sessions = Arc::new(SessionManager::new(
            dir.path().join("workspaces"),
            resolver,
            Arc::new(output.clone()),
        ));
        let app = Arc::new(MoniApp::new(MoniAppConfig {
            queue: Arc::new(queue.clone()),
            sessions,
            output: Arc::new(output.clone()),
            cron: CronEngine::default(),
            registry: BindingRegistry::new([ChannelBinding {
                channel_id: "1".to_string(),
                namespace: namespace.clone(),
                repo_url: "https://example.com/moni".to_string(),
            }])
            .unwrap(),
            state_store: None,
            voice_status: None,
        }));
        let consumer = tokio::spawn(run_nats_prompt_consumer(client, app));

        tokio::time::sleep(Duration::from_millis(250)).await;
        queue
            .client()
            .publish(
                format!("moni.namespace.{namespace}.input"),
                "not-json".into(),
            )
            .await
            .unwrap();
        queue.client().flush().await.unwrap();

        let result = timeout(Duration::from_secs(10), async {
            loop {
                queue
                    .enqueue(QueuedPrompt::new(
                        namespace.clone(),
                        None,
                        "from-nats",
                        "test",
                    ))
                    .await
                    .unwrap();
                queue.client().flush().await.unwrap();
                let messages = output.messages().await;
                if !messages.is_empty() {
                    return messages;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        consumer.abort();
        let messages = result.unwrap();

        assert!(
            messages
                .iter()
                .any(|message| message.body == "agent:from-nats")
        );
    }
}
