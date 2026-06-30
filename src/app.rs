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
}

pub struct MoniApp {
    queue: Arc<dyn NamespaceQueue>,
    sessions: Arc<SessionManager>,
    output: Arc<dyn OutputSink>,
    cron: Mutex<CronEngine>,
    registry: BindingRegistry,
    state_store: Option<Arc<dyn StateStore>>,
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
        self.ack(&binding.namespace, "registered").await?;
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
                Ok(command_outcome(binding.namespace, "registered"))
            }
            CommandAction::Reset => {
                self.sessions
                    .stop_namespace(&binding.namespace, StopReason::Reset)
                    .await?;
                Ok(command_outcome(&binding.namespace, "reset complete"))
            }
            CommandAction::Clear => {
                self.sessions
                    .stop_namespace(&binding.namespace, StopReason::Clear)
                    .await?;
                Ok(command_outcome(&binding.namespace, "clear complete"))
            }
            CommandAction::Compact => {
                self.sessions.compact(&binding.namespace).await?;
                Ok(command_outcome(&binding.namespace, "compact queued"))
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

    async fn ack(&self, namespace: &str, body: impl Into<String>) -> anyhow::Result<()> {
        self.output
            .send(OutputMessage::complete(namespace, body))
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

fn command_outcome(namespace: impl Into<String>, body: impl Into<String>) -> CommandOutcome {
    CommandOutcome {
        namespace: namespace.into(),
        body: body.into(),
    }
}

fn bool_body(changed: bool, ok: &str, missing: &str) -> String {
    if changed { ok } else { missing }.to_string()
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
        FileStateStore,
        engine::{AgentEngine, EngineConfig},
        nats::{NatsNamespaceQueue, run_nats_prompt_consumer},
        output::InMemoryOutputSink,
        queue::{InMemoryNamespaceQueue, NamespaceQueue, QueuedPrompt},
        session::StaticEngineConfigResolver,
    };

    use super::*;

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
