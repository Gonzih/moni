use std::{sync::Arc, time::Duration};

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

        if !fired.is_empty() {
            self.persist_state().await?;
        }

        Ok(fired)
    }

    pub async fn cron_count(&self) -> usize {
        self.cron.lock().await.tasks().len()
    }

    async fn handle_command(
        &self,
        binding: &ChannelBinding,
        action: CommandAction,
    ) -> anyhow::Result<()> {
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
                self.ack(&binding.namespace, "registered").await
            }
            CommandAction::Reset => {
                self.sessions
                    .stop_namespace(&binding.namespace, StopReason::Reset)
                    .await?;
                self.ack(&binding.namespace, "reset complete").await
            }
            CommandAction::Clear => {
                self.sessions
                    .stop_namespace(&binding.namespace, StopReason::Clear)
                    .await?;
                self.ack(&binding.namespace, "clear complete").await
            }
            CommandAction::Compact => {
                self.sessions.compact(&binding.namespace).await?;
                self.ack(&binding.namespace, "compact queued").await
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
                self.ack(&binding.namespace, format!("cron added {id}"))
                    .await
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
                self.ack(&binding.namespace, body).await
            }
            CommandAction::CronPause { id } => {
                let changed = self.cron.lock().await.pause(&id);
                self.persist_state().await?;
                self.ack_bool(&binding.namespace, changed, "cron paused", "cron not found")
                    .await
            }
            CommandAction::CronResume { id } => {
                let changed = self.cron.lock().await.resume(&id);
                self.persist_state().await?;
                self.ack_bool(
                    &binding.namespace,
                    changed,
                    "cron resumed",
                    "cron not found",
                )
                .await
            }
            CommandAction::CronDelete { id } => {
                let changed = self.cron.lock().await.delete(&id);
                self.persist_state().await?;
                self.ack_bool(
                    &binding.namespace,
                    changed,
                    "cron deleted",
                    "cron not found",
                )
                .await
            }
        }
    }

    async fn ack(&self, namespace: &str, body: impl Into<String>) -> anyhow::Result<()> {
        self.output
            .send(OutputMessage::complete(namespace, body))
            .await
    }

    async fn ack_bool(
        &self,
        namespace: &str,
        changed: bool,
        ok: &str,
        missing: &str,
    ) -> anyhow::Result<()> {
        self.ack(namespace, if changed { ok } else { missing })
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

pub async fn run_cron_loop(app: Arc<MoniApp>, tick_every: Duration) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(tick_every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let fired = app.tick_cron(Utc::now()).await?;
        if !fired.is_empty() {
            tracing::info!(count = fired.len(), tasks = ?fired, "cron tasks fired");
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
        let Ok(nats_url) = std::env::var("MONI_TEST_NATS_URL") else {
            return;
        };
        let dir = TempDir::new().unwrap();
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
            registry: BindingRegistry::new([binding()]).unwrap(),
            state_store: None,
        }));
        let consumer = tokio::spawn(run_nats_prompt_consumer(client, app));

        tokio::time::sleep(Duration::from_millis(100)).await;
        queue
            .enqueue(QueuedPrompt::new("moni", None, "from-nats", "test"))
            .await
            .unwrap();

        let messages = wait_for_output(&output, 1).await;
        consumer.abort();

        assert_eq!(messages[0].body, "agent:from-nats");
    }
}
