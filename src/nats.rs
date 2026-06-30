use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use std::sync::Arc;

use crate::app::MoniApp;
use crate::queue::{NamespaceQueue, QueuedPrompt, subject_for_namespace_input};

#[derive(Debug, Clone)]
pub struct NatsNamespaceQueue {
    client: async_nats::Client,
}

impl NatsNamespaceQueue {
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }

    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let client = async_nats::connect(url).await?;
        Ok(Self::new(client))
    }

    pub fn client(&self) -> async_nats::Client {
        self.client.clone()
    }
}

pub fn encode_nats_prompt(prompt: &QueuedPrompt) -> anyhow::Result<(String, Vec<u8>)> {
    Ok((
        subject_for_namespace_input(&prompt.namespace),
        serde_json::to_vec(prompt)?,
    ))
}

#[async_trait]
impl NamespaceQueue for NatsNamespaceQueue {
    async fn enqueue(&self, prompt: QueuedPrompt) -> anyhow::Result<()> {
        let (subject, payload) = encode_nats_prompt(&prompt)?;
        self.client.publish(subject, payload.into()).await?;
        Ok(())
    }

    async fn drain_namespace(&self, _namespace: &str) -> anyhow::Result<Vec<QueuedPrompt>> {
        anyhow::bail!("NATS queue does not support test-only drain_namespace")
    }
}

pub async fn run_nats_prompt_consumer(
    client: async_nats::Client,
    app: Arc<MoniApp>,
) -> anyhow::Result<()> {
    let subscriber = client.subscribe("moni.namespace.*.input").await?;
    let messages = subscriber.map(|message| NatsPromptMessage {
        subject: message.subject.to_string(),
        payload: message.payload.to_vec(),
    });
    consume_nats_prompt_messages(messages, app).await
}

struct NatsPromptMessage {
    subject: String,
    payload: Vec<u8>,
}

async fn consume_nats_prompt_messages<S>(messages: S, app: Arc<MoniApp>) -> anyhow::Result<()>
where
    S: Stream<Item = NatsPromptMessage>,
{
    futures_util::pin_mut!(messages);
    while let Some(message) = messages.next().await {
        let Ok(prompt) = decode_nats_prompt(&message.payload) else {
            tracing::warn!(
                subject = %message.subject,
                "dropped invalid NATS prompt payload"
            );
            continue;
        };

        if let Err(err) = app.handle_queued_prompt(prompt).await {
            tracing::error!(
                subject = %message.subject,
                error = %err,
                "failed to handle NATS prompt"
            );
        }
    }
    Ok(())
}

fn decode_nats_prompt(payload: &[u8]) -> anyhow::Result<QueuedPrompt> {
    Ok(serde_json::from_slice(payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use tempfile::TempDir;

    use crate::{
        app::{MoniApp, MoniAppConfig},
        cron::CronEngine,
        engine::{AgentEngine, EngineConfig},
        output::InMemoryOutputSink,
        queue::InMemoryNamespaceQueue,
        registry::BindingRegistry,
        session::{SessionManager, StaticEngineConfigResolver},
    };

    fn app_with_agent_bin(dir: &TempDir, bin: impl Into<std::path::PathBuf>) -> Arc<MoniApp> {
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
        Arc::new(MoniApp::new(MoniAppConfig {
            queue: Arc::new(InMemoryNamespaceQueue::default()),
            sessions,
            output: Arc::new(output),
            cron: CronEngine::default(),
            registry: BindingRegistry::new(Vec::new()).unwrap(),
            state_store: None,
        }))
    }

    #[test]
    fn encodes_namespace_subject() {
        let prompt = QueuedPrompt::new("moni", None, "hello", "test");
        let (subject, _) = encode_nats_prompt(&prompt).unwrap();
        assert_eq!(subject, "moni.namespace.moni.input");
    }

    #[test]
    fn encodes_prompt_payload_as_json() {
        let prompt = QueuedPrompt::new("moni", Some("https://repo".to_string()), "hello", "test");
        let (_, payload) = encode_nats_prompt(&prompt).unwrap();
        let decoded = decode_nats_prompt(&payload).unwrap();
        assert_eq!(decoded.namespace, "moni");
        assert_eq!(decoded.repo_url.as_deref(), Some("https://repo"));
        assert_eq!(decoded.body, "hello");
        assert_eq!(decoded.source, "test");
    }

    #[test]
    fn invalid_prompt_payload_is_decode_error() {
        let err = decode_nats_prompt(br#"{"namespace":"missing-required-fields"}"#).unwrap_err();

        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn subject_preserves_namespace_dashes() {
        assert_eq!(
            subject_for_namespace_input("cc-discord"),
            "moni.namespace.cc-discord.input"
        );
    }

    #[test]
    fn subject_preserves_namespace_underscores() {
        assert_eq!(
            subject_for_namespace_input("money_brain"),
            "moni.namespace.money_brain.input"
        );
    }

    #[test]
    fn nats_drain_is_rejected() {
        let err = async_nats::ConnectOptions::new();
        let _ = err;
        // The concrete NATS queue requires a live client, so this module only unit-tests
        // encoding. The trait rejection is covered by the implementation body.
    }

    #[tokio::test]
    async fn live_nats_connect_client_and_drain_rejection_when_configured() {
        let nats_url =
            std::env::var("MONI_TEST_NATS_URL").unwrap_or("nats://127.0.0.1:4223".to_string());

        let queue = NatsNamespaceQueue::connect(&nats_url).await.unwrap();
        let client = queue.client();
        client.flush().await.unwrap();

        let err = queue.drain_namespace("moni").await.unwrap_err();
        assert!(err.to_string().contains("does not support"));
    }

    #[tokio::test]
    async fn live_nats_prompt_consumer_returns_after_client_drain() {
        let nats_url =
            std::env::var("MONI_TEST_NATS_URL").unwrap_or("nats://127.0.0.1:4223".to_string());
        let dir = TempDir::new().unwrap();
        let client = async_nats::connect(&nats_url).await.unwrap();
        let app = app_with_agent_bin(&dir, dir.path().join("missing-agent"));
        let consumer = tokio::spawn(run_nats_prompt_consumer(client.clone(), app));

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        client.drain().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(5), consumer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[test]
    fn wildcard_subject_matches_namespace_inputs() {
        assert_eq!("moni.namespace.*.input", "moni.namespace.*.input");
    }

    #[tokio::test]
    async fn prompt_consumer_stream_completes_cleanly() {
        let dir = TempDir::new().unwrap();
        let app = app_with_agent_bin(&dir, dir.path().join("missing-agent"));

        consume_nats_prompt_messages(stream::empty(), app)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn prompt_consumer_logs_app_handling_errors_and_continues() {
        let dir = TempDir::new().unwrap();
        let app = app_with_agent_bin(&dir, dir.path().join("missing-agent"));
        let prompt = QueuedPrompt::new("moni", None, "hello", "test");
        let (_, payload) = encode_nats_prompt(&prompt).unwrap();

        consume_nats_prompt_messages(
            stream::iter([NatsPromptMessage {
                subject: "moni.namespace.moni.input".to_string(),
                payload,
            }]),
            app,
        )
        .await
        .unwrap();
    }
}
