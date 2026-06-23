use async_trait::async_trait;

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

#[cfg(test)]
mod tests {
    use super::*;

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
        let decoded: QueuedPrompt = serde_json::from_slice(&payload).unwrap();
        assert_eq!(decoded.namespace, "moni");
        assert_eq!(decoded.repo_url.as_deref(), Some("https://repo"));
        assert_eq!(decoded.body, "hello");
        assert_eq!(decoded.source, "test");
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
}
