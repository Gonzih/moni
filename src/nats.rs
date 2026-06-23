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

#[async_trait]
impl NamespaceQueue for NatsNamespaceQueue {
    async fn enqueue(&self, prompt: QueuedPrompt) -> anyhow::Result<()> {
        let subject = subject_for_namespace_input(&prompt.namespace);
        let payload = serde_json::to_vec(&prompt)?;
        self.client.publish(subject, payload.into()).await?;
        Ok(())
    }

    async fn drain_namespace(&self, _namespace: &str) -> anyhow::Result<Vec<QueuedPrompt>> {
        anyhow::bail!("NATS queue does not support test-only drain_namespace")
    }
}
