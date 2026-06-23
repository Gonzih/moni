use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedPrompt {
    pub id: Uuid,
    pub namespace: String,
    pub repo_url: Option<String>,
    pub body: String,
    pub source: String,
    pub created_at: DateTime<Utc>,
}

impl QueuedPrompt {
    pub fn new(
        namespace: impl Into<String>,
        repo_url: Option<String>,
        body: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            namespace: namespace.into(),
            repo_url,
            body: body.into(),
            source: source.into(),
            created_at: Utc::now(),
        }
    }
}

#[async_trait]
pub trait NamespaceQueue: Send + Sync {
    async fn enqueue(&self, prompt: QueuedPrompt) -> anyhow::Result<()>;
    async fn drain_namespace(&self, namespace: &str) -> anyhow::Result<Vec<QueuedPrompt>>;
}

pub fn subject_for_namespace_input(namespace: &str) -> String {
    format!("moni.namespace.{namespace}.input")
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryNamespaceQueue {
    inner: Arc<Mutex<HashMap<String, Vec<QueuedPrompt>>>>,
}

#[async_trait]
impl NamespaceQueue for InMemoryNamespaceQueue {
    async fn enqueue(&self, prompt: QueuedPrompt) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        inner
            .entry(prompt.namespace.clone())
            .or_default()
            .push(prompt);
        Ok(())
    }

    async fn drain_namespace(&self, namespace: &str) -> anyhow::Result<Vec<QueuedPrompt>> {
        let mut inner = self.inner.lock().await;
        Ok(inner.remove(namespace).unwrap_or_default())
    }
}
