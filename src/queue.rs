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
    async fn depth(&self, _namespace: &str) -> anyhow::Result<Option<usize>> {
        Ok(None)
    }
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

    async fn depth(&self, namespace: &str) -> anyhow::Result<Option<usize>> {
        let inner = self.inner.lock().await;
        Ok(Some(inner.get(namespace).map(Vec::len).unwrap_or_default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_prompt_sets_fields() {
        let prompt = QueuedPrompt::new(
            "moni",
            Some("https://example.com/moni".to_string()),
            "build",
            "discord:1",
        );

        assert_eq!(prompt.namespace, "moni");
        assert_eq!(prompt.repo_url.as_deref(), Some("https://example.com/moni"));
        assert_eq!(prompt.body, "build");
        assert_eq!(prompt.source, "discord:1");
    }

    #[test]
    fn queued_prompt_ids_are_unique() {
        let a = QueuedPrompt::new("moni", None, "a", "test");
        let b = QueuedPrompt::new("moni", None, "b", "test");
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn subject_for_namespace_input_uses_moni_prefix() {
        assert_eq!(
            subject_for_namespace_input("ops"),
            "moni.namespace.ops.input"
        );
    }

    #[tokio::test]
    async fn in_memory_queue_preserves_order() {
        let queue = InMemoryNamespaceQueue::default();
        queue
            .enqueue(QueuedPrompt::new("moni", None, "first", "test"))
            .await
            .unwrap();
        queue
            .enqueue(QueuedPrompt::new("moni", None, "second", "test"))
            .await
            .unwrap();

        let drained = queue.drain_namespace("moni").await.unwrap();
        assert_eq!(drained[0].body, "first");
        assert_eq!(drained[1].body, "second");
    }

    #[tokio::test]
    async fn in_memory_queue_reports_depth() {
        let queue = InMemoryNamespaceQueue::default();
        assert_eq!(queue.depth("moni").await.unwrap(), Some(0));
        queue
            .enqueue(QueuedPrompt::new("moni", None, "first", "test"))
            .await
            .unwrap();
        queue
            .enqueue(QueuedPrompt::new("moni", None, "second", "test"))
            .await
            .unwrap();

        assert_eq!(queue.depth("moni").await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn in_memory_queue_isolates_namespaces() {
        let queue = InMemoryNamespaceQueue::default();
        queue
            .enqueue(QueuedPrompt::new("moni", None, "a", "test"))
            .await
            .unwrap();
        queue
            .enqueue(QueuedPrompt::new("ops", None, "b", "test"))
            .await
            .unwrap();

        assert_eq!(queue.drain_namespace("moni").await.unwrap()[0].body, "a");
        assert_eq!(queue.drain_namespace("ops").await.unwrap()[0].body, "b");
    }

    #[tokio::test]
    async fn draining_missing_namespace_is_empty() {
        let queue = InMemoryNamespaceQueue::default();
        assert!(queue.drain_namespace("missing").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn drain_removes_items() {
        let queue = InMemoryNamespaceQueue::default();
        queue
            .enqueue(QueuedPrompt::new("moni", None, "a", "test"))
            .await
            .unwrap();

        assert_eq!(queue.drain_namespace("moni").await.unwrap().len(), 1);
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cloned_in_memory_queue_shares_state() {
        let queue = InMemoryNamespaceQueue::default();
        let cloned = queue.clone();
        queue
            .enqueue(QueuedPrompt::new("moni", None, "shared", "test"))
            .await
            .unwrap();

        assert_eq!(
            cloned.drain_namespace("moni").await.unwrap()[0].body,
            "shared"
        );
    }

    #[test]
    fn queued_prompt_round_trips_json() {
        let prompt = QueuedPrompt::new("moni", Some("repo".to_string()), "body", "source");
        let encoded = serde_json::to_string(&prompt).unwrap();
        let decoded: QueuedPrompt = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, prompt);
    }

    #[test]
    fn queued_prompt_allows_no_repo_url() {
        let prompt = QueuedPrompt::new("moni", None, "body", "source");
        assert!(prompt.repo_url.is_none());
    }
}
