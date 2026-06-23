use std::sync::Arc;

use async_trait::async_trait;
use serenity::{http::Http, model::id::ChannelId};
use tokio::sync::Mutex;

use crate::harness::{AgentEvent, EventStreamKind};
use crate::registry::BindingRegistry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputMessage {
    pub namespace: String,
    pub body: String,
}

#[async_trait]
pub trait OutputSink: Send + Sync {
    async fn send(&self, message: OutputMessage) -> anyhow::Result<()>;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryOutputSink {
    messages: Arc<Mutex<Vec<OutputMessage>>>,
}

impl InMemoryOutputSink {
    pub async fn messages(&self) -> Vec<OutputMessage> {
        self.messages.lock().await.clone()
    }
}

#[async_trait]
impl OutputSink for InMemoryOutputSink {
    async fn send(&self, message: OutputMessage) -> anyhow::Result<()> {
        self.messages.lock().await.push(message);
        Ok(())
    }
}

#[derive(Clone)]
pub struct DiscordOutputSink {
    http: Arc<Http>,
    registry: BindingRegistry,
}

impl DiscordOutputSink {
    pub fn new(
        token: impl AsRef<str>,
        bindings: impl IntoIterator<Item = (String, ChannelId)>,
    ) -> Self {
        let channel_bindings =
            bindings
                .into_iter()
                .map(|(namespace, channel_id)| crate::discord::ChannelBinding {
                    channel_id: channel_id.to_string(),
                    namespace,
                    repo_url: String::new(),
                });
        Self::with_registry(
            token,
            BindingRegistry::new(channel_bindings).expect("valid channel ids"),
        )
    }

    pub fn with_registry(token: impl AsRef<str>, registry: BindingRegistry) -> Self {
        Self {
            http: Arc::new(Http::new(token.as_ref())),
            registry,
        }
    }
}

#[async_trait]
impl OutputSink for DiscordOutputSink {
    async fn send(&self, message: OutputMessage) -> anyhow::Result<()> {
        let Some(channel_id) = self
            .registry
            .channel_for_namespace(&message.namespace)
            .await
        else {
            return Ok(());
        };
        channel_id.say(&self.http, message.body).await?;
        Ok(())
    }
}

pub fn event_to_output_message(event: AgentEvent) -> Option<OutputMessage> {
    match event.stream {
        EventStreamKind::Stdout => Some(OutputMessage {
            namespace: event.namespace,
            body: event.line,
        }),
        EventStreamKind::Stderr | EventStreamKind::Status => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::AgentEngine;

    use super::*;

    #[tokio::test]
    async fn memory_output_records_messages() {
        let sink = InMemoryOutputSink::default();
        sink.send(OutputMessage {
            namespace: "moni".to_string(),
            body: "hello".to_string(),
        })
        .await
        .unwrap();

        assert_eq!(sink.messages().await.len(), 1);
        assert_eq!(sink.messages().await[0].body, "hello");
    }

    #[test]
    fn stdout_event_becomes_output_message() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Claude,
            stream: EventStreamKind::Stdout,
            line: "hello".to_string(),
        })
        .unwrap();

        assert_eq!(output.namespace, "moni");
        assert_eq!(output.body, "hello");
    }

    #[test]
    fn stderr_event_is_not_sent_to_discord() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Claude,
            stream: EventStreamKind::Stderr,
            line: "err".to_string(),
        });

        assert!(output.is_none());
    }

    #[test]
    fn status_event_is_not_sent_to_discord() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Claude,
            stream: EventStreamKind::Status,
            line: "started".to_string(),
        });

        assert!(output.is_none());
    }
}
