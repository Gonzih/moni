use std::sync::Arc;

use async_trait::async_trait;
use serenity::{http::Http, model::id::ChannelId};
use tokio::{
    sync::Mutex,
    time::{Duration, sleep},
};

use crate::harness::{AgentEvent, EventStreamKind};
use crate::registry::BindingRegistry;

const DISCORD_MESSAGE_LIMIT: usize = 1900;
const DISCORD_SEND_ATTEMPTS: usize = 3;

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
            tracing::warn!(namespace = %message.namespace, "no Discord channel binding for output");
            return Ok(());
        };

        for chunk in split_discord_message(&message.body) {
            send_discord_chunk(&self.http, channel_id, &chunk).await?;
        }

        Ok(())
    }
}

async fn send_discord_chunk(
    http: &Arc<Http>,
    channel_id: ChannelId,
    chunk: &str,
) -> anyhow::Result<()> {
    let mut delay = Duration::from_millis(500);
    let mut last_error = None;

    for attempt in 1..=DISCORD_SEND_ATTEMPTS {
        match channel_id.say(http, chunk).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                tracing::warn!(
                    attempt,
                    max_attempts = DISCORD_SEND_ATTEMPTS,
                    error = %err,
                    "failed to send Discord output chunk"
                );
                last_error = Some(err);
                if attempt < DISCORD_SEND_ATTEMPTS {
                    sleep(delay).await;
                    delay *= 2;
                }
            }
        }
    }

    Err(anyhow::anyhow!(
        "failed to send Discord output after {DISCORD_SEND_ATTEMPTS} attempts: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    ))
}

pub fn split_discord_message(body: &str) -> Vec<String> {
    if body.is_empty() {
        return vec![" ".to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();

    for line in body.split_inclusive('\n') {
        if current.len() + line.len() <= DISCORD_MESSAGE_LIMIT {
            current.push_str(line);
            continue;
        }

        if !current.is_empty() {
            chunks.push(current);
            current = String::new();
        }

        if line.len() <= DISCORD_MESSAGE_LIMIT {
            current.push_str(line);
            continue;
        }

        for ch in line.chars() {
            if current.len() + ch.len_utf8() > DISCORD_MESSAGE_LIMIT {
                chunks.push(current);
                current = String::new();
            }
            current.push(ch);
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
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

    #[test]
    fn split_discord_message_keeps_chunks_under_limit() {
        let body = "a".repeat(DISCORD_MESSAGE_LIMIT * 2 + 17);
        let chunks = split_discord_message(&body);

        assert_eq!(chunks.len(), 3);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.len() <= DISCORD_MESSAGE_LIMIT)
        );
        assert_eq!(chunks.join(""), body);
    }

    #[test]
    fn split_discord_message_prefers_line_boundaries() {
        let body = format!("{}\n{}", "a".repeat(100), "b".repeat(100));
        let chunks = split_discord_message(&body);

        assert_eq!(chunks, vec![body]);
    }

    #[test]
    fn split_discord_message_maps_empty_body_to_sendable_content() {
        assert_eq!(split_discord_message(""), vec![" ".to_string()]);
    }
}
