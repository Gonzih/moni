use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use serenity::{
    builder::{CreateMessage, EditMessage},
    http::{Http, Typing},
    model::{channel::Message, id::ChannelId},
};
use tokio::{
    sync::Mutex,
    time::{Instant, sleep},
};

use crate::harness::{AgentEvent, EventStreamKind};
use crate::registry::BindingRegistry;

const DISCORD_MESSAGE_LIMIT: usize = 1900;
const DISCORD_SEND_ATTEMPTS: usize = 3;
const LIVE_EDIT_MIN_INTERVAL: Duration = Duration::from_millis(900);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputMessage {
    pub namespace: String,
    pub body: String,
    pub kind: OutputMessageKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMessageKind {
    Complete,
    Delta,
    Final,
}

impl OutputMessage {
    pub fn complete(namespace: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            body: body.into(),
            kind: OutputMessageKind::Complete,
        }
    }

    pub fn delta(namespace: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            body: body.into(),
            kind: OutputMessageKind::Delta,
        }
    }

    pub fn final_message(namespace: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            body: body.into(),
            kind: OutputMessageKind::Final,
        }
    }
}

#[async_trait]
pub trait OutputSink: Send + Sync {
    async fn send(&self, message: OutputMessage) -> anyhow::Result<()>;
}

#[derive(Clone, Default)]
pub struct DiscordTypingTracker {
    active: Arc<Mutex<HashMap<String, Typing>>>,
}

impl DiscordTypingTracker {
    pub async fn start(
        &self,
        namespace: impl Into<String>,
        channel_id: ChannelId,
        http: &Arc<Http>,
    ) {
        let namespace = namespace.into();
        let typing = channel_id.start_typing(http);
        if let Some(previous) = self.active.lock().await.insert(namespace.clone(), typing) {
            previous.stop();
        }

        let tracker = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15 * 60)).await;
            tracker.stop(&namespace).await;
        });
    }

    pub async fn stop(&self, namespace: &str) {
        if let Some(typing) = self.active.lock().await.remove(namespace) {
            typing.stop();
        }
    }
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
    typing: Option<DiscordTypingTracker>,
    live: Arc<Mutex<DiscordLiveMessages>>,
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
            typing: None,
            live: Arc::new(Mutex::new(DiscordLiveMessages::default())),
        }
    }

    pub fn with_typing_tracker(mut self, typing: DiscordTypingTracker) -> Self {
        self.typing = Some(typing);
        self
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

        match message.kind {
            OutputMessageKind::Delta => {
                let mut live = self.live.lock().await;
                live.apply_delta(
                    &self.http,
                    channel_id,
                    &message.namespace,
                    &message.body,
                    self.typing.as_ref(),
                )
                .await?;
                return Ok(());
            }
            OutputMessageKind::Final => {
                if let Some(typing) = &self.typing {
                    typing.stop(&message.namespace).await;
                }
                let mut live = self.live.lock().await;
                if live
                    .finalize(&self.http, channel_id, &message.namespace, &message.body)
                    .await?
                {
                    return Ok(());
                }
            }
            OutputMessageKind::Complete => {
                if let Some(typing) = &self.typing {
                    typing.stop(&message.namespace).await;
                }
            }
        }

        for chunk in split_discord_message(&message.body) {
            send_discord_chunk(&self.http, channel_id, &chunk).await?;
        }

        Ok(())
    }
}

#[derive(Default)]
struct DiscordLiveMessages {
    messages: HashMap<String, DiscordLiveMessage>,
    next_edit_at: Option<Instant>,
}

struct DiscordLiveMessage {
    message: Message,
    text: String,
}

impl DiscordLiveMessages {
    async fn apply_delta(
        &mut self,
        http: &Arc<Http>,
        channel_id: ChannelId,
        namespace: &str,
        delta: &str,
        typing: Option<&DiscordTypingTracker>,
    ) -> anyhow::Result<()> {
        if delta.is_empty() {
            return Ok(());
        }
        if !self.messages.contains_key(namespace) {
            if let Some(typing) = typing {
                typing.stop(namespace).await;
            }
            let message = channel_id
                .send_message(http, CreateMessage::new().content("..."))
                .await?;
            self.messages.insert(
                namespace.to_string(),
                DiscordLiveMessage {
                    message,
                    text: String::new(),
                },
            );
            self.next_edit_at = Some(Instant::now() + LIVE_EDIT_MIN_INTERVAL);
        }

        let ready_to_edit = self.ready_to_edit();
        let entry = self
            .messages
            .get_mut(namespace)
            .expect("live message exists");
        entry.text.push_str(delta);
        if !ready_to_edit {
            return Ok(());
        }
        let content = live_display(namespace, &entry.text);
        edit_discord_message(http, &mut entry.message, &content).await?;
        self.next_edit_at = Some(Instant::now() + LIVE_EDIT_MIN_INTERVAL);
        Ok(())
    }

    async fn finalize(
        &mut self,
        http: &Arc<Http>,
        channel_id: ChannelId,
        namespace: &str,
        body: &str,
    ) -> anyhow::Result<bool> {
        let Some(mut entry) = self.messages.remove(namespace) else {
            return Ok(false);
        };
        let chunks = split_discord_message(body);
        edit_discord_message(http, &mut entry.message, &chunks[0]).await?;
        for chunk in chunks.iter().skip(1) {
            send_discord_chunk(http, channel_id, chunk).await?;
        }
        Ok(true)
    }

    fn ready_to_edit(&self) -> bool {
        self.next_edit_at
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(true)
    }
}

async fn edit_discord_message(
    http: &Arc<Http>,
    message: &mut Message,
    body: &str,
) -> anyhow::Result<()> {
    let mut delay = Duration::from_millis(500);
    let mut last_error = None;
    for attempt in 1..=DISCORD_SEND_ATTEMPTS {
        match message
            .edit(http, EditMessage::new().content(first_discord_chunk(body)))
            .await
        {
            Ok(()) => return Ok(()),
            Err(err) => {
                tracing::warn!(
                    attempt,
                    max_attempts = DISCORD_SEND_ATTEMPTS,
                    error = %err,
                    "failed to edit Discord live output"
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
        "failed to edit Discord output after {DISCORD_SEND_ATTEMPTS} attempts: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    ))
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

fn first_discord_chunk(body: &str) -> String {
    split_discord_message(body)
        .into_iter()
        .next()
        .unwrap_or_else(|| " ".to_string())
}

fn live_display(namespace: &str, body: &str) -> String {
    first_discord_chunk(&format!("<- [{namespace}]\n{} |", body.trim_start()))
}

pub fn event_to_output_message(event: AgentEvent) -> Option<OutputMessage> {
    match event.stream {
        EventStreamKind::Stdout => Some(OutputMessage::complete(event.namespace, event.line)),
        EventStreamKind::Delta => Some(OutputMessage::delta(event.namespace, event.line)),
        EventStreamKind::Final => Some(OutputMessage::final_message(event.namespace, event.line)),
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
        sink.send(OutputMessage::complete("moni", "hello"))
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
        assert_eq!(output.kind, OutputMessageKind::Complete);
    }

    #[test]
    fn delta_event_becomes_delta_output_message() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Delta,
            line: "hel".to_string(),
        })
        .unwrap();

        assert_eq!(output.kind, OutputMessageKind::Delta);
        assert_eq!(output.body, "hel");
    }

    #[test]
    fn final_event_becomes_final_output_message() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Final,
            line: "hello".to_string(),
        })
        .unwrap();

        assert_eq!(output.kind, OutputMessageKind::Final);
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
