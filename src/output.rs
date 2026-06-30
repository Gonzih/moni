use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use serenity::{
    Error as SerenityError,
    builder::{CreateMessage, EditMessage},
    http::{Http, HttpError, StatusCode, Typing},
    model::id::{ChannelId, MessageId},
};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep},
};

use crate::harness::{AgentEvent, AgentEventPayload, EventStreamKind, TokenUsage};
use crate::registry::BindingRegistry;

const DISCORD_MESSAGE_LIMIT: usize = 1900;
const DISCORD_SEND_ATTEMPTS: usize = 3;
const DEFAULT_LIVE_EDIT_MIN_INTERVAL: Duration = Duration::from_millis(900);
const DEFAULT_LIVE_EDIT_INITIAL_BACKOFF: Duration = Duration::from_millis(1500);
const DEFAULT_LIVE_EDIT_MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiscordLiveEditConfig {
    min_interval: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl Default for DiscordLiveEditConfig {
    fn default() -> Self {
        Self {
            min_interval: DEFAULT_LIVE_EDIT_MIN_INTERVAL,
            initial_backoff: DEFAULT_LIVE_EDIT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_LIVE_EDIT_MAX_BACKOFF,
        }
    }
}

impl DiscordLiveEditConfig {
    pub fn new(
        min_interval: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> anyhow::Result<Self> {
        if min_interval.is_zero() {
            anyhow::bail!("Discord live edit interval must be greater than zero");
        }
        if initial_backoff.is_zero() {
            anyhow::bail!("Discord live edit initial backoff must be greater than zero");
        }
        if max_backoff < initial_backoff {
            anyhow::bail!(
                "Discord live edit max backoff must be greater than or equal to initial backoff"
            );
        }
        Ok(Self {
            min_interval,
            initial_backoff,
            max_backoff,
        })
    }

    pub fn min_interval(&self) -> Duration {
        self.min_interval
    }

    pub fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }
}

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
    Tool,
}

impl OutputMessage {
    pub fn complete(namespace: &str, body: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            body: body.to_string(),
            kind: OutputMessageKind::Complete,
        }
    }

    pub fn delta(namespace: &str, body: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            body: body.to_string(),
            kind: OutputMessageKind::Delta,
        }
    }

    pub fn final_message(namespace: &str, body: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            body: body.to_string(),
            kind: OutputMessageKind::Final,
        }
    }

    pub fn tool(namespace: &str, body: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            body: body.to_string(),
            kind: OutputMessageKind::Tool,
        }
    }
}

#[async_trait]
pub trait OutputSink: Send + Sync {
    async fn send(&self, message: OutputMessage) -> anyhow::Result<()>;

    async fn live_status(&self, namespace: &str) -> String;
}

#[derive(Clone, Default)]
pub struct DiscordTypingTracker {
    active: Arc<Mutex<HashMap<String, Typing>>>,
}

impl DiscordTypingTracker {
    pub async fn start(&self, namespace: &str, channel_id: ChannelId, http: &Arc<Http>) {
        let namespace = namespace.to_string();
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

    async fn live_status(&self, _namespace: &str) -> String {
        "unavailable".to_string()
    }
}

#[derive(Clone)]
pub struct DiscordOutputSink {
    transport: Arc<dyn DiscordTransport>,
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
        Self::with_transport(registry, Arc::new(SerenityDiscordTransport::new(token)))
    }

    fn with_transport(registry: BindingRegistry, transport: Arc<dyn DiscordTransport>) -> Self {
        Self {
            transport,
            registry,
            typing: None,
            live: Arc::new(Mutex::new(DiscordLiveMessages::default())),
        }
    }

    pub fn with_typing_tracker(mut self, typing: DiscordTypingTracker) -> Self {
        self.typing = Some(typing);
        self
    }

    pub fn with_live_edit_config(mut self, config: DiscordLiveEditConfig) -> Self {
        self.live = Arc::new(Mutex::new(DiscordLiveMessages::new(config)));
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
                let should_schedule = {
                    let mut live = self.live.lock().await;
                    live.apply_delta(
                        self.transport.as_ref(),
                        channel_id,
                        &message.namespace,
                        &message.body,
                        LiveMessageSlot::Response,
                        self.typing.as_ref(),
                    )
                    .await?
                };
                if should_schedule {
                    self.schedule_live_edit_drain();
                }
                return Ok(());
            }
            OutputMessageKind::Final => {
                if let Some(typing) = &self.typing {
                    typing.stop(&message.namespace).await;
                }
                let mut live = self.live.lock().await;
                let tool_message = live.finalize(&live_message_key(
                    &message.namespace,
                    LiveMessageSlot::Tools,
                ));
                let live_message = live.finalize(&live_message_key(
                    &message.namespace,
                    LiveMessageSlot::Response,
                ));
                drop(live);
                if let Some(tool_message) = tool_message {
                    let body = live_display(
                        LiveMessageSlot::Tools,
                        &message.namespace,
                        &tool_message.text,
                        false,
                    );
                    finalize_live_message(self.transport.as_ref(), channel_id, tool_message, &body)
                        .await?;
                }
                if let Some(live_message) = live_message {
                    finalize_live_message(
                        self.transport.as_ref(),
                        channel_id,
                        live_message,
                        &message.body,
                    )
                    .await?;
                    return Ok(());
                }
            }
            OutputMessageKind::Tool => {
                let should_schedule = {
                    let mut live = self.live.lock().await;
                    live.apply_delta(
                        self.transport.as_ref(),
                        channel_id,
                        &message.namespace,
                        &message.body,
                        LiveMessageSlot::Tools,
                        None,
                    )
                    .await?
                };
                if should_schedule {
                    self.schedule_live_edit_drain();
                }
                return Ok(());
            }
            OutputMessageKind::Complete => {
                if let Some(typing) = &self.typing {
                    typing.stop(&message.namespace).await;
                }
            }
        }

        for chunk in split_discord_message(&message.body) {
            send_discord_chunk(self.transport.as_ref(), channel_id, &chunk).await?;
        }

        Ok(())
    }

    async fn live_status(&self, namespace: &str) -> String {
        self.live.lock().await.status_line(namespace)
    }
}

impl DiscordOutputSink {
    fn schedule_live_edit_drain(&self) {
        let live = self.live.clone();
        let transport = self.transport.clone();
        tokio::spawn(async move {
            drain_live_edits(live, transport).await;
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiscordMessageRef {
    channel_id: ChannelId,
    message_id: MessageId,
}

#[async_trait]
trait DiscordTransport: Send + Sync {
    async fn send_message(
        &self,
        channel_id: ChannelId,
        content: &str,
    ) -> anyhow::Result<DiscordMessageRef>;

    async fn edit_message(&self, message: DiscordMessageRef, content: &str) -> anyhow::Result<()>;
}

#[derive(Debug, Error)]
#[error("Discord rate limited; retry after {retry_after_ms}ms")]
struct DiscordRetryAfter {
    retry_after_ms: u64,
}

impl DiscordRetryAfter {
    #[cfg(test)]
    fn new(retry_after: Duration) -> Self {
        Self {
            retry_after_ms: retry_after.as_millis().try_into().unwrap_or(u64::MAX),
        }
    }

    fn duration(&self) -> Duration {
        Duration::from_millis(self.retry_after_ms)
    }
}

fn discord_retry_after_or_default(
    err: &anyhow::Error,
    serenity_429_fallback: Option<Duration>,
) -> Option<Duration> {
    for cause in err.chain() {
        if let Some(retry) = cause.downcast_ref::<DiscordRetryAfter>() {
            return Some(retry.duration());
        }
        if let Some(SerenityError::Http(HttpError::UnsuccessfulRequest(response))) =
            cause.downcast_ref::<SerenityError>()
        {
            if response.status_code == StatusCode::TOO_MANY_REQUESTS {
                return serenity_429_fallback;
            }
        }
    }
    None
}

struct SerenityDiscordTransport {
    http: Arc<Http>,
}

impl SerenityDiscordTransport {
    fn new(token: impl AsRef<str>) -> Self {
        Self {
            http: Arc::new(Http::new(token.as_ref())),
        }
    }

    #[cfg(test)]
    fn with_http(http: Arc<Http>) -> Self {
        Self { http }
    }
}

#[async_trait]
impl DiscordTransport for SerenityDiscordTransport {
    async fn send_message(
        &self,
        channel_id: ChannelId,
        content: &str,
    ) -> anyhow::Result<DiscordMessageRef> {
        let message = channel_id
            .send_message(&self.http, CreateMessage::new().content(content))
            .await?;
        Ok(DiscordMessageRef {
            channel_id,
            message_id: message.id,
        })
    }

    async fn edit_message(&self, message: DiscordMessageRef, content: &str) -> anyhow::Result<()> {
        message
            .channel_id
            .edit_message(
                &self.http,
                message.message_id,
                EditMessage::new().content(content),
            )
            .await?;
        Ok(())
    }
}

struct DiscordLiveMessages {
    messages: HashMap<String, DiscordLiveMessage>,
    pending_edits: HashMap<String, PendingLiveEdit>,
    pending_order: VecDeque<String>,
    next_edit_at: Option<Instant>,
    backoff: Duration,
    drain_scheduled: bool,
    config: DiscordLiveEditConfig,
}

struct DiscordLiveMessage {
    message: DiscordMessageRef,
    text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveMessageSlot {
    Response,
    Tools,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingLiveEdit {
    message: DiscordMessageRef,
    content: String,
}

impl DiscordLiveMessages {
    fn new(config: DiscordLiveEditConfig) -> Self {
        Self {
            messages: HashMap::new(),
            pending_edits: HashMap::new(),
            pending_order: VecDeque::new(),
            next_edit_at: None,
            backoff: Duration::ZERO,
            drain_scheduled: false,
            config,
        }
    }

    async fn apply_delta(
        &mut self,
        transport: &dyn DiscordTransport,
        channel_id: ChannelId,
        namespace: &str,
        delta: &str,
        slot: LiveMessageSlot,
        typing: Option<&DiscordTypingTracker>,
    ) -> anyhow::Result<bool> {
        if delta.is_empty() {
            return Ok(false);
        }
        let key = live_message_key(namespace, slot);
        if !self.messages.contains_key(&key) {
            if slot == LiveMessageSlot::Response
                && let Some(typing) = typing
            {
                typing.stop(namespace).await;
            }
            let message = transport
                .send_message(channel_id, &live_display(slot, namespace, delta, true))
                .await?;
            self.messages.insert(
                key,
                DiscordLiveMessage {
                    message,
                    text: delta.to_string(),
                },
            );
            self.next_edit_at = Some(Instant::now() + self.config.min_interval);
            self.backoff = Duration::ZERO;
            return Ok(false);
        }

        let entry = self.messages.get_mut(&key).expect("live message exists");
        if slot == LiveMessageSlot::Tools && !entry.text.is_empty() {
            entry.text.push('\n');
        }
        entry.text.push_str(delta);
        let content = live_display(slot, namespace, &entry.text, true);
        let pending = PendingLiveEdit {
            message: entry.message,
            content,
        };
        self.enqueue_pending(&key, pending);
        Ok(self.schedule_drain_if_needed())
    }

    fn finalize(&mut self, key: &str) -> Option<DiscordLiveMessage> {
        self.remove_pending(key);
        self.messages.remove(key)
    }

    fn enqueue_pending(&mut self, namespace: &str, edit: PendingLiveEdit) {
        if !self.pending_edits.contains_key(namespace) {
            self.pending_order.push_back(namespace.to_string());
        }
        self.pending_edits.insert(namespace.to_string(), edit);
    }

    fn remove_pending(&mut self, namespace: &str) {
        self.pending_edits.remove(namespace);
        self.pending_order.retain(|queued| queued != namespace);
    }

    fn schedule_drain_if_needed(&mut self) -> bool {
        if self.pending_edits.is_empty() || self.drain_scheduled {
            return false;
        }
        self.drain_scheduled = true;
        true
    }

    fn next_edit_delay(&self, now: Instant) -> Duration {
        self.next_edit_at
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or_default()
    }

    fn take_next_edit(&mut self) -> Option<(String, PendingLiveEdit)> {
        while let Some(namespace) = self.pending_order.pop_front() {
            let Some(edit) = self.pending_edits.remove(&namespace) else {
                continue;
            };
            return Some((namespace, edit));
        }
        None
    }

    fn record_live_edit_success(&mut self, now: Instant) {
        self.backoff = Duration::ZERO;
        self.next_edit_at = Some(now + self.config.min_interval);
    }

    fn record_live_edit_failure(
        &mut self,
        namespace: String,
        edit: PendingLiveEdit,
        now: Instant,
        retry_after: Option<Duration>,
    ) {
        self.backoff = if let Some(retry_after) = retry_after {
            retry_after
        } else if self.backoff.is_zero() {
            self.config.initial_backoff
        } else {
            std::cmp::min(self.backoff.saturating_mul(2), self.config.max_backoff)
        };
        self.next_edit_at = Some(now + self.backoff);
        if self
            .messages
            .get(&namespace)
            .map(|live| live.message == edit.message)
            .unwrap_or(false)
            && !self.pending_edits.contains_key(&namespace)
        {
            self.enqueue_pending(&namespace, edit);
        }
    }

    fn finish_drain_if_idle(&mut self) {
        if self.pending_edits.is_empty() {
            self.drain_scheduled = false;
        }
    }

    fn status_line(&self, namespace: &str) -> String {
        let response_key = live_message_key(namespace, LiveMessageSlot::Response);
        let tools_key = live_message_key(namespace, LiveMessageSlot::Tools);
        let active = match (
            self.messages.contains_key(&response_key),
            self.messages.contains_key(&tools_key),
        ) {
            (true, true) => "response+tools active",
            (true, false) => "response active",
            (false, true) => "tools active",
            (false, false) => "inactive",
        };
        let pending_edits = [response_key, tools_key]
            .iter()
            .filter(|key| self.pending_edits.contains_key(*key))
            .count();
        let backoff = if self.backoff.is_zero() {
            "none".to_string()
        } else {
            format!("{}ms", self.backoff.as_millis())
        };
        format!("{active}, pending edits: {pending_edits}, backoff: {backoff}")
    }
}

impl Default for DiscordLiveMessages {
    fn default() -> Self {
        Self::new(DiscordLiveEditConfig::default())
    }
}

async fn drain_live_edits(
    live: Arc<Mutex<DiscordLiveMessages>>,
    transport: Arc<dyn DiscordTransport>,
) {
    loop {
        let delay = {
            let live = live.lock().await;
            live.next_edit_delay(Instant::now())
        };
        sleep(delay).await;

        let Some((namespace, edit)) = ({
            let mut live = live.lock().await;
            let next = live.take_next_edit();
            if next.is_none() {
                live.finish_drain_if_idle();
            }
            next
        }) else {
            return;
        };

        match edit_discord_message_once(transport.as_ref(), edit.message, &edit.content).await {
            Ok(()) => {
                let mut live = live.lock().await;
                live.record_live_edit_success(Instant::now());
                if !live.pending_edits.is_empty() {
                    continue;
                }
                live.finish_drain_if_idle();
                return;
            }
            Err(err) => {
                tracing::warn!(
                    namespace = %namespace,
                    error = %err,
                    "failed to edit Discord live output; queued retry with backoff"
                );
                let mut live = live.lock().await;
                let retry_after =
                    discord_retry_after_or_default(&err, Some(live.config.initial_backoff));
                live.record_live_edit_failure(namespace, edit, Instant::now(), retry_after);
                if !live.pending_edits.is_empty() {
                    continue;
                }
                live.finish_drain_if_idle();
                return;
            }
        }
    }
}

async fn finalize_live_message(
    transport: &dyn DiscordTransport,
    channel_id: ChannelId,
    live_message: DiscordLiveMessage,
    body: &str,
) -> anyhow::Result<()> {
    let chunks = split_discord_message(body);
    edit_discord_message(transport, live_message.message, &chunks[0]).await?;
    for chunk in chunks.iter().skip(1) {
        send_discord_chunk(transport, channel_id, chunk).await?;
    }
    Ok(())
}

async fn edit_discord_message_once(
    transport: &dyn DiscordTransport,
    message: DiscordMessageRef,
    body: &str,
) -> anyhow::Result<()> {
    transport
        .edit_message(message, &first_discord_chunk(body))
        .await
}

async fn edit_discord_message(
    transport: &dyn DiscordTransport,
    message: DiscordMessageRef,
    body: &str,
) -> anyhow::Result<()> {
    let mut delay = Duration::from_millis(500);
    let mut last_error = "unknown error".to_string();
    for attempt in 1..=DISCORD_SEND_ATTEMPTS {
        match transport
            .edit_message(message, &first_discord_chunk(body))
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
                last_error = err.to_string();
                if attempt < DISCORD_SEND_ATTEMPTS {
                    let sleep_for =
                        discord_retry_after_or_default(&err, Some(delay)).unwrap_or(delay);
                    sleep(sleep_for).await;
                    delay *= 2;
                }
            }
        }
    }
    Err(anyhow::anyhow!(
        "failed to edit Discord output after {DISCORD_SEND_ATTEMPTS} attempts: {}",
        last_error
    ))
}

async fn send_discord_chunk(
    transport: &dyn DiscordTransport,
    channel_id: ChannelId,
    chunk: &str,
) -> anyhow::Result<()> {
    let mut delay = Duration::from_millis(500);
    let mut last_error = "unknown error".to_string();

    for attempt in 1..=DISCORD_SEND_ATTEMPTS {
        match transport.send_message(channel_id, chunk).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                tracing::warn!(
                    attempt,
                    max_attempts = DISCORD_SEND_ATTEMPTS,
                    error = %err,
                    "failed to send Discord output chunk"
                );
                last_error = err.to_string();
                if attempt < DISCORD_SEND_ATTEMPTS {
                    let sleep_for =
                        discord_retry_after_or_default(&err, Some(delay)).unwrap_or(delay);
                    sleep(sleep_for).await;
                    delay *= 2;
                }
            }
        }
    }

    Err(anyhow::anyhow!(
        "failed to send Discord output after {DISCORD_SEND_ATTEMPTS} attempts: {}",
        last_error
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

    chunks.push(current);
    chunks
}

fn first_discord_chunk(body: &str) -> String {
    split_discord_message(body)
        .into_iter()
        .next()
        .expect("split_discord_message always returns at least one chunk")
}

fn live_message_key(namespace: &str, slot: LiveMessageSlot) -> String {
    match slot {
        LiveMessageSlot::Response => format!("{namespace}:response"),
        LiveMessageSlot::Tools => format!("{namespace}:tools"),
    }
}

fn live_display(slot: LiveMessageSlot, namespace: &str, body: &str, live: bool) -> String {
    let header = match slot {
        LiveMessageSlot::Response => format!("<- [{namespace}]"),
        LiveMessageSlot::Tools => format!("<- [{namespace}] tools"),
    };
    let cursor = if live { " |" } else { "" };
    first_discord_chunk(&format!("{header}\n{}{}", body.trim_start(), cursor))
}

fn render_tool_started(label: &str, kind: &str) -> String {
    format!(
        "running `{}` ({})",
        compact_inline(label),
        compact_inline(kind)
    )
}

fn render_tool_completed(
    label: &str,
    kind: &str,
    status: Option<&str>,
    exit_code: Option<i64>,
    stdout: Option<&str>,
    stderr: Option<&str>,
    error: Option<&str>,
) -> String {
    let failed = exit_code.map(|code| code != 0).unwrap_or(false)
        || error.map(|text| !text.trim().is_empty()).unwrap_or(false)
        || status
            .map(|status| {
                let normalized = status.to_ascii_lowercase();
                normalized.contains("fail") || normalized.contains("error")
            })
            .unwrap_or(false);
    let status_text = exit_code
        .map(|code| format!("exit {code}"))
        .or_else(|| status.map(str::to_string))
        .unwrap_or_else(|| "done".to_string());
    let mut line = format!(
        "{} `{}` ({})",
        if failed { "failed" } else { "done" },
        compact_inline(label),
        compact_inline(kind)
    );
    line.push_str(&format!(" - {status_text}"));
    if failed {
        if let Some(detail) = first_non_empty([error, stderr, stdout]) {
            line.push('\n');
            line.push_str(&compact_block(detail, 700));
        }
    }
    line
}

fn render_final_message(
    text: &str,
    model: Option<&str>,
    duration_ms: Option<u64>,
    usage: Option<&TokenUsage>,
    exit_status: Option<&str>,
) -> String {
    let mut footer = Vec::new();
    if let Some(model) = model.filter(|model| !model.trim().is_empty()) {
        footer.push(format!("model: {}", compact_inline(model)));
    }
    if let Some(duration_ms) = duration_ms {
        footer.push(format!("duration: {}", format_duration_ms(duration_ms)));
    }
    if let Some(usage) = usage.and_then(format_usage) {
        footer.push(usage);
    }
    if let Some(exit_status) = exit_status.filter(|status| !status.trim().is_empty()) {
        footer.push(format!("status: {}", compact_inline(exit_status)));
    }
    if footer.is_empty() {
        return text.to_string();
    }
    if text.trim().is_empty() {
        return format!("[{}]", footer.join(" | "));
    }
    format!("{}\n\n[{}]", text.trim_end(), footer.join(" | "))
}

fn format_usage(usage: &TokenUsage) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(input) = usage.input_tokens {
        parts.push(format!("in {input}"));
    }
    if let Some(output) = usage.output_tokens {
        parts.push(format!("out {output}"));
    }
    if let Some(cached) = usage.cached_input_tokens {
        parts.push(format!("cached {cached}"));
    }
    if parts.is_empty() {
        usage.total_tokens.map(|total| format!("tokens: {total}"))
    } else {
        Some(format!("tokens: {}", parts.join(", ")))
    }
}

fn format_duration_ms(duration_ms: u64) -> String {
    if duration_ms < 1000 {
        format!("{duration_ms}ms")
    } else {
        format!("{:.1}s", duration_ms as f64 / 1000.0)
    }
}

fn first_non_empty<const N: usize>(values: [Option<&str>; N]) -> Option<&str> {
    values
        .into_iter()
        .flatten()
        .find(|value| !value.trim().is_empty())
}

fn compact_inline(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn compact_block(value: &str, limit: usize) -> String {
    let compact = value
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if compact.len() <= limit {
        return compact;
    }
    let mut end = 0;
    for (idx, _) in compact.char_indices() {
        if idx > limit {
            break;
        }
        end = idx;
    }
    format!("{}...", &compact[..end])
}

pub fn event_to_output_message(event: AgentEvent) -> Option<OutputMessage> {
    if let Some(payload) = event.payload {
        return match payload {
            AgentEventPayload::Text { text } => match event.stream {
                EventStreamKind::Delta => Some(OutputMessage::delta(&event.namespace, &text)),
                EventStreamKind::Final => {
                    Some(OutputMessage::final_message(&event.namespace, &text))
                }
                EventStreamKind::Stdout => Some(OutputMessage::complete(&event.namespace, &text)),
                EventStreamKind::Stderr | EventStreamKind::Status => None,
            },
            AgentEventPayload::ToolStarted { label, kind, .. } => Some(OutputMessage::tool(
                &event.namespace,
                &render_tool_started(&label, &kind),
            )),
            AgentEventPayload::ToolCompleted {
                label,
                kind,
                status,
                exit_code,
                stdout,
                stderr,
                error,
                ..
            } => Some(OutputMessage::tool(
                &event.namespace,
                &render_tool_completed(
                    &label,
                    &kind,
                    status.as_deref(),
                    exit_code,
                    stdout.as_deref(),
                    stderr.as_deref(),
                    error.as_deref(),
                ),
            )),
            AgentEventPayload::TurnCompleted {
                final_text,
                model,
                duration_ms,
                usage,
                exit_status,
            } => Some(OutputMessage::final_message(
                &event.namespace,
                &render_final_message(
                    &final_text,
                    model.as_deref(),
                    duration_ms,
                    usage.as_ref(),
                    exit_status.as_deref(),
                ),
            )),
            AgentEventPayload::Error { message } => Some(OutputMessage::complete(
                &event.namespace,
                &format!("Codex error: {message}"),
            )),
        };
    }

    match event.stream {
        EventStreamKind::Stdout => Some(OutputMessage::complete(&event.namespace, &event.line)),
        EventStreamKind::Delta => Some(OutputMessage::delta(&event.namespace, &event.line)),
        EventStreamKind::Final => Some(OutputMessage::final_message(&event.namespace, &event.line)),
        EventStreamKind::Stderr | EventStreamKind::Status => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::AgentEngine;
    use crate::test_support::DiscordHttpProxy;
    use serenity::http::HttpBuilder;

    use super::*;

    #[derive(Default)]
    struct FakeDiscordTransport {
        state: tokio::sync::Mutex<FakeDiscordState>,
    }

    #[derive(Default, Clone)]
    struct FakeDiscordState {
        next_message_id: u64,
        sends: Vec<(ChannelId, String)>,
        edits: Vec<(DiscordMessageRef, String)>,
        fail_sends: usize,
        fail_edits: usize,
        rate_limited_sends: VecDeque<Duration>,
        rate_limited_edits: VecDeque<Duration>,
    }

    impl FakeDiscordTransport {
        async fn sends(&self) -> Vec<(ChannelId, String)> {
            self.state.lock().await.sends.clone()
        }

        async fn edits(&self) -> Vec<(DiscordMessageRef, String)> {
            self.state.lock().await.edits.clone()
        }

        async fn fail_next_sends(&self, count: usize) {
            self.state.lock().await.fail_sends = count;
        }

        async fn fail_next_edits(&self, count: usize) {
            self.state.lock().await.fail_edits = count;
        }

        async fn rate_limit_next_send(&self, retry_after: Duration) {
            self.state
                .lock()
                .await
                .rate_limited_sends
                .push_back(retry_after);
        }

        async fn rate_limit_next_edit(&self, retry_after: Duration) {
            self.state
                .lock()
                .await
                .rate_limited_edits
                .push_back(retry_after);
        }
    }

    #[async_trait]
    impl DiscordTransport for FakeDiscordTransport {
        async fn send_message(
            &self,
            channel_id: ChannelId,
            content: &str,
        ) -> anyhow::Result<DiscordMessageRef> {
            let mut state = self.state.lock().await;
            if let Some(retry_after) = state.rate_limited_sends.pop_front() {
                return Err(DiscordRetryAfter::new(retry_after).into());
            }
            if state.fail_sends > 0 {
                state.fail_sends -= 1;
                anyhow::bail!("send failed");
            }
            state.next_message_id += 1;
            state.sends.push((channel_id, content.to_string()));
            Ok(DiscordMessageRef {
                channel_id,
                message_id: MessageId::new(state.next_message_id),
            })
        }

        async fn edit_message(
            &self,
            message: DiscordMessageRef,
            content: &str,
        ) -> anyhow::Result<()> {
            let mut state = self.state.lock().await;
            if let Some(retry_after) = state.rate_limited_edits.pop_front() {
                return Err(DiscordRetryAfter::new(retry_after).into());
            }
            if state.fail_edits > 0 {
                state.fail_edits -= 1;
                anyhow::bail!("edit failed");
            }
            state.edits.push((message, content.to_string()));
            Ok(())
        }
    }

    fn registry_with_bindings(bindings: &[(&str, u64)]) -> BindingRegistry {
        BindingRegistry::new(bindings.iter().map(|(namespace, channel_id)| {
            crate::discord::ChannelBinding {
                channel_id: channel_id.to_string(),
                namespace: namespace.to_string(),
                repo_url: format!("https://github.com/Gonzih/{namespace}"),
            }
        }))
        .unwrap()
    }

    fn registry() -> BindingRegistry {
        registry_with_bindings(&[("moni", 123)])
    }

    fn output_with_transport(transport: Arc<FakeDiscordTransport>) -> DiscordOutputSink {
        DiscordOutputSink::with_transport(registry(), transport)
    }

    fn output_with_bindings(
        transport: Arc<FakeDiscordTransport>,
        bindings: &[(&str, u64)],
    ) -> DiscordOutputSink {
        DiscordOutputSink::with_transport(registry_with_bindings(bindings), transport)
    }

    fn output_with_config(
        transport: Arc<FakeDiscordTransport>,
        config: DiscordLiveEditConfig,
    ) -> DiscordOutputSink {
        DiscordOutputSink::with_transport(registry(), transport).with_live_edit_config(config)
    }

    async fn run_live_edit_tasks() {
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }

    async fn advance_live_edit_clock(duration: Duration) {
        run_live_edit_tasks().await;
        tokio::time::advance(duration).await;
        run_live_edit_tasks().await;
    }

    #[tokio::test]
    async fn memory_output_records_messages() {
        let sink = InMemoryOutputSink::default();
        sink.send(OutputMessage::complete("moni", "hello"))
            .await
            .unwrap();

        assert_eq!(sink.messages().await.len(), 1);
        assert_eq!(sink.messages().await[0].body, "hello");
    }

    #[tokio::test]
    async fn memory_output_reports_live_status_unavailable() {
        let sink = InMemoryOutputSink::default();

        assert_eq!(sink.live_status("moni").await, "unavailable");
    }

    #[test]
    fn output_message_constructors_accept_owned_strings() {
        let namespace = "moni".to_string();
        let body = "hello".to_string();

        assert_eq!(
            OutputMessage::complete(&namespace, &body).kind,
            OutputMessageKind::Complete
        );
        assert_eq!(
            OutputMessage::delta(&namespace, &body).kind,
            OutputMessageKind::Delta
        );
        assert_eq!(
            OutputMessage::final_message(&namespace, &body).kind,
            OutputMessageKind::Final
        );
        assert_eq!(
            OutputMessage::tool(&namespace, &body).kind,
            OutputMessageKind::Tool
        );
    }

    #[tokio::test(start_paused = true)]
    async fn discord_output_reports_live_status_for_active_slots() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let config = DiscordLiveEditConfig::new(
            Duration::from_secs(60),
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .unwrap();
        let sink = DiscordOutputSink::with_transport(
            registry_with_bindings(&[("moni", 123), ("tools", 456)]),
            transport,
        )
        .with_live_edit_config(config);

        assert_eq!(
            sink.live_status("moni").await,
            "inactive, pending edits: 0, backoff: none"
        );
        sink.send(OutputMessage::delta("moni", "hello"))
            .await
            .unwrap();
        assert_eq!(
            sink.live_status("moni").await,
            "response active, pending edits: 0, backoff: none"
        );
        sink.send(OutputMessage::tool("tools", "exec cargo test"))
            .await
            .unwrap();
        assert_eq!(
            sink.live_status("tools").await,
            "tools active, pending edits: 0, backoff: none"
        );
        sink.send(OutputMessage::tool("moni", "exec cargo test"))
            .await
            .unwrap();
        assert_eq!(
            sink.live_status("moni").await,
            "response+tools active, pending edits: 0, backoff: none"
        );
        sink.send(OutputMessage::delta("moni", " again"))
            .await
            .unwrap();
        assert_eq!(
            sink.live_status("moni").await,
            "response+tools active, pending edits: 1, backoff: none"
        );
        sink.live.lock().await.backoff = Duration::from_secs(2);
        assert_eq!(
            sink.live_status("moni").await,
            "response+tools active, pending edits: 1, backoff: 2000ms"
        );
    }

    #[tokio::test]
    async fn discord_output_new_maps_bindings_and_ignores_unbound_namespace() {
        let sink = DiscordOutputSink::new("token", [("moni".to_string(), ChannelId::new(123))])
            .with_typing_tracker(DiscordTypingTracker::default());

        sink.send(OutputMessage::complete("missing", "hello"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn serenity_discord_transport_sends_and_edits_through_http_proxy() {
        let proxy = DiscordHttpProxy::start();
        let http = Arc::new(
            HttpBuilder::new("token")
                .proxy(proxy.base_url())
                .ratelimiter_disabled(true)
                .build(),
        );
        let transport = SerenityDiscordTransport::with_http(http);

        let message = transport
            .send_message(ChannelId::new(123), "hello proxy")
            .await
            .unwrap();
        transport
            .edit_message(message, "edited proxy")
            .await
            .unwrap();

        let requests = proxy.requests();
        assert!(requests.iter().any(|request| request.method == "POST"
            && request.path.contains("/channels/123/messages")
            && request.body.contains("hello proxy")));
        assert!(requests.iter().any(|request| request.method == "PATCH"
            && request.path.contains("/channels/123/messages/111")
            && request.body.contains("edited proxy")));
        assert_eq!(message.message_id, MessageId::new(111));
    }

    #[tokio::test]
    async fn serenity_429_errors_use_configured_retry_after_fallback() {
        let proxy = DiscordHttpProxy::start();
        let http = Arc::new(
            HttpBuilder::new("token")
                .proxy(proxy.base_url())
                .ratelimiter_disabled(true)
                .build(),
        );
        let transport = SerenityDiscordTransport::with_http(http);

        let err = transport
            .send_message(ChannelId::new(429), "rate limited")
            .await
            .unwrap_err();

        assert_eq!(
            discord_retry_after_or_default(&err, Some(Duration::from_secs(4))),
            Some(Duration::from_secs(4))
        );
    }

    #[tokio::test]
    async fn serenity_non_429_errors_do_not_use_retry_after_fallback() {
        let proxy = DiscordHttpProxy::start();
        let http = Arc::new(
            HttpBuilder::new("token")
                .proxy(proxy.base_url())
                .ratelimiter_disabled(true)
                .build(),
        );
        let transport = SerenityDiscordTransport::with_http(http);

        let err = transport
            .send_message(ChannelId::new(400), "bad request")
            .await
            .unwrap_err();

        assert_eq!(
            discord_retry_after_or_default(&err, Some(Duration::from_secs(4))),
            None
        );
    }

    #[tokio::test]
    async fn discord_typing_tracker_starts_replaces_and_stops_typing() {
        let proxy = DiscordHttpProxy::start();
        let http = Arc::new(
            HttpBuilder::new("token")
                .proxy(proxy.base_url())
                .ratelimiter_disabled(true)
                .build(),
        );
        let tracker = DiscordTypingTracker::default();

        tracker.start("moni", ChannelId::new(123), &http).await;
        assert!(tracker.active.lock().await.contains_key("moni"));
        tracker.start("moni", ChannelId::new(123), &http).await;
        assert_eq!(tracker.active.lock().await.len(), 1);
        tracker.stop("moni").await;

        assert!(tracker.active.lock().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn discord_typing_tracker_expires_after_timeout() {
        let proxy = DiscordHttpProxy::start();
        let http = Arc::new(
            HttpBuilder::new("token")
                .proxy(proxy.base_url())
                .ratelimiter_disabled(true)
                .build(),
        );
        let tracker = DiscordTypingTracker::default();

        tracker.start("moni", ChannelId::new(123), &http).await;
        assert!(tracker.active.lock().await.contains_key("moni"));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(15 * 60)).await;
        tokio::task::yield_now().await;

        assert!(tracker.active.lock().await.is_empty());
    }

    #[tokio::test]
    async fn discord_output_sends_complete_messages_in_chunks() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());
        let body = format!(
            "{}\n{}",
            "a".repeat(100),
            "b".repeat(DISCORD_MESSAGE_LIMIT + 5)
        );

        sink.send(OutputMessage::complete("moni", &body))
            .await
            .unwrap();

        let sends = transport.sends().await;
        assert_eq!(sends.len(), 3);
        assert!(
            sends
                .iter()
                .all(|(_, chunk)| chunk.len() <= DISCORD_MESSAGE_LIMIT)
        );
        assert_eq!(
            sends
                .into_iter()
                .map(|(_, chunk)| chunk)
                .collect::<Vec<_>>()
                .join(""),
            body
        );
    }

    #[tokio::test(start_paused = true)]
    async fn discord_output_edits_live_message_and_finalizes_stream() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());
        let min_interval = DiscordLiveEditConfig::default().min_interval();

        sink.send(OutputMessage::delta("moni", "hel"))
            .await
            .unwrap();
        sink.send(OutputMessage::delta("moni", "lo")).await.unwrap();
        advance_live_edit_clock(min_interval).await;
        sink.send(OutputMessage::final_message("moni", "hello final"))
            .await
            .unwrap();

        assert_eq!(
            transport.sends().await,
            vec![(ChannelId::new(123), "<- [moni]\nhel |".to_string())]
        );
        let edits = transport.edits().await;
        assert_eq!(edits.len(), 2);
        assert_eq!(edits[0].1, "<- [moni]\nhello |");
        assert_eq!(edits[1].1, "hello final");
    }

    #[tokio::test(start_paused = true)]
    async fn discord_output_renders_tool_slot_separately_from_response() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());
        let min_interval = DiscordLiveEditConfig::default().min_interval();

        sink.send(OutputMessage::tool("moni", "running `cargo test`"))
            .await
            .unwrap();
        sink.send(OutputMessage::delta("moni", "hel"))
            .await
            .unwrap();
        sink.send(OutputMessage::tool("moni", "done `cargo test`"))
            .await
            .unwrap();
        advance_live_edit_clock(min_interval).await;
        sink.send(OutputMessage::final_message("moni", "hello final"))
            .await
            .unwrap();

        assert_eq!(
            transport.sends().await,
            vec![
                (
                    ChannelId::new(123),
                    "<- [moni] tools\nrunning `cargo test` |".to_string()
                ),
                (ChannelId::new(123), "<- [moni]\nhel |".to_string())
            ]
        );
        let edits = transport.edits().await;
        assert_eq!(
            edits[0].1,
            "<- [moni] tools\nrunning `cargo test`\ndone `cargo test` |"
        );
        assert_eq!(
            edits[1].1,
            "<- [moni] tools\nrunning `cargo test`\ndone `cargo test`"
        );
        assert_eq!(edits[2].1, "hello final");
    }

    #[tokio::test(start_paused = true)]
    async fn discord_output_flushes_pending_live_edit_after_quiet_interval() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());
        let min_interval = DiscordLiveEditConfig::default().min_interval();

        sink.send(OutputMessage::delta("moni", "hel"))
            .await
            .unwrap();
        sink.send(OutputMessage::delta("moni", "lo")).await.unwrap();
        run_live_edit_tasks().await;

        assert!(transport.edits().await.is_empty());

        advance_live_edit_clock(min_interval).await;

        assert_eq!(
            transport.edits().await,
            vec![(
                DiscordMessageRef {
                    channel_id: ChannelId::new(123),
                    message_id: MessageId::new(1),
                },
                "<- [moni]\nhello |".to_string()
            )]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn discord_output_global_live_edit_pacing_drains_one_namespace_per_interval() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_bindings(transport.clone(), &[("moni", 123), ("ops", 456)]);
        let min_interval = DiscordLiveEditConfig::default().min_interval();

        sink.send(OutputMessage::delta("moni", "he")).await.unwrap();
        sink.send(OutputMessage::delta("ops", "go")).await.unwrap();
        sink.send(OutputMessage::delta("moni", "llo"))
            .await
            .unwrap();
        sink.send(OutputMessage::delta("ops", " now"))
            .await
            .unwrap();

        advance_live_edit_clock(min_interval).await;

        let first_edits = transport.edits().await;
        assert_eq!(first_edits.len(), 1);
        assert_eq!(first_edits[0].0.channel_id, ChannelId::new(123));
        assert_eq!(first_edits[0].1, "<- [moni]\nhello |");

        advance_live_edit_clock(min_interval).await;

        let edits = transport.edits().await;
        assert_eq!(edits.len(), 2);
        assert_eq!(edits[1].0.channel_id, ChannelId::new(456));
        assert_eq!(edits[1].1, "<- [ops]\ngo now |");
    }

    #[tokio::test]
    async fn discord_output_ignores_empty_delta() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());

        sink.send(OutputMessage::delta("moni", "")).await.unwrap();

        assert!(transport.sends().await.is_empty());
        assert!(transport.edits().await.is_empty());
    }

    #[tokio::test]
    async fn discord_output_stops_typing_when_live_message_starts() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone())
            .with_typing_tracker(DiscordTypingTracker::default());

        sink.send(OutputMessage::delta("moni", "hello"))
            .await
            .unwrap();

        assert_eq!(
            transport.sends().await,
            vec![(ChannelId::new(123), "<- [moni]\nhello |".to_string())]
        );
    }

    #[tokio::test]
    async fn discord_output_reports_live_placeholder_send_error() {
        let transport = Arc::new(FakeDiscordTransport::default());
        transport.fail_next_sends(1).await;
        let sink = output_with_transport(transport);

        let err = sink
            .send(OutputMessage::delta("moni", "hello"))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("send failed"));
    }

    #[tokio::test(start_paused = true)]
    async fn discord_output_retries_live_edit_failures_with_backoff() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());
        let config = DiscordLiveEditConfig::default();

        sink.send(OutputMessage::delta("moni", "hel"))
            .await
            .unwrap();
        transport.fail_next_edits(2).await;
        sink.send(OutputMessage::delta("moni", "lo")).await.unwrap();

        advance_live_edit_clock(config.min_interval()).await;
        assert!(transport.edits().await.is_empty());
        assert_eq!(sink.live.lock().await.backoff, config.initial_backoff());

        advance_live_edit_clock(config.initial_backoff()).await;
        assert!(transport.edits().await.is_empty());
        assert_eq!(sink.live.lock().await.backoff, config.initial_backoff() * 2);

        advance_live_edit_clock(config.initial_backoff() * 2).await;
        assert_eq!(
            transport.edits().await,
            vec![(
                DiscordMessageRef {
                    channel_id: ChannelId::new(123),
                    message_id: MessageId::new(1),
                },
                "<- [moni]\nhello |".to_string()
            )]
        );
        assert_eq!(sink.live.lock().await.backoff, Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn discord_output_uses_retry_after_for_live_edit_failures() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_config(
            transport.clone(),
            DiscordLiveEditConfig::new(
                Duration::from_secs(1),
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .unwrap(),
        );

        sink.send(OutputMessage::delta("moni", "hello"))
            .await
            .unwrap();
        transport.rate_limit_next_edit(Duration::from_secs(7)).await;
        sink.send(OutputMessage::delta("moni", " world"))
            .await
            .unwrap();

        advance_live_edit_clock(Duration::from_secs(1)).await;
        assert!(transport.edits().await.is_empty());
        assert_eq!(sink.live.lock().await.backoff, Duration::from_secs(7));

        advance_live_edit_clock(Duration::from_secs(7)).await;
        assert_eq!(transport.edits().await.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn discord_output_final_flush_cancels_pending_live_edit() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());
        let min_interval = DiscordLiveEditConfig::default().min_interval();

        sink.send(OutputMessage::delta("moni", "hel"))
            .await
            .unwrap();
        sink.send(OutputMessage::delta("moni", "lo")).await.unwrap();
        sink.send(OutputMessage::final_message("moni", "hello final"))
            .await
            .unwrap();

        assert_eq!(
            transport.edits().await,
            vec![(
                DiscordMessageRef {
                    channel_id: ChannelId::new(123),
                    message_id: MessageId::new(1),
                },
                "hello final".to_string()
            )]
        );

        advance_live_edit_clock(min_interval).await;

        assert_eq!(transport.edits().await.len(), 1);
        assert!(!sink.live.lock().await.drain_scheduled);
    }

    #[tokio::test]
    async fn live_edit_drain_drops_stale_failed_edit_without_retry() {
        let transport = Arc::new(FakeDiscordTransport::default());
        transport.fail_next_edits(1).await;
        let message = DiscordMessageRef {
            channel_id: ChannelId::new(123),
            message_id: MessageId::new(1),
        };
        let mut state = DiscordLiveMessages::default();
        state.enqueue_pending(
            "moni",
            PendingLiveEdit {
                message,
                content: "<- [moni]\nhello |".to_string(),
            },
        );
        state.drain_scheduled = true;
        let live = Arc::new(Mutex::new(state));

        drain_live_edits(live.clone(), transport.clone()).await;

        let live = live.lock().await;
        assert!(live.pending_edits.is_empty());
        assert!(!live.drain_scheduled);
        assert_eq!(
            live.backoff,
            DiscordLiveEditConfig::default().initial_backoff()
        );
        assert!(transport.edits().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn discord_output_uses_custom_live_edit_config() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let config = DiscordLiveEditConfig::new(
            Duration::from_millis(50),
            Duration::from_millis(80),
            Duration::from_millis(120),
        )
        .unwrap();
        let sink = output_with_config(transport.clone(), config);

        sink.send(OutputMessage::delta("moni", "hel"))
            .await
            .unwrap();
        transport.fail_next_edits(2).await;
        sink.send(OutputMessage::delta("moni", "lo")).await.unwrap();

        advance_live_edit_clock(config.min_interval()).await;
        assert_eq!(sink.live.lock().await.backoff, config.initial_backoff());

        advance_live_edit_clock(config.initial_backoff()).await;
        assert_eq!(sink.live.lock().await.backoff, config.max_backoff());

        advance_live_edit_clock(config.max_backoff()).await;
        assert_eq!(
            transport.edits().await,
            vec![(
                DiscordMessageRef {
                    channel_id: ChannelId::new(123),
                    message_id: MessageId::new(1),
                },
                "<- [moni]\nhello |".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn discord_output_finalizes_live_message_with_followup_chunks() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());
        let final_body = "f".repeat(DISCORD_MESSAGE_LIMIT + 1);

        sink.send(OutputMessage::delta("moni", "pending"))
            .await
            .unwrap();
        sink.send(OutputMessage::final_message("moni", &final_body))
            .await
            .unwrap();

        let edits = transport.edits().await;
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].1.len(), DISCORD_MESSAGE_LIMIT);
        let sends = transport.sends().await;
        assert_eq!(sends.len(), 2);
        assert_eq!(format!("{}{}", edits[0].1, sends[1].1), final_body);
    }

    #[tokio::test]
    async fn discord_output_reports_finalize_edit_error() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());

        sink.send(OutputMessage::delta("moni", "pending"))
            .await
            .unwrap();
        transport.fail_next_edits(DISCORD_SEND_ATTEMPTS).await;
        let err = sink
            .send(OutputMessage::final_message("moni", "final"))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("failed to edit Discord output"));
    }

    #[tokio::test]
    async fn discord_output_reports_finalize_followup_send_error() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());
        let final_body = "f".repeat(DISCORD_MESSAGE_LIMIT + 1);

        sink.send(OutputMessage::delta("moni", "pending"))
            .await
            .unwrap();
        transport.fail_next_sends(DISCORD_SEND_ATTEMPTS).await;
        let err = sink
            .send(OutputMessage::final_message("moni", &final_body))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("failed to send Discord output"));
    }

    #[tokio::test]
    async fn discord_output_final_without_live_message_sends_complete_message() {
        let transport = Arc::new(FakeDiscordTransport::default());
        let sink = output_with_transport(transport.clone());

        sink.send(OutputMessage::final_message("moni", "final body"))
            .await
            .unwrap();

        assert_eq!(
            transport.sends().await,
            vec![(ChannelId::new(123), "final body".to_string())]
        );
        assert!(transport.edits().await.is_empty());
    }

    #[tokio::test]
    async fn discord_output_with_registry_ignores_unbound_delta_and_final() {
        let sink =
            DiscordOutputSink::with_registry("token", BindingRegistry::new(Vec::new()).unwrap());

        sink.send(OutputMessage::delta("moni", "hel"))
            .await
            .unwrap();
        sink.send(OutputMessage::final_message("moni", "hello"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn discord_send_chunk_retries_then_succeeds() {
        let transport = FakeDiscordTransport::default();
        transport.fail_next_sends(2).await;

        send_discord_chunk(&transport, ChannelId::new(123), "hello")
            .await
            .unwrap();

        assert_eq!(
            transport.sends().await,
            vec![(ChannelId::new(123), "hello".to_string())]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn send_discord_chunk_uses_retry_after_before_retrying() {
        let transport = FakeDiscordTransport::default();
        transport.rate_limit_next_send(Duration::from_secs(3)).await;

        let send = tokio::spawn(async move {
            send_discord_chunk(&transport, ChannelId::new(123), "hello")
                .await
                .unwrap();
            transport
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert!(!send.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        let transport = send.await.unwrap();

        assert_eq!(
            transport.sends().await,
            vec![(ChannelId::new(123), "hello".to_string())]
        );
    }

    #[tokio::test]
    async fn discord_send_chunk_reports_retry_exhaustion() {
        let transport = FakeDiscordTransport::default();
        transport.fail_next_sends(DISCORD_SEND_ATTEMPTS).await;

        let err = send_discord_chunk(&transport, ChannelId::new(123), "hello")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("failed to send Discord output"));
        assert!(err.to_string().contains("send failed"));
    }

    #[tokio::test]
    async fn discord_edit_message_retries_then_succeeds() {
        let transport = FakeDiscordTransport::default();
        let message = transport
            .send_message(ChannelId::new(123), "placeholder")
            .await
            .unwrap();
        transport.fail_next_edits(2).await;

        edit_discord_message(&transport, message, "edited")
            .await
            .unwrap();

        assert_eq!(
            transport.edits().await,
            vec![(message, "edited".to_string())]
        );
    }

    #[tokio::test]
    async fn discord_edit_message_reports_retry_exhaustion() {
        let transport = FakeDiscordTransport::default();
        let message = transport
            .send_message(ChannelId::new(123), "placeholder")
            .await
            .unwrap();
        transport.fail_next_edits(DISCORD_SEND_ATTEMPTS).await;

        let err = edit_discord_message(&transport, message, "edited")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("failed to edit Discord output"));
        assert!(err.to_string().contains("edit failed"));
    }

    #[test]
    fn stdout_event_becomes_output_message() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Claude,
            stream: EventStreamKind::Stdout,
            line: "hello".to_string(),
            payload: None,
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
            payload: None,
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
            payload: None,
        })
        .unwrap();

        assert_eq!(output.kind, OutputMessageKind::Final);
        assert_eq!(output.body, "hello");
    }

    #[test]
    fn structured_final_event_renders_usage_model_and_duration() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Final,
            line: "hello".to_string(),
            payload: Some(AgentEventPayload::TurnCompleted {
                final_text: "hello".to_string(),
                model: Some("gpt-5-codex".to_string()),
                duration_ms: Some(2450),
                usage: Some(TokenUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(20),
                    cached_input_tokens: Some(5),
                    total_tokens: None,
                }),
                exit_status: Some("completed".to_string()),
            }),
        })
        .unwrap();

        assert_eq!(output.kind, OutputMessageKind::Final);
        assert_eq!(
            output.body,
            "hello\n\n[model: gpt-5-codex | duration: 2.5s | tokens: in 10, out 20, cached 5 | status: completed]"
        );
    }

    #[test]
    fn structured_tool_events_render_compact_status_blocks() {
        let started = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Status,
            line: "tool-start:cargo test".to_string(),
            payload: Some(AgentEventPayload::ToolStarted {
                id: Some("tool-1".to_string()),
                label: "cargo test".to_string(),
                kind: "commandExecution".to_string(),
            }),
        })
        .unwrap();
        let failed = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Status,
            line: "tool-complete:cargo test".to_string(),
            payload: Some(AgentEventPayload::ToolCompleted {
                id: Some("tool-1".to_string()),
                label: "cargo test".to_string(),
                kind: "commandExecution".to_string(),
                status: Some("failed".to_string()),
                exit_code: Some(101),
                stdout: Some("lots of stdout".to_string()),
                stderr: Some("test failed".to_string()),
                error: None,
            }),
        })
        .unwrap();

        assert_eq!(started.kind, OutputMessageKind::Tool);
        assert_eq!(started.body, "running `cargo test` (commandExecution)");
        assert_eq!(failed.kind, OutputMessageKind::Tool);
        assert_eq!(
            failed.body,
            "failed `cargo test` (commandExecution) - exit 101\ntest failed"
        );
    }

    #[test]
    fn structured_tool_failed_status_and_long_output_are_compacted() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Status,
            line: "tool-complete:cargo test".to_string(),
            payload: Some(AgentEventPayload::ToolCompleted {
                id: Some("tool-1".to_string()),
                label: "cargo test".to_string(),
                kind: "commandExecution".to_string(),
                status: Some("failed".to_string()),
                exit_code: None,
                stdout: Some("x".repeat(800)),
                stderr: None,
                error: None,
            }),
        })
        .unwrap();

        assert_eq!(output.kind, OutputMessageKind::Tool);
        assert!(output.body.starts_with("failed `cargo test`"));
        assert!(output.body.ends_with("..."));
        assert!(output.body.len() < 760);
    }

    #[test]
    fn structured_tool_completion_covers_status_and_detail_variants() {
        let ok_status = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Status,
            line: "tool-complete:cargo test".to_string(),
            payload: Some(AgentEventPayload::ToolCompleted {
                id: Some("tool-1".to_string()),
                label: "cargo test".to_string(),
                kind: "commandExecution".to_string(),
                status: Some("completed".to_string()),
                exit_code: None,
                stdout: None,
                stderr: None,
                error: Some(" ".to_string()),
            }),
        })
        .unwrap();
        let failed_without_detail = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Status,
            line: "tool-complete:cargo test".to_string(),
            payload: Some(AgentEventPayload::ToolCompleted {
                id: Some("tool-2".to_string()),
                label: "cargo test".to_string(),
                kind: "commandExecution".to_string(),
                status: None,
                exit_code: Some(1),
                stdout: None,
                stderr: None,
                error: None,
            }),
        })
        .unwrap();
        let default_done = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Status,
            line: "tool-complete:cargo test".to_string(),
            payload: Some(AgentEventPayload::ToolCompleted {
                id: Some("tool-3".to_string()),
                label: "cargo test".to_string(),
                kind: "commandExecution".to_string(),
                status: None,
                exit_code: None,
                stdout: None,
                stderr: None,
                error: None,
            }),
        })
        .unwrap();

        assert_eq!(
            ok_status.body,
            "done `cargo test` (commandExecution) - completed"
        );
        assert_eq!(
            failed_without_detail.body,
            "failed `cargo test` (commandExecution) - exit 1"
        );
        assert_eq!(
            default_done.body,
            "done `cargo test` (commandExecution) - done"
        );
    }

    #[test]
    fn structured_text_payload_maps_non_delta_streams() {
        let final_output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Final,
            line: "ignored".to_string(),
            payload: Some(AgentEventPayload::Text {
                text: "final".to_string(),
            }),
        })
        .unwrap();
        let stdout_output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Stdout,
            line: "ignored".to_string(),
            payload: Some(AgentEventPayload::Text {
                text: "stdout".to_string(),
            }),
        })
        .unwrap();
        let status_output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Status,
            line: "ignored".to_string(),
            payload: Some(AgentEventPayload::Text {
                text: "status".to_string(),
            }),
        });

        assert_eq!(final_output.kind, OutputMessageKind::Final);
        assert_eq!(final_output.body, "final");
        assert_eq!(stdout_output.kind, OutputMessageKind::Complete);
        assert_eq!(stdout_output.body, "stdout");
        assert!(status_output.is_none());
    }

    #[test]
    fn structured_final_event_handles_footer_edge_cases() {
        let plain = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Final,
            line: "hello".to_string(),
            payload: Some(AgentEventPayload::TurnCompleted {
                final_text: "hello".to_string(),
                model: None,
                duration_ms: None,
                usage: None,
                exit_status: None,
            }),
        })
        .unwrap();
        let footer_only = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Final,
            line: String::new(),
            payload: Some(AgentEventPayload::TurnCompleted {
                final_text: " ".to_string(),
                model: None,
                duration_ms: None,
                usage: Some(TokenUsage {
                    input_tokens: None,
                    output_tokens: None,
                    cached_input_tokens: None,
                    total_tokens: Some(42),
                }),
                exit_status: None,
            }),
        })
        .unwrap();

        assert_eq!(plain.body, "hello");
        assert_eq!(footer_only.body, "[tokens: 42]");
    }

    #[test]
    fn structured_error_event_is_visible_output() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Codex,
            stream: EventStreamKind::Status,
            line: "codex-error:boom".to_string(),
            payload: Some(AgentEventPayload::Error {
                message: "boom".to_string(),
            }),
        })
        .unwrap();

        assert_eq!(output.kind, OutputMessageKind::Complete);
        assert_eq!(output.body, "Codex error: boom");
    }

    #[test]
    fn stderr_event_is_not_sent_to_discord() {
        let output = event_to_output_message(AgentEvent {
            namespace: "moni".to_string(),
            engine: AgentEngine::Claude,
            stream: EventStreamKind::Stderr,
            line: "err".to_string(),
            payload: None,
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
            payload: None,
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

    #[test]
    fn split_discord_message_splits_long_line_after_existing_chunk() {
        let body = format!(
            "{}\n{}",
            "a".repeat(100),
            "b".repeat(DISCORD_MESSAGE_LIMIT + 5)
        );
        let chunks = split_discord_message(&body);

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks.join(""), body);
    }

    #[test]
    fn split_discord_message_starts_new_chunk_for_short_overflowing_line() {
        let body = format!("{}\nsmall", "a".repeat(DISCORD_MESSAGE_LIMIT - 1));
        let chunks = split_discord_message(&body);

        assert_eq!(
            chunks,
            vec![
                format!("{}\n", "a".repeat(DISCORD_MESSAGE_LIMIT - 1)),
                "small".to_string()
            ]
        );
    }

    #[test]
    fn first_chunk_and_live_display_are_sendable() {
        assert_eq!(first_discord_chunk(""), " ");
        assert_eq!(
            first_discord_chunk(&"a".repeat(DISCORD_MESSAGE_LIMIT + 1)).len(),
            DISCORD_MESSAGE_LIMIT
        );
        assert_eq!(
            live_display(LiveMessageSlot::Response, "moni", "  hello", true),
            "<- [moni]\nhello |"
        );
        assert_eq!(
            live_display(LiveMessageSlot::Tools, "moni", "  cargo test", false),
            "<- [moni] tools\ncargo test"
        );
    }

    #[test]
    fn live_message_edit_delay_defaults_to_zero_and_saturates_past_deadlines() {
        let mut live = DiscordLiveMessages::default();
        let now = Instant::now();

        assert_eq!(live.next_edit_delay(now), Duration::ZERO);
        live.next_edit_at = Some(now + Duration::from_secs(60));
        assert_eq!(live.next_edit_delay(now), Duration::from_secs(60));
        live.next_edit_at = Some(now - Duration::from_secs(1));
        assert_eq!(live.next_edit_delay(now), Duration::ZERO);
    }

    #[test]
    fn live_edit_config_defaults_and_validation_are_explicit() {
        let default = DiscordLiveEditConfig::default();
        assert_eq!(default.min_interval(), Duration::from_millis(900));
        assert_eq!(default.initial_backoff(), Duration::from_millis(1500));
        assert_eq!(default.max_backoff(), Duration::from_secs(60));

        let custom = DiscordLiveEditConfig::new(
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(40),
        )
        .unwrap();
        assert_eq!(custom.min_interval(), Duration::from_millis(10));
        assert_eq!(custom.initial_backoff(), Duration::from_millis(20));
        assert_eq!(custom.max_backoff(), Duration::from_millis(40));

        assert!(
            DiscordLiveEditConfig::new(
                Duration::ZERO,
                Duration::from_millis(20),
                Duration::from_millis(40)
            )
            .unwrap_err()
            .to_string()
            .contains("interval")
        );
        assert!(
            DiscordLiveEditConfig::new(
                Duration::from_millis(10),
                Duration::ZERO,
                Duration::from_millis(40)
            )
            .unwrap_err()
            .to_string()
            .contains("initial backoff")
        );
        assert!(
            DiscordLiveEditConfig::new(
                Duration::from_millis(10),
                Duration::from_millis(50),
                Duration::from_millis(40)
            )
            .unwrap_err()
            .to_string()
            .contains("max backoff")
        );
    }

    #[test]
    fn live_message_edit_queue_skips_stale_order_entries() {
        let mut live = DiscordLiveMessages::default();
        let message = DiscordMessageRef {
            channel_id: ChannelId::new(123),
            message_id: MessageId::new(1),
        };
        let pending = PendingLiveEdit {
            message,
            content: "<- [moni]\nhello |".to_string(),
        };
        live.pending_order.push_back("stale".to_string());
        live.enqueue_pending("moni", pending.clone());

        assert_eq!(live.take_next_edit(), Some(("moni".to_string(), pending)));
        assert!(live.take_next_edit().is_none());
    }
}
