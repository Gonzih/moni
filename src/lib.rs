pub mod app;
pub mod commands;
pub mod cron;
pub mod discord;
pub mod engine;
pub mod harness;
pub mod nats;
pub mod output;
pub mod queue;
pub mod registry;
pub mod runtime;
pub mod session;
pub mod store;
pub mod voice;

#[cfg(test)]
pub(crate) mod test_support;

pub use app::{MoniApp, MoniAppConfig, run_cron_loop};
pub use commands::{CommandAction, ParsedCommand, parse_command};
pub use cron::{CronEngine, CronTask, CronTaskStatus};
pub use discord::{
    ChannelBinding, DiscordBotConfig, DiscordInboundMessage, MoniDiscordHandler,
    parse_channel_bindings, parse_channel_create_intent, run_discord_bot,
};
pub use engine::{AgentEngine, AgentProtocol, EngineConfig};
pub use harness::{
    AgentEvent, AgentEventStream, AgentHarness, AgentHarnessStatus, EventStreamKind,
    ProcessAgentHarness, StopReason,
};
pub use nats::NatsNamespaceQueue;
pub use output::{
    DiscordOutputSink, DiscordTypingTracker, InMemoryOutputSink, OutputMessage, OutputSink,
};
pub use queue::{
    InMemoryNamespaceQueue, NamespaceQueue, QueuedPrompt, subject_for_namespace_input,
};
pub use registry::BindingRegistry;
pub use session::{EngineConfigResolver, SessionManager, StaticEngineConfigResolver};
pub use store::{FileStateStore, MoniState, StateStore};
pub use voice::{VoiceTranscriber, build_voice_prompt, is_audio_attachment};
