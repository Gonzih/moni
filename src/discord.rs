use std::{collections::HashSet, future::Future, sync::Arc};

use futures_util::TryFutureExt;
use serde::{Deserialize, Serialize};
use serenity::{
    async_trait as serenity_async_trait,
    builder::{
        CreateChannel, CreateCommand, CreateCommandOption, CreateInteractionResponse,
        CreateInteractionResponseMessage,
    },
    client::ClientBuilder,
    http::Http,
    http::HttpBuilder,
    model::{
        application::{
            Command as ApplicationCommand, CommandDataOption, CommandDataOptionValue,
            CommandInteraction, CommandOptionType, Interaction,
        },
        channel::{ChannelType, Message},
        gateway::{GatewayIntents, Ready},
        id::{ChannelId, GuildId},
    },
    prelude::*,
};

use crate::app::MoniApp;
use crate::commands::{CommandAction, parse_command};
use crate::output::DiscordTypingTracker;
use crate::queue::{NamespaceQueue, QueuedPrompt};
use crate::registry::BindingRegistry;
use crate::voice::{VoiceTranscriber, is_audio_attachment};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelBinding {
    pub channel_id: String,
    pub namespace: String,
    pub repo_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscordInboundMessage {
    pub channel_id: String,
    pub author_id: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelCreateIntent {
    pub namespace: String,
    pub repo_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscordAttachmentInput {
    filename: String,
    content_type: Option<String>,
    url: String,
    size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscordMessageInput {
    channel_id: ChannelId,
    guild_id: Option<GuildId>,
    author_id: String,
    author_bot: bool,
    content: String,
    attachments: Vec<DiscordAttachmentInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscordSlashCommandInput {
    channel_id: ChannelId,
    guild_id: Option<GuildId>,
    author_id: String,
    name: String,
    options: Vec<DiscordSlashCommandOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscordSlashCommandOption {
    name: String,
    value: DiscordSlashCommandOptionValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DiscordSlashCommandOptionValue {
    String(String),
    SubCommand(Vec<DiscordSlashCommandOption>),
}

pub(crate) fn route_discord_message<'a>(
    queue: &'a dyn NamespaceQueue,
    binding: &'a ChannelBinding,
    message: DiscordInboundMessage,
) -> impl Future<Output = anyhow::Result<QueuedPrompt>> + Send + 'a {
    let prompt = QueuedPrompt::new(
        binding.namespace.clone(),
        Some(binding.repo_url.clone()),
        message.body,
        format!("discord:{}", message.channel_id),
    );
    queue.enqueue(prompt.clone()).map_ok(move |()| prompt)
}

#[derive(Debug, Clone)]
pub struct DiscordBotConfig {
    pub token: String,
    pub bindings: Vec<ChannelBinding>,
    pub allowed_user_ids: Vec<String>,
    pub default_category_id: Option<ChannelId>,
    pub voice_transcriber: Option<VoiceTranscriber>,
    pub slash_guild_ids: Vec<GuildId>,
    gateway_proxy_url: Option<String>,
}

impl DiscordBotConfig {
    pub fn new(token: impl Into<String>, bindings: Vec<ChannelBinding>) -> anyhow::Result<Self> {
        for binding in &bindings {
            binding.channel_id.parse::<u64>()?;
        }
        Ok(Self {
            token: token.into(),
            bindings,
            allowed_user_ids: Vec::new(),
            default_category_id: None,
            voice_transcriber: None,
            slash_guild_ids: Vec::new(),
            gateway_proxy_url: None,
        })
    }

    pub fn with_allowed_user_ids(mut self, allowed_user_ids: Vec<String>) -> anyhow::Result<Self> {
        for user_id in &allowed_user_ids {
            user_id.parse::<u64>()?;
        }
        self.allowed_user_ids = allowed_user_ids;
        Ok(self)
    }

    pub fn with_default_category_id(
        mut self,
        default_category_id: Option<String>,
    ) -> anyhow::Result<Self> {
        self.default_category_id = default_category_id
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.trim().parse::<u64>().map(ChannelId::new))
            .transpose()?;
        Ok(self)
    }

    pub fn with_voice_transcriber(mut self, voice_transcriber: Option<VoiceTranscriber>) -> Self {
        self.voice_transcriber = voice_transcriber;
        self
    }

    pub fn with_slash_guild_ids(mut self, slash_guild_ids: Vec<String>) -> anyhow::Result<Self> {
        self.slash_guild_ids = slash_guild_ids
            .into_iter()
            .map(|guild_id| guild_id.parse::<u64>().map(GuildId::new))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self)
    }

    pub fn with_gateway_proxy_url(mut self, gateway_proxy_url: Option<String>) -> Self {
        self.gateway_proxy_url = gateway_proxy_url;
        self
    }
}

pub struct MoniDiscordHandler {
    app: Arc<MoniApp>,
    registry: BindingRegistry,
    allowed_user_ids: HashSet<String>,
    typing: DiscordTypingTracker,
    default_category_id: Option<ChannelId>,
    voice_transcriber: Option<VoiceTranscriber>,
    slash_guild_ids: Vec<GuildId>,
}

#[serenity_async_trait]
trait DiscordActions: Send + Sync {
    async fn say(&self, channel_id: ChannelId, body: String) -> anyhow::Result<()>;

    async fn create_text_channel(
        &self,
        guild_id: GuildId,
        name: String,
        topic: String,
        category_id: Option<ChannelId>,
    ) -> anyhow::Result<ChannelId>;

    async fn start_typing(&self, namespace: String, channel_id: ChannelId) -> anyhow::Result<()>;

    async fn stop_typing(&self, namespace: &str) -> anyhow::Result<()>;
}

struct SerenityDiscordActions<'a> {
    http: Arc<Http>,
    typing: &'a DiscordTypingTracker,
}

#[serenity_async_trait]
impl DiscordActions for SerenityDiscordActions<'_> {
    async fn say(&self, channel_id: ChannelId, body: String) -> anyhow::Result<()> {
        channel_id.say(&self.http, body).await?;
        Ok(())
    }

    async fn create_text_channel(
        &self,
        guild_id: GuildId,
        name: String,
        topic: String,
        category_id: Option<ChannelId>,
    ) -> anyhow::Result<ChannelId> {
        let mut builder = CreateChannel::new(name)
            .kind(ChannelType::Text)
            .topic(topic);
        if let Some(category_id) = category_id {
            builder = builder.category(category_id);
        }
        Ok(guild_id.create_channel(&self.http, builder).await?.id)
    }

    async fn start_typing(&self, namespace: String, channel_id: ChannelId) -> anyhow::Result<()> {
        self.typing.start(&namespace, channel_id, &self.http).await;
        Ok(())
    }

    async fn stop_typing(&self, namespace: &str) -> anyhow::Result<()> {
        self.typing.stop(namespace).await;
        Ok(())
    }
}

#[serenity_async_trait]
trait DiscordSlashCommandRegistrar: Send + Sync {
    async fn register_slash_commands(
        &self,
        guild_ids: &[GuildId],
        commands: Vec<CreateCommand>,
    ) -> anyhow::Result<()>;
}

struct SerenitySlashCommandRegistrar {
    http: Arc<Http>,
}

#[serenity_async_trait]
impl DiscordSlashCommandRegistrar for SerenitySlashCommandRegistrar {
    async fn register_slash_commands(
        &self,
        guild_ids: &[GuildId],
        commands: Vec<CreateCommand>,
    ) -> anyhow::Result<()> {
        if guild_ids.is_empty() {
            ApplicationCommand::set_global_commands(&self.http, commands).await?;
            return Ok(());
        }

        for guild_id in guild_ids {
            guild_id.set_commands(&self.http, commands.clone()).await?;
        }
        Ok(())
    }
}

#[serenity_async_trait]
pub(crate) trait DiscordGateway: Send + Sync {
    async fn start(
        &self,
        token: String,
        intents: GatewayIntents,
        handler: MoniDiscordHandler,
    ) -> anyhow::Result<()>;
}

pub(crate) struct SerenityDiscordGateway {
    proxy_url: Option<String>,
}

impl SerenityDiscordGateway {
    pub(crate) fn new(proxy_url: Option<String>) -> Self {
        Self { proxy_url }
    }
}

#[serenity_async_trait]
impl DiscordGateway for SerenityDiscordGateway {
    async fn start(
        &self,
        token: String,
        intents: GatewayIntents,
        handler: MoniDiscordHandler,
    ) -> anyhow::Result<()> {
        let mut client = serenity_client_builder(&token, intents, self.proxy_url.as_deref())
            .event_handler(handler)
            .await?;
        client.start().await.map_err(Into::into)
    }
}

fn serenity_client_builder(
    token: &str,
    intents: GatewayIntents,
    proxy_url: Option<&str>,
) -> ClientBuilder {
    let mut http_builder = HttpBuilder::new(token);
    if let Some(proxy_url) = proxy_url {
        http_builder = http_builder.proxy(proxy_url).ratelimiter_disabled(true);
    }
    ClientBuilder::new_with_http(http_builder.build(), intents)
}

fn moni_slash_commands() -> Vec<CreateCommand> {
    vec![
        CreateCommand::new("reset").description("Reset the agent session for this channel"),
        CreateCommand::new("clear").description("Clear the agent session for this channel"),
        CreateCommand::new("compact").description("Compact the agent session for this channel"),
        CreateCommand::new("status").description("Show Moni status for this channel"),
        CreateCommand::new("model")
            .description("Select the agent model for this channel")
            .add_option(required_string_option("model", "Model name, e.g. prompt")),
        CreateCommand::new("register")
            .description("Register this channel to a namespace and repo")
            .add_option(required_string_option(
                "namespace",
                "Namespace to route this channel to",
            ))
            .add_option(required_string_option(
                "repo",
                "Repository URL for the namespace",
            )),
        CreateCommand::new("channel")
            .description("Create a Discord channel for a GitHub repo")
            .add_option(required_string_option(
                "repo",
                "GitHub repo URL, e.g. https://github.com/org/repo",
            )),
        CreateCommand::new("voice")
            .description("Inspect voice transcription setup")
            .add_option(subcommand_option(
                "status",
                "Show voice transcription health",
            )),
        CreateCommand::new("cron")
            .description("Manage cron jobs for this channel")
            .add_option(subcommand_option("list", "List cron jobs"))
            .add_option(
                subcommand_option("add", "Add a cron job")
                    .add_sub_option(required_string_option(
                        "schedule",
                        "Five-field cron expression, e.g. 0 * * * *",
                    ))
                    .add_sub_option(required_string_option(
                        "message",
                        "Message to enqueue when the cron fires",
                    )),
            )
            .add_option(
                subcommand_option("pause", "Pause a cron job")
                    .add_sub_option(required_string_option("id", "Cron ID")),
            )
            .add_option(
                subcommand_option("resume", "Resume a cron job")
                    .add_sub_option(required_string_option("id", "Cron ID")),
            )
            .add_option(
                subcommand_option("delete", "Delete a cron job")
                    .add_sub_option(required_string_option("id", "Cron ID")),
            ),
    ]
}

fn required_string_option(name: &str, description: &str) -> CreateCommandOption {
    CreateCommandOption::new(CommandOptionType::String, name, description).required(true)
}

fn subcommand_option(name: &str, description: &str) -> CreateCommandOption {
    CreateCommandOption::new(CommandOptionType::SubCommand, name, description)
}

fn discord_slash_command_input(command: &CommandInteraction) -> DiscordSlashCommandInput {
    DiscordSlashCommandInput {
        channel_id: command.channel_id,
        guild_id: command.guild_id,
        author_id: command.user.id.to_string(),
        name: command.data.name.clone(),
        options: command
            .data
            .options
            .iter()
            .filter_map(discord_slash_option)
            .collect(),
    }
}

fn discord_slash_option(option: &CommandDataOption) -> Option<DiscordSlashCommandOption> {
    let value = match &option.value {
        CommandDataOptionValue::String(value) => {
            DiscordSlashCommandOptionValue::String(value.clone())
        }
        CommandDataOptionValue::SubCommand(options) => DiscordSlashCommandOptionValue::SubCommand(
            options.iter().filter_map(discord_slash_option).collect(),
        ),
        _ => return None,
    };
    Some(DiscordSlashCommandOption {
        name: option.name.clone(),
        value,
    })
}

fn slash_command_body(input: &DiscordSlashCommandInput) -> anyhow::Result<Option<String>> {
    match input.name.as_str() {
        "reset" | "clear" | "compact" | "status" => Ok(Some(format!("/{}", input.name))),
        "model" => Ok(Some(format!(
            "/model {}",
            required_slash_string_option(&input.options, "model")?
        ))),
        "register" => Ok(Some(format!(
            "/register {} {}",
            required_slash_string_option(&input.options, "namespace")?,
            required_slash_string_option(&input.options, "repo")?
        ))),
        "cron" => Ok(Some(cron_slash_command_body(&input.options)?)),
        "voice" => Ok(Some(voice_slash_command_body(&input.options)?)),
        _ => Ok(None),
    }
}

fn voice_slash_command_body(options: &[DiscordSlashCommandOption]) -> anyhow::Result<String> {
    let Some((subcommand, _sub_options)) = slash_subcommand(options) else {
        anyhow::bail!("missing voice subcommand");
    };

    match subcommand {
        "status" => Ok("/voice status".to_string()),
        other => anyhow::bail!("unknown voice subcommand `{other}`"),
    }
}

fn cron_slash_command_body(options: &[DiscordSlashCommandOption]) -> anyhow::Result<String> {
    let Some((subcommand, sub_options)) = slash_subcommand(options) else {
        anyhow::bail!("missing cron subcommand");
    };

    match subcommand {
        "list" => Ok("/cron list".to_string()),
        "add" => Ok(format!(
            "/cron add {} {}",
            required_slash_string_option(sub_options, "schedule")?,
            required_slash_string_option(sub_options, "message")?
        )),
        "pause" | "resume" | "delete" => Ok(format!(
            "/cron {subcommand} {}",
            required_slash_string_option(sub_options, "id")?
        )),
        other => anyhow::bail!("unknown cron subcommand `{other}`"),
    }
}

fn slash_subcommand(
    options: &[DiscordSlashCommandOption],
) -> Option<(&str, &[DiscordSlashCommandOption])> {
    options.iter().find_map(|option| match &option.value {
        DiscordSlashCommandOptionValue::SubCommand(sub_options) => {
            Some((option.name.as_str(), sub_options.as_slice()))
        }
        DiscordSlashCommandOptionValue::String(_) => None,
    })
}

fn required_slash_string_option(
    options: &[DiscordSlashCommandOption],
    name: &str,
) -> anyhow::Result<String> {
    slash_string_option(options, name)
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("missing {name}"))
}

fn slash_string_option<'a>(
    options: &'a [DiscordSlashCommandOption],
    name: &str,
) -> Option<&'a str> {
    options.iter().find_map(|option| {
        if option.name != name {
            return None;
        }
        match &option.value {
            DiscordSlashCommandOptionValue::String(value) => Some(value.as_str()),
            DiscordSlashCommandOptionValue::SubCommand(_) => None,
        }
    })
}

fn ephemeral_interaction_response(body: impl Into<String>) -> CreateInteractionResponse {
    CreateInteractionResponse::Message(
        CreateInteractionResponseMessage::new()
            .content(body)
            .ephemeral(true),
    )
}

fn channel_created_message(binding: &ChannelBinding) -> String {
    format!(
        "Created <#{}> - messages there route to the {} meta-agent",
        binding.channel_id, binding.repo_url
    )
}

fn unconfigured_channel_message() -> String {
    "This channel is not configured. Use `channel for https://github.com/org/repo` to set it up."
        .to_string()
}

impl MoniDiscordHandler {
    pub fn new(
        app: Arc<MoniApp>,
        registry: BindingRegistry,
        allowed_user_ids: impl IntoIterator<Item = String>,
        typing: DiscordTypingTracker,
        default_category_id: Option<ChannelId>,
        voice_transcriber: Option<VoiceTranscriber>,
        slash_guild_ids: Vec<GuildId>,
    ) -> Self {
        Self {
            app,
            registry,
            allowed_user_ids: allowed_user_ids.into_iter().collect(),
            typing,
            default_category_id,
            voice_transcriber,
            slash_guild_ids,
        }
    }

    fn is_authorized(&self, author_id: &str) -> bool {
        self.allowed_user_ids.is_empty() || self.allowed_user_ids.contains(author_id)
    }

    async fn ready_with_registrar<R: DiscordSlashCommandRegistrar + ?Sized>(
        &self,
        ready: &Ready,
        registrar: &R,
    ) {
        tracing::info!(user = %ready.user.name, "moni discord gateway ready");
        if let Err(err) = registrar
            .register_slash_commands(&self.slash_guild_ids, moni_slash_commands())
            .await
        {
            tracing::error!(error = %err, "failed to register Discord slash commands");
        }
    }

    async fn create_channel_binding_with_actions<A: DiscordActions + ?Sized>(
        &self,
        actions: &A,
        guild_id: GuildId,
        intent: ChannelCreateIntent,
    ) -> anyhow::Result<ChannelBinding> {
        let channel_id = actions
            .create_text_channel(
                guild_id,
                discord_channel_name(&intent.namespace),
                format!("moni route for {}", intent.repo_url),
                self.default_category_id,
            )
            .await?;
        let binding = ChannelBinding {
            channel_id: channel_id.to_string(),
            namespace: intent.namespace,
            repo_url: intent.repo_url,
        };
        self.registry.upsert(binding.clone()).await?;
        self.app.register_binding(binding.clone()).await?;
        Ok(binding)
    }

    async fn handle_channel_create_intent_with_actions<A: DiscordActions + ?Sized>(
        &self,
        actions: &A,
        message: &DiscordMessageInput,
        intent: ChannelCreateIntent,
    ) -> anyhow::Result<()> {
        let Some(guild_id) = message.guild_id else {
            actions
                .say(
                    message.channel_id,
                    "channel creation only works in a server".to_string(),
                )
                .await?;
            return Ok(());
        };

        let binding = self
            .create_channel_binding_with_actions(actions, guild_id, intent)
            .await?;
        actions
            .say(message.channel_id, channel_created_message(&binding))
            .await?;
        Ok(())
    }

    async fn handle_slash_channel_command_with_actions<A: DiscordActions + ?Sized>(
        &self,
        actions: &A,
        input: &DiscordSlashCommandInput,
    ) -> anyhow::Result<String> {
        let repo_url = required_slash_string_option(&input.options, "repo")?;
        let intent = parse_channel_create_intent(&format!("channel for {repo_url}"))
            .ok_or_else(|| anyhow::anyhow!("invalid repo URL. Use: https://github.com/org/repo"))?;
        let Some(guild_id) = input.guild_id else {
            return Ok("channel creation only works in a server".to_string());
        };
        let binding = self
            .create_channel_binding_with_actions(actions, guild_id, intent)
            .await?;
        Ok(channel_created_message(&binding))
    }

    async fn handle_slash_command_with_actions<A: DiscordActions + ?Sized>(
        &self,
        actions: &A,
        input: DiscordSlashCommandInput,
    ) -> anyhow::Result<String> {
        if !self.is_authorized(&input.author_id) {
            tracing::warn!(
                author_id = %input.author_id,
                channel_id = %input.channel_id,
                command = %input.name,
                "ignored Discord slash command from unauthorized user"
            );
            return Ok("Not authorized.".to_string());
        }

        if input.name == "channel" {
            return self
                .handle_slash_channel_command_with_actions(actions, &input)
                .await;
        }

        let Some(body) = slash_command_body(&input)? else {
            return Ok("Unknown command.".to_string());
        };
        let preparsed =
            parse_command("", &body)?.expect("slash command body is a supported command");
        if matches!(preparsed.action, CommandAction::VoiceStatus) {
            return Ok(self.voice_status_message());
        }
        let is_register = matches!(preparsed.action, CommandAction::Register { .. });
        let binding = if is_register {
            ChannelBinding {
                channel_id: input.channel_id.to_string(),
                namespace: String::new(),
                repo_url: String::new(),
            }
        } else {
            let Some(binding) = self.registry.get_by_channel(input.channel_id).await else {
                return Ok(unconfigured_channel_message());
            };
            binding
        };
        let command = parse_command(binding.namespace.clone(), &body)?
            .expect("slash command body is a supported command");
        let registered_binding = match &command.action {
            CommandAction::Register {
                namespace,
                repo_url,
            } => Some(ChannelBinding {
                channel_id: input.channel_id.to_string(),
                namespace: namespace.clone(),
                repo_url: repo_url.clone(),
            }),
            _ => None,
        };
        let outcome = self
            .app
            .handle_command_action(&binding, command.action)
            .await?;
        if let Some(binding) = registered_binding {
            self.registry.upsert(binding).await?;
        }
        Ok(outcome.body)
    }

    fn voice_status_message(&self) -> String {
        self.voice_transcriber
            .as_ref()
            .map(VoiceTranscriber::status_report)
            .unwrap_or_else(|| {
                "voice transcription unavailable - whisper.cpp is not configured".to_string()
            })
    }

    async fn handle_voice_message_with_actions<A: DiscordActions + ?Sized>(
        &self,
        actions: &A,
        message: &DiscordMessageInput,
        binding: &ChannelBinding,
    ) -> anyhow::Result<bool> {
        let Some(attachment) = message.attachments.iter().find(|attachment| {
            is_audio_attachment(&attachment.filename, attachment.content_type.as_deref())
        }) else {
            return Ok(false);
        };

        actions
            .start_typing(binding.namespace.clone(), message.channel_id)
            .await?;

        let Some(transcriber) = &self.voice_transcriber else {
            actions.stop_typing(&binding.namespace).await?;
            actions
                .say(
                    message.channel_id,
                    "Voice transcription unavailable - whisper.cpp is not configured".to_string(),
                )
                .await?;
            return Ok(true);
        };

        transcriber.validate_attachment_size(attachment.size)?;
        let transcript = transcriber.transcribe_url(&attachment.url).await?;
        if transcript == "[empty transcription]" {
            actions.stop_typing(&binding.namespace).await?;
            actions
                .say(
                    message.channel_id,
                    "Could not transcribe voice message.".to_string(),
                )
                .await?;
            return Ok(true);
        }

        let caption = message
            .content
            .replace(|ch: char| ch == '\n' || ch == '\r', " ");
        let prompt = transcriber.build_prompt(&strip_discord_mentions(&caption), &transcript);
        let inbound = DiscordInboundMessage {
            channel_id: message.channel_id.to_string(),
            author_id: message.author_id.clone(),
            body: prompt,
        };
        self.app.handle_discord_message(binding, inbound).await?;
        Ok(true)
    }

    async fn handle_message_with_actions<A: DiscordActions + ?Sized>(
        &self,
        actions: &A,
        message: DiscordMessageInput,
    ) {
        if message.author_bot {
            return;
        }
        if !self.is_authorized(&message.author_id) {
            tracing::warn!(
                author_id = %message.author_id,
                channel_id = %message.channel_id,
                "ignored Discord message from unauthorized user"
            );
            return;
        }

        if let Some(intent) = parse_channel_create_intent(&message.content) {
            if let Err(err) = self
                .handle_channel_create_intent_with_actions(actions, &message, intent)
                .await
            {
                tracing::error!(channel_id = %message.channel_id, error = %err, "failed to create Discord channel binding");
                let _ = actions
                    .say(
                        message.channel_id,
                        format!("channel creation failed: {err}"),
                    )
                    .await;
            }
            return;
        }

        let inbound = DiscordInboundMessage {
            channel_id: message.channel_id.to_string(),
            author_id: message.author_id.clone(),
            body: message.content.clone(),
        };

        let Some(binding) = self.registry.get_by_channel(message.channel_id).await else {
            if let Err(err) = self
                .app
                .handle_unbound_discord_message(message.channel_id.to_string(), inbound)
                .await
            {
                tracing::error!(channel_id = %message.channel_id, error = %err, "failed to handle unbound discord message");
            } else {
                tracing::info!(channel_id = %message.channel_id, "ignored unbound Discord message");
            }
            return;
        };

        tracing::info!(
            channel_id = %binding.channel_id,
            namespace = %binding.namespace,
            "routing Discord message"
        );
        match parse_command(binding.namespace.clone(), &message.content) {
            Ok(Some(command)) if matches!(command.action, CommandAction::VoiceStatus) => {
                if let Err(err) = actions
                    .say(message.channel_id, self.voice_status_message())
                    .await
                {
                    tracing::error!(channel_id = %binding.channel_id, namespace = %binding.namespace, error = %err, "failed to report voice status");
                }
                return;
            }
            Ok(_) => {}
            Err(_) => {}
        }
        match self
            .handle_voice_message_with_actions(actions, &message, &binding)
            .await
        {
            Ok(true) => return,
            Ok(false) => {}
            Err(err) => {
                let _ = actions.stop_typing(&binding.namespace).await;
                let user_message = voice_error_message(&err);
                let _ = actions.say(message.channel_id, user_message).await;
                tracing::error!(channel_id = %binding.channel_id, namespace = %binding.namespace, error = %err, "failed to transcribe discord voice message");
                return;
            }
        }
        let _ = actions
            .start_typing(binding.namespace.clone(), message.channel_id)
            .await;
        if let Err(err) = self.app.handle_discord_message(&binding, inbound).await {
            let _ = actions.stop_typing(&binding.namespace).await;
            tracing::error!(channel_id = %binding.channel_id, namespace = %binding.namespace, error = %err, "failed to route discord message");
        }
    }
}

#[serenity_async_trait]
impl EventHandler for MoniDiscordHandler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        ctx.http.set_application_id(ready.application.id);
        let registrar = SerenitySlashCommandRegistrar {
            http: ctx.http.clone(),
        };
        self.ready_with_registrar(&ready, &registrar).await;
    }

    async fn message(&self, ctx: Context, message: Message) {
        let actions = SerenityDiscordActions {
            http: ctx.http.clone(),
            typing: &self.typing,
        };
        self.handle_message_with_actions(&actions, discord_message_input(&message))
            .await;
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        let Interaction::Command(command) = interaction else {
            return;
        };
        let actions = SerenityDiscordActions {
            http: ctx.http.clone(),
            typing: &self.typing,
        };
        let response_body = match self
            .handle_slash_command_with_actions(&actions, discord_slash_command_input(&command))
            .await
        {
            Ok(body) => body,
            Err(err) => {
                tracing::error!(error = %err, "failed to handle Discord slash command");
                format!("Command failed: {err}")
            }
        };
        if let Err(err) = command
            .create_response(&ctx.http, ephemeral_interaction_response(response_body))
            .await
        {
            tracing::error!(error = %err, "failed to respond to Discord slash command");
        }
    }
}

fn discord_message_input(message: &Message) -> DiscordMessageInput {
    DiscordMessageInput {
        channel_id: message.channel_id,
        guild_id: message.guild_id,
        author_id: message.author.id.to_string(),
        author_bot: message.author.bot,
        content: message.content.clone(),
        attachments: message
            .attachments
            .iter()
            .map(|attachment| DiscordAttachmentInput {
                filename: attachment.filename.clone(),
                content_type: attachment.content_type.clone(),
                url: attachment.url.clone(),
                size: Some(u64::from(attachment.size)),
            })
            .collect(),
    }
}

pub fn run_discord_bot(
    config: DiscordBotConfig,
    app: Arc<MoniApp>,
    registry: BindingRegistry,
    typing: DiscordTypingTracker,
) -> impl Future<Output = anyhow::Result<()>> + Send {
    let gateway = SerenityDiscordGateway::new(config.gateway_proxy_url.clone());
    async move { run_discord_bot_with_gateway(config, app, registry, typing, &gateway).await }
}

pub(crate) async fn run_discord_bot_with_gateway<G: DiscordGateway + ?Sized>(
    config: DiscordBotConfig,
    app: Arc<MoniApp>,
    registry: BindingRegistry,
    typing: DiscordTypingTracker,
    gateway: &G,
) -> anyhow::Result<()> {
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;
    let handler = MoniDiscordHandler::new(
        app,
        registry,
        config.allowed_user_ids,
        typing,
        config.default_category_id,
        config.voice_transcriber,
        config.slash_guild_ids,
    );
    gateway.start(config.token, intents, handler).await
}

pub fn parse_channel_bindings(input: &str) -> anyhow::Result<Vec<ChannelBinding>> {
    let mut bindings = Vec::new();
    for raw in input
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let parts = raw.splitn(3, '=').collect::<Vec<_>>();
        if parts.len() != 3 {
            anyhow::bail!(
                "invalid channel binding `{raw}`; expected channel_id=namespace=repo_url"
            );
        }
        bindings.push(ChannelBinding {
            channel_id: parts[0].to_string(),
            namespace: parts[1].to_string(),
            repo_url: parts[2].to_string(),
        });
    }
    Ok(bindings)
}

pub fn parse_channel_create_intent(text: &str) -> Option<ChannelCreateIntent> {
    let lower = text.to_ascii_lowercase();
    let marker = "channel for ";
    let marker_index = lower.find(marker)?;
    let url_start = marker_index + marker.len();
    let rest = text.get(url_start..)?.trim_start();
    let url = rest
        .split_whitespace()
        .next()?
        .trim_end_matches(|ch: char| matches!(ch, '.' | ',' | ')' | ']' | '>'));
    let repo_path = url
        .strip_prefix("https://github.com/")
        .or_else(|| url.strip_prefix("http://github.com/"))?;
    let mut segments = repo_path.split('/');
    let owner = segments.next()?;
    let repo = segments.next()?;
    if owner.is_empty() || repo.is_empty() || !is_github_segment(owner) || !is_github_segment(repo)
    {
        return None;
    }
    Some(ChannelCreateIntent {
        namespace: repo.to_string(),
        repo_url: format!("{}{}", &url[..url.len() - repo_path.len()], owner) + "/" + repo,
    })
}

fn is_github_segment(segment: &str) -> bool {
    segment
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-'))
}

fn discord_channel_name(namespace: &str) -> String {
    let mut name = namespace
        .to_ascii_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    while name.contains("--") {
        name = name.replace("--", "-");
    }
    let name = name.trim_matches('-');
    if name.len() >= 2 {
        name.to_string()
    } else {
        "moni-channel".to_string()
    }
}

fn strip_discord_mentions(text: &str) -> String {
    text.split_whitespace()
        .filter(|word| {
            !(word.starts_with("<@")
                && word.ends_with('>')
                && word
                    .trim_start_matches("<@!")
                    .trim_start_matches("<@")
                    .trim_end_matches('>')
                    .chars()
                    .all(|ch| ch.is_ascii_digit()))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn voice_error_message(err: &anyhow::Error) -> String {
    let message = err.to_string();
    if message.contains("whisper-cpp not found") {
        "Voice transcription unavailable - whisper.cpp is not installed".to_string()
    } else if message.contains("No whisper model found") {
        "Voice transcription unavailable - no whisper model found".to_string()
    } else if message.contains("ffmpeg not found") {
        "Voice transcription unavailable - ffmpeg is not installed".to_string()
    } else {
        format!("Voice transcription failed: {message}")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        net::TcpListener,
        os::unix::fs::PermissionsExt,
        path::Path,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;
    use crate::queue::InMemoryNamespaceQueue;
    use crate::test_support::DiscordHttpProxy;
    use futures::channel::mpsc;
    use serde_json::json;
    use serenity::gateway::{ShardMessenger, ShardRunnerMessage};
    use serenity::http::HttpBuilder;
    use serenity::model::id::{ApplicationId, ShardId};
    use serenity::prelude::TypeMap;
    use tempfile::TempDir;

    type TestDrainResult = anyhow::Result<Vec<QueuedPrompt>>;

    #[derive(Default, Clone)]
    struct FakeDiscordActions {
        state: Arc<tokio::sync::Mutex<FakeDiscordActionState>>,
    }

    #[derive(Default, Clone)]
    struct FakeDiscordActionState {
        says: Vec<(ChannelId, String)>,
        creates: Vec<(GuildId, String, String, Option<ChannelId>)>,
        typing_started: Vec<(String, ChannelId)>,
        typing_stopped: Vec<String>,
        fail_say: bool,
        fail_create: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct GatewayCall {
        token: String,
        intents: GatewayIntents,
        authorizes_allowed_user: bool,
        authorizes_other_user: bool,
        default_category_id: Option<ChannelId>,
        has_voice_transcriber: bool,
        slash_guild_ids: Vec<GuildId>,
    }

    #[derive(Default)]
    struct FakeDiscordGateway {
        calls: Arc<Mutex<Vec<GatewayCall>>>,
    }

    struct FailingNamespaceQueue;

    #[derive(Default)]
    struct FakeSlashCommandRegistrar {
        calls: Arc<Mutex<Vec<(Vec<GuildId>, serde_json::Value)>>>,
        fail: bool,
    }

    impl FakeDiscordActions {
        async fn state(&self) -> FakeDiscordActionState {
            self.state.lock().await.clone()
        }

        async fn fail_creates(&self) {
            self.state.lock().await.fail_create = true;
        }

        async fn fail_says(&self) {
            self.state.lock().await.fail_say = true;
        }
    }

    fn write_script(path: &Path, body: &str) {
        fs::write(path, body).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    fn voice_transcriber(dir: &TempDir, curl_body: &str, whisper_body: &str) -> VoiceTranscriber {
        let curl = dir.path().join("curl");
        let ffmpeg = dir.path().join("ffmpeg");
        let whisper = dir.path().join("whisper-cli");
        let model = dir.path().join("ggml-small.en.bin");
        fs::write(&model, "model").unwrap();
        write_script(&curl, curl_body);
        write_script(
            &ffmpeg,
            r#"#!/bin/sh
out=""
for arg in "$@"; do out="$arg"; done
printf wav > "$out"
"#,
        );
        write_script(&whisper, whisper_body);
        VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path())
    }

    fn app_for_bindings(
        bindings: Vec<ChannelBinding>,
    ) -> (Arc<MoniApp>, BindingRegistry, InMemoryNamespaceQueue) {
        let queue = InMemoryNamespaceQueue::default();
        let output = crate::InMemoryOutputSink::default();
        let registry = BindingRegistry::new(bindings).unwrap();
        let app = Arc::new(MoniApp::new(crate::app::MoniAppConfig {
            queue: Arc::new(queue.clone()),
            sessions: Arc::new(crate::SessionManager::new(
                std::path::PathBuf::from("/tmp/moni-test"),
                Arc::new(crate::StaticEngineConfigResolver::new(
                    crate::EngineConfig::new(crate::AgentEngine::Codex, "/bin/cat"),
                )),
                Arc::new(output.clone()),
            )),
            output: Arc::new(output),
            cron: crate::CronEngine::new(Vec::new()),
            registry: registry.clone(),
            state_store: None,
            voice_status: None,
        }));
        (app, registry, queue)
    }

    fn serenity_context(http: Arc<Http>) -> Context {
        let (tx, _rx) = mpsc::unbounded::<ShardRunnerMessage>();
        let shard = unsafe {
            // Serenity exposes ShardMessenger construction only through ShardRunner, which would
            // require a real websocket shard. With the collector feature disabled, ShardMessenger
            // is a single sender field; this keeps the EventHandler wrapper test local.
            std::mem::transmute::<_, ShardMessenger>(tx)
        };
        Context {
            data: Arc::new(tokio::sync::RwLock::new(TypeMap::new())),
            shard,
            shard_id: ShardId(0),
            http,
        }
    }

    fn serenity_message(channel_id: u64, author_id: &str, content: &str) -> Message {
        serde_json::from_value(json!({
            "id": "111",
            "channel_id": channel_id.to_string(),
            "author": {
                "id": author_id,
                "username": "feral",
                "discriminator": "0001",
                "global_name": null,
                "avatar": null,
                "bot": false,
                "system": false,
                "mfa_enabled": false,
                "banner": null,
                "accent_color": null,
                "locale": null,
                "verified": null,
                "email": null,
                "flags": 0,
                "premium_type": 0,
                "public_flags": 0
            },
            "content": content,
            "timestamp": "2020-01-01T00:00:00.000000+00:00",
            "edited_timestamp": null,
            "tts": false,
            "mention_everyone": false,
            "mentions": [],
            "mention_roles": [],
            "attachments": [],
            "embeds": [],
            "pinned": false,
            "type": 0
        }))
        .unwrap()
    }

    fn ready_event(user_name: &str) -> Ready {
        serde_json::from_value(json!({
            "v": 10,
            "user": {
                "id": "42",
                "username": user_name,
                "discriminator": "0001",
                "global_name": null,
                "avatar": null,
                "bot": true,
                "system": false,
                "mfa_enabled": false,
                "banner": null,
                "accent_color": null,
                "locale": null,
                "verified": null,
                "email": null,
                "flags": 0,
                "premium_type": 0,
                "public_flags": 0
            },
            "guilds": [],
            "session_id": "session",
            "resume_gateway_url": "wss://gateway.discord.test",
            "application": {
                "id": "123",
                "flags": 0
            }
        }))
        .unwrap()
    }

    fn slash_input(
        channel_id: u64,
        author_id: &str,
        name: &str,
        options: Vec<DiscordSlashCommandOption>,
    ) -> DiscordSlashCommandInput {
        DiscordSlashCommandInput {
            channel_id: ChannelId::new(channel_id),
            guild_id: None,
            author_id: author_id.to_string(),
            name: name.to_string(),
            options,
        }
    }

    fn slash_string_option(name: &str, value: &str) -> DiscordSlashCommandOption {
        DiscordSlashCommandOption {
            name: name.to_string(),
            value: DiscordSlashCommandOptionValue::String(value.to_string()),
        }
    }

    fn slash_subcommand_option(
        name: &str,
        options: Vec<DiscordSlashCommandOption>,
    ) -> DiscordSlashCommandOption {
        DiscordSlashCommandOption {
            name: name.to_string(),
            value: DiscordSlashCommandOptionValue::SubCommand(options),
        }
    }

    fn command_option_json(name: &str, value: &str) -> serde_json::Value {
        json!({
            "name": name,
            "type": 3,
            "value": value
        })
    }

    fn boolean_option_json(name: &str, value: bool) -> serde_json::Value {
        json!({
            "name": name,
            "type": 5,
            "value": value
        })
    }

    fn subcommand_option_json(name: &str, options: Vec<serde_json::Value>) -> serde_json::Value {
        json!({
            "name": name,
            "type": 1,
            "options": options
        })
    }

    fn slash_interaction_with_id(
        interaction_id: u64,
        channel_id: u64,
        guild_id: Option<u64>,
        author_id: &str,
        name: &str,
        options: Vec<serde_json::Value>,
    ) -> Interaction {
        serde_json::from_value(json!({
            "id": interaction_id.to_string(),
            "application_id": "123",
            "type": 2,
            "data": {
                "id": "900",
                "name": name,
                "type": 1,
                "options": options
            },
            "guild_id": guild_id.map(|id| id.to_string()),
            "channel": null,
            "channel_id": channel_id.to_string(),
            "member": null,
            "user": {
                "id": author_id,
                "username": "feral",
                "discriminator": "0001",
                "global_name": null,
                "avatar": null,
                "bot": false,
                "system": false,
                "mfa_enabled": false,
                "banner": null,
                "accent_color": null,
                "locale": null,
                "verified": null,
                "email": null,
                "flags": 0,
                "premium_type": 0,
                "public_flags": 0
            },
            "token": "interaction-token",
            "version": 1,
            "app_permissions": null,
            "locale": "en-US",
            "guild_locale": null,
            "entitlements": [],
            "context": null,
            "attachment_size_limit": 8388608
        }))
        .unwrap()
    }

    #[test]
    fn discord_message_input_captures_attachment_size() {
        let mut message = serenity_message(123, "42", "voice");
        message.attachments.push(
            serde_json::from_value(json!({
                "id": "222",
                "filename": "voice.ogg",
                "description": null,
                "height": null,
                "proxy_url": "https://media.discordapp.net/voice.ogg",
                "size": 12345,
                "url": "https://cdn.discordapp.com/voice.ogg",
                "width": null,
                "content_type": "audio/ogg",
                "ephemeral": false,
                "duration_secs": 12.0,
                "waveform": null
            }))
            .unwrap(),
        );

        let input = discord_message_input(&message);

        assert_eq!(input.attachments.len(), 1);
        assert_eq!(input.attachments[0].filename, "voice.ogg");
        assert_eq!(
            input.attachments[0].content_type.as_deref(),
            Some("audio/ogg")
        );
        assert_eq!(
            input.attachments[0].url,
            "https://cdn.discordapp.com/voice.ogg"
        );
        assert_eq!(input.attachments[0].size, Some(12345));
    }

    fn slash_interaction(
        channel_id: u64,
        guild_id: Option<u64>,
        author_id: &str,
        name: &str,
        options: Vec<serde_json::Value>,
    ) -> Interaction {
        slash_interaction_with_id(500, channel_id, guild_id, author_id, name, options)
    }

    fn slash_command(
        channel_id: u64,
        guild_id: Option<u64>,
        author_id: &str,
        name: &str,
        options: Vec<serde_json::Value>,
    ) -> CommandInteraction {
        slash_interaction(channel_id, guild_id, author_id, name, options)
            .command()
            .unwrap()
    }

    fn ping_interaction() -> Interaction {
        serde_json::from_value(json!({
            "id": "501",
            "application_id": "123",
            "type": 1,
            "token": "interaction-token",
            "version": 1
        }))
        .unwrap()
    }

    #[serenity_async_trait]
    impl DiscordActions for FakeDiscordActions {
        async fn say(&self, channel_id: ChannelId, body: String) -> anyhow::Result<()> {
            let mut state = self.state.lock().await;
            if state.fail_say {
                anyhow::bail!("say failed");
            }
            state.says.push((channel_id, body));
            Ok(())
        }

        async fn create_text_channel(
            &self,
            guild_id: GuildId,
            name: String,
            topic: String,
            category_id: Option<ChannelId>,
        ) -> anyhow::Result<ChannelId> {
            let mut state = self.state.lock().await;
            if state.fail_create {
                anyhow::bail!("create failed");
            }
            state.creates.push((guild_id, name, topic, category_id));
            Ok(ChannelId::new(999))
        }

        async fn start_typing(
            &self,
            namespace: String,
            channel_id: ChannelId,
        ) -> anyhow::Result<()> {
            self.state
                .lock()
                .await
                .typing_started
                .push((namespace, channel_id));
            Ok(())
        }

        async fn stop_typing(&self, namespace: &str) -> anyhow::Result<()> {
            self.state
                .lock()
                .await
                .typing_stopped
                .push(namespace.to_string());
            Ok(())
        }
    }

    #[serenity_async_trait]
    impl DiscordGateway for FakeDiscordGateway {
        async fn start(
            &self,
            token: String,
            intents: GatewayIntents,
            handler: MoniDiscordHandler,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(GatewayCall {
                token,
                intents,
                authorizes_allowed_user: handler.is_authorized("42"),
                authorizes_other_user: handler.is_authorized("7"),
                default_category_id: handler.default_category_id,
                has_voice_transcriber: handler.voice_transcriber.is_some(),
                slash_guild_ids: handler.slash_guild_ids.clone(),
            });
            Ok(())
        }
    }

    #[serenity_async_trait]
    impl DiscordSlashCommandRegistrar for FakeSlashCommandRegistrar {
        async fn register_slash_commands(
            &self,
            guild_ids: &[GuildId],
            commands: Vec<CreateCommand>,
        ) -> anyhow::Result<()> {
            if self.fail {
                anyhow::bail!("register failed");
            }
            self.calls
                .lock()
                .unwrap()
                .push((guild_ids.to_vec(), serde_json::to_value(commands).unwrap()));
            Ok(())
        }
    }

    #[serenity_async_trait]
    impl NamespaceQueue for FailingNamespaceQueue {
        async fn enqueue(&self, _prompt: QueuedPrompt) -> anyhow::Result<()> {
            anyhow::bail!("enqueue failed");
        }

        async fn drain_namespace(&self, _namespace: &str) -> TestDrainResult {
            anyhow::bail!("drain failed");
        }
    }

    fn discord_input(channel_id: u64, author_id: &str, content: &str) -> DiscordMessageInput {
        DiscordMessageInput {
            channel_id: ChannelId::new(channel_id),
            guild_id: None,
            author_id: author_id.to_string(),
            author_bot: false,
            content: content.to_string(),
            attachments: Vec::new(),
        }
    }

    fn binding_for_channel(channel_id: u64) -> ChannelBinding {
        ChannelBinding {
            channel_id: channel_id.to_string(),
            namespace: "moni".to_string(),
            repo_url: "https://github.com/Gonzih/moni".to_string(),
        }
    }

    fn handler_with_bindings(
        bindings: Vec<ChannelBinding>,
        allowed_users: Vec<String>,
        voice_transcriber: Option<VoiceTranscriber>,
    ) -> (MoniDiscordHandler, InMemoryNamespaceQueue) {
        let (handler, queue, _) = handler_with_output(bindings, allowed_users, voice_transcriber);
        (handler, queue)
    }

    fn handler_with_output(
        bindings: Vec<ChannelBinding>,
        allowed_users: Vec<String>,
        voice_transcriber: Option<VoiceTranscriber>,
    ) -> (
        MoniDiscordHandler,
        InMemoryNamespaceQueue,
        crate::InMemoryOutputSink,
    ) {
        let queue = InMemoryNamespaceQueue::default();
        let output = crate::InMemoryOutputSink::default();
        let app = Arc::new(MoniApp::new(crate::app::MoniAppConfig {
            queue: Arc::new(queue.clone()),
            sessions: Arc::new(crate::SessionManager::new(
                std::path::PathBuf::from("/tmp/moni-test"),
                Arc::new(crate::StaticEngineConfigResolver::new(
                    crate::EngineConfig::new(crate::AgentEngine::Codex, "/bin/cat"),
                )),
                Arc::new(output.clone()),
            )),
            output: Arc::new(output.clone()),
            cron: crate::CronEngine::new(Vec::new()),
            registry: BindingRegistry::new(bindings.clone()).unwrap(),
            state_store: None,
            voice_status: None,
        }));
        let handler = MoniDiscordHandler::new(
            app,
            BindingRegistry::new(bindings).unwrap(),
            allowed_users,
            DiscordTypingTracker::default(),
            Some(ChannelId::new(777)),
            voice_transcriber,
            Vec::new(),
        );
        (handler, queue, output)
    }

    #[test]
    fn slash_command_definitions_match_moni_command_surface() {
        let commands = serde_json::to_value(moni_slash_commands()).unwrap();
        let names = commands
            .as_array()
            .unwrap()
            .iter()
            .map(|command| command["name"].as_str().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "reset", "clear", "compact", "status", "model", "register", "channel", "voice",
                "cron"
            ]
        );
        let model = commands
            .as_array()
            .unwrap()
            .iter()
            .find(|command| command["name"] == "model")
            .unwrap();
        assert_eq!(model["options"][0]["name"], "model");
        assert_eq!(model["options"][0]["required"], true);
        let cron = commands
            .as_array()
            .unwrap()
            .iter()
            .find(|command| command["name"] == "cron")
            .unwrap();
        let cron_subcommands = cron["options"]
            .as_array()
            .unwrap()
            .iter()
            .map(|option| option["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            cron_subcommands,
            vec!["list", "add", "pause", "resume", "delete"]
        );
        let voice = commands
            .as_array()
            .unwrap()
            .iter()
            .find(|command| command["name"] == "voice")
            .unwrap();
        assert_eq!(voice["options"][0]["name"], "status");
    }

    #[test]
    fn slash_command_body_maps_simple_model_register_and_cron_commands() {
        assert_eq!(
            slash_command_body(&slash_input(
                123,
                "42",
                "model",
                vec![slash_string_option("model", "prompt")]
            ))
            .unwrap(),
            Some("/model prompt".to_string())
        );
        assert_eq!(
            slash_command_body(&slash_input(
                123,
                "42",
                "register",
                vec![
                    slash_string_option("namespace", "moni"),
                    slash_string_option("repo", "https://github.com/Gonzih/moni"),
                ]
            ))
            .unwrap(),
            Some("/register moni https://github.com/Gonzih/moni".to_string())
        );
        assert_eq!(
            slash_command_body(&slash_input(123, "42", "reset", Vec::new())).unwrap(),
            Some("/reset".to_string())
        );
        assert_eq!(
            slash_command_body(&slash_input(123, "42", "clear", Vec::new())).unwrap(),
            Some("/clear".to_string())
        );
        assert_eq!(
            slash_command_body(&slash_input(123, "42", "compact", Vec::new())).unwrap(),
            Some("/compact".to_string())
        );
        assert_eq!(
            slash_command_body(&slash_input(123, "42", "status", Vec::new())).unwrap(),
            Some("/status".to_string())
        );
        assert_eq!(
            slash_command_body(&slash_input(123, "42", "unknown", Vec::new())).unwrap(),
            None
        );
    }

    #[test]
    fn slash_command_body_maps_voice_status_and_errors() {
        assert_eq!(
            voice_slash_command_body(&[slash_subcommand_option("status", Vec::new())]).unwrap(),
            "/voice status"
        );
        assert_eq!(
            slash_command_body(&slash_input(
                123,
                "42",
                "voice",
                vec![slash_subcommand_option("status", Vec::new())]
            ))
            .unwrap(),
            Some("/voice status".to_string())
        );
        assert!(voice_slash_command_body(&[]).is_err());
        assert!(voice_slash_command_body(&[slash_subcommand_option("bogus", Vec::new())]).is_err());
    }

    #[test]
    fn slash_command_body_maps_cron_subcommands_and_errors() {
        assert_eq!(
            cron_slash_command_body(&[
                slash_string_option("ignored", "value"),
                slash_subcommand_option("list", Vec::new())
            ])
            .unwrap(),
            "/cron list"
        );
        assert_eq!(
            slash_command_body(&slash_input(
                123,
                "42",
                "cron",
                vec![slash_subcommand_option(
                    "add",
                    vec![
                        slash_string_option("schedule", "0 * * * *"),
                        slash_string_option("message", "run the books"),
                    ]
                )]
            ))
            .unwrap(),
            Some("/cron add 0 * * * * run the books".to_string())
        );
        for subcommand in ["pause", "resume", "delete"] {
            assert_eq!(
                cron_slash_command_body(&[slash_subcommand_option(
                    subcommand,
                    vec![slash_string_option("id", "cron-1")]
                )])
                .unwrap(),
                format!("/cron {subcommand} cron-1")
            );
        }
        assert!(cron_slash_command_body(&[]).is_err());
        assert!(cron_slash_command_body(&[slash_subcommand_option("bogus", Vec::new())]).is_err());
        assert!(slash_command_body(&slash_input(123, "42", "model", Vec::new())).is_err());
        assert!(
            required_slash_string_option(&[slash_subcommand_option("model", Vec::new())], "model")
                .is_err()
        );
        assert!(
            slash_command_body(&slash_input(
                123,
                "42",
                "cron",
                vec![slash_subcommand_option("add", Vec::new())]
            ))
            .is_err()
        );
    }

    #[test]
    fn discord_slash_command_input_extracts_supported_options() {
        let command = slash_command(
            123,
            Some(55),
            "42",
            "cron",
            vec![
                boolean_option_json("ignored", true),
                subcommand_option_json(
                    "add",
                    vec![
                        command_option_json("schedule", "0 * * * *"),
                        command_option_json("message", "run"),
                    ],
                ),
            ],
        );

        let input = discord_slash_command_input(&command);

        assert_eq!(input.channel_id, ChannelId::new(123));
        assert_eq!(input.guild_id, Some(GuildId::new(55)));
        assert_eq!(input.author_id, "42");
        assert_eq!(input.name, "cron");
        assert_eq!(
            input.options,
            vec![slash_subcommand_option(
                "add",
                vec![
                    slash_string_option("schedule", "0 * * * *"),
                    slash_string_option("message", "run"),
                ]
            )]
        );
    }

    #[test]
    fn ephemeral_interaction_response_serializes_content_and_flag() {
        let response = serde_json::to_value(ephemeral_interaction_response("done")).unwrap();

        assert_eq!(response["type"], 4);
        assert_eq!(response["data"]["content"], "done");
        assert_eq!(response["data"]["flags"], 64);
    }

    #[tokio::test]
    async fn ready_with_registrar_publishes_slash_commands_and_logs_failures() {
        let (mut handler, _queue) =
            handler_with_bindings(vec![binding_for_channel(123)], Vec::new(), None);
        handler.slash_guild_ids = vec![GuildId::new(55)];
        let registrar = FakeSlashCommandRegistrar::default();

        handler
            .ready_with_registrar(&ready_event("moni"), &registrar)
            .await;

        let calls = registrar.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, vec![GuildId::new(55)]);
        assert!(calls[0].1.to_string().contains("\"model\""));
        drop(calls);

        handler
            .ready_with_registrar(
                &ready_event("moni"),
                &FakeSlashCommandRegistrar {
                    calls: Arc::new(Mutex::new(Vec::new())),
                    fail: true,
                },
            )
            .await;
    }

    #[tokio::test]
    async fn serenity_slash_registrar_uses_global_and_guild_routes() {
        let proxy = DiscordHttpProxy::start();
        let http = Arc::new(
            HttpBuilder::new("token")
                .proxy(proxy.base_url())
                .ratelimiter_disabled(true)
                .build(),
        );
        http.set_application_id(ApplicationId::new(123));
        let registrar = SerenitySlashCommandRegistrar { http };

        registrar
            .register_slash_commands(&[], moni_slash_commands())
            .await
            .unwrap();
        registrar
            .register_slash_commands(&[GuildId::new(55)], moni_slash_commands())
            .await
            .unwrap();

        proxy.wait_for_path("/applications/").await;
        let requests = proxy.requests();
        assert!(
            requests
                .iter()
                .any(|request| request.path.contains("/applications/")
                    && request.path.contains("/commands")
                    && !request.path.contains("/guilds/55"))
        );
        assert!(
            requests
                .iter()
                .any(|request| request.path.contains("/guilds/55/commands"))
        );
    }

    #[tokio::test]
    async fn slash_model_command_runs_bound_command_without_queue_or_public_ack() {
        let (handler, queue, output) =
            handler_with_output(vec![binding_for_channel(123)], Vec::new(), None);
        let actions = FakeDiscordActions::default();

        let response = handler
            .handle_slash_command_with_actions(
                &actions,
                slash_input(
                    123,
                    "42",
                    "model",
                    vec![slash_string_option("model", "prompt")],
                ),
            )
            .await
            .unwrap();

        assert_eq!(response, "model set to prompt");
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
        assert!(output.messages().await.is_empty());
        assert!(actions.state().await.says.is_empty());
    }

    #[tokio::test]
    async fn slash_status_command_reports_bound_namespace_status() {
        let (handler, queue, output) =
            handler_with_output(vec![binding_for_channel(123)], Vec::new(), None);
        let actions = FakeDiscordActions::default();

        let response = handler
            .handle_slash_command_with_actions(&actions, slash_input(123, "42", "status", vec![]))
            .await
            .unwrap();

        assert!(response.contains("namespace: moni"));
        assert!(response.contains("repo: https://github.com/Gonzih/moni"));
        assert!(response.contains("session: idle"));
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
        assert!(output.messages().await.is_empty());
        assert!(actions.state().await.says.is_empty());
    }

    #[tokio::test]
    async fn slash_voice_status_reports_configured_and_unconfigured_health() {
        let dir = TempDir::new().unwrap();
        let transcriber = voice_transcriber(&dir, "#!/bin/sh\nexit 0\n", "#!/bin/sh\nexit 0\n");
        let (handler, queue) = handler_with_bindings(
            vec![binding_for_channel(123)],
            Vec::new(),
            Some(transcriber),
        );
        let actions = FakeDiscordActions::default();

        let response = handler
            .handle_slash_command_with_actions(
                &actions,
                slash_input(
                    999,
                    "42",
                    "voice",
                    vec![slash_subcommand_option("status", Vec::new())],
                ),
            )
            .await
            .unwrap();

        assert!(response.contains("voice transcription configured"));
        assert!(response.contains("whisper.cpp: ok"));
        assert!(response.contains("ffmpeg: ok"));
        assert!(response.contains("curl: ok"));
        assert!(response.contains("model: ok"));
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
        assert!(actions.state().await.says.is_empty());

        let (handler, _queue) =
            handler_with_bindings(vec![binding_for_channel(123)], Vec::new(), None);
        let unconfigured = handler
            .handle_slash_command_with_actions(
                &actions,
                slash_input(
                    999,
                    "42",
                    "voice",
                    vec![slash_subcommand_option("status", Vec::new())],
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            unconfigured,
            "voice transcription unavailable - whisper.cpp is not configured"
        );
    }

    #[tokio::test]
    async fn slash_register_binds_current_channel_without_existing_binding() {
        let (handler, _queue) = handler_with_bindings(Vec::new(), Vec::new(), None);
        let actions = FakeDiscordActions::default();

        let response = handler
            .handle_slash_command_with_actions(
                &actions,
                slash_input(
                    321,
                    "42",
                    "register",
                    vec![
                        slash_string_option("namespace", "moni"),
                        slash_string_option("repo", "https://github.com/Gonzih/moni"),
                    ],
                ),
            )
            .await
            .unwrap();

        assert_eq!(response, "registered");
        assert_eq!(
            handler
                .registry
                .get_by_channel(ChannelId::new(321))
                .await
                .unwrap()
                .repo_url,
            "https://github.com/Gonzih/moni"
        );
        assert!(actions.state().await.says.is_empty());
    }

    #[tokio::test]
    async fn slash_channel_command_creates_channel_and_reports_validation_errors() {
        let (handler, _queue) = handler_with_bindings(Vec::new(), Vec::new(), None);
        let actions = FakeDiscordActions::default();
        let mut input = slash_input(
            123,
            "42",
            "channel",
            vec![slash_string_option(
                "repo",
                "https://github.com/Gonzih/moni",
            )],
        );
        input.guild_id = Some(GuildId::new(55));

        let response = handler
            .handle_slash_command_with_actions(&actions, input)
            .await
            .unwrap();

        assert_eq!(
            response,
            "Created <#999> - messages there route to the https://github.com/Gonzih/moni meta-agent"
        );
        assert_eq!(
            actions.state().await.creates,
            vec![(
                GuildId::new(55),
                "moni".to_string(),
                "moni route for https://github.com/Gonzih/moni".to_string(),
                Some(ChannelId::new(777))
            )]
        );

        let no_guild = handler
            .handle_slash_command_with_actions(
                &actions,
                slash_input(
                    123,
                    "42",
                    "channel",
                    vec![slash_string_option(
                        "repo",
                        "https://github.com/Gonzih/moni",
                    )],
                ),
            )
            .await
            .unwrap();
        assert_eq!(no_guild, "channel creation only works in a server");
        assert!(
            handler
                .handle_slash_command_with_actions(
                    &actions,
                    slash_input(
                        123,
                        "42",
                        "channel",
                        vec![slash_string_option("repo", "https://example.com/nope")],
                    ),
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn slash_command_rejects_unauthorized_unknown_and_unbound_cases() {
        let (handler, _queue) =
            handler_with_bindings(vec![binding_for_channel(123)], vec!["42".to_string()], None);
        let actions = FakeDiscordActions::default();

        assert_eq!(
            handler
                .handle_slash_command_with_actions(
                    &actions,
                    slash_input(123, "99", "reset", Vec::new())
                )
                .await
                .unwrap(),
            "Not authorized."
        );
        assert_eq!(
            handler
                .handle_slash_command_with_actions(
                    &actions,
                    slash_input(123, "42", "bogus", Vec::new())
                )
                .await
                .unwrap(),
            "Unknown command."
        );
        assert_eq!(
            handler
                .handle_slash_command_with_actions(
                    &actions,
                    slash_input(999, "42", "reset", Vec::new())
                )
                .await
                .unwrap(),
            unconfigured_channel_message()
        );
    }

    #[tokio::test]
    async fn discord_messages_enter_namespace_queue() {
        let queue = InMemoryNamespaceQueue::default();
        let binding = ChannelBinding {
            channel_id: "c1".to_string(),
            namespace: "moni".to_string(),
            repo_url: "https://example.com/repo.git".to_string(),
        };
        let message = DiscordInboundMessage {
            channel_id: "c1".to_string(),
            author_id: "u1".to_string(),
            body: "ship the harness".to_string(),
        };

        route_discord_message(&queue, &binding, message)
            .await
            .unwrap();

        let drained = queue.drain_namespace("moni").await.unwrap();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].body, "ship the harness");
        assert_eq!(
            drained[0].repo_url.as_deref(),
            Some("https://example.com/repo.git")
        );
    }

    #[tokio::test]
    async fn discord_message_routing_returns_enqueue_errors() {
        let binding = ChannelBinding {
            channel_id: "c1".to_string(),
            namespace: "moni".to_string(),
            repo_url: "https://example.com/repo.git".to_string(),
        };
        let message = DiscordInboundMessage {
            channel_id: "c1".to_string(),
            author_id: "u1".to_string(),
            body: "ship the harness".to_string(),
        };

        let queue: &dyn NamespaceQueue = &FailingNamespaceQueue;
        let err = route_discord_message(queue, &binding, message)
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "enqueue failed");
        let drain_err = queue.drain_namespace("moni").await.unwrap_err();
        assert_eq!(drain_err.to_string(), "drain failed");
    }

    #[tokio::test]
    async fn message_core_ignores_bots_and_unauthorized_users() {
        let (handler, queue) =
            handler_with_bindings(vec![binding_for_channel(123)], vec!["42".to_string()], None);
        let actions = FakeDiscordActions::default();
        let mut bot_message = discord_input(123, "42", "hello");
        bot_message.author_bot = true;

        handler
            .handle_message_with_actions(&actions, bot_message)
            .await;
        handler
            .handle_message_with_actions(&actions, discord_input(123, "99", "hello"))
            .await;

        assert!(actions.state().await.says.is_empty());
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_core_creates_channel_and_registers_binding() {
        let (handler, _queue) = handler_with_bindings(Vec::new(), Vec::new(), None);
        let actions = FakeDiscordActions::default();
        let mut message = discord_input(1, "42", "channel for https://github.com/Gonzih/moni");
        message.guild_id = Some(GuildId::new(55));

        handler.handle_message_with_actions(&actions, message).await;

        let state = actions.state().await;
        assert_eq!(
            state.creates,
            vec![(
                GuildId::new(55),
                "moni".to_string(),
                "moni route for https://github.com/Gonzih/moni".to_string(),
                Some(ChannelId::new(777)),
            )]
        );
        assert_eq!(state.says.len(), 1);
        assert!(state.says[0].1.contains("Created <#999>"));
    }

    #[tokio::test]
    async fn message_core_reports_channel_create_without_guild_or_on_failure() {
        let (handler, _queue) = handler_with_bindings(Vec::new(), Vec::new(), None);
        let actions = FakeDiscordActions::default();

        handler
            .handle_message_with_actions(
                &actions,
                discord_input(1, "42", "channel for https://github.com/Gonzih/moni"),
            )
            .await;
        actions.fail_creates().await;
        let mut message = discord_input(1, "42", "channel for https://github.com/Gonzih/moni");
        message.guild_id = Some(GuildId::new(55));
        handler.handle_message_with_actions(&actions, message).await;

        let says = actions
            .state()
            .await
            .says
            .into_iter()
            .map(|(_, body)| body)
            .collect::<Vec<_>>();
        assert!(
            says.iter()
                .any(|body| body == "channel creation only works in a server")
        );
        assert!(
            says.iter()
                .any(|body| body.contains("channel creation failed"))
        );
    }

    #[tokio::test]
    async fn message_core_routes_bound_text_and_starts_typing() {
        let (handler, queue) =
            handler_with_bindings(vec![binding_for_channel(123)], Vec::new(), None);
        let actions = FakeDiscordActions::default();

        handler
            .handle_message_with_actions(&actions, discord_input(123, "42", "hello"))
            .await;

        assert_eq!(
            queue.drain_namespace("moni").await.unwrap()[0].body,
            "hello"
        );
        assert_eq!(
            actions.state().await.typing_started,
            vec![("moni".to_string(), ChannelId::new(123))]
        );
    }

    #[tokio::test]
    async fn serenity_discord_actions_use_http_proxy() {
        let proxy = DiscordHttpProxy::start();
        let http = Arc::new(
            HttpBuilder::new("token")
                .proxy(proxy.base_url())
                .ratelimiter_disabled(true)
                .build(),
        );
        let typing = DiscordTypingTracker::default();
        let actions = SerenityDiscordActions {
            http,
            typing: &typing,
        };

        actions
            .say(ChannelId::new(123), "hello from actions".to_string())
            .await
            .unwrap();
        let created = actions
            .create_text_channel(
                GuildId::new(55),
                "moni".to_string(),
                "topic".to_string(),
                Some(ChannelId::new(777)),
            )
            .await
            .unwrap();
        actions
            .start_typing("moni".to_string(), ChannelId::new(123))
            .await
            .unwrap();
        proxy.wait_for_path("/channels/123/typing").await;
        actions.stop_typing("moni").await.unwrap();

        assert_eq!(created, ChannelId::new(999));
        let requests = proxy.requests();
        assert!(
            requests
                .iter()
                .any(|request| request.path.contains("/channels/123/messages")
                    && request.body.contains("hello from actions"))
        );
        assert!(
            requests
                .iter()
                .any(|request| request.path.contains("/guilds/55/channels")
                    && request.body.contains("moni")
                    && request.body.contains("777"))
        );
    }

    #[test]
    fn serenity_client_builder_keeps_intents_with_and_without_proxy() {
        let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT;

        assert_eq!(
            serenity_client_builder("token", intents, None).get_intents(),
            intents
        );
        assert_eq!(
            serenity_client_builder("token", intents, Some("http://127.0.0.1:9")).get_intents(),
            intents
        );
    }

    #[tokio::test]
    async fn run_discord_bot_builds_gateway_handler_and_intents() {
        let dir = TempDir::new().unwrap();
        let bindings = vec![binding_for_channel(123)];
        let (app, registry, _queue) = app_for_bindings(bindings.clone());
        let gateway = FakeDiscordGateway::default();
        let transcriber = VoiceTranscriber::new(
            dir.path().join("whisper"),
            dir.path().join("ffmpeg"),
            dir.path().join("curl"),
            dir.path().join("model.bin"),
            dir.path(),
        );
        let config = DiscordBotConfig::new("token", bindings)
            .unwrap()
            .with_allowed_user_ids(vec!["42".to_string()])
            .unwrap()
            .with_default_category_id(Some("777".to_string()))
            .unwrap()
            .with_slash_guild_ids(vec!["88".to_string(), "99".to_string()])
            .unwrap()
            .with_voice_transcriber(Some(transcriber));

        run_discord_bot_with_gateway(
            config,
            app,
            registry,
            DiscordTypingTracker::default(),
            &gateway,
        )
        .await
        .unwrap();

        let calls = gateway.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].token, "token");
        assert!(calls[0].intents.contains(GatewayIntents::GUILD_MESSAGES));
        assert!(calls[0].intents.contains(GatewayIntents::DIRECT_MESSAGES));
        assert!(calls[0].intents.contains(GatewayIntents::MESSAGE_CONTENT));
        assert!(calls[0].authorizes_allowed_user);
        assert!(!calls[0].authorizes_other_user);
        assert_eq!(calls[0].default_category_id, Some(ChannelId::new(777)));
        assert!(calls[0].has_voice_transcriber);
        assert_eq!(
            calls[0].slash_guild_ids,
            vec![GuildId::new(88), GuildId::new(99)]
        );
    }

    #[tokio::test]
    async fn run_discord_bot_uses_serenity_gateway_with_proxy() {
        let proxy = DiscordHttpProxy::start();
        let bindings = vec![binding_for_channel(123)];
        let (app, registry, _queue) = app_for_bindings(bindings.clone());
        let config = DiscordBotConfig::new("token", bindings)
            .unwrap()
            .with_gateway_proxy_url(Some(proxy.base_url().to_string()));

        let result = tokio::time::timeout(
            Duration::from_millis(500),
            run_discord_bot(config, app, registry, DiscordTypingTracker::default()),
        )
        .await;

        assert!(result.is_err());
        proxy.wait_for_path("/gateway").await;
    }

    #[tokio::test]
    async fn run_discord_bot_wrapper_returns_gateway_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let bindings = vec![binding_for_channel(123)];
        let (app, registry, _queue) = app_for_bindings(bindings.clone());
        let config = DiscordBotConfig::new("token", bindings)
            .unwrap()
            .with_gateway_proxy_url(Some(proxy_url));

        let err = tokio::time::timeout(
            Duration::from_secs(5),
            run_discord_bot(config, app, registry, DiscordTypingTracker::default()),
        )
        .await
        .expect("gateway error should return before timeout")
        .unwrap_err();

        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn event_handler_ready_and_message_use_serenity_inputs() {
        let proxy = DiscordHttpProxy::start();
        let http = Arc::new(
            HttpBuilder::new("token")
                .proxy(proxy.base_url())
                .ratelimiter_disabled(true)
                .build(),
        );
        let (handler, queue) =
            handler_with_bindings(vec![binding_for_channel(123)], Vec::new(), None);

        EventHandler::ready(
            &handler,
            serenity_context(http.clone()),
            ready_event("moni"),
        )
        .await;
        proxy.wait_for_path("/applications/").await;
        EventHandler::message(
            &handler,
            serenity_context(http),
            serenity_message(123, "42", "hello through serenity"),
        )
        .await;
        proxy.wait_for_path("/channels/123/typing").await;
        handler.typing.stop("moni").await;

        let queued = queue.drain_namespace("moni").await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].body, "hello through serenity");
    }

    #[tokio::test]
    async fn event_handler_interaction_create_handles_ping_success_and_error_paths() {
        let proxy = DiscordHttpProxy::start();
        let http = Arc::new(
            HttpBuilder::new("token")
                .proxy(proxy.base_url())
                .ratelimiter_disabled(true)
                .build(),
        );
        let (handler, queue, output) =
            handler_with_output(vec![binding_for_channel(123)], Vec::new(), None);

        EventHandler::interaction_create(
            &handler,
            serenity_context(http.clone()),
            ping_interaction(),
        )
        .await;
        EventHandler::interaction_create(
            &handler,
            serenity_context(http.clone()),
            slash_interaction_with_id(
                500,
                123,
                None,
                "42",
                "model",
                vec![command_option_json("model", "prompt")],
            ),
        )
        .await;

        let callback = proxy.wait_for_path("/interactions/500").await;
        assert!(callback.body.contains("model set to prompt"));
        assert!(callback.body.contains("\"flags\":64"));
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
        assert!(output.messages().await.is_empty());

        EventHandler::interaction_create(
            &handler,
            serenity_context(http),
            slash_interaction_with_id(501, 123, None, "42", "model", Vec::new()),
        )
        .await;

        let error_callback = proxy.wait_for_path("/interactions/501").await;
        assert!(
            error_callback
                .body
                .contains("Command failed: missing model")
        );

        let bad_http = Arc::new(
            HttpBuilder::new("token")
                .proxy("http://127.0.0.1:9")
                .ratelimiter_disabled(true)
                .build(),
        );
        tokio::time::timeout(
            Duration::from_secs(2),
            EventHandler::interaction_create(
                &handler,
                serenity_context(bad_http),
                slash_interaction_with_id(
                    502,
                    123,
                    None,
                    "42",
                    "model",
                    vec![command_option_json("model", "prompt")],
                ),
            ),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn message_core_handles_model_command_for_bound_channel() {
        let (handler, queue, output) =
            handler_with_output(vec![binding_for_channel(123)], Vec::new(), None);
        let actions = FakeDiscordActions::default();

        handler
            .handle_message_with_actions(&actions, discord_input(123, "42", "/model prompt"))
            .await;

        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
        assert_eq!(output.messages().await[0].body, "model set to prompt");
        assert_eq!(
            actions.state().await.typing_started,
            vec![("moni".to_string(), ChannelId::new(123))]
        );
    }

    #[tokio::test]
    async fn message_core_reports_voice_status_without_queueing() {
        let dir = TempDir::new().unwrap();
        let transcriber = voice_transcriber(&dir, "#!/bin/sh\nexit 0\n", "#!/bin/sh\nexit 0\n");
        let (handler, queue) = handler_with_bindings(
            vec![binding_for_channel(123)],
            Vec::new(),
            Some(transcriber),
        );
        let actions = FakeDiscordActions::default();

        handler
            .handle_message_with_actions(&actions, discord_input(123, "42", "/voice status"))
            .await;

        let state = actions.state().await;
        assert_eq!(state.says.len(), 1);
        assert!(state.says[0].1.contains("voice transcription configured"));
        assert!(state.says[0].1.contains("whisper.cpp: ok"));
        assert!(state.typing_started.is_empty());
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_core_logs_voice_status_response_failures_without_queueing() {
        let (handler, queue) =
            handler_with_bindings(vec![binding_for_channel(123)], Vec::new(), None);
        let actions = FakeDiscordActions::default();
        actions.fail_says().await;

        handler
            .handle_message_with_actions(&actions, discord_input(123, "42", "/voice status"))
            .await;

        let state = actions.state().await;
        assert!(state.says.is_empty());
        assert!(state.typing_started.is_empty());
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_core_transcribes_voice_and_routes_prompt() {
        let dir = TempDir::new().unwrap();
        let transcriber = voice_transcriber(
            &dir,
            r#"#!/bin/sh
while [ "$1" != "-o" ]; do shift; done
shift
printf audio > "$1"
"#,
            r#"#!/bin/sh
while [ "$1" != "-f" ]; do shift; done
shift
printf "transcribed voice" > "$1.txt"
"#,
        )
        .with_prompt_template("heard:\n{content}");
        let (handler, queue) = handler_with_bindings(
            vec![binding_for_channel(123)],
            Vec::new(),
            Some(transcriber),
        );
        let actions = FakeDiscordActions::default();
        let mut message = discord_input(123, "42", "<@123> caption\nnext");
        message.attachments.push(DiscordAttachmentInput {
            filename: "voice.ogg".to_string(),
            content_type: Some("audio/ogg".to_string()),
            url: "https://cdn.discordapp.com/voice.ogg".to_string(),
            size: Some(5),
        });

        handler.handle_message_with_actions(&actions, message).await;

        let queued = queue.drain_namespace("moni").await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].body, "heard:\ncaption next\n\ntranscribed voice");
        assert!(actions.state().await.says.is_empty());
    }

    #[tokio::test]
    async fn message_core_reports_empty_voice_transcription() {
        let dir = TempDir::new().unwrap();
        let transcriber = voice_transcriber(
            &dir,
            r#"#!/bin/sh
while [ "$1" != "-o" ]; do shift; done
shift
printf audio > "$1"
"#,
            r#"#!/bin/sh
while [ "$1" != "-f" ]; do shift; done
shift
printf "[BLANK_AUDIO]" > "$1.txt"
"#,
        );
        let (handler, queue) = handler_with_bindings(
            vec![binding_for_channel(123)],
            Vec::new(),
            Some(transcriber),
        );
        let actions = FakeDiscordActions::default();
        let mut message = discord_input(123, "42", "caption");
        message.attachments.push(DiscordAttachmentInput {
            filename: "voice.ogg".to_string(),
            content_type: Some("audio/ogg".to_string()),
            url: "https://cdn.discordapp.com/voice.ogg".to_string(),
            size: Some(5),
        });

        handler.handle_message_with_actions(&actions, message).await;

        let state = actions.state().await;
        assert_eq!(state.typing_stopped, vec!["moni".to_string()]);
        assert_eq!(
            state.says,
            vec![(
                ChannelId::new(123),
                "Could not transcribe voice message.".to_string()
            )]
        );
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_core_reports_voice_transcription_error() {
        let dir = TempDir::new().unwrap();
        let transcriber = voice_transcriber(
            &dir,
            "#!/bin/sh\necho download failed >&2\nexit 2\n",
            "#!/bin/sh\nexit 0\n",
        );
        let (handler, queue) = handler_with_bindings(
            vec![binding_for_channel(123)],
            Vec::new(),
            Some(transcriber),
        );
        let actions = FakeDiscordActions::default();
        let mut message = discord_input(123, "42", "caption");
        message.attachments.push(DiscordAttachmentInput {
            filename: "voice.ogg".to_string(),
            content_type: Some("audio/ogg".to_string()),
            url: "https://cdn.discordapp.com/voice.ogg".to_string(),
            size: Some(5),
        });

        handler.handle_message_with_actions(&actions, message).await;

        let state = actions.state().await;
        assert_eq!(state.typing_stopped, vec!["moni".to_string()]);
        assert!(state.says[0].1.starts_with("Voice transcription failed:"));
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_core_rejects_oversized_voice_attachment_before_download() {
        let dir = TempDir::new().unwrap();
        let transcriber = voice_transcriber(
            &dir,
            "#!/bin/sh\necho should not download >&2\nexit 9\n",
            "#!/bin/sh\nexit 0\n",
        )
        .with_guardrails(4, 60);
        let (handler, queue) = handler_with_bindings(
            vec![binding_for_channel(123)],
            Vec::new(),
            Some(transcriber),
        );
        let actions = FakeDiscordActions::default();
        let mut message = discord_input(123, "42", "caption");
        message.attachments.push(DiscordAttachmentInput {
            filename: "voice.ogg".to_string(),
            content_type: Some("audio/ogg".to_string()),
            url: "https://cdn.discordapp.com/voice.ogg".to_string(),
            size: Some(5),
        });

        handler.handle_message_with_actions(&actions, message).await;

        let state = actions.state().await;
        assert_eq!(state.typing_stopped, vec!["moni".to_string()]);
        assert!(state.says[0].1.contains("voice attachment is too large"));
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_core_handles_unbound_ignored_and_error_paths() {
        let (handler, queue) = handler_with_bindings(Vec::new(), Vec::new(), None);
        let actions = FakeDiscordActions::default();

        handler
            .handle_message_with_actions(&actions, discord_input(123, "42", "hello"))
            .await;
        handler
            .handle_message_with_actions(&actions, discord_input(123, "42", "/register"))
            .await;

        assert!(actions.state().await.says.is_empty());
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_core_stops_typing_when_app_routing_errors() {
        let (handler, queue) =
            handler_with_bindings(vec![binding_for_channel(123)], Vec::new(), None);
        let actions = FakeDiscordActions::default();

        handler
            .handle_message_with_actions(&actions, discord_input(123, "42", "/cron"))
            .await;

        assert_eq!(
            actions.state().await.typing_stopped,
            vec!["moni".to_string()]
        );
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_core_handles_unconfigured_voice_transcription() {
        let (handler, queue) =
            handler_with_bindings(vec![binding_for_channel(123)], Vec::new(), None);
        let actions = FakeDiscordActions::default();
        let mut message = discord_input(123, "42", "caption");
        message.attachments.push(DiscordAttachmentInput {
            filename: "voice.ogg".to_string(),
            content_type: Some("audio/ogg".to_string()),
            url: "https://cdn.discordapp.com/voice.ogg".to_string(),
            size: Some(5),
        });

        handler.handle_message_with_actions(&actions, message).await;

        let state = actions.state().await;
        assert_eq!(
            state.typing_started,
            vec![("moni".to_string(), ChannelId::new(123))]
        );
        assert_eq!(state.typing_stopped, vec!["moni".to_string()]);
        assert_eq!(
            state.says,
            vec![(
                ChannelId::new(123),
                "Voice transcription unavailable - whisper.cpp is not configured".to_string(),
            )]
        );
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[test]
    fn parses_channel_bindings_without_breaking_https_repo_urls() {
        let bindings = parse_channel_bindings(
            "123=moni=https://github.com/example/moni,456=ops=ssh://git@example.com/repo",
        )
        .unwrap();

        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].channel_id, "123");
        assert_eq!(bindings[0].namespace, "moni");
        assert_eq!(bindings[0].repo_url, "https://github.com/example/moni");
        assert_eq!(bindings[1].repo_url, "ssh://git@example.com/repo");
    }

    #[test]
    fn parses_channel_create_intent() {
        let intent =
            parse_channel_create_intent("channel for https://github.com/Gonzih/moni").unwrap();

        assert_eq!(intent.namespace, "moni");
        assert_eq!(intent.repo_url, "https://github.com/Gonzih/moni");
    }

    #[test]
    fn parses_channel_create_intent_with_optional_verb() {
        let intent =
            parse_channel_create_intent("add channel for https://github.com/gitkb/harmony.")
                .unwrap();

        assert_eq!(intent.namespace, "harmony");
        assert_eq!(intent.repo_url, "https://github.com/gitkb/harmony");
    }

    #[test]
    fn parses_channel_create_intent_from_http_url_and_trims_wrappers() {
        let intent =
            parse_channel_create_intent("please add channel for http://github.com/Org/repo_name>,")
                .unwrap();

        assert_eq!(intent.namespace, "repo_name");
        assert_eq!(intent.repo_url, "http://github.com/Org/repo_name");
    }

    #[test]
    fn ignores_non_github_channel_create_intent() {
        assert!(parse_channel_create_intent("channel for https://example.com/a/b").is_none());
        assert!(parse_channel_create_intent("channel for https://github.com//repo").is_none());
        assert!(parse_channel_create_intent("channel for https://github.com/org/repo$").is_none());
        assert!(parse_channel_create_intent("hello").is_none());
    }

    #[tokio::test]
    async fn discord_prompt_source_includes_channel_id() {
        let queue = InMemoryNamespaceQueue::default();
        let binding = ChannelBinding {
            channel_id: "42".to_string(),
            namespace: "moni".to_string(),
            repo_url: "repo".to_string(),
        };
        let message = DiscordInboundMessage {
            channel_id: "42".to_string(),
            author_id: "u1".to_string(),
            body: "body".to_string(),
        };

        let prompt = route_discord_message(&queue, &binding, message)
            .await
            .unwrap();

        assert_eq!(prompt.source, "discord:42");
    }

    #[tokio::test]
    async fn discord_prompt_uses_binding_namespace_not_channel_id() {
        let queue = InMemoryNamespaceQueue::default();
        let binding = ChannelBinding {
            channel_id: "42".to_string(),
            namespace: "workspace-name".to_string(),
            repo_url: "repo".to_string(),
        };
        let message = DiscordInboundMessage {
            channel_id: "42".to_string(),
            author_id: "u1".to_string(),
            body: "body".to_string(),
        };

        route_discord_message(&queue, &binding, message)
            .await
            .unwrap();

        assert!(queue.drain_namespace("42").await.unwrap().is_empty());
        assert_eq!(
            queue.drain_namespace("workspace-name").await.unwrap()[0].body,
            "body"
        );
    }

    #[tokio::test]
    async fn discord_prompt_preserves_multiline_body() {
        let queue = InMemoryNamespaceQueue::default();
        let binding = ChannelBinding {
            channel_id: "42".to_string(),
            namespace: "moni".to_string(),
            repo_url: "repo".to_string(),
        };
        let body = "line one\nline two\nline three";
        let message = DiscordInboundMessage {
            channel_id: "42".to_string(),
            author_id: "u1".to_string(),
            body: body.to_string(),
        };

        let prompt = route_discord_message(&queue, &binding, message)
            .await
            .unwrap();

        assert_eq!(prompt.body, body);
    }

    #[test]
    fn parses_empty_channel_bindings_as_empty() {
        assert!(parse_channel_bindings("").unwrap().is_empty());
        assert!(parse_channel_bindings(" , ").unwrap().is_empty());
    }

    #[test]
    fn parse_channel_bindings_rejects_missing_repo() {
        let err = parse_channel_bindings("123=moni").unwrap_err();
        assert!(
            err.to_string()
                .contains("expected channel_id=namespace=repo_url")
        );
    }

    #[test]
    fn parse_channel_bindings_rejects_missing_namespace() {
        let err = parse_channel_bindings("123").unwrap_err();
        assert!(
            err.to_string()
                .contains("expected channel_id=namespace=repo_url")
        );
    }

    #[test]
    fn parse_channel_bindings_trims_entries() {
        let bindings = parse_channel_bindings(" 123=moni=repo , 456=ops=repo2 ").unwrap();
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].channel_id, "123");
        assert_eq!(bindings[1].channel_id, "456");
    }

    #[test]
    fn bot_config_keeps_validated_bindings() {
        let config = DiscordBotConfig::new(
            "token",
            vec![ChannelBinding {
                channel_id: "123".to_string(),
                namespace: "moni".to_string(),
                repo_url: "repo".to_string(),
            }],
        )
        .unwrap();

        assert_eq!(config.token, "token");
        assert_eq!(config.bindings[0].namespace, "moni");
    }

    #[test]
    fn bot_config_keeps_validated_allowed_users() {
        let config = DiscordBotConfig::new("token", Vec::new())
            .unwrap()
            .with_allowed_user_ids(vec!["42".to_string()])
            .unwrap();

        assert_eq!(config.allowed_user_ids, vec!["42".to_string()]);
    }

    #[test]
    fn bot_config_rejects_invalid_allowed_user_id() {
        let err = DiscordBotConfig::new("token", Vec::new())
            .unwrap()
            .with_allowed_user_ids(vec!["not-a-number".to_string()])
            .unwrap_err();

        assert!(err.to_string().contains("invalid digit"));
    }

    #[test]
    fn bot_config_accepts_default_category_id() {
        let config = DiscordBotConfig::new("token", Vec::new())
            .unwrap()
            .with_default_category_id(Some("123".to_string()))
            .unwrap();

        assert_eq!(config.default_category_id, Some(ChannelId::new(123)));
    }

    #[test]
    fn bot_config_ignores_blank_default_category_and_stores_voice_transcriber() {
        let transcriber = VoiceTranscriber::new(
            "/bin/whisper",
            "/bin/ffmpeg",
            "/usr/bin/curl",
            "/tmp/model.bin",
            "/tmp",
        );
        let config = DiscordBotConfig::new("token", Vec::new())
            .unwrap()
            .with_default_category_id(Some("  ".to_string()))
            .unwrap()
            .with_voice_transcriber(Some(transcriber));

        assert_eq!(config.default_category_id, None);
        assert!(config.voice_transcriber.is_some());
    }

    #[test]
    fn bot_config_accepts_slash_guild_ids() {
        let config = DiscordBotConfig::new("token", Vec::new())
            .unwrap()
            .with_slash_guild_ids(vec!["55".to_string(), "66".to_string()])
            .unwrap();

        assert_eq!(
            config.slash_guild_ids,
            vec![GuildId::new(55), GuildId::new(66)]
        );
    }

    #[test]
    fn bot_config_rejects_invalid_slash_guild_id() {
        let err = DiscordBotConfig::new("token", Vec::new())
            .unwrap()
            .with_slash_guild_ids(vec!["not-a-guild".to_string()])
            .unwrap_err();

        assert!(err.to_string().contains("invalid digit"));
    }

    #[test]
    fn handler_constructor_collects_allowed_users() {
        let handler = MoniDiscordHandler::new(
            Arc::new(MoniApp::new(crate::app::MoniAppConfig {
                queue: Arc::new(InMemoryNamespaceQueue::default()),
                sessions: Arc::new(crate::SessionManager::new(
                    std::path::PathBuf::from("/tmp/moni-test"),
                    Arc::new(crate::StaticEngineConfigResolver::new(
                        crate::EngineConfig::new(crate::AgentEngine::Codex, "/bin/cat"),
                    )),
                    Arc::new(crate::InMemoryOutputSink::default()),
                )),
                output: Arc::new(crate::InMemoryOutputSink::default()),
                cron: crate::CronEngine::new(Vec::new()),
                registry: BindingRegistry::new(Vec::new()).unwrap(),
                state_store: None,
                voice_status: None,
            })),
            BindingRegistry::new(Vec::new()).unwrap(),
            ["42".to_string()],
            DiscordTypingTracker::default(),
            Some(ChannelId::new(7)),
            None,
            vec![GuildId::new(55)],
        );

        assert!(handler.is_authorized("42"));
        assert!(!handler.is_authorized("99"));
        assert_eq!(handler.default_category_id, Some(ChannelId::new(7)));
        assert_eq!(handler.slash_guild_ids, vec![GuildId::new(55)]);
    }

    #[test]
    fn empty_allowed_user_list_authorizes_everyone() {
        let handler = MoniDiscordHandler {
            app: Arc::new(MoniApp::new(crate::app::MoniAppConfig {
                queue: Arc::new(InMemoryNamespaceQueue::default()),
                sessions: Arc::new(crate::SessionManager::new(
                    std::path::PathBuf::from("/tmp/moni-test"),
                    Arc::new(crate::StaticEngineConfigResolver::new(
                        crate::EngineConfig::new(crate::AgentEngine::Codex, "/bin/cat"),
                    )),
                    Arc::new(crate::InMemoryOutputSink::default()),
                )),
                output: Arc::new(crate::InMemoryOutputSink::default()),
                cron: crate::CronEngine::new(Vec::new()),
                registry: BindingRegistry::new(Vec::new()).unwrap(),
                state_store: None,
                voice_status: None,
            })),
            registry: BindingRegistry::new(Vec::new()).unwrap(),
            allowed_user_ids: HashSet::new(),
            typing: DiscordTypingTracker::default(),
            default_category_id: None,
            voice_transcriber: None,
            slash_guild_ids: Vec::new(),
        };

        assert!(handler.is_authorized("42"));
    }

    #[test]
    fn configured_allowed_user_list_blocks_unknown_users() {
        let handler = MoniDiscordHandler {
            app: Arc::new(MoniApp::new(crate::app::MoniAppConfig {
                queue: Arc::new(InMemoryNamespaceQueue::default()),
                sessions: Arc::new(crate::SessionManager::new(
                    std::path::PathBuf::from("/tmp/moni-test"),
                    Arc::new(crate::StaticEngineConfigResolver::new(
                        crate::EngineConfig::new(crate::AgentEngine::Codex, "/bin/cat"),
                    )),
                    Arc::new(crate::InMemoryOutputSink::default()),
                )),
                output: Arc::new(crate::InMemoryOutputSink::default()),
                cron: crate::CronEngine::new(Vec::new()),
                registry: BindingRegistry::new(Vec::new()).unwrap(),
                state_store: None,
                voice_status: None,
            })),
            registry: BindingRegistry::new(Vec::new()).unwrap(),
            allowed_user_ids: HashSet::from(["42".to_string()]),
            typing: DiscordTypingTracker::default(),
            default_category_id: None,
            voice_transcriber: None,
            slash_guild_ids: Vec::new(),
        };

        assert!(handler.is_authorized("42"));
        assert!(!handler.is_authorized("7"));
    }

    #[test]
    fn bot_config_rejects_non_numeric_channel_id() {
        let err = DiscordBotConfig::new(
            "token",
            vec![ChannelBinding {
                channel_id: "not-a-number".to_string(),
                namespace: "moni".to_string(),
                repo_url: "repo".to_string(),
            }],
        )
        .unwrap_err();

        assert!(err.to_string().contains("invalid digit"));
    }

    #[test]
    fn bot_config_preserves_duplicate_bindings_for_registry_resolution() {
        let config = DiscordBotConfig::new(
            "token",
            vec![
                ChannelBinding {
                    channel_id: "123".to_string(),
                    namespace: "old".to_string(),
                    repo_url: "old-repo".to_string(),
                },
                ChannelBinding {
                    channel_id: "123".to_string(),
                    namespace: "new".to_string(),
                    repo_url: "new-repo".to_string(),
                },
            ],
        )
        .unwrap();

        assert_eq!(config.bindings.len(), 2);
        assert_eq!(config.bindings[0].namespace, "old");
        assert_eq!(config.bindings[1].namespace, "new");
    }

    #[test]
    fn channel_binding_round_trips_json() {
        let binding = ChannelBinding {
            channel_id: "123".to_string(),
            namespace: "moni".to_string(),
            repo_url: "repo".to_string(),
        };
        let encoded = serde_json::to_string(&binding).unwrap();
        let decoded: ChannelBinding = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, binding);
    }

    #[test]
    fn inbound_message_round_trips_json() {
        let message = DiscordInboundMessage {
            channel_id: "123".to_string(),
            author_id: "u1".to_string(),
            body: "hello".to_string(),
        };
        let encoded = serde_json::to_string(&message).unwrap();
        let decoded: DiscordInboundMessage = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn discord_message_input_copies_serenity_message_fields() {
        let message: Message = serde_json::from_value(serde_json::json!({
            "id": "111",
            "channel_id": "123",
            "guild_id": "55",
            "author": {
                "id": "42",
                "username": "user",
                "discriminator": "0001",
                "global_name": null,
                "avatar": null,
                "bot": true,
                "system": false,
                "mfa_enabled": false,
                "banner": null,
                "accent_color": null,
                "locale": null,
                "verified": null,
                "email": null,
                "flags": 0,
                "premium_type": 0,
                "public_flags": 0
            },
            "content": "hello",
            "timestamp": "2020-01-01T00:00:00.000000+00:00",
            "edited_timestamp": null,
            "tts": false,
            "mention_everyone": false,
            "mentions": [],
            "mention_roles": [],
            "attachments": [{
                "id": "7",
                "filename": "voice.ogg",
                "description": null,
                "height": null,
                "proxy_url": "https://proxy.example/voice.ogg",
                "size": 10,
                "url": "https://cdn.example/voice.ogg",
                "width": null,
                "content_type": "audio/ogg"
            }],
            "embeds": [],
            "pinned": false,
            "type": 0
        }))
        .unwrap();

        let input = discord_message_input(&message);

        assert_eq!(input.channel_id, ChannelId::new(123));
        assert_eq!(input.guild_id, Some(GuildId::new(55)));
        assert_eq!(input.author_id, "42");
        assert!(input.author_bot);
        assert_eq!(input.content, "hello");
        assert_eq!(input.attachments[0].filename, "voice.ogg");
        assert_eq!(
            input.attachments[0].content_type.as_deref(),
            Some("audio/ogg")
        );
        assert_eq!(input.attachments[0].url, "https://cdn.example/voice.ogg");
    }

    #[test]
    fn discord_channel_name_normalizes_namespaces() {
        assert_eq!(discord_channel_name("Money Brain"), "money-brain");
        assert_eq!(discord_channel_name("__"), "moni-channel");
        assert_eq!(discord_channel_name("a"), "moni-channel");
        assert_eq!(discord_channel_name("cc---suite"), "cc-suite");
    }

    #[test]
    fn strip_discord_mentions_removes_user_mentions_only() {
        assert_eq!(
            strip_discord_mentions("hey <@123> and <@!456> keep <#789>"),
            "hey and keep <#789>"
        );
        assert_eq!(strip_discord_mentions("<@abc> stays"), "<@abc> stays");
    }

    #[test]
    fn voice_error_message_maps_known_setup_failures() {
        assert_eq!(
            voice_error_message(&anyhow::anyhow!("whisper-cpp not found")),
            "Voice transcription unavailable - whisper.cpp is not installed"
        );
        assert_eq!(
            voice_error_message(&anyhow::anyhow!("No whisper model found")),
            "Voice transcription unavailable - no whisper model found"
        );
        assert_eq!(
            voice_error_message(&anyhow::anyhow!("ffmpeg not found")),
            "Voice transcription unavailable - ffmpeg is not installed"
        );
        assert_eq!(
            voice_error_message(&anyhow::anyhow!("boom")),
            "Voice transcription failed: boom"
        );
    }
}
