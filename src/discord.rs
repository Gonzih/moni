use std::{collections::HashSet, sync::Arc};

use serde::{Deserialize, Serialize};
use serenity::{
    async_trait as serenity_async_trait,
    model::{
        channel::Message,
        gateway::{GatewayIntents, Ready},
    },
    prelude::*,
};

use crate::app::MoniApp;
use crate::queue::{NamespaceQueue, QueuedPrompt};
use crate::registry::BindingRegistry;

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

pub async fn route_discord_message<Q: NamespaceQueue + ?Sized>(
    queue: &Q,
    binding: &ChannelBinding,
    message: DiscordInboundMessage,
) -> anyhow::Result<QueuedPrompt> {
    let prompt = QueuedPrompt::new(
        binding.namespace.clone(),
        Some(binding.repo_url.clone()),
        message.body,
        format!("discord:{}", message.channel_id),
    );
    queue.enqueue(prompt.clone()).await?;
    Ok(prompt)
}

#[derive(Debug, Clone)]
pub struct DiscordBotConfig {
    pub token: String,
    pub bindings: Vec<ChannelBinding>,
    pub allowed_user_ids: Vec<String>,
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
        })
    }

    pub fn with_allowed_user_ids(mut self, allowed_user_ids: Vec<String>) -> anyhow::Result<Self> {
        for user_id in &allowed_user_ids {
            user_id.parse::<u64>()?;
        }
        self.allowed_user_ids = allowed_user_ids;
        Ok(self)
    }
}

pub struct MoniDiscordHandler {
    app: Arc<MoniApp>,
    registry: BindingRegistry,
    allowed_user_ids: HashSet<String>,
}

impl MoniDiscordHandler {
    pub fn new(
        app: Arc<MoniApp>,
        registry: BindingRegistry,
        allowed_user_ids: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            app,
            registry,
            allowed_user_ids: allowed_user_ids.into_iter().collect(),
        }
    }

    fn is_authorized(&self, author_id: &str) -> bool {
        self.allowed_user_ids.is_empty() || self.allowed_user_ids.contains(author_id)
    }
}

#[serenity_async_trait]
impl EventHandler for MoniDiscordHandler {
    async fn ready(&self, _ctx: Context, ready: Ready) {
        tracing::info!(user = %ready.user.name, "moni discord gateway ready");
    }

    async fn message(&self, _ctx: Context, message: Message) {
        if message.author.bot {
            return;
        }
        let author_id = message.author.id.to_string();
        if !self.is_authorized(&author_id) {
            tracing::warn!(
                author_id = %author_id,
                channel_id = %message.channel_id,
                "ignored Discord message from unauthorized user"
            );
            return;
        }

        let inbound = DiscordInboundMessage {
            channel_id: message.channel_id.to_string(),
            author_id,
            body: message.content,
        };

        let Some(binding) = self.registry.get_by_channel(message.channel_id).await else {
            if let Err(err) = self
                .app
                .handle_unbound_discord_message(message.channel_id.to_string(), inbound)
                .await
            {
                tracing::error!(channel_id = %message.channel_id, error = %err, "failed to handle unbound discord message");
            }
            return;
        };

        if let Err(err) = self.app.handle_discord_message(&binding, inbound).await {
            tracing::error!(channel_id = %binding.channel_id, namespace = %binding.namespace, error = %err, "failed to route discord message");
        }
    }
}

pub async fn run_discord_bot(
    config: DiscordBotConfig,
    app: Arc<MoniApp>,
    registry: BindingRegistry,
) -> anyhow::Result<()> {
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;
    let handler = MoniDiscordHandler::new(app, registry, config.allowed_user_ids);
    let mut client = Client::builder(config.token, intents)
        .event_handler(handler)
        .await?;
    client.start().await?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::InMemoryNamespaceQueue;

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
    fn empty_allowed_user_list_authorizes_everyone() {
        let handler = MoniDiscordHandler {
            app: Arc::new(MoniApp::new(crate::app::MoniAppConfig {
                queue: Arc::new(InMemoryNamespaceQueue::default()),
                sessions: Arc::new(crate::SessionManager::new(
                    "/tmp/moni-test",
                    Arc::new(crate::StaticEngineConfigResolver::new(
                        crate::EngineConfig::new(crate::AgentEngine::Codex, "/bin/cat"),
                    )),
                    Arc::new(crate::InMemoryOutputSink::default()),
                )),
                output: Arc::new(crate::InMemoryOutputSink::default()),
                cron: crate::CronEngine::new(Vec::new()),
                registry: BindingRegistry::new(Vec::new()).unwrap(),
                state_store: None,
            })),
            registry: BindingRegistry::new(Vec::new()).unwrap(),
            allowed_user_ids: HashSet::new(),
        };

        assert!(handler.is_authorized("42"));
    }

    #[test]
    fn configured_allowed_user_list_blocks_unknown_users() {
        let handler = MoniDiscordHandler {
            app: Arc::new(MoniApp::new(crate::app::MoniAppConfig {
                queue: Arc::new(InMemoryNamespaceQueue::default()),
                sessions: Arc::new(crate::SessionManager::new(
                    "/tmp/moni-test",
                    Arc::new(crate::StaticEngineConfigResolver::new(
                        crate::EngineConfig::new(crate::AgentEngine::Codex, "/bin/cat"),
                    )),
                    Arc::new(crate::InMemoryOutputSink::default()),
                )),
                output: Arc::new(crate::InMemoryOutputSink::default()),
                cron: crate::CronEngine::new(Vec::new()),
                registry: BindingRegistry::new(Vec::new()).unwrap(),
                state_store: None,
            })),
            registry: BindingRegistry::new(Vec::new()).unwrap(),
            allowed_user_ids: HashSet::from(["42".to_string()]),
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
}
