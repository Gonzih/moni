use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serenity::{
    async_trait as serenity_async_trait,
    model::{
        channel::Message,
        gateway::{GatewayIntents, Ready},
        id::ChannelId,
    },
    prelude::*,
};

use crate::queue::{NamespaceQueue, QueuedPrompt};

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
    pub bindings: HashMap<ChannelId, ChannelBinding>,
}

impl DiscordBotConfig {
    pub fn new(token: impl Into<String>, bindings: Vec<ChannelBinding>) -> anyhow::Result<Self> {
        let mut by_channel = HashMap::new();
        for binding in bindings {
            let channel_id = binding.channel_id.parse::<u64>()?;
            by_channel.insert(ChannelId::new(channel_id), binding);
        }
        Ok(Self {
            token: token.into(),
            bindings: by_channel,
        })
    }
}

pub struct MoniDiscordHandler {
    queue: Arc<dyn NamespaceQueue>,
    bindings: HashMap<ChannelId, ChannelBinding>,
}

impl MoniDiscordHandler {
    pub fn new(
        queue: Arc<dyn NamespaceQueue>,
        bindings: HashMap<ChannelId, ChannelBinding>,
    ) -> Self {
        Self { queue, bindings }
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

        let Some(binding) = self.bindings.get(&message.channel_id) else {
            return;
        };

        let inbound = DiscordInboundMessage {
            channel_id: message.channel_id.to_string(),
            author_id: message.author.id.to_string(),
            body: message.content,
        };

        if let Err(err) = route_discord_message(self.queue.as_ref(), binding, inbound).await {
            tracing::error!(channel_id = %binding.channel_id, namespace = %binding.namespace, error = %err, "failed to route discord message");
        }
    }
}

pub async fn run_discord_bot(
    config: DiscordBotConfig,
    queue: Arc<dyn NamespaceQueue>,
) -> anyhow::Result<()> {
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;
    let handler = MoniDiscordHandler::new(queue, config.bindings);
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
}
