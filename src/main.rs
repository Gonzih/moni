use std::{env, sync::Arc};

use moni::{DiscordBotConfig, NatsNamespaceQueue, parse_channel_bindings, run_discord_bot};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Some(token) = env::var("MONI_DISCORD_TOKEN").ok() else {
        println!(
            "moni: set MONI_DISCORD_TOKEN, MONI_CHANNELS, and optional MONI_NATS_URL to run the Discord adapter"
        );
        return Ok(());
    };
    let channels = env::var("MONI_CHANNELS").unwrap_or_default();
    let nats_url =
        env::var("MONI_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());

    let bindings = parse_channel_bindings(&channels)?;
    let config = DiscordBotConfig::new(token, bindings)?;
    let queue = Arc::new(NatsNamespaceQueue::connect(&nats_url).await?);

    run_discord_bot(config, queue).await
}
