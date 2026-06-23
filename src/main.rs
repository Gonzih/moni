use std::{env, sync::Arc};

use moni::{
    AgentEngine, BindingRegistry, CronEngine, DiscordBotConfig, DiscordOutputSink, EngineConfig,
    FileStateStore, MoniApp, MoniAppConfig, NatsNamespaceQueue, SessionManager, StateStore,
    StaticEngineConfigResolver, parse_channel_bindings, run_discord_bot,
};

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
    let workspace_root = env::var("MONI_WORKSPACE_ROOT").unwrap_or_else(|_| {
        dirs_next::home_dir()
            .unwrap_or_else(|| ".".into())
            .join("moni-workspace")
            .to_string_lossy()
            .to_string()
    });
    let engine =
        AgentEngine::from_name(&env::var("MONI_ENGINE").unwrap_or_else(|_| "codex".to_string()))?;
    let agent_bin = env::var("MONI_AGENT_BIN").unwrap_or_else(|_| engine.as_str().to_string());
    let state_store = env::var("MONI_STATE_PATH")
        .ok()
        .map(|path| Arc::new(FileStateStore::new(path)) as Arc<dyn StateStore>);

    let mut bindings = parse_channel_bindings(&channels)?;
    let cron_tasks = if let Some(store) = &state_store {
        let state = store.load().await?;
        bindings.extend(state.bindings);
        state.cron_tasks
    } else {
        Vec::new()
    };
    let registry = BindingRegistry::new(bindings.clone())?;
    let discord_config = DiscordBotConfig::new(token.clone(), bindings.clone())?;
    let nats_queue = NatsNamespaceQueue::connect(&nats_url).await?;
    let output = Arc::new(DiscordOutputSink::with_registry(token, registry.clone()));
    let resolver = Arc::new(StaticEngineConfigResolver::new(EngineConfig::new(
        engine, agent_bin,
    )));
    let sessions = Arc::new(SessionManager::new(
        workspace_root,
        resolver,
        output.clone(),
    ));
    let app = Arc::new(MoniApp::new(MoniAppConfig {
        queue: Arc::new(nats_queue.clone()),
        sessions,
        output,
        cron: CronEngine::new(cron_tasks),
        registry: registry.clone(),
        state_store,
    }));

    tokio::select! {
        result = run_discord_bot(discord_config, app.clone(), registry) => result,
        result = moni::nats::run_nats_prompt_consumer(nats_queue.client(), app) => result,
    }
}
