use std::{env, sync::Arc, time::Duration};

use moni::{
    AgentEngine, AgentProtocol, BindingRegistry, CronEngine, DiscordBotConfig, DiscordOutputSink,
    DiscordTypingTracker, EngineConfig, FileStateStore, MoniApp, MoniAppConfig, NatsNamespaceQueue,
    SessionManager, StateStore, StaticEngineConfigResolver, parse_channel_bindings,
    run_discord_bot,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "moni=info,warn".into()),
        )
        .init();

    let Some(token) = env::var("MONI_DISCORD_TOKEN").ok() else {
        println!(
            "moni: set MONI_DISCORD_TOKEN, MONI_CHANNELS, and optional MONI_NATS_URL to run the Discord adapter"
        );
        return Ok(());
    };
    let channels = env::var("MONI_CHANNELS").unwrap_or_default();
    let nats_url =
        env::var("MONI_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let cron_tick_seconds = env::var("MONI_CRON_TICK_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(30);
    let workspace_root = env::var("MONI_WORKSPACE_ROOT").unwrap_or_else(|_| {
        dirs_next::home_dir()
            .unwrap_or_else(|| ".".into())
            .join("moni-workspace")
            .to_string_lossy()
            .to_string()
    });
    let engine =
        AgentEngine::from_name(&env::var("MONI_ENGINE").unwrap_or_else(|_| "codex".to_string()))?;
    let codex_app_server = env::var("MONI_CODEX_APP_SERVER")
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let agent_bin = env::var("MONI_AGENT_BIN").unwrap_or_else(|_| engine.as_str().to_string());
    let mut agent_args = env::var("MONI_AGENT_ARGS")
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if codex_app_server && agent_args.is_empty() {
        agent_args = vec!["app-server".to_string(), "--stdio".to_string()];
    }
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
    let allowed_user_ids = env::var("MONI_ALLOWED_USER_IDS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let discord_config = DiscordBotConfig::new(token.clone(), bindings.clone())?
        .with_allowed_user_ids(allowed_user_ids)?;
    let nats_queue = NatsNamespaceQueue::connect(&nats_url).await?;
    let typing = DiscordTypingTracker::default();
    let output = Arc::new(
        DiscordOutputSink::with_registry(token, registry.clone())
            .with_typing_tracker(typing.clone()),
    );
    let protocol = if codex_app_server {
        AgentProtocol::CodexAppServer
    } else {
        AgentProtocol::Line
    };
    let resolver = Arc::new(StaticEngineConfigResolver::new(
        EngineConfig::new(engine, agent_bin)
            .with_args(agent_args)
            .with_protocol(protocol),
    ));
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
        result = run_discord_bot(discord_config, app.clone(), registry, typing) => result,
        result = moni::nats::run_nats_prompt_consumer(nats_queue.client(), app.clone()) => result,
        result = moni::run_cron_loop(app, Duration::from_secs(cron_tick_seconds)) => result,
    }
}
