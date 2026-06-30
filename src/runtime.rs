use std::{env, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::discord::{DiscordGateway, SerenityDiscordGateway, run_discord_bot_with_gateway};
use crate::{
    AgentEngine, AgentProtocol, BindingRegistry, CronEngine, DiscordBotConfig,
    DiscordLiveEditConfig, DiscordOutputSink, DiscordTypingTracker, EngineConfig,
    FileRunHistoryStore, FileStateStore, MoniApp, MoniAppConfig, NatsNamespaceQueue, RunHistory,
    SessionManager, StateStore, StaticEngineConfigResolver, VoiceTranscriber,
    parse_channel_bindings,
};

const MISSING_TOKEN_MESSAGE: &str = "moni: set MONI_DISCORD_TOKEN, MONI_CHANNELS, and optional MONI_NATS_URL to run the Discord adapter";

trait EnvSource {
    fn var(&self, key: &str) -> Option<String>;
}

struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn var(&self, key: &str) -> Option<String> {
        env::var(key).ok()
    }
}

struct RuntimeParts {
    discord_config: DiscordBotConfig,
    app: Arc<MoniApp>,
    registry: BindingRegistry,
    typing: DiscordTypingTracker,
    nats_queue: NatsNamespaceQueue,
    cron_tick_seconds: u64,
    live_edit_config: DiscordLiveEditConfig,
}

fn default_workspace_root() -> String {
    workspace_root_from_home(dirs_next::home_dir())
}

fn workspace_root_from_home(home: Option<PathBuf>) -> String {
    let root = match home {
        Some(home) => home,
        None => PathBuf::from("."),
    };
    root.join("moni-workspace").to_string_lossy().to_string()
}

fn optional_duration_ms<E: EnvSource + Sync>(
    env: &E,
    key: &str,
    default: Duration,
) -> anyhow::Result<Duration> {
    let Some(value) = env.var(key) else {
        return Ok(default);
    };
    let millis = value.parse::<u64>().map_err(|err| {
        anyhow::anyhow!("{key} must be a positive integer millisecond value: {err}")
    })?;
    if millis == 0 {
        anyhow::bail!("{key} must be greater than zero");
    }
    Ok(Duration::from_millis(millis))
}

fn live_edit_config_from_env<E: EnvSource + Sync>(
    env: &E,
) -> anyhow::Result<DiscordLiveEditConfig> {
    let defaults = DiscordLiveEditConfig::default();
    DiscordLiveEditConfig::new(
        optional_duration_ms(env, "MONI_LIVE_EDIT_INTERVAL_MS", defaults.min_interval())?,
        optional_duration_ms(
            env,
            "MONI_LIVE_EDIT_INITIAL_BACKOFF_MS",
            defaults.initial_backoff(),
        )?,
        optional_duration_ms(env, "MONI_LIVE_EDIT_MAX_BACKOFF_MS", defaults.max_backoff())?,
    )
}

#[async_trait]
trait RuntimeRunner: Send + Sync {
    async fn voice_transcriber(&self) -> anyhow::Result<VoiceTranscriber>;

    async fn run(&self, parts: RuntimeParts) -> anyhow::Result<()>;
}

struct RealRuntimeRunner<G> {
    discord_gateway: G,
}

impl<G> RealRuntimeRunner<G> {
    fn new(discord_gateway: G) -> Self {
        Self { discord_gateway }
    }
}

#[async_trait]
impl<G> RuntimeRunner for RealRuntimeRunner<G>
where
    G: DiscordGateway,
{
    async fn voice_transcriber(&self) -> anyhow::Result<VoiceTranscriber> {
        VoiceTranscriber::from_env()
    }

    async fn run(&self, parts: RuntimeParts) -> anyhow::Result<()> {
        tracing::info!(
            live_edit_interval_ms = parts.live_edit_config.min_interval().as_millis(),
            live_edit_initial_backoff_ms = parts.live_edit_config.initial_backoff().as_millis(),
            live_edit_max_backoff_ms = parts.live_edit_config.max_backoff().as_millis(),
            "configured Discord live edit policy"
        );
        tokio::select! {
            result = run_discord_bot_with_gateway(parts.discord_config, parts.app.clone(), parts.registry, parts.typing, &self.discord_gateway) => result,
            result = crate::nats::run_nats_prompt_consumer(parts.nats_queue.client(), parts.app.clone()) => result,
            result = crate::run_cron_loop(parts.app, Duration::from_secs(parts.cron_tick_seconds)) => result,
        }
    }
}

pub async fn run_from_env() -> anyhow::Result<()> {
    let runner = RealRuntimeRunner::new(SerenityDiscordGateway::new(None));
    run_with_env_and_runner(&ProcessEnv, &runner).await
}

async fn run_with_env_and_runner<E, R>(env: &E, runner: &R) -> anyhow::Result<()>
where
    E: EnvSource + Sync,
    R: RuntimeRunner,
{
    let Some(token) = env.var("MONI_DISCORD_TOKEN") else {
        println!("{MISSING_TOKEN_MESSAGE}");
        return Ok(());
    };
    let channels = env.var("MONI_CHANNELS").unwrap_or_default();
    let nats_url = env
        .var("MONI_NATS_URL")
        .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
    let cron_tick_seconds = env
        .var("MONI_CRON_TICK_SECONDS")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(30);
    let live_edit_config = live_edit_config_from_env(env)?;
    let workspace_root = env
        .var("MONI_WORKSPACE_ROOT")
        .unwrap_or_else(default_workspace_root);
    let engine = AgentEngine::from_name(
        &env.var("MONI_ENGINE")
            .unwrap_or_else(|| "codex".to_string()),
    )?;
    let codex_app_server = env
        .var("MONI_CODEX_APP_SERVER")
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let agent_bin = env
        .var("MONI_AGENT_BIN")
        .unwrap_or_else(|| engine.as_str().to_string());
    let mut agent_args = env
        .var("MONI_AGENT_ARGS")
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if codex_app_server && agent_args.is_empty() {
        agent_args = vec!["app-server".to_string(), "--stdio".to_string()];
    }
    let state_path = env.var("MONI_STATE_PATH");
    let state_store = state_path
        .clone()
        .map(|path| Arc::new(FileStateStore::new(path)) as Arc<dyn StateStore>);
    let run_history_path = env
        .var("MONI_RUN_HISTORY_PATH")
        .or_else(|| state_path.map(|path| format!("{path}.runs.json")))
        .unwrap_or_else(|| {
            PathBuf::from(&workspace_root)
                .join("run-history.json")
                .to_string_lossy()
                .to_string()
        });
    let run_history = Arc::new(
        RunHistory::from_store(Arc::new(FileRunHistoryStore::new(run_history_path))).await?,
    );

    let mut bindings = parse_channel_bindings(&channels)?;
    let cron_tasks = if let Some(store) = &state_store {
        let state = store.load().await?;
        bindings.extend(state.bindings);
        state.cron_tasks
    } else {
        Vec::new()
    };
    let registry = BindingRegistry::new(bindings.clone())?;
    let allowed_user_ids = env
        .var("MONI_ALLOWED_USER_IDS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let slash_guild_ids = env
        .var("MONI_DISCORD_SLASH_GUILD_IDS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let voice_transcriber = match runner.voice_transcriber().await {
        Ok(transcriber) => Some(transcriber),
        Err(err) => {
            tracing::warn!(error = %err, "voice transcription disabled");
            None
        }
    };
    let voice_status = voice_transcriber
        .as_ref()
        .map(VoiceTranscriber::status_report)
        .unwrap_or_else(|| {
            "voice transcription unavailable - whisper.cpp is not configured".to_string()
        });
    let discord_config = DiscordBotConfig::new(token.clone(), bindings.clone())
        .expect("bindings were validated before Discord bot config construction")
        .with_allowed_user_ids(allowed_user_ids)?
        .with_default_category_id(env.var("MONI_DEFAULT_CATEGORY_ID"))?
        .with_slash_guild_ids(slash_guild_ids)?
        .with_voice_transcriber(voice_transcriber);
    let nats_queue = NatsNamespaceQueue::connect(&nats_url).await?;
    let typing = DiscordTypingTracker::default();
    let output = Arc::new(
        DiscordOutputSink::with_registry(token, registry.clone())
            .with_live_edit_config(live_edit_config)
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
    let sessions = Arc::new(
        SessionManager::new(workspace_root.into(), resolver, output.clone())
            .with_run_history(run_history),
    );
    let app = Arc::new(MoniApp::new(MoniAppConfig {
        queue: Arc::new(nats_queue.clone()),
        sessions,
        output,
        cron: CronEngine::new(cron_tasks),
        registry: registry.clone(),
        state_store,
        voice_status: Some(voice_status),
    }));

    runner
        .run(RuntimeParts {
            discord_config,
            app,
            registry,
            typing,
            nats_queue,
            cron_tick_seconds,
            live_edit_config,
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        future::Future,
        path::Path,
        pin::Pin,
        sync::{Arc, Mutex},
    };

    use serenity::model::gateway::GatewayIntents;
    use serenity::model::id::ChannelId;
    use tempfile::TempDir;

    use crate::{
        ChannelBinding, CronTask, CronTaskStatus, MoniDiscordHandler, MoniState, store::StateStore,
    };

    use super::*;

    type RuntimeAssertion =
        fn(RuntimeParts) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

    #[derive(Default)]
    struct MapEnv {
        values: HashMap<String, String>,
    }

    impl MapEnv {
        fn with(mut self, key: &str, value: impl ToString) -> Self {
            self.values.insert(key.to_string(), value.to_string());
            self
        }
    }

    impl EnvSource for MapEnv {
        fn var(&self, key: &str) -> Option<String> {
            self.values.get(key).cloned()
        }
    }

    struct AssertingRunner {
        voice: anyhow::Result<VoiceTranscriber>,
        assert: Option<RuntimeAssertion>,
    }

    #[async_trait]
    impl RuntimeRunner for AssertingRunner {
        async fn voice_transcriber(&self) -> anyhow::Result<VoiceTranscriber> {
            match &self.voice {
                Ok(transcriber) => Ok(transcriber.clone()),
                Err(err) => anyhow::bail!(err.to_string()),
            }
        }

        async fn run(&self, parts: RuntimeParts) -> anyhow::Result<()> {
            let assert = self.assert.expect("runner assertion configured");
            assert(parts).await
        }
    }

    #[derive(Default)]
    struct ImmediateDiscordGateway {
        calls: Arc<Mutex<Vec<(String, GatewayIntents)>>>,
    }

    #[serenity::async_trait]
    impl DiscordGateway for ImmediateDiscordGateway {
        async fn start(
            &self,
            token: String,
            intents: GatewayIntents,
            _handler: MoniDiscordHandler,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push((token, intents));
            Ok(())
        }
    }

    fn transcriber(dir: &Path) -> VoiceTranscriber {
        VoiceTranscriber::new(
            dir.join("whisper"),
            dir.join("ffmpeg"),
            dir.join("curl"),
            dir.join("ggml-small.en.bin"),
            dir,
        )
    }

    fn nats_url() -> String {
        std::env::var("MONI_TEST_NATS_URL").unwrap_or("nats://127.0.0.1:4223".to_string())
    }

    #[test]
    fn workspace_root_defaults_from_home_or_current_directory() {
        assert_eq!(
            workspace_root_from_home(Some(PathBuf::from("/tmp/home"))),
            "/tmp/home/moni-workspace"
        );
        assert_eq!(workspace_root_from_home(None), "./moni-workspace");
    }

    #[tokio::test]
    async fn missing_token_returns_without_running_services() {
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        run_with_env_and_runner(&MapEnv::default(), &runner)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn builds_codex_app_server_runtime_from_env_and_state() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.json");
        let mut task = CronTask::new("state", "https://example.com/state", "* * * * *", "run");
        task.status = CronTaskStatus::Paused;
        let store = FileStateStore::new(&state_path);
        store
            .save(&MoniState {
                bindings: vec![ChannelBinding {
                    channel_id: "2".to_string(),
                    namespace: "state".to_string(),
                    repo_url: "https://example.com/state".to_string(),
                }],
                cron_tasks: vec![task],
            })
            .await
            .unwrap();
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_NATS_URL", nats_url())
            .with("MONI_CRON_TICK_SECONDS", "7")
            .with(
                "MONI_WORKSPACE_ROOT",
                dir.path().join("workspace").display(),
            )
            .with("MONI_ENGINE", "codex")
            .with("MONI_CODEX_APP_SERVER", "true")
            .with("MONI_AGENT_ARGS", "")
            .with("MONI_ALLOWED_USER_IDS", "42, 43")
            .with("MONI_DEFAULT_CATEGORY_ID", "55")
            .with("MONI_DISCORD_SLASH_GUILD_IDS", "66, 77")
            .with("MONI_LIVE_EDIT_INTERVAL_MS", "111")
            .with("MONI_LIVE_EDIT_INITIAL_BACKOFF_MS", "222")
            .with("MONI_LIVE_EDIT_MAX_BACKOFF_MS", "444")
            .with("MONI_STATE_PATH", state_path.display());
        let runner = AssertingRunner {
            voice: Ok(transcriber(dir.path())),
            assert: Some(|parts| {
                Box::pin(async move {
                    assert_eq!(parts.cron_tick_seconds, 7);
                    assert_eq!(
                        parts.live_edit_config.min_interval(),
                        Duration::from_millis(111)
                    );
                    assert_eq!(
                        parts.live_edit_config.initial_backoff(),
                        Duration::from_millis(222)
                    );
                    assert_eq!(
                        parts.live_edit_config.max_backoff(),
                        Duration::from_millis(444)
                    );
                    assert_eq!(parts.discord_config.allowed_user_ids, vec!["42", "43"]);
                    assert_eq!(
                        parts.discord_config.default_category_id,
                        Some(ChannelId::new(55))
                    );
                    assert_eq!(
                        parts.discord_config.slash_guild_ids,
                        vec![
                            serenity::model::id::GuildId::new(66),
                            serenity::model::id::GuildId::new(77)
                        ]
                    );
                    assert!(parts.discord_config.voice_transcriber.is_some());
                    assert_eq!(parts.discord_config.bindings.len(), 2);
                    assert_eq!(parts.app.cron_count().await, 1);
                    assert!(
                        parts
                            .registry
                            .get_by_channel(ChannelId::new(2))
                            .await
                            .is_some()
                    );
                    parts.nats_queue.client().flush().await.unwrap();
                    Ok(())
                })
            }),
        };

        run_with_env_and_runner(&env, &runner).await.unwrap();
    }

    #[tokio::test]
    async fn invalid_state_file_fails_before_runtime_start() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.json");
        std::fs::write(&state_path, "not-json").unwrap();
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_STATE_PATH", state_path.display());
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(err.to_string().contains("expected ident"));
    }

    #[tokio::test]
    async fn invalid_state_binding_fails_registry_build() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.json");
        let store = FileStateStore::new(&state_path);
        store
            .save(&MoniState {
                bindings: vec![ChannelBinding {
                    channel_id: "not-a-channel".to_string(),
                    namespace: "state".to_string(),
                    repo_url: "https://example.com/state".to_string(),
                }],
                cron_tasks: Vec::new(),
            })
            .await
            .unwrap();
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_STATE_PATH", state_path.display());
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(err.to_string().contains("invalid digit"));
    }

    #[tokio::test]
    async fn invalid_discord_allowed_user_fails_before_runtime_start() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_ALLOWED_USER_IDS", "not-a-user");
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(err.to_string().contains("invalid digit"));
    }

    #[tokio::test]
    async fn invalid_default_category_fails_before_runtime_start() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_DEFAULT_CATEGORY_ID", "not-a-category");
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(err.to_string().contains("invalid digit"));
    }

    #[tokio::test]
    async fn invalid_slash_guild_id_fails_before_runtime_start() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_DISCORD_SLASH_GUILD_IDS", "not-a-guild");
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(err.to_string().contains("invalid digit"));
    }

    #[tokio::test]
    async fn invalid_live_edit_interval_fails_before_runtime_start() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_LIVE_EDIT_INTERVAL_MS", "0");
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(err.to_string().contains("MONI_LIVE_EDIT_INTERVAL_MS"));
    }

    #[tokio::test]
    async fn invalid_live_edit_backoff_parse_fails_before_runtime_start() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_LIVE_EDIT_INITIAL_BACKOFF_MS", "slow");
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(
            err.to_string()
                .contains("MONI_LIVE_EDIT_INITIAL_BACKOFF_MS")
        );
    }

    #[tokio::test]
    async fn invalid_live_edit_max_backoff_fails_before_runtime_start() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_LIVE_EDIT_INITIAL_BACKOFF_MS", "500")
            .with("MONI_LIVE_EDIT_MAX_BACKOFF_MS", "100");
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(err.to_string().contains("max backoff"));
    }

    #[tokio::test]
    async fn invalid_nats_url_fails_before_runtime_start() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_NATS_URL", "not-a-url");
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("missing voice tools")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn unknown_engine_fails_before_runtime_start() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_ENGINE", "bogus");
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("voice should not be loaded")),
            assert: None,
        };

        let err = run_with_env_and_runner(&env, &runner).await.unwrap_err();

        assert!(err.to_string().contains("bogus"));
    }

    #[tokio::test]
    async fn builds_line_runtime_with_defaults_when_optional_env_is_missing() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_NATS_URL", nats_url())
            .with("MONI_CRON_TICK_SECONDS", "0")
            .with("MONI_ENGINE", "claude")
            .with("MONI_CODEX_APP_SERVER", "false")
            .with("MONI_AGENT_ARGS", "--json");
        let runner = AssertingRunner {
            voice: Err(anyhow::anyhow!("missing voice tools")),
            assert: Some(|parts| {
                Box::pin(async move {
                    assert_eq!(parts.cron_tick_seconds, 30);
                    assert_eq!(parts.live_edit_config, DiscordLiveEditConfig::default());
                    assert!(parts.discord_config.allowed_user_ids.is_empty());
                    assert!(parts.discord_config.default_category_id.is_none());
                    assert!(parts.discord_config.slash_guild_ids.is_empty());
                    assert!(parts.discord_config.voice_transcriber.is_none());
                    assert_eq!(parts.discord_config.bindings.len(), 1);
                    parts.nats_queue.client().flush().await.unwrap();
                    Ok(())
                })
            }),
        };

        run_with_env_and_runner(&env, &runner).await.unwrap();
    }

    #[tokio::test]
    async fn real_runner_returns_when_discord_gateway_returns() {
        let env = MapEnv::default()
            .with("MONI_DISCORD_TOKEN", "token")
            .with("MONI_CHANNELS", "1=moni=https://example.com/moni")
            .with("MONI_NATS_URL", nats_url());
        let gateway = ImmediateDiscordGateway::default();
        let calls = gateway.calls.clone();
        let runner = RealRuntimeRunner::new(gateway);

        run_with_env_and_runner(&env, &runner).await.unwrap();

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "token");
        assert!(calls[0].1.contains(GatewayIntents::MESSAGE_CONTENT));
    }
}
