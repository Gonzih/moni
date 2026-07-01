# moni

Rust successor to `cc-discord`.

`moni` is the Discord-native harness for local and cloud coding agents. It keeps the useful core of `cc-discord`: Discord message routing, persistent engine processes, output routed back to humans and observers, and cron scheduled tasks. It deliberately does not bake in loop semantics; future workflow behavior should compose on top of the queue and harness boundary.

## First Implementation Slice

- Engine-agnostic process harness for Claude and Codex-compatible binaries.
- Per-channel namespace message model.
- NATS message bus for namespace input.
- In-memory bus implementation for integration tests.
- Discord gateway adapter that routes registered channel messages into NATS.
- Dynamic `/register <namespace> <repo_url>` support from Discord.
- Discord `/model <name>` command for per-namespace model selection.
- Discord `/status` and `/voice status` diagnostics.
- JSON state persistence for channel bindings and cron tasks via `MONI_STATE_PATH`.
- NATS wildcard consumer that drives persistent per-namespace agent sessions.
- Agent stdout routed back to Discord through the namespace/channel registry.
- Cron scheduled task model that enqueues normal namespace messages.
- Runtime cron ticker for persisted schedules.
- Atomic JSON state writes.
- Discord output chunking and send retries.
- Structured Codex app-server JSON rendering with compact tool status blocks, final metadata, usage, model, and duration.
- Recent per-namespace run history persisted as JSON.
- Optional Discord user allowlist.
- Optional Codex app-server JSON-RPC session protocol via stdio.
- Mockable process boundary for tests.

## Product Direction

`moni` should supersede `cc-discord` as the focused Discord implementation:

- Discord is the human-facing control surface.
- Harnesses wrap agent binaries and stream back output.
- Cron schedules tasks by injecting messages into the same path as Discord.
- NATS is the transport boundary for this new iteration.
- Human Attention Steering is the workflow principle: agents run until they need human input for a blocker, credential, irreversible action, or architectural decision.

## Running the Discord Adapter

```bash
export MONI_DISCORD_TOKEN="..."
export MONI_NATS_URL="nats://127.0.0.1:4222"
export MONI_CHANNELS="123456789=moni=https://github.com/example/moni"
export MONI_STATE_PATH="$HOME/.config/moni/state.json"
export MONI_RUN_HISTORY_PATH="$HOME/.config/moni/run-history.json"
export MONI_ENGINE="codex"
export MONI_AGENT_BIN="codex"
export MONI_CODEX_APP_SERVER="1"
export MONI_ALLOWED_USER_IDS="111111111111111111,222222222222222222"
export MONI_DISCORD_SLASH_GUILD_IDS="333333333333333333"
export MONI_CRON_TICK_SECONDS="30"
export MONI_LIVE_EDIT_INTERVAL_MS="900"
export MONI_LIVE_EDIT_INITIAL_BACKOFF_MS="1500"
export MONI_LIVE_EDIT_MAX_BACKOFF_MS="60000"
export MONI_VOICE_PROMPT_TEMPLATE="[voice note - transcription may contain typos]: {content}"
export MONI_VOICE_MAX_BYTES="26214400"
export MONI_VOICE_MAX_DURATION_SECONDS="600"
export RUST_LOG="moni=info,warn"
cargo run
```

`MONI_CHANNELS` is comma-separated. Each entry is `discord_channel_id=namespace=repo_url`.

Useful Discord commands:

- `/status`: show namespace, repo, session, model, cron count, queue depth, NATS availability, live-output health, voice health, and last run summary.
- `/voice status`: show whisper.cpp, ffmpeg, curl, model path, and voice guardrail health.
- `/goal <prompt>` or `/loop <prompt>`: run an explicit long-running goal/loop prompt. Codex receives `/goal <prompt>`; Claude receives `/loop <prompt>`.
- `/model <name>`: select the model for this namespace and restart the active session.
- `/reset`, `/clear`, `/compact`: session lifecycle controls.
- `/cron ...`: manage namespace cron tasks.
- `/channel repo:<url>` or `channel for https://github.com/org/repo`: create a routed channel for a repo.

Plain Discord messages are forwarded as plain agent prompts. Moni does not wrap
every message in `/goal`; goal/loop mode is opt-in through `/goal` or `/loop`.

## Deployment

`moni` is not published to crates.io yet. The production deployment uses the GitHub
checkout on the remote machine and builds with Cargo there:

```bash
./deploy/remote-build-restart.sh
```

That script runs the clean deploy path on `feral@100.84.38.25`:

```bash
cd ~/.local/share/moni/current
git pull --ff-only origin master
cargo build --release
launchctl kickstart -k gui/$(id -u)/com.feral.moni
```

Operational helpers use the same remote default:

```bash
./deploy/status.sh
./deploy/logs.sh
./deploy/rollback.sh feral@100.84.38.25 78ee654
```

`rollback.sh` requires an explicit Git revision, checks it out on the remote,
rebuilds the release binary, reinstalls the launchd wrapper, and restarts the
service.

The launchd wrapper executes:

```bash
~/.local/share/moni/current/target/release/moni
```

Use crates.io later if `moni` becomes a general-purpose installable crate. For
this service, the GitHub checkout keeps the deploy tied to the restored
cc-discord credentials, channel mappings, and launchd wrapper.

Runtime environment:

- `MONI_DISCORD_TOKEN`: Discord bot token.
- `MONI_CHANNELS`: initial static bindings. Persisted bindings from `MONI_STATE_PATH` are merged on startup.
- `MONI_NATS_URL`: NATS URL, default `nats://127.0.0.1:4222`.
- `MONI_STATE_PATH`: JSON state path for dynamic channel bindings and cron tasks.
- `MONI_RUN_HISTORY_PATH`: JSON path for recent per-namespace runs. Defaults to `${MONI_STATE_PATH}.runs.json` when `MONI_STATE_PATH` is set, otherwise `${MONI_WORKSPACE_ROOT}/run-history.json`.
- `MONI_WORKSPACE_ROOT`: parent directory for per-namespace workspaces, default `$HOME/moni-workspace`.
- `MONI_ENGINE`: `codex` or `claude`, default `codex`.
- `MONI_AGENT_BIN`: agent executable, default matches `MONI_ENGINE`.
- `MONI_AGENT_ARGS`: whitespace-separated agent arguments. For line-mode Codex, the current unsafe automation flag is `--dangerously-bypass-approvals-and-sandbox`.
- `MONI_CODEX_APP_SERVER`: when `1`, `true`, or `yes`, runs Codex through `codex app-server --stdio`, starts a JSON-RPC thread, submits Discord/NATS prompts with `turn/start`, renders structured JSON tool/status/final events, and records run history.
- `MONI_ALLOWED_USER_IDS`: optional comma-separated Discord user IDs. Empty means any non-bot user in a reachable channel can interact with the runner.
- `MONI_DISCORD_SLASH_GUILD_IDS`: optional comma-separated guild IDs for immediate slash command publication. Empty registers global commands, which Discord can take time to propagate.
- `MONI_CRON_TICK_SECONDS`: cron polling interval, default `30`.
- `MONI_LIVE_EDIT_INTERVAL_MS`: minimum interval between live Discord message edits, default `900`.
- `MONI_LIVE_EDIT_INITIAL_BACKOFF_MS`: first retry delay after a live edit failure, default `1500`.
- `MONI_LIVE_EDIT_MAX_BACKOFF_MS`: maximum live edit retry delay, default `60000`.
- `MONI_VOICE_PROMPT_TEMPLATE`: prompt wrapper for voice messages. `{content}` is replaced with the optional caption plus transcript.
- `MONI_VOICE_MAX_BYTES`: maximum accepted/downloaded voice attachment size in bytes, default `26214400`.
- `MONI_VOICE_MAX_DURATION_SECONDS`: maximum converted 16 kHz mono WAV duration estimate, default `600`.
- `RUST_LOG`: tracing filter, default `moni=info,warn`.

## Current Confidence

The implementation now covers the core replacement path in-process:

Discord message -> NATS namespace subject -> NATS consumer -> persistent agent harness -> stdout -> Discord output sink.

Live NATS validation has been run with Docker:

```bash
docker run -d --rm -p 4224:4222 nats:2-alpine
MONI_TEST_NATS_URL=nats://127.0.0.1:4224 cargo test live_nats_publish_reaches_session_manager_when_configured -- --nocapture
```

Unit and integration-style tests are the verification boundary. They cover the runtime seams with mock agents, memory queues, dynamic registration, state persistence, run history, cron, process lifecycle, Discord command routing, Discord output formatting, voice guardrails, authorization config, Codex app-server JSON-RPC session flow, and live NATS publish/consume behavior.

## Known Replacement Gaps

- NATS usage is core pub/sub, not JetStream durable delivery.
- Codex app-server support handles initialize, thread start, turn start, assistant deltas, and turn completion; approval/input request routing is not implemented yet.
- Voice message transcription shells out through whisper.cpp now; full Discord voice-channel chat integration is not implemented yet.
- Final outputs are persisted before Discord delivery, but automatic replay after a transient Discord outage is not implemented yet.
