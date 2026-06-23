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
- JSON state persistence for channel bindings and cron tasks via `MONI_STATE_PATH`.
- NATS wildcard consumer that drives persistent per-namespace agent sessions.
- Agent stdout routed back to Discord through the namespace/channel registry.
- Cron scheduled task model that enqueues normal namespace messages.
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
export MONI_ENGINE="codex"
export MONI_AGENT_BIN="codex"
cargo run
```

`MONI_CHANNELS` is comma-separated. Each entry is `discord_channel_id=namespace=repo_url`.

## Current Confidence

The implementation now covers the core replacement path in-process:

Discord message -> NATS namespace subject -> NATS consumer -> persistent agent harness -> stdout -> Discord output sink.

Live NATS validation has been run with Docker:

```bash
docker run -d --rm -p 4224:4222 nats:2-alpine
MONI_TEST_NATS_URL=nats://127.0.0.1:4224 cargo test live_nats_publish_reaches_session_manager_when_configured -- --nocapture
```

The remaining confidence gap is live Discord validation with a real bot token/guild/channel. Unit and integration-style tests cover the runtime seams with mock agents, memory queues, dynamic registration, state persistence, cron, process lifecycle, and live NATS publish/consume behavior.
