#!/bin/sh
set -eu

OLD_PLIST="$HOME/Library/LaunchAgents/com.feral.cc-discord.plist"
OLD_GITKB="$HOME/.local/share/cc-discord/run-with-gitkb-env.sh"

plist_value() {
  /usr/libexec/PlistBuddy -c "Print :EnvironmentVariables:$1" "$OLD_PLIST" 2>/dev/null || true
}

OLD_PATH="$(plist_value PATH)"
if [ -n "$OLD_PATH" ]; then
  export PATH="$OLD_PATH"
fi

export MONI_DISCORD_TOKEN="$(plist_value DISCORD_BOT_TOKEN)"
CHANNEL_ID="$(plist_value DISCORD_NOTIFY_CHANNEL_ID)"
ALLOWED_USER_IDS="$(plist_value DISCORD_ALLOWED_USER_IDS)"
CODEX_BIN="$(plist_value CODEX_BIN)"

export MONI_NATS_URL="nats://127.0.0.1:4222"
export MONI_CHANNELS="${CHANNEL_ID}=money-brain=https://github.com/Gonzih/money-brain.git"
export MONI_STATE_PATH="$HOME/.local/share/moni/state.json"
export MONI_WORKSPACE_ROOT="$HOME"
export MONI_ENGINE="codex"
export MONI_AGENT_BIN="${CODEX_BIN:-/opt/homebrew/bin/codex}"
export MONI_CODEX_APP_SERVER="1"
export MONI_ALLOWED_USER_IDS="$ALLOWED_USER_IDS"
export MONI_CRON_TICK_SECONDS="30"
export RUST_LOG="moni=info,warn"

export GITKB_DOMAIN="gitkb.com"
if [ -f "$OLD_GITKB" ]; then
  GITKB_TOKEN_LINE="$(awk -F= '/^export GITKB_TOKEN=/{print $2; exit}' "$OLD_GITKB")"
  if [ -n "$GITKB_TOKEN_LINE" ]; then
    export GITKB_TOKEN="$GITKB_TOKEN_LINE"
  fi
fi

exec "$HOME/.local/share/moni/current/target/release/moni"
