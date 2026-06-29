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
ALLOWED_USER_IDS="$(plist_value DISCORD_ALLOWED_USER_IDS)"
CODEX_BIN="$(plist_value CODEX_BIN)"

export MONI_NATS_URL="nats://127.0.0.1:4222"
export MONI_CHANNELS="1514478248057110618=cron=https://github.com/gonzih/cron,1514524404745240609=cc-wire=https://github.com/gonzih/cc-wire,1514658379384229938=money-brain=https://github.com/gonzih/money-brain,1514659822794969309=simorgh-mobile-app=https://github.com/gonzih/simorgh-mobile-app,1514676507124170885=of-stack=https://github.com/gonzih/of-stack,1514676615668305971=simorgh-web-app=https://github.com/gonzih/simorgh-web-app,1514743825736798369=metaweb-future-path=https://github.com/gonzih/metaweb-future-path,1514785858887352400=nexus-souls=https://github.com/gonzih/nexus-souls,1517279248023290017=recruitment=https://github.com/gonzih/recruitment,1517331011187511398=nexus-research=https://github.com/gonzih/nexus-research,1517695980949078048=cc-suite-tests=https://github.com/gonzih/cc-suite-tests,1517709847053734051=cc-suite=https://github.com/gonzih/cc-suite,1518796075572396123=harmony=https://github.com/gitkb/harmony"
export MONI_STATE_PATH="$HOME/.local/share/moni/state.json"
export MONI_WORKSPACE_ROOT="$HOME/cc-discord-workspace"
export MONI_ENGINE="codex"
export MONI_AGENT_BIN="${CODEX_BIN:-/opt/homebrew/bin/codex}"
export MONI_CODEX_APP_SERVER="1"
export MONI_ALLOWED_USER_IDS="$ALLOWED_USER_IDS"
export MONI_CRON_TICK_SECONDS="30"
export RUST_LOG="moni=info,warn"

export GITKB_DOMAIN="gitkb.com"
if [ -f "$OLD_GITKB" ]; then
  GITKB_TOKEN_LINE="$(/usr/bin/awk -F= '/^export GITKB_TOKEN=/{print $2; exit}' "$OLD_GITKB")"
  if [ -n "$GITKB_TOKEN_LINE" ]; then
    export GITKB_TOKEN="$GITKB_TOKEN_LINE"
  fi
fi

exec "$HOME/.local/share/moni/current/target/release/moni"
