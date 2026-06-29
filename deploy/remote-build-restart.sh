#!/bin/sh
set -eu

REMOTE="${1:-feral@100.84.38.25}"

ssh "$REMOTE" 'sh -s' <<'REMOTE_SCRIPT'
set -eu

SERVICE="com.feral.moni"

cd "$HOME/.local/share/moni/current"
git pull --ff-only origin master
cargo build --release
shasum -a 256 target/release/moni
install -m 755 deploy/moni-run-with-cc-env.sh "$HOME/.local/share/moni/run-with-cc-env.sh"
launchctl kickstart -k "gui/$(id -u)/$SERVICE"
sleep 2
launchctl print "gui/$(id -u)/$SERVICE" 2>/dev/null | sed -n '1,45p'
REMOTE_SCRIPT
