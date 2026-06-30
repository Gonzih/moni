#!/bin/sh
set -eu

REMOTE="${1:-feral@100.84.38.25}"
SERVICE="com.feral.moni"

ssh "$REMOTE" 'sh -s' <<REMOTE_SCRIPT
set -eu

SERVICE="$SERVICE"
CURRENT="\$HOME/.local/share/moni/current"

cd "\$CURRENT"
printf 'revision: '
git rev-parse --short HEAD
printf 'binary: '
shasum -a 256 target/release/moni 2>/dev/null || printf 'missing\n'
launchctl print "gui/\$(id -u)/\$SERVICE" 2>/dev/null | sed -n '1,45p'
REMOTE_SCRIPT
