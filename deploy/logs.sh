#!/bin/sh
set -eu

REMOTE="${1:-feral@100.84.38.25}"
LINES="${2:-120}"

case "$LINES" in
  *[!0-9]*|'')
    echo "usage: $0 [remote] [line_count]" >&2
    exit 2
    ;;
esac

ssh "$REMOTE" 'sh -s' <<REMOTE_SCRIPT
set -eu

LINES="$LINES"

echo "== moni.log =="
tail -n "\$LINES" "\$HOME/Library/Logs/moni.log" 2>/dev/null || true
echo "== moni.error.log =="
tail -n "\$LINES" "\$HOME/Library/Logs/moni.error.log" 2>/dev/null || true
REMOTE_SCRIPT
