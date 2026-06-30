#!/bin/sh
set -eu

REMOTE="${1:-feral@100.84.38.25}"
REVISION="${2:-}"
SERVICE="com.feral.moni"

if [ -z "$REVISION" ]; then
  echo "usage: $0 [remote] <git-revision>" >&2
  echo "example: $0 feral@100.84.38.25 78ee654" >&2
  exit 2
fi

case "$REVISION" in
  *[!A-Za-z0-9._/@^-]*)
    echo "rollback revision contains unsupported characters: $REVISION" >&2
    exit 2
    ;;
esac

ssh "$REMOTE" 'sh -s' <<REMOTE_SCRIPT
set -eu

REVISION="$REVISION"
SERVICE="$SERVICE"
CURRENT="\$HOME/.local/share/moni/current"

cd "\$CURRENT"
git fetch origin master
git checkout --detach "\$REVISION"
cargo build --release
shasum -a 256 target/release/moni
install -m 755 deploy/moni-run-with-cc-env.sh "\$HOME/.local/share/moni/run-with-cc-env.sh"
launchctl kickstart -k "gui/\$(id -u)/\$SERVICE"
sleep 2
launchctl print "gui/\$(id -u)/\$SERVICE" 2>/dev/null | sed -n '1,45p'
REMOTE_SCRIPT
