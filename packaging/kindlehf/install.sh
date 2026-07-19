#!/bin/sh
set -eu

BASE=/var/local/kindlebridge
DATA=/mnt/us/kindlebridge-data
SOURCE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SLOT=${KINDLEBRIDGE_INSTALL_SLOT:-A}

case "$SLOT" in
    A|B) ;;
    *) echo "invalid slot: $SLOT" >&2; exit 2 ;;
esac

if [ "$(id -u)" -ne 0 ]; then
    echo "KindleBridge installation requires root" >&2
    exit 1
fi

if [ ! -w /var/local ]; then
    echo "/var/local is not writable" >&2
    exit 1
fi

if [ -e "$BASE/current" ] || [ -L "$BASE/current" ]; then
    echo "an active slot already exists; use the brokered A/B updater" >&2
    exit 1
fi

umask 077
mkdir -p \
    "$BASE/launcher" \
    "$BASE/slots/$SLOT/bin" \
    "$BASE/config" \
    "$BASE/keys" \
    "$BASE/hosts" \
    "$BASE/audit" \
    "$BASE/run" \
    "$BASE/logs" \
    "$BASE/transactions" \
    "$BASE/blocks" \
    "$BASE/profiles" \
    "$BASE/activations/generations" \
    "$DATA/apps" \
    "$DATA/exports" \
    "$DATA/packages"

install -m 0755 "$SOURCE_DIR/payload/kindlebridged" "$BASE/slots/$SLOT/bin/kindlebridged"
install -m 0755 "$SOURCE_DIR/payload/kindlebridge-broker" "$BASE/slots/$SLOT/bin/kindlebridge-broker"
install -m 0755 "$SOURCE_DIR/start.sh" "$BASE/launcher/start.sh"
install -m 0755 "$SOURCE_DIR/stop.sh" "$BASE/launcher/stop.sh"

ln -s "slots/$SLOT" "$BASE/.current.new.$$"
mv "$BASE/.current.new.$$" "$BASE/current"

if [ -x "$BASE/current/bin/kindlebridged" ]; then
    "$BASE/current/bin/kindlebridged" >"$BASE/logs/install-smoke.json"
else
    echo "installed daemon is not executable" >&2
    exit 1
fi

echo "KindleBridge installed in slot $SLOT"
echo "Start on demand with $BASE/launcher/start.sh"
echo "Persistent startup remains disabled until a validated device adapter is installed."
