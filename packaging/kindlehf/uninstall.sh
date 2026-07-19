#!/bin/sh
set -eu

BASE=/var/local/kindlebridge
DATA=/mnt/us/kindlebridge-data

if [ "$(id -u)" -ne 0 ]; then
    echo "KindleBridge uninstall requires root" >&2
    exit 1
fi
if [ -x "$BASE/launcher/stop.sh" ]; then
    "$BASE/launcher/stop.sh"
fi

rm -rf "$BASE"

if [ "${1:-}" = "--purge" ]; then
    rm -rf "$DATA"
else
    echo "Preserved application data at $DATA"
fi
