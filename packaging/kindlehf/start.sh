#!/bin/sh
set -eu

BASE=/var/local/kindlebridge
DISABLE=/mnt/us/KINDLEBRIDGE_DISABLE
PIDFILE="$BASE/run/kindlebridged.pid"
LOGFILE="$BASE/logs/kindlebridged.log"

if [ -e "$DISABLE" ]; then
    echo "KindleBridge is disabled by $DISABLE" >&2
    exit 1
fi

if [ -f "$PIDFILE" ]; then
    OLD_PID=$(sed -n '1p' "$PIDFILE")
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        exit 0
    fi
    rm -f "$PIDFILE"
fi

DAEMON="$BASE/current/bin/kindlebridged"
if [ ! -x "$DAEMON" ]; then
    echo "KindleBridge active slot is missing or invalid" >&2
    exit 1
fi

"$DAEMON" >>"$LOGFILE" 2>&1 &
PID=$!
echo "$PID" >"$PIDFILE.tmp.$$"
mv -f "$PIDFILE.tmp.$$" "$PIDFILE"
