#!/bin/sh
set -eu

BASE=/var/local/kindlebridge
PIDFILE="$BASE/run/kindlebridged.pid"

if [ ! -f "$PIDFILE" ]; then
    exit 0
fi

PID=$(sed -n '1p' "$PIDFILE")
if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    WAIT=0
    while kill -0 "$PID" 2>/dev/null && [ "$WAIT" -lt 30 ]; do
        sleep 1
        WAIT=$((WAIT + 1))
    done
    if kill -0 "$PID" 2>/dev/null; then
        kill -KILL "$PID"
    fi
fi
rm -f "$PIDFILE"
