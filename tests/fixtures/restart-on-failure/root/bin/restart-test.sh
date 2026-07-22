#!/bin/sh

attempt_file="$KINDLEBRIDGE_DATA/attempts"
attempt=0
if [ -f "$attempt_file" ]; then
    attempt="$(cat "$attempt_file")"
fi
attempt=$((attempt + 1))
printf '%s\n' "$attempt" > "$attempt_file"
echo "KindleBridge restart fixture attempt $attempt (pid=$$)"
if [ "$attempt" -lt 3 ]; then
    exit 42
fi

trap 'exit 0' HUP INT TERM
while :; do
    sleep 60 &
    wait "$!"
done
