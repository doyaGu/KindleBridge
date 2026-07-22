#!/bin/sh

echo "KindleBridge forced-stop fixture started (pid=$$)"
trap '' HUP INT TERM
while :; do
    sleep 60 || true
done
