#!/bin/sh

echo "KindleBridge lifecycle v2 fixture started (pid=$$)"
trap 'echo "KindleBridge lifecycle v2 fixture stopping"; exit 0' HUP INT TERM
while :; do
    sleep 60 &
    wait "$!"
done
