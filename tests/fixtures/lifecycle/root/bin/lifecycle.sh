#!/bin/sh

echo "KindleBridge lifecycle fixture started (pid=$$)"
trap 'echo "KindleBridge lifecycle fixture stopping"; exit 0' HUP INT TERM
while :; do
    sleep 60 &
    wait "$!"
done
