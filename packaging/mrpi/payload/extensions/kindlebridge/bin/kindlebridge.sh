#!/bin/sh

MANAGER=/mnt/us/kindlebridge/bin/usb-gadget-manager.sh

show() {
    message=$1
    shown=0
    if test -n "${KUAL:-}"; then
        if $KUAL 1 -lm=2 "$message" 2>/dev/null; then
            shown=1
        fi
    fi
    if test "$shown" -eq 0 && command -v eips >/dev/null 2>&1; then
        eips 2 38 "$message" 2>/dev/null || true
    fi
    printf '%s\n' "$message"
}

case "${1:-}" in
    start)
        show "KindleBridge: starting
USB must be unplugged"
        if output=$(sh "$MANAGER" start 0 2>&1); then
            show "KindleBridge: active
Connect USB to the computer"
        else
            show "KindleBridge start failed:
$output"
        fi
        ;;
    stop)
        show "KindleBridge: stopping
USB must be unplugged"
        if output=$(sh "$MANAGER" stop 2>&1); then
            show "KindleBridge: stopped
Connect USB for stock MTP"
        else
            show "KindleBridge stop failed:
$output"
        fi
        ;;
    status)
        status=$(sh "$MANAGER" status 2>&1 || true)
        show "KindleBridge USB:
$status"
        ;;
    apply-staged)
        show "KindleBridge: applying staged daemon
USB must be unplugged"
        if output=$(sh "$MANAGER" apply-staged 2>&1); then
            show "KindleBridge update active
Connect USB to the computer"
        else
            show "KindleBridge update failed:
$output"
        fi
        ;;
    *) show "KindleBridge: invalid action"; exit 2 ;;
esac
