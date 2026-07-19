#!/bin/sh

MANAGER=/mnt/us/kindlebridge/bin/usb-gadget-manager.sh
VERSION_FILE=/mnt/us/kindlebridge/VERSION
ERROR_LOG=/var/local/kindlebridge/last-error.log

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

show_failure() {
    action=$1
    output=$2
    mkdir -p /var/local/kindlebridge
    printf '%s\n%s\n' "$action" "$output" >"$ERROR_LOG"
    case "$output" in
        *"Unplug USB before"*)
            show "Unplug the USB cable,
then tap '$action' again.
No restart is needed."
            ;;
        *"disabled by"*)
            show "KindleBridge is disabled.
Delete KINDLEBRIDGE_DISABLE
from USB storage, then retry."
            ;;
        *"launcher or daemon exited early"*)
            show "Development service failed (E-LAUNCH).
Tap Connect for development once more.
Full details were saved to the log."
            ;;
        *"stock USB owner is not ready"*)
            show "USB handoff is not ready (E-STOCK).
Wait 5 seconds, then tap Connect again."
            ;;
        *)
            show "$action failed (E-OTHER).
Open Status / Help.
Full details were saved to the log."
            ;;
    esac
}

test -x "$MANAGER" || {
    show "KindleBridge is not installed correctly.
Run the MRPI installer again."
    exit 1
}

case "${1:-}" in
    start)
        show "Preparing development USB...
Keep the cable unplugged."
        if output=$(sh "$MANAGER" start 0 2>&1); then
            rm -f "$ERROR_LOG"
            show "Development USB is ready.
Connect the cable to the computer."
        else
            show_failure "Connect for development" "$output"
            exit 1
        fi
        ;;
    stop)
        show "Preparing USB file transfer...
Keep the cable unplugged."
        if output=$(sh "$MANAGER" stop 2>&1); then
            rm -f "$ERROR_LOG"
            show "USB file transfer is ready.
Connect the cable to the computer."
        else
            show_failure "Use USB file transfer" "$output"
            exit 1
        fi
        ;;
    status)
        status_output=$(sh "$MANAGER" status 2>&1 || true)
        status=$(printf '%s\n' "$status_output" | sed -n '1p')
        version=$(tr -d '\r\n' <"$VERSION_FILE" 2>/dev/null || echo unknown)
        case "$status" in
            active)
                show "KindleBridge $version
Development USB is ready.
Connect the cable and run KindleBridge."
                ;;
            inactive)
                show "KindleBridge $version
USB file transfer mode is active.
To develop: unplug the cable,
then choose Connect for development."
                ;;
            detached|stale|stale-from-previous-boot)
                show "USB needs recovery.
Unplug the cable, choose USB file transfer,
then Connect for development."
                ;;
            acquiring-stock-usb|starting|stopping)
                show "KindleBridge is still working.
Wait a moment, then open Status / Help again."
                ;;
            *)
                show "KindleBridge status is unknown:
$status_output"
                ;;
        esac
        ;;
    apply-staged)
        show "Applying the developer update...
Keep the cable unplugged."
        if output=$(sh "$MANAGER" apply-staged 2>&1); then
            show "Developer update is ready.
Connect the cable to the computer."
        else
            show_failure "Apply daemon update" "$output"
            exit 1
        fi
        ;;
    *) show "Unknown KindleBridge action.
Open Status / Help."; exit 2 ;;
esac
