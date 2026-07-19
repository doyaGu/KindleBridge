#!/bin/sh

MNT_US_ROOT=${KINDLEBRIDGE_MNT_US_ROOT:-/mnt/us}
VAR_LOCAL_ROOT=${KINDLEBRIDGE_VAR_LOCAL_ROOT:-/var/local}
MANAGER="$MNT_US_ROOT/kindlebridge/bin/usb-gadget-manager.sh"
VERSION_FILE="$MNT_US_ROOT/kindlebridge/VERSION"
ERROR_LOG="$VAR_LOCAL_ROOT/kindlebridge/last-error.log"
USB_LOG="$VAR_LOCAL_ROOT/kindlebridge/usb.log"

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

failure_code() {
    case "$1" in
        *"Unplug USB before"*) echo E-CABLE ;;
        *"another USB transition is active"*) echo E-BUSY ;;
        *"disabled by"*) echo E-DISABLED ;;
        *"launcher or daemon exited early"*|*"no daemon slot became healthy"*|*"could not select staged daemon"*)
            echo E-DAEMON
            ;;
        *"stock USB owner is not ready"*) echo E-STOCK ;;
        *"no staged daemon update"*) echo E-NOUPDATE ;;
        *) echo E-OTHER ;;
    esac
}

show_failure() {
    action=$1
    output=$2
    code=$(failure_code "$output")
    mkdir -p "$VAR_LOCAL_ROOT/kindlebridge"
    printf '%s\n%s\n%s\n' "$code" "$action" "$output" >"$ERROR_LOG"
    printf '%s KUAL %s failed (%s): %s\n' \
        "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$action" "$code" "$output" >>"$USB_LOG"
    case "$code" in
        E-CABLE)
            show "USB is still connected (E-CABLE).
Unplug it, then tap the action again.
No restart is needed."
            ;;
        E-BUSY)
            show "USB mode is still switching (E-BUSY).
Wait a moment, then check status."
            ;;
        E-DISABLED)
            show "KindleBridge is disabled.
Delete KINDLEBRIDGE_DISABLE
from USB storage, then retry."
            ;;
        E-DAEMON)
            show "Development service failed (E-DAEMON).
Switch to USB file transfer,
then try development mode again."
            ;;
        E-STOCK)
            show "USB handoff is not ready (E-STOCK).
Wait 5 seconds, then retry."
            ;;
        E-NOUPDATE)
            show "No daemon update is staged.
Stage one from the computer first."
            ;;
        *)
            show "$action failed (E-OTHER).
Open status for recovery steps."
            ;;
    esac
}

show_last_failure() {
    code=$1
    case "$code" in
        E-CABLE)
            show "Last action failed: E-CABLE.
Unplug USB, then retry.
USB state was not changed."
            ;;
        E-BUSY)
            show "Last action: E-BUSY.
USB mode was already switching.
Wait, then check status again."
            ;;
        E-DISABLED)
            show "Last action failed: E-DISABLED.
Delete KINDLEBRIDGE_DISABLE,
then retry."
            ;;
        E-DAEMON)
            show "Last action failed: E-DAEMON.
Unplug USB, switch to file transfer,
then try development mode again."
            ;;
        E-STOCK)
            show "Last action failed: E-STOCK.
Wait 5 seconds, then retry."
            ;;
        E-NOUPDATE)
            show "Last action: E-NOUPDATE.
No daemon update was staged."
            ;;
        *)
            show "Last action failed: E-OTHER.
Unplug USB and retry once.
Details remain in the USB log."
            ;;
    esac
    # Full diagnostics remain in usb.log. Acknowledging the result lets the
    # next Status tap show current state instead of repeating stale advice.
    rm -f "$ERROR_LOG"
}

test -x "$MANAGER" || {
    show "KindleBridge is not installed correctly.
Run the MRPI installer again."
    exit 1
}

case "${1:-}" in
    start)
        show "Switching to development mode...
Keep USB unplugged."
        if output=$(sh "$MANAGER" start 0 2>&1); then
            rm -f "$ERROR_LOG"
            show "Development mode is ready.
Connect USB to the computer."
        else
            show_failure "Development mode" "$output"
            exit 1
        fi
        ;;
    stop)
        show "Switching to USB file transfer...
Keep USB unplugged."
        if output=$(sh "$MANAGER" stop 2>&1); then
            rm -f "$ERROR_LOG"
            show "USB file transfer is ready.
Connect USB to the computer."
        else
            show_failure "USB file transfer" "$output"
            exit 1
        fi
        ;;
    status)
        status_output=$(sh "$MANAGER" status 2>&1 || true)
        status=$(printf '%s\n' "$status_output" | sed -n '1p')
        version=$(tr -d '\r\n' <"$VERSION_FILE" 2>/dev/null || echo unknown)
        if test -s "$ERROR_LOG"; then
            code=$(sed -n '1p' "$ERROR_LOG")
            show_last_failure "$code"
            exit 0
        fi
        case "$status" in
            active)
                show "KindleBridge $version
Development mode is ready.
Connect USB to the computer.
PC: kindlebridge device list"
                ;;
            inactive)
                show "KindleBridge $version
USB file transfer is ready.
To develop, keep USB unplugged
and switch to development mode."
                ;;
            recovering)
                show "KindleBridge $version
Development service is recovering.
Wait 10 seconds, then check status."
                ;;
            degraded)
                show "Development service needs recovery.
Unplug USB, switch to file transfer,
then switch to development mode."
                ;;
            detached|stale|stale-from-previous-boot)
                show "USB mode needs recovery.
Unplug USB, switch to file transfer,
then switch to development mode."
                ;;
            acquiring-stock-usb|starting|stopping)
                show "USB mode is still switching.
Wait a moment, then check status again."
                ;;
            *)
                mkdir -p "$VAR_LOCAL_ROOT/kindlebridge"
                printf '%s KUAL status unknown: %s\n' \
                    "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$status_output" >>"$USB_LOG"
                show "USB status is unknown (E-STATUS).
Unplug USB and retry once.
Details were saved to the log."
                ;;
        esac
        ;;
    apply-staged)
        show "Applying staged daemon update...
Keep USB unplugged."
        if output=$(sh "$MANAGER" apply-staged 2>&1); then
            rm -f "$ERROR_LOG"
            show "Developer update is ready.
Connect USB to the computer."
        else
            show_failure "Staged daemon update" "$output"
            exit 1
        fi
        ;;
    *) show "Unknown KindleBridge action.
Open status and recovery steps."; exit 2 ;;
esac
