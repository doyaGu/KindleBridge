#!/bin/sh

MNT_US_ROOT=${KINDLEBRIDGE_MNT_US_ROOT:-/mnt/us}
VAR_LOCAL_ROOT=${KINDLEBRIDGE_VAR_LOCAL_ROOT:-/var/local}
MANAGER="$VAR_LOCAL_ROOT/kindlebridge/control/bin/usb-gadget-manager.sh"
VERSION_FILE="$VAR_LOCAL_ROOT/kindlebridge/control/VERSION"
ERROR_LOG="$VAR_LOCAL_ROOT/kindlebridge/last-error.log"
USB_LOG="$VAR_LOCAL_ROOT/kindlebridge/usb.log"
DIAGNOSTICS_FILE="$MNT_US_ROOT/kindlebridge-diagnostics.txt"
PROC_ROOT=${KINDLEBRIDGE_PROC_ROOT:-/proc}
SYS_ROOT=${KINDLEBRIDGE_SYS_ROOT:-/sys}
DEV_ROOT=${KINDLEBRIDGE_DEV_ROOT:-/dev}

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
        *"USBNetLite owns USB"*) echo E-OWNER ;;
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
            show "USB is connected (E-CABLE).
Unplug it, then retry.
No restart is needed."
            ;;
        E-BUSY)
            show "USB is switching (E-BUSY).
Wait, then check status."
            ;;
        E-DISABLED)
            show "KindleBridge is disabled.
Delete KINDLEBRIDGE_DISABLE
from USB storage, then retry."
            ;;
        E-OWNER)
            show "USB is used by USBNetLite (E-OWNER).
In USBNetLite, turn USBNetwork off,
then retry."
            ;;
        E-DAEMON)
            show "Service failed (E-DAEMON).
Choose USB file transfer.
Then try development mode."
            ;;
        E-STOCK)
            show "USB is not ready (E-STOCK).
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
        E-OWNER)
            show "Last action failed: E-OWNER.
Turn USBNetwork off in USBNetLite,
then retry KindleBridge."
            ;;
        E-DAEMON)
            show "Last action failed: E-DAEMON.
Unplug USB.
Choose file transfer, then retry."
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

export_diagnostics() {
    temp="$VAR_LOCAL_ROOT/kindlebridge/.diagnostics.$$"
    mkdir -p "$VAR_LOCAL_ROOT/kindlebridge"
    umask 077
    {
        echo "KindleBridge diagnostics v1"
        echo "captured=$(date '+%Y-%m-%dT%H:%M:%S%z')"
        if test -f "$VERSION_FILE"; then
            echo "version=$(tr -d '\r\n' <"$VERSION_FILE")"
        else
            echo "version=unknown"
        fi
        echo
        echo "[manager status]"
        if test -x "$MANAGER"; then
            sh "$MANAGER" status 2>&1 || true
        else
            echo "manager=missing"
        fi
        echo
        echo "[system]"
        uname -a 2>&1 || true
        uptime 2>&1 || true
        echo "boot_id=$(cat "$PROC_ROOT/sys/kernel/random/boot_id" 2>/dev/null || echo unknown)"
        echo
        echo "[usb gadget]"
        for file in "$SYS_ROOT/kernel/config/usb_gadget/mtpgadget/UDC" \
            "$SYS_ROOT/class/udc"/*/state; do
            test -e "$file" || continue
            printf '%s=' "$file"
            cat "$file" 2>/dev/null || true
        done
        ls -la "$SYS_ROOT/kernel/config/usb_gadget/mtpgadget/configs/c.1" 2>&1 || true
        mount 2>&1 | sed -n '/functionfs\|configfs/p' || true
        ls -la "$DEV_ROOT/usb-ffs/kbp" 2>&1 || true
        echo
        echo "[installed files]"
        for file in \
            "$VAR_LOCAL_ROOT/kindlebridge/control/bin/kindlebridge-launcher" \
            "$VAR_LOCAL_ROOT/kindlebridge/control/runtime/slots/A/bin/kindlebridged" \
            "$VAR_LOCAL_ROOT/kindlebridge/control/runtime/slots/B/bin/kindlebridged"; do
            test -f "$file" || continue
            if command -v sha256sum >/dev/null 2>&1; then
                sha256sum "$file" 2>&1 || true
            else
                ls -l "$file" 2>&1 || true
            fi
        done
        echo
        echo "[managed processes]"
        for role in launcher daemon; do
            if test "$role" = launcher; then
                pid=$(cat "$VAR_LOCAL_ROOT/kindlebridge/usb/launcher_pid" 2>/dev/null || true)
            else
                pid=$(cat "$VAR_LOCAL_ROOT/kindlebridge/control/runtime/run/daemon.pid" 2>/dev/null || true)
            fi
            echo "$role.pid=${pid:-missing}"
            case "$pid" in
                ''|*[!0-9]*) continue ;;
            esac
            test -d "$PROC_ROOT/$pid" || { echo "$role.process=missing"; continue; }
            printf '%s.cmdline=' "$role"
            tr '\000' ' ' <"$PROC_ROOT/$pid/cmdline" 2>/dev/null || true
            echo
            sed -n '1,24p' "$PROC_ROOT/$pid/status" 2>/dev/null || true
            for task in "$PROC_ROOT/$pid/task"/*; do
                test -d "$task" || continue
                tid=${task##*/}
                printf 'thread.%s.wchan=' "$tid"
                cat "$task/wchan" 2>/dev/null || true
                printf 'thread.%s.stack=' "$tid"
                tr '\n' ' ' <"$task/stack" 2>/dev/null || true
                echo
            done
            ls -l "$PROC_ROOT/$pid/fd" 2>&1 || true
        done
        echo
        echo "[recent log]"
        tail -n 300 "$USB_LOG" 2>&1 || true
    } >"$temp"
    if ! mv "$temp" "$DIAGNOSTICS_FILE"; then
        rm -f "$temp"
        return 1
    fi
    sync
}

ACTION=${1:-}
case "$ACTION" in
    export-diagnostics) ;;
    start|stop|status|apply-staged)
        test -x "$MANAGER" || {
            show "KindleBridge is not installed correctly.
Run the MRPI installer again."
            exit 1
        }
        ;;
    *)
        show "Unknown KindleBridge action.
Open status and recovery steps."
        exit 2
        ;;
esac

case "$ACTION" in
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
        reason=$(printf '%s\n' "$status_output" | sed -n 's/^reason=//p' | sed -n '1p')
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
                case "$reason" in
                    daemon-restarted)
                        show "Development service stopped.
Unplug USB.
Choose file transfer.
Then choose development mode."
                        ;;
                    health-monitor)
                        show "Health check is not running.
Unplug USB.
Choose file transfer.
Then choose development mode."
                        ;;
                    watchdog-halted)
                        show "Service keeps failing.
Unplug USB.
Choose file transfer.
Then export diagnostics."
                        ;;
                    *)
                        show "Development mode needs recovery.
Unplug USB.
Choose file transfer.
Then choose development mode."
                        ;;
                esac
                ;;
            detached|stale|stale-from-previous-boot)
                show "USB mode needs recovery.
Unplug USB.
Choose file transfer.
Then choose development mode."
                ;;
            acquiring-stock-usb|starting|stopping)
                show "USB mode is switching.
Wait, then check status again."
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
        if ! output=$(sh "$MANAGER" preflight apply-staged 2>&1); then
            show_failure "Staged daemon update" "$output"
            exit 1
        fi
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
    export-diagnostics)
        if export_diagnostics; then
            show "Diagnostics saved.
Switch to USB file transfer,
then open kindlebridge-diagnostics.txt."
        else
            show "Could not export diagnostics (E-DIAG).
USB mode was not changed."
            exit 1
        fi
        ;;
esac
