#!/bin/sh
# KindleBridge-owned KT6 USB mode manager. Stock volumd/HAL owns the MTP
# lifecycle. This script never calls USBNetLite, KindleRoot, or MTU3 controls.

set -eu

MNT_US_ROOT=${KINDLEBRIDGE_MNT_US_ROOT:-/mnt/us}
BASE_US_ROOT=${KINDLEBRIDGE_BASE_US_ROOT:-/mnt/base-us}
VAR_LOCAL_ROOT=${KINDLEBRIDGE_VAR_LOCAL_ROOT:-/var/local}
SYS_ROOT=${KINDLEBRIDGE_SYS_ROOT:-/sys}
PROC_ROOT=${KINDLEBRIDGE_PROC_ROOT:-/proc}
PID_PROC_ROOT=${KINDLEBRIDGE_PID_PROC_ROOT:-/proc}
DEV_ROOT=${KINDLEBRIDGE_DEV_ROOT:-/dev}
TMP_ROOT=${KINDLEBRIDGE_TMP_ROOT:-/tmp}

BASE="$VAR_LOCAL_ROOT/kindlebridge/control"
LAUNCHER="$BASE/bin/kindlebridge-launcher"
RUNTIME="$BASE/runtime"
DAEMON_PID_FILE="$RUNTIME/run/daemon.pid"
STATE="$VAR_LOCAL_ROOT/kindlebridge/usb"
LOCK="$TMP_ROOT/kindlebridge-usb.lock"
LOG="$VAR_LOCAL_ROOT/kindlebridge/usb.log"
DISABLE="$MNT_US_ROOT/KINDLEBRIDGE_DISABLE"
GADGET="$SYS_ROOT/kernel/config/usb_gadget/mtpgadget"
CONFIG="$GADGET/configs/c.1"
FUNCTION="$GADGET/functions/ffs.kbp"
LINK="$CONFIG/ffs.kbp"
USBNET_NCM_LINK="$CONFIG/ncm.usbnetlite"
USBNET_RNDIS_LINK="$CONFIG/rndis.usbnetlite"
MOUNT="$DEV_ROOT/usb-ffs/kbp"
UDC_CLASS="$SYS_ROOT/class/udc"
BOOT_ID_FILE="$PROC_ROOT/sys/kernel/random/boot_id"
USID_FILE="$PROC_ROOT/usid"
MODULES_FILE="$PROC_ROOT/modules"
MOUNTS_FILE="$PROC_ROOT/mounts"
DEFAULT_UDC=11211000.usb
DISCONNECT_SETTLE_SECONDS=2
STOCK_WAIT_SECONDS=15

log() {
    mkdir -p "$VAR_LOCAL_ROOT/kindlebridge"
    printf '%s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*" >>"$LOG"
}

pid_is() {
    pid=$1
    pattern=$2
    test -n "$pid" || return 1
    test -r "$PID_PROC_ROOT/$pid/cmdline" || return 1
    grep -q "$pattern" "$PID_PROC_ROOT/$pid/cmdline" 2>/dev/null
}

pid_entrypoint() {
    pid=$1
    test -n "$pid" || return 1
    test -r "$PID_PROC_ROOT/$pid/cmdline" || return 1
    executable=$(tr '\000' '\n' <"$PID_PROC_ROOT/$pid/cmdline" 2>/dev/null | sed -n '1p')
    case "$executable" in
        */sh | */bash)
            tr '\000' '\n' <"$PID_PROC_ROOT/$pid/cmdline" 2>/dev/null | sed -n '2p'
            ;;
        *) printf '%s\n' "$executable" ;;
    esac
}

launcher_pid_is_owned() {
    test "$(pid_entrypoint "$1")" = "$LAUNCHER"
}

daemon_pid_is_owned() {
    entrypoint=$(pid_entrypoint "$1") || return 1
    case "$entrypoint" in
        "$RUNTIME"/slots/*/kindlebridged) return 0 ;;
        *) return 1 ;;
    esac
}

owned_pid_is() {
    role=$1
    pid=$2
    case "$role" in
        launcher) launcher_pid_is_owned "$pid" ;;
        daemon) daemon_pid_is_owned "$pid" ;;
        *) return 1 ;;
    esac
}

terminate_owned_pid() {
    role=$1
    pid=$2
    owned_pid_is "$role" "$pid" || return 0
    kill "$pid" 2>/dev/null || true
    attempts=5
    while test "$attempts" -gt 0 && owned_pid_is "$role" "$pid"; do
        sleep 1
        attempts=$((attempts - 1))
    done
    if owned_pid_is "$role" "$pid"; then
        log "$role process $pid ignored TERM; sending KILL"
        kill -9 "$pid" 2>/dev/null || true
        sleep 1
    fi
    if owned_pid_is "$role" "$pid"; then
        log "$role process $pid did not exit"
        return 1
    fi
}

current_daemon_pid() {
    cat "$DAEMON_PID_FILE" 2>/dev/null || read_state daemon_pid ''
}

wait_for_daemon() {
    launcher_pid=$1
    attempts=10
    while test "$attempts" -gt 0; do
        daemon_pid=$(current_daemon_pid)
        if daemon_pid_is_owned "$daemon_pid"; then
            printf '%s\n' "$daemon_pid"
            return 0
        fi
        launcher_pid_is_owned "$launcher_pid" || return 1
        attempts=$((attempts - 1))
        test "$attempts" -eq 0 || sleep 1
    done
    return 1
}

heartbeat_instance() {
    sed -n '2s/^instance=//p' "$RUNTIME/run/heartbeat" 2>/dev/null || true
}

watchdog_is_halted() {
    grep -q '^halted=1$' "$RUNTIME/launcher/watchdog-state" 2>/dev/null
}

heartbeat_is_fresh() {
    heartbeat="$RUNTIME/run/heartbeat"
    test "$(sed -n '1p' "$heartbeat" 2>/dev/null)" = KINDLEBRIDGE_HEARTBEAT_V1 || return 1
    test -n "$(heartbeat_instance)" || return 1
    timestamp_ms=$(sed -n '3s/^timestamp_ms=//p' "$heartbeat" 2>/dev/null || true)
    case "$timestamp_ms" in
        ''|*[!0-9]*) return 1 ;;
    esac
    now_seconds=$(date +%s 2>/dev/null || true)
    case "$now_seconds" in
        ''|*[!0-9]*) return 1 ;;
    esac
    heartbeat_seconds=$((timestamp_ms / 1000))
    test "$heartbeat_seconds" -le "$now_seconds" || return 1
    slot=$(cat "$RUNTIME/current" 2>/dev/null || true)
    case "$slot" in
        A|B) ;;
        *) return 1 ;;
    esac
    timeout_ms=$(sed -n 's/^heartbeat_timeout_ms=//p' \
        "$RUNTIME/slots/$slot/slot.manifest" 2>/dev/null || true)
    case "$timeout_ms" in
        ''|*[!0-9]*) timeout_ms=10000 ;;
    esac
    # Round the selected slot's liveness window up because the packaged
    # BusyBox date reports whole seconds.
    timeout_seconds=$(((timeout_ms + 999) / 1000))
    age_seconds=$((now_seconds - heartbeat_seconds))
    test "$age_seconds" -le "$timeout_seconds"
}

wait_for_active_daemon_ready() {
    previous_instance=$1
    # Three no-heartbeat startup attempts take just over 30 seconds with the
    # packaged manifest. Keep USB unbound until the launcher either confirms
    # the staged slot healthy or has started the rolled-back slot.
    attempts=45
    while test "$attempts" -gt 0; do
        launcher_pid=$(read_state launcher_pid '')
        daemon_pid=$(current_daemon_pid)
        instance=$(heartbeat_instance)
        if daemon_pid_is_owned "$daemon_pid" &&
            test -n "$instance" && test "$instance" != "$previous_instance" &&
            test ! -f "$RUNTIME/launcher/pending-slot"; then
            printf '%s\n' "$daemon_pid" >"$STATE/daemon_pid"
            return 0
        fi
        launcher_pid_is_owned "$launcher_pid" || return 1
        attempts=$((attempts - 1))
        test "$attempts" -eq 0 || sleep 1
    done
    return 1
}

select_sync_root() {
    SYNC_ROOT="$MNT_US_ROOT/kindlebridge-data"
    # Recent Kindle firmware exposes user storage through the `fsp` FUSE
    # daemon. Its 64 KiB max_write path is useful for stock content handling
    # but needlessly CPU-bounds development transfers. Use the same storage's
    # mounted ext4 backing directory when both sides of that layout are
    # positively identified; older and future layouts keep the public path.
    if test -d "$BASE_US_ROOT" &&
        grep -Fqs " $MNT_US_ROOT fuse.fsp " "$MOUNTS_FILE" &&
        grep -Fqs " $BASE_US_ROOT " "$MOUNTS_FILE"; then
        SYNC_ROOT="$BASE_US_ROOT/kindlebridge-data"
    fi
}

launch_supervised_daemon() {
    serial=$1
    select_sync_root
    # MRPI invokes installers from its temporary staging mount. Never let the
    # persistent supervisor inherit that cwd, or it pins MRPI's tmpfs after a
    # successful installation and prevents clean package-manager teardown.
    (
        cd "$BASE" || exit 1
        exec "$LAUNCHER" run --root "$RUNTIME" -- \
            serve-usb --functionfs-dir "$MOUNT" --serial "$serial" \
            --sync-root "$SYNC_ROOT"
    ) >>"$LOG" 2>&1 &
    launcher_pid=$!
    printf '%s\n' "$launcher_pid" >"$STATE/launcher_pid"
    kill -0 "$launcher_pid" 2>/dev/null || return 1
    daemon_pid=$(wait_for_daemon "$launcher_pid") || return 1
    printf '%s\n' "$daemon_pid" >"$STATE/daemon_pid"
}

select_staged_slot() {
    test -f "$RUNTIME/next" || return 0
    selected=$("$LAUNCHER" select-staged --root "$RUNTIME") || return 1
    log "$selected; activation will be verified before USB bind"
}

acquire_lock() {
    if mkdir "$LOCK" 2>/dev/null; then
        printf '%s\n' "$$" >"$LOCK/pid"
        return 0
    fi
    owner=$(cat "$LOCK/pid" 2>/dev/null || true)
    if ! pid_is "$owner" 'usb-gadget-manager.sh'; then
        rm -rf "$LOCK"
        mkdir "$LOCK" 2>/dev/null || return 1
        printf '%s\n' "$$" >"$LOCK/pid"
        return 0
    fi
    return 1
}

release_lock() {
    owner=$(cat "$LOCK/pid" 2>/dev/null || true)
    if test "$owner" = "$$"; then
        rm -rf "$LOCK"
    fi
}

read_state() {
    name=$1
    fallback=$2
    if test -f "$STATE/$name"; then
        cat "$STATE/$name"
    else
        printf '%s\n' "$fallback"
    fi
}

current_boot_id() {
    cat "$BOOT_ID_FILE" 2>/dev/null || printf '%s\n' unknown
}

udc_state() {
    udc=$1
    cat "$UDC_CLASS/$udc/state" 2>/dev/null || printf '%s\n' unknown
}

usb_connected() {
    udc=$1
    if test -r "$UDC_CLASS/$udc/connected"; then
        cat "$UDC_CLASS/$udc/connected"
        return
    fi
    case "$(udc_state "$udc")" in
        'not attached') printf '%s\n' 0 ;;
        *) printf '%s\n' 1 ;;
    esac
}

require_unplugged() {
    udc=$1
    action=$2
    connected=$(usb_connected "$udc")
    test "$connected" = 0 && return 0
    echo "Unplug USB before $action (connected=$connected, state=$(udc_state "$udc"))" >&2
    return 1
}

module_is_loaded() {
    grep -q "^$1 " "$MODULES_FILE"
}

stock_mtp_function_present() {
    test -L "$CONFIG/ffs.mtp" && return 0
    test "${KINDLEBRIDGE_TEST_ALLOW_MTP_DIRECTORY:-0}" = 1 &&
        test -d "$CONFIG/ffs.mtp"
}

volumd_network_mode() {
    lipc-get-prop -i -e -- com.lab126.volumd useUsbForNetwork 2>/dev/null |
        tr -d '\r\n'
}

mtp_is_started() {
    lipc-get-prop -i -e -- com.lab126.mtp isMtpStarted 2>/dev/null |
        tr -d '\r\n'
}

send_hal_event() {
    lipc-send-event -r 3 -d 2 com.lab126.hal "$1" >/dev/null
}

wait_for_stock_network() {
    attempts=$STOCK_WAIT_SECONDS
    while test "$attempts" -gt 0; do
        if stock_network_ready; then
            return 0
        fi
        attempts=$((attempts - 1))
        test "$attempts" -eq 0 || sleep 1
    done
    log "volumd did not release stock MTP to g_ether within ${STOCK_WAIT_SECONDS}s"
    return 1
}

stock_network_ready() {
    bound=$(cat "$GADGET/UDC" 2>/dev/null || true)
    test "$(volumd_network_mode || true)" = 1 &&
        module_is_loaded g_ether && test -z "$bound"
}

acquire_usb_from_volumd() {
    if stock_network_ready; then
        log "volumd USB network state was already ready for handoff"
        return 0
    fi
    if ! stock_mtp_owner_ready; then
        log "refusing USB acquisition from an indeterminate stock state"
        return 1
    fi

    log "requesting stock MTP shutdown through volumd"
    lipc-set-prop -i -- com.lab126.volumd useUsbForNetwork 1 >/dev/null
    send_hal_event usbUnconfigured
    send_hal_event usbPlugOut
    sleep 2
    wait_for_stock_network
}

wait_for_stock_mtp_owner() {
    attempts=$STOCK_WAIT_SECONDS
    while test "$attempts" -gt 0; do
        if stock_mtp_owner_ready; then
            return 0
        fi
        attempts=$((attempts - 1))
        test "$attempts" -eq 0 || sleep 1
    done
    log "volumd did not reclaim stock MTP within ${STOCK_WAIT_SECONDS}s"
    return 1
}

stock_mtp_owner_ready() {
    test "$(volumd_network_mode || true)" = 0 &&
        ! module_is_loaded g_ether && test "$(mtp_is_started || true)" = 1
}

usbnetlite_owns_usb() {
    test -e "$USBNET_NCM_LINK" || test -L "$USBNET_NCM_LINK" ||
        test -e "$USBNET_RNDIS_LINK" || test -L "$USBNET_RNDIS_LINK"
}

return_usb_to_volumd() {
    # volumd's supported network-to-MTP path expects g_ether to own the
    # unbound UDC. Recreate that intermediate state only while unplugged.
    if stock_mtp_owner_ready; then
        log "volumd already owns stock MTP"
        return 0
    fi

    network_mode=$(volumd_network_mode || true)
    if test "$network_mode" = 0 && ! module_is_loaded g_ether; then
        log "volumd is already reclaiming stock MTP; waiting without changing USB"
        wait_for_stock_mtp_owner
        return
    fi

    bound=$(cat "$GADGET/UDC" 2>/dev/null || true)
    if test -n "$bound" && ! module_is_loaded g_ether; then
        log "refusing to replace an unknown gadget bound to $bound"
        return 1
    fi
    if test -z "$bound" && ! module_is_loaded g_ether; then
        modprobe g_ether
    fi
    module_is_loaded g_ether || {
        log "g_ether is unavailable for the volumd handback"
        return 1
    }
    if module_is_loaded g_ether; then
        ifconfig usb0 down 2>/dev/null || true
    fi

    log "returning USB ownership to stock volumd"
    lipc-set-prop -i -- com.lab126.volumd useUsbForNetwork 0 >/dev/null
    sleep 1
    send_hal_event usbUnconfigured
    sleep 2
    send_hal_event usbPlugOut
    sleep 2
    wait_for_stock_mtp_owner
}

unbind_bridge_gadget() {
    test -e "$GADGET/UDC" || return 0
    bound=$(cat "$GADGET/UDC" 2>/dev/null || true)
    test -n "$bound" || return 0
    if ! printf '\n' >"$GADGET/UDC"; then
        log "failed to unbind KindleBridge gadget from $bound"
        return 1
    fi
    test -z "$(cat "$GADGET/UDC" 2>/dev/null || true)" || {
        log "KindleBridge gadget still reports a bound UDC after unbind"
        return 1
    }
    if test "${KINDLEBRIDGE_TEST_AFTER_UNBIND_DELAY:-0}" != 0; then
        sleep "$KINDLEBRIDGE_TEST_AFTER_UNBIND_DELAY"
    fi
}

bind_bridge_gadget() {
    udc=$1
    if ! printf '%s\n' "$udc" >"$GADGET/UDC"; then
        log "KindleBridge composite failed to bind to $udc"
        return 1
    fi
    bound=$(cat "$GADGET/UDC" 2>/dev/null || true)
    test "$bound" = "$udc" || {
        log "KindleBridge requested $udc but configfs reports ${bound:-unbound}"
        return 1
    }
    log "KindleBridge composite bound to $udc; host state $(udc_state "$udc")"
}

cleanup_bridge_payload() {
    launcher_pid=$(read_state launcher_pid '')
    daemon_pid=$(current_daemon_pid)
    # Stop the supervisor before making FunctionFS inactive. Otherwise the
    # daemon observes the intentional teardown as a crash and the still-live
    # launcher can respawn it between our PID snapshot and cleanup.
    terminate_owned_pid launcher "$launcher_pid" || return 1
    daemon_pid=$(current_daemon_pid)
    terminate_owned_pid daemon "$daemon_pid" || return 1
    if test -L "$LINK"; then
        unbind_bridge_gadget || return 1
    elif test -e "$LINK"; then
        log "refusing to remove unexpected non-symlink $LINK"
        return 1
    fi
    rm -f "$DAEMON_PID_FILE"
    rm -f "$RUNTIME/run/heartbeat" "$RUNTIME/launcher/watchdog-state"
    rm -f "$LINK" 2>/dev/null || { log "failed to remove KindleBridge config link"; return 1; }
    if grep -q " $MOUNT " "$MOUNTS_FILE" 2>/dev/null; then
        umount "$MOUNT" 2>/dev/null || { log "failed to unmount $MOUNT"; return 1; }
    fi
    rmdir "$MOUNT" 2>/dev/null || true
    if test -d "$FUNCTION"; then
        rmdir "$FUNCTION" 2>/dev/null || { log "failed to remove $FUNCTION"; return 1; }
    fi
}

clear_state() {
    rm -rf "$STATE"
}

stop_bridge() {
    test -d "$STATE" || {
        log "stop requested while inactive"
        return 0
    }
    state_boot_id=$(read_state boot_id '')
    if test -n "$state_boot_id" && test "$state_boot_id" != "$(current_boot_id)"; then
        clear_state
        log "discarded KindleBridge state from an earlier boot"
        return 0
    fi
    watchdog_pid=$(read_state watchdog_pid '')

    printf '%s\n' stopping >"$STATE/mode"
    cleanup_bridge_payload || return 1
    return_usb_to_volumd || return 1

    if test "$watchdog_pid" != "$$" && pid_is "$watchdog_pid" 'usb-gadget-manager.sh'; then
        kill "$watchdog_pid" 2>/dev/null || true
    fi
    clear_state
    log "KindleBridge USB stopped; ownership returned to volumd"
}

restore_after() {
    timeout=$1
    sleep "$timeout"
    acquire_lock || exit 0
    trap 'release_lock' EXIT
    trap 'release_lock; exit 1' HUP INT TERM
    log "USB safety timeout expired after ${timeout}s"
    udc=$(read_state udc "$DEFAULT_UDC")
    if ! require_unplugged "$udc" 'automatic restore'; then
        log "automatic restore deferred because the USB host is still attached"
        return 0
    fi
    stop_bridge
}

finish_start() {
    result=$?
    trap - EXIT HUP INT TERM
    if test "${ROLLBACK_NEEDED:-0}" = 1 && test -d "$STATE"; then
        log "rolling back incomplete USB transition through volumd"
        stop_bridge || true
    fi
    release_lock
    exit "$result"
}

start_bridge() {
    timeout=$1
    case "$timeout" in
        ''|*[!0-9]*) echo "timeout must be an integer" >&2; return 2 ;;
    esac
    if test "$timeout" -ne 0 && { test "$timeout" -lt 60 || test "$timeout" -gt 86400; }; then
        echo "timeout must be 0 (disabled) or between 60 and 86400 seconds" >&2
        return 2
    fi
    test "$(id -u)" = 0 || { echo "must run as root" >&2; return 1; }
    if usbnetlite_owns_usb; then
        echo "USBNetLite owns USB; switch it to USB file transfer first" >&2
        return 1
    fi
    if test "$(status 2>/dev/null | sed -n '1p')" = active; then
        echo "KindleBridge USB is already active"
        return 0
    fi
    test ! -e "$DISABLE" || { echo "disabled by $DISABLE" >&2; return 1; }
    test -x "$LAUNCHER" || { echo "missing launcher: $LAUNCHER" >&2; return 1; }
    test -f "$RUNTIME/current" || { echo "missing launcher slot pointer" >&2; return 1; }
    test -d "$GADGET" || { echo "stock mtpgadget is missing" >&2; return 1; }
    stock_mtp_function_present || { echo "stock MTP function is missing" >&2; return 1; }
    if ! stock_mtp_owner_ready && ! stock_network_ready; then
        echo "stock USB owner is not ready (volumd=$(volumd_network_mode || echo unknown), mtp=$(mtp_is_started || echo unknown))" >&2
        return 1
    fi
    udc=$(ls "$UDC_CLASS" | head -n 1)
    test -n "$udc" || udc=$DEFAULT_UDC
    test -d "$UDC_CLASS/$udc" || { echo "USB controller is unavailable: $udc" >&2; return 1; }
    require_unplugged "$udc" 'starting KindleBridge' || return 1

    # A manual Start is an explicit retry boundary. A previous crash fuse must
    # stop an unattended loop, not permanently block the next user-requested
    # session after the transport has been repaired.
    if test ! -d "$STATE"; then
        rm -f "$RUNTIME/launcher/watchdog-state" "$DAEMON_PID_FILE" \
            "$RUNTIME/run/heartbeat"
    fi

    mkdir -p "$VAR_LOCAL_ROOT/kindlebridge"
    acquire_lock || { echo "another USB transition is active" >&2; return 1; }
    ROLLBACK_NEEDED=0
    trap 'finish_start' EXIT
    trap 'exit 1' HUP INT TERM

    if test -d "$STATE"; then
        state_boot_id=$(read_state boot_id '')
        launcher_pid=$(read_state launcher_pid '')
        daemon_pid=$(current_daemon_pid)
        if test -n "$state_boot_id" && test "$state_boot_id" != "$(current_boot_id)"; then
            log "discarding KindleBridge state from an earlier boot"
            clear_state
        elif test "$(read_state mode '')" = active &&
            launcher_pid_is_owned "$launcher_pid" &&
            daemon_pid_is_owned "$daemon_pid"; then
            echo "KindleBridge USB is already active" >&2
            return 1
        else
            log "recovering stale KindleBridge transition through volumd"
            stop_bridge
        fi
    fi

    mkdir "$STATE"
    ROLLBACK_NEEDED=1
    current_boot_id >"$STATE/boot_id"
    printf '%s\n' acquiring-stock-usb >"$STATE/mode"
    printf '%s\n' "$udc" >"$STATE/udc"
    acquire_usb_from_volumd || return 1

    printf '%s\n' starting >"$STATE/mode"
    mkdir "$FUNCTION"
    mkdir -p "$MOUNT"
    mount -t functionfs kbp "$MOUNT"
    serial=$(tr -d '\000' <"$USID_FILE")
    select_staged_slot || { echo "could not select staged daemon" >&2; return 1; }
    previous_instance=$(heartbeat_instance)
    launch_supervised_daemon "$serial" || { echo "kindlebridge launcher or daemon exited early" >&2; return 1; }
    wait_for_active_daemon_ready "$previous_instance" || {
        echo "no daemon slot became healthy before USB bind" >&2
        return 1
    }
    ln -s "$FUNCTION" "$LINK"

    if test "$timeout" -gt 0; then
        self=$(readlink -f "$0")
        nohup sh "$self" restore-after "$timeout" </dev/null >>"$LOG" 2>&1 &
        printf '%s\n' "$!" >"$STATE/watchdog_pid"
    fi

    ifconfig usb0 down 2>/dev/null || true
    rmmod g_ether
    module_is_loaded g_ether && { echo "g_ether did not unload" >&2; return 1; }
    sleep "$DISCONNECT_SETTLE_SECONDS"
    bind_bridge_gadget "$udc" || return 1
    printf '%s\n' active >"$STATE/mode"
    if test "$timeout" -eq 0; then
        log "KindleBridge USB active for serial $serial; safety timeout disabled"
    else
        log "KindleBridge USB active for serial $serial; safety timeout ${timeout}s"
    fi
    ROLLBACK_NEEDED=0
    trap - EXIT HUP INT TERM
    release_lock
}

stop_command() {
    if test ! -d "$STATE"; then
        rm -f "$RUNTIME/launcher/watchdog-state" "$DAEMON_PID_FILE" \
            "$RUNTIME/run/heartbeat"
        echo "KindleBridge USB is already inactive"
        return 0
    fi
    if test -d "$STATE"; then
        udc=$(read_state udc "$DEFAULT_UDC")
        require_unplugged "$udc" 'stopping KindleBridge' || return 1
    fi
    acquire_lock || { echo "another USB transition is active" >&2; return 1; }
    trap 'release_lock' EXIT
    trap 'release_lock; exit 1' HUP INT TERM
    stop_bridge
    trap - EXIT HUP INT TERM
    release_lock
}

status() {
    if test -d "$STATE"; then
        state_boot_id=$(read_state boot_id '')
        if test -n "$state_boot_id" && test "$state_boot_id" != "$(current_boot_id)"; then
            echo stale-from-previous-boot
            return 1
        fi
        launcher_pid=$(read_state launcher_pid '')
        daemon_pid=$(current_daemon_pid)
        if test "$(read_state mode '')" = active &&
            launcher_pid_is_owned "$launcher_pid" &&
            daemon_pid_is_owned "$daemon_pid"; then
            udc=$(read_state udc "$DEFAULT_UDC")
            bound=$(cat "$GADGET/UDC" 2>/dev/null || true)
            if test "$bound" != "$udc"; then
                echo detached
                echo "serial=$(tr -d '\000' <"$USID_FILE") link=unbound"
                echo "slot=$(cat "$RUNTIME/current" 2>/dev/null || echo unknown)"
                return 1
            fi
            if watchdog_is_halted; then
                echo degraded
                echo "reason=watchdog-halted"
                echo "slot=$(cat "$RUNTIME/current" 2>/dev/null || echo unknown)"
                return 1
            fi
            if test -f "$RUNTIME/launcher/pending-slot" || ! heartbeat_is_fresh; then
                echo recovering
                echo "reason=daemon-heartbeat"
                echo "slot=$(cat "$RUNTIME/current" 2>/dev/null || echo unknown)"
                return 0
            fi
            echo active
            link_state=$(udc_state "$udc")
            echo "serial=$(tr -d '\000' <"$USID_FILE") link=$link_state"
            echo "slot=$(cat "$RUNTIME/current" 2>/dev/null || echo unknown)"
            watchdog_pid=$(read_state watchdog_pid '')
            if pid_is "$watchdog_pid" 'usb-gadget-manager.sh'; then
                echo "timeout_watchdog=$watchdog_pid"
            elif test -n "$watchdog_pid"; then
                echo timeout=expired-deferred
            else
                echo timeout=disabled
            fi
            if test -f "$RUNTIME/next"; then
                echo "staged_slot=$(tr -d '\r\n' <"$RUNTIME/next")"
            fi
            return 0
        fi
        mode=$(read_state mode '')
        case "$mode" in
            acquiring-stock-usb|starting|stopping) echo "$mode"; return 0 ;;
            active)
                echo degraded
                echo "reason=managed-process-missing"
                echo "slot=$(cat "$RUNTIME/current" 2>/dev/null || echo unknown)"
                return 1
                ;;
        esac
        echo stale
        return 1
    fi
    echo inactive
    test ! -e "$DISABLE" || echo "disabled=$DISABLE"
}

apply_staged_command() {
    apply_staged_preflight || return 1
    if test -d "$STATE"; then
        stop_command || return 1
    fi
    start_bridge 0
}

apply_staged_preflight() {
    test -f "$RUNTIME/next" || {
        echo "no staged daemon update" >&2
        return 1
    }
    test "$(id -u)" = 0 || { echo "must run as root" >&2; return 1; }
    if test -d "$STATE"; then
        udc=$(read_state udc "$DEFAULT_UDC")
    else
        udc=$(ls "$UDC_CLASS" | head -n 1)
        test -n "$udc" || udc=$DEFAULT_UDC
    fi
    test -d "$UDC_CLASS/$udc" || {
        echo "USB controller is unavailable: $udc" >&2
        return 1
    }
    require_unplugged "$udc" 'applying staged daemon update'
}

usage() {
    echo "usage: $0 start [TIMEOUT_SECONDS|0] | stop | status | apply-staged | preflight apply-staged" >&2
    exit 2
}

case "${1:-}" in
    start) test "$#" -le 2 || usage; start_bridge "${2:-1800}" ;;
    stop) test "$#" -eq 1 || usage; stop_command ;;
    status) test "$#" -eq 1 || usage; status ;;
    apply-staged) test "$#" -eq 1 || usage; apply_staged_command ;;
    preflight)
        test "$#" -eq 2 || usage
        case "$2" in
            apply-staged) apply_staged_preflight ;;
            *) usage ;;
        esac
        ;;
    restore-after) test "$#" -eq 2 || usage; restore_after "$2" ;;
    *) usage ;;
esac
