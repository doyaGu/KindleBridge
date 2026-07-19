#!/bin/sh
# Reversible KT6 laboratory cycle for the KindleBridge FunctionFS probe.
# This is intentionally not an installer or production USB mode manager.

set -eu

GADGET=/sys/kernel/config/usb_gadget/mtpgadget
CONFIG="$GADGET/configs/c.1"
FUNCTION="$GADGET/functions/ffs.kbp"
LINK="$CONFIG/ffs.kbp"
MOUNT=/dev/usb-ffs/kbp
STATE=/var/run/kindlebridge-unsafe-usb-lab
LOG=/var/tmp/kindlebridge-unsafe-usb-lab.log
DEFAULT_UDC=11211000.usb
UDC_CLASS=/sys/class/udc

log() {
    printf '%s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*" >>"$LOG"
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

wait_udc_configured() {
    udc=$1
    attempts=$2
    while test "$attempts" -gt 0; do
        state=$(cat "$UDC_CLASS/$udc/state" 2>/dev/null || printf '%s' missing)
        if test "$state" = configured; then
            return 0
        fi
        attempts=$((attempts - 1))
        test "$attempts" -eq 0 || sleep 1
    done
    return 1
}

soft_reconnect() {
    udc=$1
    control="$UDC_CLASS/$udc/soft_connect"
    test -w "$control" || return 1
    printf '%s\n' disconnect >"$control" || return 1
    sleep 1
    printf '%s\n' connect >"$control"
}

rebind_mtu3() {
    udc=$1
    device_ip=$2
    netmask=$3
    driver=/sys/bus/platform/drivers/mtu3

    printf '%s\n' "$udc" >"$driver/unbind" || return 1
    sleep 2
    printf '%s\n' "$udc" >"$driver/bind" || return 1
    attempts=10
    while ! test -d /sys/class/net/usb0 && test "$attempts" -gt 0; do
        attempts=$((attempts - 1))
        sleep 1
    done
    test -d /sys/class/net/usb0 || return 1
    ifconfig usb0 "$device_ip" netmask "$netmask" up || return 1
    wait_udc_configured "$udc" 15
}

load_g_ether() {
    dev_addr=$1
    host_addr=$2
    if test -n "$dev_addr" && test -n "$host_addr"; then
        modprobe g_ether dev_addr="$dev_addr" host_addr="$host_addr"
    else
        modprobe g_ether
    fi
}

recover_g_ether() {
    udc=$1
    dev_addr=$2
    host_addr=$3
    device_ip=$4
    netmask=$5

    if ! grep -q '^g_ether ' /proc/modules; then
        load_g_ether "$dev_addr" "$host_addr" || return 1
    fi
    ifconfig usb0 "$device_ip" netmask "$netmask" up || return 1
    if wait_udc_configured "$udc" 5; then
        return 0
    fi

    log "g_ether loaded but UDC was not configured; forcing software reconnect"
    if soft_reconnect "$udc" && wait_udc_configured "$udc" 10; then
        return 0
    fi

    log "software reconnect failed; reloading g_ether once"
    ifconfig usb0 down 2>/dev/null || true
    rmmod g_ether 2>/dev/null || true
    sleep 2
    load_g_ether "$dev_addr" "$host_addr" || return 1
    ifconfig usb0 "$device_ip" netmask "$netmask" up || return 1
    soft_reconnect "$udc" || true
    if wait_udc_configured "$udc" 10; then
        return 0
    fi

    log "g_ether reload did not enumerate; rebinding MTU3 controller"
    rebind_mtu3 "$udc" "$device_ip" "$netmask"
}

clear_state() {
    rm -f "$STATE/udc" "$STATE/probe_pid" "$STATE/watchdog_pid" \
        "$STATE/dev_addr" "$STATE/host_addr" "$STATE/device_ip" \
        "$STATE/netmask" "$STATE/ifconfig" "$STATE/recovery_mode" \
        "$STATE/probe_result"
    rmdir "$STATE" 2>/dev/null || true
}

restore() {
    if ! test -d "$STATE"; then
        log "restore requested with no active cycle"
        return 0
    fi

    udc=$(read_state udc "$DEFAULT_UDC")
    probe_pid=$(read_state probe_pid '')
    watchdog_pid=$(read_state watchdog_pid '')
    dev_addr=$(read_state dev_addr '')
    host_addr=$(read_state host_addr '')
    device_ip=$(read_state device_ip 192.168.15.244)
    netmask=$(read_state netmask 255.255.255.0)

    # Disconnect the composite before changing its functions.
    if test -e "$GADGET/UDC"; then
        printf '\n' >"$GADGET/UDC" 2>/dev/null || true
    fi
    if test -n "$probe_pid"; then
        kill "$probe_pid" 2>/dev/null || true
        wait "$probe_pid" 2>/dev/null || true
    fi
    rm -f "$LINK" 2>/dev/null || true
    umount "$MOUNT" 2>/dev/null || true
    rmdir "$MOUNT" 2>/dev/null || true
    rmdir "$FUNCTION" 2>/dev/null || true

    if recover_g_ether "$udc" "$dev_addr" "$host_addr" "$device_ip" "$netmask"; then
        log "restored and enumerated g_ether on $device_ip"
        if test -n "$watchdog_pid" && test "$watchdog_pid" != "$$"; then
            kill "$watchdog_pid" 2>/dev/null || true
        fi
        clear_state
        return 0
    fi

    # Keep g_ether loaded. Binding MTP here would make a cable replug recover
    # only storage and permanently remove the SSH recovery path.
    printf '%s\n' cable-replug-required >"$STATE/recovery_mode"
    log "g_ether did not enumerate; cable replug is required"
    return 1
}

restore_after() {
    timeout=$1
    elapsed=0
    while test "$elapsed" -lt "$timeout"; do
        if test -f "$STATE/probe_result"; then
            probe_result=$(cat "$STATE/probe_result")
            log "FunctionFS probe exited ($probe_result); restoring immediately"
            restore
            return
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    log "watchdog timeout expired after ${timeout}s"
    restore
}

reconnect_after() {
    udc=$1
    sleep 2
    log "starting g_ether software reconnect test"
    if soft_reconnect "$udc" && wait_udc_configured "$udc" 10; then
        log "g_ether software reconnect test passed"
        return 0
    fi
    log "g_ether software reconnect test failed"
    return 1
}

controller_rebind_after() {
    udc=$1
    sleep 2
    log "starting MTU3 controller rebind test"
    if rebind_mtu3 "$udc" 192.168.15.244 255.255.255.0; then
        log "MTU3 controller rebind test passed"
        return 0
    fi
    log "MTU3 controller rebind test failed"
    return 1
}

start_reconnect_test() {
    udc=$(ls "$UDC_CLASS" | head -n 1)
    test -n "$udc" || { echo "no UDC is available" >&2; return 1; }
    grep -q '^g_ether ' /proc/modules || { echo "g_ether is not loaded" >&2; return 1; }
    wait_udc_configured "$udc" 1 || { echo "UDC is not configured" >&2; return 1; }
    self=$(readlink -f "$0")
    nohup sh "$self" reconnect-after "$udc" </dev/null >>"$LOG" 2>&1 &
    echo "software reconnect test armed; RNDIS will disconnect in two seconds"
}

start_controller_rebind_test() {
    udc=$(ls "$UDC_CLASS" | head -n 1)
    test -n "$udc" || { echo "no UDC is available" >&2; return 1; }
    grep -q '^g_ether ' /proc/modules || { echo "g_ether is not loaded" >&2; return 1; }
    wait_udc_configured "$udc" 1 || { echo "UDC is not configured" >&2; return 1; }
    test "$(readlink -f "$UDC_CLASS/$udc/device/driver")" = /sys/bus/platform/drivers/mtu3 || {
        echo "UDC is not bound to the expected MTU3 driver" >&2
        return 1
    }
    self=$(readlink -f "$0")
    nohup sh "$self" controller-rebind-after "$udc" </dev/null >>"$LOG" 2>&1 &
    echo "MTU3 rebind test armed; RNDIS will disconnect in two seconds"
}

capture_network_state() {
    cat /sys/module/g_ether/parameters/dev_addr >"$STATE/dev_addr"
    cat /sys/module/g_ether/parameters/host_addr >"$STATE/host_addr"
    ifconfig usb0 >"$STATE/ifconfig"
    sed -n 's/.*inet addr:\([^ ]*\).*/\1/p' "$STATE/ifconfig" >"$STATE/device_ip"
    sed -n 's/.*Mask:\([^ ]*\).*/\1/p' "$STATE/ifconfig" >"$STATE/netmask"
    rm -f "$STATE/ifconfig"
    test -s "$STATE/device_ip" || printf '%s\n' 192.168.15.244 >"$STATE/device_ip"
    test -s "$STATE/netmask" || printf '%s\n' 255.255.255.0 >"$STATE/netmask"
}

preflight() {
    probe=$1
    timeout=$2

    case "$timeout" in
        ''|*[!0-9]*) echo "timeout must be an integer" >&2; return 2 ;;
    esac
    if test "$timeout" -lt 30 || test "$timeout" -gt 300; then
        echo "timeout must be between 30 and 300 seconds" >&2
        return 2
    fi
    test "$(id -u)" = 0 || { echo "must run as root" >&2; return 1; }
    test -x "$probe" || { echo "probe is not executable: $probe" >&2; return 1; }
    test -d "$GADGET" || { echo "stock mtpgadget is missing" >&2; return 1; }
    test -L "$CONFIG/ffs.mtp" || { echo "stock MTP link is missing" >&2; return 1; }
    test -z "$(cat "$GADGET/UDC")" || { echo "mtpgadget is already bound" >&2; return 1; }
    grep -q '^g_ether ' /proc/modules || { echo "g_ether recovery source is absent" >&2; return 1; }
    test -f /var/run/mtp.pid || { echo "tizen-mtp pid file is missing" >&2; return 1; }
    kill -0 "$(cat /var/run/mtp.pid)" 2>/dev/null || {
        echo "tizen-mtp is not running" >&2
        return 1
    }
    test ! -d "$STATE" || { echo "another USB cycle is active" >&2; return 1; }
    test -n "$(ls "$UDC_CLASS" | head -n 1)" || { echo "no UDC is available" >&2; return 1; }
    test -w "$UDC_CLASS/$(ls "$UDC_CLASS" | head -n 1)/soft_connect" || {
        echo "UDC software reconnect control is missing" >&2
        return 1
    }
}

start_cycle() {
    probe=$1
    timeout=$2

    preflight "$probe" "$timeout"
    mkdir "$STATE" 2>/dev/null || { echo "another USB cycle is active" >&2; return 1; }
    trap 'code=$?; if test "$code" -ne 0; then restore; fi' EXIT HUP INT TERM

    udc=$(ls "$UDC_CLASS" | head -n 1)
    test -n "$udc" || { echo "no UDC is available" >&2; return 1; }
    printf '%s\n' "$udc" >"$STATE/udc"
    capture_network_state

    mkdir "$FUNCTION"
    mkdir "$MOUNT"
    mount -t functionfs kbp "$MOUNT"
    "$probe" "$MOUNT" --completion-file "$STATE/probe_result" >>"$LOG" 2>&1 &
    probe_pid=$!
    printf '%s\n' "$probe_pid" >"$STATE/probe_pid"
    sleep 1
    kill -0 "$probe_pid" 2>/dev/null || {
        echo "FunctionFS probe exited before USB bind" >&2
        return 1
    }
    # This vendor 4.9 configfs rejects linking an f_fs instance until its
    # userspace process has submitted descriptors through ep0.
    # Use the configfs item's absolute path. This kernel canonicalizes a
    # successful gadget link to a relative target when it is later displayed.
    ln -s "$FUNCTION" "$LINK"

    self=$(readlink -f "$0")
    nohup sh "$self" restore-after "$timeout" </dev/null >>"$LOG" 2>&1 &
    watchdog_pid=$!
    printf '%s\n' "$watchdog_pid" >"$STATE/watchdog_pid"
    log "armed ${timeout}s rollback watchdog pid=$watchdog_pid"

    ifconfig usb0 down
    rmmod g_ether
    printf '%s\n' "$udc" >"$GADGET/UDC"
    if ! wait_udc_configured "$udc" 10; then
        log "composite gadget failed to enumerate"
        return 1
    fi
    log "bound and enumerated stock MTP plus KindleBridge FunctionFS on $udc"

    trap - EXIT HUP INT TERM
    echo "KindleBridge USB probe active; automatic restore in ${timeout}s"
}

start_bridge_cycle() {
    daemon=$1
    serial=$2
    timeout=$3

    test -n "$serial" || { echo "serial must not be empty" >&2; return 2; }
    preflight "$daemon" "$timeout"
    mkdir "$STATE" 2>/dev/null || { echo "another USB cycle is active" >&2; return 1; }
    trap 'code=$?; if test "$code" -ne 0; then restore; fi' EXIT HUP INT TERM

    udc=$(ls "$UDC_CLASS" | head -n 1)
    test -n "$udc" || { echo "no UDC is available" >&2; return 1; }
    printf '%s\n' "$udc" >"$STATE/udc"
    capture_network_state

    mkdir "$FUNCTION"
    mkdir "$MOUNT"
    mount -t functionfs kbp "$MOUNT"
    "$daemon" serve-usb --functionfs-dir "$MOUNT" --serial "$serial" >>"$LOG" 2>&1 &
    daemon_pid=$!
    printf '%s\n' "$daemon_pid" >"$STATE/probe_pid"
    sleep 1
    kill -0 "$daemon_pid" 2>/dev/null || {
        echo "kindlebridged exited before USB bind" >&2
        return 1
    }
    ln -s "$FUNCTION" "$LINK"

    self=$(readlink -f "$0")
    nohup sh "$self" restore-after "$timeout" </dev/null >>"$LOG" 2>&1 &
    watchdog_pid=$!
    printf '%s\n' "$watchdog_pid" >"$STATE/watchdog_pid"
    log "armed ${timeout}s bridge rollback watchdog pid=$watchdog_pid"

    ifconfig usb0 down
    rmmod g_ether
    printf '%s\n' "$udc" >"$GADGET/UDC"
    if ! wait_udc_configured "$udc" 10; then
        log "composite gadget failed to enumerate for the main bridge"
        return 1
    fi
    log "main KindleBridge USB transport active on $udc for serial $serial"

    trap - EXIT HUP INT TERM
    echo "KindleBridge USB active; automatic RNDIS restore in ${timeout}s"
}

self_test() {
    original_udc_class=$UDC_CLASS
    root="/var/tmp/kindlebridge-unsafe-usb-selftest-$$"
    UDC_CLASS=$root
    mkdir "$root"
    mkdir "$root/fake"
    printf '%s\n' configured >"$root/fake/state"
    : >"$root/fake/soft_connect"

    wait_udc_configured fake 1 || return 1
    printf '%s\n' attached >"$root/fake/state"
    if wait_udc_configured fake 1; then
        return 1
    fi
    (sleep 1; printf '%s\n' configured >"$root/fake/state") &
    updater=$!
    wait_udc_configured fake 3 || return 1
    wait "$updater"
    soft_reconnect fake
    test "$(cat "$root/fake/soft_connect")" = connect || return 1

    rm -f "$root/fake/state" "$root/fake/soft_connect"
    rmdir "$root/fake"
    rmdir "$root"
    UDC_CLASS=$original_udc_class
    echo "self-test passed"
}

usage() {
    echo "usage: $0 preflight|start PROBE_BINARY [TIMEOUT_SECONDS] | start-bridge KINDLEBRIDGED SERIAL [TIMEOUT_SECONDS] | restore | status | self-test | reconnect-test | controller-rebind-test" >&2
    exit 2
}

require_unsafe_usb_lab_opt_in() {
    test "${KINDLEBRIDGE_ALLOW_UNSAFE_USB_LAB:-0}" = 1 || {
        echo "retired unsafe USB lab; use the MRPI usb-gadget-manager.sh manager" >&2
        exit 1
    }
}

case "${1:-}" in
    preflight)
        test "$#" -ge 2 && test "$#" -le 3 || usage
        preflight "$2" "${3:-90}"
        echo "preflight passed"
        ;;
    start)
        require_unsafe_usb_lab_opt_in
        test "$#" -ge 2 && test "$#" -le 3 || usage
        start_cycle "$2" "${3:-90}"
        ;;
    start-bridge)
        require_unsafe_usb_lab_opt_in
        test "$#" -ge 3 && test "$#" -le 4 || usage
        start_bridge_cycle "$2" "$3" "${4:-300}"
        ;;
    restore)
        require_unsafe_usb_lab_opt_in
        test "$#" -eq 1 || usage
        restore
        ;;
    restore-after)
        require_unsafe_usb_lab_opt_in
        test "$#" -eq 2 || usage
        restore_after "$2"
        ;;
    reconnect-after)
        require_unsafe_usb_lab_opt_in
        test "$#" -eq 2 || usage
        reconnect_after "$2"
        ;;
    controller-rebind-after)
        require_unsafe_usb_lab_opt_in
        test "$#" -eq 2 || usage
        controller_rebind_after "$2"
        ;;
    status)
        test "$#" -eq 1 || usage
        if test -d "$STATE"; then
            echo active
            test -f "$STATE/watchdog_pid" && printf 'watchdog_pid=%s\n' "$(cat "$STATE/watchdog_pid")"
            test -f "$STATE/probe_pid" && printf 'probe_pid=%s\n' "$(cat "$STATE/probe_pid")"
            test -f "$STATE/recovery_mode" && printf 'recovery_mode=%s\n' "$(cat "$STATE/recovery_mode")"
        else
            echo inactive
        fi
        ;;
    self-test)
        test "$#" -eq 1 || usage
        self_test
        ;;
    reconnect-test)
        require_unsafe_usb_lab_opt_in
        test "$#" -eq 1 || usage
        start_reconnect_test
        ;;
    controller-rebind-test)
        require_unsafe_usb_lab_opt_in
        test "$#" -eq 1 || usage
        start_controller_rebind_test
        ;;
    *) usage ;;
esac
