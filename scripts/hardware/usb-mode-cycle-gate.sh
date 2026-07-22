#!/bin/sh

set -u

COUNT=${1:-100}
CONTROL_ROOT=${KINDLEBRIDGE_CONTROL_ROOT:-/var/local/kindlebridge/control}
MANAGER=${KINDLEBRIDGE_MANAGER:-$CONTROL_ROOT/bin/usb-gadget-manager.sh}
RUNTIME=${KINDLEBRIDGE_RUNTIME:-$CONTROL_ROOT/runtime}
STATE=${KINDLEBRIDGE_USB_STATE:-/var/local/kindlebridge/usb}
LOG=${KINDLEBRIDGE_USB_LOG:-/var/local/kindlebridge/usb.log}
GADGET=${KINDLEBRIDGE_GADGET:-/sys/kernel/config/usb_gadget/mtpgadget}
CONFIG=$GADGET/configs/c.1
BRIDGE_FUNCTION=$GADGET/functions/ffs.kbp
BRIDGE_LINK=$CONFIG/ffs.kbp
MOUNT=${KINDLEBRIDGE_FUNCTIONFS_MOUNT:-/dev/usb-ffs/kbp}
UDC_CLASS=${KINDLEBRIDGE_UDC_CLASS:-/sys/class/udc}
LOCK=${KINDLEBRIDGE_TRANSITION_LOCK:-/tmp/kindlebridge-usb.lock}
OUTPUT=/tmp/kindlebridge-cycle-gate.$$

cycle=0
phase=preflight

cleanup() {
    rm -f "$OUTPUT"
}

diagnostics() {
    echo '--- manager status ---' >&2
    "$MANAGER" status >&2 || true
    echo '--- USB ownership ---' >&2
    printf 'volumd_network_mode=%s\n' \
        "$(read_prop com.lab126.volumd useUsbForNetwork || echo unknown)" >&2
    printf 'mtp_service_started=%s\n' \
        "$(read_prop com.lab126.mtp isMtpStarted || echo unknown)" >&2
    for path in "$UDC_CLASS"/*; do
        test -d "$path" || continue
        printf '%s state=' "${path##*/}" >&2
        cat "$path/state" >&2 || true
        test ! -r "$path/connected" || {
            printf '%s connected=' "${path##*/}" >&2
            cat "$path/connected" >&2 || true
        }
    done
    echo '--- managed processes ---' >&2
    managed_processes >&2 || true
    echo '--- recent lifecycle log ---' >&2
    tail -30 "$LOG" >&2 || true
}

fail() {
    echo "FAIL: cycle=$cycle phase=$phase: $*" >&2
    diagnostics
    exit 1
}

trap cleanup EXIT
trap 'exit 130' HUP INT TERM

case "$COUNT" in
    ''|*[!0-9]*) echo 'cycle count must be an integer from 1 to 1000' >&2; exit 2 ;;
esac
test "$COUNT" -ge 1 && test "$COUNT" -le 1000 || {
    echo 'cycle count must be an integer from 1 to 1000' >&2
    exit 2
}
test "$(id -u)" = 0 || { echo 'cycle gate must run as root' >&2; exit 2; }
test -x "$MANAGER" || { echo "missing manager: $MANAGER" >&2; exit 2; }

first_line() {
    sed -n '1p'
}

read_prop() {
    lipc-get-prop -i -e -- "$1" "$2" 2>/dev/null | tr -d '\r\n'
}

managed_processes() {
    for cmdline in /proc/[0-9]*/cmdline; do
        test -r "$cmdline" || continue
        first=$(tr '\000' '\n' <"$cmdline" 2>/dev/null | sed -n '1p')
        second=$(tr '\000' '\n' <"$cmdline" 2>/dev/null | sed -n '2p')
        case "$first" in
            "$CONTROL_ROOT/bin/kindlebridge-launcher"|"$RUNTIME"/slots/*/bin/kindlebridged)
                printf '%s %s\n' "${cmdline#/proc/}" "$first"
                ;;
            sh|/bin/sh|*/sh)
                test "$second" != "$MANAGER" ||
                    printf '%s %s %s\n' "${cmdline#/proc/}" "$first" "$second"
                ;;
        esac
    done
}

managed_process_count() {
    managed_processes | wc -l | tr -d ' '
}

assert_unplugged() {
    found=0
    for path in "$UDC_CLASS"/*; do
        test -d "$path" || continue
        found=1
        if test -r "$path/connected"; then
            connected=$(cat "$path/connected" 2>/dev/null || echo unknown)
            test "$connected" = 0 || fail "USB cable is connected to ${path##*/}"
        else
            state=$(cat "$path/state" 2>/dev/null || echo unknown)
            test "$state" = 'not attached' || fail "UDC ${path##*/} is $state"
        fi
    done
    test "$found" = 1 || fail 'no USB device controller was found'
}

assert_no_transition_residue() {
    test ! -d "$LOCK" || fail "transition lock remains at $LOCK"
    test ! -f "$RUNTIME/launcher/pending-slot" || fail 'a staged daemon slot is pending'
}

validate_inactive() {
    phase=validate-mtp
    status_output=$("$MANAGER" status 2>&1)
    status_rc=$?
    test "$status_rc" = 0 || fail "inactive status returned $status_rc: $status_output"
    test "$(printf '%s\n' "$status_output" | first_line)" = inactive ||
        fail "expected inactive status: $status_output"
    assert_unplugged
    assert_no_transition_residue
    test ! -d "$STATE" || fail "Bridge state directory still exists: $STATE"
    test ! -e "$BRIDGE_LINK" && test ! -L "$BRIDGE_LINK" ||
        fail 'Bridge function remains linked in MTP mode'
    test ! -d "$BRIDGE_FUNCTION" || fail 'Bridge FunctionFS function remains in MTP mode'
    test ! -e "$MOUNT/ep0" && test ! -e "$MOUNT/ep1" && test ! -e "$MOUNT/ep2" ||
        fail 'Bridge FunctionFS endpoints remain in MTP mode'
    test "$(read_prop com.lab126.volumd useUsbForNetwork || true)" = 0 ||
        fail 'volumd did not reclaim USB ownership'
    test "$(read_prop com.lab126.mtp isMtpStarted || true)" = 1 ||
        fail 'stock MTP did not start'
    test "$(managed_process_count)" = 0 || fail 'managed processes remain in MTP mode'
}

validate_active() {
    phase=validate-development
    status_output=$("$MANAGER" status 2>&1)
    status_rc=$?
    test "$status_rc" = 0 || fail "active status returned $status_rc: $status_output"
    test "$(printf '%s\n' "$status_output" | first_line)" = active ||
        fail "expected active status: $status_output"
    printf '%s\n' "$status_output" | grep -q ' link=not attached$' ||
        fail "development link is not detached: $status_output"
    printf '%s\n' "$status_output" | grep -qx 'timeout=disabled' ||
        fail "development timeout is not disabled: $status_output"
    assert_unplugged
    assert_no_transition_residue
    test -d "$STATE" || fail 'Bridge state directory is missing'
    test ! -f "$STATE/recovery_required" || fail 'Bridge requires recovery'
    test -L "$BRIDGE_LINK" || fail 'Bridge function is not linked'
    test -d "$BRIDGE_FUNCTION" || fail 'Bridge FunctionFS function is missing'
    test -e "$MOUNT/ep0" && test -e "$MOUNT/ep1" && test -e "$MOUNT/ep2" ||
        fail 'Bridge FunctionFS endpoints are incomplete'
    test "$(read_prop com.lab126.volumd useUsbForNetwork || true)" = 1 ||
        fail 'volumd did not grant development USB ownership'
    # Development mode extends the stock composite gadget with KBP. It does
    # not remove the MTP interface or stop tizen-mtp.
    test -L "$CONFIG/ffs.mtp" || fail 'stock MTP function is not linked'
    test "$(read_prop com.lab126.mtp isMtpStarted || true)" = 1 ||
        fail 'stock MTP service is not ready in development mode'
    test "$(managed_process_count)" = 3 || fail 'expected launcher, daemon, and monitor'
}

assert_unplugged
assert_no_transition_residue
initial_status=$("$MANAGER" status 2>&1)
test $? = 0 || fail "initial status failed: $initial_status"
test "$(printf '%s\n' "$initial_status" | first_line)" = active ||
    fail 'start the gate from Development mode'
validate_active

started=$(date +%s)
while test "$cycle" -lt "$COUNT"; do
    cycle=$((cycle + 1))
    cycle_started=$(date +%s)

    phase=switch-to-mtp
    if ! "$MANAGER" stop >"$OUTPUT" 2>&1; then
        fail "switch to MTP failed: $(cat "$OUTPUT")"
    fi
    validate_inactive

    phase=switch-to-development
    if ! "$MANAGER" start 0 >"$OUTPUT" 2>&1; then
        fail "switch to Development mode failed: $(cat "$OUTPUT")"
    fi
    validate_active

    now=$(date +%s)
    echo "PASS cycle=$cycle/$COUNT elapsed_seconds=$((now - cycle_started))"
done

finished=$(date +%s)
echo "PASS: $COUNT MTP-to-Development cycles completed in $((finished - started)) seconds"
echo 'FINAL: active, link=not attached, timeout=disabled'
