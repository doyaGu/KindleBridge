#!/bin/sh

set -eu

TESTS_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$TESTS_DIR/.." && pwd)
MANAGER="$PROJECT_DIR/packaging/mrpi/payload/kindlebridge/bin/usb-gadget-manager.sh"
KUAL_WRAPPER="$PROJECT_DIR/packaging/mrpi/payload/extensions/kindlebridge/bin/kindlebridge.sh"
KUAL_MENU="$PROJECT_DIR/packaging/mrpi/payload/extensions/kindlebridge/menu.json"
FIXTURES="$TESTS_DIR/fixtures/usb-mode"
RUN_ROOT="$PROJECT_DIR/.kindlebridge-test-state/usb-mode-lifecycle.$$"
PIDS=
PASSED=0

# Git for Windows otherwise emulates symlinks by copying directories, which
# cannot exercise the configfs link ownership checks in this test.
MSYS=winsymlinks:nativestrict
export MSYS

case "$RUN_ROOT" in
    "$PROJECT_DIR"/.kindlebridge-test-state/usb-mode-lifecycle.*) ;;
    *) printf 'unsafe test root: %s\n' "$RUN_ROOT" >&2; exit 1 ;;
esac

cleanup() {
    for pid in $PIDS; do
        kill "$pid" 2>/dev/null || true
    done
    rm -rf "$RUN_ROOT"
}
trap cleanup EXIT HUP INT TERM

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

assert_equal() {
    expected=$1
    actual=$2
    message=$3
    test "$actual" = "$expected" || fail "$message (expected=$expected actual=$actual)"
}

assert_file_empty() {
    file=$1
    message=$2
    test ! -s "$file" || {
        printf '%s\n' '--- trace ---' >&2
        cat "$file" >&2
        fail "$message"
    }
}

assert_before() {
    first_pattern=$1
    second_pattern=$2
    file=$3
    message=$4
    first_line=$(grep -n "$first_pattern" "$file" | sed -n '1s/:.*//p')
    second_line=$(grep -n "$second_pattern" "$file" | sed -n '1s/:.*//p')
    test -n "$first_line" && test -n "$second_line" ||
        fail "$message (missing trace event)"
    test "$first_line" -lt "$second_line" || fail "$message"
}

pass() {
    PASSED=$((PASSED + 1))
    printf 'ok %s - %s\n' "$PASSED" "$1"
}

setup_case() {
    name=$1
    CASE_ROOT="$RUN_ROOT/$name"
    export KINDLEBRIDGE_TEST_ROOT="$CASE_ROOT"
    export KINDLEBRIDGE_MNT_US_ROOT="$CASE_ROOT/mnt/us"
    export KINDLEBRIDGE_BASE_US_ROOT="$CASE_ROOT/mnt/base-us"
    export KINDLEBRIDGE_VAR_LOCAL_ROOT="$CASE_ROOT/var/local"
    export KINDLEBRIDGE_SYS_ROOT="$CASE_ROOT/sys"
    export KINDLEBRIDGE_PROC_ROOT="$CASE_ROOT/proc"
    export KINDLEBRIDGE_PID_PROC_ROOT=/proc
    export KINDLEBRIDGE_DEV_ROOT="$CASE_ROOT/dev"
    export KINDLEBRIDGE_TMP_ROOT="$CASE_ROOT/tmp"
    CONTROL_ROOT="$CASE_ROOT/var/local/kindlebridge/control"
    export KINDLEBRIDGE_TEST_ALLOW_MTP_DIRECTORY=1
    unset KINDLEBRIDGE_TEST_MOUNT_FAIL KINDLEBRIDGE_TEST_REAL_SLEEP
    unset KINDLEBRIDGE_TEST_NO_HEARTBEAT
    unset KINDLEBRIDGE_TEST_BIND_STOCK_MTP
    unset KINDLEBRIDGE_TEST_SUPERVISOR_RACE KINDLEBRIDGE_TEST_SUPERVISOR_RESTART
    unset KINDLEBRIDGE_TEST_AFTER_UNBIND_DELAY
    export KINDLEBRIDGE_TEST_DISABLE_MONITOR=1

    mkdir -p \
        "$CASE_ROOT/bin" \
        "$CASE_ROOT/lipc" \
        "$CASE_ROOT/mnt/base-us" \
        "$CASE_ROOT/mnt/us" \
        "$CONTROL_ROOT/bin" \
        "$CONTROL_ROOT/runtime/slots/A/bin" \
        "$CONTROL_ROOT/runtime/slots/B/bin" \
        "$CONTROL_ROOT/runtime/run" \
        "$CASE_ROOT/var/local" \
        "$CASE_ROOT/tmp" \
        "$CASE_ROOT/dev" \
        "$CASE_ROOT/proc/sys/kernel/random" \
        "$CASE_ROOT/sys/class/udc/11211000.usb" \
        "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/configs/c.1" \
        "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/configs/c.1/ffs.mtp" \
        "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/functions/ffs.mtp"

    for command in id sleep lipc-get-prop lipc-set-prop lipc-send-event \
        modprobe rmmod ifconfig mount umount; do
        cp "$FIXTURES/fake-command.sh" "$CASE_ROOT/bin/$command"
        chmod 0755 "$CASE_ROOT/bin/$command"
    done
    cp "$FIXTURES/kindlebridge-launcher" "$CONTROL_ROOT/bin/kindlebridge-launcher"
    cp "$FIXTURES/kindlebridged" "$CONTROL_ROOT/runtime/slots/A/bin/kindlebridged"
    cp "$FIXTURES/kindlebridged" "$CONTROL_ROOT/runtime/slots/B/bin/kindlebridged"
    chmod 0755 "$CONTROL_ROOT/bin/kindlebridge-launcher" \
        "$CONTROL_ROOT/runtime/slots/A/bin/kindlebridged" \
        "$CONTROL_ROOT/runtime/slots/B/bin/kindlebridged"
    printf '%s\n' A >"$CONTROL_ROOT/runtime/current"

    printf '%s\n' 11211000.usb \
        >"$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC"
    printf '%s\n' 0 >"$CASE_ROOT/sys/class/udc/11211000.usb/connected"
    printf '%s\n' 'not attached' >"$CASE_ROOT/sys/class/udc/11211000.usb/state"
    printf '%s\n' test-boot-id >"$CASE_ROOT/proc/sys/kernel/random/boot_id"
    printf '%s\n' TESTSERIAL >"$CASE_ROOT/proc/usid"
    printf '%s' '' >"$CASE_ROOT/proc/modules"
    printf '%s' '' >"$CASE_ROOT/proc/mounts"
    printf '%s\n' 0 >"$CASE_ROOT/lipc/volumd.useUsbForNetwork"
    printf '%s\n' 1 >"$CASE_ROOT/lipc/mtp.isMtpStarted"
    printf '%s' '' >"$CASE_ROOT/trace"
    export PATH="$CASE_ROOT/bin:$ORIGINAL_PATH"
}

run_manager() {
    output_file=$1
    shift
    if sh "$MANAGER" "$@" >"$output_file" 2>&1; then
        MANAGER_RC=0
    else
        MANAGER_RC=$?
    fi
}

remember_daemon() {
    for state in \
        "$CASE_ROOT/var/local/kindlebridge/usb/launcher_pid" \
        "$CONTROL_ROOT/runtime/run/daemon.pid"; do
        if test -f "$state"; then
            pid=$(cat "$state")
            PIDS="$PIDS $pid"
        fi
    done
}

assert_daemon_sync_root() {
    expected=$1
    daemon_pid=$(cat "$CONTROL_ROOT/runtime/run/daemon.pid")
    actual=$(tr '\000' '\n' <"/proc/$daemon_pid/cmdline" | awk '
        previous == "--sync-root" { print; exit }
        { previous=$0 }
    ')
    assert_equal "$expected" "$actual" 'daemon received the wrong sync root'
}

ORIGINAL_PATH=$PATH
mkdir -p "$RUN_ROOT"

setup_case connected_start
printf '%s\n' 1 >"$CASE_ROOT/sys/class/udc/11211000.usb/connected"
run_manager "$CASE_ROOT/output" start 0
test "$MANAGER_RC" -ne 0 || fail 'connected start unexpectedly succeeded'
assert_equal 11211000.usb "$(cat "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC")" \
    'connected start changed the stock UDC binding'
assert_file_empty "$CASE_ROOT/trace" 'connected start issued a mutating command'
test ! -d "$CASE_ROOT/var/local/kindlebridge/usb" || fail 'connected start created transition state'
pass 'connected start is read-only'

setup_case disabled_start
printf '%s\n' disabled >"$CASE_ROOT/mnt/us/KINDLEBRIDGE_DISABLE"
run_manager "$CASE_ROOT/output" start 0
test "$MANAGER_RC" -ne 0 || fail 'disabled start unexpectedly succeeded'
assert_file_empty "$CASE_ROOT/trace" 'disabled start issued a mutating command'
pass 'disable flag blocks start without mutation'

setup_case indeterminate_start
printf '%s\n' 0 >"$CASE_ROOT/lipc/mtp.isMtpStarted"
run_manager "$CASE_ROOT/output" start 0
test "$MANAGER_RC" -ne 0 || fail 'indeterminate stock state unexpectedly started'
assert_file_empty "$CASE_ROOT/trace" 'indeterminate stock state was mutated'
assert_equal 11211000.usb "$(cat "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC")" \
    'indeterminate start changed the stock UDC binding'
pass 'indeterminate stock state fails closed'

setup_case usbnetlite_owner
mkdir -p "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/functions/ncm.usbnetlite"
ln -s "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/functions/ncm.usbnetlite" \
    "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/configs/c.1/ncm.usbnetlite"
run_manager "$CASE_ROOT/output" start 0
test "$MANAGER_RC" -ne 0 || fail 'KindleBridge started while USBNetLite owned USB'
grep -q 'USBNetLite owns USB' "$CASE_ROOT/output" ||
    fail 'USBNetLite ownership conflict was not explained'
assert_file_empty "$CASE_ROOT/trace" 'USBNetLite ownership conflict mutated USB state'
assert_equal 11211000.usb "$(cat "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC")" \
    'USBNetLite ownership conflict changed the UDC binding'
pass 'USBNetLite ownership conflict fails closed with an actionable result'

setup_case concurrent_transition
cp "$FIXTURES/kindlebridged" "$CASE_ROOT/usb-gadget-manager.sh"
chmod 0755 "$CASE_ROOT/usb-gadget-manager.sh"
"$CASE_ROOT/usb-gadget-manager.sh" &
lock_pid=$!
PIDS="$PIDS $lock_pid"
mkdir "$CASE_ROOT/tmp/kindlebridge-usb.lock"
printf '%s\n' "$lock_pid" >"$CASE_ROOT/tmp/kindlebridge-usb.lock/pid"
run_manager "$CASE_ROOT/output" start 0
test "$MANAGER_RC" -ne 0 || fail 'concurrent USB transition unexpectedly started'
assert_file_empty "$CASE_ROOT/trace" 'concurrent transition issued a mutating command'
test ! -d "$CASE_ROOT/var/local/kindlebridge/usb" || fail 'concurrent transition created state'
pass 'live transition lock fails closed'

setup_case no_speculative_poll
run_manager "$CASE_ROOT/output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/output" >&2; fail 'stock MTP to bridge start failed'; }
remember_daemon
assert_daemon_sync_root "$CASE_ROOT/mnt/us/kindlebridge-data"
launcher_pid=$(cat "$CASE_ROOT/var/local/kindlebridge/usb/launcher_pid")
assert_equal "$CONTROL_ROOT" "$(readlink "/proc/$launcher_pid/cwd")" \
    'launcher inherited the package manager working directory'
pre_request_sleeps=$(awk '
    /^lipc-set useUsbForNetwork 1$/ { print count + 0; found=1; exit }
    /^sleep 1$/ { count++ }
    END { if (!found) print -1 }
' "$CASE_ROOT/trace" | head -n 1)
assert_equal 0 "$pre_request_sleeps" 'start polled for a network state it had not requested'
pass 'stock handoff starts without a speculative 15-second poll'

setup_case direct_fsp_backing_store
printf 'fsp %s fuse.fsp rw,nosuid,nodev,noatime 0 0\n' "$CASE_ROOT/mnt/us" \
    >"$CASE_ROOT/proc/mounts"
printf '/dev/loop/0 %s ext4 rw,relatime,data=ordered 0 0\n' "$CASE_ROOT/mnt/base-us" \
    >>"$CASE_ROOT/proc/mounts"
run_manager "$CASE_ROOT/output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/output" >&2; fail 'FSP backing-store start failed'; }
remember_daemon
assert_daemon_sync_root "$CASE_ROOT/mnt/base-us/kindlebridge-data"
pass 'FSP userstore uses its direct backing store for sync'

setup_case existing_stock_network
printf '%s\n' 1 >"$CASE_ROOT/lipc/volumd.useUsbForNetwork"
printf '%s\n' 0 >"$CASE_ROOT/lipc/mtp.isMtpStarted"
printf '%s\n' 'g_ether 16384 0 - Live 0x00000000' >"$CASE_ROOT/proc/modules"
printf '%s' '' >"$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC"
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/start-output" >&2; fail 'existing stock network handoff failed'; }
remember_daemon
if grep -q '^lipc-set\|^hal-event' "$CASE_ROOT/trace"; then
    fail 'existing stock network handoff unnecessarily cycled volumd'
fi
run_manager "$CASE_ROOT/stop-output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/stop-output" >&2; fail 'existing stock network cleanup failed'; }
pass 'existing stock network state can hand off without a USBNet dependency'

setup_case already_stock_mtp
printf '%s' '' >"$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC"
mkdir -p "$CASE_ROOT/var/local/kindlebridge/usb"
printf '%s\n' test-boot-id >"$CASE_ROOT/var/local/kindlebridge/usb/boot_id"
printf '%s\n' stopping >"$CASE_ROOT/var/local/kindlebridge/usb/mode"
printf '%s\n' 11211000.usb >"$CASE_ROOT/var/local/kindlebridge/usb/udc"
run_manager "$CASE_ROOT/output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/output" >&2; fail 'ready stock MTP stop failed'; }
assert_file_empty "$CASE_ROOT/trace" 'ready stock MTP was unnecessarily cycled through g_ether'
test ! -d "$CASE_ROOT/var/local/kindlebridge/usb" || fail 'ready stock MTP left stale state'
pass 'already-ready stock MTP needs no synthetic USB cycle'

setup_case leftover_function
mkdir "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/functions/ffs.kbp"
mkdir -p "$CASE_ROOT/var/local/kindlebridge/usb"
printf '%s\n' test-boot-id >"$CASE_ROOT/var/local/kindlebridge/usb/boot_id"
printf '%s\n' stopping >"$CASE_ROOT/var/local/kindlebridge/usb/mode"
printf '%s\n' 11211000.usb >"$CASE_ROOT/var/local/kindlebridge/usb/udc"
run_manager "$CASE_ROOT/output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/output" >&2; fail 'leftover function cleanup failed'; }
assert_file_empty "$CASE_ROOT/trace" 'unlinked Bridge function caused a stock gadget cycle'
assert_equal 11211000.usb "$(cat "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC")" \
    'unlinked Bridge function caused the stock gadget to be unbound'
pass 'unlinked Bridge function cannot unbind stock MTP'

setup_case full_cycle
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/start-output" >&2; fail 'full-cycle start failed'; }
remember_daemon
assert_equal active "$(sh "$MANAGER" status | sed -n '1p')" 'status did not report active'
assert_equal 1 "$(cat "$CASE_ROOT/lipc/volumd.useUsbForNetwork")" 'volumd network ownership was not acquired'
test ! -s "$CASE_ROOT/proc/modules" || fail 'g_ether remained loaded while bridge was active'
test -L "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/configs/c.1/ffs.kbp" ||
    fail 'bridge did not link the ffs.kbp protocol function'
grep -Fq "mount -t functionfs kbp $CASE_ROOT/dev/usb-ffs/kbp" "$CASE_ROOT/trace" ||
    fail 'bridge did not mount the kbp FunctionFS instance consistently'
assert_equal 11211000.usb "$(cat "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC")" \
    'bridge was not bound to the UDC'
assert_before '^lipc-set useUsbForNetwork 1$' '^hal-event usbUnconfigured$' \
    "$CASE_ROOT/trace" 'MTP handoff sent usbUnconfigured before requesting network mode'
assert_before '^hal-event usbUnconfigured$' '^hal-event usbPlugOut$' \
    "$CASE_ROOT/trace" 'MTP handoff event order differs from the stock lifecycle'
assert_before '^hal-event usbPlugOut$' '^rmmod g_ether$' \
    "$CASE_ROOT/trace" 'bridge unloaded g_ether before volumd released MTP'
printf '%s' '' >"$CASE_ROOT/trace"
run_manager "$CASE_ROOT/stop-output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/stop-output" >&2; fail 'full-cycle stop failed'; }
assert_equal 0 "$(cat "$CASE_ROOT/lipc/volumd.useUsbForNetwork")" 'volumd did not reclaim MTP mode'
assert_equal 1 "$(cat "$CASE_ROOT/lipc/mtp.isMtpStarted")" 'stock MTP process was not restarted'
test ! -s "$CASE_ROOT/proc/modules" || fail 'g_ether remained loaded after stock MTP reclaim'
test ! -d "$CASE_ROOT/var/local/kindlebridge/usb" || fail 'full cycle left transition state'
test ! -e "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/configs/c.1/ffs.kbp" ||
    fail 'full cycle left the ffs.kbp configuration link behind'
test ! -d "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/functions/ffs.kbp" ||
    fail 'full cycle left the ffs.kbp function behind'
assert_before '^modprobe g_ether$' '^lipc-set useUsbForNetwork 0$' \
    "$CASE_ROOT/trace" 'stock handback notified volumd before creating its expected network state'
assert_before '^lipc-set useUsbForNetwork 0$' '^hal-event usbUnconfigured$' \
    "$CASE_ROOT/trace" 'stock handback event order differs from the firmware lifecycle'
assert_before '^hal-event usbUnconfigured$' '^hal-event usbPlugOut$' \
    "$CASE_ROOT/trace" 'stock handback sent usbPlugOut out of order'
pass 'stock MTP to bridge to stock MTP lifecycle'

setup_case idempotent_actions
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/start-output" >&2; fail 'idempotent start setup failed'; }
remember_daemon
printf '%s' '' >"$CASE_ROOT/trace"
run_manager "$CASE_ROOT/repeated-start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/repeated-start-output" >&2; fail 'repeated start failed'; }
grep -q 'already active' "$CASE_ROOT/repeated-start-output" ||
    fail 'repeated start did not explain the existing state'
assert_file_empty "$CASE_ROOT/trace" 'repeated start mutated an active bridge'
run_manager "$CASE_ROOT/stop-output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/stop-output" >&2; fail 'idempotent stop setup failed'; }
printf '%s' '' >"$CASE_ROOT/trace"
run_manager "$CASE_ROOT/repeated-stop-output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/repeated-stop-output" >&2; fail 'repeated stop failed'; }
grep -q 'already inactive' "$CASE_ROOT/repeated-stop-output" ||
    fail 'repeated stop did not explain the existing state'
assert_file_empty "$CASE_ROOT/trace" 'repeated stop mutated an inactive bridge'
pass 'start and stop are safe to repeat'

setup_case supervised_stop_race
export KINDLEBRIDGE_TEST_SUPERVISOR_RACE=1
export KINDLEBRIDGE_TEST_AFTER_UNBIND_DELAY=1
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/start-output" >&2; fail 'supervised-stop setup failed'; }
remember_daemon
run_manager "$CASE_ROOT/stop-output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/stop-output" >&2; fail 'supervised stop failed'; }
test ! -f "$CONTROL_ROOT/runtime/launcher/watchdog-state" ||
    fail 'controlled stop was recorded as daemon crashes and halted the next start'
pass 'controlled stop cannot trip the persistent launcher crash fuse'

setup_case manual_retry
mkdir -p "$CONTROL_ROOT/runtime/launcher"
printf 'KINDLEBRIDGE_WATCHDOG_V1\nslot=A\ncrashes=3\nnext_start_ms=1\nhalted=1\n' \
    >"$CONTROL_ROOT/runtime/launcher/watchdog-state"
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/start-output" >&2; fail 'manual retry stayed fused'; }
remember_daemon
test ! -f "$CONTROL_ROOT/runtime/launcher/watchdog-state" ||
    fail 'manual retry did not clear the previous crash fuse'
run_manager "$CASE_ROOT/stop-output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/stop-output" >&2; fail 'manual retry cleanup failed'; }
pass 'manual start clears a previous launcher crash fuse'

setup_case connected_stop
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/start-output" >&2; fail 'connected-stop setup failed'; }
remember_daemon
printf '%s\n' 1 >"$CASE_ROOT/sys/class/udc/11211000.usb/connected"
printf '%s' '' >"$CASE_ROOT/trace"
run_manager "$CASE_ROOT/stop-output" stop
test "$MANAGER_RC" -ne 0 || fail 'connected stop unexpectedly succeeded'
assert_file_empty "$CASE_ROOT/trace" 'connected stop issued a mutating command'
assert_equal active "$(cat "$CASE_ROOT/var/local/kindlebridge/usb/mode")" \
    'connected stop changed active state'
assert_equal 11211000.usb "$(cat "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC")" \
    'connected stop unbound the bridge gadget'
printf '%s\n' 0 >"$CASE_ROOT/sys/class/udc/11211000.usb/connected"
run_manager "$CASE_ROOT/cleanup-output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/cleanup-output" >&2; fail 'connected-stop cleanup failed'; }
pass 'connected stop is read-only and preserves the active bridge'

setup_case detached_status
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/start-output" >&2; fail 'detached-status setup failed'; }
remember_daemon
printf '%s' '' >"$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC"
run_manager "$CASE_ROOT/status-output" status
test "$MANAGER_RC" -ne 0 || fail 'detached gadget status unexpectedly succeeded'
assert_equal detached "$(sed -n '1p' "$CASE_ROOT/status-output")" \
    'live launcher processes hid a detached UDC'
run_manager "$CASE_ROOT/cleanup-output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/cleanup-output" >&2; fail 'detached-status cleanup failed'; }
pass 'status reports a detached UDC instead of a false active state'

setup_case daemon_restart_degraded
export KINDLEBRIDGE_TEST_SUPERVISOR_RESTART=1
export KINDLEBRIDGE_TEST_REAL_SLEEP=1
unset KINDLEBRIDGE_TEST_DISABLE_MONITOR
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || {
    cat "$CASE_ROOT/start-output" >&2
    fail 'daemon-restart degraded-state setup failed'
}
old_daemon_pid=$(cat "$CONTROL_ROOT/runtime/run/daemon.pid")
old_instance=$(sed -n '2s/^instance=//p' "$CONTROL_ROOT/runtime/run/heartbeat")
PIDS="$PIDS $old_daemon_pid"
kill -9 "$old_daemon_pid"
attempts=100
new_daemon_pid=$old_daemon_pid
while test "$attempts" -gt 0 && test "$new_daemon_pid" = "$old_daemon_pid"; do
    /usr/bin/sleep 0.02
    new_daemon_pid=$(cat "$CONTROL_ROOT/runtime/run/daemon.pid" 2>/dev/null || true)
    attempts=$((attempts - 1))
done
new_instance=$old_instance
attempts=100
while test "$attempts" -gt 0 && test "$new_instance" = "$old_instance"; do
    /usr/bin/sleep 0.02
    new_instance=$(sed -n '2s/^instance=//p' \
        "$CONTROL_ROOT/runtime/run/heartbeat" 2>/dev/null || true)
    attempts=$((attempts - 1))
done
test -n "$new_daemon_pid" && test "$new_daemon_pid" != "$old_daemon_pid" &&
    test -n "$new_instance" && test "$new_instance" != "$old_instance" ||
    fail 'fake launcher did not make the replacement daemon ready'
PIDS="$PIDS $new_daemon_pid"
attempts=100
managed_daemon_pid=$old_daemon_pid
while test "$attempts" -gt 0 && test "$managed_daemon_pid" != "$new_daemon_pid"; do
    /usr/bin/sleep 0.02
    managed_daemon_pid=$(cat "$CASE_ROOT/var/local/kindlebridge/usb/daemon_pid" 2>/dev/null || true)
    attempts=$((attempts - 1))
done
assert_equal "$new_daemon_pid" \
    "$managed_daemon_pid" \
    'manager retained the crashed daemon PID'
assert_equal 11211000.usb \
    "$(cat "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC")" \
    'daemon restart cycled the live FunctionFS gadget'
run_manager "$CASE_ROOT/status-output" status
test "$MANAGER_RC" -ne 0 || fail 'restarted daemon was reported as transparently recovered'
assert_equal degraded "$(sed -n '1p' "$CASE_ROOT/status-output")" \
    'restarted daemon did not require explicit recovery'
grep -q '^reason=daemon-restarted$' "$CASE_ROOT/status-output" ||
    fail 'restarted daemon did not report its recovery reason'
grep -q "daemon restarted $old_daemon_pid -> $new_daemon_pid; leaving USB untouched" \
    "$CASE_ROOT/var/local/kindlebridge/usb.log" ||
    fail 'daemon restart was not recorded'
run_manager "$CASE_ROOT/cleanup-output" stop
test "$MANAGER_RC" -eq 0 || {
    cat "$CASE_ROOT/cleanup-output" >&2
    fail 'daemon-restart degraded-state cleanup failed'
}
pass 'daemon restart is marked degraded without cycling FunctionFS'

setup_case healthy_monitor_lock_free
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || {
    cat "$CASE_ROOT/start-output" >&2
    fail 'healthy-monitor setup failed'
}
remember_daemon
mkdir "$CASE_ROOT/tmp/kindlebridge-usb.lock"
printf '%s\n' 999999 >"$CASE_ROOT/tmp/kindlebridge-usb.lock/pid"
run_manager "$CASE_ROOT/monitor-output" monitor-once
test "$MANAGER_RC" -eq 0 || {
    cat "$CASE_ROOT/monitor-output" >&2
    fail 'healthy monitor iteration failed'
}
assert_equal 999999 "$(cat "$CASE_ROOT/tmp/kindlebridge-usb.lock/pid")" \
    'healthy monitor acquired or replaced the transition lock'
rm -rf "$CASE_ROOT/tmp/kindlebridge-usb.lock"
run_manager "$CASE_ROOT/cleanup-output" stop
test "$MANAGER_RC" -eq 0 || {
    cat "$CASE_ROOT/cleanup-output" >&2
    fail 'healthy-monitor cleanup failed'
}
pass 'healthy monitor does not contend on the transition lock'

setup_case pid_ownership
mkdir -p "$CASE_ROOT/unrelated" "$CASE_ROOT/var/local/kindlebridge/usb"
cp "$FIXTURES/same-name-launcher" "$CASE_ROOT/unrelated/kindlebridge-launcher"
chmod 0755 "$CASE_ROOT/unrelated/kindlebridge-launcher"
export KINDLEBRIDGE_TEST_SIGNAL_MARKER="$CASE_ROOT/unrelated/signal-received"
"$CASE_ROOT/unrelated/kindlebridge-launcher" &
unrelated_pid=$!
PIDS="$PIDS $unrelated_pid"
export KINDLEBRIDGE_PID_PROC_ROOT="$CASE_ROOT/pid-proc"
mkdir -p "$KINDLEBRIDGE_PID_PROC_ROOT/$unrelated_pid"
printf '%s\0' "$CASE_ROOT/unrelated/kindlebridge-launcher" \
    >"$KINDLEBRIDGE_PID_PROC_ROOT/$unrelated_pid/cmdline"
printf '%s\n' test-boot-id >"$CASE_ROOT/var/local/kindlebridge/usb/boot_id"
printf '%s\n' active >"$CASE_ROOT/var/local/kindlebridge/usb/mode"
printf '%s\n' 11211000.usb >"$CASE_ROOT/var/local/kindlebridge/usb/udc"
printf '%s\n' "$unrelated_pid" >"$CASE_ROOT/var/local/kindlebridge/usb/launcher_pid"
run_manager "$CASE_ROOT/output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/output" >&2; fail 'PID ownership cleanup failed'; }
attempts=20
while test "$attempts" -gt 0 && test ! -e "$KINDLEBRIDGE_TEST_SIGNAL_MARKER"; do
    /usr/bin/sleep 0.05
    attempts=$((attempts - 1))
done
test ! -e "$KINDLEBRIDGE_TEST_SIGNAL_MARKER" ||
    fail 'manager signalled a same-named process outside the active runtime'
kill "$unrelated_pid" 2>/dev/null || true
wait "$unrelated_pid" 2>/dev/null || true
pass 'cleanup verifies process ownership before sending signals'

setup_case pid_argv_ownership
mkdir -p "$CASE_ROOT/unrelated" "$CASE_ROOT/var/local/kindlebridge/usb"
cp "$FIXTURES/same-name-launcher" "$CASE_ROOT/unrelated/unrelated-process"
chmod 0755 "$CASE_ROOT/unrelated/unrelated-process"
export KINDLEBRIDGE_TEST_SIGNAL_MARKER="$CASE_ROOT/unrelated/signal-received"
"$CASE_ROOT/unrelated/unrelated-process" &
unrelated_pid=$!
PIDS="$PIDS $unrelated_pid"
export KINDLEBRIDGE_PID_PROC_ROOT="$CASE_ROOT/pid-proc"
mkdir -p "$KINDLEBRIDGE_PID_PROC_ROOT/$unrelated_pid"
printf '%s\0%s\0' "$CASE_ROOT/unrelated/unrelated-process" \
    "$CONTROL_ROOT/bin/kindlebridge-launcher" \
    >"$KINDLEBRIDGE_PID_PROC_ROOT/$unrelated_pid/cmdline"
printf '%s\n' test-boot-id >"$CASE_ROOT/var/local/kindlebridge/usb/boot_id"
printf '%s\n' active >"$CASE_ROOT/var/local/kindlebridge/usb/mode"
printf '%s\n' 11211000.usb >"$CASE_ROOT/var/local/kindlebridge/usb/udc"
printf '%s\n' "$unrelated_pid" >"$CASE_ROOT/var/local/kindlebridge/usb/launcher_pid"
run_manager "$CASE_ROOT/output" stop
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/output" >&2; fail 'PID argv ownership cleanup failed'; }
attempts=20
while test "$attempts" -gt 0 && test ! -e "$KINDLEBRIDGE_TEST_SIGNAL_MARKER"; do
    /usr/bin/sleep 0.05
    attempts=$((attempts - 1))
done
test ! -e "$KINDLEBRIDGE_TEST_SIGNAL_MARKER" ||
    fail 'manager treated a command argument as the managed executable'
kill "$unrelated_pid" 2>/dev/null || true
wait "$unrelated_pid" 2>/dev/null || true
pass 'cleanup matches the managed executable rather than arbitrary arguments'

setup_case staged_apply_rechecks_cable
printf '%s\n' B >"$CONTROL_ROOT/runtime/next"
run_manager "$CASE_ROOT/preflight-output" preflight apply-staged
test "$MANAGER_RC" -eq 0 || {
    cat "$CASE_ROOT/preflight-output" >&2
    fail 'offline staged-update preflight failed'
}
printf '%s\n' 1 >"$CASE_ROOT/sys/class/udc/11211000.usb/connected"
run_manager "$CASE_ROOT/apply-output" apply-staged
test "$MANAGER_RC" -ne 0 || fail 'connected staged update unexpectedly succeeded after preflight'
grep -q 'Unplug USB before applying staged daemon update' "$CASE_ROOT/apply-output" ||
    fail 'staged update did not report its entry-point cable check'
assert_equal A "$(cat "$CONTROL_ROOT/runtime/current")" \
    'connected staged update changed the current daemon slot'
assert_equal B "$(cat "$CONTROL_ROOT/runtime/next")" \
    'connected staged update discarded the pending daemon slot'
assert_file_empty "$CASE_ROOT/trace" 'connected staged update issued a mutating command'
test ! -d "$CASE_ROOT/var/local/kindlebridge/usb" ||
    fail 'connected staged update created transition state'
pass 'staged activation rechecks the cable after read-only preflight'

setup_case staged_apply
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/start-output" >&2; fail 'staged-apply setup failed'; }
remember_daemon
printf '%s\n' B >"$CONTROL_ROOT/runtime/next"
run_manager "$CASE_ROOT/apply-output" apply-staged
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/apply-output" >&2; fail 'offline staged activation failed'; }
remember_daemon
assert_equal B "$(cat "$CONTROL_ROOT/runtime/current")" \
    'offline activation did not select the staged slot'
test ! -f "$CONTROL_ROOT/runtime/next" || fail 'offline activation left the next pointer'
test ! -f "$CONTROL_ROOT/runtime/launcher/pending-slot" ||
    fail 'USB was bound before the launcher confirmed or rolled back the staged slot'
assert_equal active "$(sh "$MANAGER" status | sed -n '1p')" \
    'offline activation did not return active status'
assert_equal 11211000.usb "$(cat "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC")" \
    'offline activation did not bind the verified daemon'
grep -q 'selected staged slot B; activation will be verified before USB bind' \
    "$CASE_ROOT/var/local/kindlebridge/usb.log" || fail 'staged slot selection was not recorded'
pass 'staged daemon activates only during an unplugged USB lifecycle'

setup_case readiness_gate
export KINDLEBRIDGE_TEST_NO_HEARTBEAT=1
run_manager "$CASE_ROOT/output" start 0
test "$MANAGER_RC" -ne 0 || fail 'daemon without readiness heartbeat was bound to USB'
assert_equal 0 "$(cat "$CASE_ROOT/lipc/volumd.useUsbForNetwork")" \
    'readiness failure did not return USB ownership to stock volumd'
test ! -e "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/configs/c.1/ffs.kbp" ||
    fail 'readiness failure linked the Bridge function'
pass 'USB bind waits for a fresh daemon readiness heartbeat'

setup_case rollback
export KINDLEBRIDGE_TEST_MOUNT_FAIL=1
run_manager "$CASE_ROOT/output" start 0
test "$MANAGER_RC" -ne 0 || fail 'injected mount failure unexpectedly succeeded'
assert_equal 0 "$(cat "$CASE_ROOT/lipc/volumd.useUsbForNetwork")" 'rollback did not return ownership to volumd'
assert_equal 1 "$(cat "$CASE_ROOT/lipc/mtp.isMtpStarted")" 'rollback did not restore MTP process ownership'
test ! -s "$CASE_ROOT/proc/modules" || fail 'rollback left g_ether loaded'
test ! -d "$CASE_ROOT/var/local/kindlebridge/usb" || fail 'rollback left transition state'
pass 'failed start rolls back through volumd'

setup_case stale_boot
mkdir -p "$CASE_ROOT/var/local/kindlebridge/usb"
printf '%s\n' previous-boot >"$CASE_ROOT/var/local/kindlebridge/usb/boot_id"
printf '%s\n' active >"$CASE_ROOT/var/local/kindlebridge/usb/mode"
run_manager "$CASE_ROOT/output" stop
test "$MANAGER_RC" -eq 0 || fail 'stale-boot cleanup failed'
assert_file_empty "$CASE_ROOT/trace" 'stale-boot cleanup mutated USB state'
test ! -d "$CASE_ROOT/var/local/kindlebridge/usb" || fail 'stale-boot cleanup left state'
pass 'previous-boot state is discarded without USB writes'

setup_case heartbeat_health
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || { cat "$CASE_ROOT/start-output" >&2; fail 'heartbeat health setup failed'; }
remember_daemon
rm -f "$CONTROL_ROOT/runtime/run/heartbeat"
run_manager "$CASE_ROOT/status-output" status
assert_equal recovering "$(sed -n '1p' "$CASE_ROOT/status-output")" \
    'status reported active while the daemon heartbeat was unavailable'
mkdir -p "$CONTROL_ROOT/runtime/launcher"
printf 'KINDLEBRIDGE_WATCHDOG_V1\nslot=A\ncrashes=3\nnext_start_ms=0\nhalted=1\n' \
    >"$CONTROL_ROOT/runtime/launcher/watchdog-state"
run_manager "$CASE_ROOT/halted-status-output" status
assert_equal degraded "$(sed -n '1p' "$CASE_ROOT/halted-status-output")" \
    'status reported active while the launcher crash fuse was halted'
pass 'status distinguishes recovering and degraded health from active USB'

setup_case missing_health_monitor
run_manager "$CASE_ROOT/start-output" start 0
test "$MANAGER_RC" -eq 0 || {
    cat "$CASE_ROOT/start-output" >&2
    fail 'missing-monitor status setup failed'
}
remember_daemon
unset KINDLEBRIDGE_TEST_DISABLE_MONITOR
run_manager "$CASE_ROOT/status-output" status
test "$MANAGER_RC" -ne 0 || fail 'missing health monitor reported active'
assert_equal degraded "$(sed -n '1p' "$CASE_ROOT/status-output")" \
    'missing health monitor was not degraded'
grep -q '^reason=health-monitor$' "$CASE_ROOT/status-output" ||
    fail 'missing health monitor did not report an actionable reason'
export KINDLEBRIDGE_TEST_DISABLE_MONITOR=1
run_manager "$CASE_ROOT/cleanup-output" stop
test "$MANAGER_RC" -eq 0 || {
    cat "$CASE_ROOT/cleanup-output" >&2
    fail 'missing-monitor status cleanup failed'
}
pass 'status fails closed when the health monitor is missing'

setup_case busybox_monitor_ownership
export KINDLEBRIDGE_PID_PROC_ROOT="$CASE_ROOT/pid-proc"
mkdir -p \
    "$KINDLEBRIDGE_PID_PROC_ROOT/101" \
    "$KINDLEBRIDGE_PID_PROC_ROOT/102" \
    "$KINDLEBRIDGE_PID_PROC_ROOT/103" \
    "$CASE_ROOT/var/local/kindlebridge/usb" \
    "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/functions/ffs.kbp" \
    "$CASE_ROOT/dev/usb-ffs/kbp"
printf '%s\0' "$CONTROL_ROOT/bin/kindlebridge-launcher" \
    >"$KINDLEBRIDGE_PID_PROC_ROOT/101/cmdline"
printf '%s\0' "$CONTROL_ROOT/runtime/slots/A/bin/kindlebridged" \
    >"$KINDLEBRIDGE_PID_PROC_ROOT/102/cmdline"
printf 'sh\0%s\0monitor\0' "$(readlink -f "$MANAGER")" \
    >"$KINDLEBRIDGE_PID_PROC_ROOT/103/cmdline"
printf '%s\n' test-boot-id >"$CASE_ROOT/var/local/kindlebridge/usb/boot_id"
printf '%s\n' active >"$CASE_ROOT/var/local/kindlebridge/usb/mode"
printf '%s\n' 11211000.usb >"$CASE_ROOT/var/local/kindlebridge/usb/udc"
printf '%s\n' 101 >"$CASE_ROOT/var/local/kindlebridge/usb/launcher_pid"
printf '%s\n' 102 >"$CASE_ROOT/var/local/kindlebridge/usb/daemon_pid"
printf '%s\n' 103 >"$CASE_ROOT/var/local/kindlebridge/usb/monitor_pid"
printf '%s\n' 102 >"$CONTROL_ROOT/runtime/run/daemon.pid"
timestamp_ms=$(($(date +%s) * 1000))
printf 'KINDLEBRIDGE_HEARTBEAT_V1\ninstance=busybox-monitor\ntimestamp_ms=%s\n' \
    "$timestamp_ms" >"$CONTROL_ROOT/runtime/run/heartbeat"
ln -s "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/functions/ffs.kbp" \
    "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/configs/c.1/ffs.kbp"
: >"$CASE_ROOT/dev/usb-ffs/kbp/ep1"
: >"$CASE_ROOT/dev/usb-ffs/kbp/ep2"
unset KINDLEBRIDGE_TEST_DISABLE_MONITOR
run_manager "$CASE_ROOT/status-output" status
test "$MANAGER_RC" -eq 0 || {
    cat "$CASE_ROOT/status-output" >&2
    fail 'BusyBox-style monitor command line was rejected'
}
assert_equal active "$(sed -n '1p' "$CASE_ROOT/status-output")" \
    'BusyBox-style health monitor did not report active'
pass 'monitor ownership accepts BusyBox bare sh argv0'

setup_case kual_feedback
cp "$MANAGER" "$CONTROL_ROOT/bin/usb-gadget-manager.sh"
chmod 0755 "$CONTROL_ROOT/bin/usb-gadget-manager.sh"
printf '%s\n' 0.1.0-test >"$CONTROL_ROOT/VERSION"
KUAL_CAPTURE="$CASE_ROOT/kual-message"
export KUAL_CAPTURE
printf '%s\n' '#!/bin/sh' 'printf "%s\n" "$3" >"$KUAL_CAPTURE"' \
    >"$CASE_ROOT/bin/kual-capture"
chmod 0755 "$CASE_ROOT/bin/kual-capture"
export KUAL="$CASE_ROOT/bin/kual-capture"
printf '%s\n' 1 >"$CASE_ROOT/sys/class/udc/11211000.usb/connected"
if sh "$KUAL_WRAPPER" start >"$CASE_ROOT/kual-output" 2>&1; then
    fail 'KUAL connected start unexpectedly succeeded'
fi
grep -q 'E-CABLE' "$KUAL_CAPTURE" || fail 'KUAL did not classify the connected-cable failure'
sh "$KUAL_WRAPPER" status >"$CASE_ROOT/kual-status-output" 2>&1
grep -q 'Last action failed: E-CABLE' "$KUAL_CAPTURE" ||
    fail 'KUAL status did not explain the last failure'
printf '%s\n' B >"$CONTROL_ROOT/runtime/next"
printf '%s' '' >"$CASE_ROOT/trace"
if sh "$KUAL_WRAPPER" apply-staged >"$CASE_ROOT/kual-apply-connected-output" 2>&1; then
    fail 'KUAL connected staged update unexpectedly succeeded'
fi
grep -q 'E-CABLE' "$KUAL_CAPTURE" ||
    fail 'KUAL did not classify the connected staged-update failure'
if grep -q 'Applying staged daemon update' "$CASE_ROOT/kual-apply-connected-output"; then
    fail 'KUAL announced staged activation before checking that USB was unplugged'
fi
assert_equal A "$(cat "$CONTROL_ROOT/runtime/current")" \
    'connected staged update changed the current daemon slot'
assert_equal B "$(cat "$CONTROL_ROOT/runtime/next")" \
    'connected staged update discarded the pending daemon slot'
assert_file_empty "$CASE_ROOT/trace" 'connected staged update issued a mutating command'
printf '%s\n' 0 >"$CASE_ROOT/sys/class/udc/11211000.usb/connected"
mkdir -p "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/functions/ncm.usbnetlite"
ln -s "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/functions/ncm.usbnetlite" \
    "$CASE_ROOT/sys/kernel/config/usb_gadget/mtpgadget/configs/c.1/ncm.usbnetlite"
if sh "$KUAL_WRAPPER" start >"$CASE_ROOT/kual-owner-output" 2>&1; then
    fail 'KUAL started KindleBridge while USBNetLite owned USB'
fi
grep -q 'E-OWNER' "$KUAL_CAPTURE" ||
    fail 'KUAL did not explain the USBNetLite ownership conflict'
mkdir -p "$CASE_ROOT/var/local/kindlebridge/usb"
printf '%s\n' 4242 >"$CASE_ROOT/var/local/kindlebridge/usb/launcher_pid"
sh "$KUAL_WRAPPER" export-diagnostics >"$CASE_ROOT/kual-diagnostics-output" 2>&1
test -s "$CASE_ROOT/mnt/us/kindlebridge-diagnostics.txt" ||
    fail 'KUAL did not export diagnostics to USB storage'
grep -q '\[manager status\]' "$CASE_ROOT/mnt/us/kindlebridge-diagnostics.txt" ||
    fail 'KUAL diagnostics omitted manager state'
grep -q '\[managed processes\]' "$CASE_ROOT/mnt/us/kindlebridge-diagnostics.txt" ||
    fail 'KUAL diagnostics omitted process state'
grep -q '^launcher.pid=4242$' "$CASE_ROOT/mnt/us/kindlebridge-diagnostics.txt" ||
    fail 'KUAL diagnostics read the launcher PID from the wrong state path'
rm -f "$CASE_ROOT/var/local/kindlebridge/last-error.log"
mv "$CONTROL_ROOT/bin/usb-gadget-manager.sh" "$CONTROL_ROOT/bin/usb-gadget-manager.real"
printf '%s\n' '#!/bin/sh' \
    'printf "%s\n" degraded reason=daemon-restarted' \
    'exit 1' >"$CONTROL_ROOT/bin/usb-gadget-manager.sh"
chmod 0755 "$CONTROL_ROOT/bin/usb-gadget-manager.sh"
sh "$KUAL_WRAPPER" status >"$CASE_ROOT/kual-degraded-output" 2>&1
grep -q 'Development service stopped' "$KUAL_CAPTURE" ||
    fail 'KUAL did not explain a daemon-restart degraded state'
grep -q 'Choose file transfer' "$KUAL_CAPTURE" ||
    fail 'KUAL degraded recovery omitted the file-transfer step'
rm -f "$CONTROL_ROOT/bin/usb-gadget-manager.sh"
sh "$KUAL_WRAPPER" export-diagnostics >"$CASE_ROOT/kual-missing-manager-diagnostics-output" 2>&1
grep -q '^manager=missing$' "$CASE_ROOT/mnt/us/kindlebridge-diagnostics.txt" ||
    fail 'KUAL could not export diagnostics after the manager was lost'
unset KUAL KUAL_CAPTURE
pass 'KUAL status preserves failures and exports diagnostics'

exitmenu_count=$(grep -c '"exitmenu": false' "$KUAL_MENU")
assert_equal 5 "$exitmenu_count" 'not every KUAL action preserves the menu'
grep -q 'Switch to development mode' "$KUAL_MENU" || fail 'KUAL development action is ambiguous'
grep -q 'Switch to USB file transfer' "$KUAL_MENU" || fail 'KUAL file-transfer action is ambiguous'
grep -q '"params": "apply-staged"' "$KUAL_MENU" || fail 'KUAL has no staged-update action'
if grep -q 'runtime/next.*-f' "$KUAL_MENU"; then
    fail 'KUAL hides the staged-update action behind a cache-stale external marker'
fi
grep -q 'start 0' "$KUAL_WRAPPER" || fail 'KUAL start still has a safety timeout'
grep -q 'preflight apply-staged' "$KUAL_WRAPPER" ||
    fail 'KUAL staged activation has no read-only cable preflight'
grep -q 'Export diagnostics' "$KUAL_MENU" || fail 'KUAL has no diagnostics export action'
grep -q 'E-DAEMON' "$KUAL_WRAPPER" || fail 'KUAL daemon failures do not fit on screen as a short code'
grep -q 'last-error.log' "$KUAL_WRAPPER" || fail 'KUAL does not preserve the full failure detail'
if grep -q 'nohup' "$KUAL_WRAPPER"; then
    fail 'KUAL wrapper hides transition output in a detached process'
fi
grep -q 'active|recovering|degraded|detached|acquiring-stock-usb|starting|stopping|stale' "$PROJECT_DIR/packaging/mrpi/install.sh" ||
    fail 'installer can replace files during USB acquisition'
grep -q 'active|recovering|degraded|detached|acquiring-stock-usb|starting|stopping|stale' "$PROJECT_DIR/packaging/mrpi/uninstall.sh" ||
    fail 'uninstaller can remove files during USB acquisition'
grep -q 'usb_gadget/mtpgadget/UDC' "$KUAL_WRAPPER" ||
    fail 'diagnostics do not inspect the managed mtpgadget UDC binding'
if grep -q 'usb_gadget/kindlebridge/UDC' "$KUAL_WRAPPER"; then
    fail 'diagnostics still inspect the obsolete kindlebridge gadget path'
fi
pass 'KUAL actions stay in-menu and start without a timeout'

printf '1..%s\n' "$PASSED"
