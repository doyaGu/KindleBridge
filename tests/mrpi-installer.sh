#!/bin/sh

set -eu

TESTS_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$TESTS_DIR/.." && pwd)
INSTALLER="$PROJECT_DIR/packaging/mrpi/install.sh"
UNINSTALLER="$PROJECT_DIR/packaging/mrpi/uninstall.sh"
RUN_ROOT="$PROJECT_DIR/.kindlebridge-test-state/mrpi-installer.$$"

cleanup() {
    rm -rf "$RUN_ROOT"
}
trap cleanup EXIT HUP INT TERM

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

make_script() {
    destination=$1
    role=$2
    mkdir -p "$(dirname "$destination")"
    printf '%s\n' \
        '#!/bin/sh' \
        "role=$role" \
        'case "${1:-}" in' \
        '  status) echo "${KINDLEBRIDGE_TEST_STATUS-inactive}" ;;' \
        '  stop) echo "$role-stop" >>"$KINDLEBRIDGE_INSTALL_TRACE"; echo "${KINDLEBRIDGE_TEST_STOP_MESSAGE:-}"; exit "${KINDLEBRIDGE_TEST_STOP_RC:-0}" ;;' \
        '  start) echo "$role-start" >>"$KINDLEBRIDGE_INSTALL_TRACE"; echo "${KINDLEBRIDGE_TEST_START_MESSAGE:-}"; if test "$role" = old; then exit "${KINDLEBRIDGE_TEST_OLD_START_RC:-0}"; else exit "${KINDLEBRIDGE_TEST_START_RC:-0}"; fi ;;' \
        '  *) exit 0 ;;' \
        'esac' >"$destination"
    chmod 0755 "$destination"
}

make_payload() {
    case_root=$1
    payload="$case_root/payload"
    package="$case_root/package"
    mkdir -p "$payload/kindlebridge/bin" \
        "$payload/kindlebridge/runtime/slots/A/bin" \
        "$payload/kindlebridge/runtime/slots/B/bin" \
        "$payload/extensions/kindlebridge/bin" "$package"
    make_script "$payload/kindlebridge/bin/usb-gadget-manager.sh" new
    make_script "$payload/kindlebridge/bin/kindlebridge-launcher" launcher
    make_script "$payload/kindlebridge/runtime/slots/A/bin/kindlebridged" daemon-a
    make_script "$payload/kindlebridge/runtime/slots/B/bin/kindlebridged" daemon-b
    make_script "$payload/extensions/kindlebridge/bin/kindlebridge.sh" kual
    printf '%s\n' new-install >"$payload/kindlebridge/install-marker"
    printf '%s\n' 0.1.0-test >"$payload/kindlebridge/VERSION"
    tar -cf "$package/payload.tar" -C "$payload" kindlebridge extensions
}

make_current_install() {
    case_root=$1
    mkdir -p "$case_root/var/local/kindlebridge/control/bin" \
        "$case_root/mnt/us/extensions/kindlebridge"
    make_script "$case_root/var/local/kindlebridge/control/bin/usb-gadget-manager.sh" old
    printf '%s\n' old-install >"$case_root/var/local/kindlebridge/control/install-marker"
}

run_installer() {
    case_root=$1
    test_status=$2
    test_stop_rc=$3
    test_start_rc=$4
    test_stop_message=${5:-}
    test_start_message=${6:-}
    trace="$case_root/trace"
    mkdir -p "$case_root/mnt/us" "$case_root/var/local" "$case_root/sys" "$case_root/proc"
    : >"$trace"
    if (
        cd "$case_root/package"
        KINDLEBRIDGE_MNT_US_ROOT="$case_root/mnt/us"
        KINDLEBRIDGE_VAR_LOCAL_ROOT="$case_root/var/local"
        KINDLEBRIDGE_SYS_ROOT="$case_root/sys"
        KINDLEBRIDGE_PROC_ROOT="$case_root/proc"
        KINDLEBRIDGE_INSTALL_TRACE="$trace"
        KINDLEBRIDGE_TEST_STATUS="$test_status"
        KINDLEBRIDGE_TEST_STOP_RC="$test_stop_rc"
        KINDLEBRIDGE_TEST_START_RC="$test_start_rc"
        KINDLEBRIDGE_TEST_STOP_MESSAGE="$test_stop_message"
        KINDLEBRIDGE_TEST_START_MESSAGE="$test_start_message"
        KINDLEBRIDGE_TEST_SKIP_SYNC=1
        export KINDLEBRIDGE_MNT_US_ROOT KINDLEBRIDGE_VAR_LOCAL_ROOT \
            KINDLEBRIDGE_SYS_ROOT KINDLEBRIDGE_PROC_ROOT \
            KINDLEBRIDGE_INSTALL_TRACE KINDLEBRIDGE_TEST_STATUS \
            KINDLEBRIDGE_TEST_STOP_RC KINDLEBRIDGE_TEST_START_RC \
            KINDLEBRIDGE_TEST_STOP_MESSAGE KINDLEBRIDGE_TEST_START_MESSAGE \
            KINDLEBRIDGE_TEST_SKIP_SYNC
        . "$INSTALLER"
    ) >"$case_root/output" 2>&1; then
        INSTALL_RC=0
    else
        INSTALL_RC=$?
    fi
}

write_transaction() {
    case_root=$1
    state=$2
    had_base=$3
    had_ext=$4
    mkdir -p "$case_root/var/local/kindlebridge"
    {
        echo KINDLEBRIDGE_INSTALL_TRANSACTION_V2
        echo "state=$state"
        echo "had_base=$had_base"
        echo "had_ext=$had_ext"
    } >"$case_root/var/local/kindlebridge/.install-transaction"
}

make_persistent_install() {
    case_root=$1
    mkdir -p "$case_root/var/local/kindlebridge/control/bin" \
        "$case_root/var/local/kindlebridge/usb" \
        "$case_root/mnt/us/extensions/kindlebridge"
    make_script "$case_root/var/local/kindlebridge/control/bin/usb-gadget-manager.sh" old
    printf '%s\n' installed >"$case_root/var/local/kindlebridge/control/install-marker"
    printf '%s\n' kual >"$case_root/mnt/us/extensions/kindlebridge/install-marker"
}

run_uninstaller() {
    case_root=$1
    test_status=$2
    trace="$case_root/trace"
    : >"$trace"
    if (
        KINDLEBRIDGE_MNT_US_ROOT="$case_root/mnt/us"
        KINDLEBRIDGE_VAR_LOCAL_ROOT="$case_root/var/local"
        KINDLEBRIDGE_INSTALL_TRACE="$trace"
        KINDLEBRIDGE_TEST_STATUS="$test_status"
        export KINDLEBRIDGE_MNT_US_ROOT KINDLEBRIDGE_VAR_LOCAL_ROOT \
            KINDLEBRIDGE_INSTALL_TRACE KINDLEBRIDGE_TEST_STATUS
        . "$UNINSTALLER"
    ) >"$case_root/output" 2>&1; then
        UNINSTALL_RC=0
    else
        UNINSTALL_RC=$?
    fi
}

case_root="$RUN_ROOT/missing-manager-state"
make_payload "$case_root"
mkdir -p "$case_root/var/local/kindlebridge/usb"
printf '%s\n' active >"$case_root/var/local/kindlebridge/usb/mode"
run_installer "$case_root" inactive 0 0
test "$INSTALL_RC" -ne 0 || fail 'installer accepted runtime state without its manager'
test "$(cat "$case_root/var/local/kindlebridge/usb/mode")" = active ||
    fail 'manager-missing refusal deleted USB state'
test ! -f "$case_root/var/local/kindlebridge/control/install-marker" ||
    fail 'manager-missing refusal installed a new control plane'
grep -q 'runtime state exists but its manager is missing' "$case_root/output" ||
    fail 'manager-missing refusal did not explain the unsafe state'

case_root="$RUN_ROOT/prepared-transaction-recovery"
mkdir -p "$case_root/package" \
    "$case_root/var/local/kindlebridge/control" \
    "$case_root/var/local/kindlebridge/.control-previous/bin" \
    "$case_root/mnt/us/.kindlebridge-extension-previous"
printf '%s\n' partial-new >"$case_root/var/local/kindlebridge/control/install-marker"
make_script "$case_root/var/local/kindlebridge/.control-previous/bin/usb-gadget-manager.sh" old
printf '%s\n' previous-install >"$case_root/var/local/kindlebridge/.control-previous/install-marker"
printf '%s\n' previous-kual >"$case_root/mnt/us/.kindlebridge-extension-previous/install-marker"
write_transaction "$case_root" prepared 1 1
printf '%s\n' invalid >"$case_root/package/payload.tar"
run_installer "$case_root" inactive 0 0
test "$INSTALL_RC" -ne 0 || fail 'invalid package unexpectedly installed after recovery'
test "$(cat "$case_root/var/local/kindlebridge/control/install-marker")" = previous-install ||
    fail 'prepared transaction recovery did not restore the previous control plane'
test "$(cat "$case_root/mnt/us/extensions/kindlebridge/install-marker")" = previous-kual ||
    fail 'prepared transaction recovery did not restore the KUAL extension'
test ! -e "$case_root/var/local/kindlebridge/.install-transaction" ||
    fail 'prepared transaction recovery left its marker'

case_root="$RUN_ROOT/committed-transaction-recovery"
mkdir -p "$case_root/package" \
    "$case_root/var/local/kindlebridge/control" \
    "$case_root/var/local/kindlebridge/.control-previous" \
    "$case_root/mnt/us/extensions/kindlebridge" \
    "$case_root/mnt/us/.kindlebridge-extension-previous"
printf '%s\n' committed-new >"$case_root/var/local/kindlebridge/control/install-marker"
printf '%s\n' previous-install >"$case_root/var/local/kindlebridge/.control-previous/install-marker"
printf '%s\n' committed-kual >"$case_root/mnt/us/extensions/kindlebridge/install-marker"
printf '%s\n' previous-kual >"$case_root/mnt/us/.kindlebridge-extension-previous/install-marker"
write_transaction "$case_root" committed 1 1
printf '%s\n' invalid >"$case_root/package/payload.tar"
run_installer "$case_root" inactive 0 0
test "$INSTALL_RC" -ne 0 || fail 'invalid package unexpectedly installed after commit cleanup'
test "$(cat "$case_root/var/local/kindlebridge/control/install-marker")" = committed-new ||
    fail 'committed transaction recovery rolled back the new control plane'
test "$(cat "$case_root/mnt/us/extensions/kindlebridge/install-marker")" = committed-kual ||
    fail 'committed transaction recovery rolled back the new KUAL extension'
test ! -e "$case_root/var/local/kindlebridge/.control-previous" ||
    fail 'committed transaction recovery left the old control plane'
test ! -e "$case_root/mnt/us/.kindlebridge-extension-previous" ||
    fail 'committed transaction recovery left the old KUAL extension'
test ! -e "$case_root/var/local/kindlebridge/.install-transaction" ||
    fail 'committed transaction recovery left its marker'

case_root="$RUN_ROOT/fresh"
make_payload "$case_root"
run_installer "$case_root" inactive 0 0
test "$INSTALL_RC" -eq 0 || { cat "$case_root/output" >&2; fail 'fresh persistent install failed'; }
test ! -s "$case_root/trace" || fail 'fresh install changed the USB mode'
test "$(cat "$case_root/var/local/kindlebridge/control/install-marker")" = new-install || fail 'fresh install did not commit the persistent control plane'
test ! -d "$case_root/mnt/us/kindlebridge" || fail 'fresh install created an FSP-backed control plane'
grep -q 'installed but inactive' "$case_root/output" || fail 'fresh install did not explain explicit activation'

case_root="$RUN_ROOT/success"
make_payload "$case_root"
make_current_install "$case_root"
run_installer "$case_root" inactive 0 0
test "$INSTALL_RC" -eq 0 || { cat "$case_root/output" >&2; fail 'inactive upgrade failed'; }
test ! -s "$case_root/trace" || fail 'inactive upgrade changed the USB mode'
test "$(cat "$case_root/var/local/kindlebridge/control/install-marker")" = new-install || fail 'new payload was not committed to persistent storage'
grep -q 'installed but inactive' "$case_root/output" || fail 'upgrade did not give an explicit next step'

case_root="$RUN_ROOT/active"
make_payload "$case_root"
make_current_install "$case_root"
run_installer "$case_root" active 0 0
test "$INSTALL_RC" -ne 0 || fail 'installer replaced an active bridge'
test ! -s "$case_root/trace" || fail 'active-upgrade refusal stopped or restarted the bridge'
test "$(cat "$case_root/var/local/kindlebridge/control/install-marker")" = old-install || fail 'active-upgrade refusal replaced the old install'
grep -q 'Switch to USB file transfer' "$case_root/output" || fail 'active-upgrade refusal did not explain recovery'

case_root="$RUN_ROOT/empty-status-upgrade"
make_payload "$case_root"
make_current_install "$case_root"
run_installer "$case_root" '' 0 0
test "$INSTALL_RC" -ne 0 || fail 'installer treated an empty existing-manager status as inactive'
test "$(cat "$case_root/var/local/kindlebridge/control/install-marker")" = old-install ||
    fail 'empty-status upgrade replaced the old install'
grep -q 'unknown USB state' "$case_root/output" ||
    fail 'empty-status upgrade did not explain the fail-closed refusal'

case_root="$RUN_ROOT/empty-status-uninstall"
make_persistent_install "$case_root"
run_uninstaller "$case_root" ''
test "$UNINSTALL_RC" -ne 0 || fail 'uninstaller accepted an empty manager status'
test -f "$case_root/var/local/kindlebridge/control/install-marker" ||
    fail 'empty-status uninstall removed the control plane'
test -f "$case_root/mnt/us/extensions/kindlebridge/install-marker" ||
    fail 'empty-status uninstall removed the KUAL extension'

case_root="$RUN_ROOT/inactive-uninstall"
make_persistent_install "$case_root"
run_uninstaller "$case_root" inactive
test "$UNINSTALL_RC" -eq 0 || { cat "$case_root/output" >&2; fail 'inactive uninstall failed'; }
test ! -e "$case_root/var/local/kindlebridge" || fail 'inactive uninstall left persistent files'
test ! -e "$case_root/mnt/us/extensions/kindlebridge" || fail 'inactive uninstall left the KUAL extension'

echo 'MRPI explicit-activation install tests passed.'
