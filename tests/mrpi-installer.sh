#!/bin/sh

set -eu

TESTS_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$TESTS_DIR/.." && pwd)
INSTALLER="$PROJECT_DIR/packaging/mrpi/install.sh"
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
        '  status) echo "${KINDLEBRIDGE_TEST_STATUS:-inactive}" ;;' \
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

make_old_install() {
    case_root=$1
    mkdir -p "$case_root/mnt/us/kindlebridge/bin" \
        "$case_root/mnt/us/extensions/kindlebridge"
    make_script "$case_root/mnt/us/kindlebridge/bin/usb-gadget-manager.sh" old
    printf '%s\n' old-install >"$case_root/mnt/us/kindlebridge/install-marker"
}

run_installer() {
    case_root=$1
    test_status=$2
    test_stop_rc=$3
    test_start_rc=$4
    test_stop_message=${5:-}
    test_start_message=${6:-}
    trace="$case_root/trace"
    : >"$trace"
    if (
        cd "$case_root/package"
        KINDLEBRIDGE_MNT_US_ROOT="$case_root/mnt/us"
        KINDLEBRIDGE_VAR_LOCAL_ROOT="$case_root/var/local"
        KINDLEBRIDGE_INSTALL_TRACE="$trace"
        KINDLEBRIDGE_TEST_STATUS="$test_status"
        KINDLEBRIDGE_TEST_STOP_RC="$test_stop_rc"
        KINDLEBRIDGE_TEST_START_RC="$test_start_rc"
        KINDLEBRIDGE_TEST_STOP_MESSAGE="$test_stop_message"
        KINDLEBRIDGE_TEST_START_MESSAGE="$test_start_message"
        export KINDLEBRIDGE_MNT_US_ROOT KINDLEBRIDGE_VAR_LOCAL_ROOT \
            KINDLEBRIDGE_INSTALL_TRACE KINDLEBRIDGE_TEST_STATUS \
            KINDLEBRIDGE_TEST_STOP_RC KINDLEBRIDGE_TEST_START_RC \
            KINDLEBRIDGE_TEST_STOP_MESSAGE KINDLEBRIDGE_TEST_START_MESSAGE
        . "$INSTALLER"
    ) >"$case_root/output" 2>&1; then
        INSTALL_RC=0
    else
        INSTALL_RC=$?
    fi
}

case_root="$RUN_ROOT/success"
make_payload "$case_root"
make_old_install "$case_root"
run_installer "$case_root" active 0 0
test "$INSTALL_RC" -eq 0 || { cat "$case_root/output" >&2; fail 'self-managed upgrade failed'; }
test "$(sed -n '1p' "$case_root/trace")" = old-stop || fail 'upgrade did not stop the old manager first'
test "$(sed -n '2p' "$case_root/trace")" = new-start || fail 'upgrade did not start the new manager'
test "$(cat "$case_root/mnt/us/kindlebridge/install-marker")" = new-install || fail 'new payload was not committed'
grep -q 'installed and ready' "$case_root/output" || fail 'successful install did not give a next step'

case_root="$RUN_ROOT/connected"
make_payload "$case_root"
make_old_install "$case_root"
run_installer "$case_root" active 1 0 'Unplug USB before stopping KindleBridge'
test "$INSTALL_RC" -ne 0 || fail 'installer replaced an active bridge it could not stop'
test "$(cat "$case_root/mnt/us/kindlebridge/install-marker")" = old-install || fail 'failed preflight replaced the old install'
grep -q 'Unplug the USB cable' "$case_root/output" || fail 'failed preflight did not tell the user what to do'

case_root="$RUN_ROOT/rollback"
make_payload "$case_root"
make_old_install "$case_root"
run_installer "$case_root" active 0 1 '' 'simulated new daemon failure'
test "$INSTALL_RC" -ne 0 || fail 'installer accepted a new bridge that did not start'
test "$(cat "$case_root/mnt/us/kindlebridge/install-marker")" = old-install || fail 'failed activation did not restore the old install'
test "$(sed -n '1p' "$case_root/trace")" = old-stop || fail 'rollback case did not stop the old bridge'
test "$(sed -n '2p' "$case_root/trace")" = new-start || fail 'rollback case did not try the new bridge'
test "$(sed -n '3p' "$case_root/trace")" = old-start || fail 'rollback case did not restart the old bridge'
grep -q 'previous version was restored' "$case_root/output" || fail 'rollback was not explained to the user'

echo 'MRPI self-managed upgrade tests passed.'
