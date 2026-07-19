#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_DIR"

for script in \
    packaging/mrpi/install.sh \
    packaging/mrpi/uninstall.sh \
    packaging/mrpi/payload/kindlebridge/bin/usb-gadget-manager.sh \
    packaging/mrpi/payload/extensions/kindlebridge/bin/kindlebridge.sh \
    scripts/archive/unsafe-kt6-usb-lab.sh \
    tests/fixtures/usb-mode/fake-command.sh \
    tests/fixtures/usb-mode/kindlebridge-launcher \
    tests/fixtures/usb-mode/kindlebridged \
    tests/fixtures/usb-mode/same-name-launcher \
    tests/mrpi-installer.sh \
    tests/usb-mode-lifecycle.sh; do
    sh -n "$script"
done

if KINDLEBRIDGE_ALLOW_UNSAFE_USB_LAB=0 \
    sh scripts/archive/unsafe-kt6-usb-lab.sh restore >/dev/null 2>&1; then
    echo 'retired unsafe USB lab accepted a mutating command' >&2
    exit 1
fi

if grep -q 'soft_connect\|/sys/bus/platform/drivers/mtu3' \
    packaging/mrpi/payload/kindlebridge/bin/usb-gadget-manager.sh; then
    echo 'production USB manager contains retired direct-controller recovery' >&2
    exit 1
fi

MANAGER=packaging/mrpi/payload/kindlebridge/bin/usb-gadget-manager.sh
if grep -q 'ffs\.kindlebridge\|usb-ffs/kindlebridge\|functionfs kindlebridge' "$MANAGER"; then
    echo 'production USB manager contains the retired FunctionFS instance name' >&2
    exit 1
fi
grep -q 'functions/ffs\.kbp' "$MANAGER" || {
    echo 'production USB manager does not create the ffs.kbp protocol function' >&2
    exit 1
}

sh tests/mrpi-installer.sh

test_output="${TMPDIR:-/tmp}/kindlebridge-usb-lifecycle.$$"
cleanup_test_output() {
    rm -f "$test_output"
}
trap cleanup_test_output EXIT
trap 'exit 1' HUP INT TERM

if sh tests/usb-mode-lifecycle.sh >"$test_output" 2>&1; then
    test_status=0
else
    test_status=$?
fi
cat "$test_output"
test "$test_status" -eq 0 || exit "$test_status"

# A signal-handling regression in a test fixture must not turn a partial TAP
# stream into a successful gate. Derive the plan from the source so adding a
# case does not require another hard-coded count here.
expected_count=$(grep -c "^pass '" tests/usb-mode-lifecycle.sh)
actual_count=$(grep -c '^ok [0-9][0-9]* - ' "$test_output" || true)
actual_plan=$(sed -n '/^1\.\.[0-9][0-9]*$/p' "$test_output" | tail -n 1)
plan_count=$(grep -c '^1\.\.[0-9][0-9]*$' "$test_output" || true)
expected_plan="1..$expected_count"
actual_numbers=$(sed -n 's/^ok \([0-9][0-9]*\) - .*/\1/p' "$test_output")
expected_numbers=$(awk -v count="$expected_count" 'BEGIN { for (i = 1; i <= count; i++) print i }')
if grep -q '^not ok ' "$test_output" ||
    test "$plan_count" -ne 1 ||
    test "$actual_count" -ne "$expected_count" ||
    test "$actual_plan" != "$expected_plan" ||
    test "$actual_numbers" != "$expected_numbers"; then
    echo "USB lifecycle test output was incomplete: expected $expected_plan with $expected_count cases, got plan '${actual_plan:-missing}' with $actual_count cases" >&2
    exit 1
fi
