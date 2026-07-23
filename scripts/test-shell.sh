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
    scripts/hardware/usb-mode-cycle-gate.sh \
    tests/fixtures/usb-mode/fake-command.sh \
    tests/fixtures/usb-mode/cleanup-process-tree.sh \
    tests/fixtures/usb-mode/kindlebridge-launcher \
    tests/fixtures/usb-mode/kindlebridged \
    tests/fixtures/usb-mode/same-name-launcher \
    tests/mrpi-installer.sh \
    tests/usb-mode-lifecycle.sh; do
    sh -n "$script"
done

cleanup_fixture="${TMPDIR:-/tmp}/kindlebridge-cleanup-process-tree.$$"
mkdir -p "$cleanup_fixture"
cat >"$cleanup_fixture/parent.sh" <<'EOF'
#!/bin/sh
sleep 30 &
printf '%s\n' "$!" >"$1"
wait
EOF
chmod 0755 "$cleanup_fixture/parent.sh"
"$cleanup_fixture/parent.sh" "$cleanup_fixture/child.pid" &
cleanup_parent=$!
cleanup_process_tree_fixture() {
    sh tests/fixtures/usb-mode/cleanup-process-tree.sh "$cleanup_fixture" >/dev/null 2>&1 || true
    wait "$cleanup_parent" 2>/dev/null || true
    rm -rf "$cleanup_fixture"
}
trap cleanup_process_tree_fixture EXIT
trap 'exit 1' HUP INT TERM
attempts=20
while test "$attempts" -gt 0 && test ! -s "$cleanup_fixture/child.pid"; do
    /usr/bin/sleep 0.05
    attempts=$((attempts - 1))
done
test -s "$cleanup_fixture/child.pid" || {
    echo 'cleanup process-tree fixture did not start' >&2
    exit 1
}
cleanup_child=$(cat "$cleanup_fixture/child.pid")
sh tests/fixtures/usb-mode/cleanup-process-tree.sh "$cleanup_fixture"
if kill -0 "$cleanup_parent" 2>/dev/null || kill -0 "$cleanup_child" 2>/dev/null; then
    echo 'cleanup process-tree fixture left a process behind' >&2
    exit 1
fi
wait "$cleanup_parent" 2>/dev/null || true
rm -rf "$cleanup_fixture"
trap - EXIT HUP INT TERM

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
