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

sh tests/usb-mode-lifecycle.sh
