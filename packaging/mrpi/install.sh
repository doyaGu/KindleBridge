#!/bin/sh
# Internal KindleBridge development installer. Userstore/varlocal only.

set -eu

BASE=/mnt/us/kindlebridge
EXT=/mnt/us/extensions/kindlebridge
STAGE=/mnt/us/.kindlebridge-install.$$
OLD=/mnt/us/.kindlebridge-previous.$$
PAYLOAD_ROOT=/mnt/us/.kindlebridge-payload.$$

cleanup() {
    rm -rf "$STAGE" "$PAYLOAD_ROOT"
    if test -d "$OLD" && test -d "$BASE"; then
        rm -rf "$OLD"
    fi
}
trap cleanup EXIT HUP INT TERM

test -f payload.tar || { echo "KindleBridge payload archive missing" >&2; return 1; }
mkdir "$PAYLOAD_ROOT"
tar -xf payload.tar -C "$PAYLOAD_ROOT"
test -d "$PAYLOAD_ROOT/kindlebridge" || { echo "KindleBridge payload missing" >&2; return 1; }
test -d "$PAYLOAD_ROOT/extensions/kindlebridge" || { echo "KindleBridge KUAL payload missing" >&2; return 1; }

if test -x "$BASE/bin/usb-gadget-manager.sh"; then
    bridge_status=$("$BASE/bin/usb-gadget-manager.sh" status 2>/dev/null | sed -n '1p' || true)
    case "$bridge_status" in
        active|detached|acquiring-stock-usb|starting|stopping|stale)
            echo "Stop KindleBridge before updating it (status: $bridge_status)" >&2
            return 1
            ;;
        *) rm -rf /var/local/kindlebridge/usb ;;
    esac
else
    rm -rf /var/local/kindlebridge/usb
fi

mkdir -p "$STAGE/bin" /mnt/us/extensions /var/local/kindlebridge
cp -af "$PAYLOAD_ROOT/kindlebridge/." "$STAGE/"
chmod 0755 "$STAGE/bin/kindlebridge-launcher" "$STAGE/bin/usb-gadget-manager.sh" \
    "$STAGE/runtime/slots/A/bin/kindlebridged" \
    "$STAGE/runtime/slots/B/bin/kindlebridged"

if test -d "$BASE"; then
    mv "$BASE" "$OLD"
fi
if ! mv "$STAGE" "$BASE"; then
    if test -d "$OLD"; then
        mv "$OLD" "$BASE" || true
    fi
    return 1
fi
rm -rf "$OLD"

rm -rf "$EXT"
cp -af "$PAYLOAD_ROOT/extensions/kindlebridge" "$EXT"
chmod 0755 "$EXT/bin/kindlebridge.sh"

printf '%s\n' '0.1.0-dev' >"$BASE/VERSION"
echo "KindleBridge development package installed"
return 0
