#!/bin/sh
# KindleBridge development installer. Userstore/varlocal only.

set -eu

MNT_US_ROOT=${KINDLEBRIDGE_MNT_US_ROOT:-/mnt/us}
VAR_LOCAL_ROOT=${KINDLEBRIDGE_VAR_LOCAL_ROOT:-/var/local}
BASE="$MNT_US_ROOT/kindlebridge"
EXT="$MNT_US_ROOT/extensions/kindlebridge"
STAGE_BASE="$MNT_US_ROOT/.kindlebridge-install.$$"
STAGE_EXT="$MNT_US_ROOT/.kindlebridge-extension.$$"
OLD_BASE="$MNT_US_ROOT/.kindlebridge-previous.$$"
OLD_EXT="$MNT_US_ROOT/.kindlebridge-extension-previous.$$"
PAYLOAD_ROOT="$MNT_US_ROOT/.kindlebridge-payload.$$"
PAYLOAD_ARCHIVE=${KINDLEBRIDGE_PAYLOAD_ARCHIVE:-payload.tar}
COMMITTED=0
OLD_WAS_RUNNING=0

cleanup() {
    rm -rf "$STAGE_BASE" "$STAGE_EXT" "$PAYLOAD_ROOT"
    if test "$COMMITTED" = 1; then
        rm -rf "$OLD_BASE" "$OLD_EXT"
    fi
}

restore_previous_install() {
    rm -rf "$BASE" "$EXT"
    if test -d "$OLD_BASE"; then
        mv "$OLD_BASE" "$BASE" || true
    fi
    if test -d "$OLD_EXT"; then
        mkdir -p "$(dirname "$EXT")"
        mv "$OLD_EXT" "$EXT" || true
    fi
}

trap cleanup EXIT HUP INT TERM

test -f "$PAYLOAD_ARCHIVE" || {
    echo "KindleBridge install file is incomplete. Copy the package again." >&2
    return 1
}
mkdir "$PAYLOAD_ROOT"
tar -xf "$PAYLOAD_ARCHIVE" -C "$PAYLOAD_ROOT"
test -d "$PAYLOAD_ROOT/kindlebridge" || {
    echo "KindleBridge program files are missing from the package." >&2
    return 1
}
test -d "$PAYLOAD_ROOT/extensions/kindlebridge" || {
    echo "KindleBridge KUAL files are missing from the package." >&2
    return 1
}
test -f "$PAYLOAD_ROOT/kindlebridge/VERSION" || {
    echo "KindleBridge package version is missing; the previous version was not replaced." >&2
    return 1
}
PACKAGE_VERSION=$(tr -d '\r\n' <"$PAYLOAD_ROOT/kindlebridge/VERSION")
case "$PACKAGE_VERSION" in
    ''|*[!0-9A-Za-z.-]*)
        echo "KindleBridge package version is invalid; the previous version was not replaced." >&2
        return 1
        ;;
esac

# Upgrades are self-managing. The old manager owns the old processes, so ask it
# to return USB to stock before replacing any executable or KUAL file.
if test -x "$BASE/bin/usb-gadget-manager.sh"; then
    bridge_status=$("$BASE/bin/usb-gadget-manager.sh" status 2>/dev/null | sed -n '1p' || true)
    case "$bridge_status" in
        active|recovering|degraded|detached|acquiring-stock-usb|starting|stopping|stale|stale-from-previous-boot)
            OLD_WAS_RUNNING=1
            if ! stop_output=$("$BASE/bin/usb-gadget-manager.sh" stop 2>&1); then
                echo "KindleBridge could not prepare the update." >&2
                echo "Unplug the USB cable, then run MRPI again." >&2
                echo "$stop_output" >&2
                return 1
            fi
            ;;
        inactive|'')
            rm -rf "$VAR_LOCAL_ROOT/kindlebridge/usb"
            ;;
        *)
            echo "KindleBridge has an unknown USB state: $bridge_status" >&2
            echo "Unplug USB, open KindleBridge status and recovery steps, then retry." >&2
            return 1
            ;;
    esac
else
    rm -rf "$VAR_LOCAL_ROOT/kindlebridge/usb"
fi

mkdir -p "$STAGE_BASE/bin" "$STAGE_EXT" "$MNT_US_ROOT/extensions" \
    "$VAR_LOCAL_ROOT/kindlebridge"
cp -af "$PAYLOAD_ROOT/kindlebridge/." "$STAGE_BASE/"
cp -af "$PAYLOAD_ROOT/extensions/kindlebridge/." "$STAGE_EXT/"
chmod 0755 "$STAGE_BASE/bin/kindlebridge-launcher" \
    "$STAGE_BASE/bin/usb-gadget-manager.sh" \
    "$STAGE_BASE/runtime/slots/A/bin/kindlebridged" \
    "$STAGE_BASE/runtime/slots/B/bin/kindlebridged" \
    "$STAGE_EXT/bin/kindlebridge.sh"

if test -d "$BASE"; then
    mv "$BASE" "$OLD_BASE"
fi
if test -d "$EXT"; then
    mv "$EXT" "$OLD_EXT"
fi
if ! mv "$STAGE_BASE" "$BASE" || ! mv "$STAGE_EXT" "$EXT"; then
    restore_previous_install
    echo "KindleBridge update could not replace its files; the previous version was restored." >&2
    return 1
fi

if test -e "$MNT_US_ROOT/KINDLEBRIDGE_DISABLE"; then
    COMMITTED=1
    echo "KindleBridge installed but disabled by KINDLEBRIDGE_DISABLE."
    echo "Remove that file, then choose 'Switch to development mode' in KUAL."
    return 0
fi

if start_output=$("$BASE/bin/usb-gadget-manager.sh" start 0 2>&1); then
    COMMITTED=1
    echo "KindleBridge $PACKAGE_VERSION installed and ready."
    echo "Connect the USB cable to the computer."
    return 0
fi

restore_previous_install
restart_note=
if test "$OLD_WAS_RUNNING" = 1 && test -x "$BASE/bin/usb-gadget-manager.sh"; then
    if "$BASE/bin/usb-gadget-manager.sh" start 0 >/dev/null 2>&1; then
        restart_note="The previous KindleBridge version is active again."
    else
        restart_note="The previous version was restored but could not be started."
    fi
fi
echo "KindleBridge update did not start, so the previous version was restored." >&2
echo "Unplug the USB cable and run MRPI again." >&2
echo "$start_output" >&2
test -z "$restart_note" || echo "$restart_note" >&2
return 1
