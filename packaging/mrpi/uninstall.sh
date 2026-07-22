#!/bin/sh
# Complete uninstall for the internal KindleBridge development package.

set -eu

MNT_US_ROOT=${KINDLEBRIDGE_MNT_US_ROOT:-/mnt/us}
VAR_LOCAL_ROOT=${KINDLEBRIDGE_VAR_LOCAL_ROOT:-/var/local}
BASE="$VAR_LOCAL_ROOT/kindlebridge/control"
MANAGER=
if test -x "$BASE/bin/usb-gadget-manager.sh"; then
    MANAGER="$BASE/bin/usb-gadget-manager.sh"
fi
if test -n "$MANAGER"; then
    bridge_status=$("$MANAGER" status 2>/dev/null | sed -n '1p' || true)
    case "$bridge_status" in
        active|recovering|degraded|detached|acquiring-stock-usb|starting|stopping|stale|stale-from-previous-boot)
            echo "Stop KindleBridge before uninstalling it (status: $bridge_status)" >&2
            return 1
            ;;
        inactive) rm -rf "$VAR_LOCAL_ROOT/kindlebridge/usb" ;;
        *)
            echo "KindleBridge has an unknown USB state: $bridge_status" >&2
            echo "Unplug USB and use recovery before uninstalling it." >&2
            return 1
            ;;
    esac
else
    if test -d "$VAR_LOCAL_ROOT/kindlebridge/usb"; then
        echo "KindleBridge USB state exists but its manager is missing." >&2
        echo "Reinstall the same version and recover USB before uninstalling it." >&2
        return 1
    fi
fi
rm -rf "$MNT_US_ROOT/extensions/kindlebridge"
rm -rf "$VAR_LOCAL_ROOT/kindlebridge"
# Deliberately preserve /mnt/us/kindlebridge-data (developer payloads).
echo "KindleBridge development package removed"
return 0
