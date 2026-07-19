#!/bin/sh
# Complete uninstall for the internal KindleBridge development package.

set -eu

BASE=/mnt/us/kindlebridge
if test -x "$BASE/bin/usb-gadget-manager.sh"; then
    bridge_status=$("$BASE/bin/usb-gadget-manager.sh" status 2>/dev/null | sed -n '1p' || true)
    case "$bridge_status" in
        active|detached|acquiring-stock-usb|starting|stopping|stale)
            echo "Stop KindleBridge before uninstalling it (status: $bridge_status)" >&2
            return 1
            ;;
        *) rm -rf /var/local/kindlebridge/usb ;;
    esac
else
    rm -rf /var/local/kindlebridge/usb
fi
rm -rf /mnt/us/extensions/kindlebridge
rm -rf "$BASE"
rm -rf /var/local/kindlebridge
# Deliberately preserve /mnt/us/kindlebridge-data (developer payloads).
echo "KindleBridge development package removed"
return 0
