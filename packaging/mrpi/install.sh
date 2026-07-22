#!/bin/sh
# KindleBridge development installer. Userstore/varlocal only.

set -eu

MNT_US_ROOT=${KINDLEBRIDGE_MNT_US_ROOT:-/mnt/us}
VAR_LOCAL_ROOT=${KINDLEBRIDGE_VAR_LOCAL_ROOT:-/var/local}
SYS_ROOT=${KINDLEBRIDGE_SYS_ROOT:-/sys}
PROC_ROOT=${KINDLEBRIDGE_PROC_ROOT:-/proc}
CONTROL_ROOT="$VAR_LOCAL_ROOT/kindlebridge"
BASE="$CONTROL_ROOT/control"
EXT="$MNT_US_ROOT/extensions/kindlebridge"
STAGE_BASE="$CONTROL_ROOT/.control-install.$$"
STAGE_EXT="$MNT_US_ROOT/.kindlebridge-extension.$$"
OLD_BASE="$CONTROL_ROOT/.control-previous"
OLD_EXT="$MNT_US_ROOT/.kindlebridge-extension-previous"
TRANSACTION="$CONTROL_ROOT/.install-transaction"
TRANSACTION_TEMP="$CONTROL_ROOT/.install-transaction.$$"
PAYLOAD_ROOT="$MNT_US_ROOT/.kindlebridge-payload.$$"
PAYLOAD_ARCHIVE=${KINDLEBRIDGE_PAYLOAD_ARCHIVE:-payload.tar}
COMMITTED=0
TRANSACTION_ACTIVE=0
HAD_BASE=0
HAD_EXT=0

durability_barrier() {
    if test "${KINDLEBRIDGE_TEST_SKIP_SYNC:-0}" = 1; then
        return 0
    fi
    sync
}

cleanup() {
    rm -rf "$STAGE_BASE" "$STAGE_EXT" "$PAYLOAD_ROOT" "$TRANSACTION_TEMP"
    if test "$COMMITTED" = 1; then
        rm -rf "$OLD_BASE" "$OLD_EXT"
        durability_barrier >/dev/null 2>&1 || true
        rm -f "$TRANSACTION"
    elif test "$TRANSACTION_ACTIVE" = 1; then
        recover_interrupted_install >/dev/null 2>&1 || true
    fi
}

transaction_value() {
    key=$1
    sed -n "s/^$key=//p" "$TRANSACTION" 2>/dev/null | sed -n '1p'
}

write_transaction() {
    state=$1
    {
        echo KINDLEBRIDGE_INSTALL_TRANSACTION_V2
        echo "state=$state"
        echo "had_base=$HAD_BASE"
        echo "had_ext=$HAD_EXT"
    } >"$TRANSACTION_TEMP" || return 1
    mv "$TRANSACTION_TEMP" "$TRANSACTION"
}

restore_orphaned_backups() {
    test -d "$OLD_BASE" || test -d "$OLD_EXT" || return 0
    # A backup without its marker can only be an interrupted replacement.
    # Prefer the known previous install over a possibly partial new tree.
    rm -rf "$BASE" "$EXT"
    if test -d "$OLD_BASE"; then
        mv "$OLD_BASE" "$BASE" || return 1
    fi
    if test -d "$OLD_EXT"; then
        mkdir -p "$(dirname "$EXT")"
        mv "$OLD_EXT" "$EXT" || return 1
    fi
}

recover_interrupted_install() {
    if ! test -f "$TRANSACTION"; then
        restore_orphaned_backups
        return
    fi
    if test "$(sed -n '1p' "$TRANSACTION" 2>/dev/null)" != KINDLEBRIDGE_INSTALL_TRANSACTION_V2; then
        echo "KindleBridge found an invalid install transaction." >&2
        return 1
    fi
    state=$(transaction_value state)
    had_base=$(transaction_value had_base)
    had_ext=$(transaction_value had_ext)
    case "$state:$had_base:$had_ext" in
        prepared:[01]:[01]|committed:[01]:[01]) ;;
        *)
            echo "KindleBridge found an incomplete install transaction." >&2
            return 1
            ;;
    esac
    if test "$state" = committed; then
        rm -rf "$OLD_BASE" "$OLD_EXT"
        durability_barrier || return 1
        rm -f "$TRANSACTION"
        return 0
    fi
    if test "$had_base" = 1; then
        if test -d "$OLD_BASE"; then
            rm -rf "$BASE"
            mv "$OLD_BASE" "$BASE" || return 1
        fi
    else
        rm -rf "$BASE"
    fi
    if test "$had_ext" = 1; then
        if test -d "$OLD_EXT"; then
            rm -rf "$EXT"
            mkdir -p "$(dirname "$EXT")"
            mv "$OLD_EXT" "$EXT" || return 1
        fi
    else
        rm -rf "$EXT"
    fi
    rm -rf "$OLD_BASE" "$OLD_EXT"
    durability_barrier || return 1
    rm -f "$TRANSACTION"
}

replace_install() {
    write_transaction prepared || return 1
    TRANSACTION_ACTIVE=1
    # The prepared marker must reach storage before any previous tree moves.
    # That ordering makes the fixed backups recoverable after sudden power loss.
    durability_barrier || return 1
    if test "$HAD_BASE" = 1; then
        mv "$BASE" "$OLD_BASE" || return 1
    fi
    if test "$HAD_EXT" = 1; then
        mv "$EXT" "$OLD_EXT" || return 1
    fi
    mv "$STAGE_BASE" "$BASE" || return 1
    mv "$STAGE_EXT" "$EXT" || return 1
    write_transaction committed || return 1
    # Persist the commit decision before cleanup can remove its rollback trees.
    durability_barrier || return 1
}

pid_file_is_live() {
    pid=$(cat "$1" 2>/dev/null || true)
    case "$pid" in
        ''|*[!0-9]*) return 1 ;;
    esac
    test -d "$PROC_ROOT/$pid"
}

unmanaged_bridge_evidence() {
    test -d "$VAR_LOCAL_ROOT/kindlebridge/usb" ||
        test -e "$SYS_ROOT/kernel/config/usb_gadget/mtpgadget/configs/c.1/ffs.kbp" ||
        test -L "$SYS_ROOT/kernel/config/usb_gadget/mtpgadget/configs/c.1/ffs.kbp" ||
        pid_file_is_live "$BASE/runtime/run/daemon.pid"
}

trap cleanup EXIT
trap 'exit 1' HUP INT TERM

mkdir -p "$CONTROL_ROOT" "$MNT_US_ROOT/extensions"
if ! recover_interrupted_install; then
    echo "The previous KindleBridge install transaction needs manual recovery." >&2
    return 1
fi
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

# Installation never changes USB ownership. An active manager must be returned
# to stock file transfer explicitly before its executable can be replaced.
OLD_MANAGER=
if test -x "$BASE/bin/usb-gadget-manager.sh"; then
    OLD_MANAGER="$BASE/bin/usb-gadget-manager.sh"
fi
if test -n "$OLD_MANAGER"; then
    bridge_status=$("$OLD_MANAGER" status 2>/dev/null | sed -n '1p' || true)
    case "$bridge_status" in
        active|recovering|degraded|detached|acquiring-stock-usb|starting|stopping|stale|stale-from-previous-boot)
            echo "KindleBridge is currently active ($bridge_status)." >&2
            echo "In KUAL, choose 'Switch to USB file transfer', then run MRPI again." >&2
            echo "Installation did not change the current USB mode." >&2
            return 1
            ;;
        inactive)
            rm -rf "$VAR_LOCAL_ROOT/kindlebridge/usb"
            ;;
        *)
            echo "KindleBridge has an unknown USB state: $bridge_status" >&2
            echo "Unplug USB, open KindleBridge status and recovery steps, then retry." >&2
            return 1
            ;;
    esac
else
    if unmanaged_bridge_evidence; then
        echo "KindleBridge runtime state exists but its manager is missing." >&2
        echo "Installation did not replace files or change USB mode." >&2
        echo "Restore the matching manager and use recovery before retrying." >&2
        return 1
    fi
fi

mkdir -p "$STAGE_BASE/bin" "$STAGE_EXT" "$MNT_US_ROOT/extensions" "$CONTROL_ROOT"
cp -af "$PAYLOAD_ROOT/kindlebridge/." "$STAGE_BASE/"
cp -af "$PAYLOAD_ROOT/extensions/kindlebridge/." "$STAGE_EXT/"
chmod 0755 "$STAGE_BASE/bin/kindlebridge-launcher" \
    "$STAGE_BASE/bin/usb-gadget-manager.sh" \
    "$STAGE_BASE/runtime/slots/A/bin/kindlebridged" \
    "$STAGE_BASE/runtime/slots/B/bin/kindlebridged" \
    "$STAGE_EXT/bin/kindlebridge.sh"

if test -d "$BASE"; then
    HAD_BASE=1
fi
if test -d "$EXT"; then
    HAD_EXT=1
fi
if ! replace_install; then
    recover_interrupted_install || true
    echo "KindleBridge update could not replace its files; the previous version was restored." >&2
    return 1
fi
COMMITTED=1

if test -e "$MNT_US_ROOT/KINDLEBRIDGE_DISABLE"; then
    echo "KindleBridge installed but disabled by KINDLEBRIDGE_DISABLE."
    echo "Remove that file, then choose 'Switch to development mode' in KUAL."
    return 0
fi

echo "KindleBridge $PACKAGE_VERSION installed but inactive."
echo "USB mode was not changed."
echo "When ready, choose 'Switch to development mode' in KUAL."
return 0
