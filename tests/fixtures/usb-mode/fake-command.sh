#!/bin/sh

set -eu

ROOT=${KINDLEBRIDGE_TEST_ROOT:?}
TRACE="$ROOT/trace"
MODULES="$ROOT/proc/modules"
MOUNTS="$ROOT/proc/mounts"
GADGET_UDC="$ROOT/sys/kernel/config/usb_gadget/mtpgadget/UDC"
NETWORK_MODE="$ROOT/lipc/volumd.useUsbForNetwork"
MTP_STARTED="$ROOT/lipc/mtp.isMtpStarted"
UDC_NAME=11211000.usb

record() {
    printf '%s\n' "$*" >>"$TRACE"
}

last_argument() {
    last=
    for argument in "$@"; do
        last=$argument
    done
    printf '%s\n' "$last"
}

load_g_ether() {
    if ! grep -q '^g_ether ' "$MODULES" 2>/dev/null; then
        printf '%s\n' 'g_ether 16384 0 - Live 0x00000000' >>"$MODULES"
    fi
}

unload_g_ether() {
    grep -v '^g_ether ' "$MODULES" >"$MODULES.next" || true
    mv "$MODULES.next" "$MODULES"
}

command_name=${0##*/}
case "$command_name" in
    id)
        test "${1:-}" = -u && { printf '%s\n' 0; exit 0; }
        exit 1
        ;;
    sleep)
        record "sleep $*"
        if test "${KINDLEBRIDGE_TEST_REAL_SLEEP:-0}" = 1; then
            # Keep lifecycle tests fast, but leave enough wall-clock time for
            # a background monitor to pass through nohup and exec on a busy
            # shared CI runner before its PID ownership is inspected.
            /usr/bin/sleep 0.2
        fi
        ;;
    lipc-get-prop)
        property=$(last_argument "$@")
        case "$property" in
            useUsbForNetwork) cat "$NETWORK_MODE" ;;
            isMtpStarted) cat "$MTP_STARTED" ;;
            *) exit 1 ;;
        esac
        ;;
    lipc-set-prop)
        value=$(last_argument "$@")
        record "lipc-set useUsbForNetwork $value"
        printf '%s\n' "$value" >"$NETWORK_MODE"
        ;;
    lipc-send-event)
        event=$(last_argument "$@")
        record "hal-event $event"
        if test "$event" = usbPlugOut; then
            if test "$(cat "$NETWORK_MODE")" = 1; then
                printf '%s\n' 0 >"$MTP_STARTED"
                printf '%s' '' >"$GADGET_UDC"
                load_g_ether
            else
                unload_g_ether
                printf '%s\n' 1 >"$MTP_STARTED"
                if test "${KINDLEBRIDGE_TEST_BIND_STOCK_MTP:-0}" = 1; then
                    printf '%s\n' "$UDC_NAME" >"$GADGET_UDC"
                else
                    printf '%s' '' >"$GADGET_UDC"
                fi
            fi
        fi
        ;;
    modprobe)
        record "modprobe $*"
        test "${1:-}" = g_ether || exit 1
        printf '%s' '' >"$GADGET_UDC"
        load_g_ether
        ;;
    rmmod)
        record "rmmod $*"
        test "${1:-}" = g_ether || exit 1
        unload_g_ether
        ;;
    ifconfig)
        record "ifconfig $*"
        ;;
    mount)
        record "mount $*"
        test "${KINDLEBRIDGE_TEST_MOUNT_FAIL:-0}" != 1 || exit 1
        target=$(last_argument "$@")
        mkdir -p "$target"
        : >"$target/ep0"
        : >"$target/ep1"
        : >"$target/ep2"
        printf 'functionfs %s functionfs rw 0 0\n' "$target" >>"$MOUNTS"
        ;;
    umount)
        record "umount $*"
        target=$(last_argument "$@")
        grep -v " $target " "$MOUNTS" >"$MOUNTS.next" || true
        mv "$MOUNTS.next" "$MOUNTS"
        rm -f "$target/ep0" "$target/ep1" "$target/ep2"
        ;;
    *)
        printf 'unexpected fake command: %s\n' "$command_name" >&2
        exit 127
        ;;
esac
