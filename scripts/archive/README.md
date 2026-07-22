# Retired device-side laboratory script

`unsafe-kt6-usb-lab.sh` is retained only so the original KT6 experiments can be
audited. Its direct configfs, `soft_connect`, and MTU3 controller-rebind recovery
caused later Code 43 and incomplete MTP recovery. It is not an installation,
development, or recovery entry point.

Use the MRPI package manager instead. It requires an unplugged cable and hands
USB ownership to and from stock `volumd`/HAL:

```sh
/var/local/kindlebridge/control/bin/usb-gadget-manager.sh start 0
/var/local/kindlebridge/control/bin/usb-gadget-manager.sh status
/var/local/kindlebridge/control/bin/usb-gadget-manager.sh stop
```

The legacy script refuses mutating commands unless a developer explicitly sets
`KINDLEBRIDGE_ALLOW_UNSAFE_USB_LAB=1`. That override is for historical
reproduction only and should not be used on a normal device.
