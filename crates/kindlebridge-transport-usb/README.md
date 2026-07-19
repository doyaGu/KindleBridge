# kindlebridge-transport-usb

Blocking USB transport for the KindleBridge KBP byte stream.

The transport identifies a device by VID/PID, an optional exact serial number,
and an exact vendor-interface class/subclass/protocol triple. It opens and
claims only that interface number. It never detaches a driver, changes the
device configuration, claims a composite parent, or claims an MTP interface.

## Windows driver requirement

On Windows, **only the KindleBridge vendor interface** must be associated with
WinUSB. Production firmware should publish a Microsoft OS/WCID descriptor that
binds WinUSB to that interface automatically. Do not replace the driver for the
composite parent and do not replace the MTP interface driver. A manual driver
tool is suitable only for development and must target the KindleBridge
interface child explicitly.

The default queue contains sixteen 64 KiB transfers in each direction. All queue
and buffer sizes are validated and bounded before USB endpoints are opened.
