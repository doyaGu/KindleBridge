# KindleBridge FunctionFS support

The library supplies FunctionFS endpoints for the production daemon. The
`kindlebridge-ffs-probe` binary is a one-shot data-plane diagnostic. Neither
component mounts FunctionFS, modifies configfs, selects a UDC, or provides TLS.

Before starting it, an external gadget owner must:

1. mount a FunctionFS instance and make its directory available (the default is
   `/dev/usb-ffs/kbp`);
2. create/link the FunctionFS function into a gadget configuration;
3. configure the composite gadget's Microsoft OS 1.0 string (`MSFT100`) and
   vendor code if WINUSB discovery is required;
4. bind the intended UDC and leave ownership of `ep0`, `ep1`, and `ep2` to the
   probe process.

The probe writes the FunctionFS v2 descriptor and string blocks, waits for
`FUNCTIONFS_ENABLE`, then treats `ep1` as bulk OUT and `ep2` as bulk IN. The
FunctionFS descriptor block supplies the WINUSB extended compatible ID; the
special index-`0xEE` Microsoft OS string belongs to the surrounding composite
gadget and cannot be supplied by the FunctionFS language string block.

Run it as:

```text
kindlebridge-ffs-probe [FUNCTIONFS_DIRECTORY]
```

The KT6 5.17.1.0.4 vendor kernel source currently hard-codes
`skip_os_desc = 1` in `f_fs.c`, despite advertising the OS-descriptor ABI in
`functionfs.h`. Actual WINUSB enumeration therefore remains a device test gate.
