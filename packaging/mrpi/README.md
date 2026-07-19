# Standalone MRPI development package

This package installs KindleBridge under `/mnt/us/kindlebridge` and its KUAL
menu under `/mnt/us/extensions/kindlebridge`. Runtime state and logs live under
`/var/local/kindlebridge`. It makes no root filesystem changes and has no
runtime dependency on USBNetLite or KindleRoot.

Starting the bridge first asks stock `volumd` and HAL to release MTP using the
same `useUsbForNetwork`, `usbUnconfigured`, and `usbPlugOut` lifecycle used by
the firmware. Only after `g_ether` owns the unplugged controller does the
manager unload it, add the KindleBridge FunctionFS interface beside stock MTP,
launch `kindlebridged`, and bind the composite gadget. Stop removes only the
Bridge link/function, recreates the unplugged `g_ether` handoff state, and asks
`volumd` to reclaim MTP. It never binds stock MTP directly and never resets the
MTU3 controller.

KUAL keeps the menu open for start, stop, and status, shows synchronous feedback
in its message area, and starts the bridge without a time limit. Bounded
timeouts remain available only for explicit laboratory manager calls.
Start and stop require the USB cable to be unplugged; once active, the bridge
supports normal host unplug/replug without another mode transition.
The first development install may still be copied over a rescue network without
coupling KindleBridge to its provider.

The lifecycle is covered by `tests/usb-mode-lifecycle.sh`, including connected
fail-closed behavior, both stock entry states, rollback, stale-state cleanup,
and stock-MTP handback. The current rewrite is not yet revalidated on KT6
hardware; keep `/mnt/us/KINDLEBRIDGE_DISABLE` in place until that test is
started deliberately.

This is an internal KT6 development package, not a public release.

`packaging/build-mrpi-dev.ps1` builds and invokes the workspace Rust
KindleTool checkout (`../KindleTool`) to create and sign the OTA2 package.
