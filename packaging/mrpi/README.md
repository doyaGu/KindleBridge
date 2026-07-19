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

Installation and upgrades use one user flow: unplug USB, run MRPI, and reconnect.
The installer stops an existing Bridge, atomically replaces the program and KUAL
files, starts the new Bridge, and rolls back to the previous version if startup
fails. A connected cable aborts before replacement with an actionable message.

KUAL keeps the menu open, uses explicit switch-to-development and
switch-to-file-transfer labels, and shows synchronous next steps in its message
area. The staged-update action appears only when an update exists. Start and stop
are idempotent and have no KUAL timeout. Bounded timeouts remain available only
for explicit laboratory manager calls. USB ownership transitions require the
cable to be unplugged; once active, the bridge supports normal host unplug/replug
without another mode transition.
The first development install may still be copied over a rescue network without
coupling KindleBridge to its provider.

The lifecycle is covered by `tests/usb-mode-lifecycle.sh`, including connected
fail-closed behavior, both stock entry states, rollback, stale-state cleanup,
and stock-MTP handback. The stock-MTP-to-Bridge path, discovery, repeated exec,
unplug/replug reconnects, and large sync have now been exercised on KT6
hardware. Bridge-to-MTP handback and re-entry have also completed repeatedly.
A long sleep exposed a heartbeat scheduling race now covered by a deterministic
launcher regression test. Repeated sleep/wake, crash recovery, and the full
repeated-cycle gate remain outstanding. Keep
`/mnt/us/KINDLEBRIDGE_DISABLE` for unattended startup until those gates pass.

This is an internal KT6 development package, not a public release.

`packaging/build-mrpi-dev.ps1` builds and invokes the workspace Rust
KindleTool checkout (`../KindleTool`) to create and sign the OTA2 package.
