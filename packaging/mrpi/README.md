# Standalone MRPI development package

This package installs the persistent KindleBridge control plane under
`/var/local/kindlebridge/control` and its KUAL menu under
`/mnt/us/extensions/kindlebridge`. Runtime state and logs also live under
`/var/local/kindlebridge`. Keeping the launcher, A/B slots, manifests, PID files,
and heartbeat off the FSP/MTP userstore prevents `ESTALE` failures when USB is
connected. It makes no root filesystem changes and has no
runtime dependency on USBNetLite or KindleRoot.

Starting the bridge first asks stock `volumd` and HAL to release MTP using the
same `useUsbForNetwork`, `usbUnconfigured`, and `usbPlugOut` lifecycle used by
the firmware. Only after `g_ether` owns the unplugged controller does the
manager unload it, add the KindleBridge FunctionFS interface beside stock MTP,
launch `kindlebridged`, and bind the composite gadget. Stop removes only the
Bridge link/function, recreates the unplugged `g_ether` handoff state, and asks
`volumd` to reclaim MTP. It never binds stock MTP directly and never resets the
MTU3 controller.

Installation and upgrades atomically replace the program and KUAL files without
changing USB mode. If KindleBridge is active, the installer aborts before
replacement and asks the user to switch to USB file transfer first. After a
successful install, the user explicitly chooses **Switch to development mode**
when the Bridge is needed. This avoids silently taking USB ownership from stock
MTP or USBNetLite during package installation.

The installer manages `/var/local/kindlebridge/control` and accepts its v2
install transaction marker. No other program layout participates in install,
recovery, or removal.

KUAL keeps the menu open, uses explicit switch-to-development and
switch-to-file-transfer labels, and shows synchronous next steps in its message
area. The staged-update action is always visible because KUAL does not invalidate
its menu cache when an external runtime marker changes; selecting it without a
staged update displays an explanatory message. Start and stop are idempotent and
have no KUAL timeout. Bounded timeouts remain available only
for explicit laboratory manager calls. USB ownership transitions require the
cable to be unplugged; once active, the bridge supports normal host unplug/replug
without another mode transition. KindleBridge start fails closed if USBNetLite's
NCM or RNDIS function owns the gadget and explains how to hand USB back first.
Staged activation runs a read-only unplugged-cable preflight before showing
progress and repeats the check immediately before changing USB or daemon state.
Once the staged daemon sustains its startup heartbeat, the menu can restore the
previous confirmed daemon slot exactly once. Rollback also performs the cable
preflight twice, consumes the rollback point instead of toggling forward, and
cancels any stale staged pointer before development mode restarts. Staging a
new candidate invalidates an older rollback point because it overwrites that
inactive slot. Startup heartbeat health is not a substitute for host-side KBP
acceptance testing.
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
