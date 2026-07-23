# KindleBridge

KindleBridge is an ADB-inspired, high-throughput development bridge for jailbroken Kindle devices.
The device link uses Kindle Bridge Protocol (KBP); deployable artifacts use the
KindleBridge Bundle (KBB) format. The first implementation target is `kindlehf`
on Kindle firmware 5.16.3 and later.

The current tree is an internal development candidate. It is not a public 1.0 release.
The MRPI development package installs only under `/mnt/us` and `/var/local`,
leaves the USB bridge inactive until the user explicitly enables development
mode in KUAL, and returns ownership to stock MTP through Kindle's `volumd`/HAL
lifecycle. See
[`STATUS.md`](STATUS.md) for the release gates and current gaps.

## Workspace

- `crates/kindlebridge-wire`: KBP framing and connection state.
- `crates/kindlebridge-schema`: host API and device-protocol payloads.
- `crates/kindlebridge-transport`: bounded scheduling and transport selection.
- `crates/kindlebridge-transport-{tcp,usb}`: TCP framing and WinUSB/libusb byte streams.
- `crates/kindlebridge-bundle`: signed incremental bundle format and activation transactions.
- `host/`: host server and CLI.
- `device/`: unprivileged daemon, privileged broker, A/B launcher, and FunctionFS support.
- `tests/fake-device`: end-to-end fake device.
- `tools/kindlebridge-tcp-probe`: direct-TCP hardware probe.
- `tools/kindlebridge-usb-bench`: raw-USB throughput benchmark.

The current KT6 hardware-lab evidence is recorded in
[`docs/hardware-lab/kt6-5.17.1.0.4.md`](docs/hardware-lab/kt6-5.17.1.0.4.md).
Repository naming rules are recorded in [`docs/naming.md`](docs/naming.md).

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The repository also includes a deterministic fake-device process, so host RPC and CLI work can be tested without touching a Kindle:

```powershell
cargo build --package kindlebridge-fake-device
cargo run --package kindlebridge -- --server target/debug/kindlebridge-fake-device --json device list
cargo run --package kindlebridge -- --server target/debug/kindlebridge-fake-device exec KT6-FAKE-0001 -- echo hello kindle
```

The real development device path uses the same CLI and host server. Start the
cross-built daemon manually on the Kindle, then point the CLI at it:

```sh
/mnt/us/kindlebridged serve-tcp \
  --listen 0.0.0.0:4765 \
  --serial YOUR_KINDLE_SERIAL \
  --allow-peer 192.168.15.1 \
  --sync-root /mnt/us/kindlebridge-data
```

```powershell
cargo build --package kindlebridge --package kindlebridge-server
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe --tcp-device 192.168.15.244:4765 device list

cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe --tcp-device 192.168.15.244:4765 exec YOUR_KINDLE_SERIAL -- uname -a

cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe --tcp-device 192.168.15.244:4765 sync push YOUR_KINDLE_SERIAL C:\work\app.bin apps/demo/app.bin
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe --tcp-device 192.168.15.244:4765 sync pull YOUR_KINDLE_SERIAL apps/demo/app.bin C:\work\app-from-kindle.bin
```

Sync local paths are absolute host paths. Device paths are relative logical
paths below the configured sync root; absolute paths and traversal are rejected.
The local JSON-RPC connection carries only paths and transfer metadata. File
content travels as raw KBP `DATA` frames with 8 MiB stream / 16 MiB connection
windows, persistent resume metadata, and an end-to-end BLAKE3 check.

This explicit TCP option is an internal bring-up path. It is unencrypted and
must not be exposed beyond a trusted development link. The production profile
still requires authenticated pairing and session encryption.

The primary USB path uses that same KBP session, exec, and sync implementation.
The archived hardware-lab RNDIS recovery script directly manipulated the USB
controller and is retained only as historical test evidence. The MRPI
manager is the supported development entry point. With the cable unplugged:

```sh
/var/local/kindlebridge/control/bin/usb-gadget-manager.sh start 0
```

Host commands then discover USB automatically:

```powershell
cargo build --package kindlebridge --package kindlebridge-server
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe device list
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe device ping YOUR_KINDLE_SERIAL
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe exec YOUR_KINDLE_SERIAL -- uname -a
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe shell YOUR_KINDLE_SERIAL
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe shell YOUR_KINDLE_SERIAL -c "uname -a"
```

The CLI starts one current-user host service on demand and all later CLI
processes share its USB connection. On Windows it listens on a named pipe; on
Linux it uses a `0600` Unix socket. It exits after ten idle minutes. Inspect or
stop it explicitly with `kindlebridge server status` and
`kindlebridge server stop`; `kindlebridge-server --stdio` remains available for
tests and IDE integrations. `device ping SERIAL` performs a bounded KBP
`PING`/`PONG` round trip through the selected Kindle; unlike `server ping`, it
tests the real device transport.

`shell` opens a persistent root terminal in `/tmp/root` with `TERM=linux`.
Interactive stdin selects a PTY automatically; redirected stdin selects the raw
binary stream. Use `-t`/`-tt` to request/force a PTY, `-T` to disable one, `-n`
to close stdin immediately, and `-e none` to disable the default line-leading
`~.` local escape. `shell -c COMMAND` streams without the structured `exec`
output limit and returns the remote exit status; use `exec` when stdout/stderr
capture or `--json` is required, and `shell --ndjson` for stream events. Devices
without `shell.v2` are rejected as incompatible. Internal builds require a
matching CLI, host server, and device daemon; there is no line-REPL or
`exec.v1` shell fallback.

The MRPI package installs the USB control plane and a small A/B daemon launcher.
Persistent executables, manifests, PID files, and heartbeats live under
`/var/local/kindlebridge/control`, not the FSP/MTP-backed `/mnt/us` tree. The
userstore retains only the KUAL entry point and developer-visible data.
The active Bridge may upload and verify a cross-built `kindlebridged`, but it
never replaces the daemon that owns its current USB transport:

```powershell
cargo run --release --package kindlebridge -- `
  --server target/release/kindlebridge-server.exe `
  daemon stage YOUR_KINDLE_SERIAL `
  C:\absolute\path\to\kindlebridged
```

`daemon stage` sends the binary through resumable `sync.v1`, verifies its
BLAKE3 digest and ELF32 little-endian ARM identity on the Kindle, writes only
the inactive A/B slot, and records it as staged. The active slot and USB session
remain untouched. Then unplug USB and choose **Apply staged daemon update** in
KUAL. The USB manager starts the requested slot while the UDC is unbound and
does not expose it to the host until its instance-bound heartbeat has remained
healthy for ten seconds. Three startup failures restore the previous slot
before USB is bound.

After a staged slot reaches that startup-heartbeat threshold, KUAL exposes a
one-shot **Roll back daemon update** action. It also requires USB to be
unplugged, stops the active daemon, restores the last confirmed slot, and then
starts development mode again. Starting another stage invalidates the older
rollback point because the inactive slot is about to be overwritten. The
heartbeat threshold verifies process startup and liveness; it does not claim
that a host has completed a KBP command, so development builds should still be
accepted from the host after activation.

There is deliberately no host-triggered live daemon activation. The launcher
may restart the currently selected daemon after a sustained liveness failure,
but changing A/B slots remains an unplugged, pre-bind operation. The daemon is
the sole owner of the FunctionFS endpoints, so an online slot replacement would
destroy the transport needed to repair it. The launcher, USB ownership manager,
and package layout therefore remain MRPI-managed control-plane components;
normal apps and payloads can still update online.

A daemon process restart is not reported as transparent USB recovery. Closing
all FunctionFS endpoints disables the function and unregisters the gadget; on
the KT6, reopening them after a process restart does not reliably reattach the
still-connected host. The USB manager therefore leaves the UDC, HAL, and driver
untouched, marks the mode `degraded`, and asks for an unplugged switch through
USB file transfer back to development mode. Reliable transparent daemon
replacement would require a separate long-lived USB frontend that owns and
services every FunctionFS endpoint while backends change behind IPC. See the
[Linux FunctionFS documentation](https://docs.kernel.org/usb/functionfs.html)
and [AOSP's single-process FunctionFS owner](https://android.googlesource.com/platform/system/core/+/c745f09b33804242c43f823242f0112645ed3a98/adb/daemon/usb_ffs.cpp).

Use `--usb-serial` only to select among multiple attached Kindles and
`--no-usb` to disable automatic discovery. Unplug the cable again before
running `usb-gadget-manager.sh stop`. The manager asks stock `volumd` to perform the
MTP-to-network and network-to-MTP transitions; it never resets the MTU3
controller or binds stock MTP directly. The current KT6 firmware does not
install the FunctionFS interface GUID automatically. On Windows, first inspect
and then install the repository's fail-closed `MI_01` onboarding change:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install-windows-winusb.ps1 -DryRun
# Run the following command from an elevated PowerShell window:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install-windows-winusb.ps1
```

This keeps the stock VID/PID and MTP driver and uses Windows' inbox WinUSB
service; it does not require pid.codes or a third-party driver. See
[`docs/windows-winusb-onboarding.md`](docs/windows-winusb-onboarding.md).

## Internal MRPI development package

Build the standalone KT6 package on Windows with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File packaging/build-mrpi-dev.ps1
```

The build uses the workspace Rust KindleTool checkout at `../KindleTool`.

Copy the generated install package from `dist/` to `/mnt/us/mrpackages`, unplug
USB, and run it through MRPI. Installation and upgrades replace the package
atomically but deliberately leave USB mode unchanged. If KindleBridge is active,
the installer refuses to replace it and asks the user to choose **Switch to USB
file transfer** first. After installation, choose **Switch to development mode**
in KUAL when the Bridge is actually needed.

The package does not install, invoke, or monitor USBNetLite. Its KUAL menu uses
task-oriented **Switch to development mode**, **Switch to USB file transfer**,
and **Show status and recovery steps** actions. The staged-update action remains
visible because KUAL does not invalidate its menu cache when the runtime marker
changes; selecting it without an update explains what to do. Repeating either
mode action is safe; transitions have no KUAL time limit and remain in the menu.
Before staged activation shows progress, KUAL performs a read-only cable check;
the manager repeats that check at the mutation boundary to reject a late replug.
`/mnt/us/KINDLEBRIDGE_DISABLE`
prevents activation. KindleBridge also refuses to start while USBNetLite owns
the USB gadget, with an instruction to turn USBNetwork off first. Explicit
manager invocations may still select a bounded
timeout for laboratory tests; `start 0` disables it. Stop always restores stock
MTP; a temporary `g_ether` rescue transport is not restored or required at runtime.
Start and stop require an unplugged USB cable on KT6. Once active, KindleBridge
supports normal host unplug/replug without another mode transition.

The ownership manager's stock-MTP-to-Bridge path, Bridge-to-MTP handback,
re-entry, host discovery, repeated exec, unplug/replug reconnects, and large
sync have been exercised repeatedly on the KT6. A long sleep exposed a wall-clock
heartbeat race; the launcher now gives an already-healthy daemon one normal
heartbeat window after a watchdog scheduling discontinuity. Repeated sleep/wake,
crash recovery, and the full soak gate still require hardware revalidation; keep
`/mnt/us/KINDLEBRIDGE_DISABLE` for unattended startup.

Portable formatting, Rust test/Clippy, shell lifecycle, and Windows-onboarding
selector checks run in GitHub Actions. The Windows host integration, ARM
`kindlehf` cross-build, packaging, and physical KT6 gates remain local because
the required toolchain and hardware are not available on hosted runners.

## KBB development bundles

An application project can keep its build loop in the same signed KBB
manifest:

```toml
[development]
build = ["meson", "compile", "-C", "build-kindlehf"]
input = "build-kindlehf/bundle-root"
signing_key = ".kindlebridge/dev.key"
watch = ["src", "meson.build", "kindlebridge.toml"]
```

The command is an argument array and is executed directly from the manifest
directory, without host-shell reparsing. The other paths are also relative to
that directory. `build` may be omitted for scripts or already-built input
trees. A one-shot run builds, creates a verified KBB, installs it
atomically, and starts the declared application:

```powershell
kindlebridge run YOUR_KINDLE_SERIAL
kindlebridge run YOUR_KINDLE_SERIAL --watch
```

The checked-in `examples/hello` project can be run without a compiler:

```powershell
kindlebridge-bundle key init --output examples/hello/.kindlebridge/dev.key
kindlebridge run YOUR_KINDLE_SERIAL --manifest examples/hello/kindlebridge.toml
```

Watch mode debounces changes to the explicitly listed paths and reconnects to
the shared host service for every deployment. Build or bundle failures occur
before any device mutation, so the previous application remains active.
Generated development bundles live below `.kindlebridge/` and receive a
monotonic development release without rewriting the source manifest. Build
cancellation and merged live application logs are the next development-loop
increments.

```powershell
cargo run --package kindlebridge-bundle -- key init --output dev.key
cargo run --package kindlebridge-bundle -- build --manifest kindlebridge.toml --input app-root --signing-key dev.key --output app.kbb
cargo run --package kindlebridge-bundle -- inspect app.kbb
cargo run --package kindlebridge-bundle -- verify app.kbb --publisher dev.key.pub --target kindlehf
cargo run --package kindlebridge -- app install YOUR_KINDLE_SERIAL .\app.kbb
cargo run --package kindlebridge -- app list YOUR_KINDLE_SERIAL
cargo run --package kindlebridge -- app start YOUR_KINDLE_SERIAL org.example.app
cargo run --package kindlebridge -- app restart YOUR_KINDLE_SERIAL org.example.app
cargo run --package kindlebridge -- app stop YOUR_KINDLE_SERIAL org.example.app
cargo run --package kindlebridge -- app rollback YOUR_KINDLE_SERIAL org.example.app
cargo run --package kindlebridge -- app uninstall YOUR_KINDLE_SERIAL org.example.app
```

The builder refuses to overwrite keys or bundles. `kindlebridge.bundle.v1`
accepts exactly one target, no dependencies or migrations, and one Ed25519
publisher signature. `app install` resolves the local path before contacting
the shared host server, verifies the KBB before spending USB bandwidth, uploads
it through resumable `sync.v1`, then has the Kindle independently verify the
signature, target, firmware range, required features, file checksum, and every
raw block. The device stores blocks by BLAKE3 address and changes the active
application inventory through a journaled, atomic activation generation.
Before activation, it reconstructs a content-addressed, read-only runtime image
and records a bounded runtime manifest containing the signed entrypoint digest
and process policy. `app start`, `app stop`, and `app restart` operate on a real
process group; list reports the observed PID and reaps exited applications.
Stop sends TERM, waits the bundle's `stop_timeout_ms`, then kills the process
group. A parent-death-aware internal runner also cleans the group if the daemon
is terminated. App stdout/stderr inherit the daemon log, so they are visible to
the current log snapshot command. Installing the identical bundle again is
idempotent and verifies that its runtime image is present. Missing or corrupt
runtime images are hard errors, not compatibility states. `app rollback`
returns to the previous distinct signed application
generation without reverting unrelated applications; a repeated rollback does
not toggle forward. `app uninstall` removes the active application while
preserving its data and immutable history. `restart = "on-failure"` permits at
most three retries after the initial start, with bounded backoff; exhaustion is
reported as `failed`, and an explicit start or stop clears that terminal state.
All spawns are owned by one daemon-lifetime supervisor thread so Linux
parent-death cleanup cannot be triggered by a short-lived RPC worker exiting.
Development bundles may currently use any internally valid publisher key;
publisher allowlists arrive with pairing/grants and are still a publication
gate.

KBP protocol revision 3 uses `rpc.v1` for generic request/reply traffic and
requires `shell.v2`. The application store accepts activation schema 3 under
`/var/local/kindlebridge/apps`. The CLI, host server, daemon, schema, and package
are developed and deployed as one matched build.

## kindlehf device build on Windows

The build script discovers the workspace-local Kindle cross-toolchain unless `KINDLEHF_TOOLCHAIN_ROOT` is set:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/build-kindlehf.ps1
```

It builds all five device-side binaries, then rejects output that is not ELF32
ARM hard-float or requires a glibc newer than the configured firmware ceiling.
