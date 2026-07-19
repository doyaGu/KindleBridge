# KindleBridge

KindleBridge is an ADB-inspired, high-throughput development bridge for jailbroken Kindle devices.
The device link uses Kindle Bridge Protocol (KBP); deployable artifacts use the
KindleBridge Bundle (KBB) format. The first implementation target is `kindlehf`
on Kindle firmware 5.16.3 and later.

The current tree is an internal development candidate. It is not a public 1.0 release.
The MRPI development package installs only under `/mnt/us` and `/var/local`,
starts the USB bridge automatically, and returns ownership to stock MTP through
Kindle's `volumd`/HAL lifecycle. See
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
The old hardware-lab RNDIS recovery script is retired: it directly manipulated the
USB controller and is retained only as historical test evidence. The MRPI
manager is the supported development entry point. With the cable unplugged:

```sh
/mnt/us/kindlebridge/bin/usb-gadget-manager.sh start 0
```

Host commands then discover USB automatically:

```powershell
cargo build --package kindlebridge --package kindlebridge-server
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe device list
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe exec YOUR_KINDLE_SERIAL -- uname -a
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe shell YOUR_KINDLE_SERIAL
cargo run --package kindlebridge -- --server target/debug/kindlebridge-server.exe shell YOUR_KINDLE_SERIAL -c "uname -a"
```

The MRPI package installs the USB control plane and a small A/B daemon launcher.
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
healthy for three seconds. Three startup failures restore the previous slot
before USB is bound.

There is deliberately no live daemon activation or transport supervisor. The
daemon is the sole owner of the FunctionFS endpoints, so killing it destroys
the transport needed to repair it. The launcher, USB ownership manager, and
package layout therefore remain MRPI-managed control-plane components; normal
apps and payloads can still update online.

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

The build uses the workspace Rust KindleTool checkout at `../KindleTool`; it
does not build or invoke the legacy C/MinGW implementation.

Copy the generated install package from `dist/` to `/mnt/us/mrpackages`, unplug
USB, and run it through MRPI. The installer stops the previous Bridge, replaces
the package atomically, starts the new version, and restores the previous version
if activation fails. The normal user flow is therefore only **unplug, run MRPI,
reconnect**. If the cable is still attached, installation fails before replacing
the existing version and tells the user to unplug it.

The package does not install, invoke, or monitor USBNetLite. Its KUAL menu uses
task-oriented **Connect for development**, **Use USB file transfer**, and
**Status / Help** actions. Repeating either connection action is safe; transitions
have no KUAL time limit and remain in the menu. `/mnt/us/KINDLEBRIDGE_DISABLE` prevents
activation. Explicit manager invocations may still select a bounded timeout for
laboratory tests; `start 0` disables it. Stop always restores stock MTP; a
temporary `g_ether` rescue transport is not restored or required at runtime.
Start and stop require an unplugged USB cable on KT6. Once active, KindleBridge
supports normal host unplug/replug without another mode transition.

The ownership manager's stock-MTP-to-Bridge path, host discovery, repeated exec,
unplug/replug reconnects, and large sync have been exercised on the KT6 in
addition to deterministic offline lifecycle coverage. Bridge-to-MTP handback
and re-entry, sleep/wake, crash recovery, and the full repeated-cycle gate have
not yet completed; keep `/mnt/us/KINDLEBRIDGE_DISABLE` for unattended startup.

Portable formatting, Rust test/Clippy, shell lifecycle, and Windows-onboarding
selector checks run in GitHub Actions. The Windows host integration, ARM
`kindlehf` cross-build, packaging, and physical KT6 gates remain local because
the required toolchain and hardware are not available on hosted runners.

## KBB development bundles

```powershell
cargo run --package kindlebridge-bundle -- key init --output dev.key
cargo run --package kindlebridge-bundle -- build --manifest kindlebridge.toml --input app-root --signing-key dev.key --output app.kbb
cargo run --package kindlebridge-bundle -- inspect app.kbb
cargo run --package kindlebridge-bundle -- verify app.kbb --publisher dev.key.pub --target kindlehf
```

The builder refuses to overwrite keys or bundles. `kindlebridge.bundle.v1` accepts exactly one target, no dependencies or migrations, and one Ed25519 publisher signature.

## kindlehf device build on Windows

The build script discovers the workspace-local Kindle cross-toolchain unless `KINDLEHF_TOOLCHAIN_ROOT` is set:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/build-kindlehf.ps1
```

It builds all five device-side binaries, then rejects output that is not ELF32
ARM hard-float or requires a glibc newer than the configured firmware ceiling.
