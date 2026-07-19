# KindleBridge implementation status

Status: **internal hardware-development baseline; not feature-complete; do not publish**.

The repository deliberately has no release artifact. A version number in a
Cargo package is a development compatibility marker, not a product release.

## Implemented and verified

- Rust workspace for the host, device, wire protocol, transport scheduler,
  bundle format, broker policy, and fake device.
- KBP1 40-byte framing, CRC32C, stream parity, session sequencing, credit
  windows, bounded traffic queues, and class scheduling.
- Content-Length framed local JSON-RPC with versioned methods, stable errors,
  device discovery, feature reporting, non-interactive exec, one-shot
  `shell -c`, and a line-oriented shell REPL. The REPL reuses one host/device
  session but is not a PTY yet.
- A real CLI-to-server-to-device path over persistent KBP/TCP sessions. It
  negotiates typed HELLO metadata, opens `shell.v1` streams, enforces connection
  and stream credit, executes argv without shell reparsing, propagates bounded
  timeout errors, and keeps the session usable across calls. The plain TCP
  entry point is development-only until pairing and encryption are connected.
- A real `sync.v1` path on the same persistent session. Public JSON-RPC carries
  validated absolute host paths rather than Base64 file content; KBP carries
  raw file bytes with 8 MiB stream / 16 MiB connection credit windows. Device
  paths are confined below `/mnt/us/kindlebridge-data`, uploads use same-volume
  staging plus atomic rename, transfer metadata survives daemon restart, and
  completed files receive an end-to-end BLAKE3 check. Tests cover a 9 MiB+
  push/pull (larger than the stream window) and a real TCP disconnect/reconnect
  resume. KT6 performance and power-loss validation remain release gates.
- Deterministic `kindlebridge.bundle.v1` KBB build, inspect, verify, Ed25519 signatures,
  BLAKE3 block hashes, zstd/none blocks, safe paths, and activation records.
- Device-side lifecycle state machines plus typed privileged broker grants.
  Non-interactive exec and sync are connected to the real device transport and
  operating-system/file APIs; app lifecycle, logs, and privileged operations
  remain library cores.
- A formal USB device link, not only a raw probe. The host automatically
  discovers and claims the exact `VID_1949:PID_9981` `ff/4b/01` interface,
  leaves MTP alone, performs the normal KBP HELLO, and exposes the same exec and
  sync services as TCP. Device `serve-usb` owns the FunctionFS endpoints and
  accepts reconnects while the gadget stays bound. Split-endpoint tests cover
  common framing and both halves of the KBP handshake. Current KT6 validation
  enumerated stock MTP plus the Bridge interface, kept the same daemon and
  launcher PIDs across repeated waits and independent CLI sessions, and passed
  1 MiB plus 128 MiB sync integrity checks. Formal one-direction sync measured
  11.31 MiB/s push and 17.30 MiB/s pull. Earlier raw-path validation reached
  14.99 MiB/s each way (29.97 MiB/s aggregate),
  but its direct-controller recovery path was later found unsafe and is not a
  supported lifecycle. The vendor kernel does not install the interface GUID,
  so the laboratory host currently needs one manual development registry value.
- A standalone, on-demand MRPI/KUAL development package exercised on the KT6.
  It installs only below `/mnt/us` and `/var/local`, has no runtime calls or
  dependency on USBNetLite or KindleRoot, and preserves stock MTP in the
  composite. The new manager uses stock `volumd`/HAL events for both handoff
  directions, never binds stock MTP directly, never resets MTU3, and refuses
  start or stop while the cable is connected.
  KUAL starts without a time limit or closing its menu; laboratory invocations
  may opt into a bounded watchdog. Sixteen deterministic shell lifecycle tests
  cover connected fail-closed behavior, MTP and existing stock-network entry,
  rollback, stale and detached state reporting, precise Bridge-only cleanup,
  stock handback, and KUAL behavior. The current stock-`volumd` manager has
  completed the stock-MTP-to-Bridge handoff on KT6; discovery, repeated root
  exec, multi-window stability, and large sync then passed. Hardware handback,
  re-entry, sleep/wake, and repeated-cycle validation remain gates.
- The package integrates a root-confined A/B daemon launcher into the USB
  manager. Host `daemon stage` uploads through `sync.v1`, verifies BLAKE3 plus
  the ELF32 little-endian ARM header on-device, writes only the inactive slot,
  and leaves the active USB session untouched. Activation is an explicit KUAL
  action with USB unplugged. The manager keeps the UDC unbound until the target
  has sustained its instance-bound readiness heartbeat for three seconds or
  the launcher has rolled back after three failed starts. Only then can USB be
  exposed to the host. Live replacement was removed: the daemon is the sole
  FunctionFS owner, so killing it also removes the transport needed for online
  recovery. The launcher distinguishes the initial-readiness deadline from the
  steady-state heartbeat deadline, tolerates transient heartbeat-file replacement
  gaps after readiness, and retries heartbeat writes instead of abandoning them
  after one filesystem error. Launcher, USB manager, and package-layout updates
  remain MRPI-only.
- `kindlehf` cross-builds that are ELF32 ARM hard-float and require at most
  GLIBC_2.18, below the KT6 firmware ceiling used by the build check.
- A one-shot KBP/TCP hardware probe ran successfully on the KT6, and bounded
  filesystem/hash/memory benchmarks established the initial hardware data-path baseline.
  See `docs/hardware-lab/kt6-5.17.1.0.4.md`.
- Workspace unit/integration tests, formatting, and Clippy with warnings denied.
  The current workspace has 151 passing Rust unit/integration tests plus 16
  passing shell USB-lifecycle tests.

## Required before an internal feature-complete candidate

- KT6 validation of stock-`volumd` Bridge-to-MTP handback and re-entry, followed
  by unplug/replug, sleep/wake, crashes, concurrency, and stock-MTP recovery tests.
  Automatic Windows interface onboarding remains required.
- Wi-Fi/TCP discovery, authenticated pairing, session encryption, automatic
  transport reconnect, and measured throughput/latency under concurrent streams.
- A narrowly scoped, locally authenticated root broker IPC implementation.
- End-to-end interactive shell/PTY, root exec grants, sync progress/cancellation
  and directory semantics, app install and rollback, `run --watch`, logs/events,
  process control, forward/reverse, GDB,
  core dumps, basic perf, screenshot, and bugreport services.
- KT6 fault-injection validation of offline daemon A/B activation and automatic
  pre-bind rollback. Safe-mode, complete uninstall, and stock USB recovery
  remain release gates. No rootfs writes.
- GTK2 Control Center. UI automation remains explicitly deferred.
- The complete CLI/RPC error, progress, cancellation, JSON, and NDJSON contract
  with fake-device coverage for every advertised command.
- KT6 hardware validation, including USB identity, startup path, sleep/wake,
  unplug/replug, storage exhaustion, low memory, crash points, large files, and
  concurrent-service soak tests.

## Publication rule

Do not create a public tag, release artifact, or supported-device claim
until every required item above has an automated fake-device test where
applicable, a successful KT6 path where hardware is involved, and the full
release checklist in `../KindleBridge-Implementation-Plan.md` passes. Partial
functionality remains internal even when its individual tests pass.
