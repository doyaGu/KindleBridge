# KindleBridge implementation status

Status: **internal hardware-development baseline; not feature-complete; do not publish**.

The repository deliberately has no release artifact. A version number in a
Cargo package is an internal build identifier, not a product release.

## Implemented and verified

- Rust workspace for the host, device, wire protocol, transport scheduler,
  bundle format, broker policy, and fake device.
- KBP1 40-byte framing, CRC32C, stream parity, session sequencing, credit
  windows, bounded traffic queues, and class scheduling.
- A current-user shared local server (Windows named pipe / Linux `0600` Unix
  socket) with Content-Length framed JSON-RPC, versioned methods, stable errors,
  automatic CLI startup, idle shutdown, and `server status` / `server stop`.
  `shell.v2` provides persistent PTY and raw streams, binary stdin/stdout/stderr,
  resize, close-input, exit/signal reporting, and bounded end-to-end credit.
- A real CLI-to-server-to-device path over actor-owned persistent KBP/TCP and
  USB sessions. It negotiates typed HELLO metadata, carries generic requests on
  `rpc.v1`, requires `shell.v2`, enforces connection and
  stream credit, executes argv without shell reparsing, propagates bounded
  timeout errors, and keeps the session usable across calls. USB providers
  lazily reconnect after unplug, sleep, or daemon restart. The plain TCP
  entry point is development-only until pairing and encryption are connected.
- A real `sync.v1` path on the same persistent session. Public JSON-RPC carries
  validated absolute host paths rather than Base64 file content; KBP carries
  raw file bytes with 8 MiB stream / 16 MiB connection credit windows. Device
  paths are confined to the user-visible `kindlebridge-data` tree, uploads use
  same-volume staging plus atomic rename, transfer metadata survives daemon
  restart, and completed files receive an end-to-end BLAKE3 check. On firmware
  where `/mnt/us` is positively identified as the `fsp` FUSE view of the
  mounted `/mnt/base-us` userstore, the USB manager gives the daemon that direct
  backing path; files remain visible through `/mnt/us`. Unknown layouts fail
  back to `/mnt/us/kindlebridge-data`. Tests cover a 9 MiB+ push/pull (larger
  than the stream window) and a real TCP disconnect/reconnect resume. KT6
  power-loss validation remains a release gate.
- Deterministic `kindlebridge.bundle.v1` KBB build, inspect, verify, Ed25519 signatures,
  BLAKE3 block hashes, zstd/none blocks, safe paths, and activation records.
  `app install SERIAL LOCAL_BUNDLE.kbb` now performs host fail-fast verification,
  automatic resumable upload, independent device verification of target,
  firmware and required features, content-addressed block ingestion, and a
  journaled atomic activation commit. Package identity/version come only from
  signed KBB metadata, duplicate installs are idempotent, and TCP end-to-end
  tests exercise install plus real activation inventory. Dev.22 added atomic,
  content-addressed runtime image
  materialization with entrypoint integrity revalidation and a device-level
  process supervisor. Real start/stop/restart use process groups, observed PIDs,
  bundle stop timeouts, forced cleanup, and a Linux parent-death runner.
  Dev.23 hardens that lifecycle boundary: runner, entrypoint, and descendants
  share one process group; main-process exit and stop-timeout paths clear the
  complete group; generic process signalling refuses to bypass a managed
  runner; and process snapshots identify managed runner PIDs by application.
  The dev.23 `kindlehf` daemon passed this lifecycle on the KT6 over the real
  USB transport: idempotent start/stop, PID-changing restart, managed-PID
  signal rejection, normal and forced whole-group cleanup, short-lived-process
  reaping, and continued Bridge availability all passed. Dev.24 adds atomic
  rollback and uninstall, a retry-safe rollback cursor, bounded `on-failure`
  restart, and a truthful terminal `failed` state. On KT6, rollback from a
  running v0.2 fixture restored v0.1, killed the old process, and preserved
  unrelated applications; retry returned `NO_ROLLBACK_AVAILABLE`. Uninstall
  killed the runner, removed only the active inventory entry, and preserved
  application data. The first hardware restart-policy check then exposed a
  Linux ownership bug: `PR_SET_PDEATHSIG` followed the short-lived RPC worker
  thread. Dev.25 moves every spawn/status/stop operation to one daemon-lifetime
  supervisor actor. A Linux regression now proves an application survives the
  requesting thread and completes two failed retries before remaining running.
  Dev.26 uses KBP negotiation revision 3, carries generic request/reply traffic
  on `rpc.v1`, requires `shell.v2`, and accepts activation schema 3 under
  `/var/local/kindlebridge/apps`. Missing runtime images are invalid state. The
  matched dev.26 package passed KT6 exec, PTY/raw Shell, 32 MiB integrity sync,
  concurrent 128 MiB sync plus two Shells and log traffic, application
  lifecycle/restart-policy, host-server loss, and physical unplug/replug
  acceptance. The daemon remained on the same PID across the physical cycle.
- Device-side lifecycle state machines plus typed privileged broker grants.
  Non-interactive exec, sync, active KBB application inventory, bounded device
  log snapshots, process snapshots, and process signalling are connected to the
  real transport and operating-system/file APIs. Application start/stop/restart
  are connected to real device processes; rollback/uninstall and bounded
  on-failure restart are connected to the signed activation store and real
  supervisor. Privileged operations remain unavailable rather than reporting
  fake success.
- A formal USB device link, not only a raw probe. The host automatically
  discovers and claims the exact `VID_1949:PID_9981` `ff/4b/01` interface,
  leaves MTP alone, performs the normal KBP HELLO, and exposes the same exec and
  sync services as TCP. Device `serve-usb` owns the FunctionFS endpoints and
  accepts reconnects while the gadget stays bound. Split-endpoint tests cover
  common framing and both halves of the KBP handshake. Earlier KT6 validation
  enumerated stock MTP plus the Bridge interface and passed 1 MiB plus 128 MiB
  sync integrity checks. A later dev.6 package run showed that keeping the A/B
  launcher and heartbeat under `/mnt/us` could return `ESTALE` as soon as MTP
  enumerated, terminating the device session after its first host call. The
  revised package keeps the complete persistent control plane under
  `/var/local/kindlebridge/control`. The dev.17 package passed repeated calls,
  server release/reclaim without a cable replug, root exec, persistent PTY/raw
  shell, and concurrent shell/sync/log hardware validation on KT6. After
  bypassing the confirmed FSP backing layer, three consecutive formal 128 MiB pushes measured 17.63,
  17.32, and 16.80 MiB/s; a full pull measured 17.93 MiB/s and matched SHA-256.
  The push median improved about 79.8% over the immediately preceding
  9.63 MiB/s FSP-path median. Earlier raw-path validation reached
  14.99 MiB/s each way (29.97 MiB/s aggregate),
  but its direct-controller recovery path was later found unsafe and is not a
  supported lifecycle. The vendor kernel does not install the interface GUID;
  the repository now provides a fail-closed Windows onboarding script that
  verifies the exact `MI_01` hardware ID and inbox WinUSB service before adding
  the stable GUID. Clean-host validation remains a release gate.
- A standalone, on-demand MRPI/KUAL development package exercised on the KT6.
  It installs only below `/mnt/us` and `/var/local`, has no runtime calls or
  dependency on USBNetLite or KindleRoot, and preserves stock MTP in the
  composite. The launcher, A/B slots, manifests, PID files, and heartbeat live
  below `/var/local/kindlebridge/control`, while `/mnt/us` contains only the
  KUAL entry point and developer data. The installer atomically replaces only
  the control layout, with rollback if the replacement cannot be committed. The manager
  uses stock `volumd`/HAL events for both handoff
  directions, never binds stock MTP directly, never resets MTU3, and refuses
  start or stop while the cable is connected.
  KUAL uses task-oriented actions, starts without a time limit or closing its
  menu, and treats repeated start/stop as success. Installation and upgrades
  never change USB ownership: an active or indeterminate manager state is rejected,
  while an inactive installation is replaced atomically and remains inactive.
  Laboratory invocations may opt into a bounded watchdog. Twenty-six
  deterministic shell lifecycle tests
  cover connected fail-closed behavior, MTP and existing stock-network entry,
  rollback, stale and detached state reporting, PID ownership, precise
  Bridge-only cleanup,
  stock handback, heartbeat-aware health reporting, direct-FSP-backing selection
  with a safe public-path fallback, and actionable KUAL behavior. The current
  stock-`volumd` manager has completed both handoff directions and re-entry on
  KT6; discovery, repeated root exec, multi-window stability, and large sync then
  passed. Dev.26 additionally passed a stock suspend/resume with an active Shell,
  Windows Terminal live resize from 30x120 to 42x132, and ten rounds of concurrent
  Shell/sync/log traffic. Larger repeated physical-cycle qualification remains a
  publication gate.
- The package integrates a root-confined A/B daemon launcher into the USB
  manager. Host `daemon stage` uploads through `sync.v1`, verifies BLAKE3 plus
  the ELF32 little-endian ARM header on-device, writes only the inactive slot,
  and leaves the active USB session untouched. Activation is an explicit KUAL
  action with USB unplugged. The manager keeps the UDC unbound until the target
  has sustained its instance-bound readiness heartbeat for ten seconds or
  the launcher has rolled back after three failed starts. Only then can USB be
  exposed to the host. Live replacement was removed: the daemon is the sole
  FunctionFS owner, so killing it also removes the transport needed for online
  recovery. The launcher distinguishes the initial-readiness deadline from the
  steady-state heartbeat deadline, tolerates transient heartbeat-file replacement
  gaps after readiness, and retries heartbeat writes instead of abandoning them
  after one filesystem error. Production manifests use a ten-second
  steady-state timeout so a userstore `fsync` cannot be mistaken for a daemon
  crash. A healthy daemon also receives one heartbeat window after a watchdog
  scheduling or wall-clock discontinuity, preventing resume-order races without
  masking a daemon that remains stuck. The watchdog records the exact timeout
  reason before a restart.
  Launcher, USB manager, and package-layout updates remain MRPI-only.
- `kindlehf` cross-builds that are ELF32 ARM hard-float and require at most
  GLIBC_2.18, below the KT6 firmware ceiling used by the build check.
- A one-shot KBP/TCP hardware probe ran successfully on the KT6, and bounded
  filesystem/hash/memory benchmarks established the initial hardware data-path baseline.
  See `docs/hardware-lab/kt6-5.17.1.0.4.md`.
- Workspace unit/integration tests, formatting, and Clippy with warnings denied.
  Shell coverage includes PTY persistence/resize, raw binary channel separation,
  a 32 MiB untruncated stream, malformed-stream isolation, and two simultaneous
  shells under concurrent sync and log traffic with a 50 ms echo-P95 gate. On
  KT6, 120 alternating echo samples during a 128 MiB push measured 15.77 ms P50
  and 25.42 ms P95; closing one shell mid-transfer no longer stalls the other
  shell or sync, and the push completed in 6.09 seconds. A separate close-during-
  sync regression completed in 6.04 seconds with the remote shell exit code
  preserved. The shared Windows service also passed three consecutive
  stop/automatic-start cycles without a cable replug.
  The shell USB-lifecycle suite defines 26 deterministic cases, including exact
  managed-entrypoint ownership, idempotent actions, controlled supervisor
  shutdown, and manual crash-fuse recovery. Nine additional MRPI installer and
  installer/uninstaller scenarios cover fresh and inactive installs, active, unknown and
  manager-missing fail-closed behavior, prepared-transaction rollback,
  committed-transaction cleanup, and inactive removal. The runner
  now rejects partial TAP output instead of trusting a premature zero exit. The
  Rust count is intentionally not frozen because it changes as coverage is added.
- A cost-bounded GitHub Actions job runs the portable formatting, Rust
  test/Clippy, shell lifecycle, and Windows-onboarding selector gates. Windows
  host integration, the `kindlehf` cross-build, package construction, and KT6
  hardware tests remain explicit local gates.

## Required before an internal feature-complete candidate

- KT6 validation of stock-`volumd` Bridge-to-MTP handback and re-entry, followed
  by unplug/replug, sleep/wake, crashes, concurrency, and stock-MTP recovery tests.
  The Windows onboarding script still needs validation on a clean host and
  across supported Windows versions.
- Wi-Fi/TCP discovery, authenticated pairing, session encryption, multi-transport
  reconnect, and hardware throughput/latency measurements under concurrent streams.
- A narrowly scoped, locally authenticated root broker IPC implementation.
- Root exec grants, sync progress/cancellation and directory semantics, app
  `run --watch`, logs/events, process control, forward/reverse, GDB,
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
