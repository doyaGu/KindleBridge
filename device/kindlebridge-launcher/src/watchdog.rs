use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::fs_safe::{entry_exists, SafeRoot};
use crate::manifest::{Slot, SlotManifest};
use crate::{Error, ErrorKind, Result};

const POINTER_FILE: &str = "current";
const PENDING_FILE: &str = "launcher/pending-slot";
pub(crate) const PREVIOUS_FILE: &str = "launcher/previous-slot";
const STATE_FILE: &str = "launcher/watchdog-state";
static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub trait Clock {
    fn now_ms(&self) -> u64;
    fn sleep_ms(&mut self, duration_ms: u64);
}

pub trait DisableFlag {
    fn is_disabled(&self) -> Result<bool>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpawnRequest {
    pub slot: Slot,
    pub executable: PathBuf,
    pub heartbeat: PathBuf,
    pub instance: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildStatus {
    Running,
    Exited { code: Option<i32> },
}

pub trait ChildRunner {
    fn spawn(&mut self, request: &SpawnRequest) -> Result<u64>;
    fn poll(&mut self, child_id: u64) -> Result<ChildStatus>;
    fn terminate(&mut self, child_id: u64) -> Result<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StepOutcome {
    Started { slot: Slot, child_id: u64 },
    Running { slot: Slot, child_id: u64 },
    Healthy { slot: Slot, child_id: u64 },
    BackingOff { slot: Slot, until_ms: u64 },
    RolledBack { failed: Slot, restored: Slot },
    Disabled,
    Halted { slot: Slot, crashes: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingSwitch {
    previous: Slot,
    target: Slot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WatchdogState {
    slot: Slot,
    crashes: u32,
    next_start_ms: u64,
    halted: bool,
}

#[derive(Clone, Debug)]
struct RunningChild {
    id: u64,
    slot: Slot,
    started_ms: u64,
    last_poll_ms: u64,
    instance: String,
    last_heartbeat_timestamp_ms: Option<u64>,
    first_heartbeat_ms: Option<u64>,
    last_valid_heartbeat_ms: Option<u64>,
    healthy: bool,
}

pub struct Launcher<R, C, D> {
    root: SafeRoot,
    runner: R,
    clock: C,
    disable: D,
    state: WatchdogState,
    running: Option<RunningChild>,
}

impl<R: ChildRunner, C: Clock, D: DisableFlag> Launcher<R, C, D> {
    pub fn open(root: impl AsRef<Path>, runner: R, clock: C, disable: D) -> Result<Self> {
        let root = SafeRoot::open(root.as_ref())?;
        recover_pending_pointer(&root)?;
        let slot = read_slot_pointer(&root, POINTER_FILE)?;
        let state = read_state(&root)?.unwrap_or(WatchdogState {
            slot,
            crashes: 0,
            next_start_ms: 0,
            halted: false,
        });
        Ok(Self {
            root,
            runner,
            clock,
            disable,
            state,
            running: None,
        })
    }

    pub fn request_slot(&mut self, target: Slot) -> Result<()> {
        let current = read_slot_pointer(&self.root, POINTER_FILE)?;
        if current == target {
            return Ok(());
        }
        if let Some(pending) = read_pending(&self.root)? {
            if pending.target == target {
                return Ok(());
            }
            return Err(Error::new(
                ErrorKind::InvalidState,
                "another slot switch is still pending",
            ));
        }
        validate_slot(&self.root, target)?;
        let pending = PendingSwitch {
            previous: current,
            target,
        };
        self.root
            .atomic_write(PENDING_FILE, encode_pending(&pending).as_bytes())?;
        write_slot_pointer(&self.root, POINTER_FILE, target)?;
        self.state = WatchdogState {
            slot: target,
            crashes: 0,
            next_start_ms: 0,
            halted: false,
        };
        write_state(&self.root, &self.state)
    }

    pub fn step(&mut self) -> Result<StepOutcome> {
        if self.disable.is_disabled()? {
            if let Some(running) = self.running.take() {
                self.runner.terminate(running.id)?;
            }
            return Ok(StepOutcome::Disabled);
        }

        let active = read_slot_pointer(&self.root, POINTER_FILE)?;
        if self
            .running
            .as_ref()
            .is_some_and(|running| running.slot != active)
        {
            let old = self.running.take().expect("running child exists");
            self.runner.terminate(old.id)?;
            self.state = WatchdogState {
                slot: active,
                crashes: 0,
                next_start_ms: 0,
                halted: false,
            };
            write_state(&self.root, &self.state)?;
        }
        if self.state.slot != active && self.running.is_none() {
            self.state = WatchdogState {
                slot: active,
                crashes: 0,
                next_start_ms: 0,
                halted: false,
            };
            write_state(&self.root, &self.state)?;
        }

        if let Some(running) = self.running.take() {
            return self.step_running(running);
        }
        if self.state.halted {
            return Ok(StepOutcome::Halted {
                slot: self.state.slot,
                crashes: self.state.crashes,
            });
        }
        let now = self.clock.now_ms();
        if now < self.state.next_start_ms {
            return Ok(StepOutcome::BackingOff {
                slot: self.state.slot,
                until_ms: self.state.next_start_ms,
            });
        }

        let manifest = match validate_slot(&self.root, active) {
            Ok(manifest) => manifest,
            Err(error) => {
                if let Some(pending) = read_pending(&self.root)? {
                    if pending.target == active {
                        return self.rollback(pending, error);
                    }
                }
                return Err(error);
            }
        };
        let heartbeat = self.root.resolve(&manifest.heartbeat)?;
        self.root.remove_file(&manifest.heartbeat)?;
        let executable = self.root.resolve(&format!(
            "slots/{}/{}",
            active.as_str(),
            manifest.executable
        ))?;
        let instance = new_instance(now);
        let request = SpawnRequest {
            slot: active,
            executable,
            heartbeat,
            instance: instance.clone(),
        };
        let child_id = self.runner.spawn(&request)?;
        self.running = Some(RunningChild {
            id: child_id,
            slot: active,
            started_ms: now,
            last_poll_ms: now,
            instance,
            last_heartbeat_timestamp_ms: None,
            first_heartbeat_ms: None,
            last_valid_heartbeat_ms: None,
            healthy: false,
        });
        Ok(StepOutcome::Started {
            slot: active,
            child_id,
        })
    }

    pub fn run(&mut self) -> Result<StepOutcome> {
        loop {
            let outcome = self.step()?;
            match outcome {
                StepOutcome::Disabled | StepOutcome::Halted { .. } => return Ok(outcome),
                StepOutcome::BackingOff { until_ms, .. } => {
                    let delay = until_ms.saturating_sub(self.clock.now_ms()).min(1000);
                    self.clock.sleep_ms(delay.max(1));
                }
                _ => self.clock.sleep_ms(100),
            }
        }
    }

    pub fn runner_mut(&mut self) -> &mut R {
        &mut self.runner
    }

    pub fn clock_mut(&mut self) -> &mut C {
        &mut self.clock
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.path()
    }

    fn step_running(&mut self, mut running: RunningChild) -> Result<StepOutcome> {
        let manifest = SlotManifest::load(&self.root, running.slot)?;
        match self.runner.poll(running.id)? {
            ChildStatus::Exited { .. } => self.record_failure(running, &manifest),
            ChildStatus::Running => {
                let now = self.clock.now_ms();
                let poll_gap = now.saturating_sub(running.last_poll_ms);
                let clock_went_backward = now < running.last_poll_ms;
                let clock_discontinuity = clock_went_backward
                    || (running.healthy && poll_gap > manifest.heartbeat_timeout_ms);
                running.last_poll_ms = now;
                if clock_discontinuity {
                    eprintln!(
                        "kindlebridge-launcher: watchdog clock changed by {poll_gap} ms; allowing heartbeat recovery"
                    );
                    if running.healthy {
                        running.last_valid_heartbeat_ms = Some(now);
                    } else {
                        running.started_ms = now;
                        running.last_heartbeat_timestamp_ms = None;
                        running.first_heartbeat_ms = None;
                        running.last_valid_heartbeat_ms = None;
                    }
                }
                let heartbeat = read_heartbeat(&self.root, &manifest.heartbeat)?;
                let heartbeat_advanced = heartbeat.as_ref().is_some_and(|heartbeat| {
                    heartbeat.instance == running.instance
                        && running.last_heartbeat_timestamp_ms != Some(heartbeat.timestamp_ms)
                });
                if heartbeat_advanced {
                    let heartbeat = heartbeat.expect("advanced heartbeat is present");
                    running.last_heartbeat_timestamp_ms = Some(heartbeat.timestamp_ms);
                    running.last_valid_heartbeat_ms = Some(now);
                    let first = *running.first_heartbeat_ms.get_or_insert(now);
                    if now.saturating_sub(first) >= manifest.healthy_after_ms {
                        if !running.healthy {
                            self.confirm_healthy(running.slot)?;
                            running.healthy = true;
                        }
                        let child_id = running.id;
                        let slot = running.slot;
                        self.running = Some(running);
                        return Ok(StepOutcome::Healthy { slot, child_id });
                    }
                } else {
                    let heartbeat_timed_out = match running.last_valid_heartbeat_ms {
                        Some(last_valid) => {
                            now.saturating_sub(last_valid) > manifest.heartbeat_timeout_ms
                        }
                        None => {
                            now.saturating_sub(running.started_ms) >= manifest.startup_timeout_ms
                        }
                    };
                    if heartbeat_timed_out {
                        eprintln!(
                            "kindlebridge-launcher: slot {} heartbeat was unavailable for longer than {} ms; restarting daemon",
                            running.slot,
                            running
                                .last_valid_heartbeat_ms
                                .map_or(manifest.startup_timeout_ms, |_| manifest.heartbeat_timeout_ms)
                        );
                        self.runner.terminate(running.id)?;
                        return self.record_failure(running, &manifest);
                    }
                }
                let child_id = running.id;
                let slot = running.slot;
                let outcome = if running.healthy {
                    StepOutcome::Healthy { slot, child_id }
                } else {
                    StepOutcome::Running { slot, child_id }
                };
                self.running = Some(running);
                Ok(outcome)
            }
        }
    }

    fn record_failure(
        &mut self,
        running: RunningChild,
        manifest: &SlotManifest,
    ) -> Result<StepOutcome> {
        self.state.slot = running.slot;
        self.state.crashes = self.state.crashes.saturating_add(1);
        if let Some(pending) = read_pending(&self.root)? {
            if pending.target == running.slot && self.state.crashes >= manifest.max_crashes {
                return self.rollback(
                    pending,
                    Error::new(ErrorKind::Child, "pending slot exceeded crash threshold"),
                );
            }
        }
        if self.state.crashes >= manifest.max_crashes {
            self.state.halted = true;
            write_state(&self.root, &self.state)?;
            return Ok(StepOutcome::Halted {
                slot: self.state.slot,
                crashes: self.state.crashes,
            });
        }
        let exponent = self.state.crashes.saturating_sub(1).min(31);
        let multiplier = 1_u64 << exponent;
        let delay = manifest
            .backoff_initial_ms
            .saturating_mul(multiplier)
            .min(manifest.backoff_max_ms);
        self.state.next_start_ms = self.clock.now_ms().saturating_add(delay);
        write_state(&self.root, &self.state)?;
        Ok(StepOutcome::BackingOff {
            slot: self.state.slot,
            until_ms: self.state.next_start_ms,
        })
    }

    fn rollback(&mut self, pending: PendingSwitch, _reason: Error) -> Result<StepOutcome> {
        write_slot_pointer(&self.root, POINTER_FILE, pending.previous)?;
        self.root.remove_file(PENDING_FILE)?;
        self.state = WatchdogState {
            slot: pending.previous,
            crashes: 0,
            next_start_ms: self.clock.now_ms(),
            halted: false,
        };
        write_state(&self.root, &self.state)?;
        Ok(StepOutcome::RolledBack {
            failed: pending.target,
            restored: pending.previous,
        })
    }

    fn confirm_healthy(&mut self, slot: Slot) -> Result<()> {
        self.state = WatchdogState {
            slot,
            crashes: 0,
            next_start_ms: 0,
            halted: false,
        };
        write_state(&self.root, &self.state)?;
        if let Some(pending) = read_pending(&self.root)?.filter(|pending| pending.target == slot) {
            write_slot_pointer(&self.root, PREVIOUS_FILE, pending.previous)?;
            self.root.remove_file(PENDING_FILE)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Heartbeat {
    instance: String,
    timestamp_ms: u64,
}

#[must_use]
pub fn encode_heartbeat(instance: &str, timestamp_ms: u64) -> Vec<u8> {
    format!("KINDLEBRIDGE_HEARTBEAT_V1\ninstance={instance}\ntimestamp_ms={timestamp_ms}\n")
        .into_bytes()
}

fn read_heartbeat(root: &SafeRoot, relative: &str) -> Result<Option<Heartbeat>> {
    let Some(bytes) = root.optional_file(relative, 1024)? else {
        return Ok(None);
    };
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(None);
    };
    let mut lines = text.lines();
    if lines.next() != Some("KINDLEBRIDGE_HEARTBEAT_V1") {
        return Ok(None);
    }
    let Some(instance) = lines.next().and_then(|line| line.strip_prefix("instance=")) else {
        return Ok(None);
    };
    let Some(timestamp) = lines
        .next()
        .and_then(|line| line.strip_prefix("timestamp_ms="))
        .and_then(|value| value.parse().ok())
    else {
        return Ok(None);
    };
    if instance.is_empty() || lines.next().is_some() || !text.ends_with('\n') {
        return Ok(None);
    }
    Ok(Some(Heartbeat {
        instance: instance.into(),
        timestamp_ms: timestamp,
    }))
}

pub(crate) fn validate_slot(root: &SafeRoot, slot: Slot) -> Result<SlotManifest> {
    let manifest = SlotManifest::load(root, slot)?;
    let executable = root.resolve(&format!("slots/{}/{}", slot.as_str(), manifest.executable))?;
    if !entry_exists(&executable)? || !fs::metadata(&executable)?.is_file() {
        return Err(Error::new(
            ErrorKind::InvalidManifest,
            "slot executable is missing or not a regular file",
        ));
    }
    Ok(manifest)
}

fn recover_pending_pointer(root: &SafeRoot) -> Result<()> {
    let Some(pending) = read_pending(root)? else {
        return Ok(());
    };
    let active = read_slot_pointer(root, POINTER_FILE)?;
    if active == pending.previous {
        write_slot_pointer(root, POINTER_FILE, pending.target)
    } else if active == pending.target {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::InvalidState,
            "active pointer is inconsistent with pending switch",
        ))
    }
}

pub(crate) fn read_slot_pointer(root: &SafeRoot, relative: &str) -> Result<Slot> {
    let bytes = root.read_file(relative, 3)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| Error::new(ErrorKind::InvalidState, "slot pointer is not UTF-8"))?;
    let value = text.strip_suffix('\n').unwrap_or(text);
    if value.len() != 1 {
        return Err(Error::new(
            ErrorKind::InvalidState,
            "slot pointer is not canonical",
        ));
    }
    Slot::parse(value).map_err(|error| Error::new(ErrorKind::InvalidState, error.message))
}

pub(crate) fn write_slot_pointer(root: &SafeRoot, relative: &str, slot: Slot) -> Result<()> {
    root.atomic_write(relative, format!("{slot}\n").as_bytes())
}

fn read_pending(root: &SafeRoot) -> Result<Option<PendingSwitch>> {
    let Some(bytes) = root.optional_file(PENDING_FILE, 128)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| Error::new(ErrorKind::InvalidState, "pending switch is not UTF-8"))?;
    let mut lines = text.lines();
    if lines.next() != Some("KINDLEBRIDGE_PENDING_V1") {
        return invalid_state("unknown pending switch format");
    }
    let previous = lines
        .next()
        .and_then(|line| line.strip_prefix("previous="))
        .ok_or_else(|| Error::new(ErrorKind::InvalidState, "missing previous slot"))?;
    let target = lines
        .next()
        .and_then(|line| line.strip_prefix("target="))
        .ok_or_else(|| Error::new(ErrorKind::InvalidState, "missing target slot"))?;
    if lines.next().is_some() || !text.ends_with('\n') {
        return invalid_state("non-canonical pending switch");
    }
    let pending = PendingSwitch {
        previous: Slot::parse(previous)?,
        target: Slot::parse(target)?,
    };
    if pending.previous == pending.target {
        return invalid_state("pending switch does not change slots");
    }
    Ok(Some(pending))
}

fn encode_pending(pending: &PendingSwitch) -> String {
    format!(
        "KINDLEBRIDGE_PENDING_V1\nprevious={}\ntarget={}\n",
        pending.previous, pending.target
    )
}

fn read_state(root: &SafeRoot) -> Result<Option<WatchdogState>> {
    let Some(bytes) = root.optional_file(STATE_FILE, 256)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| Error::new(ErrorKind::InvalidState, "watchdog state is not UTF-8"))?;
    let mut lines = text.lines();
    if lines.next() != Some("KINDLEBRIDGE_WATCHDOG_V1") {
        return invalid_state("unknown watchdog state format");
    }
    let slot = parse_state_value(&mut lines, "slot")?;
    let crashes = parse_state_value(&mut lines, "crashes")?;
    let next_start_ms = parse_state_value(&mut lines, "next_start_ms")?;
    let halted = parse_state_value(&mut lines, "halted")?;
    if lines.next().is_some() || !text.ends_with('\n') {
        return invalid_state("non-canonical watchdog state");
    }
    Ok(Some(WatchdogState {
        slot: Slot::parse(slot)?,
        crashes: crashes
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidState, "invalid crash count"))?,
        next_start_ms: next_start_ms
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidState, "invalid next start time"))?,
        halted: match halted {
            "0" => false,
            "1" => true,
            _ => return invalid_state("invalid halted state"),
        },
    }))
}

fn write_state(root: &SafeRoot, state: &WatchdogState) -> Result<()> {
    root.atomic_write(
        STATE_FILE,
        format!(
            "KINDLEBRIDGE_WATCHDOG_V1\nslot={}\ncrashes={}\nnext_start_ms={}\nhalted={}\n",
            state.slot,
            state.crashes,
            state.next_start_ms,
            u8::from(state.halted)
        )
        .as_bytes(),
    )
}

fn parse_state_value<'a>(lines: &mut impl Iterator<Item = &'a str>, key: &str) -> Result<&'a str> {
    lines
        .next()
        .and_then(|line| line.strip_prefix(&format!("{key}=")))
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidState,
                format!("missing state field {key}"),
            )
        })
}

fn new_instance(now_ms: u64) -> String {
    let counter = INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{now_ms:016x}-{}-{counter:016x}", std::process::id())
}

fn invalid_state<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorKind::InvalidState, message))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::manifest::test_manifest;

    static DIRECTORY_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let counter = DIRECTORY_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "kindlebridge-launcher-{label}-{}-{counter:016x}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Default)]
    struct MockClock {
        now: Rc<Cell<u64>>,
    }

    impl MockClock {
        fn advance(&self, millis: u64) {
            self.now.set(self.now.get() + millis);
        }
    }

    impl Clock for MockClock {
        fn now_ms(&self) -> u64 {
            self.now.get()
        }

        fn sleep_ms(&mut self, duration_ms: u64) {
            self.advance(duration_ms);
        }
    }

    #[derive(Clone, Default)]
    struct MockDisable(Rc<Cell<bool>>);

    impl DisableFlag for MockDisable {
        fn is_disabled(&self) -> Result<bool> {
            Ok(self.0.get())
        }
    }

    #[derive(Default)]
    struct MockRunner {
        next_id: u64,
        requests: Vec<SpawnRequest>,
        statuses: BTreeMap<u64, ChildStatus>,
        terminated: Vec<u64>,
    }

    impl MockRunner {
        fn exit(&mut self, child_id: u64, code: i32) {
            self.statuses
                .insert(child_id, ChildStatus::Exited { code: Some(code) });
        }
    }

    impl ChildRunner for MockRunner {
        fn spawn(&mut self, request: &SpawnRequest) -> Result<u64> {
            self.next_id += 1;
            self.requests.push(request.clone());
            self.statuses.insert(self.next_id, ChildStatus::Running);
            Ok(self.next_id)
        }

        fn poll(&mut self, child_id: u64) -> Result<ChildStatus> {
            self.statuses
                .get(&child_id)
                .copied()
                .ok_or_else(|| Error::new(ErrorKind::Child, "missing mock child"))
        }

        fn terminate(&mut self, child_id: u64) -> Result<()> {
            self.statuses.remove(&child_id);
            self.terminated.push(child_id);
            Ok(())
        }
    }

    fn setup_root(label: &str) -> TestDirectory {
        let directory = TestDirectory::new(label);
        for slot in [Slot::A, Slot::B] {
            let slot_dir = directory.0.join("slots").join(slot.as_str());
            fs::create_dir_all(slot_dir.join("bin")).unwrap();
            fs::write(slot_dir.join("slot.manifest"), test_manifest(slot)).unwrap();
            fs::write(slot_dir.join("bin/kindlebridged"), b"mock executable").unwrap();
        }
        fs::write(directory.0.join("current"), b"A\n").unwrap();
        directory
    }

    fn launcher(
        root: &Path,
        clock: MockClock,
        disable: MockDisable,
    ) -> Launcher<MockRunner, MockClock, MockDisable> {
        Launcher::open(root, MockRunner::default(), clock, disable).unwrap()
    }

    #[test]
    fn starts_and_requires_sustained_instance_bound_heartbeat() {
        let directory = setup_root("healthy");
        let clock = MockClock::default();
        let mut launcher = launcher(&directory.0, clock.clone(), MockDisable::default());
        assert_eq!(
            launcher.step().unwrap(),
            StepOutcome::Started {
                slot: Slot::A,
                child_id: 1
            }
        );
        let request = launcher.runner_mut().requests[0].clone();
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Running { .. }
        ));
        clock.advance(3001);
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        assert_eq!(
            launcher.step().unwrap(),
            StepOutcome::Healthy {
                slot: Slot::A,
                child_id: 1
            }
        );

        // Health confirmation is a state transition, not a 10 Hz durable
        // rewrite. A later watchdog tick must not touch the persisted state.
        fs::remove_file(directory.0.join(STATE_FILE)).unwrap();
        fs::create_dir(directory.0.join(STATE_FILE)).unwrap();
        clock.advance(100);
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Healthy { .. }
        ));
    }

    #[test]
    fn healthy_child_tolerates_transient_but_not_sustained_missing_heartbeat() {
        let directory = setup_root("transient-missing-heartbeat");
        let clock = MockClock::default();
        let mut launcher = launcher(&directory.0, clock.clone(), MockDisable::default());
        launcher.step().unwrap();
        let request = launcher.runner_mut().requests[0].clone();

        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        launcher.step().unwrap();
        clock.advance(3001);
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Healthy { .. }
        ));

        // The production failure happened only after the 10-second startup
        // deadline. A FAT atomic-replace lookup gap must not turn that startup
        // deadline into a one-sample steady-state kill switch.
        clock.advance(7000);
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        launcher.step().unwrap();
        fs::remove_file(&request.heartbeat).unwrap();
        clock.advance(100);

        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Healthy { .. }
        ));
        assert!(launcher.runner_mut().terminated.is_empty());

        clock.advance(901);
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::BackingOff { .. }
        ));
        assert_eq!(launcher.runner_mut().terminated, vec![1]);
    }

    #[test]
    fn healthy_child_gets_one_heartbeat_window_after_a_large_clock_jump() {
        let directory = setup_root("resume-clock-jump");
        let clock = MockClock::default();
        let mut launcher = launcher(&directory.0, clock.clone(), MockDisable::default());
        launcher.step().unwrap();
        let request = launcher.runner_mut().requests[0].clone();

        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        launcher.step().unwrap();
        clock.advance(3001);
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Healthy { .. }
        ));

        // The KT6 can resume the launcher before the daemon heartbeat thread.
        // A multi-hour wall-clock jump must allow one normal heartbeat window
        // rather than killing the healthy FunctionFS owner immediately.
        clock.advance(26_538_782);
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Healthy { .. }
        ));
        assert!(launcher.runner_mut().terminated.is_empty());

        clock.advance(100);
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Healthy { .. }
        ));
        assert!(launcher.runner_mut().terminated.is_empty());
    }

    #[test]
    fn resume_grace_does_not_hide_sustained_heartbeat_loss() {
        let directory = setup_root("resume-heartbeat-loss");
        let clock = MockClock::default();
        let mut launcher = launcher(&directory.0, clock.clone(), MockDisable::default());
        launcher.step().unwrap();
        let request = launcher.runner_mut().requests[0].clone();

        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        launcher.step().unwrap();
        clock.advance(3001);
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        launcher.step().unwrap();

        clock.advance(26_538_782);
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Healthy { .. }
        ));
        clock.advance(900);
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Healthy { .. }
        ));
        clock.advance(101);
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::BackingOff { .. }
        ));
        assert_eq!(launcher.runner_mut().terminated, vec![1]);
    }

    #[test]
    fn crashes_back_off_exponentially_then_halt() {
        let directory = setup_root("crash");
        let clock = MockClock::default();
        let mut launcher = launcher(&directory.0, clock.clone(), MockDisable::default());
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Started { .. }
        ));
        launcher.runner_mut().exit(1, 1);
        assert_eq!(
            launcher.step().unwrap(),
            StepOutcome::BackingOff {
                slot: Slot::A,
                until_ms: 100
            }
        );
        clock.advance(100);
        assert_eq!(
            launcher.step().unwrap(),
            StepOutcome::Started {
                slot: Slot::A,
                child_id: 2
            }
        );
        launcher.runner_mut().exit(2, 1);
        assert_eq!(
            launcher.step().unwrap(),
            StepOutcome::BackingOff {
                slot: Slot::A,
                until_ms: 300
            }
        );
        clock.advance(200);
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Started { .. }
        ));
        launcher.runner_mut().exit(3, 1);
        assert_eq!(
            launcher.step().unwrap(),
            StepOutcome::Halted {
                slot: Slot::A,
                crashes: 3
            }
        );
    }

    #[test]
    fn failed_pending_slot_rolls_back_to_previous_slot() {
        let directory = setup_root("rollback");
        let clock = MockClock::default();
        let mut launcher = launcher(&directory.0, clock.clone(), MockDisable::default());
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Started { .. }
        ));
        launcher.request_slot(Slot::B).unwrap();
        assert_eq!(
            launcher.step().unwrap(),
            StepOutcome::Started {
                slot: Slot::B,
                child_id: 2
            }
        );
        assert_eq!(launcher.runner_mut().terminated, vec![1]);

        for (child_id, delay) in [(2, 100), (3, 200)] {
            launcher.runner_mut().exit(child_id, 1);
            assert!(matches!(
                launcher.step().unwrap(),
                StepOutcome::BackingOff { .. }
            ));
            clock.advance(delay);
            assert!(matches!(
                launcher.step().unwrap(),
                StepOutcome::Started { .. }
            ));
        }
        launcher.runner_mut().exit(4, 1);
        assert_eq!(
            launcher.step().unwrap(),
            StepOutcome::RolledBack {
                failed: Slot::B,
                restored: Slot::A
            }
        );
        assert_eq!(fs::read(directory.0.join("current")).unwrap(), b"A\n");
        assert!(!directory.0.join(PENDING_FILE).exists());
    }

    #[test]
    fn confirmed_pending_slot_records_a_one_way_rollback_point() {
        let directory = setup_root("confirmed-rollback-point");
        let clock = MockClock::default();
        let mut launcher = launcher(&directory.0, clock.clone(), MockDisable::default());
        launcher.step().unwrap();
        launcher.request_slot(Slot::B).unwrap();
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Started { slot: Slot::B, .. }
        ));
        let request = launcher.runner_mut().requests[1].clone();
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();
        launcher.step().unwrap();
        clock.advance(3001);
        fs::write(
            &request.heartbeat,
            encode_heartbeat(&request.instance, clock.now_ms()),
        )
        .unwrap();

        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Healthy { slot: Slot::B, .. }
        ));
        assert_eq!(fs::read(directory.0.join(PREVIOUS_FILE)).unwrap(), b"A\n");
        assert!(!directory.0.join(PENDING_FILE).exists());
    }

    #[test]
    fn disable_flag_prevents_start_and_terminates_running_child() {
        let directory = setup_root("disable");
        let disable = MockDisable::default();
        disable.0.set(true);
        let mut launcher = launcher(&directory.0, MockClock::default(), disable.clone());
        assert_eq!(launcher.step().unwrap(), StepOutcome::Disabled);
        assert!(launcher.runner_mut().requests.is_empty());
        disable.0.set(false);
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::Started { .. }
        ));
        disable.0.set(true);
        assert_eq!(launcher.step().unwrap(), StepOutcome::Disabled);
        assert_eq!(launcher.runner_mut().terminated, vec![1]);
    }

    #[test]
    fn stale_heartbeat_cannot_confirm_new_process() {
        let directory = setup_root("stale-heartbeat");
        fs::create_dir(directory.0.join("run")).unwrap();
        fs::write(
            directory.0.join("run/heartbeat"),
            encode_heartbeat("old-instance", 0),
        )
        .unwrap();
        let clock = MockClock::default();
        let mut launcher = launcher(&directory.0, clock.clone(), MockDisable::default());
        launcher.step().unwrap();
        assert!(!directory.0.join("run/heartbeat").exists());
        clock.advance(10_000);
        assert!(matches!(
            launcher.step().unwrap(),
            StepOutcome::BackingOff { .. }
        ));
        assert_eq!(launcher.runner_mut().terminated, vec![1]);
    }
}
