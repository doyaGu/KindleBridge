//! Deterministic application lifecycle state machine and process supervisor.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use kindlebridge_bundle::{Digest, MaterializedApplication};
use thiserror::Error;

#[cfg(target_os = "linux")]
const MAX_CONSECUTIVE_RESTARTS: u32 = 3;
#[cfg(target_os = "linux")]
const RESTART_STABLE_WINDOW: Duration = Duration::from_secs(60);
#[cfg(target_os = "linux")]
const RESTART_BACKOFFS: [Duration; MAX_CONSECUTIVE_RESTARTS as usize] = [
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(200),
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestartPolicy {
    Never,
    OnFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppState {
    Installed,
    Starting,
    Running,
    Stopping,
    Stopped,
    Crashed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppInstance {
    pub logical_id: String,
    pub channel: String,
    pub generation_id: String,
    pub restart_policy: RestartPolicy,
    pub state: AppState,
    pub restart_count: u32,
}

impl AppInstance {
    pub fn new(
        logical_id: impl Into<String>,
        channel: impl Into<String>,
        generation_id: impl Into<String>,
        restart_policy: RestartPolicy,
    ) -> Result<Self, AppError> {
        let logical_id = logical_id.into();
        let channel = channel.into();
        if !valid_logical_id(&logical_id) {
            return Err(AppError::InvalidLogicalId);
        }
        if !valid_channel(&channel) {
            return Err(AppError::InvalidChannel);
        }
        Ok(Self {
            logical_id,
            channel,
            generation_id: generation_id.into(),
            restart_policy,
            state: AppState::Installed,
            restart_count: 0,
        })
    }

    pub fn request_start(&mut self) -> Result<(), AppError> {
        if !matches!(
            self.state,
            AppState::Installed | AppState::Stopped | AppState::Crashed
        ) {
            return Err(AppError::InvalidTransition {
                from: self.state,
                action: "start",
            });
        }
        self.state = AppState::Starting;
        Ok(())
    }

    pub fn mark_started(&mut self) -> Result<(), AppError> {
        self.transition(AppState::Starting, AppState::Running, "mark-started")
    }

    pub fn request_stop(&mut self) -> Result<(), AppError> {
        self.transition(AppState::Running, AppState::Stopping, "stop")
    }

    pub fn mark_stopped(&mut self) -> Result<(), AppError> {
        self.transition(AppState::Stopping, AppState::Stopped, "mark-stopped")
    }

    pub fn mark_exited(&mut self, successful: bool) -> Result<bool, AppError> {
        if !matches!(self.state, AppState::Starting | AppState::Running) {
            return Err(AppError::InvalidTransition {
                from: self.state,
                action: "process-exit",
            });
        }
        if successful {
            self.state = AppState::Stopped;
            return Ok(false);
        }
        self.state = AppState::Crashed;
        if self.restart_policy == RestartPolicy::OnFailure {
            self.restart_count = self.restart_count.saturating_add(1);
            return Ok(true);
        }
        Ok(false)
    }

    fn transition(
        &mut self,
        expected: AppState,
        target: AppState,
        action: &'static str,
    ) -> Result<(), AppError> {
        if self.state != expected {
            return Err(AppError::InvalidTransition {
                from: self.state,
                action,
            });
        }
        self.state = target;
        Ok(())
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum AppError {
    #[error("invalid application logical id")]
    InvalidLogicalId,
    #[error("invalid application channel")]
    InvalidChannel,
    #[error("cannot perform {action} while application is {from:?}")]
    InvalidTransition {
        from: AppState,
        action: &'static str,
    },
}

fn valid_logical_id(value: &str) -> bool {
    let mut components = value.split('.');
    let Some(first) = components.next() else {
        return false;
    };
    let mut count = 1;
    if !valid_identifier_component(first) {
        return false;
    }
    for component in components {
        count += 1;
        if !valid_identifier_component(component) {
            return false;
        }
    }
    count >= 2 && value.len() <= 255
}

fn valid_identifier_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .as_bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

fn valid_channel(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

/// Owns every application child process launched by this daemon.
///
/// All process operations run on one daemon-lifetime actor thread. On Linux,
/// `PR_SET_PDEATHSIG` follows the particular thread that spawned a child, not
/// just its containing process. Spawning from a per-RPC worker would therefore
/// terminate the application as soon as that request completed. The actor keeps
/// the runner's parent thread alive across KBP sessions while still ensuring a
/// daemon crash terminates every owned process group.
#[derive(Debug)]
pub struct AppSupervisor {
    requests: mpsc::Sender<SupervisorRequest>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Default)]
struct SupervisorState {
    children: BTreeMap<String, ManagedApplication>,
    terminal: BTreeMap<String, TerminalApplication>,
}

#[derive(Debug)]
struct ManagedApplication {
    bundle_root: Digest,
    child: Child,
}

#[derive(Debug)]
struct TerminalApplication {
    bundle_root: Digest,
    failed: bool,
}

#[derive(Debug)]
enum SupervisorRequest {
    Start {
        app: Box<MaterializedApplication>,
        data_root: PathBuf,
        reply: mpsc::Sender<Result<u32, RuntimeError>>,
    },
    Status {
        app_id: String,
        bundle_root: Digest,
        reply: mpsc::Sender<Result<RuntimeStatus, RuntimeError>>,
    },
    ManagedProcesses {
        reply: mpsc::Sender<Result<BTreeMap<u32, String>, RuntimeError>>,
    },
    Stop {
        app_id: String,
        timeout: Duration,
        reply: mpsc::Sender<Result<(), RuntimeError>>,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeStatus {
    Stopped,
    Running(u32),
    Failed,
}

impl AppSupervisor {
    #[must_use]
    pub fn new() -> Self {
        let executable = std::env::current_exe()
            .expect("KindleBridge daemon executable path could not be resolved");
        Self::with_runner_executable(executable)
    }

    #[cfg(debug_assertions)]
    #[doc(hidden)]
    #[must_use]
    pub fn with_runner_executable_for_tests(executable: PathBuf) -> Self {
        Self::with_runner_executable(executable)
    }

    fn with_runner_executable(executable: PathBuf) -> Self {
        let (requests, incoming) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("kindlebridge-app-supervisor".to_owned())
            .spawn(move || supervise_applications(incoming, &executable))
            .expect("KindleBridge application supervisor thread could not be started");
        Self {
            requests,
            worker: Mutex::new(Some(worker)),
        }
    }

    pub fn start(
        &self,
        app: &MaterializedApplication,
        data_root: &Path,
    ) -> Result<u32, RuntimeError> {
        self.request(|reply| SupervisorRequest::Start {
            app: Box::new(app.clone()),
            data_root: data_root.to_path_buf(),
            reply,
        })
    }

    /// Return a live PID only when the owned process still runs the requested
    /// immutable bundle generation.
    pub fn status(&self, app_id: &str, bundle_root: Digest) -> Result<RuntimeStatus, RuntimeError> {
        self.request(|reply| SupervisorRequest::Status {
            app_id: app_id.to_owned(),
            bundle_root,
            reply,
        })
    }

    pub fn app_id_for_pid(&self, pid: u32) -> Result<Option<String>, RuntimeError> {
        Ok(self.managed_processes()?.remove(&pid))
    }

    pub fn managed_processes(&self) -> Result<BTreeMap<u32, String>, RuntimeError> {
        self.request(|reply| SupervisorRequest::ManagedProcesses { reply })
    }

    /// Stop an owned process if present. Repeated stops are successful.
    pub fn stop(&self, app_id: &str, timeout: Duration) -> Result<(), RuntimeError> {
        self.request(|reply| SupervisorRequest::Stop {
            app_id: app_id.to_owned(),
            timeout,
            reply,
        })
    }

    fn request<T>(
        &self,
        make_request: impl FnOnce(mpsc::Sender<Result<T, RuntimeError>>) -> SupervisorRequest,
    ) -> Result<T, RuntimeError> {
        let (reply, response) = mpsc::channel();
        self.requests
            .send(make_request(reply))
            .map_err(|_| RuntimeError::Unavailable)?;
        response.recv().map_err(|_| RuntimeError::Unavailable)?
    }
}

impl Default for AppSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for AppSupervisor {
    fn drop(&mut self) {
        let _ = self.requests.send(SupervisorRequest::Shutdown);
        if let Ok(worker) = self.worker.get_mut() {
            if let Some(worker) = worker.take() {
                let _ = worker.join();
            }
        }
    }
}

fn supervise_applications(incoming: mpsc::Receiver<SupervisorRequest>, executable: &Path) {
    let mut state = SupervisorState::default();
    while let Ok(request) = incoming.recv() {
        match request {
            SupervisorRequest::Start {
                app,
                data_root,
                reply,
            } => {
                let _ = reply.send(start_application(&mut state, &app, &data_root, executable));
            }
            SupervisorRequest::Status {
                app_id,
                bundle_root,
                reply,
            } => {
                let _ = reply.send(application_status(&mut state, &app_id, bundle_root));
            }
            SupervisorRequest::ManagedProcesses { reply } => {
                let _ = reply.send(managed_processes(&mut state));
            }
            SupervisorRequest::Stop {
                app_id,
                timeout,
                reply,
            } => {
                let _ = reply.send(stop_application(&mut state, &app_id, timeout));
            }
            SupervisorRequest::Shutdown => break,
        }
    }
    for (_, mut managed) in std::mem::take(&mut state.children) {
        let _ = terminate(&mut managed.child, Duration::from_secs(2));
    }
}

fn start_application(
    state: &mut SupervisorState,
    app: &MaterializedApplication,
    data_root: &Path,
    supervisor_executable: &Path,
) -> Result<u32, RuntimeError> {
    reap_one(state, &app.app_id)?;
    if let Some(managed) = state.children.get(&app.app_id) {
        if managed.bundle_root == app.bundle_root {
            return Ok(managed.child.id());
        }
        return Err(RuntimeError::DifferentGenerationRunning);
    }
    state.terminal.remove(&app.app_id);

    let data_dir = data_root.join(&app.app_id);
    ensure_plain_directory(data_root)?;
    ensure_plain_directory(&data_dir)?;
    let working_dir = app
        .process
        .working_dir
        .as_ref()
        .map_or_else(|| app.image_root.clone(), |path| app.image_root.join(path));
    let mut command = application_command(app, supervisor_executable)?;
    command
        .current_dir(working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
        .env("HOME", &data_dir)
        .env("TMPDIR", "/tmp")
        .env("SHELL", "/bin/sh")
        .env("KINDLEBRIDGE_APP_ID", &app.app_id)
        .env("KINDLEBRIDGE_APP_ROOT", &app.image_root)
        .env("KINDLEBRIDGE_DATA", &data_dir);
    copy_device_environment(&mut command, "DISPLAY");
    copy_device_environment(&mut command, "LANG");
    copy_device_environment(&mut command, "LC_ALL");
    if let Some(environment) = &app.process.environment {
        command.envs(environment);
    }
    configure_process_group(&mut command);
    let child = command.spawn().map_err(RuntimeError::Spawn)?;
    let pid = child.id();
    state.children.insert(
        app.app_id.clone(),
        ManagedApplication {
            bundle_root: app.bundle_root,
            child,
        },
    );
    Ok(pid)
}

fn application_status(
    state: &mut SupervisorState,
    app_id: &str,
    bundle_root: Digest,
) -> Result<RuntimeStatus, RuntimeError> {
    reap_one(state, app_id)?;
    if let Some(managed) = state.children.get(app_id) {
        return if managed.bundle_root == bundle_root {
            Ok(RuntimeStatus::Running(managed.child.id()))
        } else {
            Err(RuntimeError::DifferentGenerationRunning)
        };
    }
    Ok(match state.terminal.get(app_id) {
        Some(terminal) if terminal.bundle_root == bundle_root && terminal.failed => {
            RuntimeStatus::Failed
        }
        _ => RuntimeStatus::Stopped,
    })
}

fn managed_processes(state: &mut SupervisorState) -> Result<BTreeMap<u32, String>, RuntimeError> {
    let app_ids: Vec<String> = state.children.keys().cloned().collect();
    for app_id in app_ids {
        reap_one(state, &app_id)?;
    }
    Ok(state
        .children
        .iter()
        .map(|(app_id, managed)| (managed.child.id(), app_id.clone()))
        .collect())
}

fn stop_application(
    state: &mut SupervisorState,
    app_id: &str,
    timeout: Duration,
) -> Result<(), RuntimeError> {
    state.terminal.remove(app_id);
    if let Some(mut managed) = state.children.remove(app_id) {
        terminate(&mut managed.child, timeout)?;
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("application supervisor state is unavailable")]
    Unavailable,
    #[error("another application generation is still running")]
    DifferentGenerationRunning,
    #[error("application filesystem setup failed: {0}")]
    Filesystem(#[source] std::io::Error),
    #[error("application process could not be started: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("application process status could not be read: {0}")]
    Status(#[source] std::io::Error),
    #[error("application process could not be stopped: {0}")]
    Stop(#[source] std::io::Error),
}

fn reap_one(state: &mut SupervisorState, app_id: &str) -> Result<(), RuntimeError> {
    let status = state
        .children
        .get_mut(app_id)
        .map(|managed| managed.child.try_wait())
        .transpose()
        .map_err(RuntimeError::Status)?
        .flatten();
    if let Some(status) = status {
        let managed = state
            .children
            .remove(app_id)
            .expect("the reaped child was present");
        state.terminal.insert(
            app_id.to_owned(),
            TerminalApplication {
                bundle_root: managed.bundle_root,
                failed: !status.success(),
            },
        );
    }
    Ok(())
}

fn ensure_plain_directory(path: &Path) -> Result<(), RuntimeError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(RuntimeError::Filesystem(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "application data path is not a plain directory",
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(RuntimeError::Filesystem)
        }
        Err(error) => Err(RuntimeError::Filesystem(error)),
    }
}

fn copy_device_environment(command: &mut Command, name: &str) {
    if let Some(value) = std::env::var_os(name) {
        command.env(name, value);
    }
}

#[cfg(test)]
fn application_command(
    app: &MaterializedApplication,
    _supervisor_executable: &Path,
) -> Result<Command, RuntimeError> {
    Ok(Command::new(&app.main))
}

#[cfg(not(test))]
fn application_command(
    app: &MaterializedApplication,
    supervisor_executable: &Path,
) -> Result<Command, RuntimeError> {
    let mut command = Command::new(supervisor_executable);
    command
        .arg("run-app-supervisor")
        .arg("--entrypoint")
        .arg(&app.main)
        .arg("--stop-timeout-ms")
        .arg(app.process.stop_timeout_ms.to_string());
    if app.process.restart == kindlebridge_bundle::RestartPolicy::OnFailure {
        command.arg("--restart-on-failure");
    }
    Ok(command)
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
const fn configure_process_group(_command: &mut Command) {}

fn terminate(child: &mut Child, timeout: Duration) -> Result<(), RuntimeError> {
    if child.try_wait().map_err(RuntimeError::Status)?.is_some() {
        return Ok(());
    }
    send_terminate(child)?;
    // The production child is a pdeath-aware runner which forwards TERM to a
    // separate application process group. Give it a small window to reap that
    // group after the bundle-declared grace period before killing the runner.
    let deadline = Instant::now() + timeout + Duration::from_millis(500);
    while Instant::now() < deadline {
        if child.try_wait().map_err(RuntimeError::Status)?.is_some() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    send_kill(child)?;
    child.wait().map_err(RuntimeError::Stop)?;
    Ok(())
}

/// Hidden production entry point that keeps an application process group tied
/// to the KindleBridge daemon even when the daemon is killed without unwinding.
#[cfg(target_os = "linux")]
pub fn run_application_supervisor(
    entrypoint: &Path,
    stop_timeout: Duration,
    restart_on_failure: bool,
) -> Result<(), String> {
    use nix::sys::prctl;
    use nix::sys::signal::{killpg, SigSet, Signal};
    use nix::unistd::{getpgrp, getppid, setpgid, Pid};

    // Be the single process-group leader for the runner, application, and all
    // descendants. This lets the outer daemon's last-resort SIGKILL cover the
    // whole tree even if the runner itself is wedged or killed.
    setpgid(Pid::from_raw(0), Pid::from_raw(0)).map_err(|error| error.to_string())?;

    let parent = getppid();
    prctl::set_pdeathsig(Signal::SIGTERM).map_err(|error| error.to_string())?;
    if getppid() != parent || parent == Pid::from_raw(1) {
        return Err("KindleBridge daemon exited before the app runner was ready".to_owned());
    }

    let mut waited = SigSet::empty();
    for signal in [
        Signal::SIGHUP,
        Signal::SIGINT,
        Signal::SIGTERM,
        Signal::SIGCHLD,
    ] {
        waited.add(signal);
    }
    waited.thread_block().map_err(|error| error.to_string())?;

    let mut restart_count = 0_u32;
    loop {
        let mut child_command =
            Command::new(std::env::current_exe().map_err(|error| error.to_string())?);
        child_command
            .arg("exec-app")
            .arg("--entrypoint")
            .arg(entrypoint)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        let mut child = child_command.spawn().map_err(|error| error.to_string())?;
        let started = Instant::now();
        loop {
            let signal = waited.wait().map_err(|error| error.to_string())?;
            if signal == Signal::SIGCHLD {
                let Some(status) = child.try_wait().map_err(|error| error.to_string())? else {
                    continue;
                };
                cleanup_process_group_members(getpgrp())?;
                if status.success() {
                    return Ok(());
                }
                if !restart_on_failure {
                    return Err(format!("application exited with {status}"));
                }
                if started.elapsed() >= RESTART_STABLE_WINDOW {
                    restart_count = 0;
                }
                if restart_count >= MAX_CONSECUTIVE_RESTARTS {
                    return Err(format!(
                        "application exited with {status}; restart budget exhausted after {restart_count} attempts"
                    ));
                }
                let backoff = RESTART_BACKOFFS[restart_count as usize];
                restart_count += 1;
                eprintln!(
                    "kindlebridged: application {} exited with {status}; restart {restart_count}/{MAX_CONSECUTIVE_RESTARTS} in {} ms",
                    entrypoint.display(),
                    backoff.as_millis()
                );
                thread::sleep(backoff);
                break;
            }

            let application_group = getpgrp();
            let _ = killpg(application_group, Signal::SIGTERM);
            let deadline = Instant::now() + stop_timeout;
            while Instant::now() < deadline {
                if child
                    .try_wait()
                    .map_err(|error| error.to_string())?
                    .is_some()
                {
                    cleanup_process_group_members(application_group)?;
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(20));
            }
            let _ = killpg(application_group, Signal::SIGKILL);
            child.wait().map_err(|error| error.to_string())?;
            return Ok(());
        }
    }
}

#[cfg(target_os = "linux")]
fn cleanup_process_group_members(group: nix::unistd::Pid) -> Result<(), String> {
    use nix::errno::Errno;
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::{getpid, Pid};

    let own_pid = getpid();
    for entry in fs::read_dir("/proc").map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<i32>().ok())
            .map(Pid::from_raw)
            .filter(|pid| *pid != own_pid)
        else {
            continue;
        };
        let stat = match fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error.to_string()),
        };
        let Some(fields) = stat.rsplit_once(") ").map(|(_, fields)| fields) else {
            continue;
        };
        let process_group = fields
            .split_whitespace()
            .nth(2)
            .and_then(|value| value.parse::<i32>().ok());
        if process_group != Some(group.as_raw()) {
            continue;
        }
        match kill(pid, Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn run_application_supervisor(
    entrypoint: &Path,
    _stop_timeout: Duration,
    _restart_on_failure: bool,
) -> Result<(), String> {
    let status = Command::new(entrypoint)
        .status()
        .map_err(|error| error.to_string())?;
    status
        .success()
        .then_some(())
        .ok_or_else(|| format!("application exited with {status}"))
}

/// Hidden runner child which restores the normal signal mask and atomically
/// replaces itself with the signed application entrypoint.
#[cfg(target_os = "linux")]
pub fn exec_application(entrypoint: &Path) -> Result<(), String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    use nix::sys::prctl;
    use nix::sys::signal::{SigSet, Signal};
    use nix::unistd::{execv, getppid, Pid};

    let parent = getppid();
    prctl::set_pdeathsig(Signal::SIGKILL).map_err(|error| error.to_string())?;
    if getppid() != parent || parent == Pid::from_raw(1) {
        return Err("application runner exited before exec was ready".to_owned());
    }

    SigSet::all()
        .thread_unblock()
        .map_err(|error| error.to_string())?;
    let executable = CString::new(entrypoint.as_os_str().as_bytes())
        .map_err(|_| "application entrypoint contains NUL".to_owned())?;
    execv(&executable, &[&executable])
        .map(|never| match never {})
        .map_err(|error| error.to_string())
}

#[cfg(all(unix, not(target_os = "linux")))]
pub fn exec_application(entrypoint: &Path) -> Result<(), String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    use nix::sys::signal::SigSet;
    use nix::unistd::execv;

    SigSet::all()
        .thread_unblock()
        .map_err(|error| error.to_string())?;
    let executable = CString::new(entrypoint.as_os_str().as_bytes())
        .map_err(|_| "application entrypoint contains NUL".to_owned())?;
    execv(&executable, &[&executable])
        .map(|never| match never {})
        .map_err(|error| error.to_string())
}

#[cfg(not(unix))]
pub fn exec_application(entrypoint: &Path) -> Result<(), String> {
    let status = Command::new(entrypoint)
        .status()
        .map_err(|error| error.to_string())?;
    status
        .success()
        .then_some(())
        .ok_or_else(|| format!("application exited with {status}"))
}

#[cfg(unix)]
fn send_terminate(child: &mut Child) -> Result<(), RuntimeError> {
    signal_process_group(child.id(), nix::sys::signal::Signal::SIGTERM)
}

#[cfg(not(unix))]
fn send_terminate(child: &mut Child) -> Result<(), RuntimeError> {
    child.kill().map_err(RuntimeError::Stop)
}

#[cfg(unix)]
fn send_kill(child: &mut Child) -> Result<(), RuntimeError> {
    signal_process_group(child.id(), nix::sys::signal::Signal::SIGKILL)
}

#[cfg(not(unix))]
fn send_kill(child: &mut Child) -> Result<(), RuntimeError> {
    child.kill().map_err(RuntimeError::Stop)
}

#[cfg(unix)]
fn signal_process_group(pid: u32, signal: nix::sys::signal::Signal) -> Result<(), RuntimeError> {
    use nix::errno::Errno;
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;

    let pid = i32::try_from(pid).map_err(|_| {
        RuntimeError::Stop(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "application PID exceeds i32",
        ))
    })?;
    match killpg(Pid::from_raw(pid), signal) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(error) => Err(RuntimeError::Stop(std::io::Error::from_raw_os_error(
            error as i32,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restarts_only_failed_processes_when_requested() {
        let mut app = AppInstance::new(
            "org.example.reader",
            "dev",
            "generation",
            RestartPolicy::OnFailure,
        )
        .unwrap();
        app.request_start().unwrap();
        app.mark_started().unwrap();
        assert!(app.mark_exited(false).unwrap());
        assert_eq!(app.state, AppState::Crashed);
        assert_eq!(app.restart_count, 1);
    }

    #[test]
    fn invalid_state_transitions_are_rejected() {
        let mut app = AppInstance::new(
            "org.example.reader",
            "stable",
            "generation",
            RestartPolicy::Never,
        )
        .unwrap();
        assert!(matches!(
            app.request_stop(),
            Err(AppError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn ids_are_deliberately_conservative() {
        assert!(AppInstance::new(
            "org.Example.reader",
            "dev",
            "generation",
            RestartPolicy::Never
        )
        .is_err());
        assert!(AppInstance::new(
            "org.example.reader",
            "feature/foo",
            "generation",
            RestartPolicy::Never
        )
        .is_err());
    }
}
