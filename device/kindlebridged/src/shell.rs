//! Persistent PTY and raw-process workers for `shell.v2`.

use std::io::{self, Read, Write};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use kindlebridge_schema::device_protocol::{ShellMode, ShellOpen, TerminalSize};
use kindlebridge_schema::shell_protocol::{ShellExit, MAX_SHELL_PACKET_PAYLOAD};
use portable_pty::{native_pty_system, CommandBuilder, MasterPty, PtySize};
use thiserror::Error;

const SHELL_QUEUE_DEPTH: usize = 16;
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(5);
const HANGUP_GRACE_PERIOD: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub enum ShellWorkerError {
    #[error("shell argv must contain a command")]
    EmptyArgv,
    #[error("a PTY shell requires an initial terminal size")]
    MissingTerminalSize,
    #[error("a raw shell must not include a terminal size")]
    TerminalSizeForRaw,
    #[error("terminal resize is only valid for a PTY shell")]
    ResizeForRaw,
    #[error("could not start shell process: {0}")]
    Spawn(#[source] io::Error),
    #[error("PTY operation failed: {0}")]
    Pty(String),
    #[error("shell process did not expose a required pipe")]
    MissingPipe,
    #[error("shell input packet is {length} bytes; maximum is {maximum}")]
    InputTooLarge { length: usize, maximum: usize },
    #[error("shell worker stopped")]
    WorkerStopped,
    #[error("timed out waiting for shell output")]
    ReceiveTimeout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShellEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(ShellExit),
}

#[derive(Debug)]
enum ShellCommand {
    Write(Vec<u8>),
    CloseInput,
    Resize(TerminalSize),
    Shutdown,
}

#[derive(Debug)]
pub struct ShellWorker {
    mode: ShellMode,
    commands: SyncSender<ShellCommand>,
    events: Receiver<ShellEvent>,
}

impl ShellWorker {
    pub fn spawn(open: ShellOpen) -> Result<Self, ShellWorkerError> {
        validate_open(&open)?;
        match open.mode {
            ShellMode::Raw => spawn_raw(open),
            ShellMode::Pty => spawn_pty(open),
        }
    }

    pub fn write_stdin(&self, bytes: Vec<u8>) -> Result<(), ShellWorkerError> {
        if bytes.len() > MAX_SHELL_PACKET_PAYLOAD {
            return Err(ShellWorkerError::InputTooLarge {
                length: bytes.len(),
                maximum: MAX_SHELL_PACKET_PAYLOAD,
            });
        }
        self.commands
            .send(ShellCommand::Write(bytes))
            .map_err(|_| ShellWorkerError::WorkerStopped)
    }

    pub fn close_input(&self) -> Result<(), ShellWorkerError> {
        self.commands
            .send(ShellCommand::CloseInput)
            .map_err(|_| ShellWorkerError::WorkerStopped)
    }

    pub fn resize(&self, size: TerminalSize) -> Result<(), ShellWorkerError> {
        if self.mode == ShellMode::Raw {
            return Err(ShellWorkerError::ResizeForRaw);
        }
        self.commands
            .send(ShellCommand::Resize(size))
            .map_err(|_| ShellWorkerError::WorkerStopped)
    }

    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<ShellEvent, ShellWorkerError> {
        self.events
            .recv_timeout(timeout)
            .map_err(|error| match error {
                RecvTimeoutError::Timeout => ShellWorkerError::ReceiveTimeout,
                RecvTimeoutError::Disconnected => ShellWorkerError::WorkerStopped,
            })
    }
}

impl Drop for ShellWorker {
    fn drop(&mut self) {
        let _ = self.commands.try_send(ShellCommand::Shutdown);
    }
}

fn validate_open(open: &ShellOpen) -> Result<(), ShellWorkerError> {
    if open.argv.is_empty() {
        return Err(ShellWorkerError::EmptyArgv);
    }
    match (open.mode, open.terminal_size) {
        (ShellMode::Pty, None) => Err(ShellWorkerError::MissingTerminalSize),
        (ShellMode::Raw, Some(_)) => Err(ShellWorkerError::TerminalSizeForRaw),
        _ => Ok(()),
    }
}

fn spawn_raw(open: ShellOpen) -> Result<ShellWorker, ShellWorkerError> {
    let mut command = clean_command(&open);
    configure_process_group(&mut command);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(ShellWorkerError::Spawn)?;
    let stdin = child.stdin.take().ok_or(ShellWorkerError::MissingPipe)?;
    let stdout = child.stdout.take().ok_or(ShellWorkerError::MissingPipe)?;
    let stderr = child.stderr.take().ok_or(ShellWorkerError::MissingPipe)?;

    let (command_tx, command_rx) = mpsc::sync_channel(SHELL_QUEUE_DEPTH);
    let (event_tx, event_rx) = mpsc::sync_channel(SHELL_QUEUE_DEPTH);
    let stdout_tx = event_tx.clone();
    let stdout_reader = thread::spawn(move || pump_output(stdout, stdout_tx, ShellEvent::Stdout));
    let stderr_tx = event_tx.clone();
    let stderr_reader = thread::spawn(move || pump_output(stderr, stderr_tx, ShellEvent::Stderr));
    thread::spawn(move || {
        run_raw_process(
            &mut child,
            stdin,
            command_rx,
            event_tx,
            stdout_reader,
            stderr_reader,
        );
    });

    Ok(ShellWorker {
        mode: ShellMode::Raw,
        commands: command_tx,
        events: event_rx,
    })
}

fn spawn_pty(open: ShellOpen) -> Result<ShellWorker, ShellWorkerError> {
    let size = open
        .terminal_size
        .expect("PTY terminal size validated before spawn");
    let pair = native_pty_system()
        .openpty(pty_size(size))
        .map_err(|error| ShellWorkerError::Pty(error.to_string()))?;
    let mut command = clean_pty_command(&open);
    command.set_controlling_tty(true);
    let child = pair
        .slave
        .spawn_command(command)
        .map_err(|error| ShellWorkerError::Pty(error.to_string()))?;
    drop(pair.slave);
    let reader = pair
        .master
        .try_clone_reader()
        .map_err(|error| ShellWorkerError::Pty(error.to_string()))?;
    let writer = pair
        .master
        .take_writer()
        .map_err(|error| ShellWorkerError::Pty(error.to_string()))?;

    let (command_tx, command_rx) = mpsc::sync_channel(SHELL_QUEUE_DEPTH);
    let (event_tx, event_rx) = mpsc::sync_channel(SHELL_QUEUE_DEPTH);
    let output_tx = event_tx.clone();
    let output_reader = thread::spawn(move || pump_output(reader, output_tx, ShellEvent::Stdout));
    thread::spawn(move || {
        run_pty_process(
            child,
            pair.master,
            writer,
            command_rx,
            event_tx,
            output_reader,
        );
    });

    Ok(ShellWorker {
        mode: ShellMode::Pty,
        commands: command_tx,
        events: event_rx,
    })
}

fn clean_command(open: &ShellOpen) -> Command {
    let mut command = Command::new(&open.argv[0]);
    command
        .args(&open.argv[1..])
        .current_dir(&open.cwd)
        .env_clear();
    for name in [
        "PATH",
        "LANG",
        "LC_ALL",
        "DISPLAY",
        "TMPDIR",
        "SYSTEMROOT",
        "WINDIR",
        "PATHEXT",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command
        .env("HOME", "/tmp/root")
        .env("PWD", &open.cwd)
        .env("SHELL", "/bin/sh")
        .env("TERM", &open.term);
    command
}

fn clean_pty_command(open: &ShellOpen) -> CommandBuilder {
    let mut command = CommandBuilder::new(&open.argv[0]);
    command.args(&open.argv[1..]);
    command.cwd(&open.cwd);
    command.env_clear();
    for name in [
        "PATH",
        "LANG",
        "LC_ALL",
        "DISPLAY",
        "TMPDIR",
        "SYSTEMROOT",
        "WINDIR",
        "PATHEXT",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command.env("HOME", "/tmp/root");
    command.env("PWD", &open.cwd);
    command.env("SHELL", "/bin/sh");
    command.env("TERM", &open.term);
    command
}

fn pump_output(
    mut reader: impl Read,
    events: SyncSender<ShellEvent>,
    event: fn(Vec<u8>) -> ShellEvent,
) {
    let mut buffer = vec![0_u8; MAX_SHELL_PACKET_PAYLOAD];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(count) => {
                if events.send(event(buffer[..count].to_vec())).is_err() {
                    break;
                }
            }
        }
    }
}

fn run_raw_process(
    child: &mut Child,
    stdin: ChildStdin,
    commands: Receiver<ShellCommand>,
    events: SyncSender<ShellEvent>,
    stdout_reader: thread::JoinHandle<()>,
    stderr_reader: thread::JoinHandle<()>,
) {
    let mut stdin = Some(stdin);
    let mut shutdown_deadline = None;
    let status = loop {
        loop {
            match commands.try_recv() {
                Ok(command) => match command {
                    ShellCommand::Write(bytes) => {
                        let Some(pipe) = stdin.as_mut() else {
                            continue;
                        };
                        if pipe.write_all(&bytes).is_err() || pipe.flush().is_err() {
                            stdin = None;
                        }
                    }
                    ShellCommand::CloseInput => stdin = None,
                    ShellCommand::Resize(_) => {}
                    ShellCommand::Shutdown => {
                        stdin = None;
                        begin_raw_shutdown(child, &mut shutdown_deadline);
                    }
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    stdin = None;
                    begin_raw_shutdown(child, &mut shutdown_deadline);
                    break;
                }
            }
        }
        if shutdown_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            force_kill_raw(child);
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(PROCESS_POLL_INTERVAL),
            Err(_) => {
                let _ = child.kill();
                match child.wait() {
                    Ok(status) => break status,
                    Err(_) => return,
                }
            }
        }
    };
    drop(stdin);
    let _ = stdout_reader.join();
    let _ = stderr_reader.join();
    let _ = events.send(ShellEvent::Exit(shell_exit(status)));
}

fn run_pty_process(
    mut child: Box<dyn portable_pty::Child + Send + Sync>,
    master: Box<dyn MasterPty + Send>,
    writer: Box<dyn Write + Send>,
    commands: Receiver<ShellCommand>,
    events: SyncSender<ShellEvent>,
    output_reader: thread::JoinHandle<()>,
) {
    let mut writer = Some(writer);
    let mut shutdown_deadline = None;
    let status = loop {
        loop {
            match commands.try_recv() {
                Ok(command) => match command {
                    ShellCommand::Write(bytes) => {
                        let Some(pipe) = writer.as_mut() else {
                            continue;
                        };
                        if pipe.write_all(&bytes).is_err() || pipe.flush().is_err() {
                            writer = None;
                        }
                    }
                    ShellCommand::CloseInput => writer = None,
                    ShellCommand::Resize(size) => {
                        let _ = master.resize(pty_size(size));
                    }
                    ShellCommand::Shutdown => {
                        writer = None;
                        begin_pty_shutdown(child.as_mut(), &mut shutdown_deadline);
                    }
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    writer = None;
                    begin_pty_shutdown(child.as_mut(), &mut shutdown_deadline);
                    break;
                }
            }
        }
        if shutdown_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            force_kill_pty(child.as_mut());
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(PROCESS_POLL_INTERVAL),
            Err(_) => {
                let _ = child.kill();
                match child.wait() {
                    Ok(status) => break status,
                    Err(_) => return,
                }
            }
        }
    };
    drop(writer);
    drop(master);
    let _ = output_reader.join();
    let _ = events.send(ShellEvent::Exit(pty_shell_exit(&status)));
}

const fn pty_size(size: TerminalSize) -> PtySize {
    PtySize {
        rows: size.rows,
        cols: size.columns,
        pixel_width: size.pixel_width,
        pixel_height: size.pixel_height,
    }
}

fn shell_exit(status: ExitStatus) -> ShellExit {
    ShellExit {
        exit_code: status.code().unwrap_or(-1),
        signal: exit_signal(&status),
    }
}

#[cfg(unix)]
fn exit_signal(status: &ExitStatus) -> u32 {
    use std::os::unix::process::ExitStatusExt;

    status
        .signal()
        .and_then(|value| value.try_into().ok())
        .unwrap_or(0)
}

#[cfg(not(unix))]
const fn exit_signal(_status: &ExitStatus) -> u32 {
    0
}

fn pty_shell_exit(status: &portable_pty::ExitStatus) -> ShellExit {
    let signal = status.signal().map(signal_number).unwrap_or(0);
    ShellExit {
        exit_code: if signal == 0 {
            i32::try_from(status.exit_code()).unwrap_or(-1)
        } else {
            -1
        },
        signal,
    }
}

fn signal_number(name: &str) -> u32 {
    match name {
        "Hangup" | "SIGHUP" | "Signal 1" => 1,
        "Interrupt" | "SIGINT" | "Signal 2" => 2,
        "Quit" | "SIGQUIT" | "Signal 3" => 3,
        "Killed" | "SIGKILL" | "Signal 9" => 9,
        "Terminated" | "SIGTERM" | "Signal 15" => 15,
        _ => 0,
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
const fn configure_process_group(_command: &mut Command) {}

fn begin_raw_shutdown(child: &mut Child, deadline: &mut Option<Instant>) {
    if deadline.is_some() {
        return;
    }
    signal_group(child.id(), ProcessSignal::Hangup);
    *deadline = Some(Instant::now() + HANGUP_GRACE_PERIOD);
}

fn force_kill_raw(child: &mut Child) {
    signal_group(child.id(), ProcessSignal::Kill);
    #[cfg(not(unix))]
    let _ = child.kill();
}

fn begin_pty_shutdown(child: &mut dyn portable_pty::Child, deadline: &mut Option<Instant>) {
    if deadline.is_some() {
        return;
    }
    if let Some(pid) = child.process_id() {
        signal_group(pid, ProcessSignal::Hangup);
    } else {
        let _ = child.kill();
    }
    *deadline = Some(Instant::now() + HANGUP_GRACE_PERIOD);
}

fn force_kill_pty(child: &mut dyn portable_pty::Child) {
    if let Some(pid) = child.process_id() {
        signal_group(pid, ProcessSignal::Kill);
    }
    #[cfg(not(unix))]
    let _ = child.kill();
}

#[derive(Clone, Copy)]
enum ProcessSignal {
    Hangup,
    Kill,
}

#[cfg(unix)]
fn signal_group(pid: u32, signal: ProcessSignal) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    let Ok(pid) = i32::try_from(pid) else {
        return;
    };
    let signal = match signal {
        ProcessSignal::Hangup => Signal::SIGHUP,
        ProcessSignal::Kill => Signal::SIGKILL,
    };
    let _ = killpg(Pid::from_raw(pid), signal);
}

#[cfg(not(unix))]
const fn signal_group(_pid: u32, _signal: ProcessSignal) {}
