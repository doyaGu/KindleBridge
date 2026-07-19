//! Bounded, argv-preserving non-interactive command execution.

use std::io::{self, Read};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use kindlebridge_schema::{ExecParams, ExecResult};
use thiserror::Error;

pub const MAX_EXEC_TIMEOUT_MS: u64 = 10 * 60 * 1000;
pub const MAX_EXEC_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Debug, Error)]
pub enum ExecError {
    #[error("argv must contain a command")]
    EmptyArgv,
    #[error("timeout must be between 1 and {MAX_EXEC_TIMEOUT_MS} milliseconds")]
    InvalidTimeout,
    #[error("could not start process: {0}")]
    Spawn(#[source] io::Error),
    #[error("could not capture process output")]
    MissingPipe,
    #[error("could not wait for process: {0}")]
    Wait(#[source] io::Error),
    #[error("process exceeded its {0} millisecond timeout")]
    Timeout(u64),
    #[error("stdout or stderr exceeded {MAX_EXEC_OUTPUT_BYTES} bytes")]
    OutputLimit,
    #[error("output reader thread failed")]
    ReaderPanicked,
    #[error("could not read process output: {0}")]
    ReadOutput(#[source] io::Error),
}

#[derive(Debug)]
struct CapturedOutput {
    bytes: Vec<u8>,
    exceeded: bool,
}

pub fn run(params: &ExecParams) -> Result<ExecResult, ExecError> {
    let command_name = params.argv.first().ok_or(ExecError::EmptyArgv)?;
    if !(1..=MAX_EXEC_TIMEOUT_MS).contains(&params.timeout_ms) {
        return Err(ExecError::InvalidTimeout);
    }

    let mut command = Command::new(command_name);
    command
        .args(&params.argv[1..])
        .envs(&params.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(cwd) = &params.cwd {
        command.current_dir(cwd);
    }

    let started = Instant::now();
    let mut child = command.spawn().map_err(ExecError::Spawn)?;
    let stdout = child.stdout.take().ok_or(ExecError::MissingPipe)?;
    let stderr = child.stderr.take().ok_or(ExecError::MissingPipe)?;
    let stdout_reader = thread::spawn(move || read_bounded(stdout));
    let stderr_reader = thread::spawn(move || read_bounded(stderr));

    let deadline = started + Duration::from_millis(params.timeout_ms);
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(ExecError::Wait)? {
            break status;
        }
        if Instant::now() >= deadline {
            timed_out = true;
            child.kill().map_err(ExecError::Wait)?;
            break child.wait().map_err(ExecError::Wait)?;
        }
        thread::sleep(POLL_INTERVAL);
    };

    let stdout = join_reader(stdout_reader)?;
    let stderr = join_reader(stderr_reader)?;
    if timed_out {
        return Err(ExecError::Timeout(params.timeout_ms));
    }
    if stdout.exceeded || stderr.exceeded {
        return Err(ExecError::OutputLimit);
    }

    Ok(ExecResult {
        exit_code: exit_code(status),
        stdout: String::from_utf8_lossy(&stdout.bytes).into_owned(),
        stderr: String::from_utf8_lossy(&stderr.bytes).into_owned(),
        duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
}

fn read_bounded(mut reader: impl Read) -> io::Result<CapturedOutput> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    let mut exceeded = false;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let remaining = MAX_EXEC_OUTPUT_BYTES.saturating_sub(bytes.len());
        let retained = count.min(remaining);
        bytes.extend_from_slice(&buffer[..retained]);
        exceeded |= retained != count;
    }
    Ok(CapturedOutput { bytes, exceeded })
}

fn join_reader(
    reader: thread::JoinHandle<io::Result<CapturedOutput>>,
) -> Result<CapturedOutput, ExecError> {
    reader
        .join()
        .map_err(|_| ExecError::ReaderPanicked)?
        .map_err(ExecError::ReadOutput)
}

fn exit_code(status: ExitStatus) -> i32 {
    status.code().unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn empty_argv_and_unbounded_timeout_are_rejected_before_spawn() {
        let mut params = ExecParams {
            serial: "TEST".to_owned(),
            argv: Vec::new(),
            cwd: None,
            environment: BTreeMap::new(),
            timeout_ms: 1,
        };
        assert!(matches!(run(&params), Err(ExecError::EmptyArgv)));
        params.argv.push("unused".to_owned());
        params.timeout_ms = MAX_EXEC_TIMEOUT_MS + 1;
        assert!(matches!(run(&params), Err(ExecError::InvalidTimeout)));
    }

    #[test]
    fn executes_argv_without_shell_reparsing() {
        let executable = std::env::current_exe().unwrap();
        let params = ExecParams {
            serial: "TEST".to_owned(),
            argv: vec![
                executable.to_string_lossy().into_owned(),
                "--list".to_owned(),
            ],
            cwd: None,
            environment: BTreeMap::new(),
            timeout_ms: 10_000,
        };
        let result = run(&params).unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result
            .stdout
            .contains("executes_argv_without_shell_reparsing"));
    }
}
