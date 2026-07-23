//! Device operating-system inspection services exposed by RPC.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use kindlebridge_schema::{
    error_codes, LogEntry, LogSnapshot, LogTailParams, ProcessList, ProcessSignal, ProcessState,
    ProcessSummary, RpcError,
};

const MAX_LOG_WINDOW_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LOG_SNAPSHOT: u32 = 1000;
const MAX_PROCESS_NAME_BYTES: u64 = 4096;
const MAX_PROCESS_COUNT: usize = 65_536;

pub fn process_list(proc_root: &Path) -> Result<ProcessList, RpcError> {
    let entries = match fs::read_dir(proc_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ProcessList {
                processes: Vec::new(),
            });
        }
        Err(error) => return Err(device_read_error("read process table", proc_root, &error)),
    };
    let mut processes = BTreeMap::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(device_read_error("read process entry", proc_root, &error)),
        };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|pid| *pid != 0)
        else {
            continue;
        };
        let comm = entry.path().join("comm");
        let Some(name) = read_process_name(&comm)? else {
            continue;
        };
        processes.insert(
            pid,
            ProcessSummary {
                pid,
                name,
                app_id: None,
                state: ProcessState::Running,
            },
        );
        if processes.len() > MAX_PROCESS_COUNT {
            return Err(invalid_device_state(
                "process table exceeds the device limit",
            ));
        }
    }
    Ok(ProcessList {
        processes: processes.into_values().collect(),
    })
}

pub fn process_signal(
    proc_root: &Path,
    pid: u32,
    requested_signal: &str,
) -> Result<ProcessSummary, RpcError> {
    process_signal_with(proc_root, pid, requested_signal, send_system_signal)
}

fn process_signal_with<F>(
    proc_root: &Path,
    pid: u32,
    requested_signal: &str,
    send_signal: F,
) -> Result<ProcessSummary, RpcError>
where
    F: FnOnce(i32, ProcessSignal) -> Result<(), SignalSendError>,
{
    let signal = ProcessSignal::parse(requested_signal).ok_or_else(|| {
        RpcError::new(error_codes::INVALID_SIGNAL, "Invalid signal").with_data(serde_json::json!({
            "signal": requested_signal,
            "detail": "use a Linux signal name such as TERM or SIGKILL, or a number from 1 to 31",
        }))
    })?;
    let raw_pid =
        i32::try_from(pid).map_err(|_| RpcError::invalid_params("pid is out of range"))?;
    if pid == 0 {
        return Err(RpcError::invalid_params(
            "pid 0 denotes a process group and is not accepted",
        ));
    }
    if pid == 1 {
        return Err(RpcError::invalid_params(
            "pid 1 is protected; use the root shell for explicit system control",
        ));
    }
    if pid == std::process::id() {
        return Err(RpcError::invalid_params(
            "kindlebridged cannot signal itself; switch USB mode to stop the bridge",
        ));
    }

    let process = process_summary(proc_root, pid)?.ok_or_else(|| process_not_found(pid))?;
    send_signal(raw_pid, signal).map_err(|error| match error {
        SignalSendError::NotFound => process_not_found(pid),
        SignalSendError::PermissionDenied => {
            process_signal_failed(pid, signal, "permission_denied")
        }
        SignalSendError::Unsupported => process_signal_failed(pid, signal, "unsupported_platform"),
        SignalSendError::Other => process_signal_failed(pid, signal, "operating_system_error"),
    })?;
    Ok(process)
}

fn process_summary(proc_root: &Path, pid: u32) -> Result<Option<ProcessSummary>, RpcError> {
    let Some(name) = read_process_name(&proc_root.join(pid.to_string()).join("comm"))? else {
        return Ok(None);
    };
    Ok(Some(ProcessSummary {
        pid,
        name,
        app_id: None,
        state: ProcessState::Running,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(unix), allow(dead_code))]
enum SignalSendError {
    NotFound,
    PermissionDenied,
    Unsupported,
    Other,
}

#[cfg(unix)]
fn send_system_signal(pid: i32, signal: ProcessSignal) -> Result<(), SignalSendError> {
    use nix::errno::Errno;
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;

    let signal = Signal::try_from(signal.number()).map_err(|_| SignalSendError::Unsupported)?;
    kill(Pid::from_raw(pid), signal).map_err(|error| match error {
        Errno::ESRCH => SignalSendError::NotFound,
        Errno::EPERM => SignalSendError::PermissionDenied,
        _ => SignalSendError::Other,
    })
}

#[cfg(not(unix))]
fn send_system_signal(_pid: i32, _signal: ProcessSignal) -> Result<(), SignalSendError> {
    Err(SignalSendError::Unsupported)
}

pub fn log_tail(log_path: &Path, params: &LogTailParams) -> Result<LogSnapshot, RpcError> {
    let limit = params.limit.unwrap_or(100);
    if limit == 0 || limit > MAX_LOG_SNAPSHOT {
        return Err(RpcError::invalid_params("limit must be between 1 and 1000"));
    }
    let mut file = match File::open(log_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LogSnapshot {
                entries: Vec::new(),
                next_cursor: 0,
                has_more: false,
            });
        }
        Err(error) => return Err(device_read_error("open device log", log_path, &error)),
    };
    let metadata = file
        .metadata()
        .map_err(|error| device_read_error("stat device log", log_path, &error))?;
    if !metadata.is_file() {
        return Err(invalid_device_state("device log is not a regular file"));
    }
    let file_length = metadata.len();
    let window_start = file_length.saturating_sub(MAX_LOG_WINDOW_BYTES);
    file.seek(SeekFrom::Start(window_start))
        .map_err(|error| device_read_error("seek device log", log_path, &error))?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(file_length - window_start).unwrap_or(MAX_LOG_WINDOW_BYTES as usize),
    );
    file.take(MAX_LOG_WINDOW_BYTES)
        .read_to_end(&mut bytes)
        .map_err(|error| device_read_error("read device log", log_path, &error))?;

    let discarded_prefix = if window_start == 0 {
        0
    } else {
        bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(bytes.len(), |index| index + 1)
    };
    let oldest_cursor = window_start.saturating_add(discarded_prefix as u64);
    let requested_cursor = params.cursor;
    if requested_cursor.is_some_and(|cursor| cursor < oldest_cursor || cursor > file_length) {
        return Err(
            RpcError::new(error_codes::LOG_CURSOR_EXPIRED, "Log cursor expired")
                .with_data(serde_json::json!({ "oldest_cursor": oldest_cursor })),
        );
    }

    let lines = log_lines(&bytes[discarded_prefix..], oldest_cursor);
    let published_end = lines.last().map_or(oldest_cursor, |line| line.end);
    if let Some(cursor) = requested_cursor {
        let is_boundary = cursor == published_end || lines.iter().any(|line| line.start == cursor);
        if !is_boundary {
            return Err(RpcError::invalid_params(
                "cursor must identify the beginning of a log entry",
            ));
        }
    }
    let start_index = match requested_cursor {
        Some(cursor) => lines
            .iter()
            .position(|line| line.start >= cursor)
            .unwrap_or(lines.len()),
        None => lines.len().saturating_sub(limit as usize),
    };
    let end_index = (start_index + limit as usize).min(lines.len());
    let entries = lines[start_index..end_index]
        .iter()
        .map(|line| LogEntry {
            cursor: line.start,
            level: "info".to_owned(),
            source: "kindlebridge".to_owned(),
            message: String::from_utf8_lossy(line.message).into_owned(),
        })
        .collect();
    let next_cursor = lines
        .get(end_index.saturating_sub(1))
        .filter(|_| end_index > start_index)
        .map_or_else(
            || requested_cursor.unwrap_or(published_end),
            |line| line.end,
        );
    Ok(LogSnapshot {
        entries,
        next_cursor,
        has_more: end_index < lines.len(),
    })
}

struct LogLine<'a> {
    start: u64,
    end: u64,
    message: &'a [u8],
}

fn log_lines(bytes: &[u8], base: u64) -> Vec<LogLine<'_>> {
    let mut lines = Vec::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        let Some(length) = remaining
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|index| index + 1)
        else {
            break;
        };
        let raw = &remaining[..length];
        let message = raw.strip_suffix(b"\n").unwrap_or(raw);
        let message = message.strip_suffix(b"\r").unwrap_or(message);
        let start = base.saturating_add(offset as u64);
        let end = start.saturating_add(length as u64);
        lines.push(LogLine {
            start,
            end,
            message,
        });
        offset += length;
    }
    lines
}

fn read_process_name(path: &Path) -> Result<Option<String>, RpcError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(device_read_error("open process name", path, &error)),
    };
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_PROCESS_NAME_BYTES)
        .read_to_end(&mut bytes)
        .map_err(|error| device_read_error("read process name", path, &error))?;
    while matches!(bytes.last(), Some(b'\n' | b'\r' | 0)) {
        bytes.pop();
    }
    if bytes.is_empty() {
        return Ok(None);
    }
    Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
}

fn invalid_device_state(detail: impl Into<String>) -> RpcError {
    RpcError::new(error_codes::INVALID_STATE, "Invalid device state")
        .with_data(serde_json::json!({ "detail": detail.into() }))
}

fn device_read_error(operation: &str, path: &Path, error: &std::io::Error) -> RpcError {
    RpcError::new(error_codes::INTERNAL_ERROR, "Device read failed").with_data(serde_json::json!({
        "operation": operation,
        "path": PathBuf::from(path),
        "kind": format!("{:?}", error.kind()),
    }))
}

fn process_not_found(pid: u32) -> RpcError {
    RpcError::new(error_codes::PROCESS_NOT_FOUND, "Process not found")
        .with_data(serde_json::json!({ "pid": pid }))
}

fn process_signal_failed(pid: u32, signal: ProcessSignal, reason: &str) -> RpcError {
    RpcError::new(error_codes::PROCESS_SIGNAL_FAILED, "Process signal failed").with_data(
        serde_json::json!({
            "pid": pid,
            "signal": signal.name(),
            "reason": reason,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "kindlebridge-services-{label}-{}-{id}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn process_list_reads_proc_comm_and_sorts_by_pid() {
        let directory = TestDirectory::new("proc");
        for (pid, name) in [(42, "reader"), (7, "kindlebridged")] {
            fs::create_dir(directory.0.join(pid.to_string())).unwrap();
            fs::write(directory.0.join(pid.to_string()).join("comm"), name).unwrap();
        }
        fs::create_dir(directory.0.join("self")).unwrap();

        let processes = process_list(&directory.0).unwrap();
        assert_eq!(
            processes
                .processes
                .iter()
                .map(|process| (process.pid, process.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(7, "kindlebridged"), (42, "reader")]
        );
    }

    #[test]
    fn process_signal_normalizes_the_name_and_returns_the_observed_process() {
        let directory = TestDirectory::new("process-signal");
        fs::create_dir(directory.0.join("42")).unwrap();
        fs::write(directory.0.join("42/comm"), "reader\n").unwrap();
        let mut delivered = None;

        let process = process_signal_with(&directory.0, 42, "sigterm", |pid, signal| {
            delivered = Some((pid, signal));
            Ok(())
        })
        .unwrap();

        assert_eq!(delivered, Some((42, ProcessSignal::Term)));
        assert_eq!(process.pid, 42);
        assert_eq!(process.name, "reader");
    }

    #[test]
    fn process_signal_reports_invalid_signal_missing_process_and_os_errors() {
        let directory = TestDirectory::new("process-signal-errors");

        let invalid = process_signal_with(&directory.0, 42, "BOGUS", |_, _| Ok(())).unwrap_err();
        assert_eq!(invalid.code, error_codes::INVALID_SIGNAL);

        let missing = process_signal_with(&directory.0, 42, "TERM", |_, _| Ok(())).unwrap_err();
        assert_eq!(missing.code, error_codes::PROCESS_NOT_FOUND);

        fs::create_dir(directory.0.join("42")).unwrap();
        fs::write(directory.0.join("42/comm"), "reader\n").unwrap();
        let disappeared = process_signal_with(&directory.0, 42, "TERM", |_, _| {
            Err(SignalSendError::NotFound)
        })
        .unwrap_err();
        assert_eq!(disappeared.code, error_codes::PROCESS_NOT_FOUND);

        let denied = process_signal_with(&directory.0, 42, "TERM", |_, _| {
            Err(SignalSendError::PermissionDenied)
        })
        .unwrap_err();
        assert_eq!(denied.code, error_codes::PROCESS_SIGNAL_FAILED);
        assert_eq!(denied.data.unwrap()["reason"], "permission_denied");
    }

    #[test]
    fn process_signal_protects_global_and_bridge_processes() {
        let directory = TestDirectory::new("process-signal-protected");
        for pid in [1, std::process::id()] {
            fs::create_dir(directory.0.join(pid.to_string())).unwrap();
            fs::write(
                directory.0.join(pid.to_string()).join("comm"),
                "protected\n",
            )
            .unwrap();
            let error = process_signal_with(&directory.0, pid, "TERM", |_, _| {
                panic!("protected process must not be signalled")
            })
            .unwrap_err();
            assert_eq!(error.code, error_codes::INVALID_PARAMS);
        }
    }

    #[test]
    fn log_tail_uses_byte_cursors_and_reports_pagination() {
        let directory = TestDirectory::new("log");
        let path = directory.0.join("usb.log");
        fs::write(&path, b"one\ntwo\nthree\n").unwrap();

        let first = log_tail(
            &path,
            &LogTailParams {
                serial: "TEST".to_owned(),
                cursor: Some(0),
                limit: Some(2),
            },
        )
        .unwrap();
        assert_eq!(
            first
                .entries
                .iter()
                .map(|entry| (entry.cursor, entry.message.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, "one"), (4, "two")]
        );
        assert_eq!(first.next_cursor, 8);
        assert!(first.has_more);

        let second = log_tail(
            &path,
            &LogTailParams {
                serial: "TEST".to_owned(),
                cursor: Some(first.next_cursor),
                limit: Some(2),
            },
        )
        .unwrap();
        assert_eq!(second.entries[0].message, "three");
        assert_eq!(second.next_cursor, 14);
        assert!(!second.has_more);
    }

    #[test]
    fn log_tail_does_not_publish_an_unterminated_line_or_advance_past_it() {
        let directory = TestDirectory::new("log-partial-line");
        let path = directory.0.join("usb.log");
        fs::write(&path, b"one\npartial").unwrap();

        let first = log_tail(
            &path,
            &LogTailParams {
                serial: "TEST".to_owned(),
                cursor: Some(0),
                limit: Some(10),
            },
        )
        .unwrap();
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].message, "one");
        assert_eq!(first.next_cursor, 4);

        fs::write(&path, b"one\npartial line\n").unwrap();
        let second = log_tail(
            &path,
            &LogTailParams {
                serial: "TEST".to_owned(),
                cursor: Some(first.next_cursor),
                limit: Some(10),
            },
        )
        .unwrap();
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].message, "partial line");
        assert_eq!(second.next_cursor, 17);
    }
}
