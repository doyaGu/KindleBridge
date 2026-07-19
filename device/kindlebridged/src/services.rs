//! Read-only device data sources exposed by the development RPC surface.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use kindlebridge_bundle::{ActivationGeneration, BundleKind, GenerationId};
use kindlebridge_schema::{
    error_codes, AppList, AppState, AppSummary, LogEntry, LogSnapshot, LogTailParams, ProcessList,
    ProcessState, ProcessSummary, RpcError,
};

const MAX_ACTIVATION_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LOG_WINDOW_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LOG_SNAPSHOT: u32 = 1000;
const MAX_PROCESS_NAME_BYTES: u64 = 4096;
const MAX_PROCESS_COUNT: usize = 65_536;

pub fn app_list(activation_root: &Path) -> Result<AppList, RpcError> {
    let Some(active) = load_active_generation(activation_root)? else {
        return Ok(AppList { apps: Vec::new() });
    };
    let previous = active
        .previous_generation
        .map(|id| load_generation(activation_root, id))
        .transpose()?;
    let rollback_ids: BTreeSet<&str> = previous
        .as_ref()
        .into_iter()
        .flat_map(|generation| generation.entries.iter())
        .filter(|entry| entry.kind == BundleKind::Application)
        .map(|entry| entry.id.as_str())
        .collect();

    let mut apps = BTreeMap::new();
    for entry in active
        .entries
        .iter()
        .filter(|entry| entry.kind == BundleKind::Application)
    {
        let summary = AppSummary {
            app_id: entry.id.clone(),
            version: entry.code_version.clone(),
            state: AppState::Unknown,
            rollback_available: rollback_ids.contains(entry.id.as_str()),
            pid: None,
        };
        if apps.insert(entry.id.clone(), summary).is_some() {
            return Err(invalid_device_state(
                "active generation contains multiple application channels for one app_id",
            ));
        }
    }
    Ok(AppList {
        apps: apps.into_values().collect(),
    })
}

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
    if let Some(cursor) = requested_cursor {
        let is_boundary = cursor == file_length || lines.iter().any(|line| line.start == cursor);
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
        .map_or_else(|| requested_cursor.unwrap_or(file_length), |line| line.end);
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
        let length = remaining
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(remaining.len(), |index| index + 1);
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

fn load_active_generation(root: &Path) -> Result<Option<ActivationGeneration>, RpcError> {
    let pointer = root.join("active-generation");
    let bytes = match read_bounded_regular_file(&pointer, 64)? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| invalid_device_state("active generation pointer is not UTF-8"))?;
    let value = text.strip_suffix('\n').unwrap_or(text);
    let id = parse_generation_id(value)?;
    load_generation(root, id).map(Some)
}

fn load_generation(root: &Path, id: GenerationId) -> Result<ActivationGeneration, RpcError> {
    let path = root
        .join("generations")
        .join(generation_id_hex(id))
        .join("activation.cbor");
    let bytes = read_bounded_regular_file(&path, MAX_ACTIVATION_BYTES)?
        .ok_or_else(|| invalid_device_state("activation generation is missing"))?;
    let generation = ActivationGeneration::from_cbor(&bytes)
        .map_err(|_| invalid_device_state("activation generation is invalid"))?;
    if generation.generation_id != id {
        return Err(invalid_device_state(
            "activation generation identity does not match its directory",
        ));
    }
    Ok(generation)
}

fn read_bounded_regular_file(path: &Path, limit: u64) -> Result<Option<Vec<u8>>, RpcError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(device_read_error("stat device state", path, &error)),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > limit {
        return Err(invalid_device_state(
            "device state file is unsafe or exceeds its size limit",
        ));
    }
    let mut file =
        File::open(path).map_err(|error| device_read_error("open device state", path, &error))?;
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| device_read_error("read device state", path, &error))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        return Err(invalid_device_state(
            "device state file is unsafe or exceeds its size limit",
        ));
    }
    Ok(Some(bytes))
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

fn parse_generation_id(value: &str) -> Result<GenerationId, RpcError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(invalid_device_state("active generation pointer is invalid"));
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair)
            .map_err(|_| invalid_device_state("active generation pointer is invalid"))?;
        bytes[index] = u8::from_str_radix(pair, 16)
            .map_err(|_| invalid_device_state("active generation pointer is invalid"))?;
    }
    Ok(GenerationId(bytes))
}

fn generation_id_hex(id: GenerationId) -> String {
    let mut output = String::with_capacity(32);
    for byte in id.0 {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn invalid_device_state(detail: &'static str) -> RpcError {
    RpcError::new(error_codes::INVALID_STATE, "Invalid device state")
        .with_data(serde_json::json!({ "detail": detail }))
}

fn device_read_error(operation: &str, path: &Path, error: &std::io::Error) -> RpcError {
    RpcError::new(error_codes::INTERNAL_ERROR, "Device read failed").with_data(serde_json::json!({
        "operation": operation,
        "path": PathBuf::from(path),
        "kind": format!("{:?}", error.kind()),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use kindlebridge_bundle::{ActivationEntry, Digest};

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

    fn generation(id: u8, previous: Option<GenerationId>, version: &str) -> ActivationGeneration {
        ActivationGeneration {
            schema: 1,
            generation_id: GenerationId([id; 16]),
            previous_generation: previous,
            profile_id: "kt6-5.17".to_owned(),
            profile_digest: Digest::of(b"profile"),
            entries: vec![ActivationEntry {
                id: "org.example.reader".to_owned(),
                channel: "dev".to_owned(),
                kind: BundleKind::Application,
                bundle_root: Digest::of(version.as_bytes()),
                code_version: version.to_owned(),
                data_generation: None,
                dependency_roots: Vec::new(),
            }],
        }
    }

    fn write_generation(root: &Path, generation: &ActivationGeneration) {
        let directory = root.join("generations").join(generation.directory_name());
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join("activation.cbor"),
            generation.to_cbor().unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn app_list_reads_the_active_activation_and_real_rollback_availability() {
        let directory = TestDirectory::new("apps");
        let previous = generation(1, None, "1.0.0");
        let active = generation(2, Some(previous.generation_id), "2.0.0");
        write_generation(&directory.0, &previous);
        write_generation(&directory.0, &active);
        fs::write(
            directory.0.join("active-generation"),
            format!("{}\n", active.directory_name()),
        )
        .unwrap();

        let apps = app_list(&directory.0).unwrap();
        assert_eq!(apps.apps.len(), 1);
        assert_eq!(apps.apps[0].app_id, "org.example.reader");
        assert_eq!(apps.apps[0].version, "2.0.0");
        assert!(apps.apps[0].rollback_available);
        assert_eq!(apps.apps[0].state, AppState::Unknown);
        assert_eq!(apps.apps[0].pid, None);
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
}
