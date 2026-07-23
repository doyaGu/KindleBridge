use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::fs::File;
use std::path::Path;

use kindlebridge_bundle::{verify, BundleKind, VerifyOptions};
use kindlebridge_schema::{
    error_codes, AppInstallParams, AppList, AppLogChunk, AppLogParams, AppLogSnapshot, AppState,
    AppSummary, AppTargetParams, LogEntry, LogSnapshot, LogTailParams, ProcessList, ProcessSignal,
    ProcessSignalParams, ProcessState, ProcessSummary, RpcError, SerialParams, SyncEntry,
    SyncEntryKind, SyncListParams, SyncListResult, SyncMkdirParams, SyncMkdirResult,
    SyncPullParams, SyncPullResult, SyncPushParams, SyncPushResult, SyncStatus, SyncStatusParams,
    TransferDirection, TransferState,
};
use serde_json::json;

const MAX_LOG_ENTRIES: usize = 1024;
const MAX_LOG_SNAPSHOT: u32 = 1000;

#[derive(Debug)]
struct Transfer {
    serial: String,
    direction: TransferDirection,
    remote_path: String,
    total_size: u64,
    next_offset: u64,
    state: TransferState,
    data: Vec<u8>,
}

#[derive(Debug)]
struct AppVersion {
    version: String,
    bundle_root: String,
}

#[derive(Debug)]
struct AppRuntime {
    version: String,
    bundle_root: String,
    history: Vec<AppVersion>,
    state: AppState,
    pid: Option<u32>,
}

impl AppRuntime {
    fn summary(&self, app_id: String) -> AppSummary {
        AppSummary {
            app_id,
            version: self.version.clone(),
            state: self.state.clone(),
            rollback_available: !self.history.is_empty(),
            pid: self.pid,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RuntimeState {
    next_transfer: u64,
    next_pid: u32,
    files: BTreeMap<(String, String), Vec<u8>>,
    directories: BTreeSet<(String, String)>,
    transfers: BTreeMap<String, Transfer>,
    apps: BTreeMap<(String, String), AppRuntime>,
    processes: BTreeMap<(String, u32), ProcessSummary>,
    logs: BTreeMap<String, VecDeque<LogEntry>>,
    next_log_cursor: BTreeMap<String, u64>,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            next_transfer: 1,
            next_pid: 2000,
            files: BTreeMap::new(),
            directories: BTreeSet::new(),
            transfers: BTreeMap::new(),
            apps: BTreeMap::new(),
            processes: BTreeMap::new(),
            logs: BTreeMap::new(),
            next_log_cursor: BTreeMap::new(),
        }
    }
}

impl RuntimeState {
    pub(crate) fn device_connected(&mut self, serial: &str) {
        self.log(serial, "info", "bridge", "device connected");
    }

    pub(crate) fn sync_push(&mut self, params: SyncPushParams) -> Result<SyncPushResult, RpcError> {
        validate_path(&params.remote_path)?;
        validate_host_path(&params.local_path)?;
        validate_block_size(params.block_size)?;
        let data = fs::read(&params.local_path)
            .map_err(|error| host_file_error("read", &params.local_path, &error))?;
        let total_size = u64::try_from(data.len()).map_err(|_| RpcError::internal_error())?;

        let transfer_id = if let Some(transfer_id) = params.transfer_id.clone() {
            transfer_id
        } else {
            let transfer_id = self.allocate_transfer_id("push");
            self.transfers.insert(
                transfer_id.clone(),
                Transfer {
                    serial: params.serial.clone(),
                    direction: TransferDirection::Push,
                    remote_path: params.remote_path.clone(),
                    total_size,
                    next_offset: total_size,
                    state: TransferState::Complete,
                    data: data.clone(),
                },
            );
            transfer_id
        };
        let transfer = self
            .transfers
            .get(&transfer_id)
            .ok_or_else(|| transfer_not_found(&transfer_id))?;
        validate_transfer(
            transfer,
            &params.serial,
            &params.remote_path,
            total_size,
            TransferDirection::Push,
        )?;
        if transfer.data != data {
            return Err(invalid_state("resume source does not match accepted data"));
        }
        if self
            .directories
            .contains(&(params.serial.clone(), params.remote_path.clone()))
        {
            return Err(invalid_state("remote path is a directory"));
        }
        self.files
            .insert((params.serial.clone(), params.remote_path.clone()), data);
        add_parent_directories(&mut self.directories, &params.serial, &params.remote_path);
        self.log(
            &params.serial,
            "info",
            "sync",
            &format!("pushed {total_size} bytes to {}", params.remote_path),
        );
        let transfer = &self.transfers[&transfer_id];
        Ok(SyncPushResult {
            transfer_id,
            accepted_offset: transfer.next_offset,
            state: transfer.state.clone(),
        })
    }

    pub(crate) fn sync_pull(&mut self, params: SyncPullParams) -> Result<SyncPullResult, RpcError> {
        validate_path(&params.remote_path)?;
        validate_host_path(&params.local_path)?;
        validate_block_size(params.block_size)?;
        let transfer_id = if let Some(transfer_id) = params.transfer_id.clone() {
            transfer_id
        } else {
            let data = self
                .files
                .get(&(params.serial.clone(), params.remote_path.clone()))
                .cloned()
                .ok_or_else(|| file_not_found(&params.remote_path))?;
            let total_size = u64::try_from(data.len()).map_err(|_| RpcError::internal_error())?;
            let transfer_id = self.allocate_transfer_id("pull");
            self.transfers.insert(
                transfer_id.clone(),
                Transfer {
                    serial: params.serial.clone(),
                    direction: TransferDirection::Pull,
                    remote_path: params.remote_path.clone(),
                    total_size,
                    next_offset: total_size,
                    state: TransferState::Complete,
                    data,
                },
            );
            transfer_id
        };

        let transfer = self
            .transfers
            .get(&transfer_id)
            .ok_or_else(|| transfer_not_found(&transfer_id))?;
        validate_transfer(
            transfer,
            &params.serial,
            &params.remote_path,
            transfer.total_size,
            TransferDirection::Pull,
        )?;
        write_host_file(&params.local_path, &transfer.data)?;
        Ok(SyncPullResult {
            transfer_id,
            total_size: transfer.total_size,
            received_size: transfer.total_size,
            state: transfer.state.clone(),
        })
    }

    pub(crate) fn sync_status(&self, params: &SyncStatusParams) -> Result<SyncStatus, RpcError> {
        let transfer = self
            .transfers
            .get(&params.transfer_id)
            .ok_or_else(|| transfer_not_found(&params.transfer_id))?;
        if transfer.serial != params.serial {
            return Err(transfer_not_found(&params.transfer_id));
        }
        Ok(SyncStatus {
            transfer_id: params.transfer_id.clone(),
            direction: transfer.direction.clone(),
            remote_path: transfer.remote_path.clone(),
            next_offset: transfer.next_offset,
            total_size: transfer.total_size,
            state: transfer.state.clone(),
        })
    }

    pub(crate) fn sync_mkdir(
        &mut self,
        params: &SyncMkdirParams,
    ) -> Result<SyncMkdirResult, RpcError> {
        validate_path(&params.remote_path)?;
        if self
            .files
            .contains_key(&(params.serial.clone(), params.remote_path.clone()))
        {
            return Err(invalid_state("remote path is a file"));
        }
        add_parent_directories(&mut self.directories, &params.serial, &params.remote_path);
        let created = self
            .directories
            .insert((params.serial.clone(), params.remote_path.clone()));
        Ok(SyncMkdirResult {
            remote_path: params.remote_path.clone(),
            created,
        })
    }

    pub(crate) fn sync_list(&self, params: &SyncListParams) -> Result<SyncListResult, RpcError> {
        validate_path(&params.remote_path)?;
        if params.limit == 0 || params.limit > 1024 {
            return Err(RpcError::invalid_params(
                "directory list limit must be between 1 and 1024",
            ));
        }
        if !self
            .directories
            .contains(&(params.serial.clone(), params.remote_path.clone()))
        {
            return Err(file_not_found(&params.remote_path));
        }
        let prefix = format!("{}/", params.remote_path);
        let mut entries = BTreeMap::<String, SyncEntry>::new();
        for (serial, path) in &self.directories {
            if serial == &params.serial {
                if let Some(name) = immediate_child(path, &prefix) {
                    entries.entry(name.clone()).or_insert(SyncEntry {
                        name,
                        kind: SyncEntryKind::Directory,
                        size: 0,
                    });
                }
            }
        }
        for ((serial, path), data) in &self.files {
            if serial == &params.serial {
                if let Some(name) = immediate_child(path, &prefix) {
                    let nested = path[prefix.len()..].contains('/');
                    entries.entry(name.clone()).or_insert(SyncEntry {
                        name,
                        kind: if nested {
                            SyncEntryKind::Directory
                        } else {
                            SyncEntryKind::File
                        },
                        size: if nested {
                            0
                        } else {
                            u64::try_from(data.len()).unwrap_or(u64::MAX)
                        },
                    });
                }
            }
        }
        let mut entries = entries
            .into_values()
            .filter(|entry| {
                params
                    .cursor
                    .as_ref()
                    .map_or(true, |cursor| entry.name > *cursor)
            })
            .collect::<Vec<_>>();
        let limit = usize::try_from(params.limit).map_err(|_| RpcError::internal_error())?;
        let next_cursor = (entries.len() > limit).then(|| entries[limit - 1].name.clone());
        entries.truncate(limit);
        Ok(SyncListResult {
            remote_path: params.remote_path.clone(),
            entries,
            next_cursor,
        })
    }

    pub(crate) fn app_install(&mut self, params: AppInstallParams) -> Result<AppSummary, RpcError> {
        validate_host_path(&params.bundle_path)?;
        let mut bundle = File::open(&params.bundle_path)
            .map_err(|error| host_file_error("open", &params.bundle_path, &error))?;
        let verified = verify(&mut bundle, &VerifyOptions::default()).map_err(|error| {
            RpcError::new(
                error_codes::APP_INSTALL_FAILED,
                "Application install failed",
            )
            .with_data(json!({
                "stage": "verify",
                "reason": format!("{:?}", error.code),
                "detail": error.message,
            }))
        })?;
        let envelope = &verified.inspection.envelope;
        if envelope.kind != BundleKind::Application {
            return Err(RpcError::new(
                error_codes::APP_INSTALL_FAILED,
                "Application install failed",
            )
            .with_data(json!({
                "stage": "verify",
                "reason": "bundle_kind",
                "detail": "app install accepts application bundles only",
            })));
        }
        let app_id = envelope.id.clone();
        let version = envelope.version.clone();
        let bundle_root = format!("{:?}", verified.inspection.header.bundle_root);
        let key = (params.serial.clone(), app_id.clone());
        let mut app = self.apps.remove(&key).unwrap_or(AppRuntime {
            version: version.clone(),
            bundle_root: bundle_root.clone(),
            history: Vec::new(),
            state: AppState::Stopped,
            pid: None,
        });
        if app.bundle_root != bundle_root {
            app.history.push(AppVersion {
                version: app.version.clone(),
                bundle_root: app.bundle_root.clone(),
            });
            if let Some(pid) = app.pid.take() {
                self.processes.remove(&(params.serial.clone(), pid));
            }
            app.version = version;
            app.bundle_root = bundle_root;
            app.state = AppState::Stopped;
        }
        let summary = app.summary(app_id.clone());
        self.apps.insert(key, app);
        self.log(
            &params.serial,
            "info",
            &app_id,
            &format!("installed {}", summary.version),
        );
        Ok(summary)
    }

    pub(crate) fn app_start(&mut self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        let key = (params.serial.clone(), params.app_id.clone());
        let mut app = self
            .apps
            .remove(&key)
            .ok_or_else(|| app_not_found(&params.app_id))?;
        if app.state == AppState::Stopped {
            let pid = self.allocate_pid();
            app.pid = Some(pid);
            app.state = AppState::Running;
            self.processes.insert(
                (params.serial.clone(), pid),
                ProcessSummary {
                    pid,
                    name: params.app_id.clone(),
                    app_id: Some(params.app_id.clone()),
                    state: ProcessState::Running,
                },
            );
            self.log(&params.serial, "info", &params.app_id, "started");
        }
        let summary = app.summary(params.app_id.clone());
        self.apps.insert(key, app);
        Ok(summary)
    }

    pub(crate) fn app_stop(&mut self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        let key = (params.serial.clone(), params.app_id.clone());
        let mut app = self
            .apps
            .remove(&key)
            .ok_or_else(|| app_not_found(&params.app_id))?;
        if let Some(pid) = app.pid.take() {
            self.processes.remove(&(params.serial.clone(), pid));
        }
        app.state = AppState::Stopped;
        let summary = app.summary(params.app_id.clone());
        self.apps.insert(key, app);
        self.log(&params.serial, "info", &params.app_id, "stopped");
        Ok(summary)
    }

    pub(crate) fn app_restart(&mut self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.app_stop(params)?;
        self.app_start(params)
    }

    pub(crate) fn app_rollback(
        &mut self,
        params: &AppTargetParams,
    ) -> Result<AppSummary, RpcError> {
        let key = (params.serial.clone(), params.app_id.clone());
        let mut app = self
            .apps
            .remove(&key)
            .ok_or_else(|| app_not_found(&params.app_id))?;
        let Some(previous) = app.history.pop() else {
            self.apps.insert(key, app);
            return Err(stable_error(
                error_codes::NO_ROLLBACK_AVAILABLE,
                "No rollback available",
                json!({ "app_id": params.app_id }),
            ));
        };
        if let Some(pid) = app.pid.take() {
            self.processes.remove(&(params.serial.clone(), pid));
        }
        app.version = previous.version;
        app.bundle_root = previous.bundle_root;
        app.state = AppState::Stopped;
        let summary = app.summary(params.app_id.clone());
        self.apps.insert(key, app);
        self.log(&params.serial, "info", &params.app_id, "rolled back");
        Ok(summary)
    }

    pub(crate) fn app_uninstall(
        &mut self,
        params: &AppTargetParams,
    ) -> Result<AppSummary, RpcError> {
        let key = (params.serial.clone(), params.app_id.clone());
        let app = self
            .apps
            .remove(&key)
            .ok_or_else(|| app_not_found(&params.app_id))?;
        if let Some(pid) = app.pid {
            self.processes.remove(&(params.serial.clone(), pid));
        }
        let summary = app.summary(params.app_id.clone());
        self.log(&params.serial, "info", &params.app_id, "uninstalled");
        Ok(summary)
    }

    pub(crate) fn app_list(&self, params: &SerialParams) -> AppList {
        let apps = self
            .apps
            .iter()
            .filter(|((serial, _), _)| serial == &params.serial)
            .map(|((_, app_id), app)| app.summary(app_id.clone()))
            .collect();
        AppList { apps }
    }

    pub(crate) fn app_log(&self, params: &AppLogParams) -> Result<AppLogSnapshot, RpcError> {
        let app = self
            .apps
            .get(&(params.serial.clone(), params.app_id.clone()))
            .ok_or_else(|| app_not_found(&params.app_id))?;
        let run_id = format!("fake-{}", app.pid.unwrap_or(0));
        let reset = params.run_id.as_deref() != Some(run_id.as_str());
        let stdout_cursor = if reset { 0 } else { params.stdout_cursor };
        let stderr_cursor = if reset { 0 } else { params.stderr_cursor };
        Ok(AppLogSnapshot {
            app_id: params.app_id.clone(),
            run_id,
            reset,
            state: app.state.clone(),
            pid: app.pid,
            stdout: empty_app_log_chunk(stdout_cursor),
            stderr: empty_app_log_chunk(stderr_cursor),
        })
    }

    pub(crate) fn process_list(&self, params: &SerialParams) -> ProcessList {
        let processes = self
            .processes
            .iter()
            .filter(|((serial, _), _)| serial == &params.serial)
            .map(|(_, process)| process.clone())
            .collect();
        ProcessList { processes }
    }

    pub(crate) fn process_signal(
        &mut self,
        params: &ProcessSignalParams,
    ) -> Result<ProcessSummary, RpcError> {
        let Some(signal) = ProcessSignal::parse(&params.signal) else {
            return Err(stable_error(
                error_codes::INVALID_SIGNAL,
                "Invalid signal",
                json!({ "signal": params.signal }),
            ));
        };
        let key = (params.serial.clone(), params.pid);
        let process = self
            .processes
            .get(&key)
            .cloned()
            .ok_or_else(|| process_not_found(params.pid))?;
        if matches!(signal, ProcessSignal::Term | ProcessSignal::Kill) {
            self.processes.remove(&key);
            if let Some(app_id) = &process.app_id {
                if let Some(app) = self.apps.get_mut(&(params.serial.clone(), app_id.clone())) {
                    app.state = AppState::Stopped;
                    app.pid = None;
                }
            }
        }
        self.log(
            &params.serial,
            "info",
            &process.name,
            &format!("signal {} sent to {}", signal.name(), params.pid),
        );
        Ok(process)
    }

    pub(crate) fn log_tail(&self, params: &LogTailParams) -> Result<LogSnapshot, RpcError> {
        let limit = params.limit.unwrap_or(100);
        if limit == 0 || limit > MAX_LOG_SNAPSHOT {
            return Err(RpcError::invalid_params("limit must be between 1 and 1000"));
        }
        let empty = VecDeque::new();
        let logs = self.logs.get(&params.serial).unwrap_or(&empty);
        let first = logs.front().map_or(1, |entry| entry.cursor);
        if params.cursor.is_some_and(|cursor| cursor < first) {
            return Err(stable_error(
                error_codes::LOG_CURSOR_EXPIRED,
                "Log cursor expired",
                json!({ "oldest_cursor": first }),
            ));
        }
        let start = params.cursor.unwrap_or_else(|| {
            logs.back()
                .map_or(1, |entry| entry.cursor.saturating_add(1))
                .saturating_sub(u64::from(limit))
                .max(first)
        });
        let mut matching = logs.iter().filter(|entry| entry.cursor >= start);
        let entries: Vec<_> = matching
            .by_ref()
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .cloned()
            .collect();
        let has_more = matching.next().is_some();
        let next_cursor = entries
            .last()
            .map_or(start, |entry| entry.cursor.saturating_add(1));
        Ok(LogSnapshot {
            entries,
            next_cursor,
            has_more,
        })
    }

    fn allocate_transfer_id(&mut self, prefix: &str) -> String {
        let id = format!("{prefix}-{}", self.next_transfer);
        self.next_transfer = self.next_transfer.saturating_add(1);
        id
    }

    fn allocate_pid(&mut self) -> u32 {
        let pid = self.next_pid;
        self.next_pid = self.next_pid.saturating_add(1);
        pid
    }

    fn log(&mut self, serial: &str, level: &str, source: &str, message: &str) {
        let cursor = self.next_log_cursor.entry(serial.to_owned()).or_insert(1);
        let entry = LogEntry {
            cursor: *cursor,
            level: level.to_owned(),
            source: source.to_owned(),
            message: message.to_owned(),
        };
        *cursor = cursor.saturating_add(1);
        let logs = self.logs.entry(serial.to_owned()).or_default();
        logs.push_back(entry);
        if logs.len() > MAX_LOG_ENTRIES {
            logs.pop_front();
        }
    }
}

fn empty_app_log_chunk(cursor: u64) -> AppLogChunk {
    AppLogChunk {
        cursor,
        next_cursor: cursor,
        data_base64: String::new(),
        capped: false,
    }
}

fn add_parent_directories(directories: &mut BTreeSet<(String, String)>, serial: &str, path: &str) {
    let mut parent = path;
    while let Some((next, _)) = parent.rsplit_once('/') {
        directories.insert((serial.to_owned(), next.to_owned()));
        parent = next;
    }
}

fn immediate_child(path: &str, prefix: &str) -> Option<String> {
    let suffix = path.strip_prefix(prefix)?;
    let name = suffix.split('/').next()?;
    (!name.is_empty()).then(|| name.to_owned())
}

fn validate_transfer(
    transfer: &Transfer,
    serial: &str,
    path: &str,
    total_size: u64,
    direction: TransferDirection,
) -> Result<(), RpcError> {
    if transfer.serial != serial
        || transfer.remote_path != path
        || transfer.total_size != total_size
        || transfer.direction != direction
    {
        return Err(invalid_state("transfer metadata does not match"));
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<(), RpcError> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(RpcError::invalid_params(
            "remote_path must be a normalized relative logical path",
        ));
    }
    Ok(())
}

fn validate_host_path(path: &str) -> Result<(), RpcError> {
    if Path::new(path).is_absolute() {
        Ok(())
    } else {
        Err(RpcError::invalid_params("local_path must be absolute"))
    }
}

fn validate_block_size(block_size: u32) -> Result<(), RpcError> {
    if (1..=kindlebridge_schema::MAX_SYNC_BLOCK_SIZE).contains(&block_size) {
        Ok(())
    } else {
        Err(RpcError::invalid_params(
            "block_size must be between 1 and 1048576",
        ))
    }
}

fn write_host_file(path: &str, data: &[u8]) -> Result<(), RpcError> {
    let path = Path::new(path);
    let parent = path
        .parent()
        .ok_or_else(|| RpcError::invalid_params("local_path has no parent"))?;
    fs::create_dir_all(parent).map_err(|error| host_file_error("create", path, &error))?;
    let temporary = path.with_extension("kindlebridge-part");
    fs::write(&temporary, data).map_err(|error| host_file_error("write", &temporary, &error))?;
    if path.exists() {
        fs::remove_file(path).map_err(|error| host_file_error("replace", path, &error))?;
    }
    fs::rename(&temporary, path).map_err(|error| host_file_error("commit", path, &error))
}

fn host_file_error(operation: &str, path: impl AsRef<Path>, error: &std::io::Error) -> RpcError {
    RpcError::new(error_codes::INVALID_STATE, "Host file operation failed").with_data(json!({
        "operation": operation,
        "path": path.as_ref().to_string_lossy(),
        "kind": format!("{:?}", error.kind())
    }))
}

fn stable_error(code: i64, message: &str, data: serde_json::Value) -> RpcError {
    RpcError::new(code, message).with_data(data)
}

fn invalid_state(detail: &str) -> RpcError {
    stable_error(
        error_codes::INVALID_STATE,
        "Invalid state",
        json!({ "detail": detail }),
    )
}

fn transfer_not_found(transfer_id: &str) -> RpcError {
    stable_error(
        error_codes::TRANSFER_NOT_FOUND,
        "Transfer not found",
        json!({ "transfer_id": transfer_id }),
    )
}

fn file_not_found(path: &str) -> RpcError {
    stable_error(
        error_codes::FILE_NOT_FOUND,
        "File not found",
        json!({ "remote_path": path }),
    )
}

fn app_not_found(app_id: &str) -> RpcError {
    stable_error(
        error_codes::APP_NOT_FOUND,
        "App not found",
        json!({ "app_id": app_id }),
    )
}

fn process_not_found(pid: u32) -> RpcError {
    stable_error(
        error_codes::PROCESS_NOT_FOUND,
        "Process not found",
        json!({ "pid": pid }),
    )
}
