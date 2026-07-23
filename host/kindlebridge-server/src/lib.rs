//! `KindleBridge` host RPC server.

mod client_runtime;
mod device_registry;
mod device_session;
mod host_rpc;
mod runtime;
mod sync_operation;

#[cfg(test)]
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex, MutexGuard};
#[cfg(test)]
use std::thread;

#[cfg(test)]
use base64::engine::general_purpose::STANDARD as BASE64;
#[cfg(test)]
use base64::Engine;
use kindlebridge_schema::shell_protocol::ShellPacket;
use kindlebridge_schema::{
    error_codes, parse_request_value, read_frame, write_json_frame, AppInstallParams, AppList,
    AppLogParams, AppLogSnapshot, AppSummary, AppTargetParams, DeviceFeatures, DeviceSummary,
    ExecParams, ExecResult, FramingError, LogSnapshot, LogTailParams, ProcessList,
    ProcessSignalParams, ProcessSummary, RequestId, RpcError, RpcResponse, SerialParams,
    ShellOpenParams, SyncListParams, SyncListResult, SyncMkdirParams, SyncMkdirResult,
    SyncProgressPhase, SyncPullParams, SyncPullResult, SyncPushParams, SyncPushResult, SyncStatus,
    SyncStatusParams, DEFAULT_MAX_CONTENT_LENGTH,
};
#[cfg(test)]
use kindlebridge_schema::{methods, ShellOpenResult};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::json;
use serde_json::Value;
use thiserror::Error;

#[cfg(test)]
use client_runtime::{handle_shell_open, handle_stream_notification, ClientRuntime};
use runtime::RuntimeState;

pub use device_registry::DeviceRegistry;
pub use device_session::{ConnectedDeviceProvider, DeviceShell, DeviceShellEvent};
pub use host_rpc::{reset_server_stop_requested, server_stop_requested};
pub use sync_operation::HostSyncOperation;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRecord {
    #[serde(flatten)]
    pub summary: DeviceSummary,
    pub protocol_version: u32,
    #[serde(default)]
    pub features: Vec<String>,
}

impl DeviceRecord {
    #[must_use]
    pub fn features(&self) -> DeviceFeatures {
        let mut features = self.features.clone();
        features.sort();
        features.dedup();
        DeviceFeatures {
            serial: self.summary.serial.clone(),
            protocol_version: self.protocol_version,
            features,
        }
    }
}

pub trait ShellStream: Send + Sync {
    fn send(&self, packet: ShellPacket) -> Result<(), ProviderError>;
    fn recv(&self) -> Result<DeviceShellEvent, ProviderError>;
    fn close(&self) -> Result<(), ProviderError>;
}

impl ShellStream for DeviceShell {
    fn send(&self, packet: ShellPacket) -> Result<(), ProviderError> {
        DeviceShell::send(self, packet)
    }

    fn recv(&self) -> Result<DeviceShellEvent, ProviderError> {
        DeviceShell::recv(self)
    }

    fn close(&self) -> Result<(), ProviderError> {
        DeviceShell::close(self)
    }
}

pub trait DeviceProvider: Send + Sync {
    fn perform(&self, operation: DeviceOperation) -> Result<DeviceOperationResult, RpcError>;

    fn sync_push_with_operation(
        &self,
        params: SyncPushParams,
        operation: &HostSyncOperation,
    ) -> Result<SyncPushResult, RpcError> {
        let result = self
            .perform(DeviceOperation::SyncPush(params))?
            .into_sync_push()?;
        operation.transferred(result.accepted_offset);
        Ok(result)
    }

    fn sync_pull_with_operation(
        &self,
        params: SyncPullParams,
        operation: &HostSyncOperation,
    ) -> Result<SyncPullResult, RpcError> {
        let result = self
            .perform(DeviceOperation::SyncPull(params))?
            .into_sync_pull()?;
        operation.transferred(result.received_size);
        Ok(result)
    }

    fn shell_open(&self, params: &ShellOpenParams) -> Result<Arc<dyn ShellStream>, RpcError> {
        Err(RpcError::feature_unavailable(&params.serial, "shell.v2"))
    }
}

pub enum DeviceOperation {
    List,
    Features(String),
    Ping(String),
    Exec(ExecParams),
    SyncPush(SyncPushParams),
    SyncPull(SyncPullParams),
    SyncStatus(SyncStatusParams),
    SyncList(SyncListParams),
    SyncMkdir(SyncMkdirParams),
    AppInstall(AppInstallParams),
    AppStart(AppTargetParams),
    AppStop(AppTargetParams),
    AppRestart(AppTargetParams),
    AppRollback(AppTargetParams),
    AppUninstall(AppTargetParams),
    AppList(SerialParams),
    AppLog(AppLogParams),
    ProcessList(SerialParams),
    ProcessSignal(ProcessSignalParams),
    LogTail(LogTailParams),
}

pub enum DeviceOperationResult {
    Devices(Vec<DeviceSummary>),
    Features(Option<DeviceFeatures>),
    Ping(bool),
    Exec(Option<ExecResult>),
    SyncPush(SyncPushResult),
    SyncPull(SyncPullResult),
    SyncStatus(SyncStatus),
    SyncList(SyncListResult),
    SyncMkdir(SyncMkdirResult),
    App(AppSummary),
    Apps(AppList),
    AppLog(AppLogSnapshot),
    Processes(ProcessList),
    Process(ProcessSummary),
    Log(LogSnapshot),
}

impl DeviceOperationResult {
    fn into_sync_push(self) -> Result<SyncPushResult, RpcError> {
        match self {
            Self::SyncPush(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn into_sync_pull(self) -> Result<SyncPullResult, RpcError> {
        match self {
            Self::SyncPull(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }
}

impl dyn DeviceProvider + '_ {
    fn list(&self) -> Result<Vec<DeviceSummary>, RpcError> {
        match self.perform(DeviceOperation::List)? {
            DeviceOperationResult::Devices(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn features(&self, serial: &str) -> Result<Option<DeviceFeatures>, RpcError> {
        match self.perform(DeviceOperation::Features(serial.to_owned()))? {
            DeviceOperationResult::Features(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn ping(&self, serial: &str) -> Result<bool, RpcError> {
        match self.perform(DeviceOperation::Ping(serial.to_owned()))? {
            DeviceOperationResult::Ping(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn exec(&self, params: &ExecParams) -> Result<Option<ExecResult>, RpcError> {
        match self.perform(DeviceOperation::Exec(params.clone()))? {
            DeviceOperationResult::Exec(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn sync_push(&self, params: SyncPushParams) -> Result<SyncPushResult, RpcError> {
        self.perform(DeviceOperation::SyncPush(params))?
            .into_sync_push()
    }

    fn sync_pull(&self, params: SyncPullParams) -> Result<SyncPullResult, RpcError> {
        self.perform(DeviceOperation::SyncPull(params))?
            .into_sync_pull()
    }

    fn sync_status(&self, params: &SyncStatusParams) -> Result<SyncStatus, RpcError> {
        match self.perform(DeviceOperation::SyncStatus(params.clone()))? {
            DeviceOperationResult::SyncStatus(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn sync_list(&self, params: &SyncListParams) -> Result<SyncListResult, RpcError> {
        match self.perform(DeviceOperation::SyncList(params.clone()))? {
            DeviceOperationResult::SyncList(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn sync_mkdir(&self, params: &SyncMkdirParams) -> Result<SyncMkdirResult, RpcError> {
        match self.perform(DeviceOperation::SyncMkdir(params.clone()))? {
            DeviceOperationResult::SyncMkdir(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn app_start(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.app_result(DeviceOperation::AppStart(params.clone()))
    }

    fn app_install(&self, params: AppInstallParams) -> Result<AppSummary, RpcError> {
        self.app_result(DeviceOperation::AppInstall(params))
    }

    fn app_stop(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.app_result(DeviceOperation::AppStop(params.clone()))
    }

    fn app_restart(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.app_result(DeviceOperation::AppRestart(params.clone()))
    }

    fn app_rollback(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.app_result(DeviceOperation::AppRollback(params.clone()))
    }

    fn app_uninstall(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.app_result(DeviceOperation::AppUninstall(params.clone()))
    }

    fn app_result(&self, operation: DeviceOperation) -> Result<AppSummary, RpcError> {
        match self.perform(operation)? {
            DeviceOperationResult::App(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn app_list(&self, params: &SerialParams) -> Result<AppList, RpcError> {
        match self.perform(DeviceOperation::AppList(params.clone()))? {
            DeviceOperationResult::Apps(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn app_log(&self, params: &AppLogParams) -> Result<AppLogSnapshot, RpcError> {
        match self.perform(DeviceOperation::AppLog(params.clone()))? {
            DeviceOperationResult::AppLog(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn process_list(&self, params: &SerialParams) -> Result<ProcessList, RpcError> {
        match self.perform(DeviceOperation::ProcessList(params.clone()))? {
            DeviceOperationResult::Processes(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn process_signal(&self, params: &ProcessSignalParams) -> Result<ProcessSummary, RpcError> {
        match self.perform(DeviceOperation::ProcessSignal(params.clone()))? {
            DeviceOperationResult::Process(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }

    fn log_tail(&self, params: &LogTailParams) -> Result<LogSnapshot, RpcError> {
        match self.perform(DeviceOperation::LogTail(params.clone()))? {
            DeviceOperationResult::Log(result) => Ok(result),
            _ => Err(RpcError::internal_error()),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryDeviceProvider {
    devices: Vec<DeviceRecord>,
    runtime: Arc<Mutex<RuntimeState>>,
}

impl MemoryDeviceProvider {
    #[must_use]
    pub fn new(devices: Vec<DeviceRecord>) -> Self {
        let mut runtime = RuntimeState::default();
        for device in &devices {
            runtime.device_connected(&device.summary.serial);
        }
        Self {
            devices,
            runtime: Arc::new(Mutex::new(runtime)),
        }
    }

    #[must_use]
    pub fn records(&self) -> &[DeviceRecord] {
        &self.devices
    }

    fn ensure_device(&self, serial: &str) -> Result<(), RpcError> {
        if self
            .devices
            .iter()
            .any(|device| device.summary.serial == serial)
        {
            Ok(())
        } else {
            Err(RpcError::device_not_found(serial))
        }
    }

    fn runtime(&self) -> Result<MutexGuard<'_, RuntimeState>, RpcError> {
        self.runtime.lock().map_err(|_| RpcError::internal_error())
    }
}

impl MemoryDeviceProvider {
    fn list(&self) -> Result<Vec<DeviceSummary>, ProviderError> {
        let mut devices: Vec<_> = self
            .devices
            .iter()
            .map(|record| record.summary.clone())
            .collect();
        devices.sort_by(|left, right| left.serial.cmp(&right.serial));
        Ok(devices)
    }

    fn features(&self, serial: &str) -> Result<Option<DeviceFeatures>, ProviderError> {
        Ok(self
            .devices
            .iter()
            .find(|record| record.summary.serial == serial)
            .map(DeviceRecord::features))
    }

    fn ping(&self, serial: &str) -> Result<bool, RpcError> {
        Ok(self
            .devices
            .iter()
            .any(|device| device.summary.serial == serial))
    }

    fn exec(&self, params: &ExecParams) -> Result<Option<ExecResult>, RpcError> {
        let Some(record) = self
            .devices
            .iter()
            .find(|record| record.summary.serial == params.serial)
        else {
            return Ok(None);
        };
        if !record.features.iter().any(|feature| feature == "exec.v1") {
            return Err(RpcError::feature_unavailable(&params.serial, "exec.v1"));
        }

        let (exit_code, stdout, stderr) = match params.argv.first().map(String::as_str) {
            Some("echo") => (
                0,
                format!("{}\n", params.argv[1..].join(" ")),
                String::new(),
            ),
            Some("false") => (1, String::new(), String::new()),
            Some(command) => (
                0,
                format!("fake exec: {command} {}\n", params.argv[1..].join(" ")),
                String::new(),
            ),
            None => return Err(RpcError::invalid_params("argv is empty")),
        };
        Ok(Some(ExecResult {
            exit_code,
            stdout,
            stderr,
            duration_ms: 0,
        }))
    }

    fn sync_push(&self, params: SyncPushParams) -> Result<SyncPushResult, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.sync_push(params)
    }

    fn sync_push_with_operation(
        &self,
        params: SyncPushParams,
        operation: &HostSyncOperation,
    ) -> Result<SyncPushResult, RpcError> {
        let total = std::fs::metadata(&params.local_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        operation.phase(SyncProgressPhase::Hashing, 0, total);
        if operation.is_cancelled() {
            return Err(RpcError::new(
                error_codes::TRANSFER_CANCELLED,
                "Transfer cancelled",
            ));
        }
        operation.transferred(total);
        operation.phase(SyncProgressPhase::Transferring, 0, total);
        let result = self.sync_push(params)?;
        operation.transferred(result.accepted_offset);
        operation.transfer_id(result.transfer_id.clone());
        Ok(result)
    }

    fn sync_pull(&self, params: SyncPullParams) -> Result<SyncPullResult, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.sync_pull(params)
    }

    fn sync_pull_with_operation(
        &self,
        params: SyncPullParams,
        operation: &HostSyncOperation,
    ) -> Result<SyncPullResult, RpcError> {
        operation.phase(SyncProgressPhase::Transferring, 0, 0);
        if operation.is_cancelled() {
            return Err(RpcError::new(
                error_codes::TRANSFER_CANCELLED,
                "Transfer cancelled",
            ));
        }
        let result = self.sync_pull(params)?;
        operation.transfer_id(result.transfer_id.clone());
        operation.phase(
            SyncProgressPhase::Transferring,
            result.received_size,
            result.total_size,
        );
        Ok(result)
    }

    fn sync_status(&self, params: &SyncStatusParams) -> Result<SyncStatus, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.sync_status(params)
    }

    fn sync_list(&self, params: &SyncListParams) -> Result<SyncListResult, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.sync_list(params)
    }

    fn sync_mkdir(&self, params: &SyncMkdirParams) -> Result<SyncMkdirResult, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.sync_mkdir(params)
    }

    fn app_install(&self, params: AppInstallParams) -> Result<AppSummary, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.app_install(params)
    }

    fn app_start(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.app_start(params)
    }

    fn app_stop(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.app_stop(params)
    }

    fn app_restart(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.app_restart(params)
    }

    fn app_rollback(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.app_rollback(params)
    }

    fn app_uninstall(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.app_uninstall(params)
    }

    fn app_list(&self, params: &SerialParams) -> Result<AppList, RpcError> {
        self.ensure_device(&params.serial)?;
        Ok(self.runtime()?.app_list(params))
    }

    fn app_log(&self, params: &AppLogParams) -> Result<AppLogSnapshot, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.app_log(params)
    }

    fn process_list(&self, params: &SerialParams) -> Result<ProcessList, RpcError> {
        self.ensure_device(&params.serial)?;
        Ok(self.runtime()?.process_list(params))
    }

    fn process_signal(&self, params: &ProcessSignalParams) -> Result<ProcessSummary, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.process_signal(params)
    }

    fn log_tail(&self, params: &LogTailParams) -> Result<LogSnapshot, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.log_tail(params)
    }
}

impl DeviceProvider for MemoryDeviceProvider {
    fn perform(&self, operation: DeviceOperation) -> Result<DeviceOperationResult, RpcError> {
        Ok(match operation {
            DeviceOperation::List => {
                DeviceOperationResult::Devices(self.list().map_err(provider_rpc_error)?)
            }
            DeviceOperation::Features(serial) => {
                DeviceOperationResult::Features(self.features(&serial).map_err(provider_rpc_error)?)
            }
            DeviceOperation::Ping(serial) => DeviceOperationResult::Ping(self.ping(&serial)?),
            DeviceOperation::Exec(params) => DeviceOperationResult::Exec(self.exec(&params)?),
            DeviceOperation::SyncPush(params) => {
                DeviceOperationResult::SyncPush(self.sync_push(params)?)
            }
            DeviceOperation::SyncPull(params) => {
                DeviceOperationResult::SyncPull(self.sync_pull(params)?)
            }
            DeviceOperation::SyncStatus(params) => {
                DeviceOperationResult::SyncStatus(self.sync_status(&params)?)
            }
            DeviceOperation::SyncList(params) => {
                DeviceOperationResult::SyncList(self.sync_list(&params)?)
            }
            DeviceOperation::SyncMkdir(params) => {
                DeviceOperationResult::SyncMkdir(self.sync_mkdir(&params)?)
            }
            DeviceOperation::AppInstall(params) => {
                DeviceOperationResult::App(self.app_install(params)?)
            }
            DeviceOperation::AppStart(params) => {
                DeviceOperationResult::App(self.app_start(&params)?)
            }
            DeviceOperation::AppStop(params) => DeviceOperationResult::App(self.app_stop(&params)?),
            DeviceOperation::AppRestart(params) => {
                DeviceOperationResult::App(self.app_restart(&params)?)
            }
            DeviceOperation::AppRollback(params) => {
                DeviceOperationResult::App(self.app_rollback(&params)?)
            }
            DeviceOperation::AppUninstall(params) => {
                DeviceOperationResult::App(self.app_uninstall(&params)?)
            }
            DeviceOperation::AppList(params) => {
                DeviceOperationResult::Apps(self.app_list(&params)?)
            }
            DeviceOperation::AppLog(params) => {
                DeviceOperationResult::AppLog(self.app_log(&params)?)
            }
            DeviceOperation::ProcessList(params) => {
                DeviceOperationResult::Processes(self.process_list(&params)?)
            }
            DeviceOperation::ProcessSignal(params) => {
                DeviceOperationResult::Process(self.process_signal(&params)?)
            }
            DeviceOperation::LogTail(params) => DeviceOperationResult::Log(self.log_tail(&params)?),
        })
    }

    fn sync_push_with_operation(
        &self,
        params: SyncPushParams,
        operation: &HostSyncOperation,
    ) -> Result<SyncPushResult, RpcError> {
        MemoryDeviceProvider::sync_push_with_operation(self, params, operation)
    }

    fn sync_pull_with_operation(
        &self,
        params: SyncPullParams,
        operation: &HostSyncOperation,
    ) -> Result<SyncPullResult, RpcError> {
        MemoryDeviceProvider::sync_pull_with_operation(self, params, operation)
    }
}

#[derive(Debug, Error)]
#[error("device provider failed: {message}")]
pub struct ProviderError {
    message: String,
    public_message: Option<String>,
}

impl ProviderError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            public_message: None,
        }
    }

    #[must_use]
    pub fn public(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            message: message.clone(),
            public_message: Some(message),
        }
    }
}

#[derive(Debug, Error)]
pub enum ServeError {
    #[error(transparent)]
    Framing(#[from] FramingError),
}

pub(crate) fn provider_rpc_error(error: ProviderError) -> RpcError {
    // Most provider details can contain host paths or transport internals. Only errors
    // explicitly constructed as public are safe and actionable at the CLI boundary.
    RpcError::new(
        error_codes::SERVER_NOT_READY,
        error
            .public_message
            .unwrap_or_else(|| "Server not ready".to_owned()),
    )
}

/// Serves framed JSON-RPC messages until clean EOF or a framing error.
pub fn serve<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut W,
    provider: &dyn DeviceProvider,
) -> Result<(), ServeError> {
    loop {
        let Some(payload) = read_frame(reader, DEFAULT_MAX_CONTENT_LENGTH)? else {
            return Ok(());
        };

        let Ok(value) = serde_json::from_slice::<Value>(&payload) else {
            write_json_frame(
                writer,
                &RpcResponse::failure(RequestId::Null, RpcError::parse_error()),
            )?;
            continue;
        };

        match parse_request_value(value) {
            Ok(request) => {
                if let Some(response) = host_rpc::handle(request, provider) {
                    write_json_frame(writer, &response)?;
                }
            }
            Err(error) => {
                write_json_frame(writer, &RpcResponse::failure(RequestId::Null, error))?;
            }
        }
    }
}

/// Serves the full duplex JSON-RPC API, including asynchronous shell stream
/// notifications. Each invocation owns one client's stream registry; dropping
/// the client deterministically closes every shell it opened.
pub fn serve_streaming<R, W>(
    reader: &mut R,
    writer: W,
    registry: Arc<DeviceRegistry>,
) -> Result<(), ServeError>
where
    R: BufRead,
    W: Write + Send + 'static,
{
    client_runtime::serve_streaming(reader, writer, registry)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs;
    use std::io::{self, BufReader, Cursor};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use kindlebridge_schema::{
        read_json_frame, write_json_frame, DeviceState, RpcRequest, DEFAULT_MAX_CONTENT_LENGTH,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct SharedOutput {
        bytes: Arc<Mutex<Vec<u8>>>,
        frames: Arc<AtomicUsize>,
    }

    impl Write for SharedOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.frames.fetch_add(1, Ordering::Release);
            Ok(())
        }
    }

    struct FakeShell {
        sent: Mutex<Vec<ShellPacket>>,
        events: Mutex<VecDeque<DeviceShellEvent>>,
        closed: AtomicBool,
    }

    impl FakeShell {
        fn new(events: impl IntoIterator<Item = DeviceShellEvent>) -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
                events: Mutex::new(events.into_iter().collect()),
                closed: AtomicBool::new(false),
            }
        }
    }

    impl ShellStream for FakeShell {
        fn send(&self, packet: ShellPacket) -> Result<(), ProviderError> {
            self.sent.lock().unwrap().push(packet);
            Ok(())
        }

        fn recv(&self) -> Result<DeviceShellEvent, ProviderError> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(DeviceShellEvent::Closed))
        }

        fn close(&self) -> Result<(), ProviderError> {
            self.closed.store(true, Ordering::Release);
            Ok(())
        }
    }

    struct ShellProvider {
        shell: Arc<FakeShell>,
    }

    impl DeviceProvider for ShellProvider {
        fn perform(&self, _operation: DeviceOperation) -> Result<DeviceOperationResult, RpcError> {
            Err(RpcError::internal_error())
        }

        fn shell_open(&self, _params: &ShellOpenParams) -> Result<Arc<dyn ShellStream>, RpcError> {
            Ok(self.shell.clone())
        }
    }

    fn shell_open_request() -> RpcRequest {
        RpcRequest::call(
            RequestId::Number(9),
            methods::SHELL_OPEN,
            Some(
                serde_json::to_value(ShellOpenParams {
                    serial: "KT6-TEST".to_owned(),
                    open: kindlebridge_schema::device_protocol::ShellOpen::interactive(
                        kindlebridge_schema::device_protocol::TerminalSize {
                            rows: 24,
                            columns: 80,
                            pixel_width: 0,
                            pixel_height: 0,
                        },
                    ),
                })
                .unwrap(),
            ),
        )
    }

    fn framed_values(output: &SharedOutput) -> Vec<Value> {
        let bytes = output.bytes.lock().unwrap().clone();
        let mut reader = BufReader::new(Cursor::new(bytes));
        let mut values = Vec::new();
        while let Some(value) = read_json_frame(&mut reader, DEFAULT_MAX_CONTENT_LENGTH).unwrap() {
            values.push(value);
        }
        values
    }

    fn provider() -> MemoryDeviceProvider {
        MemoryDeviceProvider::new(vec![DeviceRecord {
            summary: DeviceSummary {
                serial: "KT6-TEST".to_owned(),
                model: "KT6".to_owned(),
                state: DeviceState::Online,
                transport: "usb".to_owned(),
            },
            protocol_version: kindlebridge_schema::device_protocol::PROTOCOL_VERSION,
            features: vec!["sync.v1".to_owned(), "exec.v1".to_owned()],
        }])
    }

    fn one_call(request: &RpcRequest) -> RpcResponse {
        let mut input = Vec::new();
        write_json_frame(&mut input, request).unwrap();
        let mut output = Vec::new();
        serve(
            &mut BufReader::new(Cursor::new(input)),
            &mut output,
            &provider(),
        )
        .unwrap();
        read_json_frame(
            &mut BufReader::new(Cursor::new(output)),
            DEFAULT_MAX_CONTENT_LENGTH,
        )
        .unwrap()
        .map(|value| serde_json::from_value(value).unwrap())
        .unwrap()
    }

    #[test]
    fn ping_is_public_rpc() {
        let response = one_call(&RpcRequest::call(
            RequestId::Number(1),
            methods::SERVER_PING,
            None,
        ));
        assert_eq!(response.into_result().unwrap(), json!({ "ok": true }));
    }

    #[test]
    fn device_ping_requires_a_known_device() {
        let response = one_call(&RpcRequest::call(
            RequestId::Number(2),
            methods::DEVICE_PING,
            Some(json!({ "serial": "KT6-TEST" })),
        ));
        assert_eq!(response.into_result().unwrap(), json!({ "ok": true }));

        let missing = one_call(&RpcRequest::call(
            RequestId::Number(3),
            methods::DEVICE_PING,
            Some(json!({ "serial": "missing" })),
        ));
        assert_eq!(
            missing.error.expect("missing device must fail").code,
            error_codes::DEVICE_NOT_FOUND
        );
    }

    #[test]
    fn provider_errors_are_private_unless_marked_for_the_cli() {
        let private = provider_rpc_error(ProviderError::new("C:\\private\\transport-detail"));
        assert_eq!(private.code, error_codes::SERVER_NOT_READY);
        assert_eq!(private.message, "Server not ready");

        let public = provider_rpc_error(ProviderError::public(
            "Incompatible KindleBridge daemon protocol 2; host requires 3",
        ));
        assert_eq!(public.code, error_codes::SERVER_NOT_READY);
        assert_eq!(
            public.message,
            "Incompatible KindleBridge daemon protocol 2; host requires 3"
        );
    }

    #[test]
    fn shell_open_streams_data_exit_and_close_notifications() {
        let shell = Arc::new(FakeShell::new([
            DeviceShellEvent::Packet(ShellPacket::Stdout(b"out\0".to_vec())),
            DeviceShellEvent::Packet(ShellPacket::Stderr(b"err\0".to_vec())),
            DeviceShellEvent::Packet(ShellPacket::Exit(
                kindlebridge_schema::shell_protocol::ShellExit {
                    exit_code: 37,
                    signal: 0,
                },
            )),
            DeviceShellEvent::Closed,
        ]));
        let registry = Arc::new(DeviceRegistry::direct(Arc::new(ShellProvider {
            shell: Arc::clone(&shell),
        })));
        let output = SharedOutput::default();
        let writer = Arc::new(Mutex::new(output.clone()));
        let streams = Arc::new(Mutex::new(HashMap::new()));

        handle_shell_open(shell_open_request(), &writer, &registry, &streams).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while output.frames.load(Ordering::Acquire) < 5 && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(streams.lock().unwrap().is_empty());
        assert_eq!(output.frames.load(Ordering::Acquire), 5);

        let values = framed_values(&output);
        assert_eq!(values.len(), 5);
        let response: RpcResponse = serde_json::from_value(values[0].clone()).unwrap();
        let result: ShellOpenResult =
            serde_json::from_value(response.into_result().unwrap()).unwrap();
        assert_eq!(result.stream_id.len(), 32);
        assert_eq!(result.send_credit, 256 * 1024);

        let methods: Vec<_> = values[1..]
            .iter()
            .map(|value| value["method"].as_str().unwrap())
            .collect();
        assert_eq!(
            methods,
            [
                methods::STREAM_DATA,
                methods::STREAM_DATA,
                methods::STREAM_EXIT,
                methods::STREAM_CLOSED,
            ]
        );
        assert_eq!(values[1]["params"]["channel"], "stdout");
        assert_eq!(values[1]["params"]["data"], BASE64.encode(b"out\0"));
        assert_eq!(values[2]["params"]["channel"], "stderr");
        assert_eq!(values[3]["params"]["exit_code"], 37);
        assert!(values[4]["params"]["reason"].is_null());
    }

    #[test]
    fn sync_stream_emits_progress_before_its_final_response() {
        let source =
            std::env::temp_dir().join(format!("kindlebridge-progress-{}.bin", std::process::id()));
        fs::write(&source, b"progress payload").unwrap();
        let registry = Arc::new(DeviceRegistry::direct(Arc::new(provider())));
        let output = SharedOutput::default();
        let runtime = ClientRuntime::new(output.clone(), registry);
        let request = RpcRequest::call(
            RequestId::Number(12),
            methods::SYNC_PUSH_STREAM,
            Some(
                serde_json::to_value(SyncPushParams {
                    serial: "KT6-TEST".to_owned(),
                    local_path: source.to_string_lossy().into_owned(),
                    remote_path: "progress.bin".to_owned(),
                    transfer_id: None,
                    block_size: kindlebridge_schema::DEFAULT_SYNC_BLOCK_SIZE,
                })
                .unwrap(),
            ),
        );

        runtime.open_sync(request).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while (!runtime.is_idle() || output.frames.load(Ordering::Acquire) < 2)
            && Instant::now() < deadline
        {
            thread::yield_now();
        }

        let values = framed_values(&output);
        assert!(values.len() >= 2);
        assert!(values[..values.len() - 1]
            .iter()
            .all(|value| value["method"] == methods::SYNC_PROGRESS));
        assert_eq!(
            values[values.len() - 2]["params"]["transferred"],
            b"progress payload".len()
        );
        let response: RpcResponse = serde_json::from_value(values.last().unwrap().clone()).unwrap();
        let result: SyncPushResult =
            serde_json::from_value(response.into_result().unwrap()).unwrap();
        assert_eq!(result.accepted_offset, b"progress payload".len() as u64);
        let _ = fs::remove_file(source);
    }

    #[test]
    fn client_runtime_shutdown_is_idempotent_and_clears_all_work() {
        let shell = Arc::new(FakeShell::new([]));
        let first = Arc::new(HostSyncOperation::default());
        let second = Arc::new(HostSyncOperation::default());
        let runtime = ClientRuntime::new(
            Vec::<u8>::new(),
            Arc::new(DeviceRegistry::direct(Arc::new(provider()))),
        );
        let shell_stream: Arc<dyn ShellStream> = shell.clone();
        runtime.track_shell("shell".to_owned(), shell_stream);
        runtime.track_sync_operation("first".to_owned(), Arc::clone(&first));
        runtime.track_sync_operation("second".to_owned(), Arc::clone(&second));

        runtime.shutdown();
        runtime.shutdown();

        assert!(shell.closed.load(Ordering::Acquire));
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert!(runtime.is_idle());
    }

    #[test]
    fn dropping_client_runtime_closes_shells_and_cancels_sync_operations() {
        let shell = Arc::new(FakeShell::new([]));
        let operation = Arc::new(HostSyncOperation::default());
        {
            let runtime = ClientRuntime::new(
                Vec::<u8>::new(),
                Arc::new(DeviceRegistry::direct(Arc::new(provider()))),
            );
            let shell_stream: Arc<dyn ShellStream> = shell.clone();
            runtime.track_shell("shell".to_owned(), shell_stream);
            runtime.track_sync_operation("sync".to_owned(), Arc::clone(&operation));
        }

        assert!(shell.closed.load(Ordering::Acquire));
        assert!(operation.is_cancelled());
    }

    #[test]
    fn shell_input_notifications_return_credit_and_close_only_the_target_stream() {
        let shell = Arc::new(FakeShell::new([]));
        let shell_trait: Arc<dyn ShellStream> = shell.clone();
        let streams = Arc::new(Mutex::new(HashMap::from([(
            "stream-a".to_owned(),
            shell_trait,
        )])));
        let output = SharedOutput::default();
        let writer = Arc::new(Mutex::new(output.clone()));

        let notifications = [
            RpcRequest::notification(
                methods::STREAM_WRITE,
                Some(json!({ "stream_id": "stream-a", "data": BASE64.encode(b"input\0") })),
            ),
            RpcRequest::notification(
                methods::STREAM_RESIZE,
                Some(json!({
                    "stream_id": "stream-a",
                    "rows": 41, "columns": 119, "pixel_width": 0, "pixel_height": 0
                })),
            ),
            RpcRequest::notification(
                methods::STREAM_CLOSE_INPUT,
                Some(json!({ "stream_id": "stream-a" })),
            ),
        ];
        for notification in notifications {
            handle_stream_notification(notification, &writer, &streams).unwrap();
        }

        assert_eq!(
            *shell.sent.lock().unwrap(),
            [
                ShellPacket::Stdin(b"input\0".to_vec()),
                ShellPacket::Resize(kindlebridge_schema::device_protocol::TerminalSize {
                    rows: 41,
                    columns: 119,
                    pixel_width: 0,
                    pixel_height: 0,
                }),
                ShellPacket::CloseStdin,
            ]
        );
        let values = framed_values(&output);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["method"], methods::STREAM_CREDIT);
        assert_eq!(values[0]["params"]["bytes"], 6);

        handle_stream_notification(
            RpcRequest::notification(
                methods::STREAM_CLOSE,
                Some(json!({ "stream_id": "stream-a" })),
            ),
            &writer,
            &streams,
        )
        .unwrap();
        assert!(streams.lock().unwrap().is_empty());
        assert!(shell.closed.load(Ordering::Acquire));
        let values = framed_values(&output);
        assert_eq!(values[1]["method"], methods::STREAM_CLOSED);
        assert_eq!(values[1]["params"]["reason"], "closed by client");
    }

    #[test]
    fn missing_device_has_stable_error_code() {
        let response = one_call(&RpcRequest::call(
            RequestId::Number(2),
            methods::DEVICE_FEATURES,
            Some(json!({ "serial": "missing" })),
        ));
        assert_eq!(
            response.error.unwrap().code,
            kindlebridge_schema::error_codes::DEVICE_NOT_FOUND
        );
    }

    #[test]
    fn malformed_json_produces_parse_error() {
        let mut input = Vec::new();
        kindlebridge_schema::write_frame(&mut input, b"{").unwrap();
        let mut output = Vec::new();
        serve(
            &mut BufReader::new(Cursor::new(input)),
            &mut output,
            &provider(),
        )
        .unwrap();
        let value = read_json_frame(
            &mut BufReader::new(Cursor::new(output)),
            DEFAULT_MAX_CONTENT_LENGTH,
        )
        .unwrap()
        .unwrap();
        let response: RpcResponse = serde_json::from_value(value).unwrap();
        assert_eq!(response.error.unwrap().code, error_codes::PARSE_ERROR);
    }

    #[test]
    fn exec_is_a_typed_public_rpc_method() {
        let response = one_call(&RpcRequest::call(
            RequestId::Number(3),
            methods::EXEC_RUN,
            Some(json!({
                "serial": "KT6-TEST",
                "argv": ["echo", "hello", "kindle"],
                "environment": {},
                "timeout_ms": 1000
            })),
        ));
        let result: ExecResult = serde_json::from_value(response.into_result().unwrap()).unwrap();
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout, "hello kindle\n");
    }

    #[test]
    fn path_based_push_retry_is_idempotent_and_detects_source_changes() {
        let provider = provider();
        let source = std::env::temp_dir().join(format!(
            "kindlebridge-memory-provider-{}.bin",
            std::process::id()
        ));
        fs::write(&source, b"abcdef").unwrap();
        let first = SyncPushParams {
            serial: "KT6-TEST".to_owned(),
            local_path: source.to_string_lossy().into_owned(),
            remote_path: "resume.bin".to_owned(),
            transfer_id: None,
            block_size: 65_536,
        };
        let accepted = provider.sync_push(first.clone()).unwrap();
        let mut retry = first;
        retry.transfer_id = Some(accepted.transfer_id.clone());
        assert_eq!(
            provider.sync_push(retry).unwrap().accepted_offset,
            accepted.accepted_offset
        );

        fs::write(&source, b"changed").unwrap();
        let error = provider
            .sync_push(SyncPushParams {
                serial: "KT6-TEST".to_owned(),
                local_path: source.to_string_lossy().into_owned(),
                remote_path: "resume.bin".to_owned(),
                transfer_id: Some(accepted.transfer_id),
                block_size: 65_536,
            })
            .unwrap_err();
        assert_eq!(error.code, error_codes::INVALID_STATE);
        fs::remove_file(source).unwrap();
    }

    #[test]
    fn failed_rollback_preserves_the_installed_app() {
        let provider = provider();
        let bundle = std::env::temp_dir().join(format!(
            "kindlebridge-memory-app-{}.kbb",
            std::process::id()
        ));
        let mut config = kindlebridge_bundle::BuildConfig::new(
            kindlebridge_bundle::BundleKind::Application,
            "org.example.app",
            "1.0.0",
            1,
            "kindlehf",
        );
        config.entrypoints =
            std::collections::BTreeMap::from([("main".to_owned(), "bin/app".to_owned())]);
        let mut builder = kindlebridge_bundle::BundleBuilder::new(config);
        builder.add_file("bin/app", b"app".to_vec(), true).unwrap();
        fs::write(
            &bundle,
            builder
                .build(&ed25519_dalek::SigningKey::from_bytes(&[9; 32]))
                .unwrap(),
        )
        .unwrap();
        provider
            .app_install(AppInstallParams {
                serial: "KT6-TEST".to_owned(),
                bundle_path: bundle.to_string_lossy().into_owned(),
            })
            .unwrap();
        let error = provider
            .app_rollback(&AppTargetParams {
                serial: "KT6-TEST".to_owned(),
                app_id: "org.example.app".to_owned(),
            })
            .unwrap_err();
        assert_eq!(error.code, error_codes::NO_ROLLBACK_AVAILABLE);
        assert_eq!(
            provider
                .app_list(&SerialParams {
                    serial: "KT6-TEST".to_owned()
                })
                .unwrap()
                .apps
                .len(),
            1
        );
        fs::remove_file(bundle).unwrap();
    }
}
