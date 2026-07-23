//! `KindleBridge` host RPC server.

mod client_runtime;
mod device_registry;
mod device_session;
mod runtime;

#[cfg(test)]
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
#[cfg(test)]
use std::thread;

#[cfg(test)]
use base64::engine::general_purpose::STANDARD as BASE64;
#[cfg(test)]
use base64::Engine;
use kindlebridge_schema::shell_protocol::ShellPacket;
#[cfg(test)]
use kindlebridge_schema::ShellOpenResult;
use kindlebridge_schema::{
    error_codes, methods, parse_request_value, read_frame, write_json_frame, AppInstallParams,
    AppList, AppLogParams, AppLogSnapshot, AppSummary, AppTargetParams, DeviceFeatures,
    DeviceFeaturesParams, DeviceList, DeviceSummary, ExecParams, ExecResult, FramingError,
    LogSnapshot, LogTailParams, ProcessList, ProcessSignalParams, ProcessSummary, RequestId,
    RpcError, RpcRequest, RpcResponse, SerialParams, ServerVersion, ShellOpenParams,
    SyncListParams, SyncListResult, SyncMkdirParams, SyncMkdirResult, SyncProgress,
    SyncProgressPhase, SyncPullParams, SyncPullResult, SyncPushParams, SyncPushResult, SyncStatus,
    SyncStatusParams, TransferDirection, DEFAULT_MAX_CONTENT_LENGTH,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

#[cfg(test)]
use client_runtime::{
    handle_shell_open, handle_stream_notification, handle_sync_open, ClientRuntime,
};
use runtime::RuntimeState;

static SERVER_STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

pub use device_registry::DeviceRegistry;
pub use device_session::{ConnectedDeviceProvider, DeviceShell, DeviceShellEvent};

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

    fn sync_push_observed(
        &self,
        params: SyncPushParams,
        observer: &SyncObserver,
    ) -> Result<SyncPushResult, RpcError> {
        let result = self
            .perform(DeviceOperation::SyncPush(params))?
            .into_sync_push()?;
        observer.transferred(result.accepted_offset);
        Ok(result)
    }

    fn sync_pull_observed(
        &self,
        params: SyncPullParams,
        observer: &SyncObserver,
    ) -> Result<SyncPullResult, RpcError> {
        let result = self
            .perform(DeviceOperation::SyncPull(params))?
            .into_sync_pull()?;
        observer.transferred(result.received_size);
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

pub struct SyncObserver {
    cancelled: AtomicBool,
    transferred: AtomicU64,
    total: AtomicU64,
    phase: Mutex<SyncProgressPhase>,
    transfer_id: Mutex<Option<String>>,
    cancel_hooks: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
}

impl Default for SyncObserver {
    fn default() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            transferred: AtomicU64::new(0),
            total: AtomicU64::new(0),
            phase: Mutex::new(SyncProgressPhase::Hashing),
            transfer_id: Mutex::new(None),
            cancel_hooks: Mutex::new(Vec::new()),
        }
    }
}

impl std::fmt::Debug for SyncObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SyncObserver")
            .field("cancelled", &self.is_cancelled())
            .field("transferred", &self.transferred.load(Ordering::Acquire))
            .field("total", &self.total.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl SyncObserver {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let hooks = self
            .cancel_hooks
            .lock()
            .map(|mut hooks| std::mem::take(&mut *hooks))
            .unwrap_or_default();
        for hook in hooks {
            hook();
        }
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn phase(&self, phase: SyncProgressPhase, transferred: u64, total: u64) {
        self.total.store(total, Ordering::Release);
        self.transferred.store(transferred, Ordering::Release);
        if let Ok(mut current) = self.phase.lock() {
            *current = phase;
        }
    }

    pub fn transferred(&self, transferred: u64) {
        self.transferred.store(transferred, Ordering::Release);
    }

    pub fn transfer_id(&self, transfer_id: impl Into<String>) {
        if let Ok(mut current) = self.transfer_id.lock() {
            *current = Some(transfer_id.into());
        }
    }

    pub fn on_cancel(&self, hook: impl FnOnce() + Send + 'static) {
        if self.is_cancelled() {
            hook();
            return;
        }
        let Ok(mut hooks) = self.cancel_hooks.lock() else {
            hook();
            return;
        };
        if self.is_cancelled() {
            drop(hooks);
            hook();
        } else {
            hooks.push(Box::new(hook));
        }
    }

    fn snapshot(
        &self,
        operation_id: String,
        direction: TransferDirection,
        remote_path: String,
    ) -> SyncProgress {
        SyncProgress {
            operation_id,
            transfer_id: self.transfer_id.lock().ok().and_then(|id| id.clone()),
            direction,
            remote_path,
            phase: self
                .phase
                .lock()
                .map_or(SyncProgressPhase::Transferring, |phase| phase.clone()),
            transferred: self.transferred.load(Ordering::Acquire),
            total: self.total.load(Ordering::Acquire),
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

    fn sync_push_observed(
        &self,
        params: SyncPushParams,
        observer: &SyncObserver,
    ) -> Result<SyncPushResult, RpcError> {
        let total = std::fs::metadata(&params.local_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        observer.phase(SyncProgressPhase::Hashing, 0, total);
        if observer.is_cancelled() {
            return Err(RpcError::new(
                error_codes::TRANSFER_CANCELLED,
                "Transfer cancelled",
            ));
        }
        observer.transferred(total);
        observer.phase(SyncProgressPhase::Transferring, 0, total);
        let result = self.sync_push(params)?;
        observer.transferred(result.accepted_offset);
        observer.transfer_id(result.transfer_id.clone());
        Ok(result)
    }

    fn sync_pull(&self, params: SyncPullParams) -> Result<SyncPullResult, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.sync_pull(params)
    }

    fn sync_pull_observed(
        &self,
        params: SyncPullParams,
        observer: &SyncObserver,
    ) -> Result<SyncPullResult, RpcError> {
        observer.phase(SyncProgressPhase::Transferring, 0, 0);
        if observer.is_cancelled() {
            return Err(RpcError::new(
                error_codes::TRANSFER_CANCELLED,
                "Transfer cancelled",
            ));
        }
        let result = self.sync_pull(params)?;
        observer.transfer_id(result.transfer_id.clone());
        observer.phase(
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

    fn sync_push_observed(
        &self,
        params: SyncPushParams,
        observer: &SyncObserver,
    ) -> Result<SyncPushResult, RpcError> {
        MemoryDeviceProvider::sync_push_observed(self, params, observer)
    }

    fn sync_pull_observed(
        &self,
        params: SyncPullParams,
        observer: &SyncObserver,
    ) -> Result<SyncPullResult, RpcError> {
        MemoryDeviceProvider::sync_pull_observed(self, params, observer)
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

/// Dispatches a validated request. Notifications are executed but have no response.
#[must_use]
pub fn handle_request(request: RpcRequest, provider: &dyn DeviceProvider) -> Option<RpcResponse> {
    let result = dispatch(&request, provider);
    request.id.map(|id| match result {
        Ok(value) => RpcResponse::success(id, value),
        Err(error) => RpcResponse::failure(id, error),
    })
}

fn dispatch(request: &RpcRequest, provider: &dyn DeviceProvider) -> Result<Value, RpcError> {
    match request.method.as_str() {
        methods::SERVER_PING => {
            require_empty_params(request.params.as_ref())?;
            Ok(json!({ "ok": true }))
        }
        methods::SERVER_VERSION => {
            require_empty_params(request.params.as_ref())?;
            serde_json::to_value(ServerVersion {
                name: "kindlebridge-server".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                api_version: kindlebridge_schema::API_VERSION.to_owned(),
            })
            .map_err(|_| RpcError::internal_error())
        }
        methods::SERVER_STATUS => {
            require_empty_params(request.params.as_ref())?;
            Ok(json!({ "running": true, "pid": std::process::id() }))
        }
        methods::SERVER_STOP => {
            require_empty_params(request.params.as_ref())?;
            SERVER_STOP_REQUESTED.store(true, Ordering::Release);
            Ok(json!({ "stopping": true }))
        }
        methods::DEVICE_LIST => {
            require_empty_params(request.params.as_ref())?;
            let devices = provider.list()?;
            serde_json::to_value(DeviceList { devices }).map_err(|_| RpcError::internal_error())
        }
        methods::DEVICE_FEATURES => {
            let value = request
                .params
                .clone()
                .ok_or_else(|| RpcError::invalid_params("missing params object"))?;
            let params: DeviceFeaturesParams = serde_json::from_value(value)
                .map_err(|_| RpcError::invalid_params("expected { serial: string }"))?;
            if params.serial.is_empty() {
                return Err(RpcError::invalid_params("serial must not be empty"));
            }
            let features = provider
                .features(&params.serial)?
                .ok_or_else(|| RpcError::device_not_found(&params.serial))?;
            serde_json::to_value(features).map_err(|_| RpcError::internal_error())
        }
        methods::DEVICE_PING => {
            let params = parse_params::<SerialParams>(request, "device ping params")?;
            if params.serial.is_empty() {
                return Err(RpcError::invalid_params("serial must not be empty"));
            }
            if !provider.ping(&params.serial)? {
                return Err(RpcError::device_not_found(&params.serial));
            }
            Ok(json!({ "ok": true }))
        }
        methods::EXEC_RUN => {
            let value = request
                .params
                .clone()
                .ok_or_else(|| RpcError::invalid_params("missing params object"))?;
            let params: ExecParams = serde_json::from_value(value).map_err(|_| {
                RpcError::invalid_params(
                    "expected serial, non-empty argv, cwd, environment, timeout_ms",
                )
            })?;
            if params.serial.is_empty() || params.argv.is_empty() || params.timeout_ms == 0 {
                return Err(RpcError::invalid_params(
                    "serial and argv must be non-empty; timeout_ms must be positive",
                ));
            }
            let features = provider
                .features(&params.serial)?
                .ok_or_else(|| RpcError::device_not_found(&params.serial))?;
            if !features.features.iter().any(|feature| feature == "exec.v1") {
                return Err(RpcError::feature_unavailable(&params.serial, "exec.v1"));
            }
            let result = provider
                .exec(&params)?
                .ok_or_else(|| RpcError::device_not_found(&params.serial))?;
            serde_json::to_value(result).map_err(|_| RpcError::internal_error())
        }
        methods::SYNC_PUSH => {
            let params = parse_params::<SyncPushParams>(request, "sync push params")?;
            to_value(provider.sync_push(params)?)
        }
        methods::SYNC_PULL => {
            let params = parse_params::<SyncPullParams>(request, "sync pull params")?;
            to_value(provider.sync_pull(params)?)
        }
        methods::SYNC_STATUS => {
            let params = parse_params::<SyncStatusParams>(request, "sync status params")?;
            to_value(provider.sync_status(&params)?)
        }
        methods::SYNC_LIST => {
            let params = parse_params::<SyncListParams>(request, "sync list params")?;
            to_value(provider.sync_list(&params)?)
        }
        methods::SYNC_MKDIR => {
            let params = parse_params::<SyncMkdirParams>(request, "sync mkdir params")?;
            to_value(provider.sync_mkdir(&params)?)
        }
        methods::APP_INSTALL => {
            let params = parse_params::<AppInstallParams>(request, "app install params")?;
            to_value(provider.app_install(params)?)
        }
        methods::APP_START
        | methods::APP_STOP
        | methods::APP_RESTART
        | methods::APP_ROLLBACK
        | methods::APP_UNINSTALL => {
            let params = parse_params::<AppTargetParams>(request, "app target params")?;
            let result = match request.method.as_str() {
                methods::APP_START => provider.app_start(&params),
                methods::APP_STOP => provider.app_stop(&params),
                methods::APP_RESTART => provider.app_restart(&params),
                methods::APP_ROLLBACK => provider.app_rollback(&params),
                methods::APP_UNINSTALL => provider.app_uninstall(&params),
                _ => unreachable!(),
            }?;
            to_value(result)
        }
        methods::APP_LIST => {
            let params = parse_params::<SerialParams>(request, "serial params")?;
            to_value(provider.app_list(&params)?)
        }
        methods::APP_LOG => {
            let params = parse_params::<AppLogParams>(request, "app log params")?;
            to_value(provider.app_log(&params)?)
        }
        methods::PROCESS_LIST => {
            let params = parse_params::<SerialParams>(request, "serial params")?;
            to_value(provider.process_list(&params)?)
        }
        methods::PROCESS_SIGNAL => {
            let params = parse_params::<ProcessSignalParams>(request, "process signal params")?;
            to_value(provider.process_signal(&params)?)
        }
        methods::LOG_TAIL => {
            let params = parse_params::<LogTailParams>(request, "log tail params")?;
            to_value(provider.log_tail(&params)?)
        }
        _ => Err(RpcError::method_not_found(&request.method)),
    }
}

fn handle_registry_request(request: RpcRequest, registry: &DeviceRegistry) -> Option<RpcResponse> {
    let result = registry.rpc(|provider| dispatch(&request, provider));
    request.id.map(|id| match result {
        Ok(value) => RpcResponse::success(id, value),
        Err(error) => RpcResponse::failure(id, error),
    })
}

#[must_use]
pub fn server_stop_requested() -> bool {
    SERVER_STOP_REQUESTED.load(Ordering::Acquire)
}

pub fn reset_server_stop_requested() {
    SERVER_STOP_REQUESTED.store(false, Ordering::Release);
}

fn parse_params<T: DeserializeOwned>(request: &RpcRequest, expected: &str) -> Result<T, RpcError> {
    let value = request
        .params
        .clone()
        .ok_or_else(|| RpcError::invalid_params("missing params object"))?;
    serde_json::from_value(value).map_err(|_| RpcError::invalid_params(expected))
}

fn to_value<T: Serialize>(value: T) -> Result<Value, RpcError> {
    serde_json::to_value(value).map_err(|_| RpcError::internal_error())
}

fn require_empty_params(params: Option<&Value>) -> Result<(), RpcError> {
    match params {
        None => Ok(()),
        Some(Value::Object(object)) if object.is_empty() => Ok(()),
        Some(Value::Array(array)) if array.is_empty() => Ok(()),
        Some(_) => Err(RpcError::invalid_params("method takes no params")),
    }
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
                if let Some(response) = handle_request(request, provider) {
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
    use std::sync::atomic::{AtomicBool, AtomicUsize};
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
        let writer = Arc::new(Mutex::new(output.clone()));
        let jobs = Arc::new(Mutex::new(HashMap::new()));
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

        handle_sync_open(request, &writer, &registry, &jobs).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while (!jobs.lock().unwrap().is_empty() || output.frames.load(Ordering::Acquire) < 2)
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
        let first = Arc::new(SyncObserver::default());
        let second = Arc::new(SyncObserver::default());
        let runtime = ClientRuntime::new(
            Vec::<u8>::new(),
            Arc::new(DeviceRegistry::direct(Arc::new(provider()))),
        );
        let shell_stream: Arc<dyn ShellStream> = shell.clone();
        runtime.track_shell("shell".to_owned(), shell_stream);
        runtime.track_sync("first".to_owned(), Arc::clone(&first));
        runtime.track_sync("second".to_owned(), Arc::clone(&second));

        runtime.shutdown();
        runtime.shutdown();

        assert!(shell.closed.load(Ordering::Acquire));
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert!(runtime.is_idle());
    }

    #[test]
    fn dropping_client_runtime_closes_shells_and_cancels_sync_jobs() {
        let shell = Arc::new(FakeShell::new([]));
        let observer = Arc::new(SyncObserver::default());
        {
            let runtime = ClientRuntime::new(
                Vec::<u8>::new(),
                Arc::new(DeviceRegistry::direct(Arc::new(provider()))),
            );
            let shell_stream: Arc<dyn ShellStream> = shell.clone();
            runtime.track_shell("shell".to_owned(), shell_stream);
            runtime.track_sync("sync".to_owned(), Arc::clone(&observer));
        }

        assert!(shell.closed.load(Ordering::Acquire));
        assert!(observer.is_cancelled());
    }

    #[test]
    fn cancellation_hooks_run_once_and_late_hooks_run_immediately() {
        let observer = SyncObserver::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let first = Arc::clone(&calls);
        observer.on_cancel(move || {
            first.fetch_add(1, Ordering::AcqRel);
        });

        observer.cancel();
        observer.cancel();
        let late = Arc::clone(&calls);
        observer.on_cancel(move || {
            late.fetch_add(1, Ordering::AcqRel);
        });

        assert_eq!(calls.load(Ordering::Acquire), 2);
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
