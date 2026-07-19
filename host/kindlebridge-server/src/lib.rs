//! `KindleBridge` host RPC server.

mod device_session;
mod runtime;

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use kindlebridge_schema::shell_protocol::ShellPacket;
use kindlebridge_schema::{
    error_codes, methods, parse_request_value, read_frame, write_json_frame, AppInstallParams,
    AppList, AppSummary, AppTargetParams, DeviceFeatures, DeviceFeaturesParams, DeviceList,
    DeviceSummary, ExecParams, ExecResult, FramingError, LogSnapshot, LogTailParams, ProcessList,
    ProcessSignalParams, ProcessSummary, RequestId, RpcError, RpcRequest, RpcResponse,
    SerialParams, ServerVersion, ShellOpenParams, ShellOpenResult, StreamChannel,
    StreamClosedParams, StreamCreditParams, StreamDataParams, StreamExitParams, StreamIdParams,
    StreamResizeParams, StreamWriteParams, SyncPullParams, SyncPullResult, SyncPushParams,
    SyncPushResult, SyncStatus, SyncStatusParams, DEFAULT_MAX_CONTENT_LENGTH,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

use runtime::RuntimeState;

static SERVER_STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

pub use device_session::{
    ConnectedDeviceProvider, DeviceShell, DeviceShellEvent, ReconnectingUsbProvider,
};

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
    fn list(&self) -> Result<Vec<DeviceSummary>, ProviderError>;
    fn features(&self, serial: &str) -> Result<Option<DeviceFeatures>, ProviderError>;
    fn exec(&self, params: &ExecParams) -> Result<Option<ExecResult>, RpcError>;
    fn sync_push(&self, params: SyncPushParams) -> Result<SyncPushResult, RpcError>;
    fn sync_pull(&self, params: SyncPullParams) -> Result<SyncPullResult, RpcError>;
    fn sync_status(&self, params: &SyncStatusParams) -> Result<SyncStatus, RpcError>;
    fn app_install(&self, params: AppInstallParams) -> Result<AppSummary, RpcError>;
    fn app_start(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError>;
    fn app_stop(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError>;
    fn app_restart(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError>;
    fn app_rollback(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError>;
    fn app_uninstall(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError>;
    fn app_list(&self, params: &SerialParams) -> Result<AppList, RpcError>;
    fn process_list(&self, params: &SerialParams) -> Result<ProcessList, RpcError>;
    fn process_signal(&self, params: &ProcessSignalParams) -> Result<ProcessSummary, RpcError>;
    fn log_tail(&self, params: &LogTailParams) -> Result<LogSnapshot, RpcError>;
    fn shell_open(&self, params: &ShellOpenParams) -> Result<Arc<dyn ShellStream>, RpcError> {
        Err(RpcError::feature_unavailable(&params.serial, "shell.v2"))
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

impl DeviceProvider for MemoryDeviceProvider {
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

    fn sync_pull(&self, params: SyncPullParams) -> Result<SyncPullResult, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.sync_pull(params)
    }

    fn sync_status(&self, params: &SyncStatusParams) -> Result<SyncStatus, RpcError> {
        self.ensure_device(&params.serial)?;
        self.runtime()?.sync_status(params)
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

#[derive(Debug, Error)]
#[error("device provider failed: {message}")]
pub struct ProviderError {
    message: String,
}

impl ProviderError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
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
pub fn handle_request<P: DeviceProvider + ?Sized>(
    request: RpcRequest,
    provider: &P,
) -> Option<RpcResponse> {
    let result = dispatch(&request, provider);
    request.id.map(|id| match result {
        Ok(value) => RpcResponse::success(id, value),
        Err(error) => RpcResponse::failure(id, error),
    })
}

fn dispatch<P: DeviceProvider + ?Sized>(
    request: &RpcRequest,
    provider: &P,
) -> Result<Value, RpcError> {
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
            let devices = provider.list().map_err(provider_rpc_error)?;
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
                .features(&params.serial)
                .map_err(provider_rpc_error)?
                .ok_or_else(|| RpcError::device_not_found(&params.serial))?;
            serde_json::to_value(features).map_err(|_| RpcError::internal_error())
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
                .features(&params.serial)
                .map_err(provider_rpc_error)?
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

fn provider_rpc_error(_error: ProviderError) -> RpcError {
    // Provider details can contain host paths or transport internals. Keep the public error stable.
    RpcError::new(error_codes::SERVER_NOT_READY, "Server not ready")
}

/// Serves framed JSON-RPC messages until clean EOF or a framing error.
pub fn serve<R: BufRead, W: Write, P: DeviceProvider + ?Sized>(
    reader: &mut R,
    writer: &mut W,
    provider: &P,
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
    provider: Arc<dyn DeviceProvider>,
) -> Result<(), ServeError>
where
    R: BufRead,
    W: Write + Send + 'static,
{
    let writer = Arc::new(Mutex::new(writer));
    let streams: Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let result = serve_streaming_loop(reader, &writer, &provider, &streams);
    if let Ok(mut streams) = streams.lock() {
        for (_, stream) in streams.drain() {
            let _ = stream.close();
        }
    }
    result
}

fn serve_streaming_loop<R, W>(
    reader: &mut R,
    writer: &Arc<Mutex<W>>,
    provider: &Arc<dyn DeviceProvider>,
    streams: &Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
) -> Result<(), ServeError>
where
    R: BufRead,
    W: Write + Send + 'static,
{
    loop {
        let Some(payload) = read_frame(reader, DEFAULT_MAX_CONTENT_LENGTH)? else {
            return Ok(());
        };
        let value = match serde_json::from_slice::<Value>(&payload) {
            Ok(value) => value,
            Err(_) => {
                write_shared(
                    writer,
                    &RpcResponse::failure(RequestId::Null, RpcError::parse_error()),
                )?;
                continue;
            }
        };
        let request = match parse_request_value(value) {
            Ok(request) => request,
            Err(error) => {
                write_shared(writer, &RpcResponse::failure(RequestId::Null, error))?;
                continue;
            }
        };

        if request.method == methods::SHELL_OPEN {
            handle_shell_open(request, writer, provider, streams)?;
        } else if matches!(
            request.method.as_str(),
            methods::STREAM_WRITE
                | methods::STREAM_RESIZE
                | methods::STREAM_CLOSE_INPUT
                | methods::STREAM_CLOSE
        ) {
            handle_stream_notification(request, writer, streams)?;
        } else if let Some(response) = handle_request(request, provider.as_ref()) {
            write_shared(writer, &response)?;
        }
    }
}

fn handle_shell_open<W: Write + Send + 'static>(
    request: RpcRequest,
    writer: &Arc<Mutex<W>>,
    provider: &Arc<dyn DeviceProvider>,
    streams: &Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
) -> Result<(), ServeError> {
    let Some(id) = request.id else {
        return Ok(());
    };
    let result = request
        .params
        .ok_or_else(|| RpcError::invalid_params("missing shell open params"))
        .and_then(|value| {
            serde_json::from_value::<ShellOpenParams>(value).map_err(|_| {
                RpcError::invalid_params("expected serial and valid shell open fields")
            })
        })
        .and_then(|params| provider.shell_open(&params))
        .and_then(|shell| {
            let stream_id = random_stream_id().map_err(|_| RpcError::internal_error())?;
            streams
                .lock()
                .map_err(|_| RpcError::internal_error())?
                .insert(stream_id.clone(), Arc::clone(&shell));
            Ok((stream_id, shell))
        });

    match result {
        Ok((stream_id, shell)) => {
            write_shared(
                writer,
                &RpcResponse::success(
                    id,
                    serde_json::to_value(ShellOpenResult {
                        stream_id: stream_id.clone(),
                        send_credit: kindlebridge_schema::device_protocol::SHELL_STREAM_WINDOW,
                        receive_credit: kindlebridge_schema::device_protocol::SHELL_STREAM_WINDOW,
                    })
                    .map_err(|_| {
                        FramingError::Json(serde_json::Error::io(io::Error::other(
                            "could not encode shell open result",
                        )))
                    })?,
                ),
            )?;
            spawn_shell_output(stream_id, shell, Arc::clone(writer), Arc::clone(streams));
        }
        Err(error) => write_shared(writer, &RpcResponse::failure(id, error))?,
    }
    Ok(())
}

fn handle_stream_notification<W: Write + Send + 'static>(
    request: RpcRequest,
    writer: &Arc<Mutex<W>>,
    streams: &Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
) -> Result<(), ServeError> {
    // Stream operations are notifications. Malformed or unknown stream IDs are
    // ignored because JSON-RPC notifications cannot receive an error response;
    // the active shell remains isolated from other client streams.
    match request.method.as_str() {
        methods::STREAM_WRITE => {
            let Some(params) = decode_notification::<StreamWriteParams>(request.params) else {
                return Ok(());
            };
            let Ok(data) = BASE64.decode(params.data) else {
                return Ok(());
            };
            if data.len() > kindlebridge_schema::shell_protocol::MAX_SHELL_PACKET_PAYLOAD {
                return Ok(());
            }
            let Some(shell) = find_stream(streams, &params.stream_id) else {
                return Ok(());
            };
            if shell.send(ShellPacket::Stdin(data.clone())).is_ok() {
                emit_notification(
                    writer,
                    methods::STREAM_CREDIT,
                    &StreamCreditParams {
                        stream_id: params.stream_id,
                        bytes: u32::try_from(data.len()).unwrap_or(0),
                    },
                )?;
            }
        }
        methods::STREAM_RESIZE => {
            let Some(params) = decode_notification::<StreamResizeParams>(request.params) else {
                return Ok(());
            };
            if let Some(shell) = find_stream(streams, &params.stream_id) {
                let _ = shell.send(ShellPacket::Resize(params.size));
            }
        }
        methods::STREAM_CLOSE_INPUT => {
            let Some(params) = decode_notification::<StreamIdParams>(request.params) else {
                return Ok(());
            };
            if let Some(shell) = find_stream(streams, &params.stream_id) {
                let _ = shell.send(ShellPacket::CloseStdin);
            }
        }
        methods::STREAM_CLOSE => {
            let Some(params) = decode_notification::<StreamIdParams>(request.params) else {
                return Ok(());
            };
            let shell = streams
                .lock()
                .ok()
                .and_then(|mut streams| streams.remove(&params.stream_id));
            if let Some(shell) = shell {
                let _ = shell.close();
                emit_notification(
                    writer,
                    methods::STREAM_CLOSED,
                    &StreamClosedParams {
                        stream_id: params.stream_id,
                        reason: Some("closed by client".to_owned()),
                    },
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn spawn_shell_output<W: Write + Send + 'static>(
    stream_id: String,
    shell: Arc<dyn ShellStream>,
    writer: Arc<Mutex<W>>,
    streams: Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
) {
    thread::Builder::new()
        .name(format!("kindlebridge-shell-{stream_id}"))
        .spawn(move || {
            let reason = loop {
                match shell.recv() {
                    Ok(DeviceShellEvent::Packet(ShellPacket::Stdout(data))) => {
                        if emit_notification(
                            &writer,
                            methods::STREAM_DATA,
                            &StreamDataParams {
                                stream_id: stream_id.clone(),
                                channel: StreamChannel::Stdout,
                                data: BASE64.encode(data),
                            },
                        )
                        .is_err()
                        {
                            break Some("client connection was lost".to_owned());
                        }
                    }
                    Ok(DeviceShellEvent::Packet(ShellPacket::Stderr(data))) => {
                        if emit_notification(
                            &writer,
                            methods::STREAM_DATA,
                            &StreamDataParams {
                                stream_id: stream_id.clone(),
                                channel: StreamChannel::Stderr,
                                data: BASE64.encode(data),
                            },
                        )
                        .is_err()
                        {
                            break Some("client connection was lost".to_owned());
                        }
                    }
                    Ok(DeviceShellEvent::Packet(ShellPacket::Exit(status))) => {
                        if emit_notification(
                            &writer,
                            methods::STREAM_EXIT,
                            &StreamExitParams {
                                stream_id: stream_id.clone(),
                                exit_code: status.exit_code,
                                signal: status.signal,
                            },
                        )
                        .is_err()
                        {
                            break Some("client connection was lost".to_owned());
                        }
                    }
                    Ok(DeviceShellEvent::Packet(_)) => {
                        break Some("invalid device shell packet".to_owned())
                    }
                    Ok(DeviceShellEvent::Closed) => break None,
                    Err(error) => break Some(error.to_string()),
                }
            };
            let removed = streams
                .lock()
                .ok()
                .and_then(|mut streams| streams.remove(&stream_id))
                .is_some();
            if removed {
                let _ = emit_notification(
                    &writer,
                    methods::STREAM_CLOSED,
                    &StreamClosedParams { stream_id, reason },
                );
            }
        })
        .expect("could not start shell notification worker");
}

fn decode_notification<T: DeserializeOwned>(params: Option<Value>) -> Option<T> {
    serde_json::from_value(params?).ok()
}

fn find_stream(
    streams: &Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
    stream_id: &str,
) -> Option<Arc<dyn ShellStream>> {
    streams.lock().ok()?.get(stream_id).cloned()
}

fn random_stream_id() -> Result<String, getrandom::Error> {
    let mut bytes = [0_u8; 16];
    getrandom::getrandom(&mut bytes)?;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

fn emit_notification<W: Write, T: Serialize>(
    writer: &Arc<Mutex<W>>,
    method: &str,
    params: &T,
) -> Result<(), ServeError> {
    let value = serde_json::to_value(params).map_err(FramingError::from)?;
    write_shared(writer, &RpcRequest::notification(method, Some(value)))
}

fn write_shared<W: Write, T: Serialize>(
    writer: &Arc<Mutex<W>>,
    value: &T,
) -> Result<(), ServeError> {
    let mut writer = writer
        .lock()
        .map_err(|_| FramingError::Io(io::Error::other("RPC writer lock is poisoned")))?;
    write_json_frame(&mut *writer, value)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs;
    use std::io::{BufReader, Cursor};
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

    macro_rules! unused_provider_method {
        ($name:ident($($argument:ident: $type:ty),*) -> $result:ty) => {
            fn $name(&self, $($argument: $type),*) -> $result {
                $(let _ = $argument;)*
                unreachable!(concat!(stringify!($name), " is not used by shell tests"))
            }
        };
    }

    impl DeviceProvider for ShellProvider {
        unused_provider_method!(list() -> Result<Vec<DeviceSummary>, ProviderError>);
        unused_provider_method!(features(serial: &str) -> Result<Option<DeviceFeatures>, ProviderError>);
        unused_provider_method!(exec(params: &ExecParams) -> Result<Option<ExecResult>, RpcError>);
        unused_provider_method!(sync_push(params: SyncPushParams) -> Result<SyncPushResult, RpcError>);
        unused_provider_method!(sync_pull(params: SyncPullParams) -> Result<SyncPullResult, RpcError>);
        unused_provider_method!(sync_status(params: &SyncStatusParams) -> Result<SyncStatus, RpcError>);
        unused_provider_method!(app_install(params: AppInstallParams) -> Result<AppSummary, RpcError>);
        unused_provider_method!(app_start(params: &AppTargetParams) -> Result<AppSummary, RpcError>);
        unused_provider_method!(app_stop(params: &AppTargetParams) -> Result<AppSummary, RpcError>);
        unused_provider_method!(app_restart(params: &AppTargetParams) -> Result<AppSummary, RpcError>);
        unused_provider_method!(app_rollback(params: &AppTargetParams) -> Result<AppSummary, RpcError>);
        unused_provider_method!(app_uninstall(params: &AppTargetParams) -> Result<AppSummary, RpcError>);
        unused_provider_method!(app_list(params: &SerialParams) -> Result<AppList, RpcError>);
        unused_provider_method!(process_list(params: &SerialParams) -> Result<ProcessList, RpcError>);
        unused_provider_method!(process_signal(params: &ProcessSignalParams) -> Result<ProcessSummary, RpcError>);
        unused_provider_method!(log_tail(params: &LogTailParams) -> Result<LogSnapshot, RpcError>);

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
        let provider: Arc<dyn DeviceProvider> = Arc::new(ShellProvider {
            shell: Arc::clone(&shell),
        });
        let output = SharedOutput::default();
        let writer = Arc::new(Mutex::new(output.clone()));
        let streams = Arc::new(Mutex::new(HashMap::new()));

        handle_shell_open(shell_open_request(), &writer, &provider, &streams).unwrap();
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
        provider
            .app_install(AppInstallParams {
                serial: "KT6-TEST".to_owned(),
                app_id: "org.example.app".to_owned(),
                version: "1.0.0".to_owned(),
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
    }
}
