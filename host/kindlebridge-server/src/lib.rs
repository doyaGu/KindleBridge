//! `KindleBridge` host RPC server.

mod device_session;
mod runtime;

use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex, MutexGuard};

use kindlebridge_schema::{
    error_codes, methods, parse_request_value, read_frame, write_json_frame, AppInstallParams,
    AppList, AppSummary, AppTargetParams, DeviceFeatures, DeviceFeaturesParams, DeviceList,
    DeviceSummary, ExecParams, ExecResult, FramingError, LogSnapshot, LogTailParams, ProcessList,
    ProcessSignalParams, ProcessSummary, RequestId, RpcError, RpcRequest, RpcResponse,
    SerialParams, ServerVersion, SyncPullParams, SyncPullResult, SyncPushParams, SyncPushResult,
    SyncStatus, SyncStatusParams, DEFAULT_MAX_CONTENT_LENGTH,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

use runtime::RuntimeState;

pub use device_session::ConnectedDeviceProvider;

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

pub trait DeviceProvider {
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{BufReader, Cursor};

    use kindlebridge_schema::{
        read_json_frame, write_json_frame, DeviceState, RpcRequest, DEFAULT_MAX_CONTENT_LENGTH,
    };

    use super::*;

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
