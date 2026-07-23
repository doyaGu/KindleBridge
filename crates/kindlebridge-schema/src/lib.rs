//! Serialized types for the KindleBridge host API and device protocol.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use thiserror::Error;

pub mod device_protocol;
pub mod device_rpc;
pub mod host_rpc;
pub mod shell_protocol;

pub const JSONRPC_VERSION: &str = "2.0";
pub const API_VERSION: &str = "v1";
pub const DEFAULT_MAX_CONTENT_LENGTH: usize = 8 * 1024 * 1024;

pub mod methods {
    pub const SERVER_PING: &str = "v1.server.ping";
    pub const SERVER_VERSION: &str = "v1.server.version";
    pub const SERVER_STATUS: &str = "v1.server.status";
    pub const SERVER_STOP: &str = "v1.server.stop";
    pub const DEVICE_LIST: &str = "v1.device.list";
    pub const DEVICE_FEATURES: &str = "v1.device.features";
    pub const DEVICE_PING: &str = "v1.device.ping";
    pub const EXEC_RUN: &str = "v1.exec.run";
    pub const SYNC_PUSH: &str = "v1.sync.push";
    pub const SYNC_PULL: &str = "v1.sync.pull";
    pub const SYNC_STATUS: &str = "v1.sync.status";
    pub const SYNC_LIST: &str = "v1.sync.list";
    pub const SYNC_MKDIR: &str = "v1.sync.mkdir";
    pub const SYNC_PUSH_STREAM: &str = "v1.sync.push_stream";
    pub const SYNC_PULL_STREAM: &str = "v1.sync.pull_stream";
    pub const SYNC_CANCEL: &str = "v1.sync.cancel";
    pub const SYNC_PROGRESS: &str = "v1.sync.progress";
    pub const APP_INSTALL: &str = "v1.app.install";
    pub const APP_START: &str = "v1.app.start";
    pub const APP_STOP: &str = "v1.app.stop";
    pub const APP_RESTART: &str = "v1.app.restart";
    pub const APP_ROLLBACK: &str = "v1.app.rollback";
    pub const APP_UNINSTALL: &str = "v1.app.uninstall";
    pub const APP_LIST: &str = "v1.app.list";
    pub const APP_LOG: &str = "v1.app.log";
    pub const PROCESS_LIST: &str = "v1.process.list";
    pub const PROCESS_SIGNAL: &str = "v1.process.signal";
    pub const LOG_TAIL: &str = "v1.log.tail";
    pub const SHELL_OPEN: &str = "v1.shell.open";
    pub const STREAM_WRITE: &str = "v1.stream.write";
    pub const STREAM_RESIZE: &str = "v1.stream.resize";
    pub const STREAM_CLOSE_INPUT: &str = "v1.stream.close_input";
    pub const STREAM_CLOSE: &str = "v1.stream.close";
    pub const STREAM_DATA: &str = "v1.stream.data";
    pub const STREAM_CREDIT: &str = "v1.stream.credit";
    pub const STREAM_EXIT: &str = "v1.stream.exit";
    pub const STREAM_CLOSED: &str = "v1.stream.closed";
}

/// Stable JSON-RPC error codes exposed by the v1 API.
pub mod error_codes {
    pub const PARSE_ERROR: i64 = -32_700;
    pub const INVALID_REQUEST: i64 = -32_600;
    pub const METHOD_NOT_FOUND: i64 = -32_601;
    pub const INVALID_PARAMS: i64 = -32_602;
    pub const INTERNAL_ERROR: i64 = -32_603;

    pub const SERVER_NOT_READY: i64 = -32_001;
    pub const DEVICE_NOT_FOUND: i64 = -32_004;
    pub const FEATURE_UNAVAILABLE: i64 = -32_005;
    pub const INVALID_STATE: i64 = -32_010;
    pub const CHECKSUM_MISMATCH: i64 = -32_011;
    pub const TRANSFER_NOT_FOUND: i64 = -32_012;
    pub const FILE_NOT_FOUND: i64 = -32_013;
    pub const TRANSFER_CANCELLED: i64 = -32_014;
    pub const APP_NOT_FOUND: i64 = -32_020;
    pub const NO_ROLLBACK_AVAILABLE: i64 = -32_021;
    pub const APP_INSTALL_FAILED: i64 = -32_022;
    pub const PROCESS_NOT_FOUND: i64 = -32_030;
    pub const INVALID_SIGNAL: i64 = -32_031;
    pub const PROCESS_SIGNAL_FAILED: i64 = -32_032;
    pub const LOG_CURSOR_EXPIRED: i64 = -32_040;
    pub const EXEC_TIMEOUT: i64 = -32_050;
    pub const EXEC_OUTPUT_LIMIT: i64 = -32_051;
    pub const EXEC_FAILED: i64 = -32_052;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
    Null,
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(value) => write!(formatter, "{value}"),
            Self::String(value) => formatter.write_str(value),
            Self::Null => formatter.write_str("null"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
}

impl RpcRequest {
    #[must_use]
    pub fn call(id: RequestId, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            method: method.into(),
            params,
            id: Some(id),
        }
    }

    #[must_use]
    pub fn notification(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            method: method.into(),
            params,
            id: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    #[must_use]
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    #[must_use]
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    #[must_use]
    pub fn parse_error() -> Self {
        Self::new(error_codes::PARSE_ERROR, "Parse error")
    }

    #[must_use]
    pub fn invalid_request() -> Self {
        Self::new(error_codes::INVALID_REQUEST, "Invalid Request")
    }

    #[must_use]
    pub fn method_not_found(method: &str) -> Self {
        Self::new(error_codes::METHOD_NOT_FOUND, "Method not found")
            .with_data(json!({ "method": method }))
    }

    #[must_use]
    pub fn invalid_params(detail: impl Into<String>) -> Self {
        Self::new(error_codes::INVALID_PARAMS, "Invalid params")
            .with_data(json!({ "detail": detail.into() }))
    }

    #[must_use]
    pub fn internal_error() -> Self {
        Self::new(error_codes::INTERNAL_ERROR, "Internal error")
    }

    #[must_use]
    pub fn device_not_found(serial: &str) -> Self {
        Self::new(error_codes::DEVICE_NOT_FOUND, "Device not found")
            .with_data(json!({ "serial": serial }))
    }

    #[must_use]
    pub fn feature_unavailable(serial: &str, feature: &str) -> Self {
        Self::new(error_codes::FEATURE_UNAVAILABLE, "Feature unavailable")
            .with_data(json!({ "serial": serial, "feature": feature }))
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} ({})", self.message, self.code)?;
        if let Some(detail) = self
            .data
            .as_ref()
            .and_then(|data| data.get("detail"))
            .and_then(Value::as_str)
        {
            write!(formatter, ": {detail}")?;
        } else if let Some(data) = &self.data {
            write!(formatter, ": {data}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RpcError {}

#[cfg(test)]
mod shell_api_tests {
    use serde_json::json;

    use super::*;
    use crate::device_protocol::{ShellMode, ShellOpen};

    #[test]
    fn shell_open_api_is_flat_and_carries_symmetric_credit() {
        let params = ShellOpenParams {
            serial: "KT6".to_owned(),
            open: ShellOpen::command("printf hello"),
        };
        let value = serde_json::to_value(&params).unwrap();
        assert_eq!(value["serial"], "KT6");
        assert_eq!(value["mode"], "raw");
        assert_eq!(value["argv"], json!(["/bin/sh", "-lc", "printf hello"]));
        assert_eq!(
            serde_json::from_value::<ShellOpenParams>(value)
                .unwrap()
                .open
                .mode,
            ShellMode::Raw
        );

        let result = ShellOpenResult {
            stream_id: "opaque".to_owned(),
            send_credit: device_protocol::SHELL_STREAM_WINDOW,
            receive_credit: device_protocol::SHELL_STREAM_WINDOW,
        };
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!({
                "stream_id": "opaque",
                "send_credit": 262_144,
                "receive_credit": 262_144
            })
        );
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl RpcResponse {
    #[must_use]
    pub fn success(id: RequestId, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id,
            result: Some(result),
            error: None,
        }
    }

    #[must_use]
    pub fn failure(id: RequestId, error: RpcError) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id,
            result: None,
            error: Some(error),
        }
    }

    pub fn into_result(self) -> Result<Value, RpcError> {
        match (self.result, self.error) {
            (Some(result), None) => Ok(result),
            (None, Some(error)) => Err(error),
            _ => Err(RpcError::invalid_request()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerVersion {
    pub name: String,
    pub version: String,
    pub api_version: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyParams {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingResult {
    pub ok: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerStatus {
    pub running: bool,
    pub pid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerStopResult {
    pub stopping: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellOpenParams {
    pub serial: String,
    #[serde(flatten)]
    pub open: device_protocol::ShellOpen,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellOpenResult {
    pub stream_id: String,
    pub send_credit: u32,
    pub receive_credit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamWriteParams {
    pub stream_id: String,
    /// Base64-encoded bytes.
    pub data: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamResizeParams {
    pub stream_id: String,
    #[serde(flatten)]
    pub size: device_protocol::TerminalSize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamIdParams {
    pub stream_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamChannel {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDataParams {
    pub stream_id: String,
    pub channel: StreamChannel,
    /// Base64-encoded bytes.
    pub data: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamCreditParams {
    pub stream_id: String,
    pub bytes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamExitParams {
    pub stream_id: String,
    pub exit_code: i32,
    pub signal: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamClosedParams {
    pub stream_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceSummary {
    pub serial: String,
    pub model: String,
    pub state: DeviceState,
    pub transport: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceState {
    Online,
    Offline,
    Unauthorized,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceList {
    pub devices: Vec<DeviceSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceFeaturesParams {
    pub serial: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceFeatures {
    pub serial: String,
    pub protocol_version: u32,
    pub features: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecParams {
    pub serial: String,
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default = "default_exec_timeout_ms")]
    pub timeout_ms: u64,
}

const fn default_exec_timeout_ms() -> u64 {
    30_000
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    Push,
    Pull,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferState {
    InProgress,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPushParams {
    pub serial: String,
    /// Absolute path in the host filesystem. File bytes never enter JSON-RPC.
    pub local_path: String,
    /// Relative logical path below the device sync root.
    pub remote_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<String>,
    #[serde(default = "default_sync_block_size")]
    pub block_size: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPushResult {
    pub transfer_id: String,
    pub accepted_offset: u64,
    pub state: TransferState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPullParams {
    pub serial: String,
    /// Relative logical path below the device sync root.
    pub remote_path: String,
    /// Absolute destination path in the host filesystem.
    pub local_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<String>,
    #[serde(default = "default_sync_block_size")]
    pub block_size: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPullResult {
    pub transfer_id: String,
    pub total_size: u64,
    pub received_size: u64,
    pub state: TransferState,
}

pub const MAX_SYNC_BLOCK_SIZE: u32 = device_protocol::MAX_HOST_TO_DEVICE_PAYLOAD;
/// Default wire payload for one sync DATA frame.
///
/// KBP scheduling happens between complete frames, so a 1 MiB default can
/// occupy a full-speed USB link for longer than the interactive shell latency
/// budget. 256 KiB keeps sync efficient while giving Interactive traffic a
/// scheduling opportunity several times per 50 ms window. Callers that value
/// bulk throughput over latency can still request up to
/// [`MAX_SYNC_BLOCK_SIZE`].
pub const DEFAULT_SYNC_BLOCK_SIZE: u32 = 256 * 1024;

const fn default_sync_block_size() -> u32 {
    DEFAULT_SYNC_BLOCK_SIZE
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncStatusParams {
    pub serial: String,
    pub transfer_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncStatus {
    pub transfer_id: String,
    pub direction: TransferDirection,
    pub remote_path: String,
    pub next_offset: u64,
    pub total_size: u64,
    pub state: TransferState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncEntryKind {
    File,
    Directory,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncEntry {
    pub name: String,
    pub kind: SyncEntryKind,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncListParams {
    pub serial: String,
    pub remote_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default = "default_sync_list_limit")]
    pub limit: u32,
}

const fn default_sync_list_limit() -> u32 {
    256
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncListResult {
    pub remote_path: String,
    pub entries: Vec<SyncEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncMkdirParams {
    pub serial: String,
    pub remote_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncMkdirResult {
    pub remote_path: String,
    pub created: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncProgressPhase {
    Hashing,
    Transferring,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncProgress {
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<String>,
    pub direction: TransferDirection,
    pub remote_path: String,
    pub phase: SyncProgressPhase,
    pub transferred: u64,
    pub total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncCancelParams {
    pub operation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppState {
    Stopped,
    Running,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSummary {
    pub app_id: String,
    pub version: String,
    pub state: AppState,
    pub rollback_available: bool,
    pub pid: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppList {
    pub apps: Vec<AppSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppLogParams {
    pub serial: String,
    pub app_id: String,
    pub run_id: Option<String>,
    #[serde(default)]
    pub stdout_cursor: u64,
    #[serde(default)]
    pub stderr_cursor: u64,
    pub max_bytes: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppLogChunk {
    pub cursor: u64,
    pub next_cursor: u64,
    pub data_base64: String,
    pub capped: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppLogSnapshot {
    pub app_id: String,
    pub run_id: String,
    pub reset: bool,
    pub state: AppState,
    pub pid: Option<u32>,
    pub stdout: AppLogChunk,
    pub stderr: AppLogChunk,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppInstallParams {
    pub serial: String,
    /// Absolute KBB path on the host. The CLI resolves friendly relative input
    /// before calling the shared server so its working directory is irrelevant.
    pub bundle_path: String,
}

#[cfg(test)]
mod app_api_tests {
    use serde_json::json;

    use super::AppInstallParams;

    #[test]
    fn public_install_accepts_only_a_host_bundle_path() {
        let params: AppInstallParams = serde_json::from_value(json!({
            "serial": "KT6",
            "bundle_path": "C:\\apps\\reader.kbb",
        }))
        .unwrap();
        assert_eq!(params.bundle_path, "C:\\apps\\reader.kbb");
        assert!(serde_json::from_value::<AppInstallParams>(json!({
            "serial": "KT6",
            "app_id": "org.example.forged",
            "version": "99.0.0",
        }))
        .is_err());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppTargetParams {
    pub serial: String,
    pub app_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SerialParams {
    pub serial: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    Running,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSummary {
    pub pid: u32,
    pub name: String,
    pub app_id: Option<String>,
    pub state: ProcessState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessList {
    pub processes: Vec<ProcessSummary>,
}

/// Linux process signals accepted by the stable process-control API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ProcessSignal {
    Hup = 1,
    Int,
    Quit,
    Ill,
    Trap,
    Abrt,
    Bus,
    Fpe,
    Kill,
    Usr1,
    Segv,
    Usr2,
    Pipe,
    Alrm,
    Term,
    Stkflt,
    Chld,
    Cont,
    Stop,
    Tstp,
    Ttin,
    Ttou,
    Urg,
    Xcpu,
    Xfsz,
    Vtalrm,
    Prof,
    Winch,
    Io,
    Pwr,
    Sys,
}

impl ProcessSignal {
    /// Accepts the conventional name with or without `SIG`, case-insensitively,
    /// plus the Linux signal number used by the Kindle target.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        let normalized = value.to_ascii_uppercase();
        let name = normalized.strip_prefix("SIG").unwrap_or(&normalized);
        Some(match name {
            "HUP" | "1" => Self::Hup,
            "INT" | "2" => Self::Int,
            "QUIT" | "3" => Self::Quit,
            "ILL" | "4" => Self::Ill,
            "TRAP" | "5" => Self::Trap,
            "ABRT" | "IOT" | "6" => Self::Abrt,
            "BUS" | "7" => Self::Bus,
            "FPE" | "8" => Self::Fpe,
            "KILL" | "9" => Self::Kill,
            "USR1" | "10" => Self::Usr1,
            "SEGV" | "11" => Self::Segv,
            "USR2" | "12" => Self::Usr2,
            "PIPE" | "13" => Self::Pipe,
            "ALRM" | "14" => Self::Alrm,
            "TERM" | "15" => Self::Term,
            "STKFLT" | "16" => Self::Stkflt,
            "CHLD" | "CLD" | "17" => Self::Chld,
            "CONT" | "18" => Self::Cont,
            "STOP" | "19" => Self::Stop,
            "TSTP" | "20" => Self::Tstp,
            "TTIN" | "21" => Self::Ttin,
            "TTOU" | "22" => Self::Ttou,
            "URG" | "23" => Self::Urg,
            "XCPU" | "24" => Self::Xcpu,
            "XFSZ" | "25" => Self::Xfsz,
            "VTALRM" | "26" => Self::Vtalrm,
            "PROF" | "27" => Self::Prof,
            "WINCH" | "28" => Self::Winch,
            "IO" | "POLL" | "29" => Self::Io,
            "PWR" | "30" => Self::Pwr,
            "SYS" | "UNUSED" | "31" => Self::Sys,
            _ => return None,
        })
    }

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Hup => "HUP",
            Self::Int => "INT",
            Self::Quit => "QUIT",
            Self::Ill => "ILL",
            Self::Trap => "TRAP",
            Self::Abrt => "ABRT",
            Self::Bus => "BUS",
            Self::Fpe => "FPE",
            Self::Kill => "KILL",
            Self::Usr1 => "USR1",
            Self::Segv => "SEGV",
            Self::Usr2 => "USR2",
            Self::Pipe => "PIPE",
            Self::Alrm => "ALRM",
            Self::Term => "TERM",
            Self::Stkflt => "STKFLT",
            Self::Chld => "CHLD",
            Self::Cont => "CONT",
            Self::Stop => "STOP",
            Self::Tstp => "TSTP",
            Self::Ttin => "TTIN",
            Self::Ttou => "TTOU",
            Self::Urg => "URG",
            Self::Xcpu => "XCPU",
            Self::Xfsz => "XFSZ",
            Self::Vtalrm => "VTALRM",
            Self::Prof => "PROF",
            Self::Winch => "WINCH",
            Self::Io => "IO",
            Self::Pwr => "PWR",
            Self::Sys => "SYS",
        }
    }

    #[must_use]
    pub const fn number(self) -> i32 {
        self as i32
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessSignalParams {
    pub serial: String,
    pub pid: u32,
    pub signal: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogTailParams {
    pub serial: String,
    pub cursor: Option<u64>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub cursor: u64,
    pub level: String,
    pub source: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogSnapshot {
    pub entries: Vec<LogEntry>,
    pub next_cursor: u64,
    pub has_more: bool,
}

/// Turns a decoded JSON value into a validated JSON-RPC request.
pub fn parse_request_value(value: Value) -> Result<RpcRequest, RpcError> {
    let object = value.as_object().ok_or_else(RpcError::invalid_request)?;
    validate_request_members(object)?;
    serde_json::from_value(value).map_err(|_| RpcError::invalid_request())
}

fn validate_request_members(object: &Map<String, Value>) -> Result<(), RpcError> {
    if object.get("jsonrpc") != Some(&Value::String(JSONRPC_VERSION.to_owned())) {
        return Err(RpcError::invalid_request());
    }
    if !object.get("method").is_some_and(Value::is_string) {
        return Err(RpcError::invalid_request());
    }
    if object
        .get("params")
        .is_some_and(|params| !params.is_object() && !params.is_array())
    {
        return Err(RpcError::invalid_request());
    }
    if let Some(id) = object.get("id") {
        let valid = id.is_null() || id.is_string() || id.as_i64().is_some();
        if !valid {
            return Err(RpcError::invalid_request());
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum FramingError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("malformed LSP header")]
    MalformedHeader,
    #[error("Content-Length header is missing")]
    MissingContentLength,
    #[error("duplicate Content-Length header")]
    DuplicateContentLength,
    #[error("message length {actual} exceeds limit {limit}")]
    ContentTooLarge { actual: usize, limit: usize },
    #[error("message body ended before Content-Length bytes were read")]
    TruncatedBody,
    #[error("JSON serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Reads one `Content-Length` framed payload. EOF before a header returns `None`.
pub fn read_frame<R: BufRead>(
    reader: &mut R,
    max_content_length: usize,
) -> Result<Option<Vec<u8>>, FramingError> {
    let mut content_length = None;
    let mut saw_header = false;

    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            if saw_header {
                return Err(FramingError::MalformedHeader);
            }
            return Ok(None);
        }
        saw_header = true;

        if line == "\r\n" || line == "\n" {
            break;
        }
        let line = line.trim_end_matches(['\r', '\n']);
        let (name, value) = line.split_once(':').ok_or(FramingError::MalformedHeader)?;
        if name.eq_ignore_ascii_case("Content-Length") {
            if content_length.is_some() {
                return Err(FramingError::DuplicateContentLength);
            }
            let length = value
                .trim()
                .parse::<usize>()
                .map_err(|_| FramingError::MalformedHeader)?;
            if length > max_content_length {
                return Err(FramingError::ContentTooLarge {
                    actual: length,
                    limit: max_content_length,
                });
            }
            content_length = Some(length);
        }
    }

    let length = content_length.ok_or(FramingError::MissingContentLength)?;
    let mut payload = vec![0; length];
    if let Err(error) = reader.read_exact(&mut payload) {
        return if error.kind() == io::ErrorKind::UnexpectedEof {
            Err(FramingError::TruncatedBody)
        } else {
            Err(FramingError::Io(error))
        };
    }
    Ok(Some(payload))
}

pub fn write_frame<W: Write>(writer: &mut W, payload: &[u8]) -> Result<(), FramingError> {
    write!(writer, "Content-Length: {}\r\n\r\n", payload.len())?;
    writer.write_all(payload)?;
    writer.flush()?;
    Ok(())
}

pub fn read_json_frame<R: BufRead>(
    reader: &mut R,
    max_content_length: usize,
) -> Result<Option<Value>, FramingError> {
    read_frame(reader, max_content_length)?
        .map(|payload| serde_json::from_slice(&payload).map_err(FramingError::from))
        .transpose()
}

pub fn write_json_frame<W: Write, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), FramingError> {
    write_frame(writer, &serde_json::to_vec(value)?)
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    Framing(#[from] FramingError),
    #[error("server closed the RPC stream")]
    ServerClosed,
    #[error("invalid JSON-RPC response")]
    InvalidResponse,
    #[error("response id does not match request id")]
    MismatchedId,
    #[error(transparent)]
    Rpc(#[from] RpcError),
}

pub struct RpcClient<R, W> {
    reader: R,
    writer: W,
    next_id: i64,
    max_content_length: usize,
}

impl<R: BufRead, W: Write> RpcClient<R, W> {
    #[must_use]
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            next_id: 1,
            max_content_length: DEFAULT_MAX_CONTENT_LENGTH,
        }
    }

    #[must_use]
    pub fn with_max_content_length(mut self, max_content_length: usize) -> Self {
        self.max_content_length = max_content_length;
        self
    }

    pub fn call(&mut self, method: &str, params: Option<Value>) -> Result<Value, ClientError> {
        self.call_with_notifications(method, params, |_| Err(ClientError::InvalidResponse))
    }

    pub fn call_with_notifications(
        &mut self,
        method: &str,
        params: Option<Value>,
        mut notification: impl FnMut(&RpcRequest) -> Result<(), ClientError>,
    ) -> Result<Value, ClientError> {
        let id = RequestId::Number(self.next_id);
        self.next_id = self.next_id.checked_add(1).unwrap_or(1);
        let request = RpcRequest::call(id.clone(), method, params);
        write_json_frame(&mut self.writer, &request)?;

        loop {
            let value = read_json_frame(&mut self.reader, self.max_content_length)?
                .ok_or(ClientError::ServerClosed)?;
            if value.get("method").is_some() {
                let request = parse_request_value(value).map_err(ClientError::Rpc)?;
                if request.id.is_some() {
                    return Err(ClientError::InvalidResponse);
                }
                notification(&request)?;
                continue;
            }
            let response: RpcResponse =
                serde_json::from_value(value).map_err(|_| ClientError::InvalidResponse)?;
            if response.jsonrpc != JSONRPC_VERSION {
                return Err(ClientError::InvalidResponse);
            }
            if response.id != id {
                return Err(ClientError::MismatchedId);
            }
            return response.into_result().map_err(ClientError::Rpc);
        }
    }

    pub fn into_parts(self) -> (R, W) {
        (self.reader, self.writer)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use super::*;

    #[test]
    fn framing_round_trip() {
        let request = RpcRequest::call(RequestId::Number(7), methods::SERVER_PING, None);
        let mut wire = Vec::new();
        write_json_frame(&mut wire, &request).unwrap();

        let mut reader = BufReader::new(Cursor::new(wire));
        let decoded = read_json_frame(&mut reader, DEFAULT_MAX_CONTENT_LENGTH)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_value::<RpcRequest>(decoded).unwrap(),
            request
        );
    }

    #[test]
    fn framing_rejects_duplicate_length() {
        let input = b"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}";
        let error = read_frame(&mut Cursor::new(input), 32).unwrap_err();
        assert!(matches!(error, FramingError::DuplicateContentLength));
    }

    #[test]
    fn framing_rejects_large_messages_before_allocating() {
        let input = b"Content-Length: 999\r\n\r\n";
        let error = read_frame(&mut Cursor::new(input), 32).unwrap_err();
        assert!(matches!(
            error,
            FramingError::ContentTooLarge {
                actual: 999,
                limit: 32
            }
        ));
    }

    #[test]
    fn invalid_version_is_an_invalid_request() {
        let error = parse_request_value(json!({
            "jsonrpc": "1.0",
            "method": methods::SERVER_PING,
            "id": 1
        }))
        .unwrap_err();
        assert_eq!(error.code, error_codes::INVALID_REQUEST);
    }

    #[test]
    fn sync_default_preserves_interactive_scheduling_opportunities() {
        assert_eq!(DEFAULT_SYNC_BLOCK_SIZE, 256 * 1024);
        assert_eq!(MAX_SYNC_BLOCK_SIZE, 1024 * 1024);
    }

    #[test]
    fn rpc_client_delivers_notifications_before_the_final_result() {
        let mut input = Vec::new();
        write_json_frame(
            &mut input,
            &RpcRequest::notification(
                methods::SYNC_PROGRESS,
                Some(json!({
                    "operation_id": "operation",
                    "direction": "push",
                    "remote_path": "file.bin",
                    "phase": "transferring",
                    "transferred": 5,
                    "total": 10
                })),
            ),
        )
        .unwrap();
        write_json_frame(
            &mut input,
            &RpcResponse::success(RequestId::Number(1), json!({ "done": true })),
        )
        .unwrap();
        let mut client = RpcClient::new(BufReader::new(Cursor::new(input)), Vec::new());
        let mut notifications = Vec::new();

        let result = client
            .call_with_notifications(methods::SYNC_PUSH_STREAM, None, |request| {
                notifications.push(request.method.clone());
                Ok(())
            })
            .unwrap();

        assert_eq!(notifications, [methods::SYNC_PROGRESS]);
        assert_eq!(result, json!({ "done": true }));
    }

    #[test]
    fn process_signals_accept_linux_names_aliases_and_numbers() {
        assert_eq!(ProcessSignal::parse("term"), Some(ProcessSignal::Term));
        assert_eq!(ProcessSignal::parse("SIGKILL"), Some(ProcessSignal::Kill));
        assert_eq!(ProcessSignal::parse("CLD"), Some(ProcessSignal::Chld));
        assert_eq!(ProcessSignal::parse("29"), Some(ProcessSignal::Io));
        assert_eq!(ProcessSignal::parse("0"), None);
        assert_eq!(ProcessSignal::parse("SIGRTMIN"), None);
        assert_eq!(ProcessSignal::Term.name(), "TERM");
        assert_eq!(ProcessSignal::Term.number(), 15);
    }

    #[test]
    fn rpc_error_display_includes_actionable_detail() {
        let error =
            RpcError::invalid_params("remote_path must be relative to /mnt/us/kindlebridge-data");
        assert_eq!(
            error.to_string(),
            "Invalid params (-32602): remote_path must be relative to /mnt/us/kindlebridge-data"
        );
    }
}
