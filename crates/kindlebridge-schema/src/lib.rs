//! Serialized types for the KindleBridge host API and device protocol.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use thiserror::Error;

pub mod device_protocol;
pub mod shell_protocol;

pub const JSONRPC_VERSION: &str = "2.0";
pub const API_VERSION: &str = "v1";
pub const DEFAULT_MAX_CONTENT_LENGTH: usize = 8 * 1024 * 1024;

pub mod methods {
    pub const SERVER_PING: &str = "v1.server.ping";
    pub const SERVER_VERSION: &str = "v1.server.version";
    pub const DEVICE_LIST: &str = "v1.device.list";
    pub const DEVICE_FEATURES: &str = "v1.device.features";
    pub const EXEC_RUN: &str = "v1.exec.run";
    pub const SYNC_PUSH: &str = "v1.sync.push";
    pub const SYNC_PULL: &str = "v1.sync.pull";
    pub const SYNC_STATUS: &str = "v1.sync.status";
    pub const APP_INSTALL: &str = "v1.app.install";
    pub const APP_START: &str = "v1.app.start";
    pub const APP_STOP: &str = "v1.app.stop";
    pub const APP_RESTART: &str = "v1.app.restart";
    pub const APP_ROLLBACK: &str = "v1.app.rollback";
    pub const APP_UNINSTALL: &str = "v1.app.uninstall";
    pub const APP_LIST: &str = "v1.app.list";
    pub const PROCESS_LIST: &str = "v1.process.list";
    pub const PROCESS_SIGNAL: &str = "v1.process.signal";
    pub const LOG_TAIL: &str = "v1.log.tail";
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
    pub const APP_NOT_FOUND: i64 = -32_020;
    pub const NO_ROLLBACK_AVAILABLE: i64 = -32_021;
    pub const PROCESS_NOT_FOUND: i64 = -32_030;
    pub const INVALID_SIGNAL: i64 = -32_031;
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
pub const DEFAULT_SYNC_BLOCK_SIZE: u32 = MAX_SYNC_BLOCK_SIZE;

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
pub enum AppState {
    Unknown,
    Stopped,
    Running,
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
pub struct AppInstallParams {
    pub serial: String,
    pub app_id: String,
    pub version: String,
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
        let id = RequestId::Number(self.next_id);
        self.next_id = self.next_id.checked_add(1).unwrap_or(1);
        let request = RpcRequest::call(id.clone(), method, params);
        write_json_frame(&mut self.writer, &request)?;

        let value = read_json_frame(&mut self.reader, self.max_content_length)?
            .ok_or(ClientError::ServerClosed)?;
        let response: RpcResponse =
            serde_json::from_value(value).map_err(|_| ClientError::InvalidResponse)?;
        if response.jsonrpc != JSONRPC_VERSION {
            return Err(ClientError::InvalidResponse);
        }
        if response.id != id {
            return Err(ClientError::MismatchedId);
        }
        response.into_result().map_err(ClientError::Rpc)
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
    fn sync_defaults_to_a_full_usb_transfer_batch() {
        assert_eq!(DEFAULT_SYNC_BLOCK_SIZE, MAX_SYNC_BLOCK_SIZE);
        assert_eq!(DEFAULT_SYNC_BLOCK_SIZE, 1024 * 1024);
    }

    #[test]
    fn unknown_app_state_has_a_stable_wire_value() {
        assert_eq!(serde_json::to_value(AppState::Unknown).unwrap(), "unknown");
        assert_eq!(
            serde_json::from_str::<AppState>("\"unknown\"").unwrap(),
            AppState::Unknown
        );
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
