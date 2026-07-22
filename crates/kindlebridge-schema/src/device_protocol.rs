//! Typed payloads carried inside the KBP host-to-device session.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RpcError;

pub const PROTOCOL_VERSION: u32 = 3;
pub const SESSION_ID_HEX_LENGTH: usize = 32;
pub const DEFAULT_CONNECTION_WINDOW: u32 = 16 * 1024 * 1024;
pub const DEFAULT_STREAM_WINDOW: u32 = 8 * 1024 * 1024;
pub const SHELL_STREAM_WINDOW: u32 = 256 * 1024;
/// Largest payload the official host may place in one host-to-device frame.
///
/// Recovery must be able to overwrite one abandoned outbound frame, so this is
/// deliberately independent from the larger connection credit window.
pub const MAX_HOST_TO_DEVICE_PAYLOAD: u32 = 1024 * 1024;
pub const SYNC_CREDIT_BATCH_SIZE: u32 = DEFAULT_STREAM_WINDOW / 2;
pub const RPC_SERVICE: &str = "rpc.v1";
pub const SHELL_V2_SERVICE: &str = "shell.v2";
pub const SYNC_SERVICE: &str = "sync.v1";
pub const APP_INSTALL_FEATURE: &str = "app.install.v1";
pub const APP_LIST_FEATURE: &str = "app.list.v1";
pub const APP_RESTART_FEATURE: &str = "app.restart.v1";
pub const APP_ROLLBACK_FEATURE: &str = "app.rollback.v1";
pub const APP_START_FEATURE: &str = "app.start.v1";
pub const APP_STOP_FEATURE: &str = "app.stop.v1";
pub const APP_UNINSTALL_FEATURE: &str = "app.uninstall.v1";
pub const EXEC_FEATURE: &str = "exec.v1";
pub const LOG_TAIL_FEATURE: &str = "log.tail.v1";
pub const PROCESS_LIST_FEATURE: &str = "process.list.v1";
pub const PROCESS_SIGNAL_FEATURE: &str = "process.signal.v1";
pub const SYNC_FEATURE: &str = "sync.v1";
pub const SHELL_V2_FEATURE: &str = SHELL_V2_SERVICE;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShellMode {
    Pty,
    Raw,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalSize {
    pub rows: u16,
    pub columns: u16,
    pub pixel_width: u16,
    pub pixel_height: u16,
}

impl TerminalSize {
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.rows != 0 && self.columns != 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellOpen {
    pub mode: ShellMode,
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_size: Option<TerminalSize>,
    pub cwd: String,
    pub term: String,
}

impl ShellOpen {
    #[must_use]
    pub fn interactive(terminal_size: TerminalSize) -> Self {
        Self {
            mode: ShellMode::Pty,
            argv: vec!["/bin/sh".to_owned(), "-l".to_owned()],
            terminal_size: Some(terminal_size),
            cwd: "/tmp/root".to_owned(),
            term: "linux".to_owned(),
        }
    }

    #[must_use]
    pub fn command(command: impl Into<String>) -> Self {
        Self {
            mode: ShellMode::Raw,
            argv: vec!["/bin/sh".to_owned(), "-lc".to_owned(), command.into()],
            terminal_size: None,
            cwd: "/tmp/root".to_owned(),
            term: "linux".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostHello {
    pub protocol_version: u32,
    pub session_id: String,
    pub client_name: String,
    pub initial_connection_window: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceHello {
    pub protocol_version: u32,
    pub session_id: String,
    pub serial: String,
    pub model: String,
    pub firmware: String,
    pub target: String,
    pub features: Vec<String>,
    pub initial_connection_window: u32,
}

#[must_use]
pub fn is_valid_session_id(value: &str) -> bool {
    value.len() == SESSION_ID_HEX_LENGTH
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[must_use]
pub fn is_valid_transfer_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceOpen {
    pub service: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccept {
    pub initial_stream_window: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceCall {
    pub method: String,
    pub params: Value,
}

/// Device-internal app install request. The public host API accepts a local
/// `bundle_path`; the shared host server uploads it and sends only this bounded
/// staging reference across KBP.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceAppInstallParams {
    pub serial: String,
    pub remote_path: String,
    pub file_hash: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DeviceReply {
    Success { result: Value },
    Failure { error: RpcError },
}

/// Metadata that opens a raw-byte sync stream. File bytes follow in DATA frames.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum SyncRequest {
    Push {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transfer_id: Option<String>,
        remote_path: String,
        total_size: u64,
        file_hash: String,
        block_size: u32,
    },
    Pull {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transfer_id: Option<String>,
        remote_path: String,
        offset: u64,
        block_size: u32,
    },
}

/// Control messages returned on a sync stream. Bulk data is never encoded here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SyncReply {
    Ready {
        transfer_id: String,
        offset: u64,
        total_size: u64,
        file_hash: String,
    },
    Complete {
        transfer_id: String,
        next_offset: u64,
        total_size: u64,
    },
    Failure {
        error: RpcError,
    },
}

impl DeviceReply {
    #[must_use]
    pub const fn success(result: Value) -> Self {
        Self::Success { result }
    }

    #[must_use]
    pub const fn failure(error: RpcError) -> Self {
        Self::Failure { error }
    }

    pub fn into_result(self) -> Result<Value, RpcError> {
        match self {
            Self::Success { result } => Ok(result),
            Self::Failure { error } => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn device_reply_is_explicitly_tagged_and_round_trips() {
        let reply = DeviceReply::success(json!({ "exit_code": 0 }));
        let value = serde_json::to_value(&reply).unwrap();
        assert_eq!(value["status"], "success");
        assert_eq!(serde_json::from_value::<DeviceReply>(value).unwrap(), reply);
    }

    #[test]
    fn hello_rejects_unknown_fields() {
        assert!(serde_json::from_value::<HostHello>(json!({
            "protocol_version": PROTOCOL_VERSION,
            "session_id": "000102030405060708090a0b0c0d0e0f",
            "client_name": "test",
            "initial_connection_window": DEFAULT_CONNECTION_WINDOW,
            "unexpected": true
        }))
        .is_err());
    }

    #[test]
    fn session_ids_are_fixed_width_lowercase_hex() {
        assert!(is_valid_session_id("000102030405060708090a0b0c0d0e0f"));
        assert!(!is_valid_session_id("000102030405060708090A0B0C0D0E0F"));
        assert!(!is_valid_session_id("short"));
    }

    #[test]
    fn transfer_ids_are_bounded_filename_safe_tokens() {
        assert!(is_valid_transfer_id("pull-0123456789abcdef_test"));
        assert!(!is_valid_transfer_id(""));
        assert!(!is_valid_transfer_id("hop/../escape"));
        assert!(!is_valid_transfer_id("hop\\..\\escape"));
        assert!(!is_valid_transfer_id(&"x".repeat(129)));
    }

    #[test]
    fn sync_credit_batch_keeps_half_the_stream_window_available() {
        assert_eq!(SYNC_CREDIT_BATCH_SIZE, 4 * 1024 * 1024);
        assert_eq!(SYNC_CREDIT_BATCH_SIZE * 2, DEFAULT_STREAM_WINDOW);
    }

    #[test]
    fn device_app_install_uses_a_staged_file_not_claimed_metadata() {
        let params: DeviceAppInstallParams = serde_json::from_value(json!({
            "serial": "KT6",
            "remote_path": "packages/install-abc.kbb",
            "file_hash": "00".repeat(32),
        }))
        .unwrap();
        assert_eq!(params.remote_path, "packages/install-abc.kbb");
        assert!(serde_json::from_value::<DeviceAppInstallParams>(json!({
            "serial": "KT6",
            "remote_path": "packages/install-abc.kbb",
            "file_hash": "00".repeat(32),
            "app_id": "org.example.forged",
        }))
        .is_err());
    }
}
