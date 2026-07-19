//! Typed payloads carried inside the KBP host-to-device session.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RpcError;

pub const PROTOCOL_VERSION: u32 = 2;
pub const SESSION_ID_HEX_LENGTH: usize = 32;
pub const DEFAULT_CONNECTION_WINDOW: u32 = 16 * 1024 * 1024;
pub const DEFAULT_STREAM_WINDOW: u32 = 8 * 1024 * 1024;
/// Largest payload the official host may place in one host-to-device frame.
///
/// Recovery must be able to overwrite one abandoned outbound frame, so this is
/// deliberately independent from the larger connection credit window.
pub const MAX_HOST_TO_DEVICE_PAYLOAD: u32 = 1024 * 1024;
pub const SYNC_CREDIT_BATCH_SIZE: u32 = DEFAULT_STREAM_WINDOW / 2;
pub const SHELL_SERVICE: &str = "shell.v1";
pub const SYNC_SERVICE: &str = "sync.v1";
pub const EXEC_FEATURE: &str = "exec.v1";
pub const SYNC_FEATURE: &str = "sync.v1";

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
    fn sync_credit_batch_keeps_half_the_stream_window_available() {
        assert_eq!(SYNC_CREDIT_BATCH_SIZE, 4 * 1024 * 1024);
        assert_eq!(SYNC_CREDIT_BATCH_SIZE * 2, DEFAULT_STREAM_WINDOW);
    }
}
