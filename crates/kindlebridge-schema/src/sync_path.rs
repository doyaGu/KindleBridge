//! Logical paths below the KindleBridge sync root.

use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

const MAX_PATH_BYTES: usize = 1_024;
const MAX_COMPONENT_BYTES: usize = 255;

/// A validated location below the KindleBridge sync root.
///
/// This type intentionally does not implement Serde traits. Wire messages keep
/// using strings so each Adapter must explicitly choose how validation errors
/// are exposed.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalSyncPath(String);

impl LogicalSyncPath {
    pub fn parse(value: impl Into<String>) -> Result<Self, LogicalSyncPathError> {
        let value = value.into();
        if value.is_empty() || value.starts_with('/') || value.contains('\\') {
            return Err(LogicalSyncPathError::NotRelative);
        }
        if value.len() > MAX_PATH_BYTES {
            return Err(LogicalSyncPathError::TooLong);
        }
        if value.chars().any(char::is_control) {
            return Err(LogicalSyncPathError::ControlCharacter);
        }
        if value.nfc().ne(value.chars()) {
            return Err(LogicalSyncPathError::NotNfc);
        }
        for component in value.split('/') {
            if component.is_empty() || matches!(component, "." | "..") {
                return Err(LogicalSyncPathError::InvalidComponent);
            }
            if component.len() > MAX_COMPONENT_BYTES {
                return Err(LogicalSyncPathError::ComponentTooLong);
            }
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }

    #[must_use]
    pub fn ascii_case_fold_key(&self) -> String {
        self.0.to_ascii_lowercase()
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum LogicalSyncPathError {
    #[error("path must be non-empty, relative, and use forward slashes")]
    NotRelative,
    #[error("path exceeds 1024 UTF-8 bytes")]
    TooLong,
    #[error("path component exceeds 255 UTF-8 bytes")]
    ComponentTooLong,
    #[error("path contains an empty, dot, or dot-dot component")]
    InvalidComponent,
    #[error("path contains a control character")]
    ControlCharacter,
    #[error("path is not Unicode NFC")]
    NotNfc,
}
