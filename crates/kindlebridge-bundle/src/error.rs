use std::io;

/// Stable broad error categories suitable for mapping to CLI/RPC errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorCode {
    Io,
    Header,
    Bounds,
    CanonicalCbor,
    Schema,
    ProfilePolicy,
    Path,
    Tree,
    Block,
    Signature,
    Publisher,
    Target,
    Activation,
    Quota,
    Conflict,
    Recovery,
}

#[derive(Debug, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
    #[source]
    source: Option<io::Error>,
}

impl Error {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn io(error: io::Error) -> Self {
        Self {
            code: ErrorCode::Io,
            message: error.to_string(),
            source: Some(error),
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::io(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) trait ResultExt<T> {
    fn context(self, code: ErrorCode, message: impl Into<String>) -> Result<T>;
}

impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: std::fmt::Display,
{
    fn context(self, code: ErrorCode, message: impl Into<String>) -> Result<T> {
        self.map_err(|error| Error::new(code, format!("{}: {error}", message.into())))
    }
}
