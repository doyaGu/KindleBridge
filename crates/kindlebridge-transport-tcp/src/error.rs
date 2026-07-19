use std::{fmt, io};

use kindlebridge_wire::WireError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoOperation {
    Bind,
    Accept,
    Connect,
    Configure,
    ReadHeader,
    ReadPayload,
    WriteHeader,
    WritePayload,
    Flush,
    Shutdown,
    Address,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClass {
    CleanEof,
    Timeout,
    Truncated,
    Protocol,
    ResourceLimit,
    Configuration,
    Io,
}

#[derive(Debug)]
pub enum TransportError {
    EndOfStream,
    TruncatedHeader {
        received: usize,
    },
    TruncatedPayload {
        expected: usize,
        received: usize,
    },
    Wire(WireError),
    ConfiguredPayloadLimitTooLarge {
        configured: u32,
        hard_limit: u32,
    },
    PayloadExceedsHardLimit {
        length: u32,
        hard_limit: u32,
    },
    PayloadAllocation {
        length: usize,
    },
    FrameLengthMismatch {
        declared: u32,
        actual: usize,
    },
    ZeroTimeout(&'static str),
    Io {
        operation: IoOperation,
        source: io::Error,
    },
}

impl TransportError {
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::EndOfStream => ErrorClass::CleanEof,
            Self::TruncatedHeader { .. } | Self::TruncatedPayload { .. } => ErrorClass::Truncated,
            Self::Wire(WireError::PayloadTooLarge { .. })
            | Self::Wire(WireError::CreditTooLarge { .. })
            | Self::PayloadExceedsHardLimit { .. }
            | Self::PayloadAllocation { .. } => ErrorClass::ResourceLimit,
            Self::Wire(_) | Self::FrameLengthMismatch { .. } => ErrorClass::Protocol,
            Self::ConfiguredPayloadLimitTooLarge { .. } | Self::ZeroTimeout(_) => {
                ErrorClass::Configuration
            }
            Self::Io { source, .. }
                if matches!(
                    source.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                ErrorClass::Timeout
            }
            Self::Io { .. } => ErrorClass::Io,
        }
    }

    pub const fn operation(&self) -> Option<IoOperation> {
        match self {
            Self::Io { operation, .. } => Some(*operation),
            _ => None,
        }
    }

    pub(crate) fn io(operation: IoOperation, source: io::Error) -> Self {
        Self::Io { operation, source }
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EndOfStream => write!(formatter, "end of transport stream"),
            Self::TruncatedHeader { received } => {
                write!(formatter, "truncated KBP header after {received} bytes")
            }
            Self::TruncatedPayload { expected, received } => write!(
                formatter,
                "truncated KBP payload: expected {expected} bytes, got {received}"
            ),
            Self::Wire(error) => error.fmt(formatter),
            Self::ConfiguredPayloadLimitTooLarge {
                configured,
                hard_limit,
            } => write!(
                formatter,
                "configured payload limit {configured} exceeds hard limit {hard_limit}"
            ),
            Self::PayloadExceedsHardLimit { length, hard_limit } => write!(
                formatter,
                "payload length {length} exceeds hard limit {hard_limit}"
            ),
            Self::PayloadAllocation { length } => {
                write!(formatter, "cannot allocate {length}-byte KBP payload")
            }
            Self::FrameLengthMismatch { declared, actual } => write!(
                formatter,
                "frame declares {declared} payload bytes but contains {actual}"
            ),
            Self::ZeroTimeout(name) => write!(formatter, "{name} timeout must be non-zero"),
            Self::Io { operation, source } => write!(formatter, "{operation:?} failed: {source}"),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wire(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<WireError> for TransportError {
    fn from(value: WireError) -> Self {
        Self::Wire(value)
    }
}
