use std::time::Duration;

use kindlebridge_wire::DecodeLimits;

use crate::TransportError;

pub const DEFAULT_MAX_PAYLOAD: u32 = 16 * 1024 * 1024;
pub const HARD_MAX_PAYLOAD: u32 = 64 * 1024 * 1024;
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(30);

/// Framing limits and socket behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportConfig {
    pub limits: DecodeLimits,
    pub connect_timeout: Duration,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub nodelay: bool,
}

impl TransportConfig {
    pub const fn new(limits: DecodeLimits) -> Self {
        Self {
            limits,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            read_timeout: Some(DEFAULT_IO_TIMEOUT),
            write_timeout: Some(DEFAULT_IO_TIMEOUT),
            nodelay: true,
        }
    }

    pub fn validate(self) -> Result<Self, TransportError> {
        if self.limits.max_payload > HARD_MAX_PAYLOAD {
            return Err(TransportError::ConfiguredPayloadLimitTooLarge {
                configured: self.limits.max_payload,
                hard_limit: HARD_MAX_PAYLOAD,
            });
        }
        if self.connect_timeout.is_zero() {
            return Err(TransportError::ZeroTimeout("connect"));
        }
        if self.read_timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(TransportError::ZeroTimeout("read"));
        }
        if self.write_timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(TransportError::ZeroTimeout("write"));
        }
        Ok(self)
    }
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self::new(DecodeLimits::new(DEFAULT_MAX_PAYLOAD, DEFAULT_MAX_PAYLOAD))
    }
}
