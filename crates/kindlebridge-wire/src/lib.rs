//! Wire framing and connection state for Kindle Bridge Protocol (KBP) v1.
//!
//! This crate deliberately does not interpret service payloads. Callers decode
//! HELLO and ACCEPT payloads and pass their negotiated windows to
//! [`SessionState`] through [`FrameContext`].

mod error;
mod frame;
mod state;

pub use error::{ProtocolError, WireError};
pub use frame::{
    crc32c, Command, DecodeLimits, Frame, Header, FLAG_END_STREAM, FLAG_URGENT, HEADER_LEN, MAGIC,
    PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
pub use state::{
    Direction, EndpointRole, FrameContext, SessionConfig, SessionPhase, SessionSnapshot,
    SessionState, StreamPhase, StreamSnapshot,
};

#[cfg(test)]
mod tests;
