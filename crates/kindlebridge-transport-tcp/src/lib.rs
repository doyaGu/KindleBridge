//! Bounded KBP framing over byte streams, plus a `std::net::TcpStream` adapter.
//!
//! This crate does **not** implement TLS or authenticate peers. [`TcpFrameStream`]
//! is intentionally named as a plain TCP utility and is suitable for bootstrap
//! code and tests. Production session code should authenticate/encrypt the byte
//! stream first, implement [`AuthenticatedStream`] on that wrapper, and then use
//! [`AuthenticatedFramed`]. KBP framing is inside that authenticated stream.

mod config;
mod error;
mod framing;
mod tcp;

pub use config::{
    TransportConfig, DEFAULT_CONNECT_TIMEOUT, DEFAULT_IO_TIMEOUT, DEFAULT_MAX_PAYLOAD,
    HARD_MAX_PAYLOAD,
};
pub use error::{ErrorClass, IoOperation, TransportError};
pub use framing::{FrameIo, FrameReader, FrameWriter, FramedStream, SplitFrameStream};
pub use tcp::{
    AuthenticatedFramed, AuthenticatedStream, ShutdownMode, TcpFrameListener, TcpFrameStream,
};

#[cfg(test)]
mod tests;
