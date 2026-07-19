use core::fmt;

use crate::Command;

/// A stateless frame encoding or decoding failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireError {
    HeaderTooShort { actual: usize },
    FrameLengthMismatch { expected: usize, actual: usize },
    InvalidMagic([u8; 4]),
    InvalidHeaderSize(u16),
    UnsupportedMajor(u16),
    UnknownCommand(u16),
    HeaderCrcMismatch { expected: u32, actual: u32 },
    PayloadTooLarge { length: u32, maximum: u32 },
    CreditTooLarge { delta: u32, maximum: u32 },
    CreditPayloadNotEmpty(u32),
    InvalidCreditDelta(u32),
    UnexpectedCreditDelta { command: Command, delta: u32 },
    UnknownCriticalFlags(u32),
    EndStreamOnNonData(Command),
    InvalidStreamForCommand { command: Command, stream_id: u32 },
    ReservedNotZero(u32),
    PayloadLengthOverflow(usize),
}

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeaderTooShort { actual } => {
                write!(formatter, "KBP header needs 40 bytes, got {actual}")
            }
            Self::FrameLengthMismatch { expected, actual } => {
                write!(
                    formatter,
                    "KBP frame length is {actual}, expected {expected}"
                )
            }
            Self::InvalidMagic(magic) => write!(formatter, "invalid KBP magic {magic:?}"),
            Self::InvalidHeaderSize(size) => write!(formatter, "invalid KBP header size {size}"),
            Self::UnsupportedMajor(major) => {
                write!(formatter, "unsupported KBP protocol major {major}")
            }
            Self::UnknownCommand(command) => write!(formatter, "unknown KBP command {command}"),
            Self::HeaderCrcMismatch { expected, actual } => write!(
                formatter,
                "KBP header CRC mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::PayloadTooLarge { length, maximum } => {
                write!(
                    formatter,
                    "payload length {length} exceeds maximum {maximum}"
                )
            }
            Self::CreditTooLarge { delta, maximum } => {
                write!(formatter, "credit delta {delta} exceeds maximum {maximum}")
            }
            Self::CreditPayloadNotEmpty(length) => {
                write!(
                    formatter,
                    "CREDIT payload must be empty, got {length} bytes"
                )
            }
            Self::InvalidCreditDelta(delta) => {
                write!(formatter, "CREDIT delta must be non-zero, got {delta}")
            }
            Self::UnexpectedCreditDelta { command, delta } => {
                write!(
                    formatter,
                    "{command:?} must have zero credit delta, got {delta}"
                )
            }
            Self::UnknownCriticalFlags(flags) => {
                write!(formatter, "unknown critical flags {flags:#010x}")
            }
            Self::EndStreamOnNonData(command) => {
                write!(formatter, "END_STREAM is invalid on {command:?}")
            }
            Self::InvalidStreamForCommand { command, stream_id } => {
                write!(formatter, "{command:?} is invalid on stream {stream_id}")
            }
            Self::ReservedNotZero(value) => {
                write!(formatter, "reserved header field must be zero, got {value}")
            }
            Self::PayloadLengthOverflow(length) => {
                write!(formatter, "payload length {length} does not fit in u32")
            }
        }
    }
}

impl std::error::Error for WireError {}

/// A stateful protocol violation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    Wire(WireError),
    UnexpectedCommand {
        phase: &'static str,
        command: Command,
        stream_id: u32,
    },
    Sequence {
        stream_id: u32,
        expected: u32,
        actual: u32,
    },
    SequenceExhausted {
        stream_id: u32,
    },
    WrongStreamParity {
        stream_id: u32,
        expected_odd: bool,
    },
    StreamAlreadyUsed(u32),
    UnknownStream(u32),
    StreamNotOpening(u32),
    StreamNotAccepted(u32),
    WrongOpeningResponder(u32),
    CloseByInitiator(u32),
    DataAfterEnd(u32),
    SendCreditExceeded {
        stream_id: u32,
        needed: u32,
        available: u32,
    },
    ReceiveCreditExceeded {
        stream_id: u32,
        needed: u32,
        available: u32,
    },
    ConnectionSendCreditExceeded {
        needed: u32,
        available: u32,
    },
    ConnectionReceiveCreditExceeded {
        needed: u32,
        available: u32,
    },
    CreditOverflow {
        stream_id: u32,
        current: u32,
        delta: u32,
        maximum: u32,
    },
    MissingConnectionWindow,
    MissingStreamWindow(u32),
    UnexpectedConnectionWindow(Command),
    UnexpectedStreamWindow(Command),
    InvalidWindow {
        window: u32,
        maximum: u32,
    },
    PairingFinishOnRegularSession,
    DuplicateHello,
    DuplicatePairingFinish,
    StreamIdExhausted,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wire(error) => error.fmt(formatter),
            Self::UnexpectedCommand {
                phase,
                command,
                stream_id,
            } => write!(
                formatter,
                "unexpected {command:?} on stream {stream_id} during {phase}"
            ),
            Self::Sequence {
                stream_id,
                expected,
                actual,
            } => write!(
                formatter,
                "sequence error on stream {stream_id}: expected {expected}, got {actual}"
            ),
            Self::SequenceExhausted { stream_id } => {
                write!(formatter, "sequence exhausted on stream {stream_id}")
            }
            Self::WrongStreamParity {
                stream_id,
                expected_odd,
            } => write!(
                formatter,
                "stream {stream_id} has wrong parity; expected {}",
                if *expected_odd { "odd" } else { "even" }
            ),
            Self::StreamAlreadyUsed(id) => write!(formatter, "stream {id} was already used"),
            Self::UnknownStream(id) => write!(formatter, "unknown stream {id}"),
            Self::StreamNotOpening(id) => write!(formatter, "stream {id} is not opening"),
            Self::StreamNotAccepted(id) => write!(formatter, "stream {id} is not accepted"),
            Self::WrongOpeningResponder(id) => {
                write!(formatter, "wrong endpoint responded to opening stream {id}")
            }
            Self::CloseByInitiator(id) => {
                write!(formatter, "stream initiator cannot CLOSE stream {id}")
            }
            Self::DataAfterEnd(id) => write!(formatter, "DATA after END_STREAM on stream {id}"),
            Self::SendCreditExceeded {
                stream_id,
                needed,
                available,
            } => write!(
                formatter,
                "stream {stream_id} send credit exhausted: need {needed}, have {available}"
            ),
            Self::ReceiveCreditExceeded {
                stream_id,
                needed,
                available,
            } => write!(
                formatter,
                "stream {stream_id} receive credit exhausted: need {needed}, have {available}"
            ),
            Self::ConnectionSendCreditExceeded { needed, available } => write!(
                formatter,
                "connection send credit exhausted: need {needed}, have {available}"
            ),
            Self::ConnectionReceiveCreditExceeded { needed, available } => write!(
                formatter,
                "connection receive credit exhausted: need {needed}, have {available}"
            ),
            Self::CreditOverflow {
                stream_id,
                current,
                delta,
                maximum,
            } => write!(
                formatter,
                "credit overflow on stream {stream_id}: {current} + {delta} exceeds {maximum}"
            ),
            Self::MissingConnectionWindow => write!(formatter, "HELLO window metadata is missing"),
            Self::MissingStreamWindow(id) => {
                write!(
                    formatter,
                    "ACCEPT window metadata is missing for stream {id}"
                )
            }
            Self::UnexpectedConnectionWindow(command) => {
                write!(
                    formatter,
                    "unexpected connection window metadata on {command:?}"
                )
            }
            Self::UnexpectedStreamWindow(command) => {
                write!(
                    formatter,
                    "unexpected stream window metadata on {command:?}"
                )
            }
            Self::InvalidWindow { window, maximum } => {
                write!(formatter, "window {window} is outside 1..={maximum}")
            }
            Self::PairingFinishOnRegularSession => {
                write!(
                    formatter,
                    "PAIRING_FINISH is forbidden on a regular session"
                )
            }
            Self::DuplicateHello => write!(formatter, "duplicate HELLO"),
            Self::DuplicatePairingFinish => write!(formatter, "duplicate PAIRING_FINISH"),
            Self::StreamIdExhausted => write!(formatter, "stream ID space exhausted"),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<WireError> for ProtocolError {
    fn from(value: WireError) -> Self {
        Self::Wire(value)
    }
}
