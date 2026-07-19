use std::{fmt, io::Read};

pub const EVENT_SIZE: usize = 12;
const MAX_EVENTS_BEFORE_ENABLE: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum EventKind {
    Bind = 0,
    Unbind = 1,
    Enable = 2,
    Disable = 3,
    Setup = 4,
    Suspend = 5,
    Resume = 6,
}

impl TryFrom<u8> for EventKind {
    type Error = EventError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Bind),
            1 => Ok(Self::Unbind),
            2 => Ok(Self::Enable),
            3 => Ok(Self::Disable),
            4 => Ok(Self::Setup),
            5 => Ok(Self::Suspend),
            6 => Ok(Self::Resume),
            other => Err(EventError::UnknownType(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SetupPacket {
    pub request_type: u8,
    pub request: u8,
    pub value: u16,
    pub index: u16,
    pub length: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Event {
    pub kind: EventKind,
    pub setup: SetupPacket,
}

impl Event {
    pub fn parse(bytes: &[u8]) -> Result<Self, EventError> {
        if bytes.len() != EVENT_SIZE {
            return Err(EventError::WrongSize(bytes.len()));
        }
        Ok(Self {
            kind: EventKind::try_from(bytes[8])?,
            setup: SetupPacket {
                request_type: bytes[0],
                request: bytes[1],
                value: u16::from_le_bytes([bytes[2], bytes[3]]),
                index: u16::from_le_bytes([bytes[4], bytes[5]]),
                length: u16::from_le_bytes([bytes[6], bytes[7]]),
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitOutcome {
    Enabled,
    Disconnected,
}

#[derive(Debug)]
pub enum EventError {
    Io(std::io::Error),
    WrongSize(usize),
    UnknownType(u8),
    UnsupportedSetup(SetupPacket),
    EventLimit(usize),
}

impl fmt::Display for EventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "reading FunctionFS event failed: {error}"),
            Self::WrongSize(size) => write!(formatter, "FunctionFS event is {size}, expected 12"),
            Self::UnknownType(kind) => write!(formatter, "unknown FunctionFS event type {kind}"),
            Self::UnsupportedSetup(setup) => {
                write!(formatter, "unsupported FunctionFS SETUP request {setup:?}")
            }
            Self::EventLimit(limit) => {
                write!(
                    formatter,
                    "FunctionFS ENABLE not received within {limit} events"
                )
            }
        }
    }
}

impl std::error::Error for EventError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

/// Wait for ENABLE using one fixed-size ABI record at a time. No event-provided
/// length controls allocation, and the loop has a hard event-count bound.
pub fn wait_for_enable<R: Read>(reader: &mut R) -> Result<WaitOutcome, EventError> {
    wait_for_enable_bounded(reader, MAX_EVENTS_BEFORE_ENABLE)
}

pub(crate) fn wait_for_enable_bounded<R: Read>(
    reader: &mut R,
    max_events: usize,
) -> Result<WaitOutcome, EventError> {
    let mut bytes = [0_u8; EVENT_SIZE];
    for _ in 0..max_events {
        match reader.read_exact(&mut bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(WaitOutcome::Disconnected);
            }
            Err(error) => return Err(EventError::Io(error)),
        }
        let event = Event::parse(&bytes)?;
        match event.kind {
            EventKind::Enable => return Ok(WaitOutcome::Enabled),
            EventKind::Unbind => return Ok(WaitOutcome::Disconnected),
            EventKind::Setup => return Err(EventError::UnsupportedSetup(event.setup)),
            EventKind::Bind | EventKind::Disable | EventKind::Suspend | EventKind::Resume => {}
        }
    }
    Err(EventError::EventLimit(max_events))
}
