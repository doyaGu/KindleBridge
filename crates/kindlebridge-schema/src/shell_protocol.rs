//! Binary packets carried one-for-one in `shell.v2` KBP DATA frames.

use thiserror::Error;

use crate::device_protocol::TerminalSize;

pub const MAX_SHELL_PACKET_PAYLOAD: usize = 16 * 1024;
const HEADER_LENGTH: usize = 5;
/// Preferred data payload for the production 16 KiB FunctionFS request.
/// The Shell packet and fixed KBP header then fit in exactly one request.
pub const USB_ALIGNED_SHELL_PACKET_PAYLOAD: usize =
    16 * 1024 - kindlebridge_wire::HEADER_LEN - HEADER_LENGTH;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacketSource {
    Host,
    Device,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShellExit {
    pub exit_code: i32,
    pub signal: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShellPacket {
    Stdin(Vec<u8>),
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(ShellExit),
    CloseStdin,
    Resize(TerminalSize),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ShellPacketError {
    #[error("shell packet header is truncated: got {actual} bytes")]
    HeaderTooShort { actual: usize },
    #[error("unknown shell packet kind {0}")]
    UnknownKind(u8),
    #[error("shell packet kind {kind} is invalid in this direction")]
    InvalidDirection { kind: u8 },
    #[error("shell packet declares {declared} bytes but carries {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("shell packet payload is {length} bytes; maximum is {maximum}")]
    PayloadTooLarge { length: usize, maximum: usize },
    #[error("shell packet kind {kind} requires {expected} payload bytes, got {actual}")]
    InvalidPayloadLength {
        kind: u8,
        expected: usize,
        actual: usize,
    },
    #[error("terminal rows and columns must both be non-zero")]
    InvalidTerminalSize,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ShellStreamError {
    #[error("shell input is already closed")]
    InputClosed,
    #[error("shell packet arrived after the exit packet")]
    AfterExit,
    #[error("stderr is merged into stdout for a PTY shell")]
    StderrForPty,
    #[error("terminal resize is only valid for a PTY shell")]
    ResizeForRaw,
}

/// Direction-independent ordering rules for one accepted shell stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShellStreamState {
    mode: crate::device_protocol::ShellMode,
    input_open: bool,
    exit: Option<ShellExit>,
}

impl ShellStreamState {
    #[must_use]
    pub const fn new(mode: crate::device_protocol::ShellMode) -> Self {
        Self {
            mode,
            input_open: true,
            exit: None,
        }
    }

    pub fn accept(&mut self, packet: &ShellPacket) -> Result<(), ShellStreamError> {
        if self.exit.is_some() {
            return Err(ShellStreamError::AfterExit);
        }
        match packet {
            ShellPacket::Stdin(_) if !self.input_open => Err(ShellStreamError::InputClosed),
            ShellPacket::CloseStdin if !self.input_open => Err(ShellStreamError::InputClosed),
            ShellPacket::CloseStdin => {
                self.input_open = false;
                Ok(())
            }
            ShellPacket::Stderr(_) if self.mode == crate::device_protocol::ShellMode::Pty => {
                Err(ShellStreamError::StderrForPty)
            }
            ShellPacket::Resize(_) if self.mode == crate::device_protocol::ShellMode::Raw => {
                Err(ShellStreamError::ResizeForRaw)
            }
            ShellPacket::Exit(status) => {
                self.exit = Some(*status);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    #[must_use]
    pub const fn input_is_open(&self) -> bool {
        self.input_open
    }

    #[must_use]
    pub const fn exit(&self) -> Option<ShellExit> {
        self.exit
    }
}

impl ShellPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ShellPacketError> {
        let (kind, payload) = match self {
            Self::Stdin(data) => (0, data.clone()),
            Self::Stdout(data) => (1, data.clone()),
            Self::Stderr(data) => (2, data.clone()),
            Self::Exit(status) => {
                let mut payload = Vec::with_capacity(8);
                payload.extend_from_slice(&status.exit_code.to_le_bytes());
                payload.extend_from_slice(&status.signal.to_le_bytes());
                (3, payload)
            }
            Self::CloseStdin => (4, Vec::new()),
            Self::Resize(size) => {
                validate_terminal_size(*size)?;
                let mut payload = Vec::with_capacity(8);
                payload.extend_from_slice(&size.rows.to_le_bytes());
                payload.extend_from_slice(&size.columns.to_le_bytes());
                payload.extend_from_slice(&size.pixel_width.to_le_bytes());
                payload.extend_from_slice(&size.pixel_height.to_le_bytes());
                (5, payload)
            }
        };
        validate_payload_length(payload.len())?;

        let mut encoded = Vec::with_capacity(HEADER_LENGTH + payload.len());
        encoded.push(kind);
        encoded.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&payload);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8], source: PacketSource) -> Result<Self, ShellPacketError> {
        if encoded.len() < HEADER_LENGTH {
            return Err(ShellPacketError::HeaderTooShort {
                actual: encoded.len(),
            });
        }
        let kind = encoded[0];
        if kind > 5 {
            return Err(ShellPacketError::UnknownKind(kind));
        }
        validate_direction(kind, source)?;
        let declared = u32::from_le_bytes(encoded[1..5].try_into().expect("fixed header")) as usize;
        validate_payload_length(declared)?;
        let payload = &encoded[HEADER_LENGTH..];
        if declared != payload.len() {
            return Err(ShellPacketError::LengthMismatch {
                declared,
                actual: payload.len(),
            });
        }

        match kind {
            0 => Ok(Self::Stdin(payload.to_vec())),
            1 => Ok(Self::Stdout(payload.to_vec())),
            2 => Ok(Self::Stderr(payload.to_vec())),
            3 => {
                require_length(kind, payload, 8)?;
                Ok(Self::Exit(ShellExit {
                    exit_code: i32::from_le_bytes(
                        payload[0..4].try_into().expect("checked length"),
                    ),
                    signal: u32::from_le_bytes(payload[4..8].try_into().expect("checked length")),
                }))
            }
            4 => {
                require_length(kind, payload, 0)?;
                Ok(Self::CloseStdin)
            }
            5 => {
                require_length(kind, payload, 8)?;
                let size = TerminalSize {
                    rows: u16::from_le_bytes(payload[0..2].try_into().expect("checked length")),
                    columns: u16::from_le_bytes(payload[2..4].try_into().expect("checked length")),
                    pixel_width: u16::from_le_bytes(
                        payload[4..6].try_into().expect("checked length"),
                    ),
                    pixel_height: u16::from_le_bytes(
                        payload[6..8].try_into().expect("checked length"),
                    ),
                };
                validate_terminal_size(size)?;
                Ok(Self::Resize(size))
            }
            _ => unreachable!("kind range checked"),
        }
    }
}

fn validate_direction(kind: u8, source: PacketSource) -> Result<(), ShellPacketError> {
    let valid = match source {
        PacketSource::Host => matches!(kind, 0 | 4 | 5),
        PacketSource::Device => matches!(kind, 1..=3),
    };
    if valid {
        Ok(())
    } else {
        Err(ShellPacketError::InvalidDirection { kind })
    }
}

fn validate_payload_length(length: usize) -> Result<(), ShellPacketError> {
    if length <= MAX_SHELL_PACKET_PAYLOAD {
        Ok(())
    } else {
        Err(ShellPacketError::PayloadTooLarge {
            length,
            maximum: MAX_SHELL_PACKET_PAYLOAD,
        })
    }
}

fn require_length(kind: u8, payload: &[u8], expected: usize) -> Result<(), ShellPacketError> {
    if payload.len() == expected {
        Ok(())
    } else {
        Err(ShellPacketError::InvalidPayloadLength {
            kind,
            expected,
            actual: payload.len(),
        })
    }
}

fn validate_terminal_size(size: TerminalSize) -> Result<(), ShellPacketError> {
    if size.is_valid() {
        Ok(())
    } else {
        Err(ShellPacketError::InvalidTerminalSize)
    }
}
