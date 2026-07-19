use crate::WireError;

pub const MAGIC: [u8; 4] = *b"KBP1";
pub const HEADER_LEN: usize = 40;
pub const PROTOCOL_MAJOR: u16 = 1;
pub const PROTOCOL_MINOR: u16 = 0;

pub const FLAG_END_STREAM: u32 = 0x0000_0001;
pub const FLAG_URGENT: u32 = 0x0001_0000;

const CRITICAL_FLAG_MASK: u32 = 0x0000_ffff;
const KNOWN_CRITICAL_FLAGS: u32 = FLAG_END_STREAM;

/// Stable v1 command numbers, assigned in the order defined by the protocol.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u16)]
pub enum Command {
    Hello = 1,
    PairingFinish = 2,
    Open = 3,
    Accept = 4,
    Reject = 5,
    Data = 6,
    Credit = 7,
    Close = 8,
    Reset = 9,
    Ping = 10,
    Pong = 11,
    GoAway = 12,
    Error = 13,
}

impl TryFrom<u16> for Command {
    type Error = WireError;

    fn try_from(value: u16) -> Result<Self, WireError> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::PairingFinish),
            3 => Ok(Self::Open),
            4 => Ok(Self::Accept),
            5 => Ok(Self::Reject),
            6 => Ok(Self::Data),
            7 => Ok(Self::Credit),
            8 => Ok(Self::Close),
            9 => Ok(Self::Reset),
            10 => Ok(Self::Ping),
            11 => Ok(Self::Pong),
            12 => Ok(Self::GoAway),
            13 => Ok(Self::Error),
            other => Err(WireError::UnknownCommand(other)),
        }
    }
}

impl Command {
    const fn allows_stream_zero(self) -> bool {
        matches!(
            self,
            Self::Hello
                | Self::PairingFinish
                | Self::Credit
                | Self::Ping
                | Self::Pong
                | Self::GoAway
                | Self::Error
        )
    }

    const fn allows_nonzero_stream(self) -> bool {
        matches!(
            self,
            Self::Open
                | Self::Accept
                | Self::Reject
                | Self::Data
                | Self::Credit
                | Self::Close
                | Self::Reset
        )
    }
}

/// Negotiated upper bounds used before allocating or accepting a frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodeLimits {
    pub max_payload: u32,
    pub max_window: u32,
}

impl DecodeLimits {
    pub const fn new(max_payload: u32, max_window: u32) -> Self {
        Self {
            max_payload,
            max_window,
        }
    }
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self::new(16 * 1024 * 1024, 16 * 1024 * 1024)
    }
}

/// The logical fields of the fixed v1 header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub major: u16,
    pub minor: u16,
    pub command: Command,
    pub flags: u32,
    pub stream_id: u32,
    pub sequence: u32,
    pub payload_length: u32,
    pub credit_delta: u32,
    pub reserved: u32,
}

impl Header {
    pub const fn new(command: Command, stream_id: u32, sequence: u32) -> Self {
        Self {
            major: PROTOCOL_MAJOR,
            minor: PROTOCOL_MINOR,
            command,
            flags: 0,
            stream_id,
            sequence,
            payload_length: 0,
            credit_delta: 0,
            reserved: 0,
        }
    }

    pub fn validate(&self, limits: DecodeLimits) -> Result<(), WireError> {
        if self.major != PROTOCOL_MAJOR {
            return Err(WireError::UnsupportedMajor(self.major));
        }
        if self.payload_length > limits.max_payload {
            return Err(WireError::PayloadTooLarge {
                length: self.payload_length,
                maximum: limits.max_payload,
            });
        }
        if self.reserved != 0 {
            return Err(WireError::ReservedNotZero(self.reserved));
        }

        let unknown_critical = self.flags & CRITICAL_FLAG_MASK & !KNOWN_CRITICAL_FLAGS;
        if unknown_critical != 0 {
            return Err(WireError::UnknownCriticalFlags(unknown_critical));
        }
        if self.flags & FLAG_END_STREAM != 0 && self.command != Command::Data {
            return Err(WireError::EndStreamOnNonData(self.command));
        }

        if self.command == Command::Credit {
            if self.payload_length != 0 {
                return Err(WireError::CreditPayloadNotEmpty(self.payload_length));
            }
            if self.credit_delta == 0 {
                return Err(WireError::InvalidCreditDelta(0));
            }
            if self.credit_delta > limits.max_window {
                return Err(WireError::CreditTooLarge {
                    delta: self.credit_delta,
                    maximum: limits.max_window,
                });
            }
        } else if self.credit_delta != 0 {
            return Err(WireError::UnexpectedCreditDelta {
                command: self.command,
                delta: self.credit_delta,
            });
        }

        let stream_is_valid = if self.stream_id == 0 {
            self.command.allows_stream_zero()
        } else {
            self.command.allows_nonzero_stream()
        };
        if !stream_is_valid {
            return Err(WireError::InvalidStreamForCommand {
                command: self.command,
                stream_id: self.stream_id,
            });
        }
        Ok(())
    }

    pub fn encode(&self, limits: DecodeLimits) -> Result<[u8; HEADER_LEN], WireError> {
        self.validate(limits)?;
        let mut bytes = [0_u8; HEADER_LEN];
        bytes[0..4].copy_from_slice(&MAGIC);
        put_u16(&mut bytes, 4, self.major);
        put_u16(&mut bytes, 6, self.minor);
        put_u16(&mut bytes, 8, HEADER_LEN as u16);
        put_u16(&mut bytes, 10, self.command as u16);
        put_u32(&mut bytes, 12, self.flags);
        put_u32(&mut bytes, 16, self.stream_id);
        put_u32(&mut bytes, 20, self.sequence);
        put_u32(&mut bytes, 24, self.payload_length);
        put_u32(&mut bytes, 28, self.credit_delta);
        put_u32(&mut bytes, 32, 0);
        put_u32(&mut bytes, 36, self.reserved);
        let checksum = crc32c(&bytes);
        put_u32(&mut bytes, 32, checksum);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8], limits: DecodeLimits) -> Result<Self, WireError> {
        if bytes.len() < HEADER_LEN {
            return Err(WireError::HeaderTooShort {
                actual: bytes.len(),
            });
        }
        let mut magic = [0_u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        if magic != MAGIC {
            return Err(WireError::InvalidMagic(magic));
        }
        let header_size = get_u16(bytes, 8);
        if header_size != HEADER_LEN as u16 {
            return Err(WireError::InvalidHeaderSize(header_size));
        }

        let actual_crc = get_u32(bytes, 32);
        let mut canonical = [0_u8; HEADER_LEN];
        canonical.copy_from_slice(&bytes[..HEADER_LEN]);
        put_u32(&mut canonical, 32, 0);
        let expected_crc = crc32c(&canonical);
        if actual_crc != expected_crc {
            return Err(WireError::HeaderCrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        let header = Self {
            major: get_u16(bytes, 4),
            minor: get_u16(bytes, 6),
            command: Command::try_from(get_u16(bytes, 10))?,
            flags: get_u32(bytes, 12),
            stream_id: get_u32(bytes, 16),
            sequence: get_u32(bytes, 20),
            payload_length: get_u32(bytes, 24),
            credit_delta: get_u32(bytes, 28),
            reserved: get_u32(bytes, 36),
        };
        header.validate(limits)?;
        Ok(header)
    }
}

/// A complete KBP frame. Payload integrity and privacy are provided by TLS.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub header: Header,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(mut header: Header, payload: Vec<u8>) -> Result<Self, WireError> {
        header.payload_length = u32::try_from(payload.len())
            .map_err(|_| WireError::PayloadLengthOverflow(payload.len()))?;
        Ok(Self { header, payload })
    }

    pub fn encode(&self, limits: DecodeLimits) -> Result<Vec<u8>, WireError> {
        let actual = u32::try_from(self.payload.len())
            .map_err(|_| WireError::PayloadLengthOverflow(self.payload.len()))?;
        if actual != self.header.payload_length {
            return Err(WireError::FrameLengthMismatch {
                expected: HEADER_LEN + self.header.payload_length as usize,
                actual: HEADER_LEN + self.payload.len(),
            });
        }
        let header = self.header.encode(limits)?;
        let mut output = Vec::with_capacity(HEADER_LEN + self.payload.len());
        output.extend_from_slice(&header);
        output.extend_from_slice(&self.payload);
        Ok(output)
    }

    pub fn decode(bytes: &[u8], limits: DecodeLimits) -> Result<Self, WireError> {
        let header = Header::decode(bytes, limits)?;
        let expected = HEADER_LEN + header.payload_length as usize;
        if bytes.len() != expected {
            return Err(WireError::FrameLengthMismatch {
                expected,
                actual: bytes.len(),
            });
        }
        Ok(Self {
            header,
            payload: bytes[HEADER_LEN..].to_vec(),
        })
    }
}

/// CRC-32C/ISCSI (`poly=0x1EDC6F41`, reflected, `init/xorout=0xffffffff`).
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}
