use crate::error::{Error, ErrorCode, Result};
use crate::model::Digest;

pub const HEADER_SIZE: usize = 128;
pub const MAGIC: [u8; 8] = *b"KBB1\r\n\x1a\n";
pub const FORMAT_MAJOR: u16 = 1;
pub const FORMAT_MINOR: u16 = 0;
pub const MAX_ENVELOPE_LENGTH: u64 = 16 * 1024 * 1024;
pub const MAX_SIGNATURE_LENGTH: u64 = 1024 * 1024;
pub const MAX_PAYLOAD_LENGTH: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub envelope_length: u64,
    pub signature_offset: u64,
    pub signature_length: u64,
    pub payload_offset: u64,
    pub payload_length: u64,
    pub bundle_root: Digest,
}

impl Header {
    pub fn decode(bytes: &[u8; HEADER_SIZE], file_length: u64) -> Result<Self> {
        if bytes[..8] != MAGIC {
            return header_error("bad KBB magic");
        }
        if read_u16(bytes, 8) != FORMAT_MAJOR
            || read_u16(bytes, 10) != FORMAT_MINOR
            || read_u16(bytes, 12) != HEADER_SIZE as u16
            || read_u16(bytes, 14) != 0
            || read_u64(bytes, 16) != HEADER_SIZE as u64
        {
            return header_error("unsupported version, header size, flags, or envelope offset");
        }
        if bytes[100..].iter().any(|byte| *byte != 0) {
            return header_error("reserved header bytes are non-zero");
        }

        let envelope_length = read_u64(bytes, 24);
        let signature_offset = read_u64(bytes, 32);
        let signature_length = read_u64(bytes, 40);
        let payload_offset = read_u64(bytes, 48);
        let payload_length = read_u64(bytes, 56);
        if !(1..=MAX_ENVELOPE_LENGTH).contains(&envelope_length)
            || !(1..=MAX_SIGNATURE_LENGTH).contains(&signature_length)
            || payload_length > MAX_PAYLOAD_LENGTH
        {
            return Err(Error::new(
                ErrorCode::Bounds,
                "KBB section exceeds a static limit",
            ));
        }

        let canonical_signature = align8(
            (HEADER_SIZE as u64)
                .checked_add(envelope_length)
                .ok_or_else(|| Error::new(ErrorCode::Bounds, "envelope end overflow"))?,
        )?;
        let canonical_payload = align8(
            signature_offset
                .checked_add(signature_length)
                .ok_or_else(|| Error::new(ErrorCode::Bounds, "signature end overflow"))?,
        )?;
        let canonical_file_length = payload_offset
            .checked_add(payload_length)
            .ok_or_else(|| Error::new(ErrorCode::Bounds, "payload end overflow"))?;
        if signature_offset != canonical_signature
            || payload_offset != canonical_payload
            || file_length != canonical_file_length
        {
            return header_error("non-canonical section offsets or file length");
        }

        let expected_crc = read_u32(bytes, 96);
        let mut crc_input = *bytes;
        crc_input[96..100].fill(0);
        if crc32c::crc32c(&crc_input) != expected_crc {
            return header_error("header CRC32C mismatch");
        }
        let bundle_root = Digest(bytes[64..96].try_into().expect("fixed header slice"));
        Ok(Self {
            envelope_length,
            signature_offset,
            signature_length,
            payload_offset,
            payload_length,
            bundle_root,
        })
    }

    pub fn encode(&self) -> Result<[u8; HEADER_SIZE]> {
        let mut bytes = [0_u8; HEADER_SIZE];
        bytes[..8].copy_from_slice(&MAGIC);
        put_u16(&mut bytes, 8, FORMAT_MAJOR);
        put_u16(&mut bytes, 10, FORMAT_MINOR);
        put_u16(&mut bytes, 12, HEADER_SIZE as u16);
        put_u64(&mut bytes, 16, HEADER_SIZE as u64);
        put_u64(&mut bytes, 24, self.envelope_length);
        put_u64(&mut bytes, 32, self.signature_offset);
        put_u64(&mut bytes, 40, self.signature_length);
        put_u64(&mut bytes, 48, self.payload_offset);
        put_u64(&mut bytes, 56, self.payload_length);
        bytes[64..96].copy_from_slice(self.bundle_root.as_bytes());
        let crc = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, 96, crc);
        Ok(bytes)
    }
}

pub(crate) fn align8(value: u64) -> Result<u64> {
    value
        .checked_add(7)
        .map(|rounded| rounded & !7)
        .ok_or_else(|| Error::new(ErrorCode::Bounds, "8-byte alignment overflow"))
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("header offset"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("header offset"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("header offset"))
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn header_error<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorCode::Header, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let header = Header {
            envelope_length: 13,
            signature_offset: 144,
            signature_length: 9,
            payload_offset: 160,
            payload_length: 8,
            bundle_root: Digest::of(b"envelope"),
        };
        let encoded = header.encode().unwrap();
        assert_eq!(Header::decode(&encoded, 168).unwrap(), header);
    }

    #[test]
    fn crc_detects_mutation() {
        let header = Header {
            envelope_length: 1,
            signature_offset: 136,
            signature_length: 1,
            payload_offset: 144,
            payload_length: 0,
            bundle_root: Digest::ZERO,
        };
        let mut encoded = header.encode().unwrap();
        encoded[64] ^= 1;
        assert_eq!(
            Header::decode(&encoded, 144).unwrap_err().code,
            ErrorCode::Header
        );
    }
}
