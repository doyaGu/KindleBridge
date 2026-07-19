use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_cbor::Value;

use crate::error::{Error, ErrorCode, Result, ResultExt};

const MAX_DEPTH: usize = 32;

pub(crate) fn to_canonical_vec<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_cbor::value::to_value(value)
        .context(ErrorCode::Schema, "cannot convert value to CBOR")?;
    let mut output = Vec::new();
    encode_value(&value, &mut output)?;
    Ok(output)
}

pub(crate) fn from_canonical_slice<T: DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T> {
    let consumed = scan_item(bytes, 0, 0)?;
    if consumed != bytes.len() {
        return Err(Error::new(
            ErrorCode::CanonicalCbor,
            "trailing bytes after the top-level CBOR item",
        ));
    }
    let value: T = serde_cbor::from_slice(bytes)
        .context(ErrorCode::Schema, "CBOR does not match the expected schema")?;
    let canonical = to_canonical_vec(&value)?;
    if canonical != bytes {
        return Err(Error::new(
            ErrorCode::CanonicalCbor,
            "CBOR is not in deterministic encoding or contains duplicate/unknown fields",
        ));
    }
    Ok(value)
}

fn encode_value(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => output.push(0xf6),
        Value::Bool(false) => output.push(0xf4),
        Value::Bool(true) => output.push(0xf5),
        Value::Integer(integer) => {
            let integer = *integer;
            if integer >= 0 {
                let value = u64::try_from(integer)
                    .map_err(|_| Error::new(ErrorCode::Schema, "CBOR integer exceeds uint64"))?;
                encode_argument(0, value, output);
            } else {
                let value = u64::try_from(-1 - integer).map_err(|_| {
                    Error::new(
                        ErrorCode::Schema,
                        "CBOR integer is below int64 encoding range",
                    )
                })?;
                encode_argument(1, value, output);
            }
        }
        Value::Bytes(bytes) => {
            encode_argument(2, usize_u64(bytes.len())?, output);
            output.extend_from_slice(bytes);
        }
        Value::Text(text) => {
            encode_argument(3, usize_u64(text.len())?, output);
            output.extend_from_slice(text.as_bytes());
        }
        Value::Array(items) => {
            encode_argument(4, usize_u64(items.len())?, output);
            for item in items {
                encode_value(item, output)?;
            }
        }
        Value::Map(map) => {
            let mut entries = Vec::with_capacity(map.len());
            for (key, value) in map {
                let mut encoded_key = Vec::new();
                encode_value(key, &mut encoded_key)?;
                entries.push((encoded_key, value));
            }
            entries.sort_by(|left, right| {
                left.0
                    .len()
                    .cmp(&right.0.len())
                    .then_with(|| left.0.cmp(&right.0))
            });
            encode_argument(5, usize_u64(entries.len())?, output);
            for (key, value) in entries {
                output.extend_from_slice(&key);
                encode_value(value, output)?;
            }
        }
        Value::Tag(_, _) | Value::Float(_) => {
            return Err(Error::new(
                ErrorCode::Schema,
                "tags and floating point values are forbidden by KBB v1",
            ));
        }
        _ => {
            return Err(Error::new(
                ErrorCode::Schema,
                "unsupported CBOR value in KBB v1",
            ));
        }
    }
    Ok(())
}

fn encode_argument(major: u8, argument: u64, output: &mut Vec<u8>) {
    let prefix = major << 5;
    match argument {
        0..=23 => output.push(prefix | u8::try_from(argument).expect("argument <= 23")),
        24..=0xff => {
            output.push(prefix | 24);
            output.push(u8::try_from(argument).expect("argument <= u8::MAX"));
        }
        0x100..=0xffff => {
            output.push(prefix | 25);
            output.extend_from_slice(
                &u16::try_from(argument)
                    .expect("argument <= u16::MAX")
                    .to_be_bytes(),
            );
        }
        0x1_0000..=0xffff_ffff => {
            output.push(prefix | 26);
            output.extend_from_slice(
                &u32::try_from(argument)
                    .expect("argument <= u32::MAX")
                    .to_be_bytes(),
            );
        }
        _ => {
            output.push(prefix | 27);
            output.extend_from_slice(&argument.to_be_bytes());
        }
    }
}

fn scan_item(input: &[u8], offset: usize, depth: usize) -> Result<usize> {
    if depth >= MAX_DEPTH {
        return Err(Error::new(
            ErrorCode::CanonicalCbor,
            "CBOR nesting exceeds 32 levels",
        ));
    }
    let initial = *input
        .get(offset)
        .ok_or_else(|| Error::new(ErrorCode::CanonicalCbor, "truncated CBOR initial byte"))?;
    let major = initial >> 5;
    let additional = initial & 0x1f;
    let (argument, argument_bytes) = decode_argument(input, offset + 1, additional)?;
    let mut position = offset
        .checked_add(1 + argument_bytes)
        .ok_or_else(|| Error::new(ErrorCode::Bounds, "CBOR offset overflow"))?;

    match major {
        0 | 1 => {}
        2 | 3 => {
            let length = usize::try_from(argument)
                .map_err(|_| Error::new(ErrorCode::Bounds, "CBOR string length exceeds usize"))?;
            let end = position
                .checked_add(length)
                .ok_or_else(|| Error::new(ErrorCode::Bounds, "CBOR string end overflow"))?;
            let bytes = input
                .get(position..end)
                .ok_or_else(|| Error::new(ErrorCode::CanonicalCbor, "truncated CBOR string"))?;
            if major == 3 {
                std::str::from_utf8(bytes)
                    .context(ErrorCode::CanonicalCbor, "invalid UTF-8 in CBOR text")?;
            }
            position = end;
        }
        4 => {
            for _ in 0..argument {
                position = scan_item(input, position, depth + 1)?;
            }
        }
        5 => {
            for _ in 0..argument {
                position = scan_item(input, position, depth + 1)?;
                position = scan_item(input, position, depth + 1)?;
            }
        }
        6 => {
            return Err(Error::new(
                ErrorCode::CanonicalCbor,
                "CBOR tags are forbidden",
            ));
        }
        7 => {
            if !matches!(initial, 0xf4..=0xf6) {
                return Err(Error::new(
                    ErrorCode::CanonicalCbor,
                    "only false, true, and null CBOR simple values are allowed",
                ));
            }
        }
        _ => unreachable!(),
    }
    Ok(position)
}

fn decode_argument(input: &[u8], offset: usize, additional: u8) -> Result<(u64, usize)> {
    match additional {
        value @ 0..=23 => Ok((u64::from(value), 0)),
        24 => {
            let value = u64::from(*input.get(offset).ok_or_else(|| {
                Error::new(ErrorCode::CanonicalCbor, "truncated CBOR uint8 argument")
            })?);
            if value < 24 {
                return non_minimal();
            }
            Ok((value, 1))
        }
        25 => {
            let bytes = read_array::<2>(input, offset)?;
            let value = u64::from(u16::from_be_bytes(bytes));
            if value <= 0xff {
                return non_minimal();
            }
            Ok((value, 2))
        }
        26 => {
            let bytes = read_array::<4>(input, offset)?;
            let value = u64::from(u32::from_be_bytes(bytes));
            if value <= 0xffff {
                return non_minimal();
            }
            Ok((value, 4))
        }
        27 => {
            let value = u64::from_be_bytes(read_array::<8>(input, offset)?);
            if value <= 0xffff_ffff {
                return non_minimal();
            }
            Ok((value, 8))
        }
        31 => Err(Error::new(
            ErrorCode::CanonicalCbor,
            "indefinite-length CBOR is forbidden",
        )),
        _ => Err(Error::new(
            ErrorCode::CanonicalCbor,
            "reserved CBOR additional information",
        )),
    }
}

fn read_array<const N: usize>(input: &[u8], offset: usize) -> Result<[u8; N]> {
    input
        .get(offset..offset.saturating_add(N))
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| Error::new(ErrorCode::CanonicalCbor, "truncated CBOR argument"))
}

fn non_minimal<T>() -> Result<T> {
    Err(Error::new(
        ErrorCode::CanonicalCbor,
        "non-minimal CBOR integer/length encoding",
    ))
}

fn usize_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::new(ErrorCode::Bounds, "length exceeds uint64"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_minimal_and_indefinite_cbor() {
        assert_eq!(
            from_canonical_slice::<Value>(&[0x18, 0x01])
                .unwrap_err()
                .code,
            ErrorCode::CanonicalCbor
        );
        assert_eq!(
            from_canonical_slice::<Value>(&[0x9f, 0xff])
                .unwrap_err()
                .code,
            ErrorCode::CanonicalCbor
        );
    }

    #[test]
    fn enforces_nesting_limit_before_deserialization() {
        let mut input = vec![0x81; 33];
        input.push(0xf6);
        assert_eq!(
            from_canonical_slice::<Value>(&input).unwrap_err().code,
            ErrorCode::CanonicalCbor
        );
    }
}
