use std::collections::BTreeMap;
use std::fmt;

use crate::fs_safe::{validate_relative_path, SafeRoot};
use crate::{Error, ErrorKind, Result};

const MANIFEST_HEADER: &str = "KINDLEBRIDGE_SLOT_V1";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Slot {
    A,
    B,
}

impl Slot {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
        }
    }

    #[must_use]
    pub const fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "A" => Ok(Self::A),
            "B" => Ok(Self::B),
            _ => invalid("slot must be A or B"),
        }
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotManifest {
    pub slot: Slot,
    /// Path relative to `slots/<slot>`.
    pub executable: String,
    /// Heartbeat path relative to the launcher root, normally below `run/`.
    pub heartbeat: String,
    pub startup_timeout_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub healthy_after_ms: u64,
    pub max_crashes: u32,
    pub backoff_initial_ms: u64,
    pub backoff_max_ms: u64,
}

impl SlotManifest {
    pub fn parse(bytes: &[u8], expected_slot: Slot) -> Result<Self> {
        if bytes.len() > 8192 {
            return invalid("slot manifest exceeds 8 KiB");
        }
        let text = std::str::from_utf8(bytes)
            .map_err(|_| Error::new(ErrorKind::InvalidManifest, "manifest is not UTF-8"))?;
        if text.contains('\r') || !text.ends_with('\n') {
            return invalid("manifest must use LF and end with a newline");
        }
        let mut lines = text.lines();
        if lines.next() != Some(MANIFEST_HEADER) {
            return invalid("unknown slot manifest header");
        }
        let mut values = BTreeMap::new();
        for line in lines {
            if line.is_empty() {
                return invalid("blank manifest line is forbidden");
            }
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| Error::new(ErrorKind::InvalidManifest, "malformed manifest line"))?;
            if key.is_empty()
                || value.is_empty()
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            {
                return invalid("invalid manifest key or empty value");
            }
            if values.insert(key, value).is_some() {
                return invalid("duplicate manifest key");
            }
        }
        let required = [
            "backoff_initial_ms",
            "backoff_max_ms",
            "executable",
            "heartbeat",
            "heartbeat_timeout_ms",
            "healthy_after_ms",
            "max_crashes",
            "slot",
            "startup_timeout_ms",
        ];
        if values.len() != required.len() || required.iter().any(|key| !values.contains_key(key)) {
            return invalid("missing or unknown slot manifest field");
        }

        let slot = Slot::parse(values["slot"])?;
        if slot != expected_slot {
            return invalid("manifest slot does not match its directory");
        }
        let executable = values["executable"].to_owned();
        let heartbeat = values["heartbeat"].to_owned();
        validate_relative_path(&executable)?;
        validate_relative_path(&heartbeat)?;
        if !heartbeat.starts_with("run/") {
            return invalid("heartbeat must live below root/run");
        }

        let manifest = Self {
            slot,
            executable,
            heartbeat,
            startup_timeout_ms: parse_u64(&values, "startup_timeout_ms")?,
            heartbeat_timeout_ms: parse_u64(&values, "heartbeat_timeout_ms")?,
            healthy_after_ms: parse_u64(&values, "healthy_after_ms")?,
            max_crashes: u32::try_from(parse_u64(&values, "max_crashes")?)
                .map_err(|_| Error::new(ErrorKind::InvalidManifest, "max_crashes exceeds u32"))?,
            backoff_initial_ms: parse_u64(&values, "backoff_initial_ms")?,
            backoff_max_ms: parse_u64(&values, "backoff_max_ms")?,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub(crate) fn load(root: &SafeRoot, slot: Slot) -> Result<Self> {
        let relative = format!("slots/{}/slot.manifest", slot.as_str());
        Self::parse(&root.read_file(&relative, 8192)?, slot)
    }

    fn validate(&self) -> Result<()> {
        if self.startup_timeout_ms == 0 || self.startup_timeout_ms > 10_000 {
            return invalid("startup timeout must be within 1..=10,000 ms");
        }
        if self.heartbeat_timeout_ms == 0
            || self.heartbeat_timeout_ms > 300_000
            || self.healthy_after_ms < self.heartbeat_timeout_ms
            || self.healthy_after_ms > 600_000
        {
            return invalid("invalid heartbeat/healthy timing");
        }
        if self.max_crashes == 0
            || self.max_crashes > 16
            || self.backoff_initial_ms == 0
            || self.backoff_initial_ms > self.backoff_max_ms
            || self.backoff_max_ms > 300_000
        {
            return invalid("invalid crash/backoff policy");
        }
        Ok(())
    }
}

fn parse_u64(values: &BTreeMap<&str, &str>, key: &str) -> Result<u64> {
    let value = values[key];
    if value.len() > 20 || value.len() > 1 && value.starts_with('0') {
        return invalid(format!("{key} is not a canonical integer"));
    }
    value
        .parse()
        .map_err(|_| Error::new(ErrorKind::InvalidManifest, format!("invalid integer {key}")))
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorKind::InvalidManifest, message))
}

#[cfg(test)]
pub(crate) fn test_manifest(slot: Slot) -> String {
    format!(
        "{MANIFEST_HEADER}\nslot={slot}\nexecutable=bin/kindlebridged\nheartbeat=run/heartbeat\nstartup_timeout_ms=10000\nheartbeat_timeout_ms=1000\nhealthy_after_ms=3000\nmax_crashes=3\nbackoff_initial_ms=100\nbackoff_max_ms=1000\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_and_unsafe_manifest_fields() {
        let duplicate = format!("{}slot=A\n", test_manifest(Slot::A));
        assert_eq!(
            SlotManifest::parse(duplicate.as_bytes(), Slot::A)
                .unwrap_err()
                .kind,
            ErrorKind::InvalidManifest
        );
        let unsafe_path = test_manifest(Slot::A).replace(
            "executable=bin/kindlebridged",
            "executable=../../kindlebridged",
        );
        assert_eq!(
            SlotManifest::parse(unsafe_path.as_bytes(), Slot::A)
                .unwrap_err()
                .kind,
            ErrorKind::UnsafePath
        );
    }
}
