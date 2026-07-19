use serde::{Deserialize, Serialize};

use crate::cbor::{from_canonical_slice, to_canonical_vec};
use crate::error::{Error, ErrorCode, Result};
use crate::model::{BundleKind, Digest};
use crate::path::{validate_channel, validate_logical_id};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationId(pub [u8; 16]);

impl Serialize for GenerationId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for GenerationId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = GenerationId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("exactly 16 bytes")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let bytes = value
                    .try_into()
                    .map_err(|_| E::invalid_length(value.len(), &self))?;
                Ok(GenerationId(bytes))
            }

            fn visit_byte_buf<E>(self, value: Vec<u8>) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_bytes(&value)
            }
        }
        deserializer.deserialize_bytes(Visitor)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivationEntry {
    pub id: String,
    pub channel: String,
    pub kind: BundleKind,
    pub bundle_root: Digest,
    pub code_version: String,
    pub data_generation: Option<String>,
    pub dependency_roots: Vec<Digest>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivationGeneration {
    pub schema: u64,
    pub generation_id: GenerationId,
    pub previous_generation: Option<GenerationId>,
    pub profile_id: String,
    pub profile_digest: Digest,
    pub entries: Vec<ActivationEntry>,
}

impl ActivationGeneration {
    pub fn to_cbor(&self) -> Result<Vec<u8>> {
        self.validate()?;
        to_canonical_vec(self)
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self> {
        let generation: Self = from_canonical_slice(bytes)?;
        generation.validate()?;
        Ok(generation)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema != 1 {
            return activation_error("activation schema must be 1");
        }
        validate_component(&self.profile_id, "profile_id")?;

        let mut previous: Option<(&[u8], &[u8], &str)> = None;
        for entry in &self.entries {
            validate_logical_id(&entry.id)?;
            validate_channel(&entry.channel)?;
            if !matches!(
                entry.kind,
                BundleKind::Application
                    | BundleKind::Runtime
                    | BundleKind::Agent
                    | BundleKind::Tool
            ) {
                return activation_error("activation kind must be application/runtime/agent/tool");
            }
            validate_component(&entry.code_version, "code_version")?;
            if let Some(data_generation) = &entry.data_generation {
                validate_component(data_generation, "data_generation")?;
            }
            if !strictly_sorted(&entry.dependency_roots) {
                return activation_error("dependency roots must be strictly sorted");
            }
            let current = (
                entry.id.as_bytes(),
                entry.channel.as_bytes(),
                entry.kind.as_str(),
            );
            if previous.is_some_and(|value| value >= current) {
                return activation_error("activation entries are not strictly sorted");
            }
            previous = Some(current);
        }
        Ok(())
    }

    #[must_use]
    pub fn directory_name(&self) -> String {
        let mut output = String::with_capacity(32);
        for byte in self.generation_id.0 {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
        }
        output
    }
}

fn strictly_sorted<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn validate_component(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 255
        || matches!(value, "." | "..")
        || value.contains('/')
        || value.contains('\\')
        || value.chars().any(char::is_control)
    {
        return activation_error(format!("invalid relative {label}"));
    }
    Ok(())
}

fn activation_error<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorCode::Activation, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generation() -> ActivationGeneration {
        ActivationGeneration {
            schema: 1,
            generation_id: GenerationId([0x23; 16]),
            previous_generation: None,
            profile_id: "kt6-5.17".into(),
            profile_digest: Digest::of(b"profile"),
            entries: vec![ActivationEntry {
                id: "org.example.reader".into(),
                channel: "dev".into(),
                kind: BundleKind::Application,
                bundle_root: Digest::of(b"bundle"),
                code_version: "1-abcdef12".into(),
                data_generation: Some("data-1".into()),
                dependency_roots: Vec::new(),
            }],
        }
    }

    #[test]
    fn canonical_round_trip() {
        let generation = generation();
        let encoded = generation.to_cbor().unwrap();
        assert_eq!(
            ActivationGeneration::from_cbor(&encoded).unwrap(),
            generation
        );
        assert_eq!(generation.directory_name(), "23".repeat(16));
    }

    #[test]
    fn rejects_path_injection() {
        let mut generation = generation();
        generation.entries[0].code_version = "../../etc".into();
        assert_eq!(
            generation.validate().unwrap_err().code,
            ErrorCode::Activation
        );
    }
}
