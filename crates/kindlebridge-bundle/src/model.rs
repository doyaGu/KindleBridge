use std::collections::BTreeMap;
use std::fmt;

use serde::de::{Error as _, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub const ZERO: Self = Self([0; 32]);

    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

struct DigestVisitor;

impl<'de> Visitor<'de> for DigestVisitor {
    type Value = Digest;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("exactly 32 bytes")
    }

    fn visit_bytes<E>(self, value: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let bytes =
            <[u8; 32]>::try_from(value).map_err(|_| E::invalid_length(value.len(), &self))?;
        Ok(Digest(bytes))
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_bytes(&value)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(DigestVisitor)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BundleKind {
    #[serde(rename = "application")]
    Application,
    #[serde(rename = "runtime")]
    Runtime,
    #[serde(rename = "agent")]
    Agent,
    #[serde(rename = "tool")]
    Tool,
    #[serde(rename = "daemon")]
    Daemon,
    #[serde(rename = "device-profile")]
    DeviceProfile,
}

impl BundleKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Application => "application",
            Self::Runtime => "runtime",
            Self::Agent => "agent",
            Self::Tool => "tool",
            Self::Daemon => "daemon",
            Self::DeviceProfile => "device-profile",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Publisher {
    pub algorithm: u64,
    #[serde(with = "serde_bytes")]
    pub public_key: Vec<u8>,
    pub key_id: Digest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Variant {
    pub target: String,
    pub os: String,
    pub arch: String,
    pub abi: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub firmware_min: Option<Vec<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub firmware_max_exclusive: Option<Vec<u64>>,
    pub required_features: Vec<String>,
    pub optional_features: Vec<String>,
    pub tree: Digest,
    pub entrypoints: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FileType {
    Directory = 1,
    Regular = 2,
    SymlinkRelative = 3,
}

impl Serialize for FileType {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for FileType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match u8::deserialize(deserializer)? {
            1 => Ok(Self::Directory),
            2 => Ok(Self::Regular),
            3 => Ok(Self::SymlinkRelative),
            value => Err(D::Error::custom(format!("unknown file type {value}"))),
        }
    }
}

pub type BlockRef = (Digest, u64);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub file_type: FileType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_digest: Option<Digest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_refs: Option<Vec<BlockRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

impl FileEntry {
    #[must_use]
    pub fn directory(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            file_type: FileType::Directory,
            mode: Some(0),
            size: None,
            file_digest: None,
            block_refs: None,
            target: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Tree {
    pub root: Digest,
    pub entries: Vec<FileEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlockDescriptor {
    pub digest: Digest,
    pub raw_size: u64,
    pub codec: u64,
    pub stored_size: u64,
    pub stored_digest: Digest,
    pub payload_offset: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Permissions {
    pub requested: Vec<String>,
    pub optional: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RestartPolicy {
    #[serde(rename = "never")]
    Never,
    #[serde(rename = "on-failure")]
    OnFailure,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessPolicy {
    pub restart: RestartPolicy,
    pub stop_timeout_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DataPolicyKind {
    #[serde(rename = "preserve")]
    Preserve,
    #[serde(rename = "replace-empty")]
    ReplaceEmpty,
    #[serde(rename = "ephemeral")]
    Ephemeral,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DataPolicy {
    pub policy: DataPolicyKind,
    pub schema: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota_bytes: Option<u64>,
}

impl Default for DataPolicy {
    fn default() -> Self {
        Self {
            policy: DataPolicyKind::Preserve,
            schema: 1,
            quota_bytes: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SignaturePolicy {
    pub publisher_threshold: u64,
    pub publisher_algorithms: Vec<u64>,
    pub distributor_required: bool,
    pub distributor_threshold: u64,
    pub distributor_key_ids: Vec<Digest>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RotationProofSignedData {
    pub schema: u64,
    pub app_id: String,
    pub channel: String,
    pub from_algorithm: u64,
    #[serde(with = "serde_bytes")]
    pub from_public_key: Vec<u8>,
    pub to_algorithm: u64,
    #[serde(with = "serde_bytes")]
    pub to_public_key: Vec<u8>,
    pub valid_from_release: u64,
    pub previous_proof_digest: Digest,
    pub flags: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RotationProof {
    pub signed: RotationProofSignedData,
    #[serde(with = "serde_bytes")]
    pub signature: Vec<u8>,
}

impl Default for SignaturePolicy {
    fn default() -> Self {
        Self {
            publisher_threshold: 1,
            publisher_algorithms: vec![1],
            distributor_required: false,
            distributor_threshold: 0,
            distributor_key_ids: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Envelope {
    pub schema: u64,
    pub profile: String,
    pub kind: BundleKind,
    pub id: String,
    pub version: String,
    pub release: u64,
    pub channel: String,
    pub publisher: Publisher,
    pub variants: Vec<Variant>,
    pub trees: Vec<Tree>,
    pub blocks: Vec<BlockDescriptor>,
    pub permissions: Permissions,
    pub process: Option<ProcessPolicy>,
    pub data: DataPolicy,
    pub dependencies: Vec<serde_cbor::Value>,
    pub migrations: Vec<serde_cbor::Value>,
    pub signature_policy: SignaturePolicy,
    pub rotation: Vec<RotationProof>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SignatureEntry {
    pub role: u64,
    pub algorithm: u64,
    pub key_id: Digest,
    #[serde(with = "serde_bytes")]
    pub signature: Vec<u8>,
}
