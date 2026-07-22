use std::collections::{BTreeMap, BTreeSet};

use ed25519_dalek::{Signer as _, SigningKey};

use crate::cbor::to_canonical_vec;
use crate::error::{Error, ErrorCode, Result, ResultExt};
use crate::header::{align8, Header, HEADER_SIZE};
use crate::model::{
    BlockDescriptor, BundleKind, DataPolicy, Digest, Envelope, FileEntry, FileType, Permissions,
    ProcessPolicy, Publisher, SignatureEntry, SignaturePolicy, Tree, Variant,
};
use crate::path::{validate_bundle_path, validate_symlink_target};
use crate::verify::{
    compute_key_id, compute_tree_root, signature_input, verify_bytes, VerifyOptions,
};
use crate::{BLOCK_SIZE, PROFILE};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CompressionPolicy {
    Never,
    #[default]
    ZstdWhenSmaller,
}

#[derive(Clone, Debug)]
pub struct BuildConfig {
    pub kind: BundleKind,
    pub id: String,
    pub version: String,
    pub release: u64,
    pub channel: String,
    pub target: String,
    pub os: String,
    pub arch: String,
    pub abi: String,
    pub firmware_min: Option<Vec<u64>>,
    pub firmware_max_exclusive: Option<Vec<u64>>,
    pub required_features: Vec<String>,
    pub optional_features: Vec<String>,
    pub entrypoints: BTreeMap<String, String>,
    pub permissions: Permissions,
    pub process: Option<ProcessPolicy>,
    pub data: DataPolicy,
    pub annotations: Option<BTreeMap<String, String>>,
    pub publisher_name: Option<String>,
    pub compression: CompressionPolicy,
}

impl BuildConfig {
    #[must_use]
    pub fn new(
        kind: BundleKind,
        id: impl Into<String>,
        version: impl Into<String>,
        release: u64,
        target: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            id: id.into(),
            version: version.into(),
            release,
            channel: "dev".into(),
            target: target.into(),
            os: "linux".into(),
            arch: "arm".into(),
            abi: "gnueabihf".into(),
            firmware_min: None,
            firmware_max_exclusive: None,
            required_features: Vec::new(),
            optional_features: Vec::new(),
            entrypoints: BTreeMap::new(),
            permissions: Permissions::default(),
            process: None,
            data: DataPolicy::default(),
            annotations: None,
            publisher_name: None,
            compression: CompressionPolicy::default(),
        }
    }
}

#[derive(Clone, Debug)]
enum InputEntry {
    Directory,
    Regular { bytes: Vec<u8>, executable: bool },
    Symlink { target: String },
}

#[derive(Clone, Debug)]
struct StoredBlock {
    raw: Vec<u8>,
    stored: Vec<u8>,
    codec: u64,
}

#[derive(Clone, Debug)]
pub struct BundleBuilder {
    config: BuildConfig,
    entries: BTreeMap<String, InputEntry>,
}

impl BundleBuilder {
    #[must_use]
    pub fn new(config: BuildConfig) -> Self {
        Self {
            config,
            entries: BTreeMap::new(),
        }
    }

    pub fn add_directory(&mut self, path: impl Into<String>) -> Result<&mut Self> {
        let path = path.into();
        validate_bundle_path(&path)?;
        self.ensure_parents(&path)?;
        self.insert(path, InputEntry::Directory)?;
        Ok(self)
    }

    pub fn add_file(
        &mut self,
        path: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
        executable: bool,
    ) -> Result<&mut Self> {
        let path = path.into();
        validate_bundle_path(&path)?;
        self.ensure_parents(&path)?;
        self.insert(
            path,
            InputEntry::Regular {
                bytes: bytes.into(),
                executable,
            },
        )?;
        Ok(self)
    }

    pub fn add_symlink(
        &mut self,
        path: impl Into<String>,
        target: impl Into<String>,
    ) -> Result<&mut Self> {
        let path = path.into();
        let target = target.into();
        validate_bundle_path(&path)?;
        validate_symlink_target(&target)?;
        self.ensure_parents(&path)?;
        self.insert(path, InputEntry::Symlink { target })?;
        Ok(self)
    }

    pub fn build(&self, signing_key: &SigningKey) -> Result<Vec<u8>> {
        let mut blocks: BTreeMap<(Digest, u64), StoredBlock> = BTreeMap::new();
        let mut tree_entries = Vec::with_capacity(self.entries.len());

        for (path, entry) in &self.entries {
            match entry {
                InputEntry::Directory => tree_entries.push(FileEntry::directory(path)),
                InputEntry::Symlink { target } => tree_entries.push(FileEntry {
                    path: path.clone(),
                    file_type: FileType::SymlinkRelative,
                    mode: None,
                    size: None,
                    file_digest: None,
                    block_refs: None,
                    target: Some(target.clone()),
                }),
                InputEntry::Regular { bytes, executable } => {
                    let mut block_refs = Vec::new();
                    for raw in bytes.chunks(BLOCK_SIZE) {
                        let digest = Digest::of(raw);
                        let raw_size = u64::try_from(raw.len()).map_err(|_| {
                            Error::new(ErrorCode::Bounds, "block length exceeds uint64")
                        })?;
                        let key = (digest, raw_size);
                        if let Some(existing) = blocks.get(&key) {
                            if existing.raw != raw {
                                return Err(Error::new(
                                    ErrorCode::Block,
                                    "BLAKE3 collision for unequal block content",
                                ));
                            }
                        } else {
                            blocks.insert(key, self.store_block(raw)?);
                        }
                        block_refs.push(key);
                    }
                    tree_entries.push(FileEntry {
                        path: path.clone(),
                        file_type: FileType::Regular,
                        mode: Some(u64::from(*executable)),
                        size: Some(u64::try_from(bytes.len()).map_err(|_| {
                            Error::new(ErrorCode::Bounds, "file length exceeds uint64")
                        })?),
                        file_digest: Some(Digest::of(bytes)),
                        block_refs: Some(block_refs),
                        target: None,
                    });
                }
            }
        }

        let tree_root = compute_tree_root(&tree_entries)?;
        let tree = Tree {
            root: tree_root,
            entries: tree_entries,
        };
        let mut payload = Vec::new();
        let mut descriptors = Vec::with_capacity(blocks.len());
        for ((digest, raw_size), block) in blocks {
            pad_to_8(&mut payload)?;
            let payload_offset = u64::try_from(payload.len())
                .map_err(|_| Error::new(ErrorCode::Bounds, "payload exceeds uint64"))?;
            payload.extend_from_slice(&block.stored);
            descriptors.push(BlockDescriptor {
                digest,
                raw_size,
                codec: block.codec,
                stored_size: u64::try_from(block.stored.len())
                    .map_err(|_| Error::new(ErrorCode::Bounds, "stored block exceeds uint64"))?,
                stored_digest: Digest::of(&block.stored),
                payload_offset,
            });
        }
        pad_to_8(&mut payload)?;

        let public_key = signing_key.verifying_key().to_bytes();
        let key_id = compute_key_id(&public_key);
        let envelope = Envelope {
            schema: 1,
            profile: PROFILE.into(),
            kind: self.config.kind,
            id: self.config.id.clone(),
            version: self.config.version.clone(),
            release: self.config.release,
            channel: self.config.channel.clone(),
            publisher: Publisher {
                algorithm: 1,
                public_key: public_key.to_vec(),
                key_id,
                name: self.config.publisher_name.clone(),
            },
            variants: vec![Variant {
                target: self.config.target.clone(),
                os: self.config.os.clone(),
                arch: self.config.arch.clone(),
                abi: self.config.abi.clone(),
                firmware_min: self.config.firmware_min.clone(),
                firmware_max_exclusive: self.config.firmware_max_exclusive.clone(),
                required_features: sorted_unique(&self.config.required_features)?,
                optional_features: sorted_unique(&self.config.optional_features)?,
                tree: tree_root,
                entrypoints: self.config.entrypoints.clone(),
            }],
            trees: vec![tree],
            blocks: descriptors,
            permissions: Permissions {
                requested: sorted_unique(&self.config.permissions.requested)?,
                optional: sorted_unique(&self.config.permissions.optional)?,
            },
            process: self.config.process.clone(),
            data: self.config.data.clone(),
            dependencies: Vec::new(),
            migrations: Vec::new(),
            signature_policy: SignaturePolicy::default(),
            rotation: Vec::new(),
            annotations: self.config.annotations.clone(),
        };
        let envelope_bytes = to_canonical_vec(&envelope)?;
        let bundle_root = Digest::of(&envelope_bytes);
        let signature = signing_key.sign(&signature_input(bundle_root));
        let signature_bytes = to_canonical_vec(&vec![SignatureEntry {
            role: 1,
            algorithm: 1,
            key_id,
            signature: signature.to_bytes().to_vec(),
        }])?;

        let envelope_length = u64::try_from(envelope_bytes.len())
            .map_err(|_| Error::new(ErrorCode::Bounds, "envelope exceeds uint64"))?;
        let signature_offset = align8(HEADER_SIZE as u64 + envelope_length)?;
        let signature_length = u64::try_from(signature_bytes.len())
            .map_err(|_| Error::new(ErrorCode::Bounds, "signature section exceeds uint64"))?;
        let payload_offset = align8(signature_offset + signature_length)?;
        let header = Header {
            envelope_length,
            signature_offset,
            signature_length,
            payload_offset,
            payload_length: u64::try_from(payload.len())
                .map_err(|_| Error::new(ErrorCode::Bounds, "payload exceeds uint64"))?,
            bundle_root,
        };
        let mut output = Vec::with_capacity(
            usize::try_from(payload_offset)
                .ok()
                .and_then(|offset| offset.checked_add(payload.len()))
                .ok_or_else(|| Error::new(ErrorCode::Bounds, "bundle exceeds host usize"))?,
        );
        output.extend_from_slice(&header.encode()?);
        output.extend_from_slice(&envelope_bytes);
        pad_to_offset(&mut output, signature_offset)?;
        output.extend_from_slice(&signature_bytes);
        pad_to_offset(&mut output, payload_offset)?;
        output.extend_from_slice(&payload);

        let verifying_key = signing_key.verifying_key();
        verify_bytes(
            &output,
            &VerifyOptions {
                expected_publisher: Some(&verifying_key),
                target: Some(&self.config.target),
                firmware: None,
            },
        )?;
        Ok(output)
    }

    fn store_block(&self, raw: &[u8]) -> Result<StoredBlock> {
        if self.config.compression == CompressionPolicy::ZstdWhenSmaller {
            let compressed = zstd::bulk::compress(raw, 1)
                .context(ErrorCode::Block, "zstd-1 compression failed")?;
            if compressed.len().saturating_mul(100) <= raw.len().saturating_mul(95) {
                return Ok(StoredBlock {
                    raw: raw.to_vec(),
                    stored: compressed,
                    codec: 1,
                });
            }
        }
        Ok(StoredBlock {
            raw: raw.to_vec(),
            stored: raw.to_vec(),
            codec: 0,
        })
    }

    fn ensure_parents(&mut self, path: &str) -> Result<()> {
        let components: Vec<_> = path.split('/').collect();
        let mut parent = String::new();
        for component in &components[..components.len().saturating_sub(1)] {
            if !parent.is_empty() {
                parent.push('/');
            }
            parent.push_str(component);
            match self.entries.get(&parent) {
                Some(InputEntry::Directory) => {}
                Some(_) => {
                    return Err(Error::new(
                        ErrorCode::Path,
                        "a parent path is already a non-directory entry",
                    ));
                }
                None => {
                    self.entries.insert(parent.clone(), InputEntry::Directory);
                }
            }
        }
        Ok(())
    }

    fn insert(&mut self, path: String, entry: InputEntry) -> Result<()> {
        if self.entries.insert(path.clone(), entry).is_some() {
            return Err(Error::new(
                ErrorCode::Path,
                format!("duplicate builder path {path}"),
            ));
        }
        Ok(())
    }
}

fn sorted_unique(values: &[String]) -> Result<Vec<String>> {
    let set: BTreeSet<_> = values.iter().cloned().collect();
    if set.len() != values.len() {
        return Err(Error::new(
            ErrorCode::Schema,
            "array contains duplicate values",
        ));
    }
    Ok(set.into_iter().collect())
}

fn pad_to_8(bytes: &mut Vec<u8>) -> Result<()> {
    let target = align8(
        u64::try_from(bytes.len())
            .map_err(|_| Error::new(ErrorCode::Bounds, "buffer exceeds uint64"))?,
    )?;
    pad_to_offset(bytes, target)
}

fn pad_to_offset(bytes: &mut Vec<u8>, offset: u64) -> Result<()> {
    let offset = usize::try_from(offset)
        .map_err(|_| Error::new(ErrorCode::Bounds, "offset exceeds host usize"))?;
    if offset < bytes.len() {
        return Err(Error::new(
            ErrorCode::Bounds,
            "padding target precedes current data",
        ));
    }
    bytes.resize(offset, 0);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_is_deterministic() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut config = BuildConfig::new(
            BundleKind::Application,
            "org.example.reader",
            "1.2.3",
            4,
            "kindlehf",
        );
        config
            .entrypoints
            .insert("main".into(), "bin/reader".into());
        let mut builder = BundleBuilder::new(config);
        builder
            .add_file("bin/reader", vec![0x41; BLOCK_SIZE + 1], true)
            .unwrap();
        let first = builder.build(&key).unwrap();
        let second = builder.build(&key).unwrap();
        assert_eq!(first, second);
    }
}
