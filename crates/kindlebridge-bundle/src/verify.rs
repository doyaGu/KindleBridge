use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read, Seek, SeekFrom};

use ed25519_dalek::{Signature, Verifier as _, VerifyingKey};

use crate::cbor::{from_canonical_slice, to_canonical_vec};
use crate::error::{Error, ErrorCode, Result, ResultExt};
use crate::header::{align8, Header, FORMAT_MAJOR, FORMAT_MINOR, HEADER_SIZE};
use crate::model::{
    BlockDescriptor, BundleKind, Digest, Envelope, FileEntry, FileType, RotationProof,
    SignatureEntry, SignaturePolicy,
};
use crate::path::{
    validate_bundle_path, validate_channel, validate_logical_id, validate_symlink_target,
    validate_symlinks, validate_tree_paths,
};
use crate::{BLOCK_SIZE, PROFILE};

const MAX_ENTRIES: usize = 65_535;
const MAX_BLOCKS: usize = 65_535;
const MAX_BLOCK_REFS: usize = 131_072;
const MAX_ANNOTATIONS: usize = 128;
const KNOWN_PERMISSIONS: &[&str] = &[
    "bundle.install",
    "bundle.publish.dev",
    "debug.native",
    "device.admin",
    "device.read",
    "fs.app",
    "fs.user",
    "network.forward",
    "perf.read",
    "process.app",
    "shell.root",
    "shell.user",
    "ui.capture",
    "ui.inject",
    "ui.inspect",
];

#[derive(Clone, Debug)]
pub struct Inspection {
    pub header: Header,
    pub envelope: Envelope,
    pub signatures: Vec<SignatureEntry>,
    pub file_length: u64,
}

#[derive(Clone, Debug)]
pub struct VerifiedBundle {
    pub inspection: Inspection,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct VerifyOptions<'a> {
    /// If set, the embedded publisher key must be exactly this key.
    pub expected_publisher: Option<&'a VerifyingKey>,
    /// If set, the profile's sole variant must target this value.
    pub target: Option<&'a str>,
}

pub fn inspect<R: Read + Seek>(reader: &mut R) -> Result<Inspection> {
    let file_length = reader.seek(SeekFrom::End(0))?;
    reader.seek(SeekFrom::Start(0))?;
    let mut header_bytes = [0_u8; HEADER_SIZE];
    reader.read_exact(&mut header_bytes).map_err(|error| {
        Error::new(
            ErrorCode::Header,
            format!("cannot read fixed KBB header: {error}"),
        )
    })?;
    let header = Header::decode(&header_bytes, file_length)?;

    let envelope_bytes = read_section(reader, HEADER_SIZE as u64, header.envelope_length)?;
    let actual_root = Digest::of(&envelope_bytes);
    if actual_root != header.bundle_root {
        return Err(Error::new(
            ErrorCode::Header,
            "envelope BLAKE3 root mismatch",
        ));
    }
    check_zero_range(
        reader,
        HEADER_SIZE as u64 + header.envelope_length,
        header.signature_offset,
    )?;
    let signature_bytes = read_section(reader, header.signature_offset, header.signature_length)?;
    check_zero_range(
        reader,
        header.signature_offset + header.signature_length,
        header.payload_offset,
    )?;

    let envelope: Envelope = from_canonical_slice(&envelope_bytes)?;
    let signatures: Vec<SignatureEntry> = from_canonical_slice(&signature_bytes)?;
    validate_metadata(&envelope, &signatures, &header)?;
    Ok(Inspection {
        header,
        envelope,
        signatures,
        file_length,
    })
}

pub fn inspect_bytes(bytes: &[u8]) -> Result<Inspection> {
    inspect(&mut Cursor::new(bytes))
}

pub fn verify<R: Read + Seek>(
    reader: &mut R,
    options: &VerifyOptions<'_>,
) -> Result<VerifiedBundle> {
    let inspection = inspect(reader)?;
    if let Some(target) = options.target {
        if inspection.envelope.variants[0].target != target {
            return Err(Error::new(
                ErrorCode::Target,
                format!("bundle target does not match {target}"),
            ));
        }
    }
    verify_publisher(&inspection, options.expected_publisher)?;
    verify_rotation(&inspection.envelope)?;
    verify_payload(reader, &inspection)?;
    Ok(VerifiedBundle { inspection })
}

pub fn verify_bytes(bytes: &[u8], options: &VerifyOptions<'_>) -> Result<VerifiedBundle> {
    verify(&mut Cursor::new(bytes), options)
}

pub(crate) fn compute_key_id(public_key: &[u8; 32]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"KBB-KEY-ID-V1\0");
    hasher.update(&1_u16.to_le_bytes());
    hasher.update(public_key);
    Digest(*hasher.finalize().as_bytes())
}

pub(crate) fn compute_tree_root(entries: &[FileEntry]) -> Result<Digest> {
    let encoded = to_canonical_vec(&entries)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"KBB-TREE-V1\0");
    hasher.update(&encoded);
    Ok(Digest(*hasher.finalize().as_bytes()))
}

pub(crate) fn signature_input(bundle_root: Digest) -> Vec<u8> {
    let mut input = Vec::with_capacity(53);
    input.extend_from_slice(b"KBB-SIGNATURE-V1\0");
    input.extend_from_slice(&FORMAT_MAJOR.to_le_bytes());
    input.extend_from_slice(&FORMAT_MINOR.to_le_bytes());
    input.extend_from_slice(bundle_root.as_bytes());
    input
}

fn validate_metadata(
    envelope: &Envelope,
    signatures: &[SignatureEntry],
    header: &Header,
) -> Result<()> {
    if envelope.schema != 1 || envelope.profile != PROFILE {
        return profile_error("unknown envelope schema or profile");
    }
    if envelope.variants.len() != 1
        || !envelope.dependencies.is_empty()
        || !envelope.migrations.is_empty()
        || envelope.signature_policy != SignaturePolicy::default()
        || signatures.len() != 1
    {
        return profile_error(
            "kindlebridge.bundle.v1 requires one variant, empty dependencies/migrations, and one publisher",
        );
    }
    validate_logical_id(&envelope.id)?;
    if envelope.version.len() > 64 {
        return schema_error("version exceeds 64 bytes");
    }
    let version = semver::Version::parse(&envelope.version)
        .context(ErrorCode::Schema, "version is not SemVer 2.0.0")?;
    if version.to_string() != envelope.version || envelope.release == 0 {
        return schema_error("version is not canonical SemVer or release is zero");
    }
    validate_channel(&envelope.channel)?;
    validate_publisher(envelope)?;
    validate_variant_and_tree(envelope)?;
    validate_blocks(envelope, header)?;
    validate_permissions_and_policy(envelope)?;
    validate_signature_shape(envelope, signatures)?;
    if envelope.rotation.len() > 16 {
        return Err(Error::new(ErrorCode::Bounds, "rotation depth exceeds 16"));
    }
    if let Some(annotations) = &envelope.annotations {
        if annotations.len() > MAX_ANNOTATIONS
            || annotations
                .iter()
                .any(|(key, value)| key.len() > 256 || value.len() > 4096)
        {
            return Err(Error::new(
                ErrorCode::Bounds,
                "annotations exceed static limits",
            ));
        }
    }
    Ok(())
}

fn validate_publisher(envelope: &Envelope) -> Result<()> {
    if envelope.publisher.algorithm != 1 || envelope.publisher.public_key.len() != 32 {
        return profile_error("publisher must use a 32-byte Ed25519 key");
    }
    let public_key: &[u8; 32] = envelope
        .publisher
        .public_key
        .as_slice()
        .try_into()
        .expect("length checked");
    if compute_key_id(public_key) != envelope.publisher.key_id {
        return Err(Error::new(
            ErrorCode::Publisher,
            "publisher key ID mismatch",
        ));
    }
    Ok(())
}

fn validate_variant_and_tree(envelope: &Envelope) -> Result<()> {
    let variant = &envelope.variants[0];
    if !matches!(variant.target.as_str(), "kindlehf" | "kindlepw2") {
        return Err(Error::new(ErrorCode::Target, "unknown KBB v1 target"));
    }
    if variant.os.is_empty() || variant.arch.is_empty() || variant.abi.is_empty() {
        return schema_error("variant os/arch/abi may not be empty");
    }
    validate_firmware_bound(variant.firmware_min.as_deref())?;
    validate_firmware_bound(variant.firmware_max_exclusive.as_deref())?;
    if let (Some(min), Some(max)) = (
        variant.firmware_min.as_deref(),
        variant.firmware_max_exclusive.as_deref(),
    ) {
        if compare_firmware(min, max) != std::cmp::Ordering::Less {
            return schema_error("firmware_min must be below firmware_max_exclusive");
        }
    }
    if !strictly_sorted(&variant.required_features) || !strictly_sorted(&variant.optional_features)
    {
        return schema_error("variant feature arrays must be strictly sorted");
    }
    if envelope.trees.len() != 1 || envelope.trees[0].root != variant.tree {
        return profile_error("single variant must reference the sole tree");
    }
    let tree = &envelope.trees[0];
    if tree.entries.len() > MAX_ENTRIES {
        return Err(Error::new(
            ErrorCode::Bounds,
            "tree entry count exceeds 65,535",
        ));
    }
    validate_tree_paths(&tree.entries)?;
    let mut total_refs = 0_usize;
    for entry in &tree.entries {
        validate_file_entry(entry)?;
        if let Some(refs) = &entry.block_refs {
            total_refs = total_refs
                .checked_add(refs.len())
                .ok_or_else(|| Error::new(ErrorCode::Bounds, "block reference count overflow"))?;
        }
    }
    if total_refs > MAX_BLOCK_REFS {
        return Err(Error::new(
            ErrorCode::Bounds,
            "block reference count exceeds 131,072",
        ));
    }
    validate_symlinks(&tree.entries)?;
    if compute_tree_root(&tree.entries)? != tree.root {
        return Err(Error::new(ErrorCode::Tree, "semantic tree root mismatch"));
    }

    if envelope.kind == BundleKind::Application && !variant.entrypoints.contains_key("main") {
        return schema_error("application requires a main entrypoint");
    }
    for (name, path) in &variant.entrypoints {
        if name.is_empty() || name.len() > 64 {
            return schema_error("invalid entrypoint name");
        }
        validate_bundle_path(path)?;
        let entry = tree
            .entries
            .binary_search_by(|entry| entry.path.as_str().cmp(path.as_str()))
            .ok()
            .map(|index| &tree.entries[index])
            .ok_or_else(|| Error::new(ErrorCode::Tree, "entrypoint path does not exist"))?;
        if entry.file_type != FileType::Regular || entry.mode != Some(1) {
            return Err(Error::new(
                ErrorCode::Tree,
                "entrypoint must be an executable regular file",
            ));
        }
    }
    Ok(())
}

fn validate_file_entry(entry: &FileEntry) -> Result<()> {
    match entry.file_type {
        FileType::Directory => {
            if entry.mode != Some(0)
                || entry.size.is_some()
                || entry.file_digest.is_some()
                || entry.block_refs.is_some()
                || entry.target.is_some()
            {
                return tree_error("directory has non-canonical fields");
            }
        }
        FileType::Regular => {
            if !matches!(entry.mode, Some(0 | 1))
                || entry.size.is_none()
                || entry.file_digest.is_none()
                || entry.block_refs.is_none()
                || entry.target.is_some()
            {
                return tree_error("regular file has non-canonical fields");
            }
            let size = entry.size.expect("checked");
            let refs = entry.block_refs.as_ref().expect("checked");
            if size == 0 {
                if !refs.is_empty() || entry.file_digest != Some(Digest::of(&[])) {
                    return tree_error("empty regular file has invalid digest or block refs");
                }
            } else if refs.is_empty() {
                return tree_error("non-empty regular file has no blocks");
            }
            let mut sum = 0_u64;
            for (index, (_, raw_size)) in refs.iter().enumerate() {
                if *raw_size == 0
                    || *raw_size > BLOCK_SIZE as u64
                    || index + 1 != refs.len() && *raw_size != BLOCK_SIZE as u64
                {
                    return tree_error("regular file violates fixed 64 KiB chunking");
                }
                sum = sum
                    .checked_add(*raw_size)
                    .ok_or_else(|| Error::new(ErrorCode::Bounds, "file block size sum overflow"))?;
            }
            if sum != size {
                return tree_error("file block size sum does not equal file size");
            }
        }
        FileType::SymlinkRelative => {
            if entry.mode.is_some()
                || entry.size.is_some()
                || entry.file_digest.is_some()
                || entry.block_refs.is_some()
                || entry.target.is_none()
            {
                return tree_error("symlink has non-canonical fields");
            }
            validate_symlink_target(entry.target.as_deref().expect("checked"))?;
        }
    }
    Ok(())
}

fn validate_blocks(envelope: &Envelope, header: &Header) -> Result<()> {
    if envelope.blocks.len() > MAX_BLOCKS {
        return Err(Error::new(ErrorCode::Bounds, "block count exceeds 65,535"));
    }
    let mut referenced = BTreeSet::new();
    for entry in &envelope.trees[0].entries {
        if let Some(refs) = &entry.block_refs {
            referenced.extend(refs.iter().copied());
        }
    }

    let mut previous_key = None;
    let mut expected_offset = 0_u64;
    let mut descriptor_keys = BTreeSet::new();
    for descriptor in &envelope.blocks {
        let key = (descriptor.digest, descriptor.raw_size);
        if previous_key.is_some_and(|previous| previous >= key) {
            return block_error("block descriptors are not strictly sorted");
        }
        previous_key = Some(key);
        if !(1..=BLOCK_SIZE as u64).contains(&descriptor.raw_size)
            || descriptor.stored_size == 0
            || descriptor.stored_size > BLOCK_SIZE as u64
            || !matches!(descriptor.codec, 0 | 1)
        {
            return block_error("invalid block size or codec");
        }
        if descriptor.codec == 0
            && (descriptor.stored_size != descriptor.raw_size
                || descriptor.stored_digest != descriptor.digest)
        {
            return block_error("none codec descriptor is inconsistent");
        }
        if descriptor.codec == 1 && descriptor.stored_size >= descriptor.raw_size {
            return block_error("zstd block is not smaller than its raw block");
        }
        expected_offset = align8(expected_offset)?;
        if descriptor.payload_offset != expected_offset {
            return block_error("non-canonical block payload offset");
        }
        expected_offset = expected_offset
            .checked_add(descriptor.stored_size)
            .ok_or_else(|| Error::new(ErrorCode::Bounds, "block payload end overflow"))?;
        descriptor_keys.insert(key);
    }
    let expected_payload_length = if envelope.blocks.is_empty() {
        0
    } else {
        align8(expected_offset)?
    };
    if header.payload_length != expected_payload_length {
        return block_error("payload length is not canonical for its block table");
    }
    if descriptor_keys != referenced {
        return block_error("block table has a missing or unused descriptor");
    }
    Ok(())
}

fn validate_permissions_and_policy(envelope: &Envelope) -> Result<()> {
    if !strictly_sorted(&envelope.permissions.requested)
        || !strictly_sorted(&envelope.permissions.optional)
    {
        return schema_error("permission arrays must be strictly sorted");
    }
    if envelope
        .permissions
        .requested
        .iter()
        .any(|permission| !KNOWN_PERMISSIONS.contains(&permission.as_str()))
    {
        return schema_error("unknown requested permission");
    }
    if envelope.permissions.requested.iter().any(|permission| {
        envelope
            .permissions
            .optional
            .binary_search(permission)
            .is_ok()
    }) {
        return schema_error("permission cannot be both requested and optional");
    }
    if let Some(process) = &envelope.process {
        if process.stop_timeout_ms > 30_000 {
            return schema_error("process stop timeout exceeds 30 seconds");
        }
        if let Some(path) = &process.working_dir {
            validate_bundle_path(path)?;
        }
        if let Some(environment) = &process.environment {
            for (key, value) in environment {
                if !valid_environment_key(key)
                    || key.starts_with("KINDLEBRIDGE_")
                    || key.starts_with("LD_")
                    || value.contains('\0')
                {
                    return schema_error("invalid or reserved process environment variable");
                }
            }
        }
    }
    if envelope.kind == BundleKind::DeviceProfile
        && (!envelope.variants[0].entrypoints.is_empty()
            || !envelope.permissions.requested.is_empty()
            || !envelope.permissions.optional.is_empty()
            || envelope.process.is_some()
            || !matches!(
                envelope.data.policy,
                crate::model::DataPolicyKind::Ephemeral
            )
            || envelope.trees[0]
                .entries
                .iter()
                .any(|entry| entry.file_type == FileType::Regular && entry.mode != Some(0)))
    {
        return profile_error("device-profile violates its non-executable policy");
    }
    Ok(())
}

fn validate_signature_shape(envelope: &Envelope, signatures: &[SignatureEntry]) -> Result<()> {
    let signature = &signatures[0];
    if signature.role != 1
        || signature.algorithm != 1
        || signature.key_id != envelope.publisher.key_id
        || signature.signature.len() != 64
    {
        return profile_error("invalid single Ed25519 publisher signature entry");
    }
    Ok(())
}

fn verify_publisher(
    inspection: &Inspection,
    expected_publisher: Option<&VerifyingKey>,
) -> Result<()> {
    let public_key: &[u8; 32] = inspection
        .envelope
        .publisher
        .public_key
        .as_slice()
        .try_into()
        .expect("metadata validated key length");
    let verifying_key = VerifyingKey::from_bytes(public_key)
        .context(ErrorCode::Publisher, "invalid Ed25519 publisher key")?;
    if expected_publisher.is_some_and(|expected| expected != &verifying_key) {
        return Err(Error::new(
            ErrorCode::Publisher,
            "publisher does not match the expected key",
        ));
    }
    let signature = Signature::from_slice(&inspection.signatures[0].signature)
        .context(ErrorCode::Signature, "invalid Ed25519 signature encoding")?;
    verifying_key
        .verify(&signature_input(inspection.header.bundle_root), &signature)
        .context(
            ErrorCode::Signature,
            "publisher signature verification failed",
        )
}

fn verify_rotation(envelope: &Envelope) -> Result<()> {
    let mut previous_proof_digest = None;
    let mut previous_to_key: Option<Vec<u8>> = None;
    let mut previous_release = 0_u64;
    let mut seen_keys = BTreeSet::new();
    for (index, proof) in envelope.rotation.iter().enumerate() {
        validate_rotation_fields(proof, envelope)?;
        if index == 0 {
            seen_keys.insert(proof.signed.from_public_key.clone());
        } else if previous_to_key.as_deref() != Some(proof.signed.from_public_key.as_slice())
            || previous_proof_digest != Some(proof.signed.previous_proof_digest)
        {
            return Err(Error::new(
                ErrorCode::Publisher,
                "rotation proof chain is discontinuous",
            ));
        }
        if proof.signed.valid_from_release <= previous_release
            || proof.signed.valid_from_release > envelope.release
            || !seen_keys.insert(proof.signed.to_public_key.clone())
        {
            return Err(Error::new(
                ErrorCode::Publisher,
                "rotation release order or key lineage is invalid",
            ));
        }
        let from: &[u8; 32] = proof
            .signed
            .from_public_key
            .as_slice()
            .try_into()
            .expect("rotation key length checked");
        let key = VerifyingKey::from_bytes(from)
            .context(ErrorCode::Publisher, "invalid rotation Ed25519 key")?;
        let signed = to_canonical_vec(&proof.signed)?;
        let mut input = b"KBB-ROTATION-V1\0".to_vec();
        input.extend_from_slice(&signed);
        let signature = Signature::from_slice(&proof.signature)
            .context(ErrorCode::Signature, "invalid rotation signature encoding")?;
        key.verify(&input, &signature).context(
            ErrorCode::Signature,
            "rotation signature verification failed",
        )?;
        previous_proof_digest = Some(rotation_proof_digest(proof)?);
        previous_to_key = Some(proof.signed.to_public_key.clone());
        previous_release = proof.signed.valid_from_release;
    }
    if let Some(terminal_key) = previous_to_key {
        if terminal_key != envelope.publisher.public_key {
            return Err(Error::new(
                ErrorCode::Publisher,
                "rotation terminal key is not the bundle publisher",
            ));
        }
    }
    Ok(())
}

fn validate_rotation_fields(proof: &RotationProof, envelope: &Envelope) -> Result<()> {
    if proof.signed.schema != 1
        || proof.signed.app_id != envelope.id
        || proof.signed.channel != envelope.channel
        || proof.signed.from_algorithm != 1
        || proof.signed.to_algorithm != 1
        || proof.signed.from_public_key.len() != 32
        || proof.signed.to_public_key.len() != 32
        || proof.signed.flags != 0
        || proof.signature.len() != 64
    {
        return Err(Error::new(
            ErrorCode::Publisher,
            "rotation proof fields violate KBB v1",
        ));
    }
    Ok(())
}

fn rotation_proof_digest(proof: &RotationProof) -> Result<Digest> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"KBB-ROTATION-PROOF-V1\0");
    hasher.update(&to_canonical_vec(&proof.signed)?);
    hasher.update(&proof.signature);
    Ok(Digest(*hasher.finalize().as_bytes()))
}

fn verify_payload<R: Read + Seek>(reader: &mut R, inspection: &Inspection) -> Result<()> {
    let envelope = &inspection.envelope;
    let mut previous_end = 0_u64;
    for descriptor in &envelope.blocks {
        check_zero_range(
            reader,
            inspection.header.payload_offset + previous_end,
            inspection.header.payload_offset + descriptor.payload_offset,
        )?;
        let raw = read_raw_block(reader, inspection.header.payload_offset, descriptor)?;
        if Digest::of(&raw) != descriptor.digest {
            return block_error("raw block digest mismatch");
        }
        previous_end = descriptor.payload_offset + descriptor.stored_size;
    }
    check_zero_range(
        reader,
        inspection.header.payload_offset + previous_end,
        inspection.header.payload_offset + inspection.header.payload_length,
    )?;

    let descriptor_index: BTreeMap<_, _> = envelope
        .blocks
        .iter()
        .enumerate()
        .map(|(index, descriptor)| ((descriptor.digest, descriptor.raw_size), index))
        .collect();
    for entry in &envelope.trees[0].entries {
        if entry.file_type != FileType::Regular {
            continue;
        }
        let mut hasher = blake3::Hasher::new();
        for block_ref in entry.block_refs.as_ref().expect("metadata validated") {
            let descriptor = &envelope.blocks[*descriptor_index
                .get(block_ref)
                .expect("metadata validated block reference")];
            hasher.update(&read_raw_block(
                reader,
                inspection.header.payload_offset,
                descriptor,
            )?);
        }
        if Digest(*hasher.finalize().as_bytes()) != entry.file_digest.expect("metadata validated") {
            return tree_error("regular file digest mismatch");
        }
    }
    Ok(())
}

fn read_raw_block<R: Read + Seek>(
    reader: &mut R,
    payload_section_offset: u64,
    descriptor: &BlockDescriptor,
) -> Result<Vec<u8>> {
    let stored = read_section(
        reader,
        payload_section_offset
            .checked_add(descriptor.payload_offset)
            .ok_or_else(|| Error::new(ErrorCode::Bounds, "absolute block offset overflow"))?,
        descriptor.stored_size,
    )?;
    if Digest::of(&stored) != descriptor.stored_digest {
        return block_error("stored block digest mismatch");
    }
    if descriptor.codec == 0 {
        return Ok(stored);
    }

    let frame_size = zstd::zstd_safe::find_frame_compressed_size(&stored)
        .context(ErrorCode::Block, "invalid zstd frame")?;
    if frame_size != stored.len() {
        return block_error("zstd block contains trailing data or multiple frames");
    }
    let content_size = zstd::zstd_safe::get_frame_content_size(&stored)
        .context(ErrorCode::Block, "cannot read zstd frame content size")?;
    if content_size != Some(descriptor.raw_size) {
        return block_error("zstd frame omits or misstates content size");
    }
    if zstd::zstd_safe::get_dict_id_from_frame(&stored).is_some() {
        return block_error("zstd dictionaries are forbidden");
    }
    zstd::bulk::decompress(
        &stored,
        usize::try_from(descriptor.raw_size)
            .map_err(|_| Error::new(ErrorCode::Bounds, "raw block exceeds host usize"))?,
    )
    .context(ErrorCode::Block, "zstd block decompression failed")
}

fn read_section<R: Read + Seek>(reader: &mut R, offset: u64, length: u64) -> Result<Vec<u8>> {
    let length = usize::try_from(length)
        .map_err(|_| Error::new(ErrorCode::Bounds, "section length exceeds host usize"))?;
    let mut bytes = vec![0_u8; length];
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn check_zero_range<R: Read + Seek>(reader: &mut R, start: u64, end: u64) -> Result<()> {
    if end < start {
        return Err(Error::new(ErrorCode::Bounds, "padding range is reversed"));
    }
    reader.seek(SeekFrom::Start(start))?;
    let mut remaining = end - start;
    let mut buffer = [0_u8; 4096];
    while remaining != 0 {
        let chunk = usize::try_from(remaining.min(buffer.len() as u64)).expect("bounded chunk");
        reader.read_exact(&mut buffer[..chunk])?;
        if buffer[..chunk].iter().any(|byte| *byte != 0) {
            return Err(Error::new(
                ErrorCode::Header,
                "alignment padding is non-zero",
            ));
        }
        remaining -= chunk as u64;
    }
    Ok(())
}

fn validate_firmware_bound(bound: Option<&[u64]>) -> Result<()> {
    let Some(bound) = bound else { return Ok(()) };
    if bound.is_empty()
        || bound.len() > 8
        || bound.iter().any(|component| *component > u16::MAX as u64)
        || bound.len() > 1 && bound[0] == 0
    {
        return schema_error("invalid firmware bound");
    }
    Ok(())
}

fn compare_firmware(left: &[u64], right: &[u64]) -> std::cmp::Ordering {
    for index in 0..left.len().max(right.len()) {
        match left
            .get(index)
            .copied()
            .unwrap_or(0)
            .cmp(&right.get(index).copied().unwrap_or(0))
        {
            std::cmp::Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    std::cmp::Ordering::Equal
}

fn strictly_sorted<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn valid_environment_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && (bytes[0].is_ascii_uppercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'_')
}

fn schema_error<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorCode::Schema, message))
}

fn profile_error<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorCode::ProfilePolicy, message))
}

fn tree_error<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorCode::Tree, message))
}

fn block_error<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorCode::Block, message))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use ed25519_dalek::SigningKey;

    use super::*;
    use crate::{BuildConfig, BundleBuilder, CompressionPolicy};

    fn bundle(compression: CompressionPolicy) -> (Vec<u8>, SigningKey) {
        let key = SigningKey::from_bytes(&[9; 32]);
        let mut config = BuildConfig::new(
            BundleKind::Application,
            "org.example.reader",
            "1.0.0",
            1,
            "kindlehf",
        );
        config.compression = compression;
        config.entrypoints.insert("main".into(), "bin/main".into());
        let mut builder = BundleBuilder::new(config);
        builder
            .add_file("bin/main", vec![0x55; BLOCK_SIZE + 1], true)
            .unwrap();
        (builder.build(&key).unwrap(), key)
    }

    #[test]
    fn verifies_none_and_zstd_blocks() {
        for policy in [CompressionPolicy::Never, CompressionPolicy::ZstdWhenSmaller] {
            let (bytes, key) = bundle(policy);
            let verified = verify_bytes(
                &bytes,
                &VerifyOptions {
                    expected_publisher: Some(&key.verifying_key()),
                    target: Some("kindlehf"),
                },
            )
            .unwrap();
            assert_eq!(verified.inspection.envelope.profile, PROFILE);
        }
    }

    #[test]
    fn rejects_header_and_payload_mutation() {
        let (bytes, _) = bundle(CompressionPolicy::Never);
        let mut header_mutation = bytes.clone();
        header_mutation[100] = 1;
        assert_eq!(
            inspect_bytes(&header_mutation).unwrap_err().code,
            ErrorCode::Header
        );

        let inspection = inspect_bytes(&bytes).unwrap();
        let mut payload_mutation = bytes;
        let offset = usize::try_from(inspection.header.payload_offset).unwrap();
        payload_mutation[offset] ^= 1;
        assert_eq!(
            verify(
                &mut Cursor::new(payload_mutation),
                &VerifyOptions::default()
            )
            .unwrap_err()
            .code,
            ErrorCode::Block
        );
    }

    #[test]
    fn inspect_does_not_accept_trailing_data() {
        let (mut bytes, _) = bundle(CompressionPolicy::Never);
        bytes.push(0);
        assert_eq!(inspect_bytes(&bytes).unwrap_err().code, ErrorCode::Header);
    }

    #[test]
    fn rejects_signature_mutation_and_wrong_publisher() {
        let (mut bytes, key) = bundle(CompressionPolicy::Never);
        let inspection = inspect_bytes(&bytes).unwrap();
        let signature_end = usize::try_from(
            inspection.header.signature_offset + inspection.header.signature_length,
        )
        .unwrap();
        bytes[signature_end - 1] ^= 1;
        assert_eq!(
            verify_bytes(&bytes, &VerifyOptions::default())
                .unwrap_err()
                .code,
            ErrorCode::Signature
        );

        let (bytes, _) = bundle(CompressionPolicy::Never);
        let other = SigningKey::from_bytes(&[3; 32]);
        assert_eq!(
            verify_bytes(
                &bytes,
                &VerifyOptions {
                    expected_publisher: Some(&other.verifying_key()),
                    target: Some("kindlehf"),
                },
            )
            .unwrap_err()
            .code,
            ErrorCode::Publisher
        );
        assert_ne!(key.verifying_key(), other.verifying_key());
    }

    #[test]
    fn profile_policy_rejects_reserved_post_1_0_fields() {
        let (bytes, _) = bundle(CompressionPolicy::Never);
        let mut inspection = inspect_bytes(&bytes).unwrap();
        inspection
            .envelope
            .dependencies
            .push(serde_cbor::Value::Null);
        assert_eq!(
            validate_metadata(
                &inspection.envelope,
                &inspection.signatures,
                &inspection.header,
            )
            .unwrap_err()
            .code,
            ErrorCode::ProfilePolicy
        );
    }

    #[test]
    fn fixed_chunk_boundaries_and_block_deduplication() {
        let key = SigningKey::from_bytes(&[5; 32]);
        let mut config = BuildConfig::new(
            BundleKind::Tool,
            "org.example.blocks",
            "1.0.0",
            1,
            "kindlehf",
        );
        config.compression = CompressionPolicy::Never;
        let shared = vec![0x31; BLOCK_SIZE];
        let mut builder = BundleBuilder::new(config);
        builder.add_file("empty", Vec::new(), false).unwrap();
        builder.add_file("exact", shared.clone(), false).unwrap();
        let mut plus_one = shared.clone();
        plus_one.push(0x32);
        builder.add_file("plus-one", plus_one, false).unwrap();
        builder.add_file("shared", shared, false).unwrap();
        let bytes = builder.build(&key).unwrap();
        let inspected = inspect_bytes(&bytes).unwrap();
        assert_eq!(inspected.envelope.blocks.len(), 2);
        let entries = &inspected.envelope.trees[0].entries;
        assert!(entries
            .iter()
            .find(|entry| entry.path == "empty")
            .unwrap()
            .block_refs
            .as_ref()
            .unwrap()
            .is_empty());
        assert_eq!(
            entries
                .iter()
                .find(|entry| entry.path == "plus-one")
                .unwrap()
                .block_refs
                .as_ref()
                .unwrap()
                .len(),
            2
        );
    }
}
