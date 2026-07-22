use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::cbor::{from_canonical_slice, to_canonical_vec};
use crate::error::{Error, ErrorCode, Result};
use crate::verify::read_raw_block;
use crate::{
    DataPolicy, Digest, FileType, InstallStore, ProcessPolicy, RestartPolicy, VerifiedBundle,
};

const RUNTIME_MANIFEST_SCHEMA: u64 = 1;
const MAX_RUNTIME_MANIFEST_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RuntimeManifest {
    schema: u64,
    app_id: String,
    version: String,
    bundle_root: Digest,
    main_entrypoint: String,
    main_size: u64,
    main_digest: Digest,
    process: ProcessPolicy,
    data: DataPolicy,
}

/// A fully reconstructed, immutable application image ready for execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedApplication {
    pub app_id: String,
    pub version: String,
    pub bundle_root: Digest,
    pub image_root: PathBuf,
    pub main: PathBuf,
    pub process: ProcessPolicy,
    pub data: DataPolicy,
}

/// Copy every verified raw block into the immutable content-addressed store.
///
/// Callers must first obtain `verified` by verifying the same open file. Each
/// block is nevertheless checked again while being read, so swapping the input
/// behind a path cannot commit unchecked bytes.
pub fn ingest_verified_blocks<R: Read + Seek>(
    reader: &mut R,
    verified: &VerifiedBundle,
    store: &InstallStore,
) -> Result<usize> {
    let inspection = &verified.inspection;
    let mut inserted = 0_usize;
    for descriptor in &inspection.envelope.blocks {
        let raw = read_raw_block(reader, inspection.header.payload_offset, descriptor)?;
        if Digest::of(&raw) != descriptor.digest {
            return Err(Error::new(
                ErrorCode::Block,
                "raw block digest changed after bundle verification",
            ));
        }
        if store.put_block(descriptor.digest, &raw)? {
            inserted = inserted
                .checked_add(1)
                .ok_or_else(|| Error::new(ErrorCode::Bounds, "inserted block count overflow"))?;
        }
    }
    Ok(inserted)
}

/// Reconstruct a verified application tree from the immutable block store.
///
/// The image is staged beside its final content-addressed directory and then
/// renamed into place. A crash can therefore leave only an unreferenced staging
/// directory; it can never expose a partial executable tree as an installed
/// application.
pub fn materialize_verified_application(
    verified: &VerifiedBundle,
    store: &InstallStore,
) -> Result<MaterializedApplication> {
    let envelope = &verified.inspection.envelope;
    if envelope.kind != crate::BundleKind::Application {
        return Err(Error::new(
            ErrorCode::ProfilePolicy,
            "only application bundles can be materialized as applications",
        ));
    }
    let bundle_root = verified.inspection.header.bundle_root;
    let images = store.root().join("images");
    ensure_plain_directory(&images)?;
    let image_name = digest_hex(bundle_root);
    let final_dir = images.join(&image_name);
    if final_dir.exists() {
        return load_materialized_application(store, bundle_root);
    }

    let staging = images.join(format!(".staging-{image_name}"));
    if fs::symlink_metadata(&staging).is_ok() {
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir(&staging)?;
    let result = materialize_into(verified, store, &staging).and_then(|manifest| {
        sync_directory(&staging)?;
        match fs::rename(&staging, &final_dir) {
            Ok(()) => {
                sync_directory(&images)?;
                runtime_from_manifest(final_dir, manifest)
            }
            Err(_error) if final_dir.exists() => {
                fs::remove_dir_all(&staging)?;
                load_materialized_application(store, bundle_root)
            }
            Err(error) => Err(error.into()),
        }
    });
    if result.is_err() && staging.exists() {
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

/// Load and validate a previously materialized application image.
pub fn load_materialized_application(
    store: &InstallStore,
    bundle_root: Digest,
) -> Result<MaterializedApplication> {
    let image_dir = store.root().join("images").join(digest_hex(bundle_root));
    let metadata = fs::symlink_metadata(&image_dir)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            ErrorCode::Path,
            "materialized application image is not a plain directory",
        ));
    }
    let manifest_path = image_dir.join("runtime.cbor");
    let metadata = fs::symlink_metadata(&manifest_path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_RUNTIME_MANIFEST_BYTES
    {
        return Err(Error::new(
            ErrorCode::Bounds,
            "materialized application manifest is unsafe or too large",
        ));
    }
    let bytes = fs::read(manifest_path)?;
    let manifest: RuntimeManifest = from_canonical_slice(&bytes)?;
    if manifest.schema != RUNTIME_MANIFEST_SCHEMA || manifest.bundle_root != bundle_root {
        return Err(Error::new(
            ErrorCode::Activation,
            "materialized application identity does not match its directory",
        ));
    }
    runtime_from_manifest(image_dir, manifest)
}

fn materialize_into(
    verified: &VerifiedBundle,
    store: &InstallStore,
    staging: &Path,
) -> Result<RuntimeManifest> {
    let envelope = &verified.inspection.envelope;
    let variant = &envelope.variants[0];
    let tree = &envelope.trees[0];
    let root = staging.join("root");
    fs::create_dir(&root)?;

    for entry in tree
        .entries
        .iter()
        .filter(|entry| entry.file_type == FileType::Directory)
    {
        fs::create_dir(root.join(&entry.path))?;
    }
    for entry in tree
        .entries
        .iter()
        .filter(|entry| entry.file_type == FileType::Regular)
    {
        let path = root.join(&entry.path);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut hasher = blake3::Hasher::new();
        let mut written = 0_u64;
        for (digest, expected_size) in entry.block_refs.as_deref().unwrap_or_default() {
            let block = store
                .read_block(*digest)?
                .ok_or_else(|| Error::new(ErrorCode::Block, "materialization block is missing"))?;
            if block.len() as u64 != *expected_size || Digest::of(&block) != *digest {
                return Err(Error::new(
                    ErrorCode::Block,
                    "materialization block does not match its file reference",
                ));
            }
            file.write_all(&block)?;
            hasher.update(&block);
            written = written
                .checked_add(*expected_size)
                .ok_or_else(|| Error::new(ErrorCode::Bounds, "file size overflow"))?;
        }
        if written != entry.size.unwrap_or_default()
            || Digest(*hasher.finalize().as_bytes()) != entry.file_digest.unwrap_or(Digest::ZERO)
        {
            return Err(Error::new(
                ErrorCode::Block,
                "materialized file digest or size mismatch",
            ));
        }
        file.sync_all()?;
        set_file_mode(&path, entry.mode == Some(1))?;
    }
    for entry in tree
        .entries
        .iter()
        .filter(|entry| entry.file_type == FileType::SymlinkRelative)
    {
        create_relative_symlink(
            entry
                .target
                .as_deref()
                .expect("verified symlink has target"),
            &root.join(&entry.path),
        )?;
    }
    for entry in tree
        .entries
        .iter()
        .filter(|entry| entry.file_type == FileType::Directory)
        .rev()
    {
        set_directory_mode(&root.join(&entry.path))?;
    }
    set_directory_mode(&root)?;

    let process = envelope.process.clone().unwrap_or(ProcessPolicy {
        restart: RestartPolicy::Never,
        stop_timeout_ms: 3_000,
        working_dir: None,
        environment: None,
    });
    let main_entrypoint = variant
        .entrypoints
        .get("main")
        .expect("verified application has main entrypoint")
        .clone();
    let main_entry = tree
        .entries
        .iter()
        .find(|entry| entry.path == main_entrypoint)
        .expect("verified application main entrypoint exists");
    let manifest = RuntimeManifest {
        schema: RUNTIME_MANIFEST_SCHEMA,
        app_id: envelope.id.clone(),
        version: envelope.version.clone(),
        bundle_root: verified.inspection.header.bundle_root,
        main_entrypoint,
        main_size: main_entry.size.expect("verified main entrypoint has size"),
        main_digest: main_entry
            .file_digest
            .expect("verified main entrypoint has digest"),
        process,
        data: envelope.data.clone(),
    };
    let bytes = to_canonical_vec(&manifest)?;
    if bytes.len() as u64 > MAX_RUNTIME_MANIFEST_BYTES {
        return Err(Error::new(
            ErrorCode::Bounds,
            "materialized application manifest is too large",
        ));
    }
    let manifest_path = staging.join("runtime.cbor");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&manifest_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    set_file_mode(&manifest_path, false)?;
    Ok(manifest)
}

fn runtime_from_manifest(
    image_dir: PathBuf,
    manifest: RuntimeManifest,
) -> Result<MaterializedApplication> {
    crate::validate_bundle_path(&manifest.main_entrypoint)?;
    let image_root = image_dir.join("root");
    let main = image_root.join(&manifest.main_entrypoint);
    let metadata = fs::symlink_metadata(&main)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            ErrorCode::Activation,
            "materialized main entrypoint is not a regular file",
        ));
    }
    if metadata.len() != manifest.main_size || hash_file(&main)? != manifest.main_digest {
        return Err(Error::new(
            ErrorCode::Block,
            "materialized main entrypoint failed its integrity check",
        ));
    }
    if let Some(working_dir) = &manifest.process.working_dir {
        crate::validate_bundle_path(working_dir)?;
        let metadata = fs::symlink_metadata(image_root.join(working_dir))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(Error::new(
                ErrorCode::Activation,
                "materialized working directory is not a plain directory",
            ));
        }
    }
    Ok(MaterializedApplication {
        app_id: manifest.app_id,
        version: manifest.version,
        bundle_root: manifest.bundle_root,
        image_root,
        main,
        process: manifest.process,
        data: manifest.data,
    })
}

fn hash_file(path: &Path) -> Result<Digest> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(Digest(*hasher.finalize().as_bytes()))
}

fn ensure_plain_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(Error::new(
            ErrorCode::Path,
            "materialization root is not a plain directory",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn digest_hex(digest: Digest) -> String {
    digest.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(unix)]
fn set_file_mode(path: &Path, executable: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(
        path,
        fs::Permissions::from_mode(if executable { 0o555 } else { 0o444 }),
    )?;
    Ok(())
}

#[cfg(not(unix))]
fn set_file_mode(_path: &Path, _executable: bool) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_directory_mode(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o555))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_directory_mode(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_relative_symlink(target: &str, path: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, path)?;
    Ok(())
}

#[cfg(not(unix))]
fn create_relative_symlink(_target: &str, _path: &Path) -> Result<()> {
    Err(Error::new(
        ErrorCode::ProfilePolicy,
        "bundle symlink materialization is unsupported on this development host",
    ))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(all(test, feature = "builder"))]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use ed25519_dalek::SigningKey;

    use super::*;
    use crate::{BuildConfig, BundleBuilder, BundleKind, VerifyOptions};

    #[test]
    fn verified_blocks_are_ingested_idempotently() {
        let mut config = BuildConfig::new(
            BundleKind::Application,
            "org.example.ingest",
            "1.0.0",
            1,
            "kindlehf",
        );
        config.entrypoints = BTreeMap::from([("main".into(), "bin/app".into())]);
        let mut builder = BundleBuilder::new(config);
        builder
            .add_file("bin/app", vec![0x5a; crate::BLOCK_SIZE + 17], true)
            .unwrap();
        let bytes = builder.build(&SigningKey::from_bytes(&[7_u8; 32])).unwrap();
        let mut reader = Cursor::new(bytes);
        let verified = crate::verify(
            &mut reader,
            &VerifyOptions {
                target: Some("kindlehf"),
                ..VerifyOptions::default()
            },
        )
        .unwrap();
        let directory = std::env::temp_dir().join(format!(
            "kindlebridge-ingest-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = InstallStore::open(&directory, u64::MAX).unwrap();
        assert_eq!(
            ingest_verified_blocks(&mut reader, &verified, &store).unwrap(),
            2
        );
        assert_eq!(
            ingest_verified_blocks(&mut reader, &verified, &store).unwrap(),
            0
        );
        for block in &verified.inspection.envelope.blocks {
            assert!(store.read_block(block.digest).unwrap().is_some());
        }
        let first = materialize_verified_application(&verified, &store).unwrap();
        assert_eq!(first.app_id, "org.example.ingest");
        assert_eq!(first.main, first.image_root.join("bin/app"));
        assert_eq!(
            std::fs::read(&first.main).unwrap().len(),
            crate::BLOCK_SIZE + 17
        );
        let second = materialize_verified_application(&verified, &store).unwrap();
        assert_eq!(first, second);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&first.main, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut damaged = std::fs::read(&first.main).unwrap();
        damaged[0] ^= 0xff;
        std::fs::write(&first.main, damaged).unwrap();
        assert!(load_materialized_application(&store, first.bundle_root).is_err());
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
