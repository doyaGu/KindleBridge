use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use ed25519_dalek::SigningKey;
use serde::Deserialize;
use walkdir::WalkDir;

use crate::{
    verify_bytes, BuildConfig, BundleBuilder, BundleKind, CompressionPolicy, DataPolicy, Digest,
    Permissions, ProcessPolicy, VerifyOptions,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevelopmentConfig {
    #[serde(default)]
    pub build: Vec<String>,
    pub input: PathBuf,
    pub signing_key: PathBuf,
    #[serde(default)]
    pub watch: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectManifest {
    kind: BundleKind,
    pub id: String,
    pub version: String,
    pub release: u64,
    #[serde(default = "default_channel")]
    channel: String,
    #[serde(default = "default_target")]
    target: String,
    #[serde(default = "default_os")]
    os: String,
    #[serde(default = "default_arch")]
    arch: String,
    #[serde(default = "default_abi")]
    abi: String,
    firmware_min: Option<Vec<u64>>,
    firmware_max_exclusive: Option<Vec<u64>>,
    #[serde(default)]
    required_features: Vec<String>,
    #[serde(default)]
    optional_features: Vec<String>,
    #[serde(default)]
    entrypoints: BTreeMap<String, String>,
    #[serde(default)]
    executable: Vec<String>,
    #[serde(default)]
    permissions: Permissions,
    process: Option<ProcessPolicy>,
    #[serde(default)]
    data: DataPolicy,
    annotations: Option<BTreeMap<String, String>>,
    publisher_name: Option<String>,
    #[serde(default)]
    compression: ManifestCompression,
    pub development: Option<DevelopmentConfig>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ManifestCompression {
    Never,
    #[default]
    ZstdWhenSmaller,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuiltBundle {
    pub id: String,
    pub version: String,
    pub release: u64,
    pub bytes: usize,
    pub bundle_root: Digest,
}

fn default_channel() -> String {
    "dev".to_owned()
}

fn default_target() -> String {
    "kindlehf".to_owned()
}

fn default_os() -> String {
    "kindle-linux".to_owned()
}

fn default_arch() -> String {
    "arm".to_owned()
}

fn default_abi() -> String {
    "gnueabihf".to_owned()
}

pub fn read_project_manifest(path: &Path) -> Result<ProjectManifest, Box<dyn std::error::Error>> {
    Ok(toml::from_str(&fs::read_to_string(path)?)?)
}

pub fn build_project_bundle(
    manifest_path: &Path,
    input: &Path,
    signing_key_path: &Path,
    output: &Path,
    release_override: Option<u64>,
) -> Result<BuiltBundle, Box<dyn std::error::Error>> {
    let manifest = read_project_manifest(manifest_path)?;
    let signing_key = read_signing_key(signing_key_path)?;
    let executable: BTreeSet<String> = manifest
        .executable
        .iter()
        .chain(manifest.entrypoints.values())
        .cloned()
        .collect();
    let release = release_override.unwrap_or(manifest.release);

    let mut config = BuildConfig::new(
        manifest.kind,
        manifest.id.clone(),
        manifest.version.clone(),
        release,
        manifest.target,
    );
    config.channel = manifest.channel;
    config.os = manifest.os;
    config.arch = manifest.arch;
    config.abi = manifest.abi;
    config.firmware_min = manifest.firmware_min;
    config.firmware_max_exclusive = manifest.firmware_max_exclusive;
    config.required_features = manifest.required_features;
    config.optional_features = manifest.optional_features;
    config.entrypoints = manifest.entrypoints;
    config.permissions = manifest.permissions;
    config.process = manifest.process;
    config.data = manifest.data;
    config.annotations = manifest.annotations;
    config.publisher_name = manifest.publisher_name;
    config.compression = match manifest.compression {
        ManifestCompression::Never => CompressionPolicy::Never,
        ManifestCompression::ZstdWhenSmaller => CompressionPolicy::ZstdWhenSmaller,
    };

    let mut builder = BundleBuilder::new(config);
    add_input_tree(&mut builder, input, &executable)?;
    let bytes = builder.build(&signing_key)?;
    let verified = verify_bytes(
        &bytes,
        &VerifyOptions {
            expected_publisher: Some(&signing_key.verifying_key()),
            target: None,
            firmware: None,
        },
    )?;
    write_replace(output, &bytes)?;

    Ok(BuiltBundle {
        id: manifest.id,
        version: manifest.version,
        release,
        bytes: bytes.len(),
        bundle_root: verified.inspection.header.bundle_root,
    })
}

fn add_input_tree(
    builder: &mut BundleBuilder,
    root: &Path,
    executable: &BTreeSet<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !root.is_dir() {
        return Err(format!("input is not a directory: {}", root.display()).into());
    }
    for entry in WalkDir::new(root).follow_links(false).sort_by_file_name() {
        let entry = entry?;
        if entry.path() == root {
            continue;
        }
        let relative = entry.path().strip_prefix(root)?;
        let logical = path_to_logical(relative)?;
        if entry.file_type().is_dir() {
            builder.add_directory(logical)?;
        } else if entry.file_type().is_symlink() {
            builder.add_symlink(logical, path_to_logical(&fs::read_link(entry.path())?)?)?;
        } else if entry.file_type().is_file() {
            let is_executable = executable.contains(&logical);
            builder.add_file(logical, fs::read(entry.path())?, is_executable)?;
        } else {
            return Err(format!("unsupported input entry: {}", entry.path().display()).into());
        }
    }
    Ok(())
}

fn path_to_logical(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let mut logical = String::new();
    for component in path.components() {
        let component = component
            .as_os_str()
            .to_str()
            .ok_or("input path is not valid UTF-8")?;
        if !logical.is_empty() {
            logical.push('/');
        }
        logical.push_str(component);
    }
    Ok(logical)
}

fn read_signing_key(path: &Path) -> Result<SigningKey, Box<dyn std::error::Error>> {
    let bytes: [u8; 32] = fs::read(path)?
        .try_into()
        .map_err(|_| "signing key must contain exactly 32 raw bytes")?;
    Ok(SigningKey::from_bytes(&bytes))
}

fn write_replace(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("kbb.tmp");
    fs::write(&temporary, bytes)?;
    if path.exists() {
        fs::remove_file(path)?;
    }
    fs::rename(temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inspect_bytes;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn development_build_is_optional_for_prebuilt_input_trees() {
        let manifest: ProjectManifest = toml::from_str(
            r#"
kind = "application"
id = "org.example.script"
version = "0.1.0"
release = 1

[development]
input = "root"
signing_key = "dev.key"
"#,
        )
        .unwrap();
        assert!(manifest.development.unwrap().build.is_empty());
    }

    #[test]
    fn development_manifest_builds_with_an_overridden_release_and_replaces_output() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "kindlebridge-project-{}-{unique}",
            std::process::id()
        ));
        let input = root.join("root");
        fs::create_dir_all(input.join("bin")).unwrap();
        fs::write(input.join("bin/app"), b"#!/bin/sh\nexit 0\n").unwrap();
        fs::write(root.join("dev.key"), [0x42; 32]).unwrap();
        fs::write(
            root.join("kindlebridge.toml"),
            r#"
kind = "application"
id = "org.example.watch"
version = "0.1.0"
release = 1
entrypoints = { main = "bin/app" }

[development]
build = ["true"]
input = "root"
signing_key = "dev.key"
watch = ["src"]
"#,
        )
        .unwrap();
        let output = root.join(".kindlebridge/run.kbb");
        let built = build_project_bundle(
            &root.join("kindlebridge.toml"),
            &input,
            &root.join("dev.key"),
            &output,
            Some(42),
        )
        .unwrap();
        assert_eq!(built.id, "org.example.watch");
        assert_eq!(built.release, 42);
        let inspection = inspect_bytes(&fs::read(&output).unwrap()).unwrap();
        assert_eq!(inspection.envelope.release, 42);

        let rebuilt = build_project_bundle(
            &root.join("kindlebridge.toml"),
            &input,
            &root.join("dev.key"),
            &output,
            Some(43),
        )
        .unwrap();
        assert_eq!(rebuilt.release, 43);
        let inspection = inspect_bytes(&fs::read(&output).unwrap()).unwrap();
        assert_eq!(inspection.envelope.release, 43);
        fs::remove_dir_all(root).unwrap();
    }
}
