use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use ed25519_dalek::{SigningKey, VerifyingKey};
use kindlebridge_bundle::{
    inspect, verify, BuildConfig, BundleBuilder, BundleKind, CompressionPolicy, DataPolicy,
    Permissions, ProcessPolicy, VerifyOptions,
};
use rand_core::OsRng;
use serde::Deserialize;
use serde_json::json;
use walkdir::WalkDir;

#[derive(Debug, Parser)]
#[command(
    name = "kindlebridge-bundle",
    about = "Build and verify KindleBridge Bundles"
)]
struct Cli {
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
    Build {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        signing_key: PathBuf,
        #[arg(short, long)]
        output: PathBuf,
    },
    Inspect {
        bundle: PathBuf,
    },
    Verify {
        bundle: PathBuf,
        #[arg(long, default_value = "kindlehf")]
        target: String,
        #[arg(long)]
        publisher: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum KeyCommand {
    Init {
        #[arg(short, long)]
        output: PathBuf,
        #[arg(long, value_enum, default_value_t = KeyPurpose::Development)]
        purpose: KeyPurpose,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum KeyPurpose {
    Development,
    Release,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    kind: BundleKind,
    id: String,
    version: String,
    release: u64,
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
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ManifestCompression {
    Never,
    #[default]
    ZstdWhenSmaller,
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

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("kindlebridge-bundle: {error}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    match cli.command {
        Command::Key { command } => match command {
            KeyCommand::Init { output, purpose } => init_key(&output, purpose, cli.json),
        },
        Command::Build {
            manifest,
            input,
            signing_key,
            output,
        } => build_bundle(&manifest, &input, &signing_key, &output, cli.json),
        Command::Inspect { bundle } => inspect_bundle(&bundle, cli.json),
        Command::Verify {
            bundle,
            target,
            publisher,
        } => verify_bundle(&bundle, &target, publisher.as_deref(), cli.json),
    }
}

fn init_key(output: &Path, purpose: KeyPurpose, json_output: bool) -> Result<(), Box<dyn Error>> {
    refuse_overwrite(output)?;
    let public_path = output.with_extension(format!(
        "{}pub",
        output
            .extension()
            .and_then(|extension| extension.to_str())
            .map_or(String::new(), |extension| format!("{extension}."))
    ));
    refuse_overwrite(&public_path)?;

    let signing_key = SigningKey::generate(&mut OsRng);
    fs::write(output, signing_key.to_bytes())?;
    fs::write(&public_path, signing_key.verifying_key().to_bytes())?;

    let purpose = match purpose {
        KeyPurpose::Development => "development",
        KeyPurpose::Release => "release",
    };
    if json_output {
        println!(
            "{}",
            json!({
                "purpose": purpose,
                "private_key": output,
                "public_key": public_path,
            })
        );
    } else {
        println!("created {purpose} key: {}", output.display());
        println!("public key: {}", public_path.display());
    }
    Ok(())
}

fn build_bundle(
    manifest_path: &Path,
    input: &Path,
    signing_key_path: &Path,
    output: &Path,
    json_output: bool,
) -> Result<(), Box<dyn Error>> {
    refuse_overwrite(output)?;
    let manifest: Manifest = toml::from_str(&fs::read_to_string(manifest_path)?)?;
    let signing_key = read_signing_key(signing_key_path)?;
    let executable: BTreeSet<String> = manifest
        .executable
        .iter()
        .chain(manifest.entrypoints.values())
        .cloned()
        .collect();

    let mut config = BuildConfig::new(
        manifest.kind,
        manifest.id,
        manifest.version,
        manifest.release,
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
    fs::write(output, &bytes)?;
    let verified = kindlebridge_bundle::verify_bytes(
        &bytes,
        &VerifyOptions {
            expected_publisher: Some(&signing_key.verifying_key()),
            target: None,
            firmware: None,
        },
    )?;

    if json_output {
        println!(
            "{}",
            json!({
                "output": output,
                "bytes": bytes.len(),
                "id": verified.inspection.envelope.id,
                "version": verified.inspection.envelope.version,
                "release": verified.inspection.envelope.release,
                "bundle_root": format!("{:?}", verified.inspection.header.bundle_root),
            })
        );
    } else {
        println!(
            "built {} ({} bytes, root {:?})",
            output.display(),
            bytes.len(),
            verified.inspection.header.bundle_root
        );
    }
    Ok(())
}

fn inspect_bundle(path: &Path, json_output: bool) -> Result<(), Box<dyn Error>> {
    let mut file = fs::File::open(path)?;
    let inspection = inspect(&mut file)?;
    let envelope = &inspection.envelope;
    let value = json!({
        "profile": envelope.profile,
        "kind": envelope.kind.as_str(),
        "id": envelope.id,
        "version": envelope.version,
        "release": envelope.release,
        "channel": envelope.channel,
        "target": envelope.variants[0].target,
        "files": envelope.trees[0].entries.len(),
        "blocks": envelope.blocks.len(),
        "file_length": inspection.file_length,
        "bundle_root": format!("{:?}", inspection.header.bundle_root),
        "publisher_key_id": format!("{:?}", envelope.publisher.key_id),
    });
    if json_output {
        println!("{value}");
    } else {
        println!(
            "{} {} {} release={} target={} files={} blocks={}",
            envelope.kind.as_str(),
            envelope.id,
            envelope.version,
            envelope.release,
            envelope.variants[0].target,
            envelope.trees[0].entries.len(),
            envelope.blocks.len()
        );
        println!("bundle root: {:?}", inspection.header.bundle_root);
        println!("publisher:  {:?}", envelope.publisher.key_id);
    }
    Ok(())
}

fn verify_bundle(
    path: &Path,
    target: &str,
    publisher_path: Option<&Path>,
    json_output: bool,
) -> Result<(), Box<dyn Error>> {
    let expected_publisher = publisher_path.map(read_verifying_key).transpose()?;
    let mut file = fs::File::open(path)?;
    let verified = verify(
        &mut file,
        &VerifyOptions {
            expected_publisher: expected_publisher.as_ref(),
            target: Some(target),
            firmware: None,
        },
    )?;
    if json_output {
        println!(
            "{}",
            json!({
                "valid": true,
                "id": verified.inspection.envelope.id,
                "target": target,
                "bundle_root": format!("{:?}", verified.inspection.header.bundle_root),
            })
        );
    } else {
        println!(
            "verified {} for {} (root {:?})",
            path.display(),
            target,
            verified.inspection.header.bundle_root
        );
    }
    Ok(())
}

fn add_input_tree(
    builder: &mut BundleBuilder,
    root: &Path,
    executable: &BTreeSet<String>,
) -> Result<(), Box<dyn Error>> {
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
            let target = fs::read_link(entry.path())?;
            builder.add_symlink(logical, path_to_logical(&target)?)?;
        } else if entry.file_type().is_file() {
            let is_executable = executable.contains(&logical);
            builder.add_file(logical, fs::read(entry.path())?, is_executable)?;
        } else {
            return Err(format!("unsupported input entry: {}", entry.path().display()).into());
        }
    }
    Ok(())
}

fn path_to_logical(path: &Path) -> Result<String, Box<dyn Error>> {
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

fn read_signing_key(path: &Path) -> Result<SigningKey, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "signing key must contain exactly 32 raw bytes")?;
    Ok(SigningKey::from_bytes(&bytes))
}

fn read_verifying_key(path: &Path) -> Result<VerifyingKey, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "publisher key must contain exactly 32 raw bytes")?;
    Ok(VerifyingKey::from_bytes(&bytes)?)
}

fn refuse_overwrite(path: &Path) -> Result<(), Box<dyn Error>> {
    if path.exists() {
        return Err(format!("refusing to overwrite {}", path.display()).into());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}
