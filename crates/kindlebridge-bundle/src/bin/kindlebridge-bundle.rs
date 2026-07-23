use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use ed25519_dalek::{SigningKey, VerifyingKey};
use kindlebridge_bundle::{build_project_bundle, inspect, verify, VerifyOptions};
use rand_core::OsRng;
use serde_json::json;

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
    let built = build_project_bundle(manifest_path, input, signing_key_path, output, None)?;

    if json_output {
        println!(
            "{}",
            json!({
                "output": output,
                "bytes": built.bytes,
                "id": built.id,
                "version": built.version,
                "release": built.release,
                "bundle_root": format!("{:?}", built.bundle_root),
            })
        );
    } else {
        println!(
            "built {} ({} bytes, root {:?})",
            output.display(),
            built.bytes,
            built.bundle_root
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
