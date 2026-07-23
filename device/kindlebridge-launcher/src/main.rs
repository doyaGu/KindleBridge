use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use kindlebridge_launcher::{
    active_slot, rollback_daemon, stage_daemon, FilesystemDisableFlag, Launcher, Slot,
    SystemChildRunner, SystemClock,
};

#[derive(Debug, Parser)]
#[command(about = "KindleBridge A/B daemon launcher and updater")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Supervise the active daemon slot.
    #[command(trailing_var_arg = true)]
    Run {
        #[arg(long)]
        root: PathBuf,
        #[arg(required = true, allow_hyphen_values = true)]
        child_arguments: Vec<OsString>,
    },
    /// Verify and stage a daemon in the inactive slot.
    Stage {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        blake3: String,
    },
    /// Select the staged slot while the USB daemon is stopped.
    SelectStaged {
        #[arg(long)]
        root: PathBuf,
    },
    /// Restore the last confirmed daemon slot while the USB daemon is stopped.
    Rollback {
        #[arg(long)]
        root: PathBuf,
    },
    /// Print the current slot.
    Status {
        #[arg(long)]
        root: PathBuf,
    },
}

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("kindlebridge-launcher: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: Args) -> Result<(), String> {
    match arguments.command {
        Command::Run {
            root,
            child_arguments,
        } => {
            let mut launcher = Launcher::open(
                root,
                SystemChildRunner::with_arguments(child_arguments),
                SystemClock,
                FilesystemDisableFlag::default(),
            )
            .map_err(|error| error.to_string())?;
            launcher.run().map_err(|error| error.to_string())?;
        }
        Command::Stage {
            root,
            source,
            blake3,
        } => {
            let staged = stage_daemon(root, source, &blake3).map_err(|error| error.to_string())?;
            println!("{}\t{}\t{}", staged.slot, staged.digest, staged.size);
        }
        Command::SelectStaged { root } => {
            let slot = read_staged_slot(&root)?;
            request_slot(root.clone(), slot)?;
            std::fs::remove_file(root.join("next"))
                .map_err(|error| format!("could not clear staged slot: {error}"))?;
            println!("selected staged slot {slot}");
        }
        Command::Rollback { root } => {
            let slot = rollback_daemon(root).map_err(|error| error.to_string())?;
            println!("restored daemon slot {slot}");
        }
        Command::Status { root } => {
            println!("{}", active_slot(root).map_err(|error| error.to_string())?);
        }
    }
    Ok(())
}

fn request_slot(root: PathBuf, slot: Slot) -> Result<(), String> {
    let mut launcher = Launcher::open(
        root,
        SystemChildRunner::default(),
        SystemClock,
        FilesystemDisableFlag::default(),
    )
    .map_err(|error| error.to_string())?;
    launcher
        .request_slot(slot)
        .map_err(|error| error.to_string())
}

fn read_staged_slot(root: &std::path::Path) -> Result<Slot, String> {
    if root.join("run/daemon.pid").exists() {
        return Err("refusing to select a staged slot while a daemon PID exists".to_owned());
    }
    let value = std::fs::read_to_string(root.join("next"))
        .map_err(|error| format!("could not read staged slot: {error}"))?;
    match value.as_str() {
        "A\n" => Ok(Slot::A),
        "B\n" => Ok(Slot::B),
        _ => Err("staged slot pointer is not canonical".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_requires_explicit_child_arguments() {
        assert!(
            Args::try_parse_from(["kindlebridge-launcher", "run", "--root", "/tmp/runtime"])
                .is_err()
        );
    }

    #[test]
    fn staged_selection_refuses_a_live_daemon_marker() {
        let root = std::env::temp_dir().join(format!(
            "kindlebridge-select-staged-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("run")).unwrap();
        std::fs::write(root.join("next"), b"B\n").unwrap();
        std::fs::write(root.join("run/daemon.pid"), b"123\n").unwrap();
        assert!(read_staged_slot(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn staged_selection_rejects_a_noncanonical_pointer() {
        let root = std::env::temp_dir().join(format!(
            "kindlebridge-select-staged-canonical-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("next"), b"B\n\n").unwrap();
        assert!(read_staged_slot(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
