use std::fs;
use std::path::Path;

use crate::fs_safe::{entry_exists, reject_reparse, SafeRoot};
use crate::manifest::Slot;
use crate::watchdog::read_slot_pointer;
use crate::{Error, ErrorKind, Result};

const CURRENT_FILE: &str = "current";
const NEXT_FILE: &str = "next";
const MAX_DAEMON_SIZE: u64 = 32 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedUpdate {
    pub slot: Slot,
    pub digest: String,
    pub size: u64,
}

pub fn active_slot(root: impl AsRef<Path>) -> Result<Slot> {
    read_slot_pointer(&SafeRoot::open(root.as_ref())?, CURRENT_FILE)
}

pub fn stage_daemon(
    root: impl AsRef<Path>,
    source: impl AsRef<Path>,
    expected_digest: &str,
) -> Result<StagedUpdate> {
    let root = SafeRoot::open(root.as_ref())?;
    let source = source.as_ref();
    if !source.is_absolute() || !entry_exists(source)? {
        return invalid("update source must be an existing absolute path");
    }
    reject_reparse(source)?;
    let metadata = fs::metadata(source)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_DAEMON_SIZE {
        return invalid("update source size is invalid");
    }
    let bytes = fs::read(source)?;
    if u64::try_from(bytes.len()).ok() != Some(metadata.len()) {
        return invalid("update source changed while being read");
    }
    validate_arm_elf(&bytes)?;
    let digest = blake3::hash(&bytes).to_hex().to_string();
    if digest != expected_digest {
        return invalid("update digest does not match");
    }

    let slot = read_slot_pointer(&root, CURRENT_FILE)?.other();
    root.ensure_directory("slots")?;
    root.ensure_directory(&format!("slots/{slot}"))?;
    root.ensure_directory(&format!("slots/{slot}/bin"))?;
    let executable = format!("slots/{slot}/bin/kindlebridged");
    root.atomic_write(&executable, &bytes)?;
    set_executable(&root.resolve(&executable)?)?;
    root.atomic_write(
        &format!("slots/{slot}/slot.manifest"),
        slot_manifest(slot).as_bytes(),
    )?;
    // Staging is deliberately non-disruptive. The USB manager consumes this
    // pointer only while the gadget is offline and no daemon is running.
    root.atomic_write(NEXT_FILE, format!("{slot}\n").as_bytes())?;

    Ok(StagedUpdate {
        slot,
        digest,
        size: metadata.len(),
    })
}

fn validate_arm_elf(bytes: &[u8]) -> Result<()> {
    if bytes.len() < 20
        || &bytes[..4] != b"\x7fELF"
        || bytes[4] != 1
        || bytes[5] != 1
        || u16::from_le_bytes([bytes[18], bytes[19]]) != 40
    {
        return invalid("update is not an ELF32 little-endian ARM binary");
    }
    Ok(())
}

fn slot_manifest(slot: Slot) -> String {
    format!(
        "KINDLEBRIDGE_SLOT_V1\nslot={slot}\nexecutable=bin/kindlebridged\nheartbeat=run/heartbeat\nstartup_timeout_ms=10000\nheartbeat_timeout_ms=1500\nhealthy_after_ms=3000\nmax_crashes=3\nbackoff_initial_ms=100\nbackoff_max_ms=1000\n"
    )
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<()> {
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorKind::InvalidState, message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::SlotManifest;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "kindlebridge-launcher-update-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn arm_elf() -> Vec<u8> {
        let mut bytes = vec![0_u8; 64];
        bytes[..6].copy_from_slice(b"\x7fELF\x01\x01");
        bytes[18..20].copy_from_slice(&40_u16.to_le_bytes());
        bytes
    }

    #[test]
    fn stages_only_the_inactive_slot_after_digest_and_elf_validation() {
        let root = root("stage");
        fs::create_dir_all(root.join("slots/A/bin")).unwrap();
        fs::write(root.join("current"), b"A\n").unwrap();
        let source = root.join("candidate");
        let bytes = arm_elf();
        fs::write(&source, &bytes).unwrap();
        let digest = blake3::hash(&bytes).to_hex().to_string();

        let staged = stage_daemon(&root, &source, &digest).unwrap();
        assert_eq!(staged.slot, Slot::B);
        assert_eq!(fs::read(root.join("current")).unwrap(), b"A\n");
        assert_eq!(fs::read(root.join("next")).unwrap(), b"B\n");
        assert_eq!(
            fs::read(root.join("slots/B/bin/kindlebridged")).unwrap(),
            bytes
        );
        SlotManifest::load(&SafeRoot::open(&root).unwrap(), Slot::B).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_wrong_digest_without_touching_the_inactive_slot() {
        let root = root("digest");
        fs::create_dir_all(root.join("slots/A/bin")).unwrap();
        fs::write(root.join("current"), b"A\n").unwrap();
        let source = root.join("candidate");
        fs::write(&source, arm_elf()).unwrap();

        assert!(stage_daemon(&root, &source, &"0".repeat(64)).is_err());
        assert!(!root.join("slots/B").exists());
        assert!(!root.join("next").exists());
        fs::remove_dir_all(root).unwrap();
    }
}
