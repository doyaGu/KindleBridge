use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Error, ErrorKind, Result};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub(crate) struct SafeRoot {
    canonical: PathBuf,
}

impl SafeRoot {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return error(ErrorKind::InvalidRoot, "launcher root must be absolute");
        }
        reject_reparse(path)?;
        if !fs::metadata(path)?.is_dir() {
            return error(ErrorKind::InvalidRoot, "launcher root is not a directory");
        }
        let canonical = fs::canonicalize(path)?;
        reject_reparse(&canonical)?;
        let root = Self { canonical };
        root.ensure_directory("launcher")?;
        root.ensure_directory("run")?;
        Ok(root)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.canonical
    }

    pub(crate) fn resolve(&self, relative: &str) -> Result<PathBuf> {
        validate_relative_path(relative)?;
        let path = self.canonical.join(relative);
        self.validate_components(&path)?;
        Ok(path)
    }

    pub(crate) fn validate_components(&self, path: &Path) -> Result<()> {
        let relative = path.strip_prefix(&self.canonical).map_err(|_| {
            Error::new(
                ErrorKind::UnsafePath,
                "launcher path escaped the configured root",
            )
        })?;
        let mut current = self.canonical.clone();
        reject_reparse(&current)?;
        for component in relative.components() {
            current.push(component);
            let _ = entry_exists(&current)?;
        }
        Ok(())
    }

    pub(crate) fn ensure_directory(&self, relative: &str) -> Result<PathBuf> {
        validate_relative_path(relative)?;
        let path = self.canonical.join(relative);
        let parent = path
            .parent()
            .ok_or_else(|| Error::new(ErrorKind::UnsafePath, "directory path has no parent"))?;
        self.validate_components(parent)?;
        if entry_exists(&path)? {
            if !fs::metadata(&path)?.is_dir() {
                return error(ErrorKind::UnsafePath, "owned path is not a directory");
            }
            return Ok(path);
        }
        fs::create_dir(&path)?;
        sync_dir(parent)?;
        Ok(path)
    }

    pub(crate) fn read_file(&self, relative: &str, maximum: u64) -> Result<Vec<u8>> {
        let path = self.resolve(relative)?;
        read_regular(&path, maximum)
    }

    pub(crate) fn optional_file(&self, relative: &str, maximum: u64) -> Result<Option<Vec<u8>>> {
        let path = self.resolve(relative)?;
        if !entry_exists(&path)? {
            return Ok(None);
        }
        match read_regular(&path, maximum) {
            Ok(bytes) => Ok(Some(bytes)),
            // Heartbeats are replaced atomically by the child. Some Kindle
            // userstore filesystems expose a brief lookup gap between our
            // existence check and open; absence is already the documented
            // meaning of an optional file, so retry on the next watchdog tick.
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn atomic_write(&self, relative: &str, bytes: &[u8]) -> Result<()> {
        let path = self.resolve(relative)?;
        let parent = path
            .parent()
            .ok_or_else(|| Error::new(ErrorKind::UnsafePath, "atomic write has no parent"))?;
        reject_reparse(parent)?;
        if entry_exists(&path)? && !fs::metadata(&path)?.is_file() {
            return error(
                ErrorKind::UnsafePath,
                "atomic write destination is not a regular file",
            );
        }
        let temporary = unique_temp(parent)?;
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &path)?;
            sync_dir(parent)
        })();
        if result.is_err() && entry_exists(&temporary).unwrap_or(false) {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    pub(crate) fn remove_file(&self, relative: &str) -> Result<()> {
        let path = self.resolve(relative)?;
        if !entry_exists(&path)? {
            return Ok(());
        }
        if !fs::metadata(&path)?.is_file() {
            return error(
                ErrorKind::UnsafePath,
                "refusing to remove a non-regular file",
            );
        }
        fs::remove_file(&path)?;
        sync_dir(path.parent().expect("resolved file has parent"))
    }
}

pub(crate) fn validate_relative_path(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 1024 || value.contains('\\') || value.contains('\0') {
        return error(ErrorKind::UnsafePath, "invalid relative path");
    }
    let path = Path::new(value);
    if path.is_absolute()
        || path.components().any(|component| {
            !matches!(component, Component::Normal(_))
                || component.as_os_str().to_string_lossy().len() > 255
        })
    {
        return error(ErrorKind::UnsafePath, "path traversal is forbidden");
    }
    Ok(())
}

pub(crate) fn entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || platform_is_reparse(&metadata) {
                return error(ErrorKind::UnsafePath, "symlink/reparse point is forbidden");
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn reject_reparse(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || platform_is_reparse(&metadata) {
        return error(ErrorKind::UnsafePath, "symlink/reparse point is forbidden");
    }
    Ok(())
}

fn read_regular(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    reject_reparse(path)?;
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > maximum {
        return error(ErrorKind::UnsafePath, "file type or length is invalid");
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .map_err(|_| Error::new(ErrorKind::UnsafePath, "file is too large"))?,
    );
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() {
        return error(ErrorKind::Io, "file changed while being read");
    }
    Ok(bytes)
}

fn unique_temp(parent: &Path) -> Result<PathBuf> {
    for _ in 0..128 {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".launcher-tmp-{}-{counter:016x}",
            std::process::id()
        ));
        if !entry_exists(&candidate)? {
            return Ok(candidate);
        }
    }
    error(ErrorKind::Io, "cannot allocate temporary filename")
}

#[cfg(windows)]
fn platform_is_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn platform_is_reparse(_metadata: &fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
fn sync_dir(path: impl AsRef<Path>) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: impl AsRef<Path>) -> Result<()> {
    // Kindle's production target is Unix. std has no safe portable directory-fsync API.
    Ok(())
}

fn error<T>(kind: ErrorKind, message: impl Into<String>) -> Result<T> {
    Err(Error::new(kind, message))
}
