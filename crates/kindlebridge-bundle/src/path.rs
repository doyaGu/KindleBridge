#[cfg(feature = "verify")]
use std::collections::{BTreeMap, BTreeSet};

use unicode_normalization::UnicodeNormalization;

use crate::error::{Error, ErrorCode, Result};
#[cfg(feature = "verify")]
use crate::model::{FileEntry, FileType};

pub fn validate_bundle_path(path: &str) -> Result<()> {
    validate_nfc_text(path, 1, 1024, "path")?;
    if path.starts_with('/') || path.ends_with('/') || path.contains('\\') {
        return path_error("path must be relative, slash-separated, and have no trailing slash");
    }
    for component in path.split('/') {
        validate_component(component, false)?;
    }
    Ok(())
}

pub fn validate_symlink_target(target: &str) -> Result<()> {
    validate_nfc_text(target, 1, 1024, "symlink target")?;
    if target.starts_with('/') || target.ends_with('/') || target.contains('\\') {
        return path_error("symlink target must be relative and slash-separated");
    }
    for component in target.split('/') {
        validate_component(component, true)?;
    }
    Ok(())
}

pub(crate) fn validate_logical_id(id: &str) -> Result<()> {
    if !(3..=255).contains(&id.len()) || !id.is_ascii() || !id.contains('.') {
        return Err(Error::new(
            ErrorCode::Schema,
            "logical ID must be 3..255 ASCII bytes and contain a dot",
        ));
    }
    if id.starts_with("com.amazon.")
        || id.starts_with("com.lab126.")
        || id.starts_with("org.kindlebridge.system.")
    {
        return Err(Error::new(
            ErrorCode::Publisher,
            "reserved logical ID requires a built-in trust root",
        ));
    }
    for segment in id.split('.') {
        let bytes = segment.as_bytes();
        if bytes.is_empty()
            || !bytes[0].is_ascii_lowercase() && !bytes[0].is_ascii_digit()
            || !bytes[bytes.len() - 1].is_ascii_lowercase()
                && !bytes[bytes.len() - 1].is_ascii_digit()
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
        {
            return Err(Error::new(
                ErrorCode::Schema,
                "logical ID is not lowercase reverse-DNS syntax",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_channel(channel: &str) -> Result<()> {
    let bytes = channel.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 32
        || !(bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        || !bytes.iter().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(*byte, b'.' | b'_' | b'-')
        })
    {
        return Err(Error::new(ErrorCode::Schema, "invalid channel"));
    }
    Ok(())
}

#[cfg(feature = "verify")]
pub(crate) fn validate_tree_paths(entries: &[FileEntry]) -> Result<()> {
    let mut previous: Option<&[u8]> = None;
    let mut folded = BTreeSet::new();
    let mut types = BTreeMap::new();

    for entry in entries {
        validate_bundle_path(&entry.path)?;
        let bytes = entry.path.as_bytes();
        if previous.is_some_and(|value| value >= bytes) {
            return path_error("tree entries are not in strict UTF-8 byte order");
        }
        previous = Some(bytes);

        let ascii_folded = entry.path.to_ascii_lowercase();
        if !folded.insert(ascii_folded) {
            return path_error("tree contains an ASCII case-fold collision");
        }

        if let Some((parent, _)) = entry.path.rsplit_once('/') {
            match types.get(parent) {
                Some(FileType::Directory) => {}
                Some(_) => return path_error("an entry has a non-directory parent"),
                None => return path_error("an entry's parent directory is not declared"),
            }
        }
        types.insert(entry.path.as_str(), entry.file_type);
    }
    Ok(())
}

#[cfg(feature = "verify")]
pub(crate) fn validate_symlinks(entries: &[FileEntry]) -> Result<()> {
    let by_path: BTreeMap<&str, &FileEntry> = entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    for entry in entries {
        if entry.file_type != FileType::SymlinkRelative {
            continue;
        }
        let mut current = entry;
        let mut seen = BTreeSet::new();
        for _ in 0..=16 {
            if current.file_type != FileType::SymlinkRelative {
                break;
            }
            if !seen.insert(current.path.as_str()) {
                return path_error("symlink loop");
            }
            if seen.len() > 16 {
                return path_error("symlink chain exceeds 16 hops");
            }
            let target = current
                .target
                .as_deref()
                .ok_or_else(|| Error::new(ErrorCode::Tree, "symlink entry has no target"))?;
            validate_symlink_target(target)?;
            let resolved = resolve_target(&current.path, target)?;
            current = *by_path
                .get(resolved.as_str())
                .ok_or_else(|| Error::new(ErrorCode::Path, "dangling symlink"))?;
        }
    }
    Ok(())
}

#[cfg(feature = "verify")]
fn resolve_target(link_path: &str, target: &str) -> Result<String> {
    let mut parts: Vec<&str> = link_path.split('/').collect();
    parts.pop();
    for component in target.split('/') {
        match component {
            "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return path_error("symlink target escapes the bundle root");
                }
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        return path_error("symlink may not resolve to the implicit root");
    }
    Ok(parts.join("/"))
}

fn validate_component(component: &str, allow_dot: bool) -> Result<()> {
    if component.is_empty()
        || !allow_dot && matches!(component, "." | "..")
        || component.len() > 255
        || component.ends_with(' ')
        || component.ends_with('.') && !allow_dot
        || component.chars().any(is_control)
    {
        return path_error("invalid path component");
    }
    if allow_dot && matches!(component, "." | "..") {
        return Ok(());
    }
    let basename = component.split('.').next().unwrap_or_default();
    let upper = basename.to_ascii_uppercase();
    let numbered_device = upper.len() == 4
        && (upper.starts_with("COM") || upper.starts_with("LPT"))
        && matches!(upper.as_bytes()[3], b'1'..=b'9');
    if matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL") || numbered_device {
        return path_error("Windows device basename is forbidden");
    }
    Ok(())
}

fn validate_nfc_text(value: &str, min: usize, max: usize, label: &str) -> Result<()> {
    if !(min..=max).contains(&value.len()) {
        return Err(Error::new(
            ErrorCode::Path,
            format!("{label} length is outside {min}..={max} bytes"),
        ));
    }
    if value.nfc().ne(value.chars()) {
        return Err(Error::new(
            ErrorCode::Path,
            format!("{label} is not NFC-normalized"),
        ));
    }
    Ok(())
}

fn is_control(character: char) -> bool {
    matches!(character as u32, 0x00..=0x1f | 0x7f..=0x9f)
}

fn path_error<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::new(ErrorCode::Path, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_paths() {
        for path in ["/etc/passwd", "a/../b", "a//b", "CON.txt", "a\\b", "a/."] {
            assert!(validate_bundle_path(path).is_err(), "accepted {path}");
        }
        assert!(validate_bundle_path("bin/reader").is_ok());
        assert!(validate_bundle_path("资源/字体.bin").is_ok());
    }

    #[test]
    fn rejects_nfd() {
        assert!(validate_bundle_path("cafe\u{301}").is_err());
        assert!(validate_bundle_path("café").is_ok());
    }
}
