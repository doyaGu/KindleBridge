use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use kindlebridge_schema::{
    methods, SyncEntryKind, SyncListParams, SyncListResult, SyncMkdirParams, SyncMkdirResult,
    SyncPullParams, SyncPullResult, SyncPushParams, SyncPushResult, SyncStatus, SyncStatusParams,
    TransferState, DEFAULT_SYNC_BLOCK_SIZE, MAX_SYNC_BLOCK_SIZE,
};
use serde_json::json;

use super::{
    call_typed, normalize_host_path, pretty_json, CliError, RpcCaller, DEVICE_SYNC_ROOT,
    MAX_SYNC_TREE_ENTRIES,
};

#[derive(Debug, Args)]
pub struct SyncArgs {
    #[command(subcommand)]
    pub command: SyncCommand,
}

#[derive(Debug, Subcommand)]
pub enum SyncCommand {
    /// Push a local file or directory tree to a device.
    Push {
        /// Stable device serial from `device list`.
        serial: String,
        /// Local source file or directory.
        local_path: String,
        /// Relative device path, or an absolute path below `/mnt/us/kindlebridge-data`.
        remote_path: String,
        /// Transfer frame size; the 256 KiB default balances USB throughput and interactive latency. Values below 64 KiB are for diagnostics.
        #[arg(long, default_value_t = DEFAULT_SYNC_BLOCK_SIZE as usize)]
        block_size: usize,
        /// Continue a previously interrupted transfer by its transfer ID.
        #[arg(long)]
        resume: Option<String>,
    },
    /// Pull a device file, or a directory tree with --recursive, to the host.
    Pull {
        /// Stable device serial from `device list`.
        serial: String,
        /// Relative device path, or an absolute path below `/mnt/us/kindlebridge-data`.
        remote_path: String,
        /// Local destination file, which must not already exist.
        local_path: String,
        /// Transfer frame size; the 256 KiB default balances USB throughput and interactive latency. Values below 64 KiB are for diagnostics.
        #[arg(long, default_value_t = DEFAULT_SYNC_BLOCK_SIZE)]
        block_size: u32,
        /// Continue a previously interrupted transfer by its transfer ID.
        #[arg(long)]
        resume: Option<String>,
        /// Pull a directory tree instead of one file.
        #[arg(short = 'r', long, conflicts_with = "resume")]
        recursive: bool,
    },
    /// Inspect a resumable transfer.
    Status { serial: String, transfer_id: String },
}

pub(super) fn execute<C: RpcCaller>(
    caller: &mut C,
    command: &SyncCommand,
    json_output: bool,
) -> Result<String, CliError> {
    match command {
        SyncCommand::Push {
            serial,
            local_path,
            remote_path,
            block_size,
            resume,
        } => {
            validate_block_size(*block_size)?;
            let block_size = u32::try_from(*block_size).map_err(|_| CliError::InvalidBlockSize)?;
            let local_path = normalize_host_path(local_path)?;
            let remote_path = normalize_remote_path(remote_path)?;
            match fs::symlink_metadata(&local_path) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(CliError::LocalTree(
                        "source must not be a symbolic link".to_owned(),
                    ));
                }
                Ok(metadata) if metadata.is_dir() => {
                    if resume.is_some() {
                        return Err(CliError::DirectoryResumeUnsupported);
                    }
                    return sync_push_directory(
                        caller,
                        serial,
                        Path::new(&local_path),
                        &remote_path,
                        block_size,
                        json_output,
                    );
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(CliError::LocalTree(error.to_string())),
            }
            let started = Instant::now();
            let (value, result): (_, SyncPushResult) = call_typed(
                caller,
                methods::SYNC_PUSH,
                &SyncPushParams {
                    serial: serial.clone(),
                    local_path: local_path.clone(),
                    remote_path: remote_path.clone(),
                    transfer_id: resume.clone(),
                    block_size,
                },
                "sync push",
            )?;
            if json_output {
                pretty_json(&value)
            } else {
                Ok(format_transfer_summary(
                    "pushed",
                    result.accepted_offset,
                    "to",
                    &remote_path,
                    &result.transfer_id,
                    started.elapsed(),
                    resume.is_some(),
                ))
            }
        }
        SyncCommand::Pull {
            serial,
            remote_path,
            local_path,
            block_size,
            resume,
            recursive,
        } => {
            validate_block_size(usize::try_from(*block_size).unwrap_or(usize::MAX))?;
            let remote_path = normalize_remote_path(remote_path)?;
            let local_path = normalize_host_path(local_path)?;
            if *recursive {
                return sync_pull_directory(
                    caller,
                    serial,
                    &remote_path,
                    Path::new(&local_path),
                    *block_size,
                    json_output,
                );
            }
            let started = Instant::now();
            let (value, result): (_, SyncPullResult) = call_typed(
                caller,
                methods::SYNC_PULL,
                &SyncPullParams {
                    serial: serial.clone(),
                    remote_path: remote_path.clone(),
                    local_path: local_path.clone(),
                    transfer_id: resume.clone(),
                    block_size: *block_size,
                },
                "sync pull",
            )?;
            if json_output {
                pretty_json(&value)
            } else {
                Ok(format_transfer_summary(
                    "pulled",
                    result.received_size,
                    "to",
                    &local_path,
                    &result.transfer_id,
                    started.elapsed(),
                    resume.is_some(),
                ))
            }
        }
        SyncCommand::Status {
            serial,
            transfer_id,
        } => {
            let (value, status): (_, SyncStatus) = call_typed(
                caller,
                methods::SYNC_STATUS,
                &SyncStatusParams {
                    serial: serial.clone(),
                    transfer_id: transfer_id.clone(),
                },
                "sync status",
            )?;
            if json_output {
                pretty_json(&value)
            } else {
                Ok(format!(
                    "{} {:?} {}/{} {:?}",
                    status.transfer_id,
                    status.direction,
                    status.next_offset,
                    status.total_size,
                    status.state
                )
                .to_lowercase())
            }
        }
    }
}

fn sync_push_directory<C: RpcCaller>(
    caller: &mut C,
    serial: &str,
    local_root: &Path,
    remote_root: &str,
    block_size: u32,
    json_output: bool,
) -> Result<String, CliError> {
    let started = Instant::now();
    let tree = collect_local_tree(local_root)?;
    let mut created_directories = 0_u64;
    for relative in std::iter::once("").chain(tree.directories.iter().map(String::as_str)) {
        let remote_path = join_remote_path(remote_root, relative);
        let (_, result): (_, SyncMkdirResult) = call_typed(
            caller,
            methods::SYNC_MKDIR,
            &SyncMkdirParams {
                serial: serial.to_owned(),
                remote_path,
            },
            "sync mkdir",
        )?;
        created_directories += u64::from(result.created);
    }

    let mut bytes = 0_u64;
    let mut transfers = Vec::with_capacity(tree.files.len());
    for (relative, local_path) in &tree.files {
        let remote_path = join_remote_path(remote_root, relative);
        let (_, result): (_, SyncPushResult) = call_typed(
            caller,
            methods::SYNC_PUSH,
            &SyncPushParams {
                serial: serial.to_owned(),
                local_path: local_path.to_string_lossy().into_owned(),
                remote_path,
                transfer_id: None,
                block_size,
            },
            "sync directory push",
        )?;
        bytes = bytes.saturating_add(result.accepted_offset);
        transfers.push(result.transfer_id);
    }
    format_tree_summary(
        "push",
        local_root.to_string_lossy().as_ref(),
        remote_root,
        tree.files.len(),
        tree.directories.len() + 1,
        created_directories,
        bytes,
        transfers,
        started.elapsed(),
        json_output,
    )
}

fn sync_pull_directory<C: RpcCaller>(
    caller: &mut C,
    serial: &str,
    remote_root: &str,
    local_root: &Path,
    block_size: u32,
    json_output: bool,
) -> Result<String, CliError> {
    if local_root.exists() {
        return Err(CliError::LocalTree(format!(
            "destination already exists: {}",
            local_root.display()
        )));
    }
    let parent = local_root
        .parent()
        .ok_or_else(|| CliError::LocalTree("destination has no parent".to_owned()))?;
    fs::create_dir_all(parent).map_err(|error| CliError::LocalTree(error.to_string()))?;
    fs::create_dir(local_root).map_err(|error| CliError::LocalTree(error.to_string()))?;

    let started = Instant::now();
    let result = (|| {
        let mut pending = vec![(remote_root.to_owned(), PathBuf::new())];
        let mut files = 0_usize;
        let mut directories = 1_usize;
        let mut bytes = 0_u64;
        let mut transfers = Vec::new();
        while let Some((remote_directory, relative_directory)) = pending.pop() {
            let mut cursor = None;
            loop {
                let (_, page): (_, SyncListResult) = call_typed(
                    caller,
                    methods::SYNC_LIST,
                    &SyncListParams {
                        serial: serial.to_owned(),
                        remote_path: remote_directory.clone(),
                        cursor: cursor.clone(),
                        limit: 256,
                    },
                    "sync directory list",
                )?;
                for entry in page.entries {
                    let remote_path = join_remote_path(&remote_directory, &entry.name);
                    let relative_path = relative_directory.join(&entry.name);
                    let local_path = local_root.join(&relative_path);
                    match entry.kind {
                        SyncEntryKind::Directory => {
                            fs::create_dir(&local_path)
                                .map_err(|error| CliError::LocalTree(error.to_string()))?;
                            directories += 1;
                            pending.push((remote_path, relative_path));
                        }
                        SyncEntryKind::File => {
                            let (_, pulled): (_, SyncPullResult) = call_typed(
                                caller,
                                methods::SYNC_PULL,
                                &SyncPullParams {
                                    serial: serial.to_owned(),
                                    remote_path,
                                    local_path: local_path.to_string_lossy().into_owned(),
                                    transfer_id: None,
                                    block_size,
                                },
                                "sync directory pull",
                            )?;
                            if pulled.state != TransferState::Complete
                                || pulled.total_size != entry.size
                                || pulled.received_size != entry.size
                            {
                                return Err(CliError::InvalidResult {
                                    kind: "sync directory pull size",
                                });
                            }
                            files += 1;
                            bytes = bytes.saturating_add(pulled.received_size);
                            transfers.push(pulled.transfer_id);
                        }
                    }
                }
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
        }
        format_tree_summary(
            "pull",
            remote_root,
            local_root.to_string_lossy().as_ref(),
            files,
            directories,
            u64::try_from(directories).unwrap_or(u64::MAX),
            bytes,
            transfers,
            started.elapsed(),
            json_output,
        )
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(local_root);
    }
    result
}

struct LocalTree {
    directories: Vec<String>,
    files: Vec<(String, PathBuf)>,
}

fn collect_local_tree(root: &Path) -> Result<LocalTree, CliError> {
    let mut directories = Vec::new();
    let mut files = Vec::new();
    let mut pending = vec![(root.to_owned(), String::new())];
    while let Some((directory, relative)) = pending.pop() {
        let mut entries = fs::read_dir(&directory)
            .map_err(|error| CliError::LocalTree(error.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| CliError::LocalTree(error.to_string()))?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries.into_iter().rev() {
            if directories.len().saturating_add(files.len()) >= MAX_SYNC_TREE_ENTRIES {
                return Err(CliError::LocalTree(
                    "directory tree contains more than 100000 entries".to_owned(),
                ));
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| CliError::LocalTree("path is not valid Unicode".to_owned()))?;
            let child_relative = if relative.is_empty() {
                name
            } else {
                format!("{relative}/{name}")
            };
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|error| CliError::LocalTree(error.to_string()))?;
            if metadata.file_type().is_symlink() {
                return Err(CliError::LocalTree(format!(
                    "symbolic links are not supported: {}",
                    entry.path().display()
                )));
            }
            if metadata.is_dir() {
                directories.push(child_relative.clone());
                pending.push((entry.path(), child_relative));
            } else if metadata.is_file() {
                files.push((child_relative, entry.path()));
            } else {
                return Err(CliError::LocalTree(format!(
                    "special files are not supported: {}",
                    entry.path().display()
                )));
            }
        }
    }
    directories.sort();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(LocalTree { directories, files })
}

fn join_remote_path(root: &str, relative: &str) -> String {
    if relative.is_empty() {
        root.to_owned()
    } else {
        format!("{root}/{relative}")
    }
}

#[allow(clippy::too_many_arguments)]
fn format_tree_summary(
    direction: &str,
    source: &str,
    destination: &str,
    files: usize,
    directories: usize,
    created_directories: u64,
    bytes: u64,
    transfers: Vec<String>,
    elapsed: Duration,
    json_output: bool,
) -> Result<String, CliError> {
    if json_output {
        pretty_json(&json!({
            "direction": direction,
            "source": source,
            "destination": destination,
            "files": files,
            "directories": directories,
            "created_directories": created_directories,
            "bytes": bytes,
            "transfer_ids": transfers,
        }))
    } else {
        Ok(format!(
            "{direction}ed {files} files in {directories} directories ({bytes} bytes) in {:.2} s",
            elapsed.as_secs_f64()
        ))
    }
}

fn validate_block_size(block_size: usize) -> Result<(), CliError> {
    if (1..=usize::try_from(MAX_SYNC_BLOCK_SIZE).unwrap()).contains(&block_size) {
        Ok(())
    } else {
        Err(CliError::InvalidBlockSize)
    }
}

fn normalize_remote_path(path: &str) -> Result<String, CliError> {
    let path = path.replace('\\', "/");
    if let Some(relative) = path.strip_prefix(&format!("{DEVICE_SYNC_ROOT}/")) {
        if !relative.is_empty() {
            return Ok(relative.to_owned());
        }
    }
    if path.starts_with('/') {
        return Err(CliError::RemotePathOutsideSyncRoot(path));
    }
    Ok(path)
}

fn format_transfer_summary(
    action: &str,
    bytes: u64,
    preposition: &str,
    path: &str,
    transfer_id: &str,
    elapsed: Duration,
    resumed: bool,
) -> String {
    let seconds = elapsed.as_secs_f64();
    let timing = if resumed || seconds <= f64::EPSILON {
        format!("in {seconds:.2} s")
    } else {
        let mib_per_second = bytes as f64 / (1024.0 * 1024.0) / seconds;
        format!("in {seconds:.2} s ({mib_per_second:.2} MiB/s)")
    };
    format!("{action} {bytes} bytes {preposition} {path} {timing} ({transfer_id})")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_summary_reports_elapsed_time_and_throughput() {
        assert_eq!(
            format_transfer_summary(
                "pushed",
                32 * 1024 * 1024,
                "to",
                "apps/payload.bin",
                "push-test",
                Duration::from_secs(2),
                false,
            ),
            "pushed 33554432 bytes to apps/payload.bin in 2.00 s (16.00 MiB/s) (push-test)"
        );
    }

    #[test]
    fn remote_path_accepts_logical_and_sync_root_forms_only() {
        assert_eq!(
            normalize_remote_path("apps\\reader.kbb").unwrap(),
            "apps/reader.kbb"
        );
        assert_eq!(
            normalize_remote_path("/mnt/us/kindlebridge-data/apps/reader.kbb").unwrap(),
            "apps/reader.kbb"
        );
        assert!(matches!(
            normalize_remote_path("/mnt/us/other/reader.kbb"),
            Err(CliError::RemotePathOutsideSyncRoot(_))
        ));
    }
}
