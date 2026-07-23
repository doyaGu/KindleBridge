use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use kindlebridge_schema::{
    LogicalSyncPath, SyncEntryKind, SyncListParams, SyncListResult, SyncMkdirParams,
    SyncMkdirResult, SyncPullParams, SyncPullResult, SyncPushParams, SyncPushResult, SyncStatus,
    SyncStatusParams, TransferState, DEFAULT_SYNC_BLOCK_SIZE, MAX_SYNC_BLOCK_SIZE,
};
use serde_json::json;

use super::{
    call_method, host_rpc, normalize_host_path, pretty_json, CliError, RpcCaller, DEVICE_SYNC_ROOT,
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
            let (value, result): (_, SyncPushResult) = call_method::<_, host_rpc::SyncPush>(
                caller,
                &SyncPushParams {
                    serial: serial.clone(),
                    local_path: local_path.clone(),
                    remote_path: remote_path.as_str().to_owned(),
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
                    remote_path.as_str(),
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
            let (value, result): (_, SyncPullResult) = call_method::<_, host_rpc::SyncPull>(
                caller,
                &SyncPullParams {
                    serial: serial.clone(),
                    remote_path: remote_path.as_str().to_owned(),
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
            let (value, status): (_, SyncStatus) = call_method::<_, host_rpc::SyncStatus>(
                caller,
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
    remote_root: &LogicalSyncPath,
    block_size: u32,
    json_output: bool,
) -> Result<String, CliError> {
    let started = Instant::now();
    let tree = prepare_push_tree(remote_root, collect_local_tree(local_root)?)?;
    let mut created_directories = 0_u64;
    for remote_path in &tree.directories {
        let (_, result): (_, SyncMkdirResult) = call_method::<_, host_rpc::SyncMkdir>(
            caller,
            &SyncMkdirParams {
                serial: serial.to_owned(),
                remote_path: remote_path.as_str().to_owned(),
            },
            "sync mkdir",
        )?;
        created_directories += u64::from(result.created);
    }

    let mut bytes = 0_u64;
    let mut transfers = Vec::with_capacity(tree.files.len());
    for (remote_path, local_path) in &tree.files {
        let (_, result): (_, SyncPushResult) = call_method::<_, host_rpc::SyncPush>(
            caller,
            &SyncPushParams {
                serial: serial.to_owned(),
                local_path: local_path.to_string_lossy().into_owned(),
                remote_path: remote_path.as_str().to_owned(),
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
        remote_root.as_str(),
        tree.files.len(),
        tree.directories.len(),
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
    remote_root: &LogicalSyncPath,
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

    let started = Instant::now();
    let manifest = collect_remote_tree(caller, serial, remote_root)?;
    fs::create_dir_all(parent).map_err(|error| CliError::LocalTree(error.to_string()))?;
    fs::create_dir(local_root).map_err(|error| CliError::LocalTree(error.to_string()))?;

    let result = (|| {
        for directory in manifest
            .directories
            .iter()
            .filter(|directory| !directory.relative_path.is_empty())
        {
            fs::create_dir(local_tree_path(local_root, &directory.relative_path))
                .map_err(|error| CliError::LocalTree(error.to_string()))?;
        }

        let mut bytes = 0_u64;
        let mut transfers = Vec::with_capacity(manifest.files.len());
        for file in &manifest.files {
            let local_path = local_tree_path(local_root, &file.relative_path);
            let (_, pulled): (_, SyncPullResult) = call_method::<_, host_rpc::SyncPull>(
                caller,
                &SyncPullParams {
                    serial: serial.to_owned(),
                    remote_path: file.remote_path.as_str().to_owned(),
                    local_path: local_path.to_string_lossy().into_owned(),
                    transfer_id: None,
                    block_size,
                },
                "sync directory pull",
            )?;
            if pulled.state != TransferState::Complete
                || pulled.total_size != file.size
                || pulled.received_size != file.size
            {
                return Err(CliError::InvalidResult {
                    kind: "sync directory pull size",
                });
            }
            bytes = bytes.saturating_add(pulled.received_size);
            transfers.push(pulled.transfer_id);
        }

        let final_manifest = collect_remote_tree(caller, serial, remote_root)?;
        if final_manifest != manifest {
            return Err(CliError::RemoteTreeChanged(remote_root.as_str().to_owned()));
        }

        format_tree_summary(
            "pull",
            remote_root.as_str(),
            local_root.to_string_lossy().as_ref(),
            manifest.files.len(),
            manifest.directories.len(),
            u64::try_from(manifest.directories.len()).unwrap_or(u64::MAX),
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct RemoteTreeManifest {
    directories: Vec<RemoteDirectory>,
    files: Vec<RemoteFile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RemoteDirectory {
    remote_path: LogicalSyncPath,
    relative_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RemoteFile {
    remote_path: LogicalSyncPath,
    relative_path: String,
    size: u64,
}

fn collect_remote_tree<C: RpcCaller>(
    caller: &mut C,
    serial: &str,
    remote_root: &LogicalSyncPath,
) -> Result<RemoteTreeManifest, CliError> {
    let root = RemoteDirectory {
        remote_path: remote_root.clone(),
        relative_path: String::new(),
    };
    let mut folded_paths = BTreeMap::new();
    register_device_path(remote_root, &mut folded_paths)?;
    let mut directories = vec![root.clone()];
    let mut files = Vec::new();
    let mut pending = vec![root];

    while let Some(directory) = pending.pop() {
        let mut cursor = None;
        loop {
            let (_, page): (_, SyncListResult) = call_method::<_, host_rpc::SyncList>(
                caller,
                &SyncListParams {
                    serial: serial.to_owned(),
                    remote_path: directory.remote_path.as_str().to_owned(),
                    cursor: cursor.clone(),
                    limit: 256,
                },
                "sync directory list",
            )?;
            if page.remote_path != directory.remote_path.as_str() {
                return Err(CliError::InvalidResult {
                    kind: "sync directory list path",
                });
            }
            let entry_count = page.entries.len();
            if entry_count > 256 || (page.next_cursor.is_some() && entry_count != 256) {
                return Err(CliError::InvalidResult {
                    kind: "sync directory list page",
                });
            }

            let mut previous_name = cursor.clone();
            let mut last_name = None;
            for entry in page.entries {
                if directories
                    .len()
                    .saturating_sub(1)
                    .saturating_add(files.len())
                    >= MAX_SYNC_TREE_ENTRIES
                {
                    return Err(CliError::RemoteTreeTooLarge(
                        remote_root.as_str().to_owned(),
                    ));
                }

                let name = parse_device_entry_name(&entry.name)?;
                if previous_name
                    .as_deref()
                    .is_some_and(|previous| name.as_str() <= previous)
                {
                    return Err(CliError::InvalidResult {
                        kind: "sync directory list ordering",
                    });
                }
                previous_name = Some(name.as_str().to_owned());
                last_name = previous_name.clone();

                let remote_path = join_device_logical_path(&directory.remote_path, name.as_str())?;
                register_device_path(&remote_path, &mut folded_paths)?;
                let relative_path = join_relative_path(&directory.relative_path, name.as_str());
                match entry.kind {
                    SyncEntryKind::Directory => {
                        if entry.size != 0 {
                            return Err(CliError::InvalidResult {
                                kind: "sync directory entry size",
                            });
                        }
                        let child = RemoteDirectory {
                            remote_path,
                            relative_path,
                        };
                        directories.push(child.clone());
                        pending.push(child);
                    }
                    SyncEntryKind::File => files.push(RemoteFile {
                        remote_path,
                        relative_path,
                        size: entry.size,
                    }),
                }
            }

            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            let next_cursor = parse_device_entry_name(&next_cursor)?;
            if last_name.as_deref() != Some(next_cursor.as_str()) {
                return Err(CliError::InvalidResult {
                    kind: "sync directory list cursor",
                });
            }
            cursor = Some(next_cursor.into_string());
        }
    }

    directories.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(RemoteTreeManifest { directories, files })
}

fn parse_device_entry_name(name: &str) -> Result<LogicalSyncPath, CliError> {
    let path = LogicalSyncPath::parse(name.to_owned()).map_err(|error| {
        CliError::InvalidDeviceSyncPath {
            path: name.to_owned(),
            reason: error.to_string(),
        }
    })?;
    if path.as_str().contains('/') {
        return Err(CliError::InvalidDeviceSyncPath {
            path: name.to_owned(),
            reason: "directory entry names must contain one component".to_owned(),
        });
    }
    Ok(path)
}

fn join_device_logical_path(
    root: &LogicalSyncPath,
    name: &str,
) -> Result<LogicalSyncPath, CliError> {
    let path = format!("{}/{name}", root.as_str());
    LogicalSyncPath::parse(path.clone()).map_err(|error| CliError::InvalidDeviceSyncPath {
        path,
        reason: error.to_string(),
    })
}

fn register_device_path(
    path: &LogicalSyncPath,
    folded_paths: &mut BTreeMap<String, String>,
) -> Result<(), CliError> {
    if let Some(first) = folded_paths.insert(path.ascii_case_fold_key(), path.as_str().to_owned()) {
        return Err(CliError::DevicePathCollision {
            first,
            second: path.as_str().to_owned(),
        });
    }
    Ok(())
}

fn join_relative_path(root: &str, name: &str) -> String {
    if root.is_empty() {
        name.to_owned()
    } else {
        format!("{root}/{name}")
    }
}

fn local_tree_path(root: &Path, relative: &str) -> PathBuf {
    relative
        .split('/')
        .fold(root.to_owned(), |path, component| path.join(component))
}

struct LocalTree {
    directories: Vec<String>,
    files: Vec<(String, PathBuf)>,
}

struct PreparedPushTree {
    directories: Vec<LogicalSyncPath>,
    files: Vec<(LogicalSyncPath, PathBuf)>,
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

fn prepare_push_tree(
    remote_root: &LogicalSyncPath,
    tree: LocalTree,
) -> Result<PreparedPushTree, CliError> {
    let mut folded_paths = BTreeMap::new();
    register_unique_path(remote_root, &mut folded_paths)?;

    let mut directories = Vec::with_capacity(tree.directories.len() + 1);
    directories.push(remote_root.clone());
    for relative in tree.directories {
        let path = join_logical_path(remote_root, &relative)?;
        register_unique_path(&path, &mut folded_paths)?;
        directories.push(path);
    }

    let mut files = Vec::with_capacity(tree.files.len());
    for (relative, local_path) in tree.files {
        let path = join_logical_path(remote_root, &relative)?;
        register_unique_path(&path, &mut folded_paths)?;
        files.push((path, local_path));
    }
    Ok(PreparedPushTree { directories, files })
}

fn register_unique_path(
    path: &LogicalSyncPath,
    folded_paths: &mut BTreeMap<String, String>,
) -> Result<(), CliError> {
    if let Some(first) = folded_paths.insert(path.ascii_case_fold_key(), path.as_str().to_owned()) {
        return Err(CliError::RemotePathCollision {
            first,
            second: path.as_str().to_owned(),
        });
    }
    Ok(())
}

fn join_logical_path(root: &LogicalSyncPath, relative: &str) -> Result<LogicalSyncPath, CliError> {
    if relative.is_empty() {
        return Ok(root.clone());
    }
    let path = format!("{}/{relative}", root.as_str());
    LogicalSyncPath::parse(path.clone()).map_err(|error| CliError::InvalidRemotePath {
        path,
        reason: error.to_string(),
    })
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

fn normalize_remote_path(input: &str) -> Result<LogicalSyncPath, CliError> {
    let path = input.replace('\\', "/");
    let logical = if let Some(relative) = path.strip_prefix(&format!("{DEVICE_SYNC_ROOT}/")) {
        if !relative.is_empty() {
            relative
        } else {
            path.as_str()
        }
    } else {
        path.as_str()
    };
    if logical.starts_with('/') {
        return Err(CliError::InvalidRemotePath {
            path: input.to_owned(),
            reason: format!("absolute paths must be below {DEVICE_SYNC_ROOT}"),
        });
    }
    LogicalSyncPath::parse(logical.to_owned()).map_err(|error| CliError::InvalidRemotePath {
        path: input.to_owned(),
        reason: error.to_string(),
    })
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
            normalize_remote_path("apps\\reader.kbb").unwrap().as_str(),
            "apps/reader.kbb",
        );
        assert_eq!(
            normalize_remote_path("/mnt/us/kindlebridge-data/apps/reader.kbb")
                .unwrap()
                .as_str(),
            "apps/reader.kbb",
        );
        assert!(matches!(
            normalize_remote_path("/mnt/us/other/reader.kbb"),
            Err(CliError::InvalidRemotePath { path, reason })
                if path == "/mnt/us/other/reader.kbb"
                    && reason == "absolute paths must be below /mnt/us/kindlebridge-data"
        ));
        assert!(matches!(
            normalize_remote_path("apps/../reader.kbb"),
            Err(CliError::InvalidRemotePath { path, reason })
                if path == "apps/../reader.kbb"
                    && reason == "path contains an empty, dot, or dot-dot component"
        ));
    }

    #[test]
    fn push_tree_rejects_ascii_case_collisions_during_preflight() {
        let root = LogicalSyncPath::parse("tree").unwrap();
        let error = prepare_push_tree(
            &root,
            LocalTree {
                directories: vec!["Assets".to_owned(), "assets".to_owned()],
                files: Vec::new(),
            },
        )
        .err()
        .unwrap();

        assert!(matches!(
            error,
            CliError::RemotePathCollision { first, second }
                if first == "tree/Assets" && second == "tree/assets"
        ));
    }

    #[test]
    fn push_tree_validates_every_derived_path_during_preflight() {
        let root = LogicalSyncPath::parse("tree").unwrap();
        let relative = [
            "a".repeat(255),
            "b".repeat(255),
            "c".repeat(255),
            "d".repeat(255),
        ]
        .join("/");
        let error = prepare_push_tree(
            &root,
            LocalTree {
                directories: Vec::new(),
                files: vec![(relative.clone(), PathBuf::from("source"))],
            },
        )
        .err()
        .unwrap();

        assert!(matches!(
            error,
            CliError::InvalidRemotePath { path, reason }
                if path == format!("tree/{relative}")
                    && reason == "path exceeds 1024 UTF-8 bytes"
        ));
    }
}
