//! Recursive Sync Tree planning and execution.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use kindlebridge_schema::{
    LogicalSyncPath, SyncEntryKind, SyncListParams, SyncListResult, SyncMkdirParams,
    SyncMkdirResult, SyncPullParams, SyncPullResult, SyncPushParams, SyncPushResult, TransferState,
};

use super::{call_method, host_rpc, CliError, RpcCaller, MAX_SYNC_TREE_ENTRIES};

pub(super) struct SyncTreeResult {
    pub files: usize,
    pub directories: usize,
    pub created_directories: u64,
    pub bytes: u64,
    pub transfer_ids: Vec<String>,
}

pub(super) fn push<C: RpcCaller>(
    caller: &mut C,
    serial: &str,
    local_root: &Path,
    remote_root: &LogicalSyncPath,
    block_size: u32,
) -> Result<SyncTreeResult, CliError> {
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
    let mut transfer_ids = Vec::with_capacity(tree.files.len());
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
        transfer_ids.push(result.transfer_id);
    }
    Ok(SyncTreeResult {
        files: tree.files.len(),
        directories: tree.directories.len(),
        created_directories,
        bytes,
        transfer_ids,
    })
}

pub(super) fn pull<C: RpcCaller>(
    caller: &mut C,
    serial: &str,
    remote_root: &LogicalSyncPath,
    local_root: &Path,
    block_size: u32,
) -> Result<SyncTreeResult, CliError> {
    if local_root.exists() {
        return Err(CliError::LocalTree(format!(
            "destination already exists: {}",
            local_root.display()
        )));
    }
    let parent = local_root
        .parent()
        .ok_or_else(|| CliError::LocalTree("destination has no parent".to_owned()))?;

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
        let mut transfer_ids = Vec::with_capacity(manifest.files.len());
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
            transfer_ids.push(pulled.transfer_id);
        }

        let final_manifest = collect_remote_tree(caller, serial, remote_root)?;
        if final_manifest != manifest {
            return Err(CliError::RemoteTreeChanged(remote_root.as_str().to_owned()));
        }

        Ok(SyncTreeResult {
            files: manifest.files.len(),
            directories: manifest.directories.len(),
            created_directories: u64::try_from(manifest.directories.len()).unwrap_or(u64::MAX),
            bytes,
            transfer_ids,
        })
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
            let child_relative = join_relative_path(&relative, &name);
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

#[cfg(test)]
mod tests {
    use super::*;

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
