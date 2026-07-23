use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use kindlebridge_schema::{
    LogicalSyncPath, SyncPullParams, SyncPullResult, SyncPushParams, SyncPushResult, SyncStatus,
    SyncStatusParams, DEFAULT_SYNC_BLOCK_SIZE, MAX_SYNC_BLOCK_SIZE,
};
use serde_json::json;

use super::{
    call_method, host_rpc, normalize_host_path, pretty_json, sync_tree, CliError, RpcCaller,
    DEVICE_SYNC_ROOT,
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
                    let started = Instant::now();
                    let result = sync_tree::push(
                        caller,
                        serial,
                        Path::new(&local_path),
                        &remote_path,
                        block_size,
                    )?;
                    return format_tree_summary(
                        "push",
                        &local_path,
                        remote_path.as_str(),
                        result,
                        started.elapsed(),
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
                let started = Instant::now();
                let result = sync_tree::pull(
                    caller,
                    serial,
                    &remote_path,
                    Path::new(&local_path),
                    *block_size,
                )?;
                return format_tree_summary(
                    "pull",
                    remote_path.as_str(),
                    &local_path,
                    result,
                    started.elapsed(),
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

fn format_tree_summary(
    direction: &str,
    source: &str,
    destination: &str,
    result: sync_tree::SyncTreeResult,
    elapsed: Duration,
    json_output: bool,
) -> Result<String, CliError> {
    let sync_tree::SyncTreeResult {
        files,
        directories,
        created_directories,
        bytes,
        transfer_ids,
    } = result;
    if json_output {
        pretty_json(&json!({
            "direction": direction,
            "source": source,
            "destination": destination,
            "files": files,
            "directories": directories,
            "created_directories": created_directories,
            "bytes": bytes,
            "transfer_ids": transfer_ids,
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
}
