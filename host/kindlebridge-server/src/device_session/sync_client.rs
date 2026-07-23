//! Host ownership of Sync Stream and local-file lifecycle rules.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use kindlebridge_schema::device_protocol::{
    is_valid_transfer_id, SyncReply, SyncRequest, DEFAULT_STREAM_WINDOW, SYNC_SERVICE,
};
use kindlebridge_schema::{
    error_codes, RpcError, SyncProgressPhase, SyncPullParams, SyncPullResult, SyncPushParams,
    SyncPushResult, TransferState, MAX_SYNC_BLOCK_SIZE,
};
use kindlebridge_transport::actor::{Connection, Stream as ActorStream};
use kindlebridge_transport::TrafficClass;
use kindlebridge_wire::{Command, Frame, FLAG_END_STREAM};
use serde::Serialize;

use super::{expect, link_rpc_error, LinkError};
use crate::HostSyncOperation;

#[derive(Clone, Debug)]
pub(super) struct SyncClient {
    connection: Connection,
}

impl SyncClient {
    pub(super) const fn new(connection: Connection) -> Self {
        Self { connection }
    }

    pub(super) fn push_with_operation(
        &self,
        params: SyncPushParams,
        operation: &HostSyncOperation,
    ) -> Result<SyncPushResult, RpcError> {
        validate_host_path(&params.local_path)?;
        validate_block_size(params.block_size)?;
        validate_requested_transfer_id(params.transfer_id.as_deref())?;
        let mut file = File::open(&params.local_path)
            .map_err(|error| host_file_error("read", &params.local_path, &error))?;
        if !file
            .metadata()
            .map_err(|error| host_file_error("stat", &params.local_path, &error))?
            .is_file()
        {
            return Err(RpcError::invalid_params("local_path must name a file"));
        }
        let total_size = file
            .metadata()
            .map_err(|error| host_file_error("stat", &params.local_path, &error))?
            .len();
        operation.phase(SyncProgressPhase::Hashing, 0, total_size);
        let file_hash = hash_file_with_operation(&mut file, total_size, operation)?;
        operation.phase(SyncProgressPhase::Transferring, 0, total_size);
        self.push_open_file(&params, &mut file, total_size, &file_hash, operation)
            .map_err(link_rpc_error)
    }

    pub(super) fn pull_with_operation(
        &self,
        params: SyncPullParams,
        operation: &HostSyncOperation,
    ) -> Result<SyncPullResult, RpcError> {
        validate_host_path(&params.local_path)?;
        validate_block_size(params.block_size)?;
        validate_requested_transfer_id(params.transfer_id.as_deref())?;
        self.pull_stream(&params, operation).map_err(link_rpc_error)
    }

    pub(super) fn push_open_file(
        &self,
        params: &SyncPushParams,
        file: &mut File,
        total_size: u64,
        file_hash: &str,
        operation: &HostSyncOperation,
    ) -> Result<SyncPushResult, LinkError> {
        let mut stream =
            self.connection
                .open(SYNC_SERVICE, DEFAULT_STREAM_WINDOW, TrafficClass::Bulk)?;
        register_cancel(operation, &stream);
        if operation.is_cancelled() {
            return Err(sync_cancelled_error());
        }
        stream.send_data(
            encode(&SyncRequest::Push {
                transfer_id: params.transfer_id.clone(),
                remote_path: params.remote_path.clone(),
                total_size,
                file_hash: file_hash.to_owned(),
                block_size: params.block_size,
            })?,
            false,
        )?;
        let ready: SyncReply = decode(&actor_data(&mut stream)?.payload, "sync reply")?;
        let (transfer_id, offset) = match ready {
            SyncReply::Ready {
                transfer_id,
                offset,
                total_size: remote_size,
                file_hash: remote_hash,
            } if remote_size == total_size && remote_hash == file_hash => (transfer_id, offset),
            SyncReply::Failure { error } => return Err(LinkError::Remote(error)),
            _ => return Err(LinkError::UnexpectedFrame("invalid sync push READY")),
        };
        if !is_valid_transfer_id(&transfer_id)
            || params
                .transfer_id
                .as_ref()
                .is_some_and(|expected| expected != &transfer_id)
            || offset > total_size
        {
            return Err(LinkError::UnexpectedFrame("sync push resume mismatch"));
        }
        operation.transfer_id(transfer_id.clone());
        operation.phase(SyncProgressPhase::Transferring, offset, total_size);
        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = vec![0_u8; params.block_size as usize];
        let mut sent = offset;
        if sent == total_size {
            stream.send_data(Vec::new(), true)?;
            operation.transferred(sent);
        } else {
            loop {
                if operation.is_cancelled() {
                    let _ = stream.reset("sync cancelled");
                    return Err(sync_cancelled_error());
                }
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    return Err(LinkError::UnexpectedFrame(
                        "local file ended before its declared size",
                    ));
                }
                sent = sent
                    .checked_add(count as u64)
                    .ok_or(LinkError::SequenceExhausted)?;
                if sent > total_size {
                    return Err(LinkError::UnexpectedFrame("local file grew during sync"));
                }
                let last = sent == total_size;
                stream.send_data(buffer[..count].to_vec(), last)?;
                operation.transferred(sent);
                if last {
                    break;
                }
            }
        }
        let completion: SyncReply = decode(&actor_data(&mut stream)?.payload, "sync completion")?;
        let result = match completion {
            SyncReply::Complete {
                transfer_id: completed_id,
                next_offset,
                total_size: completed_size,
            } if completed_id == transfer_id
                && next_offset == total_size
                && completed_size == total_size =>
            {
                SyncPushResult {
                    transfer_id,
                    accepted_offset: next_offset,
                    state: TransferState::Complete,
                }
            }
            SyncReply::Failure { error } => return Err(LinkError::Remote(error)),
            _ => return Err(LinkError::UnexpectedFrame("invalid sync push completion")),
        };
        actor_close(&mut stream)?;
        Ok(result)
    }

    fn pull_stream(
        &self,
        params: &SyncPullParams,
        operation: &HostSyncOperation,
    ) -> Result<SyncPullResult, LinkError> {
        let started = Instant::now();
        let mut stream =
            self.connection
                .open(SYNC_SERVICE, DEFAULT_STREAM_WINDOW, TrafficClass::Bulk)?;
        register_cancel(operation, &stream);
        if operation.is_cancelled() {
            return Err(sync_cancelled_error());
        }
        let staging = params
            .transfer_id
            .as_deref()
            .map(|id| staging_path(Path::new(&params.local_path), id))
            .transpose()?;
        let offset = staging
            .as_ref()
            .and_then(|path| fs::metadata(path).ok())
            .map_or(0, |metadata| metadata.len());
        stream.send_data(
            encode(&SyncRequest::Pull {
                transfer_id: params.transfer_id.clone(),
                remote_path: params.remote_path.clone(),
                offset,
                block_size: params.block_size,
            })?,
            true,
        )?;
        let ready: SyncReply = decode(&actor_data(&mut stream)?.payload, "sync reply")?;
        let (transfer_id, remote_offset, total_size, file_hash) = match ready {
            SyncReply::Ready {
                transfer_id,
                offset,
                total_size,
                file_hash,
            } => (transfer_id, offset, total_size, file_hash),
            SyncReply::Failure { error } => return Err(LinkError::Remote(error)),
            _ => return Err(LinkError::UnexpectedFrame("invalid sync pull READY")),
        };
        let ready_at = started.elapsed();
        trace(format_args!(
            "pull {transfer_id}: READY after {:.3}s (offset {remote_offset}, total {total_size}, block {})",
            ready_at.as_secs_f64(),
            params.block_size
        ));
        if !is_valid_transfer_id(&transfer_id)
            || params
                .transfer_id
                .as_ref()
                .is_some_and(|expected| expected != &transfer_id)
            || remote_offset != offset
            || remote_offset > total_size
        {
            return Err(LinkError::UnexpectedFrame("sync pull resume mismatch"));
        }
        operation.transfer_id(transfer_id.clone());
        operation.phase(SyncProgressPhase::Transferring, remote_offset, total_size);
        let staging = match staging {
            Some(path) => path,
            None => staging_path(Path::new(&params.local_path), &transfer_id)?,
        };
        let mut output = open_staging(&staging, remote_offset)?;
        let mut hasher = hash_prefix(&mut output, remote_offset)?;
        output.seek(SeekFrom::Start(remote_offset))?;
        let mut received = remote_offset;
        loop {
            if operation.is_cancelled() {
                let _ = stream.reset("sync cancelled");
                return Err(sync_cancelled_error());
            }
            let data = actor_data(&mut stream)?;
            output.write_all(&data.payload)?;
            hasher.update(&data.payload);
            received = received
                .checked_add(data.payload.len() as u64)
                .ok_or(LinkError::SequenceExhausted)?;
            operation.transferred(received);
            if received > total_size {
                return Err(LinkError::UnexpectedFrame(
                    "sync pull exceeded declared size",
                ));
            }
            if data.header.flags & FLAG_END_STREAM != 0 {
                break;
            }
        }
        let payload_at = started.elapsed();
        trace(format_args!(
            "pull {transfer_id}: received {} bytes in {:.3}s after READY ({:.3}s total)",
            received.saturating_sub(remote_offset),
            payload_at.saturating_sub(ready_at).as_secs_f64(),
            payload_at.as_secs_f64()
        ));
        if received != total_size || hasher.finalize().to_hex().as_str() != file_hash {
            output.set_len(0)?;
            output.sync_all()?;
            return Err(LinkError::Remote(RpcError::new(
                error_codes::CHECKSUM_MISMATCH,
                "Checksum mismatch",
            )));
        }
        output.flush()?;
        output.sync_all()?;
        drop(output);
        commit_host_file(&staging, Path::new(&params.local_path))?;
        operation.transferred(received);
        let committed_at = started.elapsed();
        trace(format_args!(
            "pull {transfer_id}: host durability and commit took {:.3}s ({:.3}s total)",
            committed_at.saturating_sub(payload_at).as_secs_f64(),
            committed_at.as_secs_f64()
        ));
        actor_close(&mut stream)?;
        Ok(SyncPullResult {
            transfer_id,
            total_size,
            received_size: received,
            state: TransferState::Complete,
        })
    }
}

pub(super) fn validate_host_path(path: &str) -> Result<(), RpcError> {
    if Path::new(path).is_absolute() {
        Ok(())
    } else {
        Err(RpcError::invalid_params("host paths must be absolute"))
    }
}

fn validate_block_size(block_size: u32) -> Result<(), RpcError> {
    if block_size == 0 || block_size > MAX_SYNC_BLOCK_SIZE {
        Err(RpcError::invalid_params(
            "block_size must be between 1 and 1048576",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn hash_file(file: &mut File, length: u64) -> Result<String, RpcError> {
    hash_prefix(file, length)
        .map(|hasher| hasher.finalize().to_hex().to_string())
        .map_err(link_rpc_error)
}

fn hash_file_with_operation(
    file: &mut File,
    length: u64,
    operation: &HostSyncOperation,
) -> Result<String, RpcError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|error| host_file_error("seek", "source", &error))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; kindlebridge_schema::DEFAULT_SYNC_BLOCK_SIZE as usize];
    let mut hashed = 0_u64;
    while hashed < length {
        if operation.is_cancelled() {
            return Err(cancelled_rpc_error());
        }
        let remaining = length - hashed;
        let limit = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let count = file
            .read(&mut buffer[..limit])
            .map_err(|error| host_file_error("read", "source", &error))?;
        if count == 0 {
            return Err(RpcError::invalid_params("local file ended while hashing"));
        }
        hasher.update(&buffer[..count]);
        hashed += count as u64;
        operation.transferred(hashed);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn cancelled_rpc_error() -> RpcError {
    RpcError::new(error_codes::TRANSFER_CANCELLED, "Transfer cancelled")
}

fn sync_cancelled_error() -> LinkError {
    LinkError::Remote(cancelled_rpc_error())
}

fn hash_prefix(file: &mut File, length: u64) -> Result<blake3::Hasher, LinkError> {
    file.seek(SeekFrom::Start(0))?;
    let mut hasher = blake3::Hasher::new();
    let mut remaining = length;
    let mut buffer = vec![0_u8; kindlebridge_schema::DEFAULT_SYNC_BLOCK_SIZE as usize];
    while remaining > 0 {
        let limit = usize::try_from(remaining)
            .map_err(|_| LinkError::UnexpectedFrame("host file is too large"))?
            .min(buffer.len());
        let read = file.read(&mut buffer[..limit])?;
        if read == 0 {
            return Err(LinkError::UnexpectedFrame(
                "host staging file was truncated",
            ));
        }
        hasher.update(&buffer[..read]);
        remaining -= u64::try_from(read)
            .map_err(|_| LinkError::UnexpectedFrame("host file is too large"))?;
    }
    Ok(hasher)
}

fn staging_path(destination: &Path, transfer_id: &str) -> Result<PathBuf, LinkError> {
    if !is_valid_transfer_id(transfer_id) {
        return Err(LinkError::UnexpectedFrame("invalid sync transfer ID"));
    }
    Ok(PathBuf::from(format!(
        "{}.kindlebridge.{transfer_id}.part",
        destination.display()
    )))
}

fn validate_requested_transfer_id(transfer_id: Option<&str>) -> Result<(), RpcError> {
    if transfer_id.is_some_and(|value| !is_valid_transfer_id(value)) {
        Err(RpcError::invalid_params("invalid transfer_id"))
    } else {
        Ok(())
    }
}

fn open_staging(path: &Path, offset: u64) -> Result<File, LinkError> {
    let parent = path
        .parent()
        .ok_or(LinkError::UnexpectedFrame("host destination has no parent"))?;
    fs::create_dir_all(parent)?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    if offset == 0 {
        options.truncate(true);
    }
    let file = options.open(path)?;
    if file.metadata()?.len() != offset {
        return Err(LinkError::UnexpectedFrame(
            "host staging size changed before resume",
        ));
    }
    Ok(file)
}

fn commit_host_file(staging: &Path, destination: &Path) -> Result<(), LinkError> {
    if destination.exists() {
        let metadata = fs::symlink_metadata(destination)?;
        if !metadata.file_type().is_file() {
            return Err(LinkError::UnexpectedFrame(
                "host destination must be a regular file",
            ));
        }
        fs::remove_file(destination)?;
    }
    fs::rename(staging, destination)?;
    Ok(())
}

pub(super) fn host_file_error(operation: &str, path: &str, error: &std::io::Error) -> RpcError {
    RpcError::new(
        error_codes::INVALID_PARAMS,
        format!("Unable to {operation} host file"),
    )
    .with_data(serde_json::json!({
        "path": path,
        "detail": error.to_string(),
    }))
}

fn register_cancel(operation: &HostSyncOperation, stream: &ActorStream) {
    let stream = stream.clone();
    operation.on_cancel(move || {
        let _ = std::thread::Builder::new()
            .name("kindlebridge-sync-cancel".to_owned())
            .spawn(move || {
                let _ = stream.cancel_receive();
                let _ = stream.reset("sync cancelled");
            });
    });
}

fn trace(arguments: std::fmt::Arguments<'_>) {
    if std::env::var_os("KINDLEBRIDGE_TRACE_SYNC").is_some() {
        eprintln!("[kindlebridge-sync] {arguments}");
    }
}

fn actor_data(stream: &mut ActorStream) -> Result<Frame, LinkError> {
    let frame = stream.recv()?;
    expect(&frame, Command::Data, stream.id())?;
    Ok(frame)
}

fn actor_close(stream: &mut ActorStream) -> Result<(), LinkError> {
    let frame = stream.recv()?;
    expect(&frame, Command::Close, stream.id())
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, LinkError> {
    Ok(serde_json::to_vec(value)?)
}

fn decode<T: serde::de::DeserializeOwned>(
    payload: &[u8],
    label: &'static str,
) -> Result<T, LinkError> {
    serde_json::from_slice(payload).map_err(|source| LinkError::InvalidPayload { label, source })
}
