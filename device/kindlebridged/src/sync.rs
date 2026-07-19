//! Resumable sync transaction state independent of the transport backend.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::cell::Cell;

use kindlebridge_schema::device_protocol::is_valid_transfer_id;
use kindlebridge_schema::{error_codes, RpcError, SyncStatus, TransferDirection, TransferState};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

pub const SYNC_BLOCK_SIZE: u64 = 65_536;
// A checkpoint forces both data and metadata to stable storage. On the Kindle
// userstore that stalls an otherwise sequential USB push. Transport disconnects
// already checkpoint the writer, and completion always syncs the whole file, so
// reserve periodic barriers for genuinely large transfers. At 256 MiB the worst
// replay after a daemon/device failure stays bounded without penalising routine
// development payloads.
const CHECKPOINT_INTERVAL: u64 = 256 * 1024 * 1024;
const METADATA_VERSION: u32 = 1;
static NEXT_TRANSFER: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
thread_local! {
    static HASH_PREFIX_CALLS: Cell<u64> = const { Cell::new(0) };
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalPath(String);

impl LogicalPath {
    pub fn parse(value: impl Into<String>) -> Result<Self, PathError> {
        let value = value.into();
        if value.is_empty() || value.starts_with('/') || value.contains('\\') {
            return Err(PathError::NotRelative);
        }
        if value.len() > 1_024 {
            return Err(PathError::TooLong);
        }
        if value.chars().any(|character| character.is_control()) {
            return Err(PathError::ControlCharacter);
        }
        if value.nfc().ne(value.chars()) {
            return Err(PathError::NotNfc);
        }
        for component in value.split('/') {
            if component.is_empty() || matches!(component, "." | "..") {
                return Err(PathError::InvalidComponent);
            }
            if component.len() > 255 {
                return Err(PathError::ComponentTooLong);
            }
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn ascii_case_fold_key(&self) -> String {
        self.0.to_ascii_lowercase()
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PathError {
    #[error("path must be non-empty, relative, and use forward slashes")]
    NotRelative,
    #[error("path exceeds 1024 UTF-8 bytes")]
    TooLong,
    #[error("path component exceeds 255 UTF-8 bytes")]
    ComponentTooLong,
    #[error("path contains an empty, dot, or dot-dot component")]
    InvalidComponent,
    #[error("path contains a control character")]
    ControlCharacter,
    #[error("path is not Unicode NFC")]
    NotNfc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncFileSpec {
    pub path: LogicalPath,
    pub size: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SyncFileState {
    spec: SyncFileSpec,
    received: BTreeMap<u32, [u8; 32]>,
    verified: bool,
}

impl SyncFileState {
    fn block_count(&self) -> u32 {
        self.spec
            .size
            .div_ceil(SYNC_BLOCK_SIZE)
            .try_into()
            .unwrap_or(u32::MAX)
    }

    fn expected_block_len(&self, index: u32) -> Option<u64> {
        let count = self.block_count();
        if index >= count {
            return None;
        }
        if index + 1 < count {
            return Some(SYNC_BLOCK_SIZE);
        }
        Some(self.spec.size - u64::from(index) * SYNC_BLOCK_SIZE)
    }

    fn complete(&self) -> bool {
        self.verified && self.received.len() == self.block_count() as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPhase {
    Receiving,
    ReadyToCommit,
    Committed,
    Aborted,
}

#[derive(Debug)]
pub struct SyncTransaction {
    id: String,
    phase: SyncPhase,
    files: BTreeMap<LogicalPath, SyncFileState>,
}

impl SyncTransaction {
    pub fn new(id: impl Into<String>, specs: Vec<SyncFileSpec>) -> Result<Self, SyncError> {
        let id = id.into();
        if id.is_empty() || id.len() > 128 {
            return Err(SyncError::InvalidTransactionId);
        }

        let mut files = BTreeMap::new();
        let mut folded_paths = BTreeMap::<String, LogicalPath>::new();
        for spec in specs {
            let folded = spec.path.ascii_case_fold_key();
            if let Some(previous) = folded_paths.insert(folded, spec.path.clone()) {
                return Err(SyncError::CaseCollision {
                    first: previous,
                    second: spec.path,
                });
            }
            let path = spec.path.clone();
            if files
                .insert(
                    path.clone(),
                    SyncFileState {
                        spec,
                        received: BTreeMap::new(),
                        verified: false,
                    },
                )
                .is_some()
            {
                return Err(SyncError::DuplicatePath(path));
            }
        }

        Ok(Self {
            id,
            phase: SyncPhase::Receiving,
            files,
        })
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub const fn phase(&self) -> SyncPhase {
        self.phase
    }

    pub fn record_block(
        &mut self,
        path: &LogicalPath,
        index: u32,
        raw_len: u64,
        digest: [u8; 32],
    ) -> Result<(), SyncError> {
        self.require_receiving()?;
        let file = self
            .files
            .get_mut(path)
            .ok_or_else(|| SyncError::UnknownPath(path.clone()))?;
        let expected = file
            .expected_block_len(index)
            .ok_or(SyncError::InvalidBlockIndex(index))?;
        if raw_len != expected {
            return Err(SyncError::InvalidBlockLength {
                expected,
                actual: raw_len,
            });
        }
        if let Some(previous) = file.received.insert(index, digest) {
            if previous != digest {
                file.received.insert(index, previous);
                return Err(SyncError::ConflictingResumeBlock(index));
            }
        }
        Ok(())
    }

    pub fn verify_file(
        &mut self,
        path: &LogicalPath,
        actual_digest: [u8; 32],
    ) -> Result<(), SyncError> {
        self.require_receiving()?;
        let file = self
            .files
            .get_mut(path)
            .ok_or_else(|| SyncError::UnknownPath(path.clone()))?;
        if file.received.len() != file.block_count() as usize {
            return Err(SyncError::MissingBlocks);
        }
        if actual_digest != file.spec.digest {
            return Err(SyncError::FileDigestMismatch);
        }
        file.verified = true;
        if self.files.values().all(SyncFileState::complete) {
            self.phase = SyncPhase::ReadyToCommit;
        }
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), SyncError> {
        if self.phase != SyncPhase::ReadyToCommit {
            return Err(SyncError::NotReadyToCommit);
        }
        self.phase = SyncPhase::Committed;
        Ok(())
    }

    pub fn abort(&mut self) -> Result<(), SyncError> {
        if matches!(self.phase, SyncPhase::Committed | SyncPhase::Aborted) {
            return Err(SyncError::TerminalTransaction);
        }
        self.phase = SyncPhase::Aborted;
        Ok(())
    }

    fn require_receiving(&self) -> Result<(), SyncError> {
        if self.phase != SyncPhase::Receiving {
            return Err(SyncError::NotReceiving);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SyncError {
    #[error("invalid transaction id")]
    InvalidTransactionId,
    #[error("duplicate path: {0:?}")]
    DuplicatePath(LogicalPath),
    #[error("ASCII case-fold collision between {first:?} and {second:?}")]
    CaseCollision {
        first: LogicalPath,
        second: LogicalPath,
    },
    #[error("unknown path: {0:?}")]
    UnknownPath(LogicalPath),
    #[error("invalid block index: {0}")]
    InvalidBlockIndex(u32),
    #[error("invalid block length: expected {expected}, got {actual}")]
    InvalidBlockLength { expected: u64, actual: u64 },
    #[error("resume metadata conflicts for block {0}")]
    ConflictingResumeBlock(u32),
    #[error("file still has missing blocks")]
    MissingBlocks,
    #[error("file digest mismatch")]
    FileDigestMismatch,
    #[error("transaction is not receiving blocks")]
    NotReceiving,
    #[error("transaction is not ready to commit")]
    NotReadyToCommit,
    #[error("transaction is already terminal")]
    TerminalTransaction,
}

/// Filesystem-backed sync storage. Remote paths are always relative to this
/// root, and staging files live beside the final tree for atomic commits.
#[derive(Clone, Debug)]
pub struct SyncStore {
    root: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TransferMetadata {
    version: u32,
    transfer_id: String,
    direction: TransferDirection,
    remote_path: String,
    total_size: u64,
    file_hash: String,
    next_offset: u64,
    state: TransferState,
}

pub struct PushTransfer {
    store: SyncStore,
    metadata: TransferMetadata,
    file: Option<File>,
    hasher: blake3::Hasher,
    next_checkpoint: u64,
}

pub struct PullTransfer {
    store: SyncStore,
    metadata: TransferMetadata,
    file: File,
    next_checkpoint: u64,
}

impl SyncStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn begin_push(
        &self,
        transfer_id: Option<&str>,
        remote_path: &str,
        total_size: u64,
        file_hash: &str,
    ) -> Result<PushTransfer, StoreError> {
        let logical = LogicalPath::parse(remote_path.to_owned())?;
        parse_hash(file_hash)?;
        self.ensure_layout()?;

        let mut metadata = if let Some(id) = transfer_id {
            validate_transfer_id(id)?;
            let metadata = self.read_metadata(id)?;
            validate_metadata(
                &metadata,
                TransferDirection::Push,
                logical.as_str(),
                total_size,
                file_hash,
            )?;
            metadata
        } else {
            let metadata = TransferMetadata {
                version: METADATA_VERSION,
                transfer_id: allocate_transfer_id("push"),
                direction: TransferDirection::Push,
                remote_path: logical.as_str().to_owned(),
                total_size,
                file_hash: file_hash.to_owned(),
                next_offset: 0,
                state: TransferState::InProgress,
            };
            File::create(self.part_path(&metadata.transfer_id))?;
            self.write_metadata(&metadata)?;
            metadata
        };

        if metadata.state == TransferState::Complete {
            return Ok(PushTransfer {
                store: self.clone(),
                metadata,
                file: None,
                hasher: blake3::Hasher::new(),
                next_checkpoint: u64::MAX,
            });
        }

        let part_path = self.part_path(&metadata.transfer_id);
        let mut file = OpenOptions::new().read(true).write(true).open(part_path)?;
        let actual_size = file.metadata()?.len();
        if actual_size > total_size {
            return Err(StoreError::InvalidState("staging file exceeds total_size"));
        }
        metadata.next_offset = actual_size;
        let hasher = hash_prefix(&mut file, actual_size)?;
        file.seek(SeekFrom::End(0))?;
        self.write_metadata(&metadata)?;
        let next_checkpoint = actual_size
            .saturating_add(CHECKPOINT_INTERVAL)
            .min(total_size);
        Ok(PushTransfer {
            store: self.clone(),
            metadata,
            file: Some(file),
            hasher,
            next_checkpoint,
        })
    }

    pub fn begin_pull(
        &self,
        transfer_id: Option<&str>,
        remote_path: &str,
        offset: u64,
    ) -> Result<PullTransfer, StoreError> {
        let logical = LogicalPath::parse(remote_path.to_owned())?;
        self.ensure_layout()?;
        let target = self.open_existing_target(&logical)?;
        let mut file = File::open(target)?;
        let total_size = file.metadata()?.len();
        if offset > total_size {
            return Err(StoreError::InvalidState("resume offset exceeds file size"));
        }
        let file_hash = hash_file(&mut file)?;
        let mut metadata = if let Some(id) = transfer_id {
            validate_transfer_id(id)?;
            let mut metadata = self.read_metadata(id)?;
            validate_metadata(
                &metadata,
                TransferDirection::Pull,
                logical.as_str(),
                total_size,
                &file_hash,
            )?;
            // The host staging length is the durable acknowledgement after a
            // disconnect; replaying from it is safe.
            metadata.next_offset = offset;
            metadata
        } else {
            TransferMetadata {
                version: METADATA_VERSION,
                transfer_id: allocate_transfer_id("pull"),
                direction: TransferDirection::Pull,
                remote_path: logical.as_str().to_owned(),
                total_size,
                file_hash,
                next_offset: offset,
                state: TransferState::InProgress,
            }
        };
        metadata.state = if offset == total_size {
            TransferState::Complete
        } else {
            TransferState::InProgress
        };
        file.seek(SeekFrom::Start(offset))?;
        self.write_metadata(&metadata)?;
        Ok(PullTransfer {
            store: self.clone(),
            metadata,
            file,
            next_checkpoint: offset.saturating_add(CHECKPOINT_INTERVAL),
        })
    }

    pub fn status(&self, transfer_id: &str) -> Result<SyncStatus, StoreError> {
        validate_transfer_id(transfer_id)?;
        let metadata = self.read_metadata(transfer_id)?;
        Ok(SyncStatus {
            transfer_id: metadata.transfer_id,
            direction: metadata.direction,
            remote_path: metadata.remote_path,
            next_offset: metadata.next_offset,
            total_size: metadata.total_size,
            state: metadata.state,
        })
    }

    fn ensure_layout(&self) -> Result<(), StoreError> {
        fs::create_dir_all(&self.root)?;
        fs::create_dir_all(self.stage_root())?;
        Ok(())
    }

    fn stage_root(&self) -> PathBuf {
        self.root.join(".kindlebridge-sync")
    }

    fn part_path(&self, transfer_id: &str) -> PathBuf {
        self.stage_root().join(format!("{transfer_id}.part"))
    }

    fn metadata_path(&self, transfer_id: &str) -> PathBuf {
        self.stage_root().join(format!("{transfer_id}.json"))
    }

    fn read_metadata(&self, transfer_id: &str) -> Result<TransferMetadata, StoreError> {
        let bytes = fs::read(self.metadata_path(transfer_id)).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                StoreError::TransferNotFound(transfer_id.to_owned())
            } else {
                StoreError::Io(error)
            }
        })?;
        let metadata: TransferMetadata = serde_json::from_slice(&bytes)?;
        if metadata.version != METADATA_VERSION || metadata.transfer_id != transfer_id {
            return Err(StoreError::InvalidState("invalid transfer metadata"));
        }
        Ok(metadata)
    }

    fn write_metadata(&self, metadata: &TransferMetadata) -> Result<(), StoreError> {
        let path = self.metadata_path(&metadata.transfer_id);
        let temporary = path.with_extension("json.tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(&serde_json::to_vec(metadata)?)?;
        file.sync_all()?;
        #[cfg(windows)]
        if path.exists() {
            fs::remove_file(&path)?;
        }
        fs::rename(temporary, path)?;
        Ok(())
    }

    fn commit_target(&self, logical: &LogicalPath, staging: &Path) -> Result<(), StoreError> {
        let target = self.prepare_target(logical)?;
        if target.exists() {
            let metadata = fs::symlink_metadata(&target)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(StoreError::UnsafePath);
            }
            #[cfg(windows)]
            fs::remove_file(&target)?;
        }
        fs::rename(staging, target)?;
        Ok(())
    }

    fn prepare_target(&self, logical: &LogicalPath) -> Result<PathBuf, StoreError> {
        let mut current = self.root.clone();
        let mut components = logical.as_str().split('/').peekable();
        while let Some(component) = components.next() {
            current.push(component);
            if components.peek().is_none() {
                break;
            }
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => return Err(StoreError::UnsafePath),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    fs::create_dir(&current)?;
                }
                Err(error) => return Err(StoreError::Io(error)),
            }
        }
        Ok(current)
    }

    fn open_existing_target(&self, logical: &LogicalPath) -> Result<PathBuf, StoreError> {
        let mut current = self.root.clone();
        for component in logical.as_str().split('/') {
            current.push(component);
            let metadata = fs::symlink_metadata(&current).map_err(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    StoreError::FileNotFound(logical.as_str().to_owned())
                } else {
                    StoreError::Io(error)
                }
            })?;
            if metadata.file_type().is_symlink() {
                return Err(StoreError::UnsafePath);
            }
        }
        if !fs::metadata(&current)?.is_file() {
            return Err(StoreError::FileNotFound(logical.as_str().to_owned()));
        }
        Ok(current)
    }
}

impl PushTransfer {
    #[must_use]
    pub fn transfer_id(&self) -> &str {
        &self.metadata.transfer_id
    }

    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.metadata.next_offset
    }

    #[must_use]
    pub const fn total_size(&self) -> u64 {
        self.metadata.total_size
    }

    #[must_use]
    pub fn file_hash(&self) -> &str {
        &self.metadata.file_hash
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.metadata.state == TransferState::Complete
    }

    pub fn write_chunk(&mut self, data: &[u8]) -> Result<(), StoreError> {
        self.write_chunk_without_hash(data)?;
        self.hasher.update(data);
        Ok(())
    }

    pub(crate) fn write_chunk_without_hash(&mut self, data: &[u8]) -> Result<(), StoreError> {
        let length = u64::try_from(data.len()).map_err(|_| StoreError::SizeOverflow)?;
        let end = self
            .metadata
            .next_offset
            .checked_add(length)
            .ok_or(StoreError::SizeOverflow)?;
        if end > self.metadata.total_size || self.file.is_none() {
            return Err(StoreError::InvalidState("push exceeds declared size"));
        }
        self.file
            .as_mut()
            .expect("file presence checked")
            .write_all(data)?;
        self.metadata.next_offset = end;
        if end >= self.next_checkpoint && end < self.metadata.total_size {
            self.checkpoint()?;
            self.next_checkpoint = end.saturating_add(CHECKPOINT_INTERVAL);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn hash_state(&self) -> blake3::Hasher {
        self.hasher.clone()
    }

    pub fn checkpoint(&mut self) -> Result<(), StoreError> {
        if let Some(file) = self.file.as_mut() {
            file.flush()?;
            file.sync_data()?;
            self.store.write_metadata(&self.metadata)?;
        }
        Ok(())
    }

    /// Conservatively discard bytes from the last frame of an interrupted
    /// session. A USB recovery burst may have supplied the missing tail of that
    /// frame, so only earlier complete frames are safe resume points.
    pub fn rollback_for_resume(mut self, offset: u64) -> Result<(), StoreError> {
        if offset > self.metadata.next_offset {
            return Err(StoreError::InvalidState(
                "rollback offset exceeds the received data",
            ));
        }
        let file = self.file.as_mut().ok_or(StoreError::InvalidState(
            "completed push has no staging file",
        ))?;
        file.set_len(offset)?;
        file.sync_data()?;
        self.metadata.next_offset = offset;
        self.store.write_metadata(&self.metadata)?;
        Ok(())
    }

    pub fn finish(self) -> Result<SyncStatus, StoreError> {
        let digest = self.hasher.finalize();
        self.finish_with_digest(digest)
    }

    pub(crate) fn finish_with_digest(
        mut self,
        digest: blake3::Hash,
    ) -> Result<SyncStatus, StoreError> {
        if self.is_complete() {
            return self.store.status(&self.metadata.transfer_id);
        }
        if self.metadata.next_offset != self.metadata.total_size {
            self.checkpoint()?;
            return Err(StoreError::InvalidState("push ended before total_size"));
        }
        let mut file = self.file.take().ok_or(StoreError::InvalidState(
            "completed push has no staging file",
        ))?;
        file.flush()?;
        file.sync_all()?;
        if digest.to_hex().as_str() != self.metadata.file_hash {
            file.set_len(0)?;
            file.sync_all()?;
            self.metadata.next_offset = 0;
            self.store.write_metadata(&self.metadata)?;
            return Err(StoreError::ChecksumMismatch);
        }
        drop(file);
        let logical = LogicalPath::parse(self.metadata.remote_path.clone())?;
        self.store
            .commit_target(&logical, &self.store.part_path(&self.metadata.transfer_id))?;
        self.metadata.state = TransferState::Complete;
        self.store.write_metadata(&self.metadata)?;
        self.store.status(&self.metadata.transfer_id)
    }
}

impl PullTransfer {
    #[must_use]
    pub fn transfer_id(&self) -> &str {
        &self.metadata.transfer_id
    }

    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.metadata.next_offset
    }

    #[must_use]
    pub const fn total_size(&self) -> u64 {
        self.metadata.total_size
    }

    #[must_use]
    pub fn file_hash(&self) -> &str {
        &self.metadata.file_hash
    }

    pub fn read_chunk(&mut self, buffer: &mut [u8]) -> Result<usize, StoreError> {
        let read = self.file.read(buffer)?;
        self.metadata.next_offset = self
            .metadata
            .next_offset
            .checked_add(u64::try_from(read).map_err(|_| StoreError::SizeOverflow)?)
            .ok_or(StoreError::SizeOverflow)?;
        Ok(read)
    }

    pub fn checkpoint(&self) -> Result<(), StoreError> {
        self.store.write_metadata(&self.metadata)
    }

    pub fn checkpoint_if_due(&mut self) -> Result<(), StoreError> {
        if self.metadata.next_offset >= self.next_checkpoint
            && self.metadata.next_offset < self.metadata.total_size
        {
            self.checkpoint()?;
            self.next_checkpoint = self
                .metadata
                .next_offset
                .saturating_add(CHECKPOINT_INTERVAL);
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<SyncStatus, StoreError> {
        if self.metadata.next_offset != self.metadata.total_size {
            return Err(StoreError::InvalidState("pull ended before total_size"));
        }
        self.metadata.state = TransferState::Complete;
        self.store.write_metadata(&self.metadata)?;
        self.store.status(&self.metadata.transfer_id)
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid BLAKE3 digest")]
    InvalidHash,
    #[error("invalid transfer id")]
    InvalidTransferId,
    #[error("transfer not found: {0}")]
    TransferNotFound(String),
    #[error("file not found: {0}")]
    FileNotFound(String),
    #[error("transfer metadata does not match")]
    MetadataMismatch,
    #[error("unsafe path component")]
    UnsafePath,
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("size overflow")]
    SizeOverflow,
    #[error("invalid state: {0}")]
    InvalidState(&'static str),
}

impl StoreError {
    #[must_use]
    pub fn into_rpc(self) -> RpcError {
        match self {
            Self::Path(_) | Self::InvalidHash | Self::InvalidTransferId => {
                RpcError::invalid_params(self.to_string())
            }
            Self::TransferNotFound(transfer_id) => {
                RpcError::new(error_codes::TRANSFER_NOT_FOUND, "Transfer not found")
                    .with_data(json!({ "transfer_id": transfer_id }))
            }
            Self::FileNotFound(remote_path) => {
                RpcError::new(error_codes::FILE_NOT_FOUND, "File not found")
                    .with_data(json!({ "remote_path": remote_path }))
            }
            Self::ChecksumMismatch => {
                RpcError::new(error_codes::CHECKSUM_MISMATCH, "Checksum mismatch")
            }
            Self::MetadataMismatch | Self::UnsafePath | Self::InvalidState(_) => {
                RpcError::new(error_codes::INVALID_STATE, "Invalid sync state")
                    .with_data(json!({ "detail": self.to_string() }))
            }
            Self::Io(_) | Self::Json(_) | Self::SizeOverflow => RpcError::internal_error(),
        }
    }
}

fn validate_transfer_id(value: &str) -> Result<(), StoreError> {
    if is_valid_transfer_id(value) {
        Ok(())
    } else {
        Err(StoreError::InvalidTransferId)
    }
}

fn validate_metadata(
    metadata: &TransferMetadata,
    direction: TransferDirection,
    remote_path: &str,
    total_size: u64,
    file_hash: &str,
) -> Result<(), StoreError> {
    if metadata.direction == direction
        && metadata.remote_path == remote_path
        && metadata.total_size == total_size
        && metadata.file_hash == file_hash
    {
        Ok(())
    } else {
        Err(StoreError::MetadataMismatch)
    }
}

fn parse_hash(value: &str) -> Result<blake3::Hash, StoreError> {
    blake3::Hash::from_hex(value).map_err(|_| StoreError::InvalidHash)
}

fn hash_prefix(file: &mut File, length: u64) -> Result<blake3::Hasher, StoreError> {
    #[cfg(test)]
    HASH_PREFIX_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    file.seek(SeekFrom::Start(0))?;
    let mut remaining = length;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    while remaining != 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| StoreError::SizeOverflow)?;
        let read = file.read(&mut buffer[..limit])?;
        if read == 0 {
            return Err(StoreError::InvalidState("staging file was truncated"));
        }
        hasher.update(&buffer[..read]);
        remaining -= u64::try_from(read).map_err(|_| StoreError::SizeOverflow)?;
    }
    Ok(hasher)
}

#[cfg(test)]
fn hash_prefix_call_count() -> u64 {
    HASH_PREFIX_CALLS.with(Cell::get)
}

fn hash_file(file: &mut File) -> Result<String, StoreError> {
    let length = file.metadata()?.len();
    let hasher = hash_prefix(file, length)?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn allocate_transfer_id(prefix: &str) -> String {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = NEXT_TRANSFER.fetch_add(1, Ordering::Relaxed);
    let seed = format!("{prefix}:{}:{time}:{counter}", std::process::id());
    let digest = blake3::hash(seed.as_bytes()).to_hex();
    format!("{prefix}-{}", &digest.as_str()[..24])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "kindlebridge-sync-{label}-{}-{}",
            std::process::id(),
            NEXT_TRANSFER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn rejects_escape_and_non_normalized_paths() {
        assert_eq!(
            LogicalPath::parse("../etc/passwd"),
            Err(PathError::InvalidComponent)
        );
        assert_eq!(
            LogicalPath::parse("assets/e\u{301}.txt"),
            Err(PathError::NotNfc)
        );
    }

    #[test]
    fn resumable_blocks_are_idempotent_but_not_replaceable() {
        let path = LogicalPath::parse("bin/app").unwrap();
        let file_digest = [9; 32];
        let mut transaction = SyncTransaction::new(
            "tx-1",
            vec![SyncFileSpec {
                path: path.clone(),
                size: SYNC_BLOCK_SIZE + 1,
                digest: file_digest,
            }],
        )
        .unwrap();
        transaction
            .record_block(&path, 0, SYNC_BLOCK_SIZE, [1; 32])
            .unwrap();
        transaction
            .record_block(&path, 0, SYNC_BLOCK_SIZE, [1; 32])
            .unwrap();
        assert_eq!(
            transaction.record_block(&path, 0, SYNC_BLOCK_SIZE, [2; 32]),
            Err(SyncError::ConflictingResumeBlock(0))
        );
        transaction.record_block(&path, 1, 1, [3; 32]).unwrap();
        transaction.verify_file(&path, file_digest).unwrap();
        assert_eq!(transaction.phase(), SyncPhase::ReadyToCommit);
        transaction.commit().unwrap();
        assert_eq!(transaction.phase(), SyncPhase::Committed);
    }

    #[test]
    fn rejects_case_collisions_for_cross_platform_reproducibility() {
        let result = SyncTransaction::new(
            "tx",
            vec![
                SyncFileSpec {
                    path: LogicalPath::parse("Assets/Icon.png").unwrap(),
                    size: 0,
                    digest: [0; 32],
                },
                SyncFileSpec {
                    path: LogicalPath::parse("assets/icon.png").unwrap(),
                    size: 0,
                    digest: [0; 32],
                },
            ],
        );
        assert!(matches!(result, Err(SyncError::CaseCollision { .. })));
    }

    #[test]
    fn filesystem_push_resumes_and_commits_inside_the_sync_root() {
        let root = test_root("resume");
        let store = SyncStore::new(&root);
        let payload: Vec<u8> = (0..1_500_123_u64)
            .map(|index| (index.wrapping_mul(31) & 0xff) as u8)
            .collect();
        let digest = blake3::hash(&payload).to_hex().to_string();
        let split = 700_001;

        let mut first = store
            .begin_push(None, "apps/demo/payload.bin", payload.len() as u64, &digest)
            .unwrap();
        let transfer_id = first.transfer_id().to_owned();
        first.write_chunk(&payload[..split]).unwrap();
        first.checkpoint().unwrap();
        drop(first);

        let restarted_store = SyncStore::new(&root);
        let mut resumed = restarted_store
            .begin_push(
                Some(&transfer_id),
                "apps/demo/payload.bin",
                payload.len() as u64,
                &digest,
            )
            .unwrap();
        assert_eq!(resumed.offset(), split as u64);
        resumed.write_chunk(&payload[split..]).unwrap();
        let status = resumed.finish().unwrap();
        assert_eq!(status.state, TransferState::Complete);
        assert_eq!(
            fs::read(root.join("apps/demo/payload.bin")).unwrap(),
            payload
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interrupted_push_rolls_back_the_last_frame_before_resume() {
        let root = test_root("frame-rollback");
        let store = SyncStore::new(&root);
        let payload = vec![0x6d; 3 * SYNC_BLOCK_SIZE as usize];
        let digest = blake3::hash(&payload).to_hex().to_string();
        let mut transfer = store
            .begin_push(None, "rollback/payload.bin", payload.len() as u64, &digest)
            .unwrap();
        let transfer_id = transfer.transfer_id().to_owned();
        let safe_offset = SYNC_BLOCK_SIZE;
        transfer
            .write_chunk(&payload[..SYNC_BLOCK_SIZE as usize])
            .unwrap();
        transfer
            .write_chunk(&payload[SYNC_BLOCK_SIZE as usize..2 * SYNC_BLOCK_SIZE as usize])
            .unwrap();

        let hash_calls_before_rollback = hash_prefix_call_count();
        transfer.rollback_for_resume(safe_offset).unwrap();
        assert_eq!(hash_prefix_call_count(), hash_calls_before_rollback);
        assert_eq!(store.status(&transfer_id).unwrap().next_offset, safe_offset);

        let mut resumed = store
            .begin_push(
                Some(&transfer_id),
                "rollback/payload.bin",
                payload.len() as u64,
                &digest,
            )
            .unwrap();
        resumed
            .write_chunk(&payload[safe_offset as usize..])
            .unwrap();
        assert_eq!(resumed.finish().unwrap().state, TransferState::Complete);
        assert_eq!(
            fs::read(root.join("rollback/payload.bin")).unwrap(),
            payload
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn filesystem_push_checksum_failure_resets_the_resumable_offset() {
        let root = test_root("checksum");
        let store = SyncStore::new(&root);
        let digest = blake3::hash(b"abc").to_hex().to_string();
        let mut transfer = store.begin_push(None, "bad.bin", 3, &digest).unwrap();
        let transfer_id = transfer.transfer_id().to_owned();
        transfer.write_chunk(b"abd").unwrap();
        let result = transfer.finish();
        assert!(
            matches!(result, Err(StoreError::ChecksumMismatch)),
            "unexpected finish result: {result:?}"
        );
        assert_eq!(store.status(&transfer_id).unwrap().next_offset, 0);
        assert!(!root.join("bad.bin").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn large_pushes_do_not_insert_excessive_durability_barriers() {
        let transfer_size = 128 * 1024 * 1024_u64;
        let intermediate_barriers = (transfer_size - 1) / CHECKPOINT_INTERVAL;
        assert_eq!(
            intermediate_barriers, 0,
            "routine 128 MiB pushes should rely on disconnect/final checkpoints"
        );
    }

    #[test]
    fn filesystem_pull_reads_committed_data_and_rejects_escape_paths() {
        let root = test_root("pull");
        fs::create_dir_all(root.join("assets")).unwrap();
        fs::write(root.join("assets/icon.bin"), b"kindlebridge").unwrap();
        let store = SyncStore::new(&root);
        let mut pull = store.begin_pull(None, "assets/icon.bin", 0).unwrap();
        let mut output = Vec::new();
        let mut buffer = [0_u8; 4];
        loop {
            let read = pull.read_chunk(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..read]);
        }
        assert_eq!(output, b"kindlebridge");
        assert_eq!(pull.finish().unwrap().state, TransferState::Complete);
        assert!(matches!(
            store.begin_pull(None, "../outside", 0),
            Err(StoreError::Path(PathError::InvalidComponent))
        ));
        fs::remove_dir_all(root).unwrap();
    }
}
