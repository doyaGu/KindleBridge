use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::activation::{ActivationGeneration, GenerationId};
use crate::cbor::{from_canonical_slice, to_canonical_vec};
use crate::error::{Error, ErrorCode, Result};
use crate::model::Digest;
use crate::BLOCK_SIZE;

const MAX_ACTIVATION_BYTES: u64 = 16 * 1024 * 1024;
const MAX_JOURNAL_BYTES: u64 = 64 * 1024;
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockStatus {
    Missing,
    Valid { size: u64 },
    Corrupt { size: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOutcome {
    Committed,
    AlreadyActive,
    AlreadyCommitted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedGeneration {
    pub transaction_id: String,
    pub generation_id: GenerationId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryAction {
    RemovedOrphan { transaction_id: String },
    AbortedStaging { transaction_id: String },
    CompletedCommit { transaction_id: String },
    RecordedCommit { transaction_id: String },
    AlreadyCommitted { transaction_id: String },
    DiscardedConflict { transaction_id: String },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub actions: Vec<RecoveryAction>,
}

/// A block and activation store rooted entirely below a caller-selected directory.
///
/// `block_quota_bytes` applies to immutable block contents. Transaction journals and
/// activation metadata remain writable so recovery can always make progress.
#[derive(Debug)]
pub struct InstallStore {
    root: PathBuf,
    block_quota_bytes: u64,
    lock: Mutex<()>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum TransactionPhase {
    #[serde(rename = "staged")]
    Staged,
    #[serde(rename = "generation-durable")]
    GenerationDurable,
    #[serde(rename = "committed")]
    Committed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TransactionJournal {
    schema: u64,
    transaction_id: String,
    generation_id: GenerationId,
    base_generation: Option<GenerationId>,
    phase: TransactionPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    None,
    AfterGenerationRename,
    AfterJournalDurable,
    AfterPointerRename,
}

impl InstallStore {
    pub fn open(root: impl AsRef<Path>, block_quota_bytes: u64) -> Result<Self> {
        let requested = root.as_ref();
        if entry_exists(requested)? {
            if !fs::metadata(requested)?.is_dir() {
                return install_error(ErrorCode::Path, "install root is not a directory");
            }
        } else {
            fs::create_dir_all(requested)?;
        }
        let root = fs::canonicalize(requested)?;
        reject_reparse(&root)?;
        let store = Self {
            root,
            block_quota_bytes,
            lock: Mutex::new(()),
        };
        store.ensure_layout()?;
        Ok(store)
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn block_usage_bytes(&self) -> Result<u64> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        self.block_usage_unlocked()
    }

    pub fn block_status(&self, digest: Digest) -> Result<BlockStatus> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        self.block_status_unlocked(digest)
    }

    pub fn read_block(&self, digest: Digest) -> Result<Option<Vec<u8>>> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        match self.block_status_unlocked(digest)? {
            BlockStatus::Missing => Ok(None),
            BlockStatus::Corrupt { .. } => {
                install_error(ErrorCode::Block, "content-addressed block is corrupt")
            }
            BlockStatus::Valid { .. } => Ok(Some(read_owned_file(
                &self.block_path(digest),
                BLOCK_SIZE as u64,
            )?)),
        }
    }

    pub fn missing_blocks(&self, requested: &[Digest]) -> Result<Vec<Digest>> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        let mut unique = BTreeSet::new();
        for digest in requested {
            if !matches!(
                self.block_status_unlocked(*digest)?,
                BlockStatus::Valid { .. }
            ) {
                unique.insert(*digest);
            }
        }
        Ok(unique.into_iter().collect())
    }

    pub fn put_block(&self, expected: Digest, bytes: &[u8]) -> Result<bool> {
        if bytes.is_empty() || bytes.len() > BLOCK_SIZE {
            return install_error(ErrorCode::Block, "block must contain 1..=65,536 bytes");
        }
        if Digest::of(bytes) != expected {
            return install_error(ErrorCode::Block, "block does not match its BLAKE3 address");
        }
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        match self.block_status_unlocked(expected)? {
            BlockStatus::Valid { .. } => return Ok(false),
            BlockStatus::Corrupt { .. } => {
                return install_error(
                    ErrorCode::Block,
                    "refusing to overwrite a corrupt content-addressed block",
                );
            }
            BlockStatus::Missing => {}
        }
        let usage = self.block_usage_unlocked()?;
        let new_usage = usage
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| Error::new(ErrorCode::Bounds, "block length exceeds uint64"))?,
            )
            .ok_or_else(|| Error::new(ErrorCode::Bounds, "block quota arithmetic overflow"))?;
        if new_usage > self.block_quota_bytes {
            return install_error(ErrorCode::Quota, "block store quota exceeded");
        }

        let path = self.block_path(expected);
        self.validate_owned_path(&path)?;
        let parent = path.parent().expect("block path has parent");
        ensure_directory(parent)?;
        match atomic_write(&path, bytes, false) {
            Ok(()) => Ok(true),
            Err(error) if entry_exists(&path)? => match self.block_status_unlocked(expected)? {
                BlockStatus::Valid { .. } => Ok(false),
                _ => Err(error),
            },
            Err(error) => Err(error),
        }
    }

    pub fn stage_generation(
        &self,
        transaction_id: &str,
        generation: &ActivationGeneration,
    ) -> Result<StagedGeneration> {
        validate_transaction_id(transaction_id)?;
        generation.validate()?;
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        if self.active_generation_id_unlocked()? != generation.previous_generation {
            return install_error(
                ErrorCode::Conflict,
                "activation base changed before generation staging",
            );
        }

        let transaction_dir = self.transaction_dir(transaction_id);
        self.validate_owned_path(&transaction_dir)?;
        if entry_exists(&transaction_dir)? {
            let journal = self.read_journal(transaction_id)?;
            if journal.generation_id == generation.generation_id
                && journal.base_generation == generation.previous_generation
            {
                let staged = transaction_dir.join("staged-generation");
                let existing = if entry_exists(&staged)? {
                    read_activation(&staged.join("activation.cbor"))?
                } else {
                    self.load_generation_unlocked(generation.generation_id)?
                };
                if existing != *generation {
                    return install_error(
                        ErrorCode::Conflict,
                        "transaction retry changed immutable generation content",
                    );
                }
                return Ok(StagedGeneration {
                    transaction_id: transaction_id.into(),
                    generation_id: generation.generation_id,
                });
            }
            return install_error(
                ErrorCode::Conflict,
                "transaction ID already stages different content",
            );
        }

        let generation_dir = self.generation_dir(generation.generation_id);
        self.validate_owned_path(&generation_dir)?;
        if entry_exists(&generation_dir)? {
            return install_error(
                ErrorCode::Conflict,
                "generation ID already names immutable content",
            );
        }

        fs::create_dir(&transaction_dir)?;
        sync_dir(self.transactions_dir())?;
        let staged_dir = transaction_dir.join("staged-generation");
        fs::create_dir(&staged_dir)?;
        let activation = generation.to_cbor()?;
        atomic_write(&staged_dir.join("activation.cbor"), &activation, false)?;
        sync_dir(&staged_dir)?;
        let journal = TransactionJournal {
            schema: 1,
            transaction_id: transaction_id.into(),
            generation_id: generation.generation_id,
            base_generation: generation.previous_generation,
            phase: TransactionPhase::Staged,
        };
        self.write_journal(&journal)?;
        sync_dir(&transaction_dir)?;
        Ok(StagedGeneration {
            transaction_id: transaction_id.into(),
            generation_id: generation.generation_id,
        })
    }

    pub fn commit_generation(&self, transaction_id: &str) -> Result<CommitOutcome> {
        validate_transaction_id(transaction_id)?;
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        self.commit_generation_unlocked(transaction_id, CrashPoint::None)
    }

    pub fn active_generation_id(&self) -> Result<Option<GenerationId>> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        self.active_generation_id_unlocked()
    }

    pub fn load_generation(&self, generation_id: GenerationId) -> Result<ActivationGeneration> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        self.load_generation_unlocked(generation_id)
    }

    pub fn select_rollback(&self) -> Result<Option<ActivationGeneration>> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        let Some(active) = self.active_generation_id_unlocked()? else {
            return Ok(None);
        };
        let current = self.load_generation_unlocked(active)?;
        current
            .previous_generation
            .map(|previous| self.load_generation_unlocked(previous))
            .transpose()
    }

    pub fn activate_existing(
        &self,
        target: GenerationId,
        expected_current: GenerationId,
    ) -> Result<()> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        self.load_generation_unlocked(target)?;
        if self.active_generation_id_unlocked()? != Some(expected_current) {
            return install_error(
                ErrorCode::Conflict,
                "active generation changed before rollback",
            );
        }
        self.write_active_pointer(target)
    }

    pub fn recover(&self) -> Result<RecoveryReport> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| Error::new(ErrorCode::Recovery, "install store lock is poisoned"))?;
        self.validate_owned_path(&self.transactions_dir())?;
        self.validate_owned_path(&self.generations_dir())?;
        let mut transaction_ids = Vec::new();
        for entry in fs::read_dir(self.transactions_dir())? {
            let entry = entry?;
            reject_reparse(&entry.path())?;
            if !entry.file_type()?.is_dir() {
                return install_error(
                    ErrorCode::Recovery,
                    "unexpected non-directory in transactions store",
                );
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::new(ErrorCode::Recovery, "non-UTF-8 transaction name"))?;
            validate_transaction_id(&name)?;
            transaction_ids.push(name);
        }
        transaction_ids.sort();

        let mut report = RecoveryReport::default();
        for transaction_id in transaction_ids {
            let transaction_dir = self.transaction_dir(&transaction_id);
            let journal_path = transaction_dir.join("journal.cbor");
            if !entry_exists(&journal_path)? {
                safe_remove_tree(&transaction_dir)?;
                report
                    .actions
                    .push(RecoveryAction::RemovedOrphan { transaction_id });
                continue;
            }
            let mut journal = self.read_journal(&transaction_id)?;
            if journal.phase == TransactionPhase::Committed {
                let staged = transaction_dir.join("staged-generation");
                if entry_exists(&staged)? {
                    safe_remove_tree(&staged)?;
                }
                report
                    .actions
                    .push(RecoveryAction::AlreadyCommitted { transaction_id });
                continue;
            }

            let staged = transaction_dir.join("staged-generation");
            let generation_dir = self.generation_dir(journal.generation_id);
            if !entry_exists(&generation_dir)? {
                if entry_exists(&staged)? {
                    safe_remove_tree(&transaction_dir)?;
                    report
                        .actions
                        .push(RecoveryAction::AbortedStaging { transaction_id });
                } else {
                    safe_remove_tree(&transaction_dir)?;
                    report
                        .actions
                        .push(RecoveryAction::RemovedOrphan { transaction_id });
                }
                continue;
            }

            let durable = self.load_generation_unlocked(journal.generation_id)?;
            if entry_exists(&staged)? {
                let staged_generation = read_activation(&staged.join("activation.cbor"))?;
                if staged_generation != durable
                    || staged_generation.generation_id != journal.generation_id
                    || staged_generation.previous_generation != journal.base_generation
                {
                    return install_error(
                        ErrorCode::Recovery,
                        "staged and durable generations disagree",
                    );
                }
            }
            let active = self.active_generation_id_unlocked()?;
            if active == Some(journal.generation_id) {
                journal.phase = TransactionPhase::Committed;
                self.write_journal(&journal)?;
                report
                    .actions
                    .push(RecoveryAction::RecordedCommit { transaction_id });
            } else if active == journal.base_generation {
                self.write_active_pointer(journal.generation_id)?;
                journal.phase = TransactionPhase::Committed;
                self.write_journal(&journal)?;
                report
                    .actions
                    .push(RecoveryAction::CompletedCommit { transaction_id });
            } else {
                safe_remove_tree(&transaction_dir)?;
                report
                    .actions
                    .push(RecoveryAction::DiscardedConflict { transaction_id });
            }
        }
        Ok(report)
    }

    fn commit_generation_unlocked(
        &self,
        transaction_id: &str,
        crash_point: CrashPoint,
    ) -> Result<CommitOutcome> {
        let mut journal = self.read_journal(transaction_id)?;
        if journal.phase == TransactionPhase::Committed {
            return Ok(CommitOutcome::AlreadyCommitted);
        }
        let staged_dir = self
            .transaction_dir(transaction_id)
            .join("staged-generation");
        let generation_dir = self.generation_dir(journal.generation_id);
        if entry_exists(&generation_dir)? {
            let durable_generation = self.load_generation_unlocked(journal.generation_id)?;
            if entry_exists(&staged_dir)? {
                let staged_generation = read_activation(&staged_dir.join("activation.cbor"))?;
                if staged_generation != durable_generation
                    || staged_generation.generation_id != journal.generation_id
                    || staged_generation.previous_generation != journal.base_generation
                {
                    return install_error(
                        ErrorCode::Conflict,
                        "generation ID collision with different immutable content",
                    );
                }
                safe_remove_tree(&staged_dir)?;
            }
        } else {
            if !entry_exists(&staged_dir)? {
                return install_error(
                    ErrorCode::Recovery,
                    "transaction has neither staged nor durable generation",
                );
            }
            let staged_generation = read_activation(&staged_dir.join("activation.cbor"))?;
            if staged_generation.generation_id != journal.generation_id
                || staged_generation.previous_generation != journal.base_generation
            {
                return install_error(
                    ErrorCode::Recovery,
                    "staged generation does not match its journal",
                );
            }
            fs::rename(&staged_dir, &generation_dir)?;
            sync_dir(self.generations_dir())?;
        }
        crash_if(crash_point, CrashPoint::AfterGenerationRename)?;

        journal.phase = TransactionPhase::GenerationDurable;
        self.write_journal(&journal)?;
        crash_if(crash_point, CrashPoint::AfterJournalDurable)?;

        let active = self.active_generation_id_unlocked()?;
        if active == Some(journal.generation_id) {
            journal.phase = TransactionPhase::Committed;
            self.write_journal(&journal)?;
            return Ok(CommitOutcome::AlreadyActive);
        }
        if active != journal.base_generation {
            return install_error(
                ErrorCode::Conflict,
                "active generation changed during transaction",
            );
        }
        self.write_active_pointer(journal.generation_id)?;
        crash_if(crash_point, CrashPoint::AfterPointerRename)?;
        journal.phase = TransactionPhase::Committed;
        self.write_journal(&journal)?;
        Ok(CommitOutcome::Committed)
    }

    fn ensure_layout(&self) -> Result<()> {
        ensure_directory(&self.root)?;
        ensure_directory(&self.blocks_dir())?;
        ensure_directory(&self.transactions_dir())?;
        ensure_directory(&self.generations_dir())?;
        sync_dir(&self.root)
    }

    fn block_status_unlocked(&self, digest: Digest) -> Result<BlockStatus> {
        let path = self.block_path(digest);
        self.validate_owned_path(&path)?;
        if !entry_exists(&path)? {
            return Ok(BlockStatus::Missing);
        }
        reject_reparse(&path)?;
        let metadata = fs::metadata(&path)?;
        if !metadata.is_file() {
            return install_error(ErrorCode::Path, "block path is not a regular file");
        }
        let size = metadata.len();
        if size == 0 || size > BLOCK_SIZE as u64 {
            return Ok(BlockStatus::Corrupt { size });
        }
        let bytes = read_owned_file(&path, BLOCK_SIZE as u64)?;
        if Digest::of(&bytes) == digest {
            Ok(BlockStatus::Valid { size })
        } else {
            Ok(BlockStatus::Corrupt { size })
        }
    }

    fn block_usage_unlocked(&self) -> Result<u64> {
        self.validate_owned_path(&self.blocks_dir())?;
        directory_file_bytes(&self.blocks_dir())
    }

    fn active_generation_id_unlocked(&self) -> Result<Option<GenerationId>> {
        let path = self.active_pointer_path();
        self.validate_owned_path(&path)?;
        if !entry_exists(&path)? {
            return Ok(None);
        }
        let bytes = read_owned_file(&path, 64)?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| Error::new(ErrorCode::Activation, "active pointer is not UTF-8"))?;
        let text = text.strip_suffix('\n').unwrap_or(text);
        Ok(Some(parse_generation_id(text)?))
    }

    fn load_generation_unlocked(
        &self,
        generation_id: GenerationId,
    ) -> Result<ActivationGeneration> {
        let directory = self.generation_dir(generation_id);
        self.validate_owned_path(&directory)?;
        reject_reparse(&directory)?;
        if !fs::metadata(&directory)?.is_dir() {
            return install_error(ErrorCode::Activation, "generation path is not a directory");
        }
        validate_generation_directory(&directory)?;
        let generation = read_activation(&directory.join("activation.cbor"))?;
        if generation.generation_id != generation_id
            || generation.directory_name() != generation_id_hex(generation_id)
        {
            return install_error(
                ErrorCode::Activation,
                "generation directory does not match activation content",
            );
        }
        Ok(generation)
    }

    fn write_active_pointer(&self, generation_id: GenerationId) -> Result<()> {
        self.validate_owned_path(&self.active_pointer_path())?;
        let mut bytes = generation_id_hex(generation_id).into_bytes();
        bytes.push(b'\n');
        atomic_write(&self.active_pointer_path(), &bytes, true)
    }

    fn read_journal(&self, transaction_id: &str) -> Result<TransactionJournal> {
        self.validate_owned_path(&self.transaction_dir(transaction_id))?;
        let bytes = read_owned_file(
            &self.transaction_dir(transaction_id).join("journal.cbor"),
            MAX_JOURNAL_BYTES,
        )?;
        let journal: TransactionJournal = from_canonical_slice(&bytes)?;
        if journal.schema != 1 || journal.transaction_id != transaction_id {
            return install_error(ErrorCode::Recovery, "transaction journal identity mismatch");
        }
        Ok(journal)
    }

    fn write_journal(&self, journal: &TransactionJournal) -> Result<()> {
        self.validate_owned_path(&self.transaction_dir(&journal.transaction_id))?;
        let bytes = to_canonical_vec(journal)?;
        atomic_write(
            &self
                .transaction_dir(&journal.transaction_id)
                .join("journal.cbor"),
            &bytes,
            true,
        )
    }

    fn blocks_dir(&self) -> PathBuf {
        self.root.join("blocks")
    }

    fn transactions_dir(&self) -> PathBuf {
        self.root.join("transactions")
    }

    fn generations_dir(&self) -> PathBuf {
        self.root.join("generations")
    }

    fn active_pointer_path(&self) -> PathBuf {
        self.root.join("active-generation")
    }

    fn transaction_dir(&self, transaction_id: &str) -> PathBuf {
        self.transactions_dir().join(transaction_id)
    }

    fn generation_dir(&self, generation_id: GenerationId) -> PathBuf {
        self.generations_dir()
            .join(generation_id_hex(generation_id))
    }

    fn block_path(&self, digest: Digest) -> PathBuf {
        let hex = digest_hex(digest);
        self.blocks_dir()
            .join(&hex[..2])
            .join(format!("{hex}.block"))
    }

    fn validate_owned_path(&self, path: &Path) -> Result<()> {
        let relative = path.strip_prefix(&self.root).map_err(|_| {
            Error::new(
                ErrorCode::Path,
                "install operation escaped the caller-provided root",
            )
        })?;
        let mut current = self.root.clone();
        reject_reparse(&current)?;
        for component in relative.components() {
            current.push(component);
            let _ = entry_exists(&current)?;
        }
        Ok(())
    }
}

fn read_activation(path: &Path) -> Result<ActivationGeneration> {
    let bytes = read_owned_file(path, MAX_ACTIVATION_BYTES)?;
    ActivationGeneration::from_cbor(&bytes)
}

fn validate_generation_directory(path: &Path) -> Result<()> {
    let mut activation_seen = false;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        reject_reparse(&entry.path())?;
        if entry.file_name() != "activation.cbor" || !entry.file_type()?.is_file() {
            return install_error(
                ErrorCode::Activation,
                "immutable generation contains an unexpected entry",
            );
        }
        if activation_seen {
            return install_error(ErrorCode::Activation, "duplicate activation metadata");
        }
        activation_seen = true;
    }
    if !activation_seen {
        return install_error(
            ErrorCode::Activation,
            "generation has no activation metadata",
        );
    }
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::new(ErrorCode::Path, "atomic-write path has no parent"))?;
    reject_reparse(parent)?;
    if !fs::metadata(parent)?.is_dir() {
        return install_error(ErrorCode::Path, "atomic-write parent is not a directory");
    }
    if entry_exists(path)? && !replace {
        return install_error(ErrorCode::Conflict, "atomic-write destination exists");
    }
    let temp_path = unique_temp_path(parent)?;
    let write_result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, path)?;
        sync_dir(parent)
    })();
    if write_result.is_err() && entry_exists(&temp_path).unwrap_or(false) {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

fn unique_temp_path(parent: &Path) -> Result<PathBuf> {
    for _ in 0..128 {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(".kbb-tmp-{}-{counter:016x}", std::process::id()));
        if !entry_exists(&candidate)? {
            return Ok(candidate);
        }
    }
    install_error(
        ErrorCode::Io,
        "unable to allocate a unique temporary filename",
    )
}

fn ensure_directory(path: &Path) -> Result<()> {
    if entry_exists(path)? {
        if !fs::metadata(path)?.is_dir() {
            return install_error(ErrorCode::Path, "owned path is not a directory");
        }
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| Error::new(ErrorCode::Path, "directory has no parent"))?;
    reject_reparse(parent)?;
    fs::create_dir(path)?;
    sync_dir(parent)
}

fn read_owned_file(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    reject_reparse(path)?;
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > maximum {
        return install_error(ErrorCode::Bounds, "owned file type or size is invalid");
    }
    let mut file = File::open(path)?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .map_err(|_| Error::new(ErrorCode::Bounds, "file size exceeds host usize"))?,
    );
    file.read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() {
        return install_error(ErrorCode::Io, "owned file changed while being read");
    }
    Ok(bytes)
}

fn directory_file_bytes(path: &Path) -> Result<u64> {
    reject_reparse(path)?;
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        reject_reparse(&child)?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            total = total
                .checked_add(directory_file_bytes(&child)?)
                .ok_or_else(|| Error::new(ErrorCode::Bounds, "usage count overflow"))?;
        } else if file_type.is_file() {
            total = total
                .checked_add(entry.metadata()?.len())
                .ok_or_else(|| Error::new(ErrorCode::Bounds, "usage count overflow"))?;
        } else {
            return install_error(ErrorCode::Path, "special file in owned directory");
        }
    }
    Ok(total)
}

fn safe_remove_tree(path: &Path) -> Result<()> {
    reject_reparse(path)?;
    if !fs::metadata(path)?.is_dir() {
        return install_error(
            ErrorCode::Path,
            "refusing to recursively remove a non-directory",
        );
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        reject_reparse(&child)?;
        if entry.file_type()?.is_dir() {
            safe_remove_tree(&child)?;
        } else if entry.file_type()?.is_file() {
            fs::remove_file(&child)?;
        } else {
            return install_error(ErrorCode::Path, "refusing to remove a special file");
        }
    }
    fs::remove_dir(path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn reject_reparse(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || platform_is_reparse(&metadata) {
        return install_error(
            ErrorCode::Path,
            "symlink/reparse point is forbidden in install root",
        );
    }
    Ok(())
}

fn entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || platform_is_reparse(&metadata) {
                return install_error(
                    ErrorCode::Path,
                    "symlink/reparse point is forbidden in install root",
                );
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
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
    // Windows does not expose a safe std API for opening a directory handle. Every
    // file is still sync_all'd before rename; Kindle's production target uses Unix.
    Ok(())
}

fn validate_transaction_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return install_error(ErrorCode::Path, "invalid transaction ID");
    }
    Ok(())
}

fn digest_hex(digest: Digest) -> String {
    bytes_hex(&digest.0)
}

fn generation_id_hex(id: GenerationId) -> String {
    bytes_hex(&id.0)
}

fn bytes_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn parse_generation_id(value: &str) -> Result<GenerationId> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return install_error(ErrorCode::Activation, "invalid active generation ID");
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).expect("ASCII hex");
        bytes[index] = u8::from_str_radix(pair, 16)
            .map_err(|_| Error::new(ErrorCode::Activation, "invalid generation ID hex"))?;
    }
    Ok(GenerationId(bytes))
}

fn crash_if(actual: CrashPoint, expected: CrashPoint) -> Result<()> {
    if actual == expected {
        return install_error(ErrorCode::Recovery, "simulated commit crash point");
    }
    Ok(())
}

fn install_error<T>(code: ErrorCode, message: impl Into<String>) -> Result<T> {
    Err(Error::new(code, message))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::{ActivationEntry, BundleKind};

    static TEST_DIRECTORY_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let counter = TEST_DIRECTORY_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "kindlebridge-kbb-{label}-{}-{counter:016x}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn generation(id_byte: u8, previous: Option<GenerationId>) -> ActivationGeneration {
        ActivationGeneration {
            schema: 1,
            generation_id: GenerationId([id_byte; 16]),
            previous_generation: previous,
            profile_id: "kt6-5.17".into(),
            profile_digest: Digest::of(b"profile"),
            entries: vec![ActivationEntry {
                id: "org.example.reader".into(),
                channel: "dev".into(),
                kind: BundleKind::Application,
                bundle_root: Digest::of(&[id_byte]),
                code_version: format!("{id_byte}-abcdef12"),
                data_generation: None,
                dependency_roots: Vec::new(),
            }],
        }
    }

    #[test]
    fn block_store_deduplicates_reports_missing_and_detects_corruption() {
        let directory = TestDirectory::new("blocks");
        let store = InstallStore::open(&directory.0, 1024).unwrap();
        let bytes = b"immutable block";
        let digest = Digest::of(bytes);
        assert!(store.put_block(digest, bytes).unwrap());
        assert!(!store.put_block(digest, bytes).unwrap());
        assert_eq!(
            store.block_status(digest).unwrap(),
            BlockStatus::Valid {
                size: bytes.len() as u64
            }
        );
        assert_eq!(store.read_block(digest).unwrap().unwrap(), bytes);
        assert!(store
            .missing_blocks(&[digest, Digest::of(b"missing")])
            .unwrap()
            .contains(&Digest::of(b"missing")));

        fs::write(store.block_path(digest), b"corrupt").unwrap();
        assert_eq!(
            store.block_status(digest).unwrap(),
            BlockStatus::Corrupt { size: 7 }
        );
        assert_eq!(store.read_block(digest).unwrap_err().code, ErrorCode::Block);
    }

    #[test]
    fn block_quota_fails_before_creating_content() {
        let directory = TestDirectory::new("quota");
        let store = InstallStore::open(&directory.0, 2).unwrap();
        let bytes = b"three";
        assert_eq!(
            store.put_block(Digest::of(bytes), bytes).unwrap_err().code,
            ErrorCode::Quota
        );
        assert_eq!(store.block_usage_bytes().unwrap(), 0);
    }

    #[test]
    fn staged_transaction_is_aborted_without_pointer_change() {
        let directory = TestDirectory::new("staging-recovery");
        let store = InstallStore::open(&directory.0, u64::MAX).unwrap();
        store
            .stage_generation("tx-staged", &generation(1, None))
            .unwrap();
        let first = store.recover().unwrap();
        assert_eq!(
            first.actions,
            vec![RecoveryAction::AbortedStaging {
                transaction_id: "tx-staged".into()
            }]
        );
        assert_eq!(store.active_generation_id().unwrap(), None);
        assert!(store.recover().unwrap().actions.is_empty());
    }

    #[test]
    fn every_durable_commit_crash_point_recovers_idempotently() {
        for (index, crash_point) in [
            CrashPoint::AfterGenerationRename,
            CrashPoint::AfterJournalDurable,
            CrashPoint::AfterPointerRename,
        ]
        .into_iter()
        .enumerate()
        {
            let directory = TestDirectory::new(&format!("crash-{index}"));
            let store = InstallStore::open(&directory.0, u64::MAX).unwrap();
            let generation = generation(u8::try_from(index + 1).unwrap(), None);
            let transaction = format!("tx-crash-{index}");
            store.stage_generation(&transaction, &generation).unwrap();
            {
                let _guard = store.lock.lock().unwrap();
                assert_eq!(
                    store
                        .commit_generation_unlocked(&transaction, crash_point)
                        .unwrap_err()
                        .code,
                    ErrorCode::Recovery
                );
            }
            let reopened = InstallStore::open(&directory.0, u64::MAX).unwrap();
            reopened.recover().unwrap();
            assert_eq!(
                reopened.active_generation_id().unwrap(),
                Some(generation.generation_id)
            );
            assert_eq!(
                reopened.load_generation(generation.generation_id).unwrap(),
                generation
            );
            let active_before = reopened.active_generation_id().unwrap();
            reopened.recover().unwrap();
            assert_eq!(reopened.active_generation_id().unwrap(), active_before);
        }
    }

    #[test]
    fn commits_two_generations_and_rolls_back_with_compare_and_swap() {
        let directory = TestDirectory::new("rollback");
        let store = InstallStore::open(&directory.0, u64::MAX).unwrap();
        let first = generation(1, None);
        store.stage_generation("tx-one", &first).unwrap();
        assert_eq!(
            store.commit_generation("tx-one").unwrap(),
            CommitOutcome::Committed
        );

        let second = generation(2, Some(first.generation_id));
        store.stage_generation("tx-two", &second).unwrap();
        assert_eq!(
            store.commit_generation("tx-two").unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(store.select_rollback().unwrap(), Some(first.clone()));
        store
            .activate_existing(first.generation_id, second.generation_id)
            .unwrap();
        assert_eq!(
            store.active_generation_id().unwrap(),
            Some(first.generation_id)
        );
        assert_eq!(
            store
                .activate_existing(second.generation_id, second.generation_id)
                .unwrap_err()
                .code,
            ErrorCode::Conflict
        );

        let mut collision = generation(1, Some(first.generation_id));
        collision.entries[0].code_version = "different-content".into();
        assert_eq!(
            store
                .stage_generation("tx-collision", &collision)
                .unwrap_err()
                .code,
            ErrorCode::Conflict
        );
    }

    #[test]
    fn rejects_path_escape_transaction_ids() {
        let directory = TestDirectory::new("escape");
        let store = InstallStore::open(&directory.0, u64::MAX).unwrap();
        for transaction in ["../escape", "a/b", "a\\b", ""] {
            assert_eq!(
                store
                    .stage_generation(transaction, &generation(1, None))
                    .unwrap_err()
                    .code,
                ErrorCode::Path
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_inside_owned_store() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new("symlink");
        let outside = TestDirectory::new("outside");
        let store = InstallStore::open(&directory.0, u64::MAX).unwrap();
        let bytes = b"linked";
        let digest = Digest::of(bytes);
        let path = store.block_path(digest);
        ensure_directory(path.parent().unwrap()).unwrap();
        symlink(&outside.0, &path).unwrap();
        assert_eq!(
            store.block_status(digest).unwrap_err().code,
            ErrorCode::Path
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_in_ancestor_component() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new("ancestor-symlink");
        let outside = TestDirectory::new("ancestor-outside");
        let store = InstallStore::open(&directory.0, u64::MAX).unwrap();
        let digest = Digest::of(b"ancestor");
        let path = store.block_path(digest);
        symlink(&outside.0, path.parent().unwrap()).unwrap();
        assert_eq!(
            store.block_status(digest).unwrap_err().code,
            ErrorCode::Path
        );
    }
}
