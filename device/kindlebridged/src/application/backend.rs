//! Application-domain implementation behind `ApplicationManager`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use kindlebridge_bundle::{
    ingest_verified_blocks, load_materialized_application, materialize_verified_application,
    verify, ActivationAction, ActivationEntry, ActivationGeneration, BundleKind, Digest,
    Error as BundleError, GenerationId, InstallStore, MaterializedApplication, VerifyOptions,
};
use kindlebridge_schema::{
    error_codes, AppList, AppLogChunk, AppLogParams, AppLogSnapshot, AppState, AppSummary, RpcError,
};

use crate::app::{AppSupervisor, RuntimeError, RuntimeStatus};

const MAX_ACTIVATION_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ACTIVATION_HISTORY: usize = 4096;
const APP_BLOCK_QUOTA_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_APP_STOP_TIMEOUT_MS: u64 = 3_000;
const MAX_APP_LOG_READ: u32 = 64 * 1024;
const MAX_APP_LOG_BYTES: u64 = 4 * 1024 * 1024;

pub fn app_install(
    bundle: &mut File,
    expected_file_hash: &str,
    activation_root: &Path,
    target: &str,
    firmware: &str,
    available_features: &[&str],
    supervisor: &AppSupervisor,
) -> Result<AppSummary, RpcError> {
    validate_file_hash(expected_file_hash)?;
    let actual_file_hash = hash_open_file(bundle)?;
    if actual_file_hash != expected_file_hash {
        return Err(
            RpcError::new(error_codes::CHECKSUM_MISMATCH, "Bundle checksum mismatch").with_data(
                serde_json::json!({
                    "expected": expected_file_hash,
                    "actual": actual_file_hash,
                }),
            ),
        );
    }

    bundle
        .seek(SeekFrom::Start(0))
        .map_err(|error| app_install_io("seek_bundle", &error))?;
    let verified = verify(
        bundle,
        &VerifyOptions {
            expected_publisher: None,
            target: Some(target),
            firmware: Some(firmware),
        },
    )
    .map_err(|error| app_install_bundle_error("verify", &error))?;
    let envelope = &verified.inspection.envelope;
    if envelope.kind != BundleKind::Application {
        return Err(app_install_failure(
            "verify",
            "bundle_kind",
            "app install accepts application bundles only",
        ));
    }
    if let Some(feature) = envelope.variants[0]
        .required_features
        .iter()
        .find(|feature| !available_features.contains(&feature.as_str()))
    {
        return Err(app_install_failure(
            "compatibility",
            "required_feature",
            &format!("device does not provide required feature {feature}"),
        ));
    }

    let store = InstallStore::open(activation_root, APP_BLOCK_QUOTA_BYTES)
        .map_err(|error| app_install_bundle_error("open_store", &error))?;
    store
        .recover()
        .map_err(|error| app_install_bundle_error("recover", &error))?;
    let active_id = store
        .active_generation_id()
        .map_err(|error| app_install_bundle_error("read_activation", &error))?;
    let active = active_id
        .map(|id| store.load_generation(id))
        .transpose()
        .map_err(|error| app_install_bundle_error("read_activation", &error))?;
    let already_active = active.as_ref().is_some_and(|generation| {
        generation.entries.iter().any(|entry| {
            entry.kind == BundleKind::Application
                && entry.id == envelope.id
                && entry.channel == envelope.channel
                && entry.code_version == envelope.version
                && entry.bundle_root == verified.inspection.header.bundle_root
        })
    });

    bundle
        .seek(SeekFrom::Start(0))
        .map_err(|error| app_install_io("seek_bundle", &error))?;
    ingest_verified_blocks(bundle, &verified, &store)
        .map_err(|error| app_install_bundle_error("ingest_blocks", &error))?;
    materialize_verified_application(&verified, &store)
        .map_err(|error| app_install_bundle_error("materialize", &error))?;

    if already_active {
        return app_summary_from_generation(
            activation_root,
            active.as_ref().expect("checked above"),
            &envelope.id,
            supervisor,
        );
    }

    let mut entries = active
        .as_ref()
        .map_or_else(Vec::new, |generation| generation.entries.clone());
    entries.retain(|entry| !(entry.kind == BundleKind::Application && entry.id == envelope.id));
    entries.push(ActivationEntry {
        id: envelope.id.clone(),
        channel: envelope.channel.clone(),
        kind: BundleKind::Application,
        bundle_root: verified.inspection.header.bundle_root,
        code_version: envelope.version.clone(),
        data_generation: None,
        dependency_roots: Vec::new(),
    });
    entries.sort_by(|left, right| {
        (&left.id, &left.channel, left.kind.as_str()).cmp(&(
            &right.id,
            &right.channel,
            right.kind.as_str(),
        ))
    });
    let profile_digest = device_profile_digest(target, firmware, available_features);
    let generation = ActivationGeneration::new(active_id, target, profile_digest, entries)
        .map_err(|error| app_install_bundle_error("build_activation", &error))?;
    let transaction_id = format!("app-{}", generation.directory_name());
    store
        .stage_generation(&transaction_id, &generation)
        .map_err(|error| app_install_bundle_error("stage_activation", &error))?;
    let stop_timeout = active
        .as_ref()
        .and_then(|generation| {
            generation
                .entries
                .iter()
                .find(|entry| entry.kind == BundleKind::Application && entry.id == envelope.id)
        })
        .and_then(|entry| load_materialized_application(&store, entry.bundle_root).ok())
        .map_or(3_000, |app| app.process.stop_timeout_ms);
    supervisor
        .stop(&envelope.id, Duration::from_millis(stop_timeout))
        .map_err(|error| app_runtime_error("stop_previous", &envelope.id, &error))?;
    store
        .commit_generation(&transaction_id)
        .map_err(|error| app_install_bundle_error("commit_activation", &error))?;
    app_summary_from_generation(activation_root, &generation, &envelope.id, supervisor)
}

fn validate_file_hash(value: &str) -> Result<(), RpcError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(RpcError::invalid_params(
            "file_hash must be a 64-character lowercase BLAKE3 digest",
        ))
    }
}

fn hash_open_file(file: &mut File) -> Result<String, RpcError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|error| app_install_io("seek_bundle", &error))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| app_install_io("hash_bundle", &error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn device_profile_digest(target: &str, firmware: &str, available_features: &[&str]) -> Digest {
    let mut features = available_features.to_vec();
    features.sort_unstable();
    features.dedup();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"KINDLEBRIDGE-DEVICE-PROFILE-V1\0");
    for component in std::iter::once(target)
        .chain(std::iter::once(firmware))
        .chain(features)
    {
        hasher.update(&(component.len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }
    Digest(*hasher.finalize().as_bytes())
}

fn app_summary_from_generation(
    activation_root: &Path,
    generation: &ActivationGeneration,
    app_id: &str,
    supervisor: &AppSupervisor,
) -> Result<AppSummary, RpcError> {
    let entry = generation
        .entries
        .iter()
        .find(|entry| entry.kind == BundleKind::Application && entry.id == app_id)
        .ok_or_else(|| {
            app_install_failure(
                "commit_activation",
                "internal_state",
                "committed activation does not contain the installed app",
            )
        })?;
    let (state, pid) = application_runtime_state(supervisor, entry)?;
    let store = InstallStore::open(activation_root, APP_BLOCK_QUOTA_BYTES)
        .map_err(|error| invalid_device_state(error.to_string()))?;
    Ok(AppSummary {
        app_id: entry.id.clone(),
        version: entry.code_version.clone(),
        state,
        rollback_available: find_rollback_entry(&store, generation, entry)?.is_some(),
        pid,
    })
}

fn app_install_bundle_error(stage: &str, error: &BundleError) -> RpcError {
    app_install_failure(stage, &format!("{:?}", error.code), &error.message)
}

fn app_install_io(stage: &str, error: &std::io::Error) -> RpcError {
    app_install_failure(stage, &format!("io_{:?}", error.kind()), &error.to_string())
}

fn app_install_failure(stage: &str, reason: &str, detail: &str) -> RpcError {
    RpcError::new(
        error_codes::APP_INSTALL_FAILED,
        "Application install failed",
    )
    .with_data(serde_json::json!({
        "stage": stage,
        "reason": reason,
        "detail": detail,
    }))
}

pub fn app_list(activation_root: &Path, supervisor: &AppSupervisor) -> Result<AppList, RpcError> {
    app_list_unlocked(activation_root, supervisor)
}

fn app_list_unlocked(
    activation_root: &Path,
    supervisor: &AppSupervisor,
) -> Result<AppList, RpcError> {
    let Some(active) = load_active_generation(activation_root)? else {
        return Ok(AppList { apps: Vec::new() });
    };
    let mut apps = BTreeMap::new();
    let store = InstallStore::open(activation_root, APP_BLOCK_QUOTA_BYTES)
        .map_err(|error| invalid_device_state(error.to_string()))?;
    for entry in active
        .entries
        .iter()
        .filter(|entry| entry.kind == BundleKind::Application)
    {
        load_materialized_application(&store, entry.bundle_root).map_err(|error| {
            invalid_device_state(format!(
                "application {} runtime image is unavailable or corrupt: {error}",
                entry.id
            ))
        })?;
        let (state, pid) = application_runtime_state(supervisor, entry)?;
        let summary = AppSummary {
            app_id: entry.id.clone(),
            version: entry.code_version.clone(),
            state,
            rollback_available: find_rollback_entry(&store, &active, entry)?.is_some(),
            pid,
        };
        if apps.insert(entry.id.clone(), summary).is_some() {
            return Err(invalid_device_state(
                "active generation contains multiple application channels for one app_id",
            ));
        }
    }
    Ok(AppList {
        apps: apps.into_values().collect(),
    })
}

pub fn app_start(
    activation_root: &Path,
    supervisor: &AppSupervisor,
    app_id: &str,
) -> Result<AppSummary, RpcError> {
    app_start_unlocked(activation_root, supervisor, app_id)
}

pub fn app_log(
    activation_root: &Path,
    supervisor: &AppSupervisor,
    params: &AppLogParams,
) -> Result<AppLogSnapshot, RpcError> {
    let (_active, entry) = active_application_entry(activation_root, &params.app_id)?;
    let (state, pid) = application_runtime_state(supervisor, &entry)?;
    let limit = params.max_bytes.unwrap_or(16 * 1024);
    if limit == 0 || limit > MAX_APP_LOG_READ {
        return Err(RpcError::invalid_params(
            "max_bytes must be between 1 and 65536",
        ));
    }
    let root = activation_root
        .join("data")
        .join(&params.app_id)
        .join(".kindlebridge-logs");
    let run_id = match fs::read_to_string(root.join("run-id")) {
        Ok(run_id) => run_id.trim().to_owned(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(AppLogSnapshot {
                app_id: params.app_id.clone(),
                run_id: "not-started".to_owned(),
                reset: params.run_id.as_deref() != Some("not-started"),
                state,
                pid,
                stdout: empty_app_log_chunk(),
                stderr: empty_app_log_chunk(),
            });
        }
        Err(error) => return Err(app_log_io("read_run_id", &error)),
    };
    if run_id.is_empty() {
        return Err(invalid_device_state("application log run ID is empty"));
    }
    let reset = params.run_id.as_deref() != Some(&run_id);
    let stdout_cursor = if reset { 0 } else { params.stdout_cursor };
    let stderr_cursor = if reset { 0 } else { params.stderr_cursor };
    Ok(AppLogSnapshot {
        app_id: params.app_id.clone(),
        run_id,
        reset,
        state,
        pid,
        stdout: read_app_log_chunk(&root.join("stdout.log"), stdout_cursor, limit)?,
        stderr: read_app_log_chunk(&root.join("stderr.log"), stderr_cursor, limit)?,
    })
}

fn read_app_log_chunk(path: &Path, cursor: u64, limit: u32) -> Result<AppLogChunk, RpcError> {
    let mut file = File::open(path).map_err(|error| app_log_io("open", &error))?;
    let length = file
        .metadata()
        .map_err(|error| app_log_io("stat", &error))?
        .len();
    let cursor = cursor.min(length);
    file.seek(SeekFrom::Start(cursor))
        .map_err(|error| app_log_io("seek", &error))?;
    let mut data = vec![0; limit as usize];
    let count = file
        .read(&mut data)
        .map_err(|error| app_log_io("read", &error))?;
    data.truncate(count);
    Ok(AppLogChunk {
        cursor,
        next_cursor: cursor + count as u64,
        data_base64: BASE64.encode(data),
        capped: length >= MAX_APP_LOG_BYTES,
    })
}

fn empty_app_log_chunk() -> AppLogChunk {
    AppLogChunk {
        cursor: 0,
        next_cursor: 0,
        data_base64: String::new(),
        capped: false,
    }
}

fn app_log_io(operation: &str, error: &std::io::Error) -> RpcError {
    RpcError::new(error_codes::INVALID_STATE, "Application log is unavailable").with_data(
        serde_json::json!({
            "operation": operation,
            "reason": format!("io_{:?}", error.kind()),
            "detail": error.to_string(),
        }),
    )
}

fn app_start_unlocked(
    activation_root: &Path,
    supervisor: &AppSupervisor,
    app_id: &str,
) -> Result<AppSummary, RpcError> {
    let (active, _entry, materialized) = active_materialized_application(activation_root, app_id)?;
    let pid = supervisor
        .start(&materialized, &activation_root.join("data"))
        .map_err(|error| app_runtime_error("start", app_id, &error))?;
    let mut summary = app_summary_from_generation(activation_root, &active, app_id, supervisor)?;
    summary.state = AppState::Running;
    summary.pid = Some(pid);
    Ok(summary)
}

pub fn app_stop(
    activation_root: &Path,
    supervisor: &AppSupervisor,
    app_id: &str,
) -> Result<AppSummary, RpcError> {
    let (active, _entry, materialized) = active_materialized_application(activation_root, app_id)?;
    supervisor
        .stop(
            app_id,
            Duration::from_millis(materialized.process.stop_timeout_ms),
        )
        .map_err(|error| app_runtime_error("stop", app_id, &error))?;
    app_summary_from_generation(activation_root, &active, app_id, supervisor)
}

pub fn app_restart(
    activation_root: &Path,
    supervisor: &AppSupervisor,
    app_id: &str,
) -> Result<AppSummary, RpcError> {
    let (_, _, materialized) = active_materialized_application(activation_root, app_id)?;
    supervisor
        .stop(
            app_id,
            Duration::from_millis(materialized.process.stop_timeout_ms),
        )
        .map_err(|error| app_runtime_error("restart_stop", app_id, &error))?;
    app_start_unlocked(activation_root, supervisor, app_id)
}

pub fn app_rollback(
    activation_root: &Path,
    supervisor: &AppSupervisor,
    app_id: &str,
) -> Result<AppSummary, RpcError> {
    let store = open_recovered_store(activation_root, "rollback", app_id)?;
    let active_id = store
        .active_generation_id()
        .map_err(|error| app_activation_error("rollback", app_id, &error))?
        .ok_or_else(|| app_not_found(app_id))?;
    let active = store
        .load_generation(active_id)
        .map_err(|error| app_activation_error("rollback", app_id, &error))?;
    let current = active
        .entries
        .iter()
        .find(|entry| entry.kind == BundleKind::Application && entry.id == app_id)
        .cloned()
        .ok_or_else(|| app_not_found(app_id))?;
    let rollback = find_rollback_entry(&store, &active, &current)?.ok_or_else(|| {
        RpcError::new(error_codes::NO_ROLLBACK_AVAILABLE, "No rollback available")
            .with_data(serde_json::json!({ "app_id": app_id }))
    })?;
    let replacement = rollback.entry;

    // Validate the complete target image before stopping the current process or
    // making any activation state visible.
    let replacement_image = load_materialized_application(&store, replacement.bundle_root)
        .map_err(|error| {
            invalid_device_state(format!(
                "rollback application image is unavailable or corrupt: {error}"
            ))
        })?;
    if replacement_image.app_id != replacement.id
        || replacement_image.version != replacement.code_version
    {
        return Err(invalid_device_state(
            "rollback application image does not match activation identity",
        ));
    }

    let mut entries = active.entries.clone();
    let target = entries
        .iter_mut()
        .find(|entry| entry.kind == BundleKind::Application && entry.id == app_id)
        .expect("current entry was found above");
    *target = replacement;
    entries.sort_by(|left, right| {
        (&left.id, &left.channel, left.kind.as_str()).cmp(&(
            &right.id,
            &right.channel,
            right.kind.as_str(),
        ))
    });
    let generation = ActivationGeneration::new_rollback(
        active_id,
        &active.profile_id,
        active.profile_digest,
        entries,
        app_id,
        rollback.next_generation,
    )
    .map_err(|error| app_activation_error("rollback", app_id, &error))?;
    stage_application_generation(&store, "rollback", app_id, &generation)?;

    let stop_timeout = load_materialized_application(&store, current.bundle_root)
        .map_or(DEFAULT_APP_STOP_TIMEOUT_MS, |app| {
            app.process.stop_timeout_ms
        });
    supervisor
        .stop(app_id, Duration::from_millis(stop_timeout))
        .map_err(|error| app_runtime_error("rollback_stop", app_id, &error))?;
    commit_application_generation(&store, "rollback", app_id, &generation)?;
    app_summary_from_generation(activation_root, &generation, app_id, supervisor)
}

pub fn app_uninstall(
    activation_root: &Path,
    supervisor: &AppSupervisor,
    app_id: &str,
) -> Result<AppSummary, RpcError> {
    let store = open_recovered_store(activation_root, "uninstall", app_id)?;
    let active_id = store
        .active_generation_id()
        .map_err(|error| app_activation_error("uninstall", app_id, &error))?
        .ok_or_else(|| app_not_found(app_id))?;
    let active = store
        .load_generation(active_id)
        .map_err(|error| app_activation_error("uninstall", app_id, &error))?;
    let removed = active
        .entries
        .iter()
        .find(|entry| entry.kind == BundleKind::Application && entry.id == app_id)
        .cloned()
        .ok_or_else(|| app_not_found(app_id))?;
    let entries = active
        .entries
        .iter()
        .filter(|entry| !(entry.kind == BundleKind::Application && entry.id == app_id))
        .cloned()
        .collect();
    let generation = ActivationGeneration::new(
        Some(active_id),
        &active.profile_id,
        active.profile_digest,
        entries,
    )
    .map_err(|error| app_activation_error("uninstall", app_id, &error))?;
    stage_application_generation(&store, "uninstall", app_id, &generation)?;

    let stop_timeout = load_materialized_application(&store, removed.bundle_root)
        .map_or(DEFAULT_APP_STOP_TIMEOUT_MS, |app| {
            app.process.stop_timeout_ms
        });
    supervisor
        .stop(app_id, Duration::from_millis(stop_timeout))
        .map_err(|error| app_runtime_error("uninstall_stop", app_id, &error))?;
    commit_application_generation(&store, "uninstall", app_id, &generation)?;
    Ok(AppSummary {
        app_id: removed.id,
        version: removed.code_version,
        state: AppState::Stopped,
        rollback_available: false,
        pid: None,
    })
}

fn open_recovered_store(
    activation_root: &Path,
    operation: &str,
    app_id: &str,
) -> Result<InstallStore, RpcError> {
    let store = InstallStore::open(activation_root, APP_BLOCK_QUOTA_BYTES)
        .map_err(|error| app_activation_error(operation, app_id, &error))?;
    store
        .recover()
        .map_err(|error| app_activation_error(operation, app_id, &error))?;
    Ok(store)
}

fn stage_application_generation(
    store: &InstallStore,
    operation: &str,
    app_id: &str,
    generation: &ActivationGeneration,
) -> Result<(), RpcError> {
    let transaction_id = format!("{operation}-{}", generation.directory_name());
    store
        .stage_generation(&transaction_id, generation)
        .map(|_| ())
        .map_err(|error| app_activation_error(operation, app_id, &error))
}

fn commit_application_generation(
    store: &InstallStore,
    operation: &str,
    app_id: &str,
    generation: &ActivationGeneration,
) -> Result<(), RpcError> {
    let transaction_id = format!("{operation}-{}", generation.directory_name());
    store
        .commit_generation(&transaction_id)
        .map(|_| ())
        .map_err(|error| app_activation_error(operation, app_id, &error))
}

fn app_activation_error(operation: &str, app_id: &str, error: &BundleError) -> RpcError {
    RpcError::new(error_codes::INVALID_STATE, "Application activation failed").with_data(
        serde_json::json!({
            "operation": operation,
            "app_id": app_id,
            "reason": format!("{:?}", error.code),
            "detail": error.message,
        }),
    )
}

fn find_rollback_entry(
    store: &InstallStore,
    active: &ActivationGeneration,
    current: &ActivationEntry,
) -> Result<Option<RollbackCandidate>, RpcError> {
    let mut generation_id = history_predecessor(active, &current.id);
    let mut visited = BTreeSet::new();
    for _ in 0..MAX_ACTIVATION_HISTORY {
        let Some(id) = generation_id else {
            return Ok(None);
        };
        if !visited.insert(id.0) {
            return Err(invalid_device_state("activation history contains a cycle"));
        }
        let generation = store
            .load_generation(id)
            .map_err(|error| invalid_device_state(error.to_string()))?;
        let next_generation = history_predecessor(&generation, &current.id);
        if let Some(candidate) = generation.entries.iter().find(|entry| {
            entry.kind == BundleKind::Application && entry.id == current.id && *entry != current
        }) {
            return Ok(Some(RollbackCandidate {
                entry: candidate.clone(),
                next_generation,
            }));
        }
        generation_id = next_generation;
    }
    Err(invalid_device_state(
        "activation history exceeds the device traversal limit",
    ))
}

struct RollbackCandidate {
    entry: ActivationEntry,
    next_generation: Option<GenerationId>,
}

fn history_predecessor(generation: &ActivationGeneration, app_id: &str) -> Option<GenerationId> {
    match &generation.action {
        Some(ActivationAction::Rollback {
            app_id: rolled_back,
            next_generation,
        }) if rolled_back == app_id => *next_generation,
        _ => generation.previous_generation,
    }
}

fn active_materialized_application(
    activation_root: &Path,
    app_id: &str,
) -> Result<
    (
        ActivationGeneration,
        ActivationEntry,
        MaterializedApplication,
    ),
    RpcError,
> {
    let (active, entry) = active_application_entry(activation_root, app_id)?;
    let store = InstallStore::open(activation_root, APP_BLOCK_QUOTA_BYTES)
        .map_err(|error| invalid_device_state(error.to_string()))?;
    let materialized =
        load_materialized_application(&store, entry.bundle_root).map_err(|error| {
            invalid_device_state(format!(
                "application image is unavailable; reinstall its KBB bundle: {error}"
            ))
        })?;
    if materialized.app_id != entry.id || materialized.version != entry.code_version {
        return Err(invalid_device_state(
            "materialized application identity does not match active activation",
        ));
    }
    Ok((active, entry, materialized))
}

fn active_application_entry(
    activation_root: &Path,
    app_id: &str,
) -> Result<(ActivationGeneration, ActivationEntry), RpcError> {
    let active = load_active_generation(activation_root)?.ok_or_else(|| app_not_found(app_id))?;
    let entry = active
        .entries
        .iter()
        .find(|entry| entry.kind == BundleKind::Application && entry.id == app_id)
        .cloned()
        .ok_or_else(|| app_not_found(app_id))?;
    Ok((active, entry))
}

fn application_runtime_state(
    supervisor: &AppSupervisor,
    entry: &ActivationEntry,
) -> Result<(AppState, Option<u32>), RpcError> {
    let status = supervisor
        .status(&entry.id, entry.bundle_root)
        .map_err(|error| app_runtime_error("status", &entry.id, &error))?;
    Ok(match status {
        RuntimeStatus::Stopped => (AppState::Stopped, None),
        RuntimeStatus::Running(pid) => (AppState::Running, Some(pid)),
        RuntimeStatus::Failed => (AppState::Failed, None),
    })
}

fn app_runtime_error(operation: &str, app_id: &str, error: &RuntimeError) -> RpcError {
    RpcError::new(
        error_codes::INVALID_STATE,
        "Application lifecycle operation failed",
    )
    .with_data(serde_json::json!({
        "operation": operation,
        "app_id": app_id,
        "detail": error.to_string(),
    }))
}

fn load_active_generation(root: &Path) -> Result<Option<ActivationGeneration>, RpcError> {
    let pointer = root.join("active-generation");
    let bytes = match read_bounded_regular_file(&pointer, 64)? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| invalid_device_state("active generation pointer is not UTF-8"))?;
    let value = text.strip_suffix('\n').unwrap_or(text);
    let id = parse_generation_id(value)?;
    load_generation(root, id).map(Some)
}

fn load_generation(root: &Path, id: GenerationId) -> Result<ActivationGeneration, RpcError> {
    let path = root
        .join("generations")
        .join(generation_id_hex(id))
        .join("activation.cbor");
    let bytes = read_bounded_regular_file(&path, MAX_ACTIVATION_BYTES)?
        .ok_or_else(|| invalid_device_state("activation generation is missing"))?;
    let generation = ActivationGeneration::from_cbor(&bytes)
        .map_err(|_| invalid_device_state("activation generation is invalid"))?;
    if generation.generation_id != id {
        return Err(invalid_device_state(
            "activation generation identity does not match its directory",
        ));
    }
    Ok(generation)
}

fn read_bounded_regular_file(path: &Path, limit: u64) -> Result<Option<Vec<u8>>, RpcError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(device_read_error("stat device state", path, &error)),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > limit {
        return Err(invalid_device_state(
            "device state file is unsafe or exceeds its size limit",
        ));
    }
    let mut file =
        File::open(path).map_err(|error| device_read_error("open device state", path, &error))?;
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| device_read_error("read device state", path, &error))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        return Err(invalid_device_state(
            "device state file is unsafe or exceeds its size limit",
        ));
    }
    Ok(Some(bytes))
}

fn parse_generation_id(value: &str) -> Result<GenerationId, RpcError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(invalid_device_state("active generation pointer is invalid"));
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair)
            .map_err(|_| invalid_device_state("active generation pointer is invalid"))?;
        bytes[index] = u8::from_str_radix(pair, 16)
            .map_err(|_| invalid_device_state("active generation pointer is invalid"))?;
    }
    Ok(GenerationId(bytes))
}

fn generation_id_hex(id: GenerationId) -> String {
    let mut output = String::with_capacity(32);
    for byte in id.0 {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn invalid_device_state(detail: impl Into<String>) -> RpcError {
    RpcError::new(error_codes::INVALID_STATE, "Invalid device state")
        .with_data(serde_json::json!({ "detail": detail.into() }))
}

fn app_not_found(app_id: &str) -> RpcError {
    RpcError::new(error_codes::APP_NOT_FOUND, "App not found")
        .with_data(serde_json::json!({ "app_id": app_id }))
}

fn device_read_error(operation: &str, path: &Path, error: &std::io::Error) -> RpcError {
    RpcError::new(error_codes::INTERNAL_ERROR, "Device read failed").with_data(serde_json::json!({
        "operation": operation,
        "path": PathBuf::from(path),
        "kind": format!("{:?}", error.kind()),
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use ed25519_dalek::SigningKey;
    use kindlebridge_bundle::{ActivationEntry, BuildConfig, BundleBuilder, Digest};

    use super::*;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "kindlebridge-services-{label}-{}-{id}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn generation(id: u8, previous: Option<GenerationId>, version: &str) -> ActivationGeneration {
        ActivationGeneration {
            schema: kindlebridge_bundle::ACTIVATION_SCHEMA_VERSION,
            generation_id: GenerationId([id; 16]),
            previous_generation: previous,
            profile_id: "kt6-5.17".to_owned(),
            profile_digest: Digest::of(b"profile"),
            entries: vec![ActivationEntry {
                id: "org.example.reader".to_owned(),
                channel: "dev".to_owned(),
                kind: BundleKind::Application,
                bundle_root: Digest::of(version.as_bytes()),
                code_version: version.to_owned(),
                data_generation: None,
                dependency_roots: Vec::new(),
            }],
            action: None,
        }
    }

    fn write_generation(root: &Path, generation: &ActivationGeneration) {
        let directory = root.join("generations").join(generation.directory_name());
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join("activation.cbor"),
            generation.to_cbor().unwrap(),
        )
        .unwrap();
    }

    fn application_bundle(app_id: &str, version: &str, release: u64) -> Vec<u8> {
        let mut config = BuildConfig::new(
            BundleKind::Application,
            app_id,
            version,
            release,
            "kindlehf",
        );
        config.firmware_min = Some(vec![5, 16, 3]);
        config.firmware_max_exclusive = Some(vec![6]);
        config.required_features = vec!["sync.v1".to_owned()];
        config.entrypoints = BTreeMap::from([("main".to_owned(), "bin/app".to_owned())]);
        let mut builder = BundleBuilder::new(config);
        builder
            .add_file(
                "bin/app",
                format!("#!/bin/sh\necho {version}\n").into_bytes(),
                true,
            )
            .unwrap();
        builder
            .build(&SigningKey::from_bytes(&[0x33_u8; 32]))
            .unwrap()
    }

    #[cfg(unix)]
    fn long_running_application_bundle(app_id: &str) -> Vec<u8> {
        long_running_application_bundle_version(app_id, "1.0.0", 1)
    }

    #[cfg(unix)]
    fn long_running_application_bundle_version(
        app_id: &str,
        version: &str,
        release: u64,
    ) -> Vec<u8> {
        let mut config = BuildConfig::new(
            BundleKind::Application,
            app_id,
            version,
            release,
            "kindlehf",
        );
        config.entrypoints = BTreeMap::from([("main".to_owned(), "bin/app".to_owned())]);
        config.process = Some(kindlebridge_bundle::ProcessPolicy {
            restart: kindlebridge_bundle::RestartPolicy::Never,
            stop_timeout_ms: 500,
            working_dir: None,
            environment: Some(BTreeMap::from([(
                "APP_TEST_VALUE".to_owned(),
                "ready".to_owned(),
            )])),
        });
        let mut builder = BundleBuilder::new(config);
        builder
            .add_file(
                "bin/app",
                b"#!/bin/sh\ntest \"$APP_TEST_VALUE\" = ready || exit 41\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n".to_vec(),
                true,
            )
            .unwrap();
        builder
            .build(&SigningKey::from_bytes(&[0x44_u8; 32]))
            .unwrap()
    }

    #[cfg(unix)]
    fn failing_application_bundle(app_id: &str) -> Vec<u8> {
        let mut config = BuildConfig::new(BundleKind::Application, app_id, "1.0.0", 1, "kindlehf");
        config.entrypoints = BTreeMap::from([("main".to_owned(), "bin/app".to_owned())]);
        let mut builder = BundleBuilder::new(config);
        builder
            .add_file("bin/app", b"#!/bin/sh\nexit 42\n".to_vec(), true)
            .unwrap();
        builder
            .build(&SigningKey::from_bytes(&[0x45_u8; 32]))
            .unwrap()
    }

    #[test]
    fn app_install_verifies_ingests_and_atomically_upgrades_a_real_bundle() {
        let directory = TestDirectory::new("app-install");
        let activation_root = directory.0.join("activations");
        let bundle_path = directory.0.join("app.kbb");
        let supervisor = AppSupervisor::new();
        let first_bytes = application_bundle("org.example.reader", "1.0.0", 1);
        fs::write(&bundle_path, &first_bytes).unwrap();
        let first_hash = blake3::hash(&first_bytes).to_hex().to_string();
        let mut first_file = File::open(&bundle_path).unwrap();
        let first = app_install(
            &mut first_file,
            &first_hash,
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1", "sync.v1"],
            &supervisor,
        )
        .unwrap();
        assert_eq!(first.app_id, "org.example.reader");
        assert_eq!(first.version, "1.0.0");
        assert_eq!(first.state, AppState::Stopped);
        assert!(!first.rollback_available);

        let store = InstallStore::open(&activation_root, APP_BLOCK_QUOTA_BYTES).unwrap();
        let first_generation = store.active_generation_id().unwrap();
        drop(store);
        let mut repeated_file = File::open(&bundle_path).unwrap();
        app_install(
            &mut repeated_file,
            &first_hash,
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1", "sync.v1"],
            &supervisor,
        )
        .unwrap();
        let store = InstallStore::open(&activation_root, APP_BLOCK_QUOTA_BYTES).unwrap();
        assert_eq!(store.active_generation_id().unwrap(), first_generation);
        drop(store);

        let second_bytes = application_bundle("org.example.reader", "2.0.0", 2);
        fs::write(&bundle_path, &second_bytes).unwrap();
        let second_hash = blake3::hash(&second_bytes).to_hex().to_string();
        let mut second_file = File::open(&bundle_path).unwrap();
        let second = app_install(
            &mut second_file,
            &second_hash,
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1", "sync.v1"],
            &supervisor,
        )
        .unwrap();
        assert_eq!(second.version, "2.0.0");
        assert!(second.rollback_available);
        assert_eq!(
            app_list(&activation_root, &supervisor).unwrap().apps,
            vec![second]
        );
    }

    #[test]
    fn app_install_rejects_wrong_hash_and_missing_required_feature_before_activation() {
        let directory = TestDirectory::new("app-install-reject");
        let activation_root = directory.0.join("activations");
        let bundle_path = directory.0.join("app.kbb");
        let bytes = application_bundle("org.example.reader", "1.0.0", 1);
        let supervisor = AppSupervisor::new();
        fs::write(&bundle_path, &bytes).unwrap();
        let mut file = File::open(&bundle_path).unwrap();
        let checksum = app_install(
            &mut file,
            &"00".repeat(32),
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["sync.v1"],
            &supervisor,
        )
        .unwrap_err();
        assert_eq!(checksum.code, error_codes::CHECKSUM_MISMATCH);
        assert!(!activation_root.exists());

        let mut file = File::open(&bundle_path).unwrap();
        let feature = app_install(
            &mut file,
            blake3::hash(&bytes).to_hex().as_ref(),
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1"],
            &supervisor,
        )
        .unwrap_err();
        assert_eq!(feature.code, error_codes::APP_INSTALL_FAILED);
        assert_eq!(feature.data.unwrap()["reason"], "required_feature");
        assert!(!activation_root.exists());
    }

    #[test]
    fn app_log_tracks_independent_cursors_and_resets_for_a_new_run() {
        let directory = TestDirectory::new("app-log");
        let activation_root = directory.0.join("activations");
        let bundle_path = directory.0.join("app.kbb");
        let bytes = application_bundle("org.example.reader", "1.0.0", 1);
        fs::write(&bundle_path, &bytes).unwrap();
        let supervisor = AppSupervisor::new();
        app_install(
            &mut File::open(&bundle_path).unwrap(),
            blake3::hash(&bytes).to_hex().as_ref(),
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1", "sync.v1"],
            &supervisor,
        )
        .unwrap();

        let mut params = AppLogParams {
            serial: "KT6-TEST".to_owned(),
            app_id: "org.example.reader".to_owned(),
            run_id: None,
            stdout_cursor: 0,
            stderr_cursor: 0,
            max_bytes: Some(4),
        };
        let not_started = app_log(&activation_root, &supervisor, &params).unwrap();
        assert_eq!(not_started.run_id, "not-started");
        assert!(not_started.reset);
        assert_eq!(not_started.state, AppState::Stopped);
        assert_eq!(not_started.pid, None);
        assert_eq!(not_started.stdout.next_cursor, 0);

        let logs = activation_root
            .join("data")
            .join("org.example.reader")
            .join(".kindlebridge-logs");
        fs::create_dir_all(&logs).unwrap();
        fs::write(logs.join("run-id"), "run-1\n").unwrap();
        fs::write(logs.join("stdout.log"), b"abcdefgh").unwrap();
        fs::write(logs.join("stderr.log"), b"error!").unwrap();

        params.run_id = Some(not_started.run_id);
        params.stdout_cursor = 99;
        let first = app_log(&activation_root, &supervisor, &params).unwrap();
        assert!(first.reset);
        assert_eq!(first.run_id, "run-1");
        assert_eq!(BASE64.decode(first.stdout.data_base64).unwrap(), b"abcd");
        assert_eq!(BASE64.decode(first.stderr.data_base64).unwrap(), b"erro");

        params.run_id = Some(first.run_id);
        params.stdout_cursor = first.stdout.next_cursor;
        params.stderr_cursor = first.stderr.next_cursor;
        let second = app_log(&activation_root, &supervisor, &params).unwrap();
        assert!(!second.reset);
        assert_eq!(BASE64.decode(second.stdout.data_base64).unwrap(), b"efgh");
        assert_eq!(BASE64.decode(second.stderr.data_base64).unwrap(), b"r!");
    }

    #[cfg(unix)]
    #[test]
    fn real_application_lifecycle_is_idempotent_and_restart_gets_a_new_pid() {
        let directory = TestDirectory::new("app-lifecycle");
        let activation_root = directory.0.join("activations");
        let bundle_path = directory.0.join("app.kbb");
        let bytes = long_running_application_bundle("org.example.lifecycle");
        fs::write(&bundle_path, &bytes).unwrap();
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let supervisor = AppSupervisor::new();
        let installed = app_install(
            &mut File::open(&bundle_path).unwrap(),
            &hash,
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1"],
            &supervisor,
        )
        .unwrap();
        assert_eq!(installed.state, AppState::Stopped);

        let first = app_start(&activation_root, &supervisor, "org.example.lifecycle").unwrap();
        assert_eq!(first.state, AppState::Running);
        let first_pid = first.pid.unwrap();
        assert_eq!(
            app_start(&activation_root, &supervisor, "org.example.lifecycle")
                .unwrap()
                .pid,
            Some(first_pid)
        );
        assert_eq!(
            app_list(&activation_root, &supervisor).unwrap().apps[0].pid,
            Some(first_pid)
        );

        let restarted =
            app_restart(&activation_root, &supervisor, "org.example.lifecycle").unwrap();
        assert_eq!(restarted.state, AppState::Running);
        assert_ne!(restarted.pid, Some(first_pid));
        let stopped = app_stop(&activation_root, &supervisor, "org.example.lifecycle").unwrap();
        assert_eq!(stopped.state, AppState::Stopped);
        assert_eq!(stopped.pid, None);
        assert_eq!(
            app_stop(&activation_root, &supervisor, "org.example.lifecycle").unwrap(),
            stopped
        );
    }

    #[test]
    fn rollback_finds_the_previous_distinct_app_without_reverting_other_apps() {
        let directory = TestDirectory::new("app-rollback");
        let activation_root = directory.0.join("activations");
        let bundle_path = directory.0.join("app.kbb");
        let supervisor = AppSupervisor::new();
        let install = |app_id: &str, version: &str, release: u64| {
            let bytes = application_bundle(app_id, version, release);
            fs::write(&bundle_path, &bytes).unwrap();
            app_install(
                &mut File::open(&bundle_path).unwrap(),
                blake3::hash(&bytes).to_hex().as_ref(),
                &activation_root,
                "kindlehf",
                "5.17.1.0.4",
                &["app.install.v1", "sync.v1"],
                &supervisor,
            )
            .unwrap()
        };

        install("org.example.reader", "1.0.0", 1);
        install("org.example.other", "1.0.0", 1);
        install("org.example.reader", "2.0.0", 2);
        install("org.example.other", "2.0.0", 2);

        let before = app_list(&activation_root, &supervisor).unwrap();
        assert!(
            before
                .apps
                .iter()
                .find(|app| app.app_id == "org.example.reader")
                .unwrap()
                .rollback_available
        );
        let rolled_back =
            app_rollback(&activation_root, &supervisor, "org.example.reader").unwrap();
        assert_eq!(rolled_back.version, "1.0.0");
        assert_eq!(rolled_back.state, AppState::Stopped);

        let after = app_list(&activation_root, &supervisor).unwrap();
        assert_eq!(
            after
                .apps
                .iter()
                .find(|app| app.app_id == "org.example.reader")
                .unwrap()
                .version,
            "1.0.0"
        );
        assert_eq!(
            after
                .apps
                .iter()
                .find(|app| app.app_id == "org.example.other")
                .unwrap()
                .version,
            "2.0.0"
        );
        assert!(
            !after
                .apps
                .iter()
                .find(|app| app.app_id == "org.example.reader")
                .unwrap()
                .rollback_available
        );

        // An unrelated activation after rollback must not expose the popped
        // reader version again, and a retried rollback must not toggle forward.
        install("org.example.other", "3.0.0", 3);
        let error = app_rollback(&activation_root, &supervisor, "org.example.reader").unwrap_err();
        assert_eq!(error.code, error_codes::NO_ROLLBACK_AVAILABLE);
        let final_apps = app_list(&activation_root, &supervisor).unwrap();
        assert_eq!(
            final_apps
                .apps
                .iter()
                .find(|app| app.app_id == "org.example.reader")
                .unwrap()
                .version,
            "1.0.0"
        );
    }

    #[test]
    fn rollback_without_a_distinct_predecessor_is_a_stable_error() {
        let directory = TestDirectory::new("app-rollback-missing");
        let activation_root = directory.0.join("activations");
        let bundle_path = directory.0.join("app.kbb");
        let bytes = application_bundle("org.example.reader", "1.0.0", 1);
        fs::write(&bundle_path, &bytes).unwrap();
        let supervisor = AppSupervisor::new();
        app_install(
            &mut File::open(&bundle_path).unwrap(),
            blake3::hash(&bytes).to_hex().as_ref(),
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1", "sync.v1"],
            &supervisor,
        )
        .unwrap();

        let error = app_rollback(&activation_root, &supervisor, "org.example.reader").unwrap_err();
        assert_eq!(error.code, error_codes::NO_ROLLBACK_AVAILABLE);
        assert_eq!(error.data.unwrap()["app_id"], "org.example.reader");
    }

    #[cfg(unix)]
    #[test]
    fn uninstall_stops_the_process_but_preserves_data_and_unrelated_apps() {
        let directory = TestDirectory::new("app-uninstall");
        let activation_root = directory.0.join("activations");
        let bundle_path = directory.0.join("app.kbb");
        let supervisor = AppSupervisor::new();

        let target = long_running_application_bundle("org.example.lifecycle");
        fs::write(&bundle_path, &target).unwrap();
        app_install(
            &mut File::open(&bundle_path).unwrap(),
            blake3::hash(&target).to_hex().as_ref(),
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1"],
            &supervisor,
        )
        .unwrap();
        let other = application_bundle("org.example.other", "1.0.0", 1);
        fs::write(&bundle_path, &other).unwrap();
        app_install(
            &mut File::open(&bundle_path).unwrap(),
            blake3::hash(&other).to_hex().as_ref(),
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1", "sync.v1"],
            &supervisor,
        )
        .unwrap();

        let data = activation_root.join("data/org.example.lifecycle/preserved.txt");
        fs::create_dir_all(data.parent().unwrap()).unwrap();
        fs::write(&data, "keep").unwrap();
        let started = app_start(&activation_root, &supervisor, "org.example.lifecycle").unwrap();
        let pid = started.pid.unwrap();
        let removed =
            app_uninstall(&activation_root, &supervisor, "org.example.lifecycle").unwrap();
        assert_eq!(removed.state, AppState::Stopped);
        assert_eq!(removed.pid, None);
        assert_eq!(supervisor.app_id_for_pid(pid).unwrap(), None);
        assert_eq!(fs::read_to_string(data).unwrap(), "keep");

        let apps = app_list(&activation_root, &supervisor).unwrap();
        assert_eq!(apps.apps.len(), 1);
        assert_eq!(apps.apps[0].app_id, "org.example.other");
        assert_eq!(
            app_uninstall(&activation_root, &supervisor, "org.example.lifecycle")
                .unwrap_err()
                .code,
            error_codes::APP_NOT_FOUND
        );
    }

    #[cfg(unix)]
    #[test]
    fn corrupt_rollback_image_is_rejected_before_the_current_process_stops() {
        let directory = TestDirectory::new("app-rollback-corrupt");
        let activation_root = directory.0.join("activations");
        let bundle_path = directory.0.join("app.kbb");
        let supervisor = AppSupervisor::new();
        for (version, release) in [("1.0.0", 1), ("2.0.0", 2)] {
            let bytes =
                long_running_application_bundle_version("org.example.lifecycle", version, release);
            fs::write(&bundle_path, &bytes).unwrap();
            app_install(
                &mut File::open(&bundle_path).unwrap(),
                blake3::hash(&bytes).to_hex().as_ref(),
                &activation_root,
                "kindlehf",
                "5.17.1.0.4",
                &["app.install.v1"],
                &supervisor,
            )
            .unwrap();
        }

        let store = InstallStore::open(&activation_root, APP_BLOCK_QUOTA_BYTES).unwrap();
        let active = store
            .load_generation(store.active_generation_id().unwrap().unwrap())
            .unwrap();
        let previous = store
            .load_generation(active.previous_generation.unwrap())
            .unwrap();
        let previous_entry = previous
            .entries
            .iter()
            .find(|entry| entry.id == "org.example.lifecycle")
            .unwrap();
        let previous_image =
            load_materialized_application(&store, previous_entry.bundle_root).unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(
            previous_image.main.parent().unwrap(),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        fs::remove_file(previous_image.main).unwrap();

        let started = app_start(&activation_root, &supervisor, "org.example.lifecycle").unwrap();
        let pid = started.pid.unwrap();
        let error =
            app_rollback(&activation_root, &supervisor, "org.example.lifecycle").unwrap_err();
        assert_eq!(error.code, error_codes::INVALID_STATE);
        assert_eq!(
            app_list(&activation_root, &supervisor).unwrap().apps[0].pid,
            Some(pid)
        );
        app_stop(&activation_root, &supervisor, "org.example.lifecycle").unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn failed_process_is_reported_distinctly_until_manual_stop_clears_it() {
        let directory = TestDirectory::new("app-failed-state");
        let activation_root = directory.0.join("activations");
        let bundle_path = directory.0.join("app.kbb");
        let bytes = failing_application_bundle("org.example.failure");
        fs::write(&bundle_path, &bytes).unwrap();
        let supervisor = AppSupervisor::new();
        app_install(
            &mut File::open(&bundle_path).unwrap(),
            blake3::hash(&bytes).to_hex().as_ref(),
            &activation_root,
            "kindlehf",
            "5.17.1.0.4",
            &["app.install.v1"],
            &supervisor,
        )
        .unwrap();
        app_start(&activation_root, &supervisor, "org.example.failure").unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let failed = loop {
            let summary = app_list(&activation_root, &supervisor).unwrap().apps[0].clone();
            if summary.state == AppState::Failed {
                break summary;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "application did not reach failed state"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(failed.pid, None);
        let params = AppLogParams {
            serial: "KT6-TEST".to_owned(),
            app_id: "org.example.failure".to_owned(),
            run_id: None,
            stdout_cursor: 0,
            stderr_cursor: 0,
            max_bytes: None,
        };
        let failed_log = app_log(&activation_root, &supervisor, &params).unwrap();
        assert_eq!(failed_log.state, AppState::Failed);
        assert_eq!(failed_log.pid, None);

        let stopped = app_stop(&activation_root, &supervisor, "org.example.failure").unwrap();
        assert_eq!(stopped.state, AppState::Stopped);
        assert_eq!(stopped.pid, None);
        assert_eq!(
            app_log(&activation_root, &supervisor, &params)
                .unwrap()
                .state,
            AppState::Stopped
        );
    }

    #[test]
    fn app_list_rejects_an_activation_without_a_runtime_image() {
        let directory = TestDirectory::new("apps");
        let previous = generation(1, None, "1.0.0");
        let active = generation(2, Some(previous.generation_id), "2.0.0");
        write_generation(&directory.0, &previous);
        write_generation(&directory.0, &active);
        fs::write(
            directory.0.join("active-generation"),
            format!("{}\n", active.directory_name()),
        )
        .unwrap();

        let error = app_list(&directory.0, &AppSupervisor::new()).unwrap_err();
        assert_eq!(error.code, error_codes::INVALID_STATE);
        assert!(error.data.as_ref().is_some_and(|data| data["detail"]
            .as_str()
            .is_some_and(|detail| { detail.contains("runtime image is unavailable or corrupt") })));
    }
}
