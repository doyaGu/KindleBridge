//! Ownership of the Host Sync Operations opened by one local client.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::thread;

use kindlebridge_schema::{
    error_codes, methods, FramingError, RpcError, RpcRequest, RpcResponse, SyncCancelParams,
    SyncProgressPhase, SyncPullParams, SyncPushParams, TransferDirection,
};
use serde::Serialize;
use serde_json::Value;

use super::{emit_notification, random_stream_id, write_shared};
use crate::{DeviceRegistry, HostSyncOperation, ServeError};

const MAX_CLIENT_SYNC_OPERATIONS: usize = 4;

pub(super) struct SyncOperations<W> {
    writer: Arc<Mutex<W>>,
    registry: Arc<DeviceRegistry>,
    active: Arc<Mutex<HashMap<String, Arc<HostSyncOperation>>>>,
}

impl<W> SyncOperations<W> {
    pub(super) fn new(writer: Arc<Mutex<W>>, registry: Arc<DeviceRegistry>) -> Self {
        Self {
            writer,
            registry,
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn shutdown(&self) {
        if let Ok(mut active) = self.active.lock() {
            for (_, operation) in active.drain() {
                operation.cancel();
            }
        }
    }

    pub(super) fn cancel(&self, request: RpcRequest) {
        let Some(params) = request
            .params
            .and_then(|value| serde_json::from_value::<SyncCancelParams>(value).ok())
        else {
            return;
        };
        if let Some(operation) = self
            .active
            .lock()
            .ok()
            .and_then(|active| active.get(&params.operation_id).cloned())
        {
            operation.cancel();
        }
    }

    #[cfg(test)]
    pub(super) fn track(&self, operation_id: String, operation: Arc<HostSyncOperation>) {
        self.active.lock().unwrap().insert(operation_id, operation);
    }

    #[cfg(test)]
    pub(super) fn is_idle(&self) -> bool {
        self.active.lock().unwrap().is_empty()
    }
}

impl<W> Drop for SyncOperations<W> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl<W> SyncOperations<W>
where
    W: std::io::Write + Send + 'static,
{
    pub(super) fn open(&self, request: RpcRequest) -> Result<(), ServeError> {
        let Some(id) = request.id else {
            return Ok(());
        };
        let active_operations = self
            .active
            .lock()
            .map_err(|_| unavailable_registry_error())?
            .len();
        if active_operations >= MAX_CLIENT_SYNC_OPERATIONS {
            return write_shared(
                &self.writer,
                &RpcResponse::failure(
                    id,
                    RpcError::new(
                        error_codes::INVALID_STATE,
                        "Too many active sync operations for this client",
                    ),
                ),
            );
        }
        let operation_id = random_stream_id().map_err(|_| {
            FramingError::Json(serde_json::Error::io(io::Error::other(
                "could not allocate sync operation ID",
            )))
        })?;
        let operation = Arc::new(HostSyncOperation::default());
        self.active
            .lock()
            .map_err(|_| unavailable_registry_error())?
            .insert(operation_id.clone(), Arc::clone(&operation));

        let method = request.method;
        let params = request.params;
        let writer = Arc::clone(&self.writer);
        let registry = Arc::clone(&self.registry);
        let active = Arc::clone(&self.active);
        thread::spawn(move || {
            let result = if method == methods::SYNC_PUSH_STREAM {
                match params
                    .ok_or_else(|| RpcError::invalid_params("missing sync push params"))
                    .and_then(|value| {
                        serde_json::from_value::<SyncPushParams>(value)
                            .map_err(|_| RpcError::invalid_params("invalid sync push params"))
                    }) {
                    Ok(params) => {
                        let remote_path = params.remote_path.clone();
                        let total = std::fs::metadata(&params.local_path)
                            .map(|metadata| metadata.len())
                            .unwrap_or(0);
                        operation.phase(SyncProgressPhase::Hashing, 0, total);
                        run_with_progress(
                            &writer,
                            &operation,
                            &operation_id,
                            TransferDirection::Push,
                            &remote_path,
                            || {
                                registry.rpc(|provider| {
                                    provider.sync_push_with_operation(params, &operation)
                                })
                            },
                        )
                    }
                    Err(error) => Err(error),
                }
            } else {
                match params
                    .ok_or_else(|| RpcError::invalid_params("missing sync pull params"))
                    .and_then(|value| {
                        serde_json::from_value::<SyncPullParams>(value)
                            .map_err(|_| RpcError::invalid_params("invalid sync pull params"))
                    }) {
                    Ok(params) => {
                        let remote_path = params.remote_path.clone();
                        operation.phase(SyncProgressPhase::Transferring, 0, 0);
                        run_with_progress(
                            &writer,
                            &operation,
                            &operation_id,
                            TransferDirection::Pull,
                            &remote_path,
                            || {
                                registry.rpc(|provider| {
                                    provider.sync_pull_with_operation(params, &operation)
                                })
                            },
                        )
                    }
                    Err(error) => Err(error),
                }
            };
            let response = match result {
                Ok(value) => RpcResponse::success(id, value),
                Err(error) => RpcResponse::failure(id, error),
            };
            let _ = write_shared(&writer, &response);
            if let Ok(mut active) = active.lock() {
                active.remove(&operation_id);
            }
        });
        Ok(())
    }
}

fn unavailable_registry_error() -> FramingError {
    FramingError::Json(serde_json::Error::io(io::Error::other(
        "sync operation registry is unavailable",
    )))
}

fn run_with_progress<W, T>(
    writer: &Arc<Mutex<W>>,
    operation: &Arc<HostSyncOperation>,
    operation_id: &str,
    direction: TransferDirection,
    remote_path: &str,
    run: impl FnOnce() -> Result<T, RpcError>,
) -> Result<Value, RpcError>
where
    W: std::io::Write + Send + 'static,
    T: Serialize,
{
    let (done_sender, done_receiver) = std::sync::mpsc::channel();
    let reporter_writer = Arc::clone(writer);
    let reporter_operation = Arc::clone(operation);
    let reporter_id = operation_id.to_owned();
    let reporter_path = remote_path.to_owned();
    let reporter_direction = direction.clone();
    let reporter = thread::spawn(move || loop {
        if emit_notification(
            &reporter_writer,
            methods::SYNC_PROGRESS,
            &reporter_operation.snapshot(
                reporter_id.clone(),
                reporter_direction.clone(),
                reporter_path.clone(),
            ),
        )
        .is_err()
        {
            reporter_operation.cancel();
            return;
        }
        match done_receiver.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
    });
    let result = run().and_then(to_value);
    let _ = done_sender.send(());
    let _ = reporter.join();
    let _ = emit_notification(
        writer,
        methods::SYNC_PROGRESS,
        &operation.snapshot(operation_id.to_owned(), direction, remote_path.to_owned()),
    );
    result
}

fn to_value<T: Serialize>(value: T) -> Result<Value, RpcError> {
    serde_json::to_value(value).map_err(|_| RpcError::internal_error())
}
