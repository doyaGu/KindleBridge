use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};
use std::thread;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use kindlebridge_schema::shell_protocol::ShellPacket;
use kindlebridge_schema::{
    error_codes, methods, parse_request_value, read_frame, write_json_frame, FramingError,
    RequestId, RpcError, RpcRequest, RpcResponse, ShellOpenParams, ShellOpenResult, StreamChannel,
    StreamClosedParams, StreamCreditParams, StreamDataParams, StreamExitParams, StreamIdParams,
    StreamResizeParams, StreamWriteParams, SyncCancelParams, SyncProgressPhase, SyncPullParams,
    SyncPushParams, TransferDirection, DEFAULT_MAX_CONTENT_LENGTH,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use super::{
    handle_request, to_value, DeviceProvider, DeviceShellEvent, ServeError, ShellStream,
    SyncObserver,
};

const MAX_CLIENT_SYNC_JOBS: usize = 4;

/// Serves the full duplex JSON-RPC API, including asynchronous shell stream
/// notifications. Each invocation owns one client's stream registry; dropping
/// the client deterministically closes every shell it opened.
pub(super) fn serve_streaming<R, W>(
    reader: &mut R,
    writer: W,
    provider: Arc<dyn DeviceProvider>,
) -> Result<(), ServeError>
where
    R: BufRead,
    W: Write + Send + 'static,
{
    let writer = Arc::new(Mutex::new(writer));
    let streams: Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let sync_jobs: Arc<Mutex<HashMap<String, Arc<SyncObserver>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let result = serve_streaming_loop(reader, &writer, &provider, &streams, &sync_jobs);
    if let Ok(mut streams) = streams.lock() {
        for (_, stream) in streams.drain() {
            let _ = stream.close();
        }
    }
    cancel_sync_jobs(&sync_jobs);
    result
}

pub(super) fn cancel_sync_jobs(jobs: &Arc<Mutex<HashMap<String, Arc<SyncObserver>>>>) {
    if let Ok(mut jobs) = jobs.lock() {
        for (_, observer) in jobs.drain() {
            observer.cancel();
        }
    }
}

fn serve_streaming_loop<R, W>(
    reader: &mut R,
    writer: &Arc<Mutex<W>>,
    provider: &Arc<dyn DeviceProvider>,
    streams: &Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
    sync_jobs: &Arc<Mutex<HashMap<String, Arc<SyncObserver>>>>,
) -> Result<(), ServeError>
where
    R: BufRead,
    W: Write + Send + 'static,
{
    loop {
        let Some(payload) = read_frame(reader, DEFAULT_MAX_CONTENT_LENGTH)? else {
            return Ok(());
        };
        let value = match serde_json::from_slice::<Value>(&payload) {
            Ok(value) => value,
            Err(_) => {
                write_shared(
                    writer,
                    &RpcResponse::failure(RequestId::Null, RpcError::parse_error()),
                )?;
                continue;
            }
        };
        let request = match parse_request_value(value) {
            Ok(request) => request,
            Err(error) => {
                write_shared(writer, &RpcResponse::failure(RequestId::Null, error))?;
                continue;
            }
        };

        if request.method == methods::SHELL_OPEN {
            handle_shell_open(request, writer, provider, streams)?;
        } else if matches!(
            request.method.as_str(),
            methods::SYNC_PUSH_STREAM | methods::SYNC_PULL_STREAM
        ) {
            handle_sync_open(request, writer, provider, sync_jobs)?;
        } else if request.method == methods::SYNC_CANCEL {
            handle_sync_cancel(request, sync_jobs);
        } else if matches!(
            request.method.as_str(),
            methods::STREAM_WRITE
                | methods::STREAM_RESIZE
                | methods::STREAM_CLOSE_INPUT
                | methods::STREAM_CLOSE
        ) {
            handle_stream_notification(request, writer, streams)?;
        } else if let Some(response) = handle_request(request, provider.as_ref()) {
            write_shared(writer, &response)?;
        }
    }
}

pub(super) fn handle_sync_open<W: Write + Send + 'static>(
    request: RpcRequest,
    writer: &Arc<Mutex<W>>,
    provider: &Arc<dyn DeviceProvider>,
    jobs: &Arc<Mutex<HashMap<String, Arc<SyncObserver>>>>,
) -> Result<(), ServeError> {
    let Some(id) = request.id else {
        return Ok(());
    };
    let active_jobs = jobs
        .lock()
        .map_err(|_| {
            FramingError::Json(serde_json::Error::io(io::Error::other(
                "sync operation registry is unavailable",
            )))
        })?
        .len();
    if active_jobs >= MAX_CLIENT_SYNC_JOBS {
        return write_shared(
            writer,
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
    let observer = Arc::new(SyncObserver::default());
    jobs.lock()
        .map_err(|_| {
            FramingError::Json(serde_json::Error::io(io::Error::other(
                "sync operation registry is unavailable",
            )))
        })?
        .insert(operation_id.clone(), Arc::clone(&observer));

    let method = request.method;
    let params = request.params;
    let writer = Arc::clone(writer);
    let provider = Arc::clone(provider);
    let jobs = Arc::clone(jobs);
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
                    observer.phase(SyncProgressPhase::Hashing, 0, total);
                    run_with_progress(
                        &writer,
                        &observer,
                        &operation_id,
                        TransferDirection::Push,
                        &remote_path,
                        || provider.sync_push_observed(params, &observer),
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
                    observer.phase(SyncProgressPhase::Transferring, 0, 0);
                    run_with_progress(
                        &writer,
                        &observer,
                        &operation_id,
                        TransferDirection::Pull,
                        &remote_path,
                        || provider.sync_pull_observed(params, &observer),
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
        if let Ok(mut jobs) = jobs.lock() {
            jobs.remove(&operation_id);
        }
    });
    Ok(())
}

fn run_with_progress<W, T>(
    writer: &Arc<Mutex<W>>,
    observer: &Arc<SyncObserver>,
    operation_id: &str,
    direction: TransferDirection,
    remote_path: &str,
    operation: impl FnOnce() -> Result<T, RpcError>,
) -> Result<Value, RpcError>
where
    W: Write + Send + 'static,
    T: Serialize,
{
    let (done_sender, done_receiver) = std::sync::mpsc::channel();
    let reporter_writer = Arc::clone(writer);
    let reporter_observer = Arc::clone(observer);
    let reporter_id = operation_id.to_owned();
    let reporter_path = remote_path.to_owned();
    let reporter_direction = direction.clone();
    let reporter = thread::spawn(move || loop {
        if emit_notification(
            &reporter_writer,
            methods::SYNC_PROGRESS,
            &reporter_observer.snapshot(
                reporter_id.clone(),
                reporter_direction.clone(),
                reporter_path.clone(),
            ),
        )
        .is_err()
        {
            reporter_observer.cancel();
            return;
        }
        match done_receiver.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
    });
    let result = operation().and_then(to_value);
    let _ = done_sender.send(());
    let _ = reporter.join();
    let _ = emit_notification(
        writer,
        methods::SYNC_PROGRESS,
        &observer.snapshot(operation_id.to_owned(), direction, remote_path.to_owned()),
    );
    result
}

fn handle_sync_cancel(request: RpcRequest, jobs: &Arc<Mutex<HashMap<String, Arc<SyncObserver>>>>) {
    let Some(params) = request
        .params
        .and_then(|value| serde_json::from_value::<SyncCancelParams>(value).ok())
    else {
        return;
    };
    if let Some(observer) = jobs
        .lock()
        .ok()
        .and_then(|jobs| jobs.get(&params.operation_id).cloned())
    {
        observer.cancel();
    }
}

pub(super) fn handle_shell_open<W: Write + Send + 'static>(
    request: RpcRequest,
    writer: &Arc<Mutex<W>>,
    provider: &Arc<dyn DeviceProvider>,
    streams: &Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
) -> Result<(), ServeError> {
    let Some(id) = request.id else {
        return Ok(());
    };
    let result = request
        .params
        .ok_or_else(|| RpcError::invalid_params("missing shell open params"))
        .and_then(|value| {
            serde_json::from_value::<ShellOpenParams>(value).map_err(|_| {
                RpcError::invalid_params("expected serial and valid shell open fields")
            })
        })
        .and_then(|params| provider.shell_open(&params))
        .and_then(|shell| {
            let stream_id = random_stream_id().map_err(|_| RpcError::internal_error())?;
            streams
                .lock()
                .map_err(|_| RpcError::internal_error())?
                .insert(stream_id.clone(), Arc::clone(&shell));
            Ok((stream_id, shell))
        });

    match result {
        Ok((stream_id, shell)) => {
            write_shared(
                writer,
                &RpcResponse::success(
                    id,
                    serde_json::to_value(ShellOpenResult {
                        stream_id: stream_id.clone(),
                        send_credit: kindlebridge_schema::device_protocol::SHELL_STREAM_WINDOW,
                        receive_credit: kindlebridge_schema::device_protocol::SHELL_STREAM_WINDOW,
                    })
                    .map_err(|_| {
                        FramingError::Json(serde_json::Error::io(io::Error::other(
                            "could not encode shell open result",
                        )))
                    })?,
                ),
            )?;
            spawn_shell_output(stream_id, shell, Arc::clone(writer), Arc::clone(streams));
        }
        Err(error) => write_shared(writer, &RpcResponse::failure(id, error))?,
    }
    Ok(())
}

pub(super) fn handle_stream_notification<W: Write + Send + 'static>(
    request: RpcRequest,
    writer: &Arc<Mutex<W>>,
    streams: &Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
) -> Result<(), ServeError> {
    // Stream operations are notifications. Malformed or unknown stream IDs are
    // ignored because JSON-RPC notifications cannot receive an error response;
    // the active shell remains isolated from other client streams.
    match request.method.as_str() {
        methods::STREAM_WRITE => {
            let Some(params) = decode_notification::<StreamWriteParams>(request.params) else {
                return Ok(());
            };
            let Ok(data) = BASE64.decode(params.data) else {
                return Ok(());
            };
            if data.len() > kindlebridge_schema::shell_protocol::MAX_SHELL_PACKET_PAYLOAD {
                return Ok(());
            }
            let Some(shell) = find_stream(streams, &params.stream_id) else {
                return Ok(());
            };
            if shell.send(ShellPacket::Stdin(data.clone())).is_ok() {
                emit_notification(
                    writer,
                    methods::STREAM_CREDIT,
                    &StreamCreditParams {
                        stream_id: params.stream_id,
                        bytes: u32::try_from(data.len()).unwrap_or(0),
                    },
                )?;
            }
        }
        methods::STREAM_RESIZE => {
            let Some(params) = decode_notification::<StreamResizeParams>(request.params) else {
                return Ok(());
            };
            if let Some(shell) = find_stream(streams, &params.stream_id) {
                let _ = shell.send(ShellPacket::Resize(params.size));
            }
        }
        methods::STREAM_CLOSE_INPUT => {
            let Some(params) = decode_notification::<StreamIdParams>(request.params) else {
                return Ok(());
            };
            if let Some(shell) = find_stream(streams, &params.stream_id) {
                let _ = shell.send(ShellPacket::CloseStdin);
            }
        }
        methods::STREAM_CLOSE => {
            let Some(params) = decode_notification::<StreamIdParams>(request.params) else {
                return Ok(());
            };
            let shell = streams
                .lock()
                .ok()
                .and_then(|mut streams| streams.remove(&params.stream_id));
            if let Some(shell) = shell {
                let _ = shell.close();
                emit_notification(
                    writer,
                    methods::STREAM_CLOSED,
                    &StreamClosedParams {
                        stream_id: params.stream_id,
                        reason: Some("closed by client".to_owned()),
                    },
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn spawn_shell_output<W: Write + Send + 'static>(
    stream_id: String,
    shell: Arc<dyn ShellStream>,
    writer: Arc<Mutex<W>>,
    streams: Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
) {
    thread::Builder::new()
        .name(format!("kindlebridge-shell-{stream_id}"))
        .spawn(move || {
            let reason = loop {
                match shell.recv() {
                    Ok(DeviceShellEvent::Packet(ShellPacket::Stdout(data))) => {
                        if emit_notification(
                            &writer,
                            methods::STREAM_DATA,
                            &StreamDataParams {
                                stream_id: stream_id.clone(),
                                channel: StreamChannel::Stdout,
                                data: BASE64.encode(data),
                            },
                        )
                        .is_err()
                        {
                            break Some("client connection was lost".to_owned());
                        }
                    }
                    Ok(DeviceShellEvent::Packet(ShellPacket::Stderr(data))) => {
                        if emit_notification(
                            &writer,
                            methods::STREAM_DATA,
                            &StreamDataParams {
                                stream_id: stream_id.clone(),
                                channel: StreamChannel::Stderr,
                                data: BASE64.encode(data),
                            },
                        )
                        .is_err()
                        {
                            break Some("client connection was lost".to_owned());
                        }
                    }
                    Ok(DeviceShellEvent::Packet(ShellPacket::Exit(status))) => {
                        if emit_notification(
                            &writer,
                            methods::STREAM_EXIT,
                            &StreamExitParams {
                                stream_id: stream_id.clone(),
                                exit_code: status.exit_code,
                                signal: status.signal,
                            },
                        )
                        .is_err()
                        {
                            break Some("client connection was lost".to_owned());
                        }
                    }
                    Ok(DeviceShellEvent::Packet(_)) => {
                        break Some("invalid device shell packet".to_owned())
                    }
                    Ok(DeviceShellEvent::Closed) => break None,
                    Err(error) => break Some(error.to_string()),
                }
            };
            let removed = streams
                .lock()
                .ok()
                .and_then(|mut streams| streams.remove(&stream_id))
                .is_some();
            if removed {
                let _ = emit_notification(
                    &writer,
                    methods::STREAM_CLOSED,
                    &StreamClosedParams { stream_id, reason },
                );
            }
        })
        .expect("could not start shell notification worker");
}

fn decode_notification<T: DeserializeOwned>(params: Option<Value>) -> Option<T> {
    serde_json::from_value(params?).ok()
}

fn find_stream(
    streams: &Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
    stream_id: &str,
) -> Option<Arc<dyn ShellStream>> {
    streams.lock().ok()?.get(stream_id).cloned()
}

fn random_stream_id() -> Result<String, getrandom::Error> {
    let mut bytes = [0_u8; 16];
    getrandom::getrandom(&mut bytes)?;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

fn emit_notification<W: Write, T: Serialize>(
    writer: &Arc<Mutex<W>>,
    method: &str,
    params: &T,
) -> Result<(), ServeError> {
    let value = serde_json::to_value(params).map_err(FramingError::from)?;
    write_shared(writer, &RpcRequest::notification(method, Some(value)))
}

fn write_shared<W: Write, T: Serialize>(
    writer: &Arc<Mutex<W>>,
    value: &T,
) -> Result<(), ServeError> {
    let mut writer = writer
        .lock()
        .map_err(|_| FramingError::Io(io::Error::other("RPC writer lock is poisoned")))?;
    write_json_frame(&mut *writer, value)?;
    Ok(())
}
