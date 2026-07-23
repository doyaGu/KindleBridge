mod sync_operations;

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};
use std::thread;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use kindlebridge_schema::shell_protocol::ShellPacket;
use kindlebridge_schema::{
    methods, parse_request_value, read_frame, write_json_frame, FramingError, RequestId, RpcError,
    RpcRequest, RpcResponse, ShellOpenParams, ShellOpenResult, StreamChannel, StreamClosedParams,
    StreamCreditParams, StreamDataParams, StreamExitParams, StreamIdParams, StreamResizeParams,
    StreamWriteParams, DEFAULT_MAX_CONTENT_LENGTH,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use super::host_rpc::handle_registry;
use super::{DeviceRegistry, DeviceShellEvent, ServeError, ShellStream};
use sync_operations::SyncOperations;

pub(super) struct ClientRuntime<W> {
    writer: Arc<Mutex<W>>,
    registry: Arc<DeviceRegistry>,
    streams: Arc<Mutex<HashMap<String, Arc<dyn ShellStream>>>>,
    sync: SyncOperations<W>,
}

impl<W> ClientRuntime<W> {
    pub(super) fn new(writer: W, registry: Arc<DeviceRegistry>) -> Self {
        let writer = Arc::new(Mutex::new(writer));
        let sync = SyncOperations::new(Arc::clone(&writer), Arc::clone(&registry));
        Self {
            writer,
            registry,
            streams: Arc::new(Mutex::new(HashMap::new())),
            sync,
        }
    }

    pub(super) fn shutdown(&self) {
        if let Ok(mut streams) = self.streams.lock() {
            for (_, stream) in streams.drain() {
                let _ = stream.close();
            }
        }
        self.sync.shutdown();
    }

    #[cfg(test)]
    pub(super) fn track_shell(&self, stream_id: String, shell: Arc<dyn ShellStream>) {
        self.streams.lock().unwrap().insert(stream_id, shell);
    }

    #[cfg(test)]
    pub(super) fn track_sync_operation(
        &self,
        operation_id: String,
        operation: Arc<super::HostSyncOperation>,
    ) {
        self.sync.track(operation_id, operation);
    }

    #[cfg(test)]
    pub(super) fn is_idle(&self) -> bool {
        self.streams.lock().unwrap().is_empty() && self.sync.is_idle()
    }
}

impl<W> Drop for ClientRuntime<W> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Serves the full duplex JSON-RPC API, including asynchronous shell stream
/// notifications. Each invocation owns one client's stream registry; dropping
/// the client deterministically closes every shell it opened.
pub(super) fn serve_streaming<R, W>(
    reader: &mut R,
    writer: W,
    registry: Arc<DeviceRegistry>,
) -> Result<(), ServeError>
where
    R: BufRead,
    W: Write + Send + 'static,
{
    ClientRuntime::new(writer, registry).serve(reader)
}

impl<W> ClientRuntime<W>
where
    W: Write + Send + 'static,
{
    #[cfg(test)]
    pub(super) fn open_sync(&self, request: RpcRequest) -> Result<(), ServeError> {
        self.sync.open(request)
    }

    fn serve<R: BufRead>(&self, reader: &mut R) -> Result<(), ServeError> {
        loop {
            let Some(payload) = read_frame(reader, DEFAULT_MAX_CONTENT_LENGTH)? else {
                return Ok(());
            };
            let value = match serde_json::from_slice::<Value>(&payload) {
                Ok(value) => value,
                Err(_) => {
                    write_shared(
                        &self.writer,
                        &RpcResponse::failure(RequestId::Null, RpcError::parse_error()),
                    )?;
                    continue;
                }
            };
            let request = match parse_request_value(value) {
                Ok(request) => request,
                Err(error) => {
                    write_shared(&self.writer, &RpcResponse::failure(RequestId::Null, error))?;
                    continue;
                }
            };

            if request.method == methods::SHELL_OPEN {
                handle_shell_open(request, &self.writer, &self.registry, &self.streams)?;
            } else if matches!(
                request.method.as_str(),
                methods::SYNC_PUSH_STREAM | methods::SYNC_PULL_STREAM
            ) {
                self.sync.open(request)?;
            } else if request.method == methods::SYNC_CANCEL {
                self.sync.cancel(request);
            } else if matches!(
                request.method.as_str(),
                methods::STREAM_WRITE
                    | methods::STREAM_RESIZE
                    | methods::STREAM_CLOSE_INPUT
                    | methods::STREAM_CLOSE
            ) {
                handle_stream_notification(request, &self.writer, &self.streams)?;
            } else if let Some(response) = handle_registry(request, &self.registry) {
                write_shared(&self.writer, &response)?;
            }
        }
    }
}

pub(super) fn handle_shell_open<W: Write + Send + 'static>(
    request: RpcRequest,
    writer: &Arc<Mutex<W>>,
    registry: &Arc<DeviceRegistry>,
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
        .and_then(|params| registry.rpc(|provider| provider.shell_open(&params)))
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
