//! Persistent development KBP listener for the unprivileged device daemon.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use kindlebridge_functionfs::{FunctionFsDevice, FunctionFsError, FunctionFsFrameStream};
use kindlebridge_schema::device_protocol::{
    is_valid_session_id, DeviceCall, DeviceHello, DeviceReply, HostHello, ServiceAccept,
    ServiceOpen, SyncReply, SyncRequest, DEFAULT_CONNECTION_WINDOW, DEFAULT_STREAM_WINDOW,
    EXEC_FEATURE, PROTOCOL_VERSION, SHELL_SERVICE, SYNC_CREDIT_BATCH_SIZE, SYNC_FEATURE,
    SYNC_SERVICE,
};
use kindlebridge_schema::{
    error_codes, methods, ExecParams, RpcError, SyncStatusParams, MAX_SYNC_BLOCK_SIZE,
};
use kindlebridge_transport_tcp::{
    ErrorClass, FrameIo, TcpFrameListener, TransportConfig, TransportError,
};
use kindlebridge_wire::{
    Command, DecodeLimits, EndpointRole, Frame, FrameContext, Header, ProtocolError, SessionConfig,
    SessionState, WireError, FLAG_END_STREAM,
};
use serde::Serialize;
use thiserror::Error;

use crate::exec::{self, ExecError};
use crate::sync::{PullTransfer, PushTransfer, StoreError, SyncStore};
use crate::DeviceInfo;

const SESSION_IO_TIMEOUT: Duration = Duration::from_secs(10 * 60 + 30);
const DEFAULT_SYNC_ROOT: &str = "/mnt/us/kindlebridge-data";
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub device: DeviceInfo,
    pub allowed_peer: Option<IpAddr>,
    pub connection_window: u32,
    pub stream_window: u32,
    pub sync_root: PathBuf,
}

impl ServerConfig {
    #[must_use]
    pub fn new(device: DeviceInfo) -> Self {
        Self {
            device,
            allowed_peer: None,
            connection_window: DEFAULT_CONNECTION_WINDOW,
            stream_window: DEFAULT_STREAM_WINDOW,
            sync_root: PathBuf::from(DEFAULT_SYNC_ROOT),
        }
    }

    #[must_use]
    pub const fn allow_peer(mut self, peer: IpAddr) -> Self {
        self.allowed_peer = Some(peer);
        self
    }

    #[must_use]
    pub fn sync_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.sync_root = root.into();
        self
    }

    fn limits(&self) -> DecodeLimits {
        DecodeLimits::new(self.connection_window, self.connection_window)
    }
}

#[derive(Debug)]
pub struct TcpServer {
    listener: TcpFrameListener,
    config: ServerConfig,
    sync_store: SyncStore,
}

impl TcpServer {
    pub fn bind(address: SocketAddr, config: ServerConfig) -> Result<Self, ServerError> {
        validate_config(&config)?;
        let listener = TcpFrameListener::bind(address, transport_config(&config))?;
        let sync_store = SyncStore::new(config.sync_root.clone());
        Ok(Self {
            listener,
            config,
            sync_store,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        Ok(self.listener.local_addr()?)
    }

    pub fn serve_once(&self) -> Result<(), ServerError> {
        let (mut stream, peer) = self.listener.accept()?;
        self.validate_peer(peer.ip())?;
        serve_session(&mut stream, &self.config, &self.sync_store, false)
    }

    pub fn serve_forever(&self) -> Result<(), ServerError> {
        loop {
            let (mut stream, peer) = self.listener.accept()?;
            if self.validate_peer(peer.ip()).is_err() {
                continue;
            }
            // One host owns the development link. A disconnect returns to
            // accept without terminating the daemon.
            let _ = serve_session(&mut stream, &self.config, &self.sync_store, false);
        }
    }

    fn validate_peer(&self, peer: IpAddr) -> Result<(), ServerError> {
        if self
            .config
            .allowed_peer
            .is_some_and(|allowed| peer != allowed)
        {
            Err(ServerError::PeerNotAllowed(peer))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub struct UsbServer {
    functionfs: FunctionFsDevice,
    config: ServerConfig,
    sync_store: SyncStore,
}

impl UsbServer {
    /// Consume an already-mounted FunctionFS directory and publish descriptors.
    /// Gadget/configfs ownership deliberately remains outside the daemon.
    pub fn open(
        functionfs_dir: impl AsRef<std::path::Path>,
        config: ServerConfig,
    ) -> Result<Self, ServerError> {
        validate_config(&config)?;
        let functionfs = FunctionFsDevice::open(functionfs_dir.as_ref())?;
        let sync_store = SyncStore::new(config.sync_root.clone());
        Ok(Self {
            functionfs,
            config,
            sync_store,
        })
    }

    /// Serve one USB configuration. Returns `false` if FunctionFS was unbound
    /// before a host enabled the interface.
    pub fn serve_once(&mut self) -> Result<bool, ServerError> {
        let Some(endpoints) = self.functionfs.accept()? else {
            return Ok(false);
        };
        let mut stream = FunctionFsFrameStream::new(endpoints, transport_config(&self.config))?;
        serve_session(&mut stream, &self.config, &self.sync_store, false)?;
        Ok(true)
    }

    /// Keep accepting host reconnects while the FunctionFS gadget remains bound.
    pub fn serve_forever(&mut self) -> Result<(), ServerError> {
        loop {
            let Some(endpoints) = self.functionfs.accept()? else {
                return Ok(());
            };
            let mut stream = FunctionFsFrameStream::new(endpoints, transport_config(&self.config))?;
            // A host CLI process may release and reclaim WinUSB without
            // disabling the USB configuration. Keep the endpoint pair open
            // and admit a fresh KBP HELLO in place; physical disconnects still
            // return here so FunctionFS can wait for its next ENABLE event.
            if let Err(error) = serve_session(&mut stream, &self.config, &self.sync_store, true) {
                eprintln!("KindleBridge USB session ended: {error}");
            }
        }
    }
}

fn serve_session(
    stream: &mut dyn FrameIo,
    config: &ServerConfig,
    sync_store: &SyncStore,
    allow_in_place_restart: bool,
) -> Result<(), ServerError> {
    let mut pending_hello = None;
    'sessions: loop {
        let hello_frame = match pending_hello.take() {
            Some(frame) => frame,
            None => loop {
                match stream.read_frame() {
                    Ok(frame) => break frame,
                    Err(TransportError::EndOfStream) => return Ok(()),
                    Err(error)
                        if allow_in_place_restart
                            && transport_error_allows_in_place_restart(&error) =>
                    {
                        recover_frame_boundary(stream)?;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                }
            },
        };
        // Reclaiming a WinUSB interface does not disable and re-enable the
        // FunctionFS function. After an interrupted host process, complete
        // frames from the abandoned session may still precede the next HELLO.
        // They are stale by definition because no session state exists here.
        if allow_in_place_restart && !is_fresh_hello(&hello_frame) {
            continue 'sessions;
        }
        let mut state =
            SessionState::new(SessionConfig::new(EndpointRole::Device, config.limits()));
        expect(&hello_frame, Command::Hello, 0)?;
        let host_hello: HostHello = decode(&hello_frame.payload, "host HELLO")?;
        validate_hello(&host_hello, config)?;
        state.process_inbound(
            &hello_frame.header,
            FrameContext::hello(host_hello.initial_connection_window),
        )?;

        let hello = DeviceHello {
            protocol_version: PROTOCOL_VERSION,
            session_id: host_hello.session_id.clone(),
            serial: config.device.serial.clone(),
            model: config.device.model.clone(),
            firmware: config.device.firmware.clone(),
            target: config.device.target.clone(),
            features: vec![EXEC_FEATURE.to_owned(), SYNC_FEATURE.to_owned()],
            initial_connection_window: config.connection_window,
        };
        if let Err(error) = send(
            stream,
            &mut state,
            frame(Command::Hello, 0, 0, encode(&hello)?)?,
            FrameContext::hello(config.connection_window),
        ) {
            if allow_in_place_restart && server_error_allows_in_place_restart(&error) {
                recover_frame_boundary(stream)?;
                continue 'sessions;
            }
            return Err(error);
        }

        let mut control_sequence = 1_u32;
        loop {
            let open_frame = match read_application_frame(stream, &mut state) {
                Ok(Some(frame)) => frame,
                Ok(None) => return Ok(()),
                Err(error)
                    if allow_in_place_restart && server_error_allows_in_place_restart(&error) =>
                {
                    recover_frame_boundary(stream)?;
                    // The next complete frame belongs to a new host transport.
                    // Returning to the HELLO gate also discards the recovery
                    // Ping marker without interpreting it as an OPEN in the
                    // abandoned session state.
                    continue 'sessions;
                }
                Err(error) => return Err(error),
            };
            if allow_in_place_restart
                && open_frame.header.command == Command::Hello
                && open_frame.header.stream_id == 0
            {
                pending_hello = Some(open_frame);
                continue 'sessions;
            }
            if open_frame.header.command == Command::GoAway {
                state.process_inbound(&open_frame.header, FrameContext::default())?;
                if allow_in_place_restart {
                    continue 'sessions;
                }
                return Ok(());
            }
            // Credits for a responder's final DATA may arrive immediately after
            // CLOSE. They belong to the completed stream and must not be mistaken
            // for the next OPEN.
            if open_frame.header.command == Command::Credit {
                continue;
            }
            expect(&open_frame, Command::Open, open_frame.header.stream_id)?;
            if open_frame.header.stream_id == 0 {
                return Err(ServerError::UnexpectedFrame("OPEN on stream zero"));
            }
            state.process_inbound(&open_frame.header, FrameContext::default())?;
            let service: ServiceOpen = decode(&open_frame.payload, "OPEN")?;
            let stream_id = open_frame.header.stream_id;
            if !matches!(service.service.as_str(), SHELL_SERVICE | SYNC_SERVICE) {
                send(
                    stream,
                    &mut state,
                    frame(
                        Command::Reject,
                        stream_id,
                        0,
                        format!("unsupported service {}", service.service).into_bytes(),
                    )?,
                    FrameContext::default(),
                )?;
                continue;
            }

            if let Err(error) = send(
                stream,
                &mut state,
                frame(
                    Command::Accept,
                    stream_id,
                    0,
                    encode(&ServiceAccept {
                        initial_stream_window: config.stream_window,
                    })?,
                )?,
                FrameContext::accept(config.stream_window),
            ) {
                if allow_in_place_restart && server_error_allows_in_place_restart(&error) {
                    recover_frame_boundary(stream)?;
                    continue 'sessions;
                }
                return Err(error);
            }
            let service_result = match service.service.as_str() {
                SHELL_SERVICE => serve_shell_stream(
                    stream,
                    &mut state,
                    config,
                    sync_store,
                    stream_id,
                    &mut control_sequence,
                ),
                SYNC_SERVICE => serve_sync_stream(
                    stream,
                    &mut state,
                    config,
                    sync_store,
                    stream_id,
                    &mut control_sequence,
                ),
                _ => unreachable!(),
            };
            if let Err(error) = service_result {
                if allow_in_place_restart {
                    if let ServerError::FreshHello(frame) = error {
                        pending_hello = Some(*frame);
                        continue 'sessions;
                    }
                    if server_error_allows_in_place_restart(&error) {
                        recover_frame_boundary(stream)?;
                        continue 'sessions;
                    }
                }
                return Err(error);
            }
        }
    }
}

fn server_error_allows_in_place_restart(error: &ServerError) -> bool {
    matches!(
        error,
        ServerError::Transport(error) if transport_error_allows_in_place_restart(error)
    ) || matches!(
        error,
        ServerError::Protocol(_)
            | ServerError::InvalidPayload { .. }
            | ServerError::UnexpectedFrame(_)
            | ServerError::SequenceExhausted
    )
}

fn is_fresh_hello(frame: &Frame) -> bool {
    frame.header.command == Command::Hello && frame.header.stream_id == 0
}

fn transport_error_allows_in_place_restart(error: &TransportError) -> bool {
    if matches!(
        error,
        TransportError::Io { source, .. }
            if source.kind() == std::io::ErrorKind::ConnectionAborted
    ) {
        return false;
    }
    matches!(
        error.class(),
        ErrorClass::Timeout | ErrorClass::Io | ErrorClass::Truncated | ErrorClass::Protocol
    )
}

fn pause_before_in_place_restart() {
    thread::sleep(Duration::from_millis(50));
}

fn recover_frame_boundary(stream: &mut dyn FrameIo) -> Result<(), ServerError> {
    loop {
        match stream.resynchronize() {
            Ok(()) => return Ok(()),
            Err(error) if transport_error_allows_in_place_restart(&error) => {
                pause_before_in_place_restart();
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn serve_shell_stream(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    config: &ServerConfig,
    sync_store: &SyncStore,
    stream_id: u32,
    control_sequence: &mut u32,
) -> Result<(), ServerError> {
    let response_credit =
        read_application_frame(stream, state)?.ok_or(ServerError::Disconnected)?;
    expect(&response_credit, Command::Credit, stream_id)?;
    state.process_inbound(&response_credit.header, FrameContext::default())?;

    let request = read_application_frame(stream, state)?.ok_or(ServerError::Disconnected)?;
    expect(&request, Command::Data, stream_id)?;
    if request.header.flags & FLAG_END_STREAM == 0 {
        return Err(ServerError::UnexpectedFrame(
            "device calls must fit in one END_STREAM DATA frame",
        ));
    }
    state.process_inbound(&request.header, FrameContext::default())?;
    restore_connection_credit(
        stream,
        state,
        control_sequence,
        request.header.payload_length,
    )?;

    let call: DeviceCall = decode(&request.payload, "device call")?;
    let mut reply = dispatch(call, &config.device, sync_store);
    let mut response_payload = encode(&reply)?;
    if response_payload.len() > usize::try_from(config.stream_window).unwrap_or(usize::MAX) {
        reply = DeviceReply::failure(RpcError::new(
            error_codes::EXEC_OUTPUT_LIMIT,
            "Command output exceeds the device link limit",
        ));
        response_payload = encode(&reply)?;
    }
    send_data(stream, state, stream_id, 1, response_payload, true)?;
    send(
        stream,
        state,
        frame(Command::Close, stream_id, 2, Vec::new())?,
        FrameContext::default(),
    )
}

fn serve_sync_stream(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    config: &ServerConfig,
    sync_store: &SyncStore,
    stream_id: u32,
    control_sequence: &mut u32,
) -> Result<(), ServerError> {
    let response_credit =
        read_application_frame(stream, state)?.ok_or(ServerError::Disconnected)?;
    expect(&response_credit, Command::Credit, stream_id)?;
    state.process_inbound(&response_credit.header, FrameContext::default())?;

    let request_frame = read_application_frame(stream, state)?.ok_or(ServerError::Disconnected)?;
    expect(&request_frame, Command::Data, stream_id)?;
    state.process_inbound(&request_frame.header, FrameContext::default())?;
    let request: SyncRequest = decode(&request_frame.payload, "sync request")?;
    let block_size = match &request {
        SyncRequest::Push { block_size, .. } | SyncRequest::Pull { block_size, .. } => *block_size,
    };
    if block_size == 0 || block_size > MAX_SYNC_BLOCK_SIZE {
        return send_sync_failure(
            stream,
            state,
            stream_id,
            1,
            RpcError::invalid_params("block_size must be between 1 and 1048576"),
        );
    }

    match request {
        SyncRequest::Push {
            transfer_id,
            remote_path,
            total_size,
            file_hash,
            block_size,
        } => {
            if request_frame.header.flags & FLAG_END_STREAM != 0 {
                return send_sync_failure(
                    stream,
                    state,
                    stream_id,
                    1,
                    RpcError::invalid_params("push metadata must not end the stream"),
                );
            }
            let transfer = match sync_store.begin_push(
                transfer_id.as_deref(),
                &remote_path,
                total_size,
                &file_hash,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    return send_sync_failure(stream, state, stream_id, 1, error.into_rpc())
                }
            };
            serve_sync_push(
                stream,
                state,
                stream_id,
                control_sequence,
                request_frame.header.payload_length,
                block_size,
                transfer,
            )
        }
        SyncRequest::Pull {
            transfer_id,
            remote_path,
            offset,
            block_size,
        } => {
            if request_frame.header.flags & FLAG_END_STREAM == 0 {
                return send_sync_failure(
                    stream,
                    state,
                    stream_id,
                    1,
                    RpcError::invalid_params("pull metadata must end the stream"),
                );
            }
            let transfer = match sync_store.begin_pull(transfer_id.as_deref(), &remote_path, offset)
            {
                Ok(transfer) => transfer,
                Err(error) => {
                    return send_sync_failure(stream, state, stream_id, 1, error.into_rpc())
                }
            };
            serve_sync_pull(
                stream,
                state,
                config,
                stream_id,
                control_sequence,
                request_frame.header.payload_length,
                block_size,
                transfer,
            )
        }
    }
}

fn serve_sync_push(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
    control_sequence: &mut u32,
    request_length: u32,
    block_size: u32,
    mut transfer: PushTransfer,
) -> Result<(), ServerError> {
    let ready = SyncReply::Ready {
        transfer_id: transfer.transfer_id().to_owned(),
        offset: transfer.offset(),
        total_size: transfer.total_size(),
        file_hash: transfer.file_hash().to_owned(),
    };
    let mut send_sequence = 1_u32;
    send_data(
        stream,
        state,
        stream_id,
        send_sequence,
        encode(&ready)?,
        false,
    )?;
    send_sequence = next_sequence(send_sequence)?;
    restore_received_credit(
        stream,
        state,
        stream_id,
        &mut send_sequence,
        control_sequence,
        request_length,
    )?;

    let block_size = usize::try_from(block_size).map_err(|_| ServerError::InvalidConfig)?;
    let mut received_batch = 0_u64;
    let mut last_frame_start = None;
    let receive_result = loop {
        let data = match read_stream_data(stream, state, stream_id) {
            Ok(Some(frame)) => frame,
            Ok(None) => break Err(ServerError::Disconnected),
            Err(error) => break Err(error),
        };
        let payload_length = data.header.payload_length;
        let is_last = data.header.flags & FLAG_END_STREAM != 0;
        if data.payload.len() > block_size {
            break Err(ServerError::UnexpectedFrame(
                "sync DATA exceeds the negotiated block size",
            ));
        }
        // Tie USB credit to bytes accepted by storage. The KT6 userstore is
        // faster than the USB link, so an extra writer queue adds a second
        // backpressure loop without increasing steady-state throughput.
        let frame_start = transfer.offset();
        if let Err(error) = transfer.write_chunk(&data.payload) {
            break Err(ServerError::Sync(error));
        }
        last_frame_start = Some(frame_start);
        let Some(next_received_batch) = received_batch.checked_add(u64::from(payload_length))
        else {
            break Err(ServerError::SequenceExhausted);
        };
        received_batch = next_received_batch;
        if received_batch >= u64::from(SYNC_CREDIT_BATCH_SIZE) || is_last {
            let delta = match u32::try_from(received_batch) {
                Ok(delta) => delta,
                Err(_) => {
                    break Err(ServerError::UnexpectedFrame(
                        "sync credit batch is too large",
                    ))
                }
            };
            if let Err(error) = restore_received_credit(
                stream,
                state,
                stream_id,
                &mut send_sequence,
                control_sequence,
                delta,
            ) {
                break Err(error);
            }
            received_batch = 0;
        }
        if is_last {
            break Ok(());
        }
    };

    if let Err(receive_error) = receive_result {
        if let Some(offset) = last_frame_start {
            transfer.rollback_for_resume(offset)?;
        } else {
            transfer.checkpoint()?;
        }
        return Err(receive_error);
    }

    let status = match transfer.finish() {
        Ok(status) => status,
        Err(error) => {
            return send_sync_failure(stream, state, stream_id, send_sequence, error.into_rpc())
        }
    };
    let complete = SyncReply::Complete {
        transfer_id: status.transfer_id,
        next_offset: status.next_offset,
        total_size: status.total_size,
    };
    send_data(
        stream,
        state,
        stream_id,
        send_sequence,
        encode(&complete)?,
        true,
    )?;
    send_sequence = next_sequence(send_sequence)?;
    send(
        stream,
        state,
        frame(Command::Close, stream_id, send_sequence, Vec::new())?,
        FrameContext::default(),
    )
}

#[allow(clippy::too_many_arguments)]
fn serve_sync_pull(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    config: &ServerConfig,
    stream_id: u32,
    control_sequence: &mut u32,
    request_length: u32,
    block_size: u32,
    mut transfer: PullTransfer,
) -> Result<(), ServerError> {
    let ready = SyncReply::Ready {
        transfer_id: transfer.transfer_id().to_owned(),
        offset: transfer.offset(),
        total_size: transfer.total_size(),
        file_hash: transfer.file_hash().to_owned(),
    };
    let mut send_sequence = 1_u32;
    send_data(
        stream,
        state,
        stream_id,
        send_sequence,
        encode(&ready)?,
        false,
    )?;
    send_sequence = next_sequence(send_sequence)?;
    restore_received_credit(
        stream,
        state,
        stream_id,
        &mut send_sequence,
        control_sequence,
        request_length,
    )?;

    let buffer_size = usize::try_from(block_size).map_err(|_| ServerError::InvalidConfig)?;
    let mut buffer = vec![0_u8; buffer_size];
    let mut batch_bytes = 0_u64;
    if transfer.offset() == transfer.total_size() {
        transfer.finish().map_err(ServerError::Sync)?;
        wait_for_send_capacity(stream, state, stream_id, 0)?;
        send_data(stream, state, stream_id, send_sequence, Vec::new(), true)?;
        send_sequence = next_sequence(send_sequence)?;
    } else {
        loop {
            let read = transfer
                .read_chunk(&mut buffer)
                .map_err(ServerError::Sync)?;
            if read == 0 {
                return Err(ServerError::UnexpectedFrame(
                    "sync source ended before its declared size",
                ));
            }
            let is_last = transfer.offset() == transfer.total_size();
            let needed = u32::try_from(read).map_err(|_| ServerError::InvalidConfig)?;
            wait_for_send_capacity(stream, state, stream_id, needed)?;
            if is_last {
                transfer.finish().map_err(ServerError::Sync)?;
            }
            send_data_buffered(
                stream,
                state,
                stream_id,
                send_sequence,
                buffer[..read].to_vec(),
                is_last,
            )?;
            send_sequence = next_sequence(send_sequence)?;
            batch_bytes = batch_bytes.saturating_add(u64::from(needed));
            if is_last {
                flush_outbound(stream)?;
                break;
            }
            if batch_bytes >= u64::from(SYNC_CREDIT_BATCH_SIZE) {
                flush_outbound(stream)?;
                wait_for_full_send_credit(stream, state, stream_id, config)?;
                transfer.checkpoint_if_due().map_err(ServerError::Sync)?;
                batch_bytes = 0;
            }
        }
    }
    send(
        stream,
        state,
        frame(Command::Close, stream_id, send_sequence, Vec::new())?,
        FrameContext::default(),
    )
}

fn dispatch(call: DeviceCall, device: &DeviceInfo, sync_store: &SyncStore) -> DeviceReply {
    if call.method == methods::SYNC_STATUS {
        let params = match serde_json::from_value::<SyncStatusParams>(call.params) {
            Ok(params) => params,
            Err(_) => {
                return DeviceReply::failure(RpcError::invalid_params(
                    "expected serial and transfer_id",
                ))
            }
        };
        if params.serial != device.serial {
            return DeviceReply::failure(RpcError::device_not_found(&params.serial));
        }
        return match sync_store.status(&params.transfer_id) {
            Ok(status) => match serde_json::to_value(status) {
                Ok(value) => DeviceReply::success(value),
                Err(_) => DeviceReply::failure(RpcError::internal_error()),
            },
            Err(error) => DeviceReply::failure(error.into_rpc()),
        };
    }
    if call.method != methods::EXEC_RUN {
        return DeviceReply::failure(RpcError::method_not_found(&call.method));
    }
    let params = match serde_json::from_value::<ExecParams>(call.params) {
        Ok(params) => params,
        Err(_) => {
            return DeviceReply::failure(RpcError::invalid_params(
                "expected serial, argv, cwd, environment, and timeout_ms",
            ))
        }
    };
    if params.serial != device.serial {
        return DeviceReply::failure(RpcError::device_not_found(&params.serial));
    }
    match exec::run(&params) {
        Ok(result) => match serde_json::to_value(result) {
            Ok(result) => DeviceReply::success(result),
            Err(_) => DeviceReply::failure(RpcError::internal_error()),
        },
        Err(ExecError::EmptyArgv | ExecError::InvalidTimeout) => {
            DeviceReply::failure(RpcError::invalid_params("invalid exec bounds"))
        }
        Err(ExecError::Timeout(timeout)) => DeviceReply::failure(
            RpcError::new(error_codes::EXEC_TIMEOUT, "Command timed out")
                .with_data(serde_json::json!({ "timeout_ms": timeout })),
        ),
        Err(ExecError::OutputLimit) => DeviceReply::failure(RpcError::new(
            error_codes::EXEC_OUTPUT_LIMIT,
            "Command output exceeds the device limit",
        )),
        Err(_) => DeviceReply::failure(RpcError::new(
            error_codes::EXEC_FAILED,
            "Command could not be executed",
        )),
    }
}

fn read_stream_data(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
) -> Result<Option<Frame>, ServerError> {
    loop {
        let Some(frame) = read_application_frame(stream, state)? else {
            return Ok(None);
        };
        if frame.header.command == Command::Credit && frame.header.stream_id == stream_id {
            state.process_inbound(&frame.header, FrameContext::default())?;
            continue;
        }
        expect(&frame, Command::Data, stream_id)?;
        state.process_inbound(&frame.header, FrameContext::default())?;
        return Ok(Some(frame));
    }
}

fn restore_received_credit(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
    stream_sequence: &mut u32,
    control_sequence: &mut u32,
    delta: u32,
) -> Result<(), ServerError> {
    if delta == 0 {
        return Ok(());
    }
    send_credit(stream, state, stream_id, *stream_sequence, delta)?;
    *stream_sequence = next_sequence(*stream_sequence)?;
    restore_connection_credit(stream, state, control_sequence, delta)
}

fn restore_connection_credit(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    control_sequence: &mut u32,
    delta: u32,
) -> Result<(), ServerError> {
    if delta == 0 {
        return Ok(());
    }
    send_credit(stream, state, 0, *control_sequence, delta)?;
    *control_sequence = next_sequence(*control_sequence)?;
    Ok(())
}

fn wait_for_send_capacity(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
    needed: u32,
) -> Result<(), ServerError> {
    loop {
        let stream_credit = state
            .stream(stream_id)
            .ok_or(ServerError::UnexpectedFrame("sync stream disappeared"))?
            .send_credit;
        let connection_credit = state.snapshot().connection_send_credit;
        if stream_credit >= needed && connection_credit >= needed {
            return Ok(());
        }
        read_credit(stream, state, stream_id)?;
    }
}

fn wait_for_full_send_credit(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
    config: &ServerConfig,
) -> Result<(), ServerError> {
    loop {
        let stream_credit = state
            .stream(stream_id)
            .ok_or(ServerError::UnexpectedFrame("sync stream disappeared"))?
            .send_credit;
        let connection_credit = state.snapshot().connection_send_credit;
        if stream_credit == config.stream_window && connection_credit == config.connection_window {
            return Ok(());
        }
        read_credit(stream, state, stream_id)?;
    }
}

fn read_credit(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
) -> Result<(), ServerError> {
    let frame = match stream.read_frame() {
        Ok(frame) => frame,
        Err(TransportError::EndOfStream) => return Err(ServerError::Disconnected),
        Err(error) => return Err(error.into()),
    };
    if is_fresh_hello(&frame) {
        return Err(ServerError::FreshHello(Box::new(frame)));
    }
    if frame.header.command != Command::Credit
        || (frame.header.stream_id != 0 && frame.header.stream_id != stream_id)
    {
        return Err(ServerError::UnexpectedFrame(
            "expected sync flow-control credit",
        ));
    }
    state.process_inbound(&frame.header, FrameContext::default())?;
    Ok(())
}

fn send_sync_failure(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
    sequence: u32,
    error: RpcError,
) -> Result<(), ServerError> {
    send_data(
        stream,
        state,
        stream_id,
        sequence,
        encode(&SyncReply::Failure { error })?,
        true,
    )?;
    send(
        stream,
        state,
        frame(
            Command::Close,
            stream_id,
            next_sequence(sequence)?,
            Vec::new(),
        )?,
        FrameContext::default(),
    )
}

fn send_data(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
    sequence: u32,
    payload: Vec<u8>,
    end_stream: bool,
) -> Result<(), ServerError> {
    let mut data = frame(Command::Data, stream_id, sequence, payload)?;
    if end_stream {
        data.header.flags = FLAG_END_STREAM;
    }
    send(stream, state, data, FrameContext::default())
}

fn send_data_buffered(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
    sequence: u32,
    payload: Vec<u8>,
    end_stream: bool,
) -> Result<(), ServerError> {
    let mut data = frame(Command::Data, stream_id, sequence, payload)?;
    if end_stream {
        data.header.flags = FLAG_END_STREAM;
    }
    send_buffered(stream, state, data, FrameContext::default())
}

fn next_sequence(sequence: u32) -> Result<u32, ServerError> {
    sequence
        .checked_add(1)
        .ok_or(ServerError::SequenceExhausted)
}

fn read_application_frame(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
) -> Result<Option<Frame>, ServerError> {
    loop {
        let frame = match stream.read_frame() {
            Ok(frame) => frame,
            Err(TransportError::EndOfStream) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if frame.header.command == Command::Credit && frame.header.stream_id == 0 {
            state.process_inbound(&frame.header, FrameContext::default())?;
            continue;
        }
        return Ok(Some(frame));
    }
}

fn send_credit(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    stream_id: u32,
    sequence: u32,
    delta: u32,
) -> Result<(), ServerError> {
    let mut header = Header::new(Command::Credit, stream_id, sequence);
    header.credit_delta = delta;
    send(
        stream,
        state,
        Frame::new(header, Vec::new())?,
        FrameContext::default(),
    )
}

fn send(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    frame: Frame,
    context: FrameContext,
) -> Result<(), ServerError> {
    send_buffered(stream, state, frame, context)?;
    flush_outbound(stream)
}

fn send_buffered(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    frame: Frame,
    context: FrameContext,
) -> Result<(), ServerError> {
    state.process_outbound(&frame.header, context)?;
    stream.write_frame(&frame)?;
    Ok(())
}

fn flush_outbound(stream: &mut dyn FrameIo) -> Result<(), ServerError> {
    stream.flush()?;
    Ok(())
}

fn expect(frame: &Frame, command: Command, stream_id: u32) -> Result<(), ServerError> {
    if frame.header.command == command && frame.header.stream_id == stream_id {
        Ok(())
    } else if is_fresh_hello(frame) {
        Err(ServerError::FreshHello(Box::new(frame.clone())))
    } else {
        Err(ServerError::UnexpectedFrame(
            "unexpected command or stream identifier",
        ))
    }
}

fn frame(
    command: Command,
    stream_id: u32,
    sequence: u32,
    payload: Vec<u8>,
) -> Result<Frame, WireError> {
    Frame::new(Header::new(command, stream_id, sequence), payload)
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, ServerError> {
    Ok(serde_json::to_vec(value)?)
}

fn decode<T: serde::de::DeserializeOwned>(
    payload: &[u8],
    label: &'static str,
) -> Result<T, ServerError> {
    serde_json::from_slice(payload).map_err(|source| ServerError::InvalidPayload { label, source })
}

fn validate_config(config: &ServerConfig) -> Result<(), ServerError> {
    if config.connection_window == 0
        || config.stream_window == 0
        || config.stream_window > config.connection_window
    {
        return Err(ServerError::InvalidConfig);
    }
    Ok(())
}

fn validate_hello(hello: &HostHello, config: &ServerConfig) -> Result<(), ServerError> {
    if hello.protocol_version != PROTOCOL_VERSION
        || !is_valid_session_id(&hello.session_id)
        || hello.client_name.is_empty()
        || hello.initial_connection_window == 0
        || hello.initial_connection_window > config.connection_window
    {
        return Err(ServerError::InvalidHello);
    }
    Ok(())
}

fn transport_config(config: &ServerConfig) -> TransportConfig {
    TransportConfig {
        read_timeout: Some(SESSION_IO_TIMEOUT),
        write_timeout: Some(SESSION_IO_TIMEOUT),
        ..TransportConfig::new(config.limits())
    }
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    FunctionFs(#[from] FunctionFsError),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Sync(#[from] StoreError),
    #[error("invalid {label} payload: {source}")]
    InvalidPayload {
        label: &'static str,
        source: serde_json::Error,
    },
    #[error("peer {0} is not allowlisted")]
    PeerNotAllowed(IpAddr),
    #[error("device link configuration is invalid")]
    InvalidConfig,
    #[error("host HELLO is incompatible")]
    InvalidHello,
    #[error("device link disconnected during a call")]
    Disconnected,
    #[error("a fresh USB session started while the previous session was active")]
    FreshHello(Box<Frame>),
    #[error("{0}")]
    UnexpectedFrame(&'static str),
    #[error("sequence space exhausted")]
    SequenceExhausted,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::fs;
    use std::io::{self, Cursor};

    use kindlebridge_transport_tcp::SplitFrameStream;

    use super::*;

    const TEST_SESSION_ID: &str = "000102030405060708090a0b0c0d0e0f";

    struct ScriptedFrameIo {
        reads: VecDeque<Result<Frame, TransportError>>,
        writes: Vec<Frame>,
    }

    impl FrameIo for ScriptedFrameIo {
        fn read_frame(&mut self) -> Result<Frame, TransportError> {
            self.reads
                .pop_front()
                .unwrap_or(Err(TransportError::EndOfStream))
        }

        fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
            self.writes.push(frame.clone());
            Ok(())
        }

        fn flush(&mut self) -> Result<(), TransportError> {
            Ok(())
        }
    }

    #[test]
    fn active_usb_transport_errors_discard_stale_frames_until_a_fresh_hello() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-USB-RECOVERY"));
        let hello = || {
            frame(
                Command::Hello,
                0,
                0,
                encode(&HostHello {
                    protocol_version: PROTOCOL_VERSION,
                    session_id: TEST_SESSION_ID.to_owned(),
                    client_name: "usb-recovery-test".to_owned(),
                    initial_connection_window: DEFAULT_CONNECTION_WINDOW,
                })
                .unwrap(),
            )
            .unwrap()
        };
        let open_sync = frame(
            Command::Open,
            1,
            0,
            encode(&ServiceOpen {
                service: SYNC_SERVICE.to_owned(),
            })
            .unwrap(),
        )
        .unwrap();
        for (kind, message) in [
            (io::ErrorKind::TimedOut, "simulated FunctionFS timeout"),
            (io::ErrorKind::BrokenPipe, "simulated FunctionFS disconnect"),
        ] {
            let transport_error = TransportError::Io {
                operation: kindlebridge_transport_tcp::IoOperation::ReadHeader,
                source: io::Error::new(kind, message),
            };
            let mut stream = ScriptedFrameIo {
                reads: VecDeque::from([
                    Ok(hello()),
                    Ok(open_sync.clone()),
                    Err(transport_error),
                    Ok(frame(Command::Data, 1, 3, vec![0; 1024]).unwrap()),
                    Ok(frame(Command::Credit, 1, 4, Vec::new()).unwrap()),
                    Ok(hello()),
                    Ok(frame(Command::GoAway, 0, 1, Vec::new()).unwrap()),
                ]),
                writes: Vec::new(),
            };

            serve_session(
                &mut stream,
                &config,
                &SyncStore::new(config.sync_root.clone()),
                true,
            )
            .unwrap();
            assert_eq!(
                stream
                    .writes
                    .iter()
                    .filter(|frame| frame.header.command == Command::Hello)
                    .count(),
                2
            );
        }
    }

    #[test]
    fn functionfs_lifecycle_abort_reopens_endpoints_instead_of_resynchronizing_in_place() {
        let lifecycle_abort = TransportError::Io {
            operation: kindlebridge_transport_tcp::IoOperation::ReadHeader,
            source: io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "simulated FunctionFS lifecycle event",
            ),
        };

        assert!(!transport_error_allows_in_place_restart(&lifecycle_abort));
    }

    #[test]
    fn top_level_usb_recovery_returns_to_hello_before_reading_the_ping_marker() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-USB-PING-RECOVERY"));
        let hello = || {
            frame(
                Command::Hello,
                0,
                0,
                encode(&HostHello {
                    protocol_version: PROTOCOL_VERSION,
                    session_id: TEST_SESSION_ID.to_owned(),
                    client_name: "usb-ping-recovery-test".to_owned(),
                    initial_connection_window: DEFAULT_CONNECTION_WINDOW,
                })
                .unwrap(),
            )
            .unwrap()
        };
        let invalid_magic = TransportError::Io {
            operation: kindlebridge_transport_tcp::IoOperation::ReadHeader,
            source: io::Error::new(io::ErrorKind::InvalidData, "abandoned frame"),
        };
        let mut stream = ScriptedFrameIo {
            reads: VecDeque::from([
                Ok(hello()),
                Err(invalid_magic),
                Ok(frame(Command::Ping, 0, 0, Vec::new()).unwrap()),
                Ok(hello()),
                Ok(frame(Command::GoAway, 0, 1, Vec::new()).unwrap()),
            ]),
            writes: Vec::new(),
        };

        serve_session(
            &mut stream,
            &config,
            &SyncStore::new(config.sync_root.clone()),
            true,
        )
        .unwrap();
        assert_eq!(
            stream
                .writes
                .iter()
                .filter(|frame| frame.header.command == Command::Hello)
                .count(),
            2
        );
    }

    #[test]
    fn fresh_hello_interrupting_an_active_usb_stream_is_not_lost() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-USB-RESTART"));
        let hello = || {
            frame(
                Command::Hello,
                0,
                0,
                encode(&HostHello {
                    protocol_version: PROTOCOL_VERSION,
                    session_id: TEST_SESSION_ID.to_owned(),
                    client_name: "usb-restart-test".to_owned(),
                    initial_connection_window: DEFAULT_CONNECTION_WINDOW,
                })
                .unwrap(),
            )
            .unwrap()
        };
        let mut stream = ScriptedFrameIo {
            reads: VecDeque::from([
                Ok(hello()),
                Ok(frame(
                    Command::Open,
                    1,
                    0,
                    encode(&ServiceOpen {
                        service: SYNC_SERVICE.to_owned(),
                    })
                    .unwrap(),
                )
                .unwrap()),
                Ok(hello()),
                Ok(frame(Command::GoAway, 0, 1, Vec::new()).unwrap()),
            ]),
            writes: Vec::new(),
        };

        serve_session(
            &mut stream,
            &config,
            &SyncStore::new(config.sync_root.clone()),
            true,
        )
        .unwrap();
        assert_eq!(
            stream
                .writes
                .iter()
                .filter(|frame| frame.header.command == Command::Hello)
                .count(),
            2
        );
    }

    #[test]
    fn split_functionfs_endpoints_serve_a_device_session() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-USB-SERVER"))
            .sync_root(std::env::temp_dir().join("kindlebridge-usb-session-test"));
        let host_hello = HostHello {
            protocol_version: PROTOCOL_VERSION,
            session_id: TEST_SESSION_ID.to_owned(),
            client_name: "usb-test".to_owned(),
            initial_connection_window: DEFAULT_CONNECTION_WINDOW,
        };
        let mut incoming = frame(Command::Hello, 0, 0, encode(&host_hello).unwrap())
            .unwrap()
            .encode(config.limits())
            .unwrap();
        incoming.extend_from_slice(
            &frame(Command::GoAway, 0, 1, Vec::new())
                .unwrap()
                .encode(config.limits())
                .unwrap(),
        );
        let mut stream = SplitFrameStream::new(
            Cursor::new(incoming),
            Cursor::new(Vec::<u8>::new()),
            transport_config(&config),
        )
        .unwrap();
        serve_session(
            &mut stream,
            &config,
            &SyncStore::new(config.sync_root.clone()),
            false,
        )
        .unwrap();

        let (_, output) = stream.into_inner();
        let mut reader = kindlebridge_transport_tcp::FrameReader::new(
            Cursor::new(output.into_inner()),
            transport_config(&config),
        )
        .unwrap();
        let hello = reader.read_frame().unwrap();
        expect(&hello, Command::Hello, 0).unwrap();
        let device: DeviceHello = decode(&hello.payload, "device HELLO").unwrap();
        assert_eq!(device.serial, "KT6-USB-SERVER");
        assert_eq!(device.session_id, TEST_SESSION_ID);
    }

    #[test]
    fn usb_style_sync_push_batches_received_credit() {
        let root = std::env::temp_dir().join(format!(
            "kindlebridge-usb-credit-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-USB-CREDIT")).sync_root(root.clone());
        let payload = vec![0x5a; SYNC_CREDIT_BATCH_SIZE as usize];
        let request = SyncRequest::Push {
            transfer_id: None,
            remote_path: "credit/payload.bin".to_owned(),
            total_size: payload.len() as u64,
            file_hash: blake3::hash(&payload).to_hex().to_string(),
            block_size: 64 * 1024,
        };
        let host_hello = HostHello {
            protocol_version: PROTOCOL_VERSION,
            session_id: TEST_SESSION_ID.to_owned(),
            client_name: "usb-credit-test".to_owned(),
            initial_connection_window: DEFAULT_CONNECTION_WINDOW,
        };
        let mut incoming = Vec::new();
        let mut append = |frame: Frame| {
            incoming.extend_from_slice(&frame.encode(config.limits()).unwrap());
        };
        append(frame(Command::Hello, 0, 0, encode(&host_hello).unwrap()).unwrap());
        append(
            frame(
                Command::Open,
                1,
                0,
                encode(&ServiceOpen {
                    service: SYNC_SERVICE.to_owned(),
                })
                .unwrap(),
            )
            .unwrap(),
        );
        let mut response_credit = Header::new(Command::Credit, 1, 1);
        response_credit.credit_delta = DEFAULT_STREAM_WINDOW;
        append(Frame::new(response_credit, Vec::new()).unwrap());
        append(frame(Command::Data, 1, 2, encode(&request).unwrap()).unwrap());
        for (index, chunk) in payload.chunks(64 * 1024).enumerate() {
            let mut data = frame(Command::Data, 1, 3 + index as u32, chunk.to_vec()).unwrap();
            if index + 1 == payload.len() / (64 * 1024) {
                data.header.flags = FLAG_END_STREAM;
            }
            append(data);
        }
        append(frame(Command::GoAway, 0, 1, Vec::new()).unwrap());

        let mut stream = SplitFrameStream::new(
            Cursor::new(incoming),
            Cursor::new(Vec::<u8>::new()),
            transport_config(&config),
        )
        .unwrap();
        serve_session(
            &mut stream,
            &config,
            &SyncStore::new(config.sync_root.clone()),
            false,
        )
        .unwrap();

        let (_, output) = stream.into_inner();
        let mut reader = kindlebridge_transport_tcp::FrameReader::new(
            Cursor::new(output.into_inner()),
            transport_config(&config),
        )
        .unwrap();
        let mut batched_credits = 0;
        while let Ok(frame) = reader.read_frame() {
            if frame.header.command == Command::Credit
                && frame.header.credit_delta == SYNC_CREDIT_BATCH_SIZE
            {
                batched_credits += 1;
            }
        }
        assert_eq!(batched_credits, 2, "stream and connection credit");
        assert_eq!(fs::read(root.join("credit/payload.bin")).unwrap(), payload);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interrupted_sync_session_rolls_back_its_last_data_frame() {
        let root = std::env::temp_dir().join(format!(
            "kindlebridge-usb-frame-rollback-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-USB-ROLLBACK")).sync_root(root.clone());
        let payload = vec![0x35; 2 * 64 * 1024];
        let request = SyncRequest::Push {
            transfer_id: None,
            remote_path: "rollback/payload.bin".to_owned(),
            total_size: payload.len() as u64,
            file_hash: blake3::hash(&payload).to_hex().to_string(),
            block_size: 64 * 1024,
        };
        let host_hello = HostHello {
            protocol_version: PROTOCOL_VERSION,
            session_id: TEST_SESSION_ID.to_owned(),
            client_name: "usb-frame-rollback-test".to_owned(),
            initial_connection_window: DEFAULT_CONNECTION_WINDOW,
        };
        let mut incoming = Vec::new();
        let mut append = |frame: Frame| {
            incoming.extend_from_slice(&frame.encode(config.limits()).unwrap());
        };
        append(frame(Command::Hello, 0, 0, encode(&host_hello).unwrap()).unwrap());
        append(
            frame(
                Command::Open,
                1,
                0,
                encode(&ServiceOpen {
                    service: SYNC_SERVICE.to_owned(),
                })
                .unwrap(),
            )
            .unwrap(),
        );
        let mut response_credit = Header::new(Command::Credit, 1, 1);
        response_credit.credit_delta = DEFAULT_STREAM_WINDOW;
        append(Frame::new(response_credit, Vec::new()).unwrap());
        append(frame(Command::Data, 1, 2, encode(&request).unwrap()).unwrap());
        append(frame(Command::Data, 1, 3, payload[..64 * 1024].to_vec()).unwrap());
        append(frame(Command::Ping, 0, 1, Vec::new()).unwrap());

        let mut stream = SplitFrameStream::new(
            Cursor::new(incoming),
            Cursor::new(Vec::<u8>::new()),
            transport_config(&config),
        )
        .unwrap();
        assert!(matches!(
            serve_session(
                &mut stream,
                &config,
                &SyncStore::new(config.sync_root.clone()),
                false,
            ),
            Err(ServerError::UnexpectedFrame(_))
        ));

        let staging = root.join(".kindlebridge-sync");
        let part = fs::read_dir(&staging)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "part")
            })
            .unwrap();
        assert_eq!(fs::metadata(part).unwrap().len(), 0);
        let metadata = fs::read_dir(staging)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fs::read(metadata).unwrap()).unwrap()
                ["next_offset"],
            0
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn dispatch_exec_preserves_typed_result() {
        let executable = std::env::current_exe().unwrap();
        let params = ExecParams {
            serial: "KT6-LINK".to_owned(),
            argv: vec![
                executable.to_string_lossy().into_owned(),
                "--list".to_owned(),
            ],
            cwd: None,
            environment: BTreeMap::new(),
            timeout_ms: 10_000,
        };
        let reply = dispatch(
            DeviceCall {
                method: methods::EXEC_RUN.to_owned(),
                params: serde_json::to_value(params).unwrap(),
            },
            &DeviceInfo::kt6("KT6-LINK"),
            &SyncStore::new(std::env::temp_dir().join("kindlebridge-dispatch-test")),
        );
        let result = reply.into_result().unwrap();
        assert_eq!(result["exit_code"], 0);
        assert!(result["stdout"]
            .as_str()
            .unwrap()
            .contains("dispatch_exec_preserves_typed_result"));
    }

    #[test]
    fn wrong_serial_is_a_stable_device_error() {
        let reply = dispatch(
            DeviceCall {
                method: methods::EXEC_RUN.to_owned(),
                params: serde_json::json!({
                    "serial": "OTHER",
                    "argv": ["unused"],
                    "environment": {},
                    "timeout_ms": 1
                }),
            },
            &DeviceInfo::kt6("KT6-LINK"),
            &SyncStore::new(std::env::temp_dir().join("kindlebridge-dispatch-test")),
        );
        assert_eq!(
            reply.into_result().unwrap_err().code,
            error_codes::DEVICE_NOT_FOUND
        );
    }
}
