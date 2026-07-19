//! Persistent development KBP listener for the unprivileged device daemon.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::mpsc::{self, TryRecvError, TrySendError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use kindlebridge_functionfs::{FunctionFsDevice, FunctionFsError, FunctionFsFrameStream};
use kindlebridge_schema::device_protocol::{
    is_valid_session_id, DeviceCall, DeviceHello, DeviceReply, HostHello, ServiceAccept,
    ServiceOpen, SyncReply, SyncRequest, APP_INSTALL_FEATURE, APP_LIST_FEATURE,
    APP_RESTART_FEATURE, APP_ROLLBACK_FEATURE, APP_START_FEATURE, APP_STOP_FEATURE,
    APP_UNINSTALL_FEATURE, DEFAULT_CONNECTION_WINDOW, DEFAULT_STREAM_WINDOW, EXEC_FEATURE,
    LOG_TAIL_FEATURE, PROCESS_LIST_FEATURE, PROCESS_SIGNAL_FEATURE, PROTOCOL_VERSION,
    SHELL_SERVICE, SYNC_CREDIT_BATCH_SIZE, SYNC_FEATURE, SYNC_SERVICE,
};
use kindlebridge_schema::{
    error_codes, methods, AppInstallParams, AppTargetParams, ExecParams, LogTailParams,
    ProcessSignalParams, RpcError, SerialParams, SyncStatusParams, MAX_SYNC_BLOCK_SIZE,
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
use crate::services;
use crate::sync::{PullTransfer, PushTransfer, StoreError, SyncStore};
use crate::DeviceInfo;

const SESSION_IO_TIMEOUT: Duration = Duration::from_secs(10 * 60 + 30);
const DEFAULT_SYNC_ROOT: &str = "/mnt/us/kindlebridge-data";
const DEFAULT_ACTIVATION_ROOT: &str = "/var/local/kindlebridge/activations";
const DEFAULT_PROC_ROOT: &str = "/proc";
const DEFAULT_LOG_PATH: &str = "/var/local/kindlebridge/usb.log";
const SYNC_PUSH_QUEUE_DEPTH: usize = 3;

#[derive(Debug)]
enum PipelineFailure<E> {
    Stage(E),
    WorkerStopped,
    WorkerPanicked,
}

struct PipelineOutcome<A, E> {
    result: Result<(), PipelineFailure<E>>,
    last_written: Option<A>,
}

fn run_bounded_pipeline<C, T, A, E, Read, Write, Stored, MustDrain>(
    context: &mut C,
    mut read: Read,
    mut write: Write,
    mut stored: Stored,
    mut must_drain: MustDrain,
) -> PipelineOutcome<A, E>
where
    Read: FnMut(&mut C) -> Result<Option<T>, E>,
    Write: FnMut(T) -> Result<A, E> + Send,
    Stored: FnMut(&mut C, A) -> Result<(), E>,
    MustDrain: FnMut(&C) -> bool,
    T: Send,
    A: Clone + Send,
    E: Send,
{
    let (work_tx, work_rx) = mpsc::sync_channel::<T>(SYNC_PUSH_QUEUE_DEPTH);
    let (ack_tx, ack_rx) = mpsc::sync_channel::<Result<A, E>>(SYNC_PUSH_QUEUE_DEPTH);

    thread::scope(|scope| {
        let writer = scope.spawn(move || {
            let mut last_written = None;
            while let Ok(item) = work_rx.recv() {
                match write(item) {
                    Ok(acknowledgement) => {
                        last_written = Some(acknowledgement.clone());
                        if ack_tx.send(Ok(acknowledgement)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = ack_tx.send(Err(error));
                        break;
                    }
                }
            }
            last_written
        });

        let mut result = 'produce: loop {
            loop {
                match ack_rx.try_recv() {
                    Ok(Ok(acknowledgement)) => {
                        if let Err(error) = stored(context, acknowledgement) {
                            break 'produce Err(PipelineFailure::Stage(error));
                        }
                    }
                    Ok(Err(error)) => break 'produce Err(PipelineFailure::Stage(error)),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        break 'produce Err(PipelineFailure::WorkerStopped)
                    }
                }
            }

            let mut pending = match read(context) {
                Ok(Some(item)) => item,
                Ok(None) => break Ok(()),
                Err(error) => break Err(PipelineFailure::Stage(error)),
            };
            loop {
                match work_tx.try_send(pending) {
                    Ok(()) => {
                        while must_drain(context) {
                            match ack_rx.recv() {
                                Ok(Ok(acknowledgement)) => {
                                    if let Err(error) = stored(context, acknowledgement) {
                                        break 'produce Err(PipelineFailure::Stage(error));
                                    }
                                }
                                Ok(Err(error)) => {
                                    break 'produce Err(PipelineFailure::Stage(error))
                                }
                                Err(_) => break 'produce Err(PipelineFailure::WorkerStopped),
                            }
                        }
                        break;
                    }
                    Err(TrySendError::Full(item)) => {
                        pending = item;
                        match ack_rx.recv() {
                            Ok(Ok(acknowledgement)) => {
                                if let Err(error) = stored(context, acknowledgement) {
                                    break 'produce Err(PipelineFailure::Stage(error));
                                }
                            }
                            Ok(Err(error)) => break 'produce Err(PipelineFailure::Stage(error)),
                            Err(_) => break 'produce Err(PipelineFailure::WorkerStopped),
                        }
                    }
                    Err(TrySendError::Disconnected(_)) => loop {
                        match ack_rx.recv() {
                            Ok(Ok(acknowledgement)) => {
                                if let Err(error) = stored(context, acknowledgement) {
                                    break 'produce Err(PipelineFailure::Stage(error));
                                }
                            }
                            Ok(Err(error)) => break 'produce Err(PipelineFailure::Stage(error)),
                            Err(_) => break 'produce Err(PipelineFailure::WorkerStopped),
                        }
                    },
                }
            }
        };

        drop(work_tx);
        if result.is_ok() {
            loop {
                match ack_rx.recv() {
                    Ok(Ok(acknowledgement)) => {
                        if let Err(error) = stored(context, acknowledgement) {
                            result = Err(PipelineFailure::Stage(error));
                            break;
                        }
                    }
                    Ok(Err(error)) => {
                        result = Err(PipelineFailure::Stage(error));
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
        drop(ack_rx);

        match writer.join() {
            Ok(last_written) => PipelineOutcome {
                result,
                last_written,
            },
            Err(_) => PipelineOutcome {
                result: Err(PipelineFailure::WorkerPanicked),
                last_written: None,
            },
        }
    })
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub device: DeviceInfo,
    pub allowed_peer: Option<IpAddr>,
    pub connection_window: u32,
    pub stream_window: u32,
    pub sync_root: PathBuf,
    pub activation_root: PathBuf,
    pub proc_root: PathBuf,
    pub log_path: PathBuf,
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
            activation_root: PathBuf::from(DEFAULT_ACTIVATION_ROOT),
            proc_root: PathBuf::from(DEFAULT_PROC_ROOT),
            log_path: PathBuf::from(DEFAULT_LOG_PATH),
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

    #[must_use]
    pub fn activation_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.activation_root = root.into();
        self
    }

    #[must_use]
    pub fn proc_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.proc_root = root.into();
        self
    }

    #[must_use]
    pub fn log_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.log_path = path.into();
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
            features: vec![
                APP_LIST_FEATURE.to_owned(),
                EXEC_FEATURE.to_owned(),
                LOG_TAIL_FEATURE.to_owned(),
                PROCESS_LIST_FEATURE.to_owned(),
                SYNC_FEATURE.to_owned(),
            ],
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
    let mut reply = dispatch(call, config, sync_store);
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

struct PushChunk {
    payload: Arc<[u8]>,
    payload_length: u32,
    is_last: bool,
}

#[derive(Clone, Copy)]
struct StoredPushChunk {
    frame_start: u64,
    payload_length: u32,
    is_last: bool,
}

struct PushReceiveContext<'a> {
    stream: &'a mut dyn FrameIo,
    state: &'a mut SessionState,
    control_sequence: &'a mut u32,
    stream_id: u32,
    send_sequence: u32,
    received_batch: u64,
    received_since_credit: u64,
    credit_barrier: bool,
    block_size: usize,
    end_stream_received: bool,
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

    if transfer.is_complete() {
        let completion =
            read_stream_data(stream, state, stream_id)?.ok_or(ServerError::Disconnected)?;
        if !completion.payload.is_empty() || completion.header.flags & FLAG_END_STREAM == 0 {
            return Err(ServerError::UnexpectedFrame(
                "completed sync push must end with an empty DATA frame",
            ));
        }
        let status = transfer.finish().map_err(ServerError::Sync)?;
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
        return send(
            stream,
            state,
            frame(Command::Close, stream_id, send_sequence, Vec::new())?,
            FrameContext::default(),
        );
    }

    let block_size = usize::try_from(block_size).map_err(|_| ServerError::InvalidConfig)?;
    let hash_state = transfer.hash_state();
    let mut context = PushReceiveContext {
        stream,
        state,
        control_sequence,
        stream_id,
        send_sequence,
        received_batch: 0,
        received_since_credit: 0,
        credit_barrier: false,
        block_size,
        end_stream_received: false,
    };
    // Hashing receives the same reference as the storage worker through a
    // zero-capacity handoff. This overlaps USB, BLAKE3 and storage without
    // retaining another queued payload or weakening final verification.
    let (pipeline, digest_result) = thread::scope(|scope| {
        let (hash_tx, hash_rx) = mpsc::sync_channel::<Arc<[u8]>>(0);
        let hash_worker = scope.spawn(move || {
            let mut hasher = hash_state;
            while let Ok(payload) = hash_rx.recv() {
                hasher.update(&payload);
            }
            hasher.finalize()
        });
        let pipeline = run_bounded_pipeline(
            &mut context,
            |context| {
                if context.end_stream_received {
                    return Ok(None);
                }
                let data = read_stream_data(context.stream, context.state, context.stream_id)?
                    .ok_or(ServerError::Disconnected)?;
                if data.payload.len() > context.block_size {
                    return Err(ServerError::UnexpectedFrame(
                        "sync DATA exceeds the negotiated block size",
                    ));
                }
                let is_last = data.header.flags & FLAG_END_STREAM != 0;
                context.end_stream_received = is_last;
                context.received_since_credit = context
                    .received_since_credit
                    .checked_add(u64::from(data.header.payload_length))
                    .ok_or(ServerError::SequenceExhausted)?;
                if context.received_since_credit >= u64::from(SYNC_CREDIT_BATCH_SIZE) || is_last {
                    context.credit_barrier = true;
                }
                Ok(Some(PushChunk {
                    payload: Arc::from(data.payload),
                    payload_length: data.header.payload_length,
                    is_last,
                }))
            },
            |chunk| {
                hash_tx
                    .send(Arc::clone(&chunk.payload))
                    .map_err(|_| ServerError::UnexpectedFrame("sync hash worker stopped"))?;
                let frame_start = transfer.offset();
                transfer.write_chunk_without_hash(&chunk.payload)?;
                Ok(StoredPushChunk {
                    frame_start,
                    payload_length: chunk.payload_length,
                    is_last: chunk.is_last,
                })
            },
            |context, stored| {
                context.received_batch = context
                    .received_batch
                    .checked_add(u64::from(stored.payload_length))
                    .ok_or(ServerError::SequenceExhausted)?;
                if context.received_batch >= u64::from(SYNC_CREDIT_BATCH_SIZE) || stored.is_last {
                    let delta = u32::try_from(context.received_batch).map_err(|_| {
                        ServerError::UnexpectedFrame("sync credit batch is too large")
                    })?;
                    restore_received_credit(
                        context.stream,
                        context.state,
                        context.stream_id,
                        &mut context.send_sequence,
                        context.control_sequence,
                        delta,
                    )?;
                    context.received_batch = 0;
                    context.received_since_credit = 0;
                    context.credit_barrier = false;
                }
                Ok(())
            },
            |context| context.credit_barrier,
        );
        drop(hash_tx);
        let digest = hash_worker
            .join()
            .map_err(|_| ServerError::UnexpectedFrame("sync hash worker panicked"));
        (pipeline, digest)
    });
    send_sequence = context.send_sequence;
    let last_frame_start = pipeline.last_written.map(|stored| stored.frame_start);
    let receive_result = match pipeline.result {
        Ok(()) => Ok(()),
        Err(PipelineFailure::Stage(error)) => Err(error),
        Err(PipelineFailure::WorkerStopped) => Err(ServerError::UnexpectedFrame(
            "sync storage worker stopped before the transfer completed",
        )),
        Err(PipelineFailure::WorkerPanicked) => {
            Err(ServerError::UnexpectedFrame("sync storage worker panicked"))
        }
    };

    let digest = match receive_result.and(digest_result) {
        Ok(digest) => digest,
        Err(receive_error) => {
            if let Some(offset) = last_frame_start {
                transfer.rollback_for_resume(offset)?;
            } else {
                transfer.checkpoint()?;
            }
            return Err(receive_error);
        }
    };

    let status = match transfer.finish_with_digest(digest) {
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

fn dispatch(call: DeviceCall, config: &ServerConfig, sync_store: &SyncStore) -> DeviceReply {
    match call.method.as_str() {
        methods::SYNC_STATUS => reply(dispatch_sync_status(call.params, config, sync_store)),
        methods::EXEC_RUN => reply(dispatch_exec(call.params, config)),
        methods::APP_LIST => reply(dispatch_app_list(call.params, config)),
        methods::PROCESS_LIST => reply(dispatch_process_list(call.params, config)),
        methods::LOG_TAIL => reply(dispatch_log_tail(call.params, config)),
        methods::APP_INSTALL => reply(unavailable_app_install(call.params, config)),
        methods::APP_START => reply(unavailable_app_target(
            call.params,
            config,
            APP_START_FEATURE,
        )),
        methods::APP_STOP => reply(unavailable_app_target(
            call.params,
            config,
            APP_STOP_FEATURE,
        )),
        methods::APP_RESTART => reply(unavailable_app_target(
            call.params,
            config,
            APP_RESTART_FEATURE,
        )),
        methods::APP_ROLLBACK => reply(unavailable_app_target(
            call.params,
            config,
            APP_ROLLBACK_FEATURE,
        )),
        methods::APP_UNINSTALL => reply(unavailable_app_target(
            call.params,
            config,
            APP_UNINSTALL_FEATURE,
        )),
        methods::PROCESS_SIGNAL => reply(unavailable_process_signal(call.params, config)),
        _ => DeviceReply::failure(RpcError::method_not_found(&call.method)),
    }
}

fn reply<T: Serialize>(result: Result<T, RpcError>) -> DeviceReply {
    match result
        .and_then(|value| serde_json::to_value(value).map_err(|_| RpcError::internal_error()))
    {
        Ok(value) => DeviceReply::success(value),
        Err(error) => DeviceReply::failure(error),
    }
}

fn dispatch_sync_status(
    params: serde_json::Value,
    config: &ServerConfig,
    sync_store: &SyncStore,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<SyncStatusParams>(params, "expected serial and transfer_id")?;
    require_serial(&params.serial, config)?;
    sync_store
        .status(&params.transfer_id)
        .map_err(StoreError::into_rpc)
}

fn dispatch_exec(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<ExecParams>(
        params,
        "expected serial, argv, cwd, environment, and timeout_ms",
    )?;
    require_serial(&params.serial, config)?;
    match exec::run(&params) {
        Ok(result) => Ok(result),
        Err(ExecError::EmptyArgv | ExecError::InvalidTimeout) => {
            Err(RpcError::invalid_params("invalid exec bounds"))
        }
        Err(ExecError::Timeout(timeout)) => Err(RpcError::new(
            error_codes::EXEC_TIMEOUT,
            "Command timed out",
        )
        .with_data(serde_json::json!({ "timeout_ms": timeout }))),
        Err(ExecError::OutputLimit) => Err(RpcError::new(
            error_codes::EXEC_OUTPUT_LIMIT,
            "Command output exceeds the device limit",
        )),
        Err(_) => Err(RpcError::new(
            error_codes::EXEC_FAILED,
            "Command could not be executed",
        )),
    }
}

fn dispatch_app_list(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<SerialParams>(params, "expected serial")?;
    require_serial(&params.serial, config)?;
    services::app_list(&config.activation_root)
}

fn dispatch_process_list(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<SerialParams>(params, "expected serial")?;
    require_serial(&params.serial, config)?;
    services::process_list(&config.proc_root)
}

fn dispatch_log_tail(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<LogTailParams>(params, "expected serial, cursor, and limit")?;
    require_serial(&params.serial, config)?;
    services::log_tail(&config.log_path, &params)
}

fn unavailable_app_install(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<serde_json::Value, RpcError> {
    let params = decode_params::<AppInstallParams>(params, "expected serial, app_id, and version")?;
    require_serial(&params.serial, config)?;
    Err(RpcError::feature_unavailable(
        &params.serial,
        APP_INSTALL_FEATURE,
    ))
}

fn unavailable_app_target(
    params: serde_json::Value,
    config: &ServerConfig,
    feature: &str,
) -> Result<serde_json::Value, RpcError> {
    let params = decode_params::<AppTargetParams>(params, "expected serial and app_id")?;
    require_serial(&params.serial, config)?;
    Err(RpcError::feature_unavailable(&params.serial, feature))
}

fn unavailable_process_signal(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<serde_json::Value, RpcError> {
    let params = decode_params::<ProcessSignalParams>(params, "expected serial, pid, and signal")?;
    require_serial(&params.serial, config)?;
    Err(RpcError::feature_unavailable(
        &params.serial,
        PROCESS_SIGNAL_FEATURE,
    ))
}

fn decode_params<T: serde::de::DeserializeOwned>(
    params: serde_json::Value,
    detail: &'static str,
) -> Result<T, RpcError> {
    serde_json::from_value(params).map_err(|_| RpcError::invalid_params(detail))
}

fn require_serial(serial: &str, config: &ServerConfig) -> Result<(), RpcError> {
    if serial == config.device.serial {
        Ok(())
    } else {
        Err(RpcError::device_not_found(serial))
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc, Condvar, Mutex};
    use std::time::Duration;

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

    struct TrackedChunk {
        index: usize,
        live: Arc<AtomicUsize>,
    }

    impl TrackedChunk {
        fn new(index: usize, live: Arc<AtomicUsize>, maximum: &AtomicUsize) -> Self {
            let current = live.fetch_add(1, Ordering::SeqCst) + 1;
            maximum.fetch_max(current, Ordering::SeqCst);
            Self { index, live }
        }
    }

    impl Drop for TrackedChunk {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn bounded_pipeline_overlaps_reads_with_slow_storage_without_unbounded_payloads() {
        const CHUNK_COUNT: usize = 12;
        let live = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let overlapped = Arc::new(AtomicBool::new(false));
        let written = Arc::new(AtomicUsize::new(0));
        let write_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (writer_started_tx, writer_started_rx) = mpsc::sync_channel(1);
        let mut next = 0;
        let mut stored_count = 0;

        let outcome: PipelineOutcome<usize, &'static str> = run_bounded_pipeline(
            &mut stored_count,
            {
                let live = Arc::clone(&live);
                let maximum = Arc::clone(&maximum);
                let write_gate = Arc::clone(&write_gate);
                move |_| {
                    if next == 1 {
                        writer_started_rx
                            .recv_timeout(Duration::from_secs(1))
                            .map_err(|_| "writer did not start")?;
                        let (ready, condition) = &*write_gate;
                        *ready.lock().map_err(|_| "write gate poisoned")? = true;
                        condition.notify_one();
                    }
                    if next == CHUNK_COUNT {
                        return Ok(None);
                    }
                    let chunk = TrackedChunk::new(next, Arc::clone(&live), &maximum);
                    next += 1;
                    Ok(Some(chunk))
                }
            },
            {
                let overlapped = Arc::clone(&overlapped);
                let write_gate = Arc::clone(&write_gate);
                let written = Arc::clone(&written);
                move |chunk: TrackedChunk| {
                    if chunk.index == 0 {
                        writer_started_tx.send(()).map_err(|_| "reader stopped")?;
                        let (ready, condition) = &*write_gate;
                        let mut guard = ready.lock().map_err(|_| "write gate poisoned")?;
                        if !*guard {
                            (guard, _) = condition
                                .wait_timeout(guard, Duration::from_millis(250))
                                .map_err(|_| "write gate poisoned")?;
                        }
                        overlapped.store(*guard, Ordering::SeqCst);
                    }
                    thread::sleep(Duration::from_millis(5));
                    written.fetch_add(1, Ordering::SeqCst);
                    Ok(chunk.index)
                }
            },
            {
                let written = Arc::clone(&written);
                move |stored_count, _| {
                    assert!(
                        *stored_count < written.load(Ordering::SeqCst),
                        "storage acknowledgement ran before the write completed"
                    );
                    *stored_count += 1;
                    Ok(())
                }
            },
            |_| false,
        );

        assert!(
            outcome.result.is_ok(),
            "pipeline failed: {:?}",
            outcome.result
        );
        assert_eq!(stored_count, CHUNK_COUNT);
        assert!(
            overlapped.load(Ordering::SeqCst),
            "the next USB read did not run while storage was blocked"
        );
        assert!(
            maximum.load(Ordering::SeqCst) <= SYNC_PUSH_QUEUE_DEPTH + 2,
            "payload ownership exceeded the queue, active writer, and producer slots"
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn bounded_pipeline_stops_at_storage_error_and_discards_queued_payloads() {
        let attempted = Arc::new(Mutex::new(Vec::new()));
        let mut next = 0_usize;
        let mut stored = Vec::new();
        let outcome: PipelineOutcome<usize, &'static str> = run_bounded_pipeline(
            &mut stored,
            |_| {
                if next == 12 {
                    Ok(None)
                } else {
                    let item = next;
                    next += 1;
                    Ok(Some(item))
                }
            },
            {
                let attempted = Arc::clone(&attempted);
                move |item| {
                    attempted.lock().unwrap().push(item);
                    if item == 2 {
                        Err("simulated storage failure")
                    } else {
                        Ok(item)
                    }
                }
            },
            |stored, item| {
                stored.push(item);
                Ok(())
            },
            |_| false,
        );

        assert!(matches!(
            outcome.result,
            Err(PipelineFailure::Stage("simulated storage failure"))
        ));
        assert_eq!(outcome.last_written, Some(1));
        assert_eq!(*attempted.lock().unwrap(), vec![0, 1, 2]);
        assert_eq!(stored, vec![0, 1]);
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
        let root = std::env::temp_dir().join(format!(
            "kindlebridge-usb-fresh-hello-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-USB-RESTART")).sync_root(root.clone());
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
        let payload = vec![0x6b; 2 * 64 * 1024];
        let request = SyncRequest::Push {
            transfer_id: None,
            remote_path: "fresh-hello/payload.bin".to_owned(),
            total_size: payload.len() as u64,
            file_hash: blake3::hash(&payload).to_hex().to_string(),
            block_size: 64 * 1024,
        };
        let mut response_credit = Header::new(Command::Credit, 1, 1);
        response_credit.credit_delta = DEFAULT_STREAM_WINDOW;
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
                Ok(Frame::new(response_credit, Vec::new()).unwrap()),
                Ok(frame(Command::Data, 1, 2, encode(&request).unwrap()).unwrap()),
                Ok(frame(Command::Data, 1, 3, payload[..64 * 1024].to_vec()).unwrap()),
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
        let staging = root.join(".kindlebridge-sync");
        let part = fs::read_dir(staging)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "part")
            })
            .unwrap();
        assert_eq!(fs::metadata(part).unwrap().len(), 0);
        fs::remove_dir_all(root).unwrap();
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
    fn split_endpoints_serve_a_real_process_list_call() {
        let proc_root = std::env::temp_dir().join(format!(
            "kindlebridge-split-process-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&proc_root);
        fs::create_dir_all(proc_root.join("42")).unwrap();
        fs::write(proc_root.join("42/comm"), b"reader\n").unwrap();
        let config =
            ServerConfig::new(DeviceInfo::kt6("KT6-USB-PROCESS")).proc_root(proc_root.clone());
        let host_hello = HostHello {
            protocol_version: PROTOCOL_VERSION,
            session_id: TEST_SESSION_ID.to_owned(),
            client_name: "usb-process-test".to_owned(),
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
                    service: SHELL_SERVICE.to_owned(),
                })
                .unwrap(),
            )
            .unwrap(),
        );
        let mut response_credit = Header::new(Command::Credit, 1, 1);
        response_credit.credit_delta = DEFAULT_STREAM_WINDOW;
        append(Frame::new(response_credit, Vec::new()).unwrap());
        let mut request = frame(
            Command::Data,
            1,
            2,
            encode(&DeviceCall {
                method: methods::PROCESS_LIST.to_owned(),
                params: serde_json::json!({ "serial": "KT6-USB-PROCESS" }),
            })
            .unwrap(),
        )
        .unwrap();
        request.header.flags = FLAG_END_STREAM;
        append(request);
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
        let mut process_reply = None;
        while let Ok(frame) = reader.read_frame() {
            if frame.header.command == Command::Data && frame.header.stream_id == 1 {
                process_reply = Some(
                    decode::<DeviceReply>(&frame.payload, "process reply")
                        .unwrap()
                        .into_result()
                        .unwrap(),
                );
            }
        }
        let result: kindlebridge_schema::ProcessList =
            serde_json::from_value(process_reply.unwrap()).unwrap();
        assert_eq!(result.processes.len(), 1);
        assert_eq!(result.processes[0].pid, 42);
        assert_eq!(result.processes[0].name, "reader");
        fs::remove_dir_all(proc_root).unwrap();
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
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-LINK"));
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
            &config,
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
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-LINK"));
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
            &config,
            &SyncStore::new(std::env::temp_dir().join("kindlebridge-dispatch-test")),
        );
        assert_eq!(
            reply.into_result().unwrap_err().code,
            error_codes::DEVICE_NOT_FOUND
        );
    }

    #[test]
    fn unsupported_mutations_report_the_exact_unadvertised_capability() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-LINK"));
        let sync_store = SyncStore::new(std::env::temp_dir().join("kindlebridge-dispatch-test"));
        for (method, params, feature) in [
            (
                methods::APP_INSTALL,
                serde_json::json!({
                    "serial": "KT6-LINK",
                    "app_id": "org.example.reader",
                    "version": "1.0.0"
                }),
                APP_INSTALL_FEATURE,
            ),
            (
                methods::APP_START,
                serde_json::json!({
                    "serial": "KT6-LINK",
                    "app_id": "org.example.reader"
                }),
                APP_START_FEATURE,
            ),
            (
                methods::APP_STOP,
                serde_json::json!({
                    "serial": "KT6-LINK",
                    "app_id": "org.example.reader"
                }),
                APP_STOP_FEATURE,
            ),
            (
                methods::APP_RESTART,
                serde_json::json!({
                    "serial": "KT6-LINK",
                    "app_id": "org.example.reader"
                }),
                APP_RESTART_FEATURE,
            ),
            (
                methods::APP_ROLLBACK,
                serde_json::json!({
                    "serial": "KT6-LINK",
                    "app_id": "org.example.reader"
                }),
                APP_ROLLBACK_FEATURE,
            ),
            (
                methods::APP_UNINSTALL,
                serde_json::json!({
                    "serial": "KT6-LINK",
                    "app_id": "org.example.reader"
                }),
                APP_UNINSTALL_FEATURE,
            ),
            (
                methods::PROCESS_SIGNAL,
                serde_json::json!({
                    "serial": "KT6-LINK",
                    "pid": 42,
                    "signal": "TERM"
                }),
                PROCESS_SIGNAL_FEATURE,
            ),
        ] {
            let error = dispatch(
                DeviceCall {
                    method: method.to_owned(),
                    params,
                },
                &config,
                &sync_store,
            )
            .into_result()
            .unwrap_err();
            assert_eq!(error.code, error_codes::FEATURE_UNAVAILABLE);
            assert_eq!(error.data.unwrap()["feature"], feature);
        }
    }
}
