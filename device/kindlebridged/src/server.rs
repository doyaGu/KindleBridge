//! Persistent development KBP listener for the unprivileged device daemon.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, TryRecvError, TrySendError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use kindlebridge_functionfs::{
    FunctionFsDevice, FunctionFsError, FunctionFsFrameReader, FunctionFsFrameStream,
    FunctionFsFrameWriter,
};
use kindlebridge_schema::device_protocol::{
    is_valid_session_id, DeviceAppInstallParams, DeviceCall, DeviceHello, DeviceReply, HostHello,
    ShellOpen, SyncReply, SyncRequest, APP_INSTALL_FEATURE, APP_LIST_FEATURE, APP_LOG_FEATURE,
    APP_RESTART_FEATURE, APP_ROLLBACK_FEATURE, APP_START_FEATURE, APP_STOP_FEATURE,
    APP_UNINSTALL_FEATURE, DEFAULT_CONNECTION_WINDOW, DEFAULT_STREAM_WINDOW, EXEC_FEATURE,
    LOG_TAIL_FEATURE, PROCESS_LIST_FEATURE, PROCESS_SIGNAL_FEATURE, PROTOCOL_VERSION, RPC_SERVICE,
    SHELL_STREAM_WINDOW, SHELL_V2_FEATURE, SHELL_V2_SERVICE, SYNC_FEATURE, SYNC_SERVICE,
};
#[cfg(test)]
use kindlebridge_schema::device_protocol::{ServiceAccept, ServiceOpen, SYNC_CREDIT_BATCH_SIZE};
use kindlebridge_schema::shell_protocol::{PacketSource, ShellPacket, ShellStreamState};
use kindlebridge_schema::{
    error_codes, methods, AppTargetParams, ExecParams, LogTailParams, ProcessSignalParams,
    RpcError, SerialParams, SyncStatusParams, MAX_SYNC_BLOCK_SIZE,
};
use kindlebridge_transport::{
    actor::{
        Connection, ConnectionError, FrameSink as ActorFrameSink, FrameSource as ActorFrameSource,
        IncomingStream as ActorIncomingStream, RestartedSession, Stream as ActorStream,
    },
    TrafficClass,
};
use kindlebridge_transport_tcp::ErrorClass;
use kindlebridge_transport_tcp::{
    FrameIo, ShutdownMode, TcpFrameListener, TransportConfig, TransportError,
};
use kindlebridge_wire::{
    Command, DecodeLimits, EndpointRole, Frame, FrameContext, Header, ProtocolError, SessionConfig,
    SessionState, WireError, FLAG_END_STREAM,
};
use serde::Serialize;
use thiserror::Error;

use crate::app::AppSupervisor;
use crate::exec::{self, ExecError};
use crate::services;
use crate::shell::{ShellEvent, ShellWorker, ShellWorkerError};
use crate::sync::{PullTransfer, PushTransfer, StoreError, SyncStore};
use crate::DeviceInfo;

const SESSION_IO_TIMEOUT: Duration = Duration::from_secs(10 * 60 + 30);
const DEFAULT_SYNC_ROOT: &str = "/mnt/us/kindlebridge-data";
const DEFAULT_ACTIVATION_ROOT: &str = "/var/local/kindlebridge/apps";
const DEFAULT_PROC_ROOT: &str = "/proc";
const DEFAULT_LOG_PATH: &str = "/var/local/kindlebridge/usb.log";
const SYNC_PIPELINE_QUEUE_DEPTH: usize = 3;
const MAX_CONCURRENT_SHELLS: usize = 4;
const DEVICE_RUNTIME_FEATURES: &[&str] = &[
    APP_INSTALL_FEATURE,
    APP_LIST_FEATURE,
    APP_LOG_FEATURE,
    APP_RESTART_FEATURE,
    APP_ROLLBACK_FEATURE,
    APP_START_FEATURE,
    APP_STOP_FEATURE,
    APP_UNINSTALL_FEATURE,
    EXEC_FEATURE,
    LOG_TAIL_FEATURE,
    PROCESS_LIST_FEATURE,
    PROCESS_SIGNAL_FEATURE,
    SHELL_V2_FEATURE,
    SYNC_FEATURE,
];

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
    let (work_tx, work_rx) = mpsc::sync_channel::<T>(SYNC_PIPELINE_QUEUE_DEPTH);
    let (ack_tx, ack_rx) = mpsc::sync_channel::<Result<A, E>>(SYNC_PIPELINE_QUEUE_DEPTH);

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
    app_supervisor: Arc<AppSupervisor>,
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
            app_supervisor: Arc::new(AppSupervisor::new()),
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
        let reader = stream.try_clone()?;
        let state = negotiate_actor_session(&mut stream, &self.config)?;
        serve_actor_connection(
            state,
            TcpActorSource(reader),
            TcpActorSink(stream),
            &self.config,
            &self.sync_store,
            false,
        )
    }

    pub fn serve_forever(&self) -> Result<(), ServerError> {
        loop {
            let (mut stream, peer) = self.listener.accept()?;
            if self.validate_peer(peer.ip()).is_err() {
                continue;
            }
            let result = stream
                .try_clone()
                .map_err(ServerError::from)
                .and_then(|reader| {
                    let state = negotiate_actor_session(&mut stream, &self.config)?;
                    serve_actor_connection(
                        state,
                        TcpActorSource(reader),
                        TcpActorSink(stream),
                        &self.config,
                        &self.sync_store,
                        false,
                    )
                });
            if let Err(error) = result {
                eprintln!("KindleBridge TCP session ended: {error}");
            }
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
        let state = negotiate_usb_actor_session(&mut stream, &self.config)?;
        let (reader, writer) = stream.into_split();
        serve_actor_connection(
            state,
            FunctionFsActorSource(reader),
            FunctionFsActorSink(writer),
            &self.config,
            &self.sync_store,
            true,
        )?;
        Ok(true)
    }

    /// Keep accepting host reconnects while the FunctionFS gadget remains bound.
    pub fn serve_forever(&mut self) -> Result<(), ServerError> {
        loop {
            let Some(endpoints) = self.functionfs.accept()? else {
                return Ok(());
            };
            let mut stream = FunctionFsFrameStream::new(endpoints, transport_config(&self.config))?;
            let result = negotiate_usb_actor_session(&mut stream, &self.config).and_then(|state| {
                let (reader, writer) = stream.into_split();
                serve_actor_connection(
                    state,
                    FunctionFsActorSource(reader),
                    FunctionFsActorSink(writer),
                    &self.config,
                    &self.sync_store,
                    true,
                )
            });
            if let Err(error) = result {
                eprintln!("KindleBridge USB session ended: {error}");
            }
        }
    }
}

struct TcpActorSource(kindlebridge_transport_tcp::TcpFrameStream);

impl ActorFrameSource for TcpActorSource {
    fn read_frame(&mut self) -> Result<Frame, String> {
        self.0.read_frame().map_err(|error| error.to_string())
    }
}

struct TcpActorSink(kindlebridge_transport_tcp::TcpFrameStream);

impl Drop for TcpActorSink {
    fn drop(&mut self) {
        let _ = self.0.shutdown(ShutdownMode::Both);
    }
}

impl ActorFrameSink for TcpActorSink {
    fn write_frame(&mut self, frame: &Frame) -> Result<(), String> {
        self.0.write_frame(frame).map_err(|error| error.to_string())
    }

    fn flush(&mut self) -> Result<(), String> {
        self.0.flush().map_err(|error| error.to_string())
    }
}

struct FunctionFsActorSource(FunctionFsFrameReader);

impl ActorFrameSource for FunctionFsActorSource {
    fn read_frame(&mut self) -> Result<Frame, String> {
        loop {
            match self.0.read_frame() {
                Ok(frame) => return Ok(frame),
                Err(error) if transport_error_allows_in_place_restart(&error) => loop {
                    match self.0.resynchronize() {
                        Ok(()) => break,
                        Err(error) if transport_error_allows_in_place_restart(&error) => {
                            thread::sleep(Duration::from_millis(50));
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                },
                Err(error) => return Err(error.to_string()),
            }
        }
    }
}

struct FunctionFsActorSink(FunctionFsFrameWriter);

impl ActorFrameSink for FunctionFsActorSink {
    fn write_frame(&mut self, frame: &Frame) -> Result<(), String> {
        self.0.write_frame(frame).map_err(|error| error.to_string())
    }

    fn flush(&mut self) -> Result<(), String> {
        self.0.flush().map_err(|error| error.to_string())
    }
}

fn negotiate_actor_session(
    stream: &mut dyn FrameIo,
    config: &ServerConfig,
) -> Result<SessionState, ServerError> {
    let hello_frame = stream.read_frame()?;
    let RestartedSession {
        state,
        hello_response,
    } = prepare_actor_session(hello_frame, config)?;
    stream.write_frame(&hello_response)?;
    stream.flush()?;
    Ok(state)
}

/// Admit a new host session without requiring a composite USB re-enumeration.
///
/// Releasing WinUSB can abort the old FunctionFS endpoint request without
/// producing a new ENABLE event. The next host first sends a bounded recovery
/// fill and a PING marker, then its HELLO. Keep the newly reopened endpoint
/// pair alive while discarding that stale prefix so the HELLO is not lost in a
/// rapid open/error/reopen loop.
fn negotiate_usb_actor_session(
    stream: &mut dyn FrameIo,
    config: &ServerConfig,
) -> Result<SessionState, ServerError> {
    loop {
        let hello_frame = match stream.read_frame() {
            Ok(frame) => frame,
            Err(error) if transport_error_allows_in_place_restart(&error) => {
                recover_frame_boundary(stream)?;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        if !is_fresh_hello(&hello_frame) {
            continue;
        }
        let RestartedSession {
            state,
            hello_response,
        } = match prepare_actor_session(hello_frame, config) {
            Ok(session) => session,
            Err(error) if server_error_allows_in_place_restart(&error) => continue,
            Err(error) => return Err(error),
        };
        match stream
            .write_frame(&hello_response)
            .and_then(|()| stream.flush())
        {
            Ok(()) => return Ok(state),
            Err(error) if transport_error_allows_in_place_restart(&error) => {
                recover_frame_boundary(stream)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn prepare_actor_session(
    hello_frame: Frame,
    config: &ServerConfig,
) -> Result<RestartedSession, ServerError> {
    expect(&hello_frame, Command::Hello, 0)?;
    let host_hello: HostHello = decode(&hello_frame.payload, "host HELLO")?;
    validate_hello(&host_hello, config)?;
    let mut state = SessionState::new(SessionConfig::new(EndpointRole::Device, config.limits()));
    state.process_inbound(
        &hello_frame.header,
        FrameContext::hello(host_hello.initial_connection_window),
    )?;
    let hello = DeviceHello {
        protocol_version: PROTOCOL_VERSION,
        session_id: host_hello.session_id,
        serial: config.device.serial.clone(),
        model: config.device.model.clone(),
        firmware: config.device.firmware.clone(),
        target: config.device.target.clone(),
        features: DEVICE_RUNTIME_FEATURES
            .iter()
            .map(|feature| (*feature).to_owned())
            .collect(),
        initial_connection_window: config.connection_window,
    };
    let hello_response = frame(Command::Hello, 0, 0, encode(&hello)?)?;
    state.process_outbound(
        &hello_response.header,
        FrameContext::hello(config.connection_window),
    )?;
    Ok(RestartedSession {
        state,
        hello_response,
    })
}

fn serve_actor_connection<R, W>(
    state: SessionState,
    source: R,
    sink: W,
    config: &ServerConfig,
    sync_store: &SyncStore,
    restart_in_place: bool,
) -> Result<(), ServerError>
where
    R: ActorFrameSource,
    W: ActorFrameSink,
{
    let (_connection, incoming) = if restart_in_place {
        let restart_config = config.clone();
        Connection::start_restartable(state, source, sink, move |hello| {
            prepare_actor_session(hello, &restart_config)
                .map_err(|error| ConnectionError::Protocol(error.to_string()))
        })
    } else {
        Connection::start(state, source, sink)
    };
    let shells = Arc::new(AtomicUsize::new(0));
    loop {
        let incoming = match incoming.recv() {
            Ok(incoming) => incoming,
            Err(ConnectionError::Disconnected | ConnectionError::Transport(_)) => return Ok(()),
            Err(error) => return Err(ServerError::Connection(error)),
        };
        let config = config.clone();
        let sync_store = sync_store.clone();
        let shells = Arc::clone(&shells);
        thread::spawn(move || {
            let service = incoming.service.clone();
            let result = match service.as_str() {
                RPC_SERVICE => serve_actor_rpc(incoming, &config, &sync_store),
                SYNC_SERVICE => serve_actor_sync(incoming, &config, &sync_store),
                SHELL_V2_SERVICE => match ShellSlot::reserve(shells) {
                    Some(slot) => serve_actor_shell(incoming, slot),
                    None => incoming
                        .reject("at most four shell sessions may be active")
                        .map_err(ServerError::Connection),
                },
                _ => incoming
                    .reject(format!("unsupported service {service}"))
                    .map_err(ServerError::Connection),
            };
            if let Err(error) = result {
                eprintln!("KindleBridge {service} stream ended: {error}");
            }
        });
    }
}

struct ShellSlot(Arc<AtomicUsize>);

impl ShellSlot {
    fn reserve(active: Arc<AtomicUsize>) -> Option<Self> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                (value < MAX_CONCURRENT_SHELLS).then_some(value + 1)
            })
            .ok()
            .map(|_| Self(active))
    }
}

impl Drop for ShellSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn serve_actor_rpc(
    incoming: ActorIncomingStream,
    config: &ServerConfig,
    sync_store: &SyncStore,
) -> Result<(), ServerError> {
    let mut stream = incoming.accept(config.stream_window, TrafficClass::Bulk)?;
    let request = actor_data(&mut stream)?;
    if request.header.flags & FLAG_END_STREAM == 0 {
        stream.reset("device calls must end their request")?;
        return Ok(());
    }
    let call: DeviceCall = decode(&request.payload, "device call")?;
    let mut reply = dispatch(call, config, sync_store);
    let mut response = encode(&reply)?;
    if response.len() > config.stream_window as usize {
        reply = DeviceReply::failure(RpcError::new(
            error_codes::EXEC_OUTPUT_LIMIT,
            "Command output exceeds the device link limit",
        ));
        response = encode(&reply)?;
    }
    stream.send_data(response, true)?;
    stream.close()?;
    Ok(())
}

fn serve_actor_shell(incoming: ActorIncomingStream, _slot: ShellSlot) -> Result<(), ServerError> {
    let mut stream = incoming.accept(SHELL_STREAM_WINDOW, TrafficClass::Interactive)?;
    let open_frame = actor_data(&mut stream)?;
    if open_frame.header.flags & FLAG_END_STREAM != 0 {
        stream.reset("shell open metadata must not end the stream")?;
        return Ok(());
    }
    let open: ShellOpen = match decode(&open_frame.payload, "shell open") {
        Ok(open) => open,
        Err(error) => {
            stream.reset(error.to_string())?;
            return Ok(());
        }
    };
    let mut worker = match ShellWorker::spawn(open.clone()) {
        Ok(worker) => worker,
        Err(error) => {
            stream.reset(error.to_string())?;
            return Ok(());
        }
    };
    let input = worker.input();
    let input_stream = stream.clone();
    let stream_stopped = Arc::new(AtomicBool::new(false));
    let input_stopped = Arc::clone(&stream_stopped);
    let input_thread = thread::spawn(move || {
        let mut protocol = ShellStreamState::new(open.mode);
        loop {
            let frame = match input_stream.recv() {
                Ok(frame) => frame,
                Err(_) => {
                    input_stopped.store(true, Ordering::Release);
                    let _ = input.hangup();
                    break;
                }
            };
            match frame.header.command {
                Command::Data => {
                    let packet = match ShellPacket::decode(&frame.payload, PacketSource::Host) {
                        Ok(packet) => packet,
                        Err(error) => {
                            input_stopped.store(true, Ordering::Release);
                            let _ = input_stream.reset(error.to_string());
                            let _ = input.hangup();
                            break;
                        }
                    };
                    if let Err(error) = protocol.accept(&packet) {
                        input_stopped.store(true, Ordering::Release);
                        let _ = input_stream.reset(error.to_string());
                        let _ = input.hangup();
                        break;
                    }
                    let result = match packet {
                        ShellPacket::Stdin(bytes) => input.write_stdin(bytes),
                        ShellPacket::CloseStdin => input.close_input(),
                        ShellPacket::Resize(size) => input.resize(size),
                        _ => unreachable!("host packet direction was validated"),
                    };
                    if result.is_err() {
                        input_stopped.store(true, Ordering::Release);
                        let _ = input_stream.reset("shell process input stopped");
                        break;
                    }
                }
                Command::Reset | Command::Close => {
                    input_stopped.store(true, Ordering::Release);
                    let _ = input.hangup();
                    break;
                }
                _ => {
                    input_stopped.store(true, Ordering::Release);
                    let _ = input_stream.reset("unexpected shell stream frame");
                    let _ = input.hangup();
                    break;
                }
            }
        }
    });

    loop {
        match worker.recv_timeout(Duration::from_secs(1)) {
            Ok(ShellEvent::Stdout(bytes)) => {
                if stream_stopped.load(Ordering::Acquire) {
                    break;
                }
                stream.send_data(ShellPacket::Stdout(bytes).encode()?, false)?;
            }
            Ok(ShellEvent::Stderr(bytes)) => {
                if stream_stopped.load(Ordering::Acquire) {
                    break;
                }
                stream.send_data(ShellPacket::Stderr(bytes).encode()?, false)?;
            }
            Ok(ShellEvent::Exit(status)) => {
                if stream_stopped.load(Ordering::Acquire) {
                    break;
                }
                let result = stream.send_data(ShellPacket::Exit(status).encode()?, true);
                let _ = stream.cancel_receive();
                result?;
                stream.close()?;
                break;
            }
            Err(ShellWorkerError::ReceiveTimeout) => {}
            Err(error) => {
                if stream_stopped.load(Ordering::Acquire) {
                    break;
                }
                let _ = stream.cancel_receive();
                stream.reset(error.to_string())?;
                break;
            }
        }
    }
    let _ = input_thread.join();
    Ok(())
}

fn serve_actor_sync(
    incoming: ActorIncomingStream,
    _config: &ServerConfig,
    sync_store: &SyncStore,
) -> Result<(), ServerError> {
    let mut stream = incoming.accept(DEFAULT_STREAM_WINDOW, TrafficClass::Bulk)?;
    let request_frame = actor_data(&mut stream)?;
    let request: SyncRequest = decode(&request_frame.payload, "sync request")?;
    let block_size = match &request {
        SyncRequest::Push { block_size, .. } | SyncRequest::Pull { block_size, .. } => *block_size,
    };
    if block_size == 0 || block_size > MAX_SYNC_BLOCK_SIZE {
        return actor_sync_failure(
            &stream,
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
                return actor_sync_failure(
                    &stream,
                    RpcError::invalid_params("push metadata must not end the stream"),
                );
            }
            let transfer = sync_store
                .begin_push(transfer_id.as_deref(), &remote_path, total_size, &file_hash)
                .map_err(StoreError::into_rpc);
            match transfer {
                Ok(transfer) => actor_sync_push(stream, block_size, transfer),
                Err(error) => actor_sync_failure(&stream, error),
            }
        }
        SyncRequest::Pull {
            transfer_id,
            remote_path,
            offset,
            block_size,
        } => {
            if request_frame.header.flags & FLAG_END_STREAM == 0 {
                return actor_sync_failure(
                    &stream,
                    RpcError::invalid_params("pull metadata must end the stream"),
                );
            }
            let transfer = sync_store
                .begin_pull(transfer_id.as_deref(), &remote_path, offset)
                .map_err(StoreError::into_rpc);
            match transfer {
                Ok(transfer) => actor_sync_pull(stream, block_size, transfer),
                Err(error) => actor_sync_failure(&stream, error),
            }
        }
    }
}

fn actor_sync_push(
    mut stream: ActorStream,
    block_size: u32,
    mut transfer: PushTransfer,
) -> Result<(), ServerError> {
    let ready = SyncReply::Ready {
        transfer_id: transfer.transfer_id().to_owned(),
        offset: transfer.offset(),
        total_size: transfer.total_size(),
        file_hash: transfer.file_hash().to_owned(),
    };
    stream.send_data(encode(&ready)?, false)?;
    let block_size = block_size as usize;
    let digest = if transfer.is_complete() {
        loop {
            let data = actor_data(&mut stream)?;
            if data.payload.len() > block_size {
                stream.reset("sync DATA exceeds negotiated block size")?;
                return Ok(());
            }
            if !data.payload.is_empty() {
                stream.reset("completed sync push accepts only an empty final DATA")?;
                return Ok(());
            }
            if data.header.flags & FLAG_END_STREAM != 0 {
                break;
            }
        }
        None
    } else {
        struct ReceiveContext<'a> {
            stream: &'a mut ActorStream,
            block_size: usize,
            end_stream_received: bool,
        }

        let hash_state = transfer.hash_state();
        let mut context = ReceiveContext {
            stream: &mut stream,
            block_size,
            end_stream_received: false,
        };
        // A small bounded queue overlaps USB receive, BLAKE3 and backing-store
        // writes. Payloads are shared with the hash worker, so the pipeline
        // retains at most three blocks without making another full-size copy.
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
                    let data = actor_data(context.stream)?;
                    if data.payload.len() > context.block_size {
                        return Err(ServerError::UnexpectedFrame(
                            "sync DATA exceeds negotiated block size",
                        ));
                    }
                    let is_last = data.header.flags & FLAG_END_STREAM != 0;
                    context.end_stream_received = is_last;
                    Ok(Some(Arc::<[u8]>::from(data.payload)))
                },
                |payload| {
                    hash_tx
                        .send(Arc::clone(&payload))
                        .map_err(|_| ServerError::UnexpectedFrame("sync hash worker stopped"))?;
                    let frame_start = transfer.offset();
                    transfer.write_chunk_without_hash(&payload)?;
                    Ok(frame_start)
                },
                |_context, _stored| Ok(()),
                |_context| false,
            );
            drop(hash_tx);
            let digest = hash_worker
                .join()
                .map_err(|_| ServerError::UnexpectedFrame("sync hash worker panicked"));
            (pipeline, digest)
        });
        let last_frame_start = pipeline.last_written;
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
        match receive_result.and(digest_result) {
            Ok(digest) => Some(digest),
            Err(error) => {
                if let Some(offset) = last_frame_start {
                    transfer.rollback_for_resume(offset)?;
                } else {
                    transfer.checkpoint()?;
                }
                return Err(error);
            }
        }
    };
    let status = match digest {
        Some(digest) => transfer.finish_with_digest(digest),
        None => transfer.finish(),
    };
    let status = match status {
        Ok(status) => status,
        Err(error) => return actor_sync_failure(&stream, error.into_rpc()),
    };
    let complete = SyncReply::Complete {
        transfer_id: status.transfer_id,
        next_offset: status.next_offset,
        total_size: status.total_size,
    };
    stream.send_data(encode(&complete)?, true)?;
    stream.close()?;
    Ok(())
}

fn actor_sync_pull(
    stream: ActorStream,
    block_size: u32,
    mut transfer: PullTransfer,
) -> Result<(), ServerError> {
    let ready = SyncReply::Ready {
        transfer_id: transfer.transfer_id().to_owned(),
        offset: transfer.offset(),
        total_size: transfer.total_size(),
        file_hash: transfer.file_hash().to_owned(),
    };
    stream.send_data(encode(&ready)?, false)?;
    if transfer.offset() == transfer.total_size() {
        transfer.finish()?;
        stream.send_data(Vec::new(), true)?;
    } else {
        struct PullChunk {
            payload: Vec<u8>,
            is_last: bool,
        }

        let (send_result, reader_result) = thread::scope(|scope| {
            let (chunk_tx, chunk_rx) =
                mpsc::sync_channel::<Result<PullChunk, ServerError>>(SYNC_PIPELINE_QUEUE_DEPTH);
            let reader = scope.spawn(move || {
                let result = (|| -> Result<(), ServerError> {
                    let mut buffer = vec![0_u8; block_size as usize];
                    loop {
                        let count = transfer.read_chunk(&mut buffer)?;
                        if count == 0 {
                            return Err(ServerError::UnexpectedFrame(
                                "sync source ended before declared size",
                            ));
                        }
                        let is_last = transfer.offset() == transfer.total_size();
                        if is_last {
                            transfer.finish()?;
                        } else {
                            transfer.checkpoint_if_due()?;
                        }
                        let chunk = PullChunk {
                            payload: buffer[..count].to_vec(),
                            is_last,
                        };
                        if chunk_tx.send(Ok(chunk)).is_err() {
                            return Ok(());
                        }
                        if is_last {
                            return Ok(());
                        }
                    }
                })();
                if let Err(error) = result {
                    let _ = chunk_tx.send(Err(error));
                }
            });

            let send_result = loop {
                match chunk_rx.recv() {
                    Ok(Ok(chunk)) => {
                        if let Err(error) = stream.send_data(chunk.payload, chunk.is_last) {
                            break Err(ServerError::Connection(error));
                        }
                        if chunk.is_last {
                            break Ok(());
                        }
                    }
                    Ok(Err(error)) => break Err(error),
                    Err(_) => {
                        break Err(ServerError::UnexpectedFrame(
                            "sync storage reader stopped before the transfer completed",
                        ));
                    }
                }
            };
            // If USB sending failed, wake a reader blocked on the bounded queue
            // before joining it. This keeps disconnect cleanup deterministic.
            drop(chunk_rx);
            let reader_result = reader
                .join()
                .map_err(|_| ServerError::UnexpectedFrame("sync storage reader panicked"));
            (send_result, reader_result)
        });
        send_result?;
        reader_result?;
    }
    stream.close()?;
    Ok(())
}

fn actor_sync_failure(stream: &ActorStream, error: RpcError) -> Result<(), ServerError> {
    stream.send_data(encode(&SyncReply::Failure { error })?, true)?;
    stream.close()?;
    Ok(())
}

fn actor_data(stream: &mut ActorStream) -> Result<Frame, ServerError> {
    let frame = stream.recv()?;
    if frame.header.command != Command::Data {
        return Err(ServerError::UnexpectedFrame(
            "expected DATA on actor service stream",
        ));
    }
    Ok(frame)
}

#[cfg(test)]
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
            features: DEVICE_RUNTIME_FEATURES
                .iter()
                .copied()
                .filter(|feature| *feature != SHELL_V2_FEATURE)
                .map(str::to_owned)
                .collect(),
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
            if !matches!(service.service.as_str(), RPC_SERVICE | SYNC_SERVICE) {
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
                RPC_SERVICE => serve_rpc_stream(
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

#[cfg(test)]
fn serve_rpc_stream(
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

#[cfg(test)]
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

#[cfg(test)]
struct PushChunk {
    payload: Arc<[u8]>,
    payload_length: u32,
    is_last: bool,
}

#[derive(Clone, Copy)]
#[cfg(test)]
struct StoredPushChunk {
    frame_start: u64,
    payload_length: u32,
    is_last: bool,
}

#[cfg(test)]
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

#[cfg(test)]
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
#[cfg(test)]
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
        methods::APP_INSTALL => reply(dispatch_app_install(call.params, config, sync_store)),
        methods::APP_START => reply(dispatch_app_start(call.params, config)),
        methods::APP_LOG => reply(dispatch_app_log(call.params, config)),
        methods::APP_STOP => reply(dispatch_app_stop(call.params, config)),
        methods::APP_RESTART => reply(dispatch_app_restart(call.params, config)),
        methods::APP_ROLLBACK => reply(dispatch_app_rollback(call.params, config)),
        methods::APP_UNINSTALL => reply(dispatch_app_uninstall(call.params, config)),
        methods::PROCESS_SIGNAL => reply(dispatch_process_signal(call.params, config)),
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
    services::app_list(&config.activation_root, &config.app_supervisor)
}

fn dispatch_process_list(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<SerialParams>(params, "expected serial")?;
    require_serial(&params.serial, config)?;
    let mut list = services::process_list(&config.proc_root)?;
    let managed = config.app_supervisor.managed_processes().map_err(|error| {
        RpcError::new(
            error_codes::INVALID_STATE,
            "Application supervisor unavailable",
        )
        .with_data(serde_json::json!({ "detail": error.to_string() }))
    })?;
    for process in &mut list.processes {
        process.app_id = managed.get(&process.pid).cloned();
    }
    Ok(list)
}

fn dispatch_process_signal(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<ProcessSignalParams>(params, "expected serial, pid, and signal")?;
    require_serial(&params.serial, config)?;
    if let Some(app_id) = config
        .app_supervisor
        .app_id_for_pid(params.pid)
        .map_err(|error| {
            RpcError::new(
                error_codes::INVALID_STATE,
                "Application supervisor unavailable",
            )
            .with_data(serde_json::json!({ "detail": error.to_string() }))
        })?
    {
        return Err(RpcError::invalid_params(format!(
            "PID {} is managed by {app_id}; use app stop or app restart",
            params.pid
        )));
    }
    services::process_signal(&config.proc_root, params.pid, &params.signal)
}

fn dispatch_log_tail(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<LogTailParams>(params, "expected serial, cursor, and limit")?;
    require_serial(&params.serial, config)?;
    services::log_tail(&config.log_path, &params)
}

fn dispatch_app_install(
    params: serde_json::Value,
    config: &ServerConfig,
    sync_store: &SyncStore,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<DeviceAppInstallParams>(
        params,
        "expected serial, remote_path, and file_hash",
    )?;
    require_serial(&params.serial, config)?;
    let mut bundle = sync_store
        .open_committed(&params.remote_path)
        .map_err(StoreError::into_rpc)?;
    services::app_install(
        &mut bundle,
        &params.file_hash,
        &config.activation_root,
        &config.device.target,
        &config.device.firmware,
        DEVICE_RUNTIME_FEATURES,
        &config.app_supervisor,
    )
}

fn dispatch_app_start(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<AppTargetParams>(params, "expected serial and app_id")?;
    require_serial(&params.serial, config)?;
    services::app_start(
        &config.activation_root,
        &config.app_supervisor,
        &params.app_id,
    )
}

fn dispatch_app_log(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<kindlebridge_schema::AppLogParams>(
        params,
        "expected serial, app_id, run_id, cursors, and max_bytes",
    )?;
    require_serial(&params.serial, config)?;
    services::app_log(&config.activation_root, &params)
}

fn dispatch_app_stop(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<AppTargetParams>(params, "expected serial and app_id")?;
    require_serial(&params.serial, config)?;
    services::app_stop(
        &config.activation_root,
        &config.app_supervisor,
        &params.app_id,
    )
}

fn dispatch_app_restart(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<AppTargetParams>(params, "expected serial and app_id")?;
    require_serial(&params.serial, config)?;
    services::app_restart(
        &config.activation_root,
        &config.app_supervisor,
        &params.app_id,
    )
}

fn dispatch_app_rollback(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<AppTargetParams>(params, "expected serial and app_id")?;
    require_serial(&params.serial, config)?;
    services::app_rollback(
        &config.activation_root,
        &config.app_supervisor,
        &params.app_id,
    )
}

fn dispatch_app_uninstall(
    params: serde_json::Value,
    config: &ServerConfig,
) -> Result<impl Serialize, RpcError> {
    let params = decode_params::<AppTargetParams>(params, "expected serial and app_id")?;
    require_serial(&params.serial, config)?;
    services::app_uninstall(
        &config.activation_root,
        &config.app_supervisor,
        &params.app_id,
    )
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn next_sequence(sequence: u32) -> Result<u32, ServerError> {
    sequence
        .checked_add(1)
        .ok_or(ServerError::SequenceExhausted)
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn send(
    stream: &mut dyn FrameIo,
    state: &mut SessionState,
    frame: Frame,
    context: FrameContext,
) -> Result<(), ServerError> {
    send_buffered(stream, state, frame, context)?;
    flush_outbound(stream)
}

#[cfg(test)]
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

#[cfg(test)]
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
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    #[error(transparent)]
    Shell(#[from] ShellWorkerError),
    #[error(transparent)]
    ShellPacket(#[from] kindlebridge_schema::shell_protocol::ShellPacketError),
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

    #[test]
    fn rejects_a_mismatched_host_protocol() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-PROTOCOL-TEST"));
        let hello = HostHello {
            protocol_version: PROTOCOL_VERSION - 1,
            session_id: TEST_SESSION_ID.to_owned(),
            client_name: "kindlebridge-test".to_owned(),
            initial_connection_window: DEFAULT_CONNECTION_WINDOW,
        };
        assert!(matches!(
            validate_hello(&hello, &config),
            Err(ServerError::InvalidHello)
        ));
    }

    #[test]
    fn shell_registry_allows_four_sessions_and_releases_slots() {
        let active = Arc::new(AtomicUsize::new(0));
        let mut slots = Vec::new();
        for _ in 0..MAX_CONCURRENT_SHELLS {
            slots.push(ShellSlot::reserve(Arc::clone(&active)).unwrap());
        }
        assert!(ShellSlot::reserve(Arc::clone(&active)).is_none());
        slots.pop();
        assert!(ShellSlot::reserve(active).is_some());
    }

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
            maximum.load(Ordering::SeqCst) <= SYNC_PIPELINE_QUEUE_DEPTH + 2,
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
    fn actor_usb_hello_gate_discards_recovery_fill_and_ping() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-ACTOR-USB-RECOVERY"));
        let hello = frame(
            Command::Hello,
            0,
            0,
            encode(&HostHello {
                protocol_version: PROTOCOL_VERSION,
                session_id: TEST_SESSION_ID.to_owned(),
                client_name: "actor-usb-recovery-test".to_owned(),
                initial_connection_window: DEFAULT_CONNECTION_WINDOW,
            })
            .unwrap(),
        )
        .unwrap();
        let invalid_magic = TransportError::Io {
            operation: kindlebridge_transport_tcp::IoOperation::ReadHeader,
            source: io::Error::new(io::ErrorKind::InvalidData, "abandoned frame"),
        };
        let mut stream = ScriptedFrameIo {
            reads: VecDeque::from([
                Err(invalid_magic),
                Ok(frame(Command::Ping, 0, 0, Vec::new()).unwrap()),
                Ok(hello),
            ]),
            writes: Vec::new(),
        };

        negotiate_usb_actor_session(&mut stream, &config).unwrap();

        assert_eq!(stream.writes.len(), 1);
        assert_eq!(stream.writes[0].header.command, Command::Hello);
        let reply: DeviceHello = decode(&stream.writes[0].payload, "device HELLO").unwrap();
        assert_eq!(reply.session_id, TEST_SESSION_ID);
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
                    service: RPC_SERVICE.to_owned(),
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
    fn application_mutation_features_are_advertised_together() {
        assert!(DEVICE_RUNTIME_FEATURES.contains(&APP_ROLLBACK_FEATURE));
        assert!(DEVICE_RUNTIME_FEATURES.contains(&APP_UNINSTALL_FEATURE));
    }
}
