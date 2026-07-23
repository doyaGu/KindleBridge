//! Persistent development KBP listener for the unprivileged device daemon.

mod rpc;

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use kindlebridge_functionfs::{
    FunctionFsDevice, FunctionFsError, FunctionFsFrameReader, FunctionFsFrameStream,
    FunctionFsFrameWriter,
};
use kindlebridge_schema::device_protocol::{
    is_valid_session_id, DeviceCall, DeviceHello, DeviceReply, HostHello,
    DEFAULT_CONNECTION_WINDOW, DEFAULT_STREAM_WINDOW, PROTOCOL_VERSION, RPC_SERVICE,
    SHELL_V2_FEATURE, SHELL_V2_SERVICE, SYNC_FEATURE, SYNC_SERVICE,
};
use kindlebridge_schema::device_rpc::{self as rpc_method, RpcMethod};
use kindlebridge_schema::{error_codes, RpcError};
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

use crate::application::ApplicationManager;
use crate::shell_stream::{ShellStreamError, ShellStreams};
use crate::sync::{StoreError, SyncStore};
use crate::DeviceInfo;

const SESSION_IO_TIMEOUT: Duration = Duration::from_secs(10 * 60 + 30);
const DEFAULT_SYNC_ROOT: &str = "/mnt/us/kindlebridge-data";
const DEFAULT_ACTIVATION_ROOT: &str = "/var/local/kindlebridge/apps";
const DEFAULT_PROC_ROOT: &str = "/proc";
const DEFAULT_LOG_PATH: &str = "/var/local/kindlebridge/usb.log";
const DEVICE_RUNTIME_FEATURES: &[&str] = &[
    rpc_method::AppInstall::FEATURE,
    rpc_method::AppList::FEATURE,
    rpc_method::AppLog::FEATURE,
    rpc_method::AppRestart::FEATURE,
    rpc_method::AppRollback::FEATURE,
    rpc_method::AppStart::FEATURE,
    rpc_method::AppStop::FEATURE,
    rpc_method::AppUninstall::FEATURE,
    rpc_method::ExecRun::FEATURE,
    rpc_method::LogTail::FEATURE,
    rpc_method::ProcessList::FEATURE,
    rpc_method::ProcessSignal::FEATURE,
    SHELL_V2_FEATURE,
    SYNC_FEATURE,
    rpc_method::SyncList::FEATURE,
];

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub device: DeviceInfo,
    pub allowed_peer: Option<IpAddr>,
    pub connection_window: u32,
    pub stream_window: u32,
    pub sync_root: PathBuf,
    pub proc_root: PathBuf,
    pub log_path: PathBuf,
    applications: ApplicationManager,
}

impl ServerConfig {
    #[must_use]
    pub fn new(device: DeviceInfo) -> Self {
        let applications = ApplicationManager::new(
            DEFAULT_ACTIVATION_ROOT,
            device.target.clone(),
            device.firmware.clone(),
            DEVICE_RUNTIME_FEATURES,
        );
        Self {
            device,
            allowed_peer: None,
            connection_window: DEFAULT_CONNECTION_WINDOW,
            stream_window: DEFAULT_STREAM_WINDOW,
            sync_root: PathBuf::from(DEFAULT_SYNC_ROOT),
            proc_root: PathBuf::from(DEFAULT_PROC_ROOT),
            log_path: PathBuf::from(DEFAULT_LOG_PATH),
            applications,
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
        self.applications = ApplicationManager::new(
            root,
            self.device.target.clone(),
            self.device.firmware.clone(),
            DEVICE_RUNTIME_FEATURES,
        );
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
    shells: ShellStreams,
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
            shells: ShellStreams::new(),
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
            &self.shells,
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
                        &self.shells,
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
    shells: ShellStreams,
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
            shells: ShellStreams::new(),
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
            &self.shells,
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
                    &self.shells,
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
    shells: &ShellStreams,
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
    loop {
        let incoming = match incoming.recv() {
            Ok(incoming) => incoming,
            Err(ConnectionError::Disconnected | ConnectionError::Transport(_)) => return Ok(()),
            Err(error) => return Err(ServerError::Connection(error)),
        };
        let config = config.clone();
        let sync_store = sync_store.clone();
        let shells = shells.clone();
        thread::spawn(move || {
            let service = incoming.service.clone();
            let result = match service.as_str() {
                RPC_SERVICE => serve_actor_rpc(incoming, &config, &sync_store),
                SYNC_SERVICE => crate::sync_stream::serve(incoming, &sync_store),
                SHELL_V2_SERVICE => shells.serve(incoming).map_err(ServerError::from),
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
    let mut reply = rpc::dispatch(call, config, sync_store);
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

fn actor_data(stream: &mut ActorStream) -> Result<Frame, ServerError> {
    let frame = stream.recv()?;
    if frame.header.command != Command::Data {
        return Err(ServerError::UnexpectedFrame(
            "expected DATA on actor service stream",
        ));
    }
    Ok(frame)
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
    ShellStream(#[from] ShellStreamError),
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
    use std::collections::VecDeque;
    use std::io;

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
    fn application_mutation_features_are_advertised_together() {
        assert!(DEVICE_RUNTIME_FEATURES.contains(&rpc_method::AppRollback::FEATURE));
        assert!(DEVICE_RUNTIME_FEATURES.contains(&rpc_method::AppUninstall::FEATURE));
    }
}
