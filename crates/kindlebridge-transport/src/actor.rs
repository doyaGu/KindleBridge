//! Single-owner KBP connection state with independently blocking frame input.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use kindlebridge_schema::device_protocol::{ServiceAccept, ServiceOpen};
use kindlebridge_wire::{
    Command, Frame, FrameContext, Header, ProtocolError, SessionState, StreamPhase, FLAG_END_STREAM,
};
use thiserror::Error;

use crate::{FrameScheduler, ScheduledFrame, TrafficClass};

const COMMAND_QUEUE_DEPTH: usize = 64;
const INBOUND_QUEUE_DEPTH: usize = 64;
const INCOMING_STREAM_DEPTH: usize = 16;
const MAX_SCHEDULED_BYTES: usize = 16 * 1024 * 1024;
const IDLE_POLL: Duration = Duration::from_millis(1);

pub trait FrameSource: Send + 'static {
    fn read_frame(&mut self) -> Result<Frame, String>;
}

pub trait FrameSink: Send + 'static {
    fn write_frame(&mut self, frame: &Frame) -> Result<(), String>;

    fn flush(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ConnectionError {
    #[error("KBP connection is disconnected")]
    Disconnected,
    #[error("KBP stream was rejected: {0}")]
    Rejected(String),
    #[error("KBP protocol error: {0}")]
    Protocol(String),
    #[error("KBP transport error: {0}")]
    Transport(String),
    #[error("KBP stream {0} does not exist")]
    UnknownStream(u32),
    #[error("KBP stream {0} already has a pending receive")]
    ReceivePending(u32),
    #[error("KBP stream {0} already has a pending send")]
    SendPending(u32),
    #[error("bounded KBP writer queue is full")]
    QueueFull,
    #[error("invalid service response: {0}")]
    InvalidService(String),
}

#[derive(Clone, Debug)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

#[derive(Debug)]
struct ConnectionInner {
    commands: SyncSender<ActorCommand>,
    terminal_error: Mutex<Option<ConnectionError>>,
    generation: AtomicU64,
}

#[derive(Debug)]
pub struct IncomingStreams {
    receiver: Receiver<Result<IncomingStream, ConnectionError>>,
}

/// A fresh, fully negotiated session that can replace an abandoned session
/// without releasing the underlying transport endpoints.
#[derive(Debug)]
pub struct RestartedSession {
    pub state: SessionState,
    pub hello_response: Frame,
}

#[derive(Clone, Debug)]
pub struct Stream {
    id: u32,
    connection: Connection,
    generation: u64,
}

#[derive(Clone, Debug)]
pub struct IncomingStream {
    pub service: String,
    stream: Stream,
}

impl Connection {
    #[must_use]
    pub fn start<R, W>(state: SessionState, source: R, sink: W) -> (Self, IncomingStreams)
    where
        R: FrameSource,
        W: FrameSink,
    {
        Self::start_inner(state, source, sink, None)
    }

    /// Start a connection that can accept a new peer HELLO on the same
    /// transport. FunctionFS keeps its endpoint pair configured when a host
    /// process releases and reclaims WinUSB, so USB users must restart the KBP
    /// session in place instead of closing and reopening ep1/ep2.
    pub fn start_restartable<R, W, Restart>(
        state: SessionState,
        source: R,
        sink: W,
        restart: Restart,
    ) -> (Self, IncomingStreams)
    where
        R: FrameSource,
        W: FrameSink,
        Restart: FnMut(Frame) -> Result<RestartedSession, ConnectionError> + Send + 'static,
    {
        Self::start_inner(state, source, sink, Some(Box::new(restart)))
    }

    fn start_inner<R, W>(
        state: SessionState,
        source: R,
        sink: W,
        restart: Option<Box<RestartHandler>>,
    ) -> (Self, IncomingStreams)
    where
        R: FrameSource,
        W: FrameSink,
    {
        let (command_tx, command_rx) = mpsc::sync_channel(COMMAND_QUEUE_DEPTH);
        let (inbound_tx, inbound_rx) = mpsc::sync_channel(INBOUND_QUEUE_DEPTH);
        let (incoming_tx, incoming_rx) = mpsc::sync_channel(INCOMING_STREAM_DEPTH);
        let connection = Self {
            inner: Arc::new(ConnectionInner {
                commands: command_tx.clone(),
                terminal_error: Mutex::new(None),
                generation: AtomicU64::new(0),
            }),
        };
        let actor_connection = Arc::downgrade(&connection.inner);
        thread::Builder::new()
            .name("kbp-reader".to_owned())
            .spawn(move || read_frames(source, inbound_tx))
            .expect("could not start KBP reader");
        thread::Builder::new()
            .name("kbp-connection".to_owned())
            .spawn(move || {
                Actor::new(state, sink, actor_connection, incoming_tx, restart)
                    .run(command_rx, inbound_rx);
            })
            .expect("could not start KBP connection actor");
        (
            connection,
            IncomingStreams {
                receiver: incoming_rx,
            },
        )
    }

    pub fn open(
        &self,
        service: impl Into<String>,
        receive_window: u32,
        class: TrafficClass,
    ) -> Result<Stream, ConnectionError> {
        let generation = self.inner.generation.load(Ordering::Acquire);
        let stream_id = self.request(|response| ActorCommand::Open {
            generation,
            service: service.into(),
            receive_window,
            class,
            response,
        })?;
        Ok(Stream {
            id: stream_id,
            connection: self.clone(),
            generation,
        })
    }

    pub fn shutdown(&self) {
        let _ = self.inner.commands.try_send(ActorCommand::Shutdown);
    }

    #[must_use]
    pub fn is_online(&self) -> bool {
        self.inner
            .terminal_error
            .lock()
            .is_ok_and(|error| error.is_none())
    }

    fn request<T: Send + 'static>(
        &self,
        build: impl FnOnce(SyncSender<Result<T, ConnectionError>>) -> ActorCommand,
    ) -> Result<T, ConnectionError> {
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        self.inner
            .commands
            .send(build(response_tx))
            .map_err(|_| self.disconnect_error())?;
        response_rx.recv().map_err(|_| self.disconnect_error())?
    }

    fn disconnect_error(&self) -> ConnectionError {
        self.inner
            .terminal_error
            .lock()
            .ok()
            .and_then(|error| error.clone())
            .unwrap_or(ConnectionError::Disconnected)
    }
}

type RestartHandler =
    dyn FnMut(Frame) -> Result<RestartedSession, ConnectionError> + Send + 'static;

impl IncomingStreams {
    pub fn recv(&self) -> Result<IncomingStream, ConnectionError> {
        self.receiver
            .recv()
            .map_err(|_| ConnectionError::Disconnected)?
    }
}

impl IncomingStream {
    pub fn accept(
        self,
        receive_window: u32,
        class: TrafficClass,
    ) -> Result<Stream, ConnectionError> {
        self.stream
            .connection
            .request(|response| ActorCommand::Accept {
                generation: self.stream.generation,
                stream_id: self.stream.id,
                receive_window,
                class,
                response,
            })?;
        Ok(self.stream)
    }

    pub fn reject(self, reason: impl Into<String>) -> Result<(), ConnectionError> {
        self.stream
            .connection
            .request(|response| ActorCommand::Reject {
                generation: self.stream.generation,
                stream_id: self.stream.id,
                reason: reason.into(),
                response,
            })
    }
}

impl Stream {
    #[must_use]
    pub const fn id(&self) -> u32 {
        self.id
    }

    pub fn send_data(&self, payload: Vec<u8>, end_stream: bool) -> Result<(), ConnectionError> {
        self.connection.request(|response| ActorCommand::SendData {
            generation: self.generation,
            stream_id: self.id,
            payload,
            end_stream,
            response,
        })
    }

    pub fn recv(&self) -> Result<Frame, ConnectionError> {
        self.connection.request(|response| ActorCommand::Receive {
            generation: self.generation,
            stream_id: self.id,
            response,
        })
    }

    pub fn close(&self) -> Result<(), ConnectionError> {
        self.connection.request(|response| ActorCommand::Close {
            generation: self.generation,
            stream_id: self.id,
            response,
        })
    }

    pub fn reset(&self, reason: impl Into<Vec<u8>>) -> Result<(), ConnectionError> {
        self.connection.request(|response| ActorCommand::Reset {
            generation: self.generation,
            stream_id: self.id,
            reason: reason.into(),
            response,
        })
    }

    pub fn cancel_receive(&self) -> Result<(), ConnectionError> {
        self.connection
            .request(|response| ActorCommand::CancelReceive {
                generation: self.generation,
                stream_id: self.id,
                response,
            })
    }
}

enum InboundEvent {
    Frame(Frame),
    Failed(String),
}

#[derive(Debug)]
enum ActorCommand {
    Open {
        generation: u64,
        service: String,
        receive_window: u32,
        class: TrafficClass,
        response: SyncSender<Result<u32, ConnectionError>>,
    },
    Accept {
        generation: u64,
        stream_id: u32,
        receive_window: u32,
        class: TrafficClass,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    Reject {
        generation: u64,
        stream_id: u32,
        reason: String,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    SendData {
        generation: u64,
        stream_id: u32,
        payload: Vec<u8>,
        end_stream: bool,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    Receive {
        generation: u64,
        stream_id: u32,
        response: SyncSender<Result<Frame, ConnectionError>>,
    },
    Close {
        generation: u64,
        stream_id: u32,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    Reset {
        generation: u64,
        stream_id: u32,
        reason: Vec<u8>,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    CancelReceive {
        generation: u64,
        stream_id: u32,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    Shutdown,
}

impl ActorCommand {
    const fn generation(&self) -> Option<u64> {
        match self {
            Self::Open { generation, .. }
            | Self::Accept { generation, .. }
            | Self::Reject { generation, .. }
            | Self::SendData { generation, .. }
            | Self::Receive { generation, .. }
            | Self::Close { generation, .. }
            | Self::Reset { generation, .. }
            | Self::CancelReceive { generation, .. } => Some(*generation),
            Self::Shutdown => None,
        }
    }

    fn respond_error(self, error: ConnectionError) {
        match self {
            Self::Open { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::Accept { response, .. }
            | Self::Reject { response, .. }
            | Self::SendData { response, .. }
            | Self::Close { response, .. }
            | Self::Reset { response, .. }
            | Self::CancelReceive { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::Receive { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::Shutdown => {}
        }
    }
}

struct PendingSend {
    payload: Vec<u8>,
    end_stream: bool,
    response: SyncSender<Result<(), ConnectionError>>,
}

struct StreamEntry {
    class: TrafficClass,
    receive_window: u32,
    inbox: VecDeque<Frame>,
    inbox_bytes: usize,
    receiver: Option<SyncSender<Result<Frame, ConnectionError>>>,
    opener: Option<SyncSender<Result<u32, ConnectionError>>>,
    pending_send: Option<PendingSend>,
}

impl StreamEntry {
    fn new(class: TrafficClass, receive_window: u32) -> Self {
        Self {
            class,
            receive_window,
            inbox: VecDeque::new(),
            inbox_bytes: 0,
            receiver: None,
            opener: None,
            pending_send: None,
        }
    }
}

struct Actor<W> {
    state: SessionState,
    sink: W,
    own_connection: Weak<ConnectionInner>,
    incoming: SyncSender<Result<IncomingStream, ConnectionError>>,
    streams: HashMap<u32, StreamEntry>,
    sequences: HashMap<u32, u32>,
    write_waiters: HashMap<(u32, u32), SyncSender<Result<(), ConnectionError>>>,
    control_sequence: u32,
    scheduler: FrameScheduler,
    restart: Option<Box<RestartHandler>>,
    awaiting_restart: bool,
    generation: u64,
}

impl<W: FrameSink> Actor<W> {
    fn new(
        state: SessionState,
        sink: W,
        commands: Weak<ConnectionInner>,
        incoming: SyncSender<Result<IncomingStream, ConnectionError>>,
        restart: Option<Box<RestartHandler>>,
    ) -> Self {
        Self {
            state,
            sink,
            own_connection: commands,
            incoming,
            streams: HashMap::new(),
            sequences: HashMap::new(),
            write_waiters: HashMap::new(),
            control_sequence: 1,
            scheduler: FrameScheduler::new(MAX_SCHEDULED_BYTES),
            restart,
            awaiting_restart: false,
            generation: 0,
        }
    }

    fn handle_current_command(&mut self, command: ActorCommand) -> Result<(), ConnectionError> {
        if self.awaiting_restart && !matches!(&command, ActorCommand::Shutdown) {
            command.respond_error(ConnectionError::Disconnected);
            return Ok(());
        }
        if command
            .generation()
            .is_some_and(|generation| generation != self.generation)
        {
            command.respond_error(ConnectionError::Disconnected);
            return Ok(());
        }
        self.handle_command(command)
    }

    fn run(mut self, commands: Receiver<ActorCommand>, inbound: Receiver<InboundEvent>) {
        loop {
            let mut progressed = false;
            match commands.try_recv() {
                Ok(ActorCommand::Shutdown) | Err(TryRecvError::Disconnected) => break,
                Ok(command) => {
                    progressed = true;
                    if let Err(error) = self.handle_current_command(command) {
                        if !self.begin_restart(error.clone()) {
                            self.fail_all(error);
                            break;
                        }
                    }
                }
                Err(TryRecvError::Empty) => {}
            }
            match inbound.try_recv() {
                Ok(InboundEvent::Frame(frame)) => {
                    progressed = true;
                    if let Err(error) = self.handle_inbound(frame) {
                        if !self.begin_restart(error.clone()) {
                            self.fail_all(error);
                            break;
                        }
                    }
                }
                Ok(InboundEvent::Failed(error)) => {
                    self.fail_all(ConnectionError::Transport(error));
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    self.fail_all(ConnectionError::Disconnected);
                    break;
                }
                Err(TryRecvError::Empty) => {}
            }
            // Do not block for another command while an outbound frame is
            // already queued. More importantly, a command received by the
            // blocking idle path must reach the scheduler before the next
            // command is considered. Otherwise a worker can enqueue CLOSE
            // immediately after DATA and make the terminal command visible to
            // the actor before DATA has even been submitted to the sink.
            if !progressed && self.scheduler.is_empty() {
                match commands.recv_timeout(IDLE_POLL) {
                    Ok(ActorCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Ok(command) => {
                        if let Err(error) = self.handle_current_command(command) {
                            if !self.begin_restart(error.clone()) {
                                self.fail_all(error);
                                break;
                            }
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            if let Some(item) = self.scheduler.dequeue() {
                let key = (item.frame.header.stream_id, item.frame.header.sequence);
                let stream_id = item.frame.header.stream_id;
                let terminal = matches!(item.frame.header.command, Command::Close | Command::Reset);
                if let Err(error) = self
                    .sink
                    .write_frame(&item.frame)
                    .and_then(|()| self.sink.flush())
                {
                    let error = ConnectionError::Transport(error);
                    if !self.begin_restart(error.clone()) {
                        self.fail_all(error);
                        break;
                    }
                } else if let Some(response) = self.write_waiters.remove(&key) {
                    let _ = response.send(Ok(()));
                }
                if terminal {
                    self.retire_stream(stream_id, ConnectionError::Disconnected);
                }
            }
        }
        self.fail_all(ConnectionError::Disconnected);
    }

    fn handle_command(&mut self, command: ActorCommand) -> Result<(), ConnectionError> {
        match command {
            ActorCommand::Open {
                generation: _,
                service,
                receive_window,
                class,
                response,
            } => {
                let stream_id = self.state.allocate_stream_id().map_err(protocol_error)?;
                let mut entry = StreamEntry::new(class, receive_window);
                entry.opener = Some(response);
                self.streams.insert(stream_id, entry);
                let payload = serde_json::to_vec(&ServiceOpen { service })
                    .map_err(|error| ConnectionError::InvalidService(error.to_string()))?;
                self.queue_frame(
                    Command::Open,
                    stream_id,
                    payload,
                    false,
                    class,
                    FrameContext::default(),
                )
            }
            ActorCommand::Accept {
                generation: _,
                stream_id,
                receive_window,
                class,
                response,
            } => {
                if !self.streams.contains_key(&stream_id) {
                    let _ = response.send(Err(ConnectionError::Disconnected));
                    return Ok(());
                }
                let entry = self
                    .streams
                    .get_mut(&stream_id)
                    .expect("stream existence was checked");
                entry.class = class;
                entry.receive_window = receive_window;
                let payload = serde_json::to_vec(&ServiceAccept {
                    initial_stream_window: receive_window,
                })
                .map_err(|error| ConnectionError::InvalidService(error.to_string()))?;
                let result = self.queue_frame(
                    Command::Accept,
                    stream_id,
                    payload,
                    false,
                    TrafficClass::Control,
                    FrameContext::accept(receive_window),
                );
                let _ = response.send(result.clone());
                result
            }
            ActorCommand::Reject {
                generation: _,
                stream_id,
                reason,
                response,
            } => {
                if !self.streams.contains_key(&stream_id) {
                    let _ = response.send(Err(ConnectionError::Disconnected));
                    return Ok(());
                }
                let result = self.queue_frame(
                    Command::Reject,
                    stream_id,
                    reason.into_bytes(),
                    false,
                    TrafficClass::Control,
                    FrameContext::default(),
                );
                let _ = response.send(result.clone());
                result
            }
            ActorCommand::SendData {
                generation: _,
                stream_id,
                payload,
                end_stream,
                response,
            } => {
                if !self.streams.contains_key(&stream_id) {
                    let _ = response.send(Err(ConnectionError::Disconnected));
                    return Ok(());
                }
                self.send_or_wait(stream_id, payload, end_stream, response)
            }
            ActorCommand::Receive {
                generation: _,
                stream_id,
                response,
            } => {
                if !self.streams.contains_key(&stream_id) {
                    let _ = response.send(Err(ConnectionError::Disconnected));
                    return Ok(());
                }
                self.receive_or_wait(stream_id, response)
            }
            ActorCommand::Close {
                generation: _,
                stream_id,
                response,
            } => {
                if !self.streams.contains_key(&stream_id) {
                    let _ = response.send(Err(ConnectionError::Disconnected));
                    return Ok(());
                }
                let result = self.queue_frame(
                    Command::Close,
                    stream_id,
                    Vec::new(),
                    false,
                    TrafficClass::Control,
                    FrameContext::default(),
                );
                let _ = response.send(result.clone());
                result
            }
            ActorCommand::Reset {
                generation: _,
                stream_id,
                reason,
                response,
            } => {
                if !self.streams.contains_key(&stream_id) {
                    let _ = response.send(Err(ConnectionError::Disconnected));
                    return Ok(());
                }
                let result = self.queue_frame(
                    Command::Reset,
                    stream_id,
                    reason,
                    false,
                    TrafficClass::Control,
                    FrameContext::default(),
                );
                let _ = response.send(result.clone());
                result
            }
            ActorCommand::CancelReceive {
                generation: _,
                stream_id,
                response,
            } => {
                if !self.streams.contains_key(&stream_id) {
                    let _ = response.send(Err(ConnectionError::Disconnected));
                    return Ok(());
                }
                let entry = self
                    .streams
                    .get_mut(&stream_id)
                    .expect("stream existence was checked");
                if let Some(receiver) = entry.receiver.take() {
                    let _ = receiver.send(Err(ConnectionError::Disconnected));
                }
                let _ = response.send(Ok(()));
                Ok(())
            }
            ActorCommand::Shutdown => Ok(()),
        }
    }

    fn handle_inbound(&mut self, frame: Frame) -> Result<(), ConnectionError> {
        if frame.header.command == Command::Hello && frame.header.stream_id == 0 {
            if self.restart.is_some() {
                return self.restart_session(frame);
            }
        } else if self.awaiting_restart {
            // A cancelled WinUSB transfer can leave complete frames from the
            // abandoned session queued ahead of the new HELLO.
            return Ok(());
        }

        let discard_terminal_data = frame.header.command == Command::Data
            && self
                .state
                .stream(frame.header.stream_id)
                .is_some_and(|stream| {
                    matches!(stream.phase, StreamPhase::Closed | StreamPhase::Reset)
                });
        let context = if frame.header.command == Command::Accept {
            let accept: ServiceAccept = serde_json::from_slice(&frame.payload)
                .map_err(|error| ConnectionError::InvalidService(error.to_string()))?;
            FrameContext::accept(accept.initial_stream_window)
        } else {
            FrameContext::default()
        };
        self.state
            .process_inbound(&frame.header, context)
            .map_err(|error| {
                ConnectionError::Protocol(format!(
                    "inbound {:?} on stream {}: {error}",
                    frame.header.command, frame.header.stream_id
                ))
            })?;

        if discard_terminal_data {
            // The wire state charged these in-flight bytes against connection
            // credit. Consume them here without exposing terminal-stream input
            // to a worker that has already been torn down.
            return self.restore_consumed_credit(&frame);
        }

        match frame.header.command {
            Command::Open => self.accept_incoming_open(frame),
            Command::Accept => self.complete_open(frame.header.stream_id),
            Command::Reject => self.reject_open(frame),
            Command::Credit => self.flush_pending(frame.header.stream_id),
            Command::Data | Command::Close | Command::Reset => self.deliver(frame),
            Command::Ping => self.queue_frame(
                Command::Pong,
                0,
                frame.payload,
                false,
                TrafficClass::Control,
                FrameContext::default(),
            ),
            Command::Pong => Ok(()),
            Command::GoAway | Command::Error => Err(ConnectionError::Disconnected),
            _ => Err(ConnectionError::Protocol(format!(
                "unexpected {:?} after handshake",
                frame.header.command
            ))),
        }
    }

    fn accept_incoming_open(&mut self, frame: Frame) -> Result<(), ConnectionError> {
        let service: ServiceOpen = serde_json::from_slice(&frame.payload)
            .map_err(|error| ConnectionError::InvalidService(error.to_string()))?;
        let stream_id = frame.header.stream_id;
        self.streams
            .insert(stream_id, StreamEntry::new(TrafficClass::Bulk, 0));
        let connection = self
            .own_connection
            .upgrade()
            .map(|inner| Connection { inner })
            .ok_or(ConnectionError::Disconnected)?;
        self.incoming
            .send(Ok(IncomingStream {
                service: service.service,
                stream: Stream {
                    id: stream_id,
                    connection,
                    generation: self.generation,
                },
            }))
            .map_err(|_| ConnectionError::Disconnected)
    }

    fn complete_open(&mut self, stream_id: u32) -> Result<(), ConnectionError> {
        let receive_window = self
            .streams
            .get(&stream_id)
            .ok_or(ConnectionError::UnknownStream(stream_id))?
            .receive_window;
        self.queue_credit(stream_id, receive_window)?;
        if let Some(opener) = self
            .streams
            .get_mut(&stream_id)
            .and_then(|entry| entry.opener.take())
        {
            let _ = opener.send(Ok(stream_id));
        }
        Ok(())
    }

    fn reject_open(&mut self, frame: Frame) -> Result<(), ConnectionError> {
        let stream_id = frame.header.stream_id;
        if let Some(opener) = self
            .streams
            .get_mut(&stream_id)
            .and_then(|entry| entry.opener.take())
        {
            let reason = String::from_utf8_lossy(&frame.payload).into_owned();
            let _ = opener.send(Err(ConnectionError::Rejected(reason)));
        }
        self.streams.remove(&stream_id);
        Ok(())
    }

    fn send_or_wait(
        &mut self,
        stream_id: u32,
        payload: Vec<u8>,
        end_stream: bool,
        response: SyncSender<Result<(), ConnectionError>>,
    ) -> Result<(), ConnectionError> {
        let class = {
            let entry = self
                .streams
                .get(&stream_id)
                .ok_or(ConnectionError::UnknownStream(stream_id))?;
            if entry.pending_send.is_some()
                || self
                    .write_waiters
                    .keys()
                    .any(|(pending_stream_id, _)| *pending_stream_id == stream_id)
            {
                let error = ConnectionError::SendPending(stream_id);
                let _ = response.send(Err(error.clone()));
                return Err(error);
            }
            entry.class
        };
        let needed = u32::try_from(payload.len())
            .map_err(|_| ConnectionError::Protocol("DATA payload is too large".to_owned()))?;
        if self.has_send_credit(stream_id, needed) {
            let sequence = self.next_sequence_for(stream_id);
            let result = self.queue_frame(
                Command::Data,
                stream_id,
                payload,
                end_stream,
                class,
                FrameContext::default(),
            );
            match result {
                Ok(()) => {
                    self.write_waiters.insert((stream_id, sequence), response);
                    Ok(())
                }
                Err(error) => {
                    let _ = response.send(Err(error.clone()));
                    Err(error)
                }
            }
        } else {
            self.streams
                .get_mut(&stream_id)
                .expect("stream checked")
                .pending_send = Some(PendingSend {
                payload,
                end_stream,
                response,
            });
            Ok(())
        }
    }

    fn flush_pending(&mut self, stream_id: u32) -> Result<(), ConnectionError> {
        if stream_id == 0 {
            let ids: Vec<_> = self.streams.keys().copied().collect();
            for id in ids {
                self.flush_pending(id)?;
            }
            return Ok(());
        }
        let Some(pending) = self
            .streams
            .get_mut(&stream_id)
            .and_then(|entry| entry.pending_send.take())
        else {
            return Ok(());
        };
        let needed = u32::try_from(pending.payload.len())
            .map_err(|_| ConnectionError::Protocol("DATA payload is too large".to_owned()))?;
        if !self.has_send_credit(stream_id, needed) {
            self.streams
                .get_mut(&stream_id)
                .expect("stream checked")
                .pending_send = Some(pending);
            return Ok(());
        }
        let class = self.streams[&stream_id].class;
        let sequence = self.next_sequence_for(stream_id);
        let result = self.queue_frame(
            Command::Data,
            stream_id,
            pending.payload,
            pending.end_stream,
            class,
            FrameContext::default(),
        );
        match result {
            Ok(()) => {
                self.write_waiters
                    .insert((stream_id, sequence), pending.response);
                Ok(())
            }
            Err(error) => {
                let _ = pending.response.send(Err(error.clone()));
                Err(error)
            }
        }
    }

    fn has_send_credit(&self, stream_id: u32, needed: u32) -> bool {
        self.state
            .stream(stream_id)
            .is_some_and(|stream| stream.send_credit >= needed)
            && self.state.snapshot().connection_send_credit >= needed
    }

    fn receive_or_wait(
        &mut self,
        stream_id: u32,
        response: SyncSender<Result<Frame, ConnectionError>>,
    ) -> Result<(), ConnectionError> {
        let frame = {
            let entry = self
                .streams
                .get_mut(&stream_id)
                .ok_or(ConnectionError::UnknownStream(stream_id))?;
            if entry.receiver.is_some() {
                let error = ConnectionError::ReceivePending(stream_id);
                let _ = response.send(Err(error.clone()));
                return Err(error);
            }
            match entry.inbox.pop_front() {
                Some(frame) => {
                    entry.inbox_bytes = entry.inbox_bytes.saturating_sub(frame.payload.len());
                    frame
                }
                None => {
                    entry.receiver = Some(response);
                    return Ok(());
                }
            }
        };
        let terminal = matches!(frame.header.command, Command::Close | Command::Reset);
        self.restore_consumed_credit(&frame)?;
        let _ = response.send(Ok(frame));
        if terminal {
            self.retire_stream(stream_id, ConnectionError::Disconnected);
        }
        Ok(())
    }

    fn deliver(&mut self, frame: Frame) -> Result<(), ConnectionError> {
        let stream_id = frame.header.stream_id;
        let terminal = matches!(frame.header.command, Command::Close | Command::Reset);
        let receiver = self
            .streams
            .get_mut(&stream_id)
            .ok_or(ConnectionError::UnknownStream(stream_id))?
            .receiver
            .take();
        if let Some(receiver) = receiver {
            self.restore_consumed_credit(&frame)?;
            let _ = receiver.send(Ok(frame));
            if terminal {
                self.retire_stream(stream_id, ConnectionError::Disconnected);
            }
            return Ok(());
        }
        let entry = self
            .streams
            .get_mut(&stream_id)
            .expect("stream existence was checked");
        entry.inbox_bytes = entry
            .inbox_bytes
            .checked_add(frame.payload.len())
            .ok_or(ConnectionError::QueueFull)?;
        if entry.inbox_bytes > entry.receive_window as usize {
            return Err(ConnectionError::QueueFull);
        }
        entry.inbox.push_back(frame);
        Ok(())
    }

    fn retire_stream(&mut self, stream_id: u32, error: ConnectionError) {
        if let Some(mut entry) = self.streams.remove(&stream_id) {
            if let Some(opener) = entry.opener.take() {
                let _ = opener.send(Err(error.clone()));
            }
            if let Some(receiver) = entry.receiver.take() {
                let _ = receiver.send(Err(error.clone()));
            }
            if let Some(pending) = entry.pending_send.take() {
                let _ = pending.response.send(Err(error.clone()));
            }
        }
        let waiting: Vec<_> = self
            .write_waiters
            .keys()
            .filter(|(pending_stream_id, _)| *pending_stream_id == stream_id)
            .copied()
            .collect();
        for key in waiting {
            if let Some(response) = self.write_waiters.remove(&key) {
                let _ = response.send(Err(error.clone()));
            }
        }
    }

    fn restore_consumed_credit(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        if frame.header.command != Command::Data || frame.header.payload_length == 0 {
            return Ok(());
        }
        // Once END_STREAM has been observed, stream-level credit is no longer
        // useful and may race the peer's CLOSE.  Connection credit remains
        // necessary so bytes consumed by a completed stream can be reused by
        // other streams on the same transport.
        if frame.header.flags & FLAG_END_STREAM == 0
            && self
                .state
                .stream(frame.header.stream_id)
                .is_some_and(|stream| stream.phase == StreamPhase::Accepted)
        {
            self.queue_credit(frame.header.stream_id, frame.header.payload_length)?;
        }
        self.queue_credit(0, frame.header.payload_length)
    }

    fn queue_credit(&mut self, stream_id: u32, delta: u32) -> Result<(), ConnectionError> {
        let sequence = self.take_sequence(stream_id)?;
        let mut header = Header::new(Command::Credit, stream_id, sequence);
        header.credit_delta = delta;
        let frame = Frame::new(header, Vec::new()).map_err(wire_error)?;
        self.state
            .process_outbound(&frame.header, FrameContext::default())
            .map_err(|error| {
                ConnectionError::Protocol(format!(
                    "outbound CREDIT on stream {}: {error}",
                    frame.header.stream_id
                ))
            })?;
        self.scheduler
            .enqueue(ScheduledFrame {
                class: TrafficClass::Control,
                frame,
            })
            .map_err(|_| ConnectionError::QueueFull)
    }

    fn queue_frame(
        &mut self,
        command: Command,
        stream_id: u32,
        payload: Vec<u8>,
        end_stream: bool,
        class: TrafficClass,
        context: FrameContext,
    ) -> Result<(), ConnectionError> {
        let sequence = self.take_sequence(stream_id)?;
        let mut header = Header::new(command, stream_id, sequence);
        if end_stream {
            header.flags |= FLAG_END_STREAM;
        }
        let frame = Frame::new(header, payload).map_err(wire_error)?;
        self.state
            .process_outbound(&frame.header, context)
            .map_err(|error| {
                ConnectionError::Protocol(format!(
                    "outbound {:?} on stream {}: {error}",
                    frame.header.command, frame.header.stream_id
                ))
            })?;
        let class = if TrafficClass::for_command(command) == TrafficClass::Control {
            TrafficClass::Control
        } else {
            class
        };
        self.scheduler
            .enqueue(ScheduledFrame { class, frame })
            .map_err(|_| ConnectionError::QueueFull)
    }

    fn take_sequence(&mut self, stream_id: u32) -> Result<u32, ConnectionError> {
        let sequence = if stream_id == 0 {
            &mut self.control_sequence
        } else {
            self.sequences.entry(stream_id).or_insert(0)
        };
        let current = *sequence;
        *sequence = sequence
            .checked_add(1)
            .ok_or_else(|| ConnectionError::Protocol("sequence exhausted".to_owned()))?;
        Ok(current)
    }

    fn next_sequence_for(&self, stream_id: u32) -> u32 {
        if stream_id == 0 {
            self.control_sequence
        } else {
            self.sequences.get(&stream_id).copied().unwrap_or(0)
        }
    }

    fn fail_all(&mut self, error: ConnectionError) {
        if let Some(connection) = self.own_connection.upgrade() {
            if let Ok(mut terminal_error) = connection.terminal_error.lock() {
                if terminal_error.is_none() {
                    *terminal_error = Some(error.clone());
                }
            }
        }
        for entry in self.streams.values_mut() {
            if let Some(opener) = entry.opener.take() {
                let _ = opener.send(Err(error.clone()));
            }
            if let Some(receiver) = entry.receiver.take() {
                let _ = receiver.send(Err(error.clone()));
            }
            if let Some(pending) = entry.pending_send.take() {
                let _ = pending.response.send(Err(error.clone()));
            }
        }
        for (_, response) in self.write_waiters.drain() {
            let _ = response.send(Err(error.clone()));
        }
        let _ = self.incoming.try_send(Err(error));
    }

    fn begin_restart(&mut self, error: ConnectionError) -> bool {
        if self.restart.is_none() {
            return false;
        }
        self.fail_streams(error);
        self.awaiting_restart = true;
        true
    }

    fn restart_session(&mut self, hello: Frame) -> Result<(), ConnectionError> {
        let restarted = self.restart.as_mut().expect("restart handler was checked")(hello)?;
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| ConnectionError::Protocol("session generation exhausted".to_owned()))?;
        if let Some(connection) = self.own_connection.upgrade() {
            connection
                .generation
                .store(self.generation, Ordering::Release);
        }
        self.fail_streams(ConnectionError::Disconnected);
        self.state = restarted.state;
        self.awaiting_restart = false;
        match self
            .sink
            .write_frame(&restarted.hello_response)
            .and_then(|()| self.sink.flush())
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.awaiting_restart = true;
                self.fail_streams(ConnectionError::Transport(error));
                Ok(())
            }
        }
    }

    fn fail_streams(&mut self, error: ConnectionError) {
        for entry in self.streams.values_mut() {
            if let Some(opener) = entry.opener.take() {
                let _ = opener.send(Err(error.clone()));
            }
            if let Some(receiver) = entry.receiver.take() {
                let _ = receiver.send(Err(error.clone()));
            }
            if let Some(pending) = entry.pending_send.take() {
                let _ = pending.response.send(Err(error.clone()));
            }
        }
        self.streams.clear();
        self.sequences.clear();
        for (_, response) in self.write_waiters.drain() {
            let _ = response.send(Err(error.clone()));
        }
        self.control_sequence = 1;
        self.scheduler = FrameScheduler::new(MAX_SCHEDULED_BYTES);
    }
}

fn read_frames<R: FrameSource>(mut source: R, sender: SyncSender<InboundEvent>) {
    loop {
        match source.read_frame() {
            Ok(frame) => {
                if sender.send(InboundEvent::Frame(frame)).is_err() {
                    break;
                }
            }
            Err(error) => {
                let _ = sender.send(InboundEvent::Failed(error));
                break;
            }
        }
    }
}

fn protocol_error(error: ProtocolError) -> ConnectionError {
    ConnectionError::Protocol(error.to_string())
}

fn wire_error(error: kindlebridge_wire::WireError) -> ConnectionError {
    ConnectionError::Protocol(error.to_string())
}
