//! Single-owner KBP connection state with independently blocking frame input.

use std::collections::{HashMap, VecDeque};
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
}

#[derive(Debug)]
pub struct IncomingStreams {
    receiver: Receiver<Result<IncomingStream, ConnectionError>>,
}

#[derive(Clone, Debug)]
pub struct Stream {
    id: u32,
    connection: Connection,
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
        let (command_tx, command_rx) = mpsc::sync_channel(COMMAND_QUEUE_DEPTH);
        let (inbound_tx, inbound_rx) = mpsc::sync_channel(INBOUND_QUEUE_DEPTH);
        let (incoming_tx, incoming_rx) = mpsc::sync_channel(INCOMING_STREAM_DEPTH);
        let connection = Self {
            inner: Arc::new(ConnectionInner {
                commands: command_tx.clone(),
                terminal_error: Mutex::new(None),
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
                Actor::new(state, sink, actor_connection, incoming_tx).run(command_rx, inbound_rx);
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
        let stream_id = self.request(|response| ActorCommand::Open {
            service: service.into(),
            receive_window,
            class,
            response,
        })?;
        Ok(Stream {
            id: stream_id,
            connection: self.clone(),
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
            stream_id: self.id,
            payload,
            end_stream,
            response,
        })
    }

    pub fn recv(&self) -> Result<Frame, ConnectionError> {
        self.connection.request(|response| ActorCommand::Receive {
            stream_id: self.id,
            response,
        })
    }

    pub fn close(&self) -> Result<(), ConnectionError> {
        self.connection.request(|response| ActorCommand::Close {
            stream_id: self.id,
            response,
        })
    }

    pub fn reset(&self, reason: impl Into<Vec<u8>>) -> Result<(), ConnectionError> {
        self.connection.request(|response| ActorCommand::Reset {
            stream_id: self.id,
            reason: reason.into(),
            response,
        })
    }

    pub fn cancel_receive(&self) -> Result<(), ConnectionError> {
        self.connection
            .request(|response| ActorCommand::CancelReceive {
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
        service: String,
        receive_window: u32,
        class: TrafficClass,
        response: SyncSender<Result<u32, ConnectionError>>,
    },
    Accept {
        stream_id: u32,
        receive_window: u32,
        class: TrafficClass,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    Reject {
        stream_id: u32,
        reason: String,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    SendData {
        stream_id: u32,
        payload: Vec<u8>,
        end_stream: bool,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    Receive {
        stream_id: u32,
        response: SyncSender<Result<Frame, ConnectionError>>,
    },
    Close {
        stream_id: u32,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    Reset {
        stream_id: u32,
        reason: Vec<u8>,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    CancelReceive {
        stream_id: u32,
        response: SyncSender<Result<(), ConnectionError>>,
    },
    Shutdown,
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
    control_sequence: u32,
    scheduler: FrameScheduler,
}

impl<W: FrameSink> Actor<W> {
    fn new(
        state: SessionState,
        sink: W,
        commands: Weak<ConnectionInner>,
        incoming: SyncSender<Result<IncomingStream, ConnectionError>>,
    ) -> Self {
        Self {
            state,
            sink,
            own_connection: commands,
            incoming,
            streams: HashMap::new(),
            sequences: HashMap::new(),
            control_sequence: 1,
            scheduler: FrameScheduler::new(MAX_SCHEDULED_BYTES),
        }
    }

    fn run(mut self, commands: Receiver<ActorCommand>, inbound: Receiver<InboundEvent>) {
        loop {
            let mut progressed = false;
            match commands.try_recv() {
                Ok(ActorCommand::Shutdown) | Err(TryRecvError::Disconnected) => break,
                Ok(command) => {
                    progressed = true;
                    if let Err(error) = self.handle_command(command) {
                        self.fail_all(error);
                        break;
                    }
                }
                Err(TryRecvError::Empty) => {}
            }
            match inbound.try_recv() {
                Ok(InboundEvent::Frame(frame)) => {
                    progressed = true;
                    if let Err(error) = self.handle_inbound(frame) {
                        self.fail_all(error);
                        break;
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
            if let Some(item) = self.scheduler.dequeue() {
                progressed = true;
                if let Err(error) = self
                    .sink
                    .write_frame(&item.frame)
                    .and_then(|()| self.sink.flush())
                {
                    self.fail_all(ConnectionError::Transport(error));
                    break;
                }
            }
            if !progressed {
                match commands.recv_timeout(IDLE_POLL) {
                    Ok(ActorCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Ok(command) => {
                        if let Err(error) = self.handle_command(command) {
                            self.fail_all(error);
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
        }
        self.fail_all(ConnectionError::Disconnected);
    }

    fn handle_command(&mut self, command: ActorCommand) -> Result<(), ConnectionError> {
        match command {
            ActorCommand::Open {
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
                stream_id,
                receive_window,
                class,
                response,
            } => {
                let entry = self
                    .streams
                    .get_mut(&stream_id)
                    .ok_or(ConnectionError::UnknownStream(stream_id))?;
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
                stream_id,
                reason,
                response,
            } => {
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
                stream_id,
                payload,
                end_stream,
                response,
            } => self.send_or_wait(stream_id, payload, end_stream, response),
            ActorCommand::Receive {
                stream_id,
                response,
            } => self.receive_or_wait(stream_id, response),
            ActorCommand::Close {
                stream_id,
                response,
            } => {
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
                stream_id,
                reason,
                response,
            } => {
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
                stream_id,
                response,
            } => {
                let entry = self
                    .streams
                    .get_mut(&stream_id)
                    .ok_or(ConnectionError::UnknownStream(stream_id))?;
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
            if entry.pending_send.is_some() {
                let error = ConnectionError::SendPending(stream_id);
                let _ = response.send(Err(error.clone()));
                return Err(error);
            }
            entry.class
        };
        let needed = u32::try_from(payload.len())
            .map_err(|_| ConnectionError::Protocol("DATA payload is too large".to_owned()))?;
        if self.has_send_credit(stream_id, needed) {
            let result = self.queue_frame(
                Command::Data,
                stream_id,
                payload,
                end_stream,
                class,
                FrameContext::default(),
            );
            let _ = response.send(result.clone());
            result
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
        let result = self.queue_frame(
            Command::Data,
            stream_id,
            pending.payload,
            pending.end_stream,
            class,
            FrameContext::default(),
        );
        let _ = pending.response.send(result.clone());
        result
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
        let entry = self
            .streams
            .get_mut(&stream_id)
            .ok_or(ConnectionError::UnknownStream(stream_id))?;
        if entry.receiver.is_some() {
            let error = ConnectionError::ReceivePending(stream_id);
            let _ = response.send(Err(error.clone()));
            return Err(error);
        }
        if let Some(frame) = entry.inbox.pop_front() {
            entry.inbox_bytes = entry.inbox_bytes.saturating_sub(frame.payload.len());
            self.restore_consumed_credit(&frame)?;
            let _ = response.send(Ok(frame));
        } else {
            entry.receiver = Some(response);
        }
        Ok(())
    }

    fn deliver(&mut self, frame: Frame) -> Result<(), ConnectionError> {
        let stream_id = frame.header.stream_id;
        let entry = self
            .streams
            .get_mut(&stream_id)
            .ok_or(ConnectionError::UnknownStream(stream_id))?;
        if let Some(receiver) = entry.receiver.take() {
            self.restore_consumed_credit(&frame)?;
            let _ = receiver.send(Ok(frame));
            return Ok(());
        }
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
        let _ = self.incoming.try_send(Err(error));
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
