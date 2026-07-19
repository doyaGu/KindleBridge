use std::collections::HashMap;

use crate::{Command, DecodeLimits, Header, ProtocolError, FLAG_END_STREAM};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointRole {
    Host,
    Device,
}

impl EndpointRole {
    const fn local_streams_are_odd(self) -> bool {
        matches!(self, Self::Host)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    const fn sender_is_local(self) -> bool {
        matches!(self, Self::Outbound)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionPhase {
    HelloExchange,
    Online,
    GoingAway,
    Closed,
}

impl SessionPhase {
    const fn name(self) -> &'static str {
        match self {
            Self::HelloExchange => "HELLO_EXCHANGE",
            Self::Online => "ONLINE",
            Self::GoingAway => "GOAWAY",
            Self::Closed => "CLOSED",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamPhase {
    Opening,
    Accepted,
    Rejected,
    Closed,
    Reset,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FrameContext {
    /// The connection receive window advertised by a decoded HELLO payload.
    pub initial_connection_window: Option<u32>,
    /// The stream receive window advertised by a decoded ACCEPT payload.
    pub initial_stream_window: Option<u32>,
}

impl FrameContext {
    pub const fn hello(initial_connection_window: u32) -> Self {
        Self {
            initial_connection_window: Some(initial_connection_window),
            initial_stream_window: None,
        }
    }

    pub const fn accept(initial_stream_window: u32) -> Self {
        Self {
            initial_connection_window: None,
            initial_stream_window: Some(initial_stream_window),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionConfig {
    pub role: EndpointRole,
    pub pairing_session: bool,
    pub limits: DecodeLimits,
}

impl SessionConfig {
    pub const fn new(role: EndpointRole, limits: DecodeLimits) -> Self {
        Self {
            role,
            pairing_session: false,
            limits,
        }
    }

    pub const fn pairing(mut self, enabled: bool) -> Self {
        self.pairing_session = enabled;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionSnapshot {
    pub phase: SessionPhase,
    pub connection_send_credit: u32,
    pub connection_send_limit: u32,
    pub connection_receive_credit: u32,
    pub connection_receive_limit: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamSnapshot {
    pub phase: StreamPhase,
    pub initiated_by_local: bool,
    pub local_end_stream: bool,
    pub peer_end_stream: bool,
    pub send_credit: u32,
    pub send_limit: u32,
    pub receive_credit: u32,
    pub receive_limit: u32,
}

#[derive(Clone, Debug)]
struct StreamState {
    phase: StreamPhase,
    initiated_by_local: bool,
    local_end_stream: bool,
    peer_end_stream: bool,
    send_credit: u32,
    send_limit: u32,
    receive_credit: u32,
    receive_limit: u32,
}

impl StreamState {
    const fn opening(initiated_by_local: bool) -> Self {
        Self {
            phase: StreamPhase::Opening,
            initiated_by_local,
            local_end_stream: false,
            peer_end_stream: false,
            send_credit: 0,
            send_limit: 0,
            receive_credit: 0,
            receive_limit: 0,
        }
    }

    const fn snapshot(&self) -> StreamSnapshot {
        StreamSnapshot {
            phase: self.phase,
            initiated_by_local: self.initiated_by_local,
            local_end_stream: self.local_end_stream,
            peer_end_stream: self.peer_end_stream,
            send_credit: self.send_credit,
            send_limit: self.send_limit,
            receive_credit: self.receive_credit,
            receive_limit: self.receive_limit,
        }
    }
}

/// Stateful validation for one endpoint of a KBP v1 connection.
///
/// A frame is committed only after all sequence, phase, parity, and flow-control
/// checks pass. Rejected frames therefore do not advance sequence counters or
/// consume credit.
#[derive(Clone, Debug)]
pub struct SessionState {
    config: SessionConfig,
    phase: SessionPhase,
    local_hello: bool,
    peer_hello: bool,
    local_pairing_finish: bool,
    peer_pairing_finish: bool,
    next_local_stream_id: Option<u32>,
    inbound_sequences: HashMap<u32, u32>,
    outbound_sequences: HashMap<u32, u32>,
    streams: HashMap<u32, StreamState>,
    connection_send_credit: u32,
    connection_send_limit: u32,
    connection_receive_credit: u32,
    connection_receive_limit: u32,
}

impl SessionState {
    pub fn new(config: SessionConfig) -> Self {
        Self {
            next_local_stream_id: Some(if config.role.local_streams_are_odd() {
                1
            } else {
                2
            }),
            config,
            phase: SessionPhase::HelloExchange,
            local_hello: false,
            peer_hello: false,
            local_pairing_finish: false,
            peer_pairing_finish: false,
            inbound_sequences: HashMap::new(),
            outbound_sequences: HashMap::new(),
            streams: HashMap::new(),
            connection_send_credit: 0,
            connection_send_limit: 0,
            connection_receive_credit: 0,
            connection_receive_limit: 0,
        }
    }

    pub const fn phase(&self) -> SessionPhase {
        self.phase
    }

    pub const fn snapshot(&self) -> SessionSnapshot {
        SessionSnapshot {
            phase: self.phase,
            connection_send_credit: self.connection_send_credit,
            connection_send_limit: self.connection_send_limit,
            connection_receive_credit: self.connection_receive_credit,
            connection_receive_limit: self.connection_receive_limit,
        }
    }

    pub fn stream(&self, stream_id: u32) -> Option<StreamSnapshot> {
        self.streams.get(&stream_id).map(StreamState::snapshot)
    }

    /// Returns a fresh local stream ID and advances the allocator. Skipped IDs
    /// remain unused; processing OPEN is what permanently claims an ID.
    pub fn allocate_stream_id(&mut self) -> Result<u32, ProtocolError> {
        let stream_id = self
            .next_local_stream_id
            .ok_or(ProtocolError::StreamIdExhausted)?;
        self.next_local_stream_id = stream_id.checked_add(2);
        Ok(stream_id)
    }

    pub fn process_inbound(
        &mut self,
        header: &Header,
        context: FrameContext,
    ) -> Result<(), ProtocolError> {
        self.process(Direction::Inbound, header, context)
    }

    pub fn process_outbound(
        &mut self,
        header: &Header,
        context: FrameContext,
    ) -> Result<(), ProtocolError> {
        self.process(Direction::Outbound, header, context)
    }

    fn process(
        &mut self,
        direction: Direction,
        header: &Header,
        context: FrameContext,
    ) -> Result<(), ProtocolError> {
        header.validate(self.config.limits)?;
        self.validate_context(header, context)?;
        if self.phase == SessionPhase::Closed {
            return Err(self.unexpected(header));
        }
        self.validate_sequence(direction, header)?;

        if header.stream_id == 0 {
            self.apply_control(direction, header, context)?;
        } else if header.command == Command::Open {
            self.apply_open(direction, header)?;
        } else {
            self.apply_stream(direction, header, context)?;
        }

        self.advance_sequence(direction, header);
        Ok(())
    }

    fn validate_context(
        &self,
        header: &Header,
        context: FrameContext,
    ) -> Result<(), ProtocolError> {
        match header.command {
            Command::Hello => {
                if context.initial_stream_window.is_some() {
                    return Err(ProtocolError::UnexpectedStreamWindow(header.command));
                }
                let window = context
                    .initial_connection_window
                    .ok_or(ProtocolError::MissingConnectionWindow)?;
                self.validate_window(window)?;
            }
            Command::Accept => {
                if context.initial_connection_window.is_some() {
                    return Err(ProtocolError::UnexpectedConnectionWindow(header.command));
                }
                let window = context
                    .initial_stream_window
                    .ok_or(ProtocolError::MissingStreamWindow(header.stream_id))?;
                self.validate_window(window)?;
            }
            command => {
                if context.initial_connection_window.is_some() {
                    return Err(ProtocolError::UnexpectedConnectionWindow(command));
                }
                if context.initial_stream_window.is_some() {
                    return Err(ProtocolError::UnexpectedStreamWindow(command));
                }
            }
        }
        Ok(())
    }

    fn validate_window(&self, window: u32) -> Result<(), ProtocolError> {
        if window == 0 || window > self.config.limits.max_window {
            return Err(ProtocolError::InvalidWindow {
                window,
                maximum: self.config.limits.max_window,
            });
        }
        Ok(())
    }

    fn validate_sequence(
        &self,
        direction: Direction,
        header: &Header,
    ) -> Result<(), ProtocolError> {
        let sequences = match direction {
            Direction::Inbound => &self.inbound_sequences,
            Direction::Outbound => &self.outbound_sequences,
        };
        let expected = sequences.get(&header.stream_id).copied().unwrap_or(0);
        if header.sequence != expected {
            return Err(ProtocolError::Sequence {
                stream_id: header.stream_id,
                expected,
                actual: header.sequence,
            });
        }
        if header.sequence == u32::MAX && header.command != Command::Reset {
            return Err(ProtocolError::SequenceExhausted {
                stream_id: header.stream_id,
            });
        }
        Ok(())
    }

    fn advance_sequence(&mut self, direction: Direction, header: &Header) {
        if header.sequence == u32::MAX {
            return;
        }
        let sequences = match direction {
            Direction::Inbound => &mut self.inbound_sequences,
            Direction::Outbound => &mut self.outbound_sequences,
        };
        sequences.insert(header.stream_id, header.sequence + 1);
    }

    fn apply_control(
        &mut self,
        direction: Direction,
        header: &Header,
        context: FrameContext,
    ) -> Result<(), ProtocolError> {
        match header.command {
            Command::Hello => self.apply_hello(direction, context),
            Command::PairingFinish => self.apply_pairing_finish(direction),
            Command::Credit => {
                self.require_online_or_going(header)?;
                if direction == Direction::Inbound {
                    self.connection_send_credit = checked_credit(
                        0,
                        self.connection_send_credit,
                        header.credit_delta,
                        self.connection_send_limit,
                    )?;
                } else {
                    self.connection_receive_credit = checked_credit(
                        0,
                        self.connection_receive_credit,
                        header.credit_delta,
                        self.connection_receive_limit,
                    )?;
                }
                Ok(())
            }
            Command::Ping | Command::Pong => self.require_online_or_going(header),
            Command::GoAway => {
                self.require_online_or_going(header)?;
                self.phase = SessionPhase::GoingAway;
                Ok(())
            }
            Command::Error => {
                self.phase = SessionPhase::Closed;
                Ok(())
            }
            _ => Err(self.unexpected(header)),
        }
    }

    fn apply_hello(
        &mut self,
        direction: Direction,
        context: FrameContext,
    ) -> Result<(), ProtocolError> {
        if self.phase != SessionPhase::HelloExchange {
            return Err(ProtocolError::DuplicateHello);
        }
        let window = context
            .initial_connection_window
            .ok_or(ProtocolError::MissingConnectionWindow)?;
        if direction == Direction::Outbound {
            if self.local_hello {
                return Err(ProtocolError::DuplicateHello);
            }
            self.local_hello = true;
            self.connection_receive_credit = window;
            self.connection_receive_limit = window;
        } else {
            if self.peer_hello {
                return Err(ProtocolError::DuplicateHello);
            }
            self.peer_hello = true;
            self.connection_send_credit = window;
            self.connection_send_limit = window;
        }
        if self.local_hello && self.peer_hello {
            self.phase = SessionPhase::Online;
        }
        Ok(())
    }

    fn apply_pairing_finish(&mut self, direction: Direction) -> Result<(), ProtocolError> {
        if !self.config.pairing_session {
            return Err(ProtocolError::PairingFinishOnRegularSession);
        }
        if self.phase != SessionPhase::HelloExchange {
            return Err(ProtocolError::DuplicatePairingFinish);
        }
        let seen = if direction == Direction::Outbound {
            &mut self.local_pairing_finish
        } else {
            &mut self.peer_pairing_finish
        };
        if *seen {
            return Err(ProtocolError::DuplicatePairingFinish);
        }
        *seen = true;
        Ok(())
    }

    fn apply_open(&mut self, direction: Direction, header: &Header) -> Result<(), ProtocolError> {
        if self.phase != SessionPhase::Online {
            return Err(self.unexpected(header));
        }
        let expected_odd = if direction == Direction::Outbound {
            self.config.role.local_streams_are_odd()
        } else {
            !self.config.role.local_streams_are_odd()
        };
        if (header.stream_id % 2 == 1) != expected_odd {
            return Err(ProtocolError::WrongStreamParity {
                stream_id: header.stream_id,
                expected_odd,
            });
        }
        if self.streams.contains_key(&header.stream_id) {
            return Err(ProtocolError::StreamAlreadyUsed(header.stream_id));
        }
        self.streams.insert(
            header.stream_id,
            StreamState::opening(direction.sender_is_local()),
        );

        if direction == Direction::Outbound
            && self
                .next_local_stream_id
                .is_some_and(|next| header.stream_id >= next)
        {
            self.next_local_stream_id = header.stream_id.checked_add(2);
        }
        Ok(())
    }

    fn apply_stream(
        &mut self,
        direction: Direction,
        header: &Header,
        context: FrameContext,
    ) -> Result<(), ProtocolError> {
        let phase = self
            .streams
            .get(&header.stream_id)
            .ok_or(ProtocolError::UnknownStream(header.stream_id))?
            .phase;
        match header.command {
            Command::Accept | Command::Reject => {
                if phase != StreamPhase::Opening {
                    return Err(ProtocolError::StreamNotOpening(header.stream_id));
                }
                self.apply_open_response(direction, header, context)
            }
            Command::Data | Command::Credit | Command::Close | Command::Reset => {
                if phase != StreamPhase::Accepted {
                    return Err(ProtocolError::StreamNotAccepted(header.stream_id));
                }
                match header.command {
                    Command::Data => self.apply_data(direction, header),
                    Command::Credit => self.apply_stream_credit(direction, header),
                    Command::Close => self.apply_close(direction, header.stream_id),
                    Command::Reset => {
                        self.streams
                            .get_mut(&header.stream_id)
                            .expect("stream existence checked")
                            .phase = StreamPhase::Reset;
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            }
            _ => Err(self.unexpected(header)),
        }
    }

    fn apply_open_response(
        &mut self,
        direction: Direction,
        header: &Header,
        context: FrameContext,
    ) -> Result<(), ProtocolError> {
        let stream = self
            .streams
            .get_mut(&header.stream_id)
            .expect("stream existence checked");
        let responder_is_local = !stream.initiated_by_local;
        if direction.sender_is_local() != responder_is_local {
            return Err(ProtocolError::WrongOpeningResponder(header.stream_id));
        }

        if header.command == Command::Reject {
            stream.phase = StreamPhase::Rejected;
            return Ok(());
        }

        let window = context
            .initial_stream_window
            .ok_or(ProtocolError::MissingStreamWindow(header.stream_id))?;
        stream.phase = StreamPhase::Accepted;
        stream.send_limit = window;
        stream.receive_limit = window;
        if direction == Direction::Inbound {
            // The peer's ACCEPT grants the local initiator its initial window.
            stream.send_credit = window;
        } else {
            // Our ACCEPT grants the peer initiator its initial window.
            stream.receive_credit = window;
        }
        Ok(())
    }

    fn apply_data(&mut self, direction: Direction, header: &Header) -> Result<(), ProtocolError> {
        let length = header.payload_length;
        let stream = self
            .streams
            .get_mut(&header.stream_id)
            .expect("stream existence checked");
        if direction == Direction::Outbound {
            if stream.local_end_stream {
                return Err(ProtocolError::DataAfterEnd(header.stream_id));
            }
            if length > stream.send_credit {
                return Err(ProtocolError::SendCreditExceeded {
                    stream_id: header.stream_id,
                    needed: length,
                    available: stream.send_credit,
                });
            }
            if length > self.connection_send_credit {
                return Err(ProtocolError::ConnectionSendCreditExceeded {
                    needed: length,
                    available: self.connection_send_credit,
                });
            }
            stream.send_credit -= length;
            self.connection_send_credit -= length;
            if header.flags & FLAG_END_STREAM != 0 {
                stream.local_end_stream = true;
            }
        } else {
            if stream.peer_end_stream {
                return Err(ProtocolError::DataAfterEnd(header.stream_id));
            }
            if length > stream.receive_credit {
                return Err(ProtocolError::ReceiveCreditExceeded {
                    stream_id: header.stream_id,
                    needed: length,
                    available: stream.receive_credit,
                });
            }
            if length > self.connection_receive_credit {
                return Err(ProtocolError::ConnectionReceiveCreditExceeded {
                    needed: length,
                    available: self.connection_receive_credit,
                });
            }
            stream.receive_credit -= length;
            self.connection_receive_credit -= length;
            if header.flags & FLAG_END_STREAM != 0 {
                stream.peer_end_stream = true;
            }
        }
        Ok(())
    }

    fn apply_stream_credit(
        &mut self,
        direction: Direction,
        header: &Header,
    ) -> Result<(), ProtocolError> {
        let stream = self
            .streams
            .get_mut(&header.stream_id)
            .expect("stream existence checked");
        if direction == Direction::Inbound {
            stream.send_credit = checked_credit(
                header.stream_id,
                stream.send_credit,
                header.credit_delta,
                stream.send_limit,
            )?;
        } else {
            stream.receive_credit = checked_credit(
                header.stream_id,
                stream.receive_credit,
                header.credit_delta,
                stream.receive_limit,
            )?;
        }
        Ok(())
    }

    fn apply_close(&mut self, direction: Direction, stream_id: u32) -> Result<(), ProtocolError> {
        let stream = self
            .streams
            .get_mut(&stream_id)
            .expect("stream existence checked");
        let responder_is_local = !stream.initiated_by_local;
        if direction.sender_is_local() != responder_is_local {
            return Err(ProtocolError::CloseByInitiator(stream_id));
        }
        stream.local_end_stream = true;
        stream.peer_end_stream = true;
        stream.phase = StreamPhase::Closed;
        Ok(())
    }

    fn require_online_or_going(&self, header: &Header) -> Result<(), ProtocolError> {
        if matches!(self.phase, SessionPhase::Online | SessionPhase::GoingAway) {
            Ok(())
        } else {
            Err(self.unexpected(header))
        }
    }

    fn unexpected(&self, header: &Header) -> ProtocolError {
        ProtocolError::UnexpectedCommand {
            phase: self.phase.name(),
            command: header.command,
            stream_id: header.stream_id,
        }
    }
}

fn checked_credit(
    stream_id: u32,
    current: u32,
    delta: u32,
    maximum: u32,
) -> Result<u32, ProtocolError> {
    let value = current
        .checked_add(delta)
        .filter(|value| *value <= maximum)
        .ok_or(ProtocolError::CreditOverflow {
            stream_id,
            current,
            delta,
            maximum,
        })?;
    Ok(value)
}
