use crate::{
    crc32c, Command, DecodeLimits, EndpointRole, Frame, FrameContext, Header, ProtocolError,
    SessionConfig, SessionPhase, SessionState, StreamPhase, WireError, FLAG_END_STREAM,
    FLAG_URGENT, HEADER_LEN, MAGIC,
};

const LIMITS: DecodeLimits = DecodeLimits::new(1024, 1024);

fn header(command: Command, stream_id: u32, sequence: u32) -> Header {
    Header::new(command, stream_id, sequence)
}

fn data(stream_id: u32, sequence: u32, length: u32) -> Header {
    let mut value = header(Command::Data, stream_id, sequence);
    value.payload_length = length;
    value
}

fn credit(stream_id: u32, sequence: u32, delta: u32) -> Header {
    let mut value = header(Command::Credit, stream_id, sequence);
    value.credit_delta = delta;
    value
}

fn online_host() -> SessionState {
    let mut session = SessionState::new(SessionConfig::new(EndpointRole::Host, LIMITS));
    session
        .process_outbound(&header(Command::Hello, 0, 0), FrameContext::hello(80))
        .unwrap();
    session
        .process_inbound(&header(Command::Hello, 0, 0), FrameContext::hello(100))
        .unwrap();
    assert_eq!(session.phase(), SessionPhase::Online);
    session
}

#[test]
fn crc32c_matches_iscsi_check_value() {
    assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(crc32c(b""), 0);
}

#[test]
fn fixed_header_is_little_endian_and_round_trips() {
    let mut value = header(Command::Data, 0x1122_3345, 0x5566_7788);
    value.minor = 7;
    value.flags = FLAG_END_STREAM | FLAG_URGENT | 0x8000_0000;
    value.payload_length = 0x0000_0123;

    let encoded = value.encode(LIMITS).unwrap();
    assert_eq!(encoded.len(), HEADER_LEN);
    assert_eq!(&encoded[0..4], &MAGIC);
    assert_eq!(&encoded[4..8], &[1, 0, 7, 0]);
    assert_eq!(&encoded[8..12], &[40, 0, Command::Data as u8, 0]);
    assert_eq!(&encoded[16..20], &[0x45, 0x33, 0x22, 0x11]);
    assert_eq!(&encoded[20..24], &[0x88, 0x77, 0x66, 0x55]);
    assert_eq!(&encoded[24..28], &[0x23, 0x01, 0, 0]);

    let mut crc_input = encoded;
    crc_input[32..36].fill(0);
    assert_eq!(
        u32::from_le_bytes(encoded[32..36].try_into().unwrap()),
        crc32c(&crc_input)
    );
    assert_eq!(Header::decode(&encoded, LIMITS).unwrap(), value);
}

#[test]
fn complete_frame_round_trips_and_requires_exact_length() {
    let value = Frame::new(header(Command::Open, 1, 0), b"sync.v1".to_vec()).unwrap();
    let encoded = value.encode(LIMITS).unwrap();
    assert_eq!(Frame::decode(&encoded, LIMITS).unwrap(), value);

    let truncated = &encoded[..encoded.len() - 1];
    assert!(matches!(
        Frame::decode(truncated, LIMITS),
        Err(WireError::FrameLengthMismatch { .. })
    ));

    let mut trailing = encoded;
    trailing.push(0);
    assert!(matches!(
        Frame::decode(&trailing, LIMITS),
        Err(WireError::FrameLengthMismatch { .. })
    ));
}

#[test]
fn header_crc_detects_every_header_mutation() {
    let encoded = header(Command::Ping, 0, 9).encode(LIMITS).unwrap();
    for offset in [4, 10, 12, 16, 20, 24, 28, 36] {
        let mut corrupt = encoded;
        corrupt[offset] ^= 0x40;
        assert!(matches!(
            Header::decode(&corrupt, LIMITS),
            Err(WireError::HeaderCrcMismatch { .. })
        ));
    }
}

#[test]
fn stateless_rules_reject_invalid_flags_credit_and_streams() {
    let mut wrong_major = header(Command::Ping, 0, 0);
    wrong_major.major = 2;
    assert_eq!(
        wrong_major.validate(LIMITS),
        Err(WireError::UnsupportedMajor(2))
    );

    let mut unknown_critical = header(Command::Data, 1, 0);
    unknown_critical.flags = 0x0000_0002;
    assert_eq!(
        unknown_critical.validate(LIMITS),
        Err(WireError::UnknownCriticalFlags(2))
    );

    let mut bad_end = header(Command::Close, 1, 0);
    bad_end.flags = FLAG_END_STREAM;
    assert_eq!(
        bad_end.validate(LIMITS),
        Err(WireError::EndStreamOnNonData(Command::Close))
    );

    let mut bad_credit = credit(1, 0, 0);
    assert_eq!(
        bad_credit.validate(LIMITS),
        Err(WireError::InvalidCreditDelta(0))
    );
    bad_credit.credit_delta = 8;
    bad_credit.payload_length = 1;
    assert_eq!(
        bad_credit.validate(LIMITS),
        Err(WireError::CreditPayloadNotEmpty(1))
    );

    let mut non_credit = header(Command::Ping, 0, 0);
    non_credit.credit_delta = 1;
    assert!(matches!(
        non_credit.validate(LIMITS),
        Err(WireError::UnexpectedCreditDelta { .. })
    ));

    assert!(matches!(
        header(Command::Open, 0, 0).validate(LIMITS),
        Err(WireError::InvalidStreamForCommand { .. })
    ));
    assert!(matches!(
        header(Command::Hello, 1, 0).validate(LIMITS),
        Err(WireError::InvalidStreamForCommand { .. })
    ));

    let mut advisory = header(Command::Ping, 0, 0);
    advisory.flags = FLAG_URGENT | 0x8000_0000;
    advisory.validate(LIMITS).unwrap();
}

#[test]
fn hello_establishes_independent_directional_connection_windows() {
    let mut session = SessionState::new(SessionConfig::new(EndpointRole::Host, LIMITS));
    session
        .process_outbound(&header(Command::Hello, 0, 0), FrameContext::hello(80))
        .unwrap();
    assert_eq!(session.phase(), SessionPhase::HelloExchange);
    session
        .process_inbound(&header(Command::Hello, 0, 0), FrameContext::hello(100))
        .unwrap();

    assert_eq!(
        session.snapshot(),
        crate::SessionSnapshot {
            phase: SessionPhase::Online,
            connection_send_credit: 100,
            connection_send_limit: 100,
            connection_receive_credit: 80,
            connection_receive_limit: 80,
        }
    );
}

#[test]
fn failed_frame_does_not_advance_sequence() {
    let mut session = online_host();
    assert_eq!(
        session.process_inbound(&header(Command::Hello, 0, 1), FrameContext::hello(100)),
        Err(ProtocolError::DuplicateHello)
    );
    session
        .process_inbound(&header(Command::Ping, 0, 1), FrameContext::default())
        .unwrap();
    assert_eq!(
        session.process_inbound(&header(Command::Pong, 0, 1), FrameContext::default()),
        Err(ProtocolError::Sequence {
            stream_id: 0,
            expected: 2,
            actual: 1,
        })
    );
}

#[test]
fn pairing_finish_is_confined_to_pairing_sessions() {
    let mut regular = SessionState::new(SessionConfig::new(EndpointRole::Host, LIMITS));
    assert_eq!(
        regular.process_outbound(
            &header(Command::PairingFinish, 0, 0),
            FrameContext::default()
        ),
        Err(ProtocolError::PairingFinishOnRegularSession)
    );
    regular
        .process_outbound(&header(Command::Hello, 0, 0), FrameContext::hello(80))
        .unwrap();

    let mut pairing =
        SessionState::new(SessionConfig::new(EndpointRole::Host, LIMITS).pairing(true));
    pairing
        .process_outbound(
            &header(Command::PairingFinish, 0, 0),
            FrameContext::default(),
        )
        .unwrap();
    assert_eq!(
        pairing.process_outbound(
            &header(Command::PairingFinish, 0, 1),
            FrameContext::default()
        ),
        Err(ProtocolError::DuplicatePairingFinish)
    );
    pairing
        .process_outbound(&header(Command::Hello, 0, 1), FrameContext::hello(80))
        .unwrap();
}

#[test]
fn stream_ids_follow_host_odd_device_even_rule() {
    let mut host = online_host();
    host.process_outbound(&header(Command::Open, 1, 0), FrameContext::default())
        .unwrap();
    host.process_inbound(&header(Command::Open, 2, 0), FrameContext::default())
        .unwrap();

    assert_eq!(
        host.process_outbound(&header(Command::Open, 4, 0), FrameContext::default()),
        Err(ProtocolError::WrongStreamParity {
            stream_id: 4,
            expected_odd: true,
        })
    );
    assert_eq!(
        host.process_inbound(&header(Command::Open, 3, 0), FrameContext::default()),
        Err(ProtocolError::WrongStreamParity {
            stream_id: 3,
            expected_odd: false,
        })
    );

    let mut device = SessionState::new(SessionConfig::new(EndpointRole::Device, LIMITS));
    device
        .process_outbound(&header(Command::Hello, 0, 0), FrameContext::hello(80))
        .unwrap();
    device
        .process_inbound(&header(Command::Hello, 0, 0), FrameContext::hello(80))
        .unwrap();
    device
        .process_outbound(&header(Command::Open, 2, 0), FrameContext::default())
        .unwrap();
    device
        .process_inbound(&header(Command::Open, 1, 0), FrameContext::default())
        .unwrap();
}

#[test]
fn opening_requires_the_other_endpoint_to_accept_or_reject() {
    let mut session = online_host();
    session
        .process_outbound(&header(Command::Open, 1, 0), FrameContext::default())
        .unwrap();
    assert_eq!(
        session.process_outbound(&header(Command::Accept, 1, 1), FrameContext::accept(50)),
        Err(ProtocolError::WrongOpeningResponder(1))
    );
    session
        .process_inbound(&header(Command::Accept, 1, 0), FrameContext::accept(50))
        .unwrap();
    assert_eq!(session.stream(1).unwrap().phase, StreamPhase::Accepted);
}

#[test]
fn outbound_data_consumes_both_credit_levels_and_credit_restores_each() {
    let mut session = online_host();
    session
        .process_outbound(&header(Command::Open, 1, 0), FrameContext::default())
        .unwrap();
    session
        .process_inbound(&header(Command::Accept, 1, 0), FrameContext::accept(60))
        .unwrap();

    session
        .process_outbound(&data(1, 1, 40), FrameContext::default())
        .unwrap();
    assert_eq!(session.stream(1).unwrap().send_credit, 20);
    assert_eq!(session.snapshot().connection_send_credit, 60);

    assert_eq!(
        session.process_outbound(&data(1, 2, 30), FrameContext::default()),
        Err(ProtocolError::SendCreditExceeded {
            stream_id: 1,
            needed: 30,
            available: 20,
        })
    );
    assert_eq!(session.snapshot().connection_send_credit, 60);

    session
        .process_inbound(&credit(1, 1, 40), FrameContext::default())
        .unwrap();
    session
        .process_inbound(&credit(0, 1, 40), FrameContext::default())
        .unwrap();
    session
        .process_outbound(&data(1, 2, 30), FrameContext::default())
        .unwrap();
    assert_eq!(session.stream(1).unwrap().send_credit, 30);
    assert_eq!(session.snapshot().connection_send_credit, 70);
}

#[test]
fn inbound_data_requires_granted_credit_and_end_stream_half_closes() {
    let mut session = online_host();
    session
        .process_inbound(&header(Command::Open, 2, 0), FrameContext::default())
        .unwrap();
    session
        .process_outbound(&header(Command::Accept, 2, 0), FrameContext::accept(50))
        .unwrap();

    session
        .process_inbound(&data(2, 1, 30), FrameContext::default())
        .unwrap();
    assert_eq!(session.stream(2).unwrap().receive_credit, 20);
    assert_eq!(session.snapshot().connection_receive_credit, 50);

    session
        .process_outbound(&credit(2, 1, 30), FrameContext::default())
        .unwrap();
    session
        .process_outbound(&credit(0, 1, 30), FrameContext::default())
        .unwrap();

    let mut final_data = data(2, 2, 10);
    final_data.flags = FLAG_END_STREAM;
    session
        .process_inbound(&final_data, FrameContext::default())
        .unwrap();
    assert!(session.stream(2).unwrap().peer_end_stream);

    assert_eq!(
        session.process_inbound(&data(2, 3, 1), FrameContext::default()),
        Err(ProtocolError::DataAfterEnd(2))
    );
    session
        .process_inbound(&header(Command::Reset, 2, 3), FrameContext::default())
        .unwrap();
    assert_eq!(session.stream(2).unwrap().phase, StreamPhase::Reset);
}

#[test]
fn stream_and_connection_credit_cannot_exceed_advertised_windows() {
    let mut session = online_host();
    session
        .process_outbound(&header(Command::Open, 1, 0), FrameContext::default())
        .unwrap();
    session
        .process_inbound(&header(Command::Accept, 1, 0), FrameContext::accept(50))
        .unwrap();

    assert_eq!(
        session.process_inbound(&credit(1, 1, 1), FrameContext::default()),
        Err(ProtocolError::CreditOverflow {
            stream_id: 1,
            current: 50,
            delta: 1,
            maximum: 50,
        })
    );
    session
        .process_inbound(&header(Command::Close, 1, 1), FrameContext::default())
        .unwrap();

    assert_eq!(
        session.process_inbound(&credit(0, 1, 1), FrameContext::default()),
        Err(ProtocolError::CreditOverflow {
            stream_id: 0,
            current: 100,
            delta: 1,
            maximum: 100,
        })
    );
    session
        .process_inbound(&header(Command::Ping, 0, 1), FrameContext::default())
        .unwrap();
}

#[test]
fn only_responder_closes_and_stream_ids_are_never_reused() {
    let mut session = online_host();
    session
        .process_outbound(&header(Command::Open, 1, 0), FrameContext::default())
        .unwrap();
    session
        .process_inbound(&header(Command::Accept, 1, 0), FrameContext::accept(50))
        .unwrap();

    assert_eq!(
        session.process_outbound(&header(Command::Close, 1, 1), FrameContext::default()),
        Err(ProtocolError::CloseByInitiator(1))
    );
    session
        .process_inbound(&header(Command::Close, 1, 1), FrameContext::default())
        .unwrap();
    let closed = session.stream(1).unwrap();
    assert_eq!(closed.phase, StreamPhase::Closed);
    assert!(closed.local_end_stream && closed.peer_end_stream);

    assert_eq!(
        session.process_outbound(&header(Command::Open, 1, 1), FrameContext::default()),
        Err(ProtocolError::StreamAlreadyUsed(1))
    );
}

#[test]
fn in_flight_credit_after_stream_close_is_ignored() {
    let mut device = SessionState::new(SessionConfig::new(EndpointRole::Device, LIMITS));
    device
        .process_outbound(&header(Command::Hello, 0, 0), FrameContext::hello(80))
        .unwrap();
    device
        .process_inbound(&header(Command::Hello, 0, 0), FrameContext::hello(100))
        .unwrap();
    device
        .process_inbound(&header(Command::Open, 1, 0), FrameContext::default())
        .unwrap();
    device
        .process_outbound(&header(Command::Accept, 1, 0), FrameContext::accept(50))
        .unwrap();
    device
        .process_outbound(&header(Command::Close, 1, 1), FrameContext::default())
        .unwrap();

    device
        .process_inbound(&credit(1, 1, 10), FrameContext::default())
        .unwrap();
    assert_eq!(device.stream(1).unwrap().phase, StreamPhase::Closed);
}

#[test]
fn reject_is_terminal_and_goaway_blocks_new_streams() {
    let mut session = online_host();
    session
        .process_outbound(&header(Command::Open, 1, 0), FrameContext::default())
        .unwrap();
    session
        .process_inbound(&header(Command::Reject, 1, 0), FrameContext::default())
        .unwrap();
    assert_eq!(session.stream(1).unwrap().phase, StreamPhase::Rejected);
    assert!(matches!(
        session.process_inbound(&data(1, 1, 1), FrameContext::default()),
        Err(ProtocolError::StreamNotAccepted(1))
    ));

    session
        .process_inbound(&header(Command::GoAway, 0, 1), FrameContext::default())
        .unwrap();
    assert_eq!(session.phase(), SessionPhase::GoingAway);
    assert!(matches!(
        session.process_outbound(&header(Command::Open, 3, 0), FrameContext::default()),
        Err(ProtocolError::UnexpectedCommand { .. })
    ));
}

#[test]
fn allocator_returns_role_parity_without_reuse() {
    let mut host = SessionState::new(SessionConfig::new(EndpointRole::Host, LIMITS));
    assert_eq!(host.allocate_stream_id().unwrap(), 1);
    assert_eq!(host.allocate_stream_id().unwrap(), 3);

    let mut device = SessionState::new(SessionConfig::new(EndpointRole::Device, LIMITS));
    assert_eq!(device.allocate_stream_id().unwrap(), 2);
    assert_eq!(device.allocate_stream_id().unwrap(), 4);
}

#[test]
fn hello_and_accept_require_decoded_window_metadata() {
    let mut session = SessionState::new(SessionConfig::new(EndpointRole::Host, LIMITS));
    assert_eq!(
        session.process_outbound(&header(Command::Hello, 0, 0), FrameContext::default()),
        Err(ProtocolError::MissingConnectionWindow)
    );
    assert_eq!(
        session.process_outbound(&header(Command::Hello, 0, 0), FrameContext::hello(0)),
        Err(ProtocolError::InvalidWindow {
            window: 0,
            maximum: 1024,
        })
    );

    let mut session = online_host();
    session
        .process_outbound(&header(Command::Open, 1, 0), FrameContext::default())
        .unwrap();
    assert_eq!(
        session.process_inbound(&header(Command::Accept, 1, 0), FrameContext::default()),
        Err(ProtocolError::MissingStreamWindow(1))
    );
    session
        .process_inbound(&header(Command::Accept, 1, 0), FrameContext::accept(50))
        .unwrap();
}
