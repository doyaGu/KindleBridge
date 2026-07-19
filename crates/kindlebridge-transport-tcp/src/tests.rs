use std::{
    io::{self, Cursor, Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use kindlebridge_wire::{Command, DecodeLimits, Frame, Header, WireError, HEADER_LEN};

use crate::{
    AuthenticatedFramed, AuthenticatedStream, ErrorClass, FrameIo, FrameReader, FrameWriter,
    IoOperation, ShutdownMode, SplitFrameStream, TcpFrameListener, TcpFrameStream, TransportConfig,
    TransportError, HARD_MAX_PAYLOAD,
};

fn loopback() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
}

fn test_config() -> TransportConfig {
    let mut config = TransportConfig::new(DecodeLimits::new(1024, 1024));
    config.connect_timeout = Duration::from_secs(2);
    config.read_timeout = Some(Duration::from_secs(2));
    config.write_timeout = Some(Duration::from_secs(2));
    config
}

fn frame(command: Command, stream_id: u32, sequence: u32, payload: &[u8]) -> Frame {
    Frame::new(Header::new(command, stream_id, sequence), payload.to_vec()).unwrap()
}

#[test]
fn frame_reader_distinguishes_clean_eof_and_truncation() {
    let mut empty = FrameReader::new(Cursor::new(Vec::<u8>::new()), test_config()).unwrap();
    let error = empty.read_frame().unwrap_err();
    assert_eq!(error.class(), ErrorClass::CleanEof);
    assert!(matches!(error, TransportError::EndOfStream));

    let mut partial = FrameReader::new(Cursor::new(vec![0; 17]), test_config()).unwrap();
    assert!(matches!(
        partial.read_frame(),
        Err(TransportError::TruncatedHeader { received: 17 })
    ));

    let value = frame(Command::Data, 1, 0, b"payload");
    let mut bytes = value.encode(test_config().limits).unwrap();
    bytes.truncate(HEADER_LEN + 3);
    let mut partial = FrameReader::new(Cursor::new(bytes), test_config()).unwrap();
    assert!(matches!(
        partial.read_frame(),
        Err(TransportError::TruncatedPayload {
            expected: 7,
            received: 3,
        })
    ));
}

#[test]
fn split_endpoint_stream_uses_the_same_kbp_framing() {
    let incoming = frame(Command::Ping, 0, 7, b"usb-in")
        .encode(test_config().limits)
        .unwrap();
    let mut stream = SplitFrameStream::new(
        Cursor::new(incoming),
        Cursor::new(Vec::<u8>::new()),
        test_config(),
    )
    .unwrap();
    assert_eq!(
        stream.read_frame().unwrap(),
        frame(Command::Ping, 0, 7, b"usb-in")
    );
    stream
        .write_frame(&frame(Command::Pong, 0, 8, b"usb-out"))
        .unwrap();
    stream.flush().unwrap();

    let (_, output) = stream.into_inner();
    let mut reader = FrameReader::new(Cursor::new(output.into_inner()), test_config()).unwrap();
    assert_eq!(
        reader.read_frame().unwrap(),
        frame(Command::Pong, 0, 8, b"usb-out")
    );
}

#[derive(Debug, Default)]
struct ShortWriter {
    bytes: Vec<u8>,
    max_write: usize,
    calls: usize,
}

impl Write for ShortWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        let count = bytes.len().min(self.max_write);
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn frame_writer_uses_write_all_for_header_and_payload() {
    let output = ShortWriter {
        max_write: 3,
        ..ShortWriter::default()
    };
    let mut writer = FrameWriter::new(output, test_config()).unwrap();
    let expected = frame(Command::Data, 1, 0, b"abcdefghij");
    writer.write_frame(&expected).unwrap();
    writer.flush().unwrap();
    let output = writer.into_inner();

    assert!(output.calls > 2);
    let mut reader = FrameReader::new(Cursor::new(output.bytes), test_config()).unwrap();
    assert_eq!(reader.read_frame().unwrap(), expected);
}

#[test]
fn writer_rejects_declared_and_actual_payload_mismatch_before_io() {
    let mut bad = frame(Command::Data, 1, 0, b"abc");
    bad.header.payload_length = 4;
    let mut writer = FrameWriter::new(Vec::<u8>::new(), test_config()).unwrap();
    assert!(matches!(
        writer.write_frame(&bad),
        Err(TransportError::FrameLengthMismatch {
            declared: 4,
            actual: 3,
        })
    ));
    assert!(writer.get_ref().is_empty());
}

#[test]
fn configured_and_wire_payload_limits_are_enforced_before_allocation() {
    let oversized_config =
        TransportConfig::new(DecodeLimits::new(HARD_MAX_PAYLOAD + 1, HARD_MAX_PAYLOAD));
    assert!(matches!(
        oversized_config.validate(),
        Err(TransportError::ConfiguredPayloadLimitTooLarge { .. })
    ));

    let small = TransportConfig::new(DecodeLimits::new(32, 1024));
    let mut oversized = Header::new(Command::Data, 1, 0);
    oversized.payload_length = 33;
    let encoded = oversized.encode(DecodeLimits::new(64, 1024)).unwrap();
    let mut reader = FrameReader::new(Cursor::new(encoded), small).unwrap();
    let error = reader.read_frame().unwrap_err();
    assert_eq!(error.class(), ErrorClass::ResourceLimit);
    assert!(matches!(
        error,
        TransportError::Wire(WireError::PayloadTooLarge {
            length: 33,
            maximum: 32,
        })
    ));
}

#[test]
fn malformed_header_is_a_protocol_error() {
    let valid = Header::new(Command::Ping, 0, 0)
        .encode(test_config().limits)
        .unwrap();
    let mut corrupt = valid;
    corrupt[20] ^= 0x80;
    let mut reader = FrameReader::new(Cursor::new(corrupt), test_config()).unwrap();
    let error = reader.read_frame().unwrap_err();
    assert_eq!(error.class(), ErrorClass::Protocol);
    assert!(matches!(
        error,
        TransportError::Wire(WireError::HeaderCrcMismatch { .. })
    ));
}

#[test]
fn listener_and_client_exchange_frames_on_loopback() {
    let config = test_config();
    let listener = TcpFrameListener::bind(loopback(), config).unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut connection, peer) = listener.accept().unwrap();
        assert!(peer.ip().is_loopback());
        let request = connection.read_frame().unwrap();
        assert_eq!(request, frame(Command::Open, 1, 0, b"sync.v1"));
        connection
            .write_frame(&frame(Command::Accept, 1, 0, b"accepted"))
            .unwrap();
        connection.flush().unwrap();
    });

    let mut client = TcpFrameStream::connect(address, config).unwrap();
    assert_eq!(client.peer_addr().unwrap(), address);
    assert!(client.local_addr().unwrap().ip().is_loopback());
    client
        .write_frame(&frame(Command::Open, 1, 0, b"sync.v1"))
        .unwrap();
    client.flush().unwrap();
    assert_eq!(
        client.read_frame().unwrap(),
        frame(Command::Accept, 1, 0, b"accepted")
    );
    server.join().unwrap();
}

#[test]
fn write_half_close_preserves_the_read_direction() {
    let config = test_config();
    let listener = TcpFrameListener::bind(loopback(), config).unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        assert_eq!(
            connection.read_frame().unwrap(),
            frame(Command::Ping, 0, 0, b"request")
        );
        assert!(matches!(
            connection.read_frame(),
            Err(TransportError::EndOfStream)
        ));
        connection
            .write_frame(&frame(Command::Pong, 0, 0, b"response"))
            .unwrap();
        connection.flush().unwrap();
    });

    let mut client = TcpFrameStream::connect(address, config).unwrap();
    client
        .write_frame(&frame(Command::Ping, 0, 0, b"request"))
        .unwrap();
    client.flush().unwrap();
    client.shutdown(ShutdownMode::Write).unwrap();
    assert_eq!(
        client.read_frame().unwrap(),
        frame(Command::Pong, 0, 0, b"response")
    );
    server.join().unwrap();
}

#[test]
fn tcp_malformed_and_oversized_headers_fail_without_waiting_for_payload() {
    let mut config = test_config();
    config.limits.max_payload = 32;
    let listener = TcpFrameListener::bind(loopback(), config).unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        let error = connection.read_frame().unwrap_err();
        assert!(matches!(
            error,
            TransportError::Wire(WireError::PayloadTooLarge {
                length: 33,
                maximum: 32,
            })
        ));
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut oversized = Header::new(Command::Data, 1, 0);
    oversized.payload_length = 33;
    client
        .write_all(&oversized.encode(DecodeLimits::new(64, 1024)).unwrap())
        .unwrap();
    server.join().unwrap();
}

#[test]
fn socket_read_timeout_is_classified() {
    let mut config = test_config();
    config.read_timeout = Some(Duration::from_millis(75));
    let listener = TcpFrameListener::bind(loopback(), config).unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        connection.read_frame().unwrap_err()
    });

    let _silent_client = TcpStream::connect(address).unwrap();
    let error = server.join().unwrap();
    assert_eq!(error.class(), ErrorClass::Timeout);
    assert_eq!(error.operation(), Some(IoOperation::ReadHeader));
}

#[derive(Debug)]
struct TestAuthenticatedStream {
    input: Cursor<Vec<u8>>,
    output: Arc<Mutex<Vec<u8>>>,
    shutdowns: Arc<Mutex<Vec<ShutdownMode>>>,
}

impl Read for TestAuthenticatedStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.input.read(bytes)
    }
}

impl Write for TestAuthenticatedStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.output.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AuthenticatedStream for TestAuthenticatedStream {
    fn shutdown(&mut self, mode: ShutdownMode) -> io::Result<()> {
        self.shutdowns.lock().unwrap().push(mode);
        Ok(())
    }
}

#[test]
fn authenticated_boundary_accepts_only_explicit_marker_implementations() {
    let incoming = frame(Command::Ping, 0, 0, b"inside-authenticated-stream")
        .encode(test_config().limits)
        .unwrap();
    let output = Arc::new(Mutex::new(Vec::new()));
    let shutdowns = Arc::new(Mutex::new(Vec::new()));
    let stream = TestAuthenticatedStream {
        input: Cursor::new(incoming),
        output: Arc::clone(&output),
        shutdowns: Arc::clone(&shutdowns),
    };
    let mut framed = AuthenticatedFramed::new(stream, test_config()).unwrap();
    assert_eq!(
        framed.read_frame().unwrap(),
        frame(Command::Ping, 0, 0, b"inside-authenticated-stream")
    );
    framed
        .write_frame(&frame(Command::Pong, 0, 0, b"reply"))
        .unwrap();
    framed.shutdown(ShutdownMode::Write).unwrap();

    let bytes = output.lock().unwrap().clone();
    let mut reader = FrameReader::new(Cursor::new(bytes), test_config()).unwrap();
    assert_eq!(
        reader.read_frame().unwrap(),
        frame(Command::Pong, 0, 0, b"reply")
    );
    assert_eq!(*shutdowns.lock().unwrap(), vec![ShutdownMode::Write]);
}
