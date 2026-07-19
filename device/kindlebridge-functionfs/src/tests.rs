use std::{
    io::{self, Cursor, Read, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use kindlebridge_transport_tcp::{FrameReader, FrameWriter, TransportConfig};
use kindlebridge_wire::{Command, DecodeLimits, Frame, Header, WireError, HEADER_LEN, MAGIC};

use crate::{
    descriptor_bytes,
    event::{wait_for_enable_bounded, EventError},
    probe::{
        buffer_functionfs_reader, receive_expected, write_control_block, ResynchronizingReader,
        MAX_RESYNCHRONIZE_BYTES,
    },
    run_probe_session, string_bytes, Event, EventKind, FunctionFsError, SessionOutcome,
    SetupPacket, WaitOutcome, DESCRIPTOR_LENGTH, EVENT_SIZE, MAX_FRAME_COUNT, MAX_FUNCTIONFS_IO,
    MAX_PAYLOAD, STRING_LENGTH,
};

const GOLDEN_DESCRIPTORS: [u8; DESCRIPTOR_LENGTH] = [
    0x03, 0x00, 0x00, 0x00, // magic v2
    0xbf, 0x00, 0x00, 0x00, // length 191
    0x0b, 0x00, 0x00, 0x00, // FS | HS | MS OS
    0x03, 0x00, 0x00, 0x00, // fs_count
    0x03, 0x00, 0x00, 0x00, // hs_count
    0x02, 0x00, 0x00, 0x00, // os_count
    0x09, 0x04, 0x00, 0x00, 0x02, 0xff, 0x4b, 0x01, 0x01, // FS interface
    0x07, 0x05, 0x01, 0x02, 0x40, 0x00, 0x00, // FS bulk OUT
    0x07, 0x05, 0x82, 0x02, 0x40, 0x00, 0x00, // FS bulk IN
    0x09, 0x04, 0x00, 0x00, 0x02, 0xff, 0x4b, 0x01, 0x01, // HS interface
    0x07, 0x05, 0x01, 0x02, 0x00, 0x02, 0x00, // HS bulk OUT
    0x07, 0x05, 0x82, 0x02, 0x00, 0x02, 0x00, // HS bulk IN
    0x00, // OS interface
    0x23, 0x00, 0x00, 0x00, // OS dwLength
    0x01, 0x00, // bcdVersion
    0x04, 0x00, // wIndex extended compat
    0x01, 0x00, // bCount, reserved
    0x00, 0x01, // first interface, Reserved1
    b'W', b'I', b'N', b'U', b'S', b'B', 0x00, 0x00, // CompatibleID
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // SubCompatibleID
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Reserved2
    0x00, // OS interface
    0x56, 0x00, 0x00, 0x00, // OS dwLength
    0x01, 0x00, // bcdVersion
    0x05, 0x00, // wIndex extended properties
    0x01, 0x00, // wCount
    0x4b, 0x00, 0x00, 0x00, // property dwSize
    0x07, 0x00, 0x00, 0x00, // REG_MULTI_SZ
    0x15, 0x00, // property name length
    b'D', b'e', b'v', b'i', b'c', b'e', b'I', b'n', b't', b'e', b'r', b'f', b'a', b'c', b'e', b'G',
    b'U', b'I', b'D', b's', 0x00, 0x28, 0x00, 0x00, 0x00, // property data length
    b'{', b'3', b'F', b'5', b'E', b'C', b'0', b'1', b'1', b'-', b'3', b'C', b'D', b'6', b'-', b'4',
    b'E', b'0', b'D', b'-', b'8', b'1', b'9', b'C', b'-', b'3', b'8', b'7', b'B', b'E', b'D', b'7',
    b'D', b'B', b'3', b'B', b'5', b'}', 0x00, 0x00,
];

const GOLDEN_STRINGS: [u8; STRING_LENGTH] = [
    0x02, 0x00, 0x00, 0x00, // strings magic
    0x1f, 0x00, 0x00, 0x00, // length 31
    0x01, 0x00, 0x00, 0x00, // str_count
    0x01, 0x00, 0x00, 0x00, // lang_count
    0x09, 0x04, // en-US
    b'K', b'i', b'n', b'd', b'l', b'e', b'B', b'r', b'i', b'd', b'g', b'e', 0x00,
];

fn event(kind: EventKind) -> [u8; EVENT_SIZE] {
    let mut bytes = [0_u8; EVENT_SIZE];
    bytes[8] = kind as u8;
    bytes
}

fn frame(command: Command, sequence: u32, payload: &[u8]) -> Frame {
    Frame::new(Header::new(command, 0, sequence), payload.to_vec()).unwrap()
}

fn probe_hello(mode: &str, payload_size: u32, rounds: u32) -> Frame {
    frame(
        Command::Hello,
        0,
        format!("kindlebridge-usb-bench/0.2;mode={mode};payload={payload_size};rounds={rounds}")
            .as_bytes(),
    )
}

fn test_payload(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect()
}

#[test]
fn descriptor_blob_matches_functionfs_v2_golden_bytes() {
    assert_eq!(descriptor_bytes(), GOLDEN_DESCRIPTORS);
    assert_eq!(
        crate::DEVICE_INTERFACE_GUID,
        "{3F5EC011-3CD6-4E0D-819C-387BED7DB3B5}"
    );
}

#[test]
fn string_blob_matches_functionfs_golden_bytes() {
    assert_eq!(string_bytes(), GOLDEN_STRINGS);
}

#[test]
fn event_parser_matches_fixed_twelve_byte_abi() {
    let bytes = [0x80, 0x06, 0x34, 0x12, 0x78, 0x56, 0xbc, 0x9a, 4, 0, 0, 0];
    assert_eq!(
        Event::parse(&bytes).unwrap(),
        Event {
            kind: EventKind::Setup,
            setup: SetupPacket {
                request_type: 0x80,
                request: 0x06,
                value: 0x1234,
                index: 0x5678,
                length: 0x9abc,
            },
        }
    );
    assert!(matches!(
        Event::parse(&bytes[..11]),
        Err(EventError::WrongSize(11))
    ));
    let mut unknown = bytes;
    unknown[8] = 7;
    assert!(matches!(
        Event::parse(&unknown),
        Err(EventError::UnknownType(7))
    ));
}

#[test]
fn enable_wait_is_fixed_buffered_and_event_bounded() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&event(EventKind::Bind));
    bytes.extend_from_slice(&event(EventKind::Disable));
    bytes.extend_from_slice(&event(EventKind::Enable));
    assert_eq!(
        wait_for_enable_bounded(&mut Cursor::new(bytes), 3).unwrap(),
        WaitOutcome::Enabled
    );

    let mut only_bind = Vec::new();
    only_bind.extend_from_slice(&event(EventKind::Bind));
    only_bind.extend_from_slice(&event(EventKind::Bind));
    assert!(matches!(
        wait_for_enable_bounded(&mut Cursor::new(only_bind), 2),
        Err(EventError::EventLimit(2))
    ));

    assert_eq!(
        wait_for_enable_bounded(&mut Cursor::new(event(EventKind::Unbind)), 1).unwrap(),
        WaitOutcome::Disconnected
    );
    assert_eq!(
        wait_for_enable_bounded(&mut Cursor::new(Vec::<u8>::new()), 1).unwrap(),
        WaitOutcome::Disconnected
    );
}

#[test]
fn setup_event_is_rejected_without_unbounded_control_transfer() {
    let mut bytes = event(EventKind::Setup);
    bytes[0] = 0x40;
    bytes[1] = 0x55;
    bytes[6..8].copy_from_slice(&0xffff_u16.to_le_bytes());
    assert!(matches!(
        wait_for_enable_bounded(&mut Cursor::new(bytes), 1),
        Err(EventError::UnsupportedSetup(SetupPacket {
            request_type: 0x40,
            request: 0x55,
            length: 0xffff,
            ..
        }))
    ));
}

#[derive(Debug)]
struct ShortControlWriter {
    calls: usize,
}

impl Write for ShortControlWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        Ok(bytes.len() - 1)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn ep0_control_block_is_never_continued_after_short_write() {
    let mut writer = ShortControlWriter { calls: 0 };
    assert!(matches!(
        write_control_block(&mut writer, "descriptor", &GOLDEN_DESCRIPTORS),
        Err(FunctionFsError::PartialControlWrite {
            block: "descriptor",
            expected: DESCRIPTOR_LENGTH,
            actual: 190,
        })
    ));
    assert_eq!(writer.calls, 1);
}

#[derive(Clone, Debug, Default)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl Write for SharedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct TrackingReader {
    inner: Cursor<Vec<u8>>,
    maximum_request: Arc<AtomicUsize>,
}

#[test]
fn functionfs_buffers_protocol_sized_reads_into_controller_sized_requests() {
    let maximum_read = Arc::new(AtomicUsize::new(0));
    let reader = TrackingReader {
        inner: Cursor::new(vec![0xa5; 4096]),
        maximum_request: Arc::clone(&maximum_read),
    };
    let mut reader = buffer_functionfs_reader(reader);
    let mut protocol_header = [0_u8; kindlebridge_wire::HEADER_LEN];

    reader.read_exact(&mut protocol_header).unwrap();

    assert_eq!(maximum_read.load(Ordering::Relaxed), MAX_FUNCTIONFS_IO);
}

#[test]
fn functionfs_io_avoids_fragile_high_order_kernel_allocations() {
    const PAGE_SIZE: usize = 4096;
    const MAX_SAFE_ORDER_TWO_REQUEST: usize = 4 * PAGE_SIZE;
    let request_size = std::hint::black_box(MAX_FUNCTIONFS_IO);

    assert_eq!(request_size % PAGE_SIZE, 0);
    assert!(request_size <= MAX_SAFE_ORDER_TWO_REQUEST);
}

#[test]
fn functionfs_resynchronization_discards_abandoned_payload_and_replays_magic() {
    let mut bytes = vec![0_u8; 257];
    bytes.extend_from_slice(&kindlebridge_wire::MAGIC);
    bytes.extend_from_slice(b"after-magic");
    let mut reader = ResynchronizingReader::new(Cursor::new(bytes));

    reader.resynchronize().unwrap();

    let mut recovered = [0_u8; 15];
    reader.read_exact(&mut recovered).unwrap();
    assert_eq!(&recovered, b"KBP1after-magic");
}

#[test]
fn functionfs_resynchronization_retries_after_a_maximum_fill() {
    let limits = DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD);
    let config = TransportConfig::new(limits);
    let mut bytes = vec![0_u8; MAX_RESYNCHRONIZE_BYTES];
    bytes.extend_from_slice(&frame(Command::Ping, 0, &[]).encode(limits).unwrap());
    bytes.extend_from_slice(&frame(Command::Hello, 0, &[]).encode(limits).unwrap());
    let reader = ResynchronizingReader::new(Cursor::new(bytes));
    let mut frames = FrameReader::new(reader, config).unwrap();

    assert!(frames.get_mut().resynchronize().is_err());
    frames.get_mut().resynchronize().unwrap();
    assert_eq!(frames.read_frame().unwrap().header.command, Command::Ping);
    assert_eq!(frames.read_frame().unwrap().header.command, Command::Hello);
}

#[test]
fn functionfs_resynchronization_skips_false_magic_before_the_marker() {
    let limits = DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD);
    let config = TransportConfig::new(limits);
    let mut bytes = b"abandoned-payload".to_vec();
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&[0_u8; HEADER_LEN]);
    bytes.extend_from_slice(&frame(Command::Ping, 0, &[]).encode(limits).unwrap());
    bytes.extend_from_slice(&frame(Command::Hello, 0, &[]).encode(limits).unwrap());
    let reader = ResynchronizingReader::new(Cursor::new(bytes));
    let mut frames = FrameReader::new(reader, config).unwrap();

    frames.get_mut().resynchronize().unwrap();
    assert!(frames.read_frame().is_err());
    frames.get_mut().resynchronize().unwrap();
    assert_eq!(frames.read_frame().unwrap().header.command, Command::Ping);
    assert_eq!(frames.read_frame().unwrap().header.command, Command::Hello);
}

impl io::Read for TrackingReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.maximum_request
            .fetch_max(buffer.len(), Ordering::Relaxed);
        io::Read::read(&mut self.inner, buffer)
    }
}

#[derive(Clone, Default)]
struct TrackingWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
    maximum_request: Arc<AtomicUsize>,
    request_lengths: Arc<Mutex<Vec<usize>>>,
}

impl Write for TrackingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.maximum_request
            .fetch_max(bytes.len(), Ordering::Relaxed);
        self.request_lengths.lock().unwrap().push(bytes.len());
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn functionfs_keeps_header_and_small_payload_in_one_usb_request_sequence() {
    const USB_MAX_PACKET: usize = 512;
    let limits = DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD);
    let config = TransportConfig::new(limits);
    let tracking = TrackingWriter::default();
    let request_lengths = Arc::clone(&tracking.request_lengths);
    let bounded = crate::probe::FunctionFsIo::new(tracking);
    let mut writer = FrameWriter::new(bounded, config).unwrap();
    let data = Frame::new(Header::new(Command::Data, 1, 1), vec![0x5a; 4096]).unwrap();

    writer.write_frame_contiguous(&data).unwrap();

    let lengths = request_lengths.lock().unwrap();
    assert_eq!(lengths.as_slice(), &[HEADER_LEN + 4096]);
    assert_ne!(lengths.last().unwrap() % USB_MAX_PACKET, 0);
}

#[test]
fn bounded_session_exchanges_hello_multiple_pings_and_goaway() {
    let limits = DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD);
    let payload = test_payload(5);
    let mut input = Vec::new();
    input.extend_from_slice(&probe_hello("duplex", 5, 2).encode(limits).unwrap());
    input.extend_from_slice(&frame(Command::Ping, 1, &payload).encode(limits).unwrap());
    input.extend_from_slice(&frame(Command::Ping, 2, &payload).encode(limits).unwrap());
    input.extend_from_slice(&frame(Command::GoAway, 3, &[]).encode(limits).unwrap());
    let output = SharedWriter::default();
    let captured = Arc::clone(&output.0);

    assert_eq!(
        run_probe_session(Cursor::new(input), output).unwrap(),
        SessionOutcome::Completed
    );

    let config = TransportConfig::new(limits);
    let mut reader =
        FrameReader::new(Cursor::new(captured.lock().unwrap().clone()), config).unwrap();
    let hello = reader.read_frame().unwrap();
    assert_eq!(hello.header.command, Command::Hello);
    assert_eq!(hello.header.sequence, 0);
    assert_eq!(hello.payload.first(), Some(&0xae)); // canonical CBOR map(14)
    assert_eq!(
        reader.read_frame().unwrap(),
        frame(Command::Pong, 1, &payload)
    );
    assert_eq!(
        reader.read_frame().unwrap(),
        frame(Command::Pong, 2, &payload)
    );
    assert_eq!(reader.read_frame().unwrap(), frame(Command::GoAway, 3, &[]));
    assert!(matches!(
        reader.read_frame(),
        Err(kindlebridge_transport_tcp::TransportError::EndOfStream)
    ));
}

#[test]
fn one_mib_frames_are_split_below_the_mtu3_request_limit() {
    let limits = DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD);
    let payload = test_payload(usize::try_from(MAX_PAYLOAD).unwrap());
    let mut input = Vec::new();
    input.extend_from_slice(
        &probe_hello("duplex", MAX_PAYLOAD, 1)
            .encode(limits)
            .unwrap(),
    );
    input.extend_from_slice(&frame(Command::Ping, 1, &payload).encode(limits).unwrap());
    input.extend_from_slice(&frame(Command::GoAway, 2, &[]).encode(limits).unwrap());

    let maximum_read = Arc::new(AtomicUsize::new(0));
    let reader = TrackingReader {
        inner: Cursor::new(input),
        maximum_request: Arc::clone(&maximum_read),
    };
    let writer = TrackingWriter::default();
    let output = Arc::clone(&writer.bytes);
    let maximum_write = Arc::clone(&writer.maximum_request);

    assert_eq!(
        run_probe_session(reader, writer).unwrap(),
        SessionOutcome::Completed
    );
    assert!(maximum_read.load(Ordering::Relaxed) <= MAX_FUNCTIONFS_IO);
    assert!(maximum_write.load(Ordering::Relaxed) <= MAX_FUNCTIONFS_IO);

    let config = TransportConfig::new(limits);
    let mut output_reader =
        FrameReader::new(Cursor::new(output.lock().unwrap().clone()), config).unwrap();
    assert_eq!(
        output_reader.read_frame().unwrap().header.command,
        Command::Hello
    );
    assert_eq!(
        output_reader.read_frame().unwrap(),
        frame(Command::Pong, 1, &payload)
    );
    assert_eq!(
        output_reader.read_frame().unwrap(),
        frame(Command::GoAway, 2, &[])
    );
}

#[test]
fn directional_sessions_move_payload_only_in_the_requested_direction() {
    let limits = DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD);
    let payload = test_payload(4);

    for (mode, host_payload, device_payload) in [
        ("host-to-device", payload.as_slice(), &[][..]),
        ("device-to-host", &[][..], payload.as_slice()),
    ] {
        let mut input = Vec::new();
        input.extend_from_slice(&probe_hello(mode, 4, 1).encode(limits).unwrap());
        input.extend_from_slice(
            &frame(Command::Ping, 1, host_payload)
                .encode(limits)
                .unwrap(),
        );
        input.extend_from_slice(&frame(Command::GoAway, 2, &[]).encode(limits).unwrap());
        let output = SharedWriter::default();
        let captured = Arc::clone(&output.0);

        assert_eq!(
            run_probe_session(Cursor::new(input), output).unwrap(),
            SessionOutcome::Completed
        );

        let config = TransportConfig::new(limits);
        let mut reader =
            FrameReader::new(Cursor::new(captured.lock().unwrap().clone()), config).unwrap();
        assert_eq!(reader.read_frame().unwrap().header.command, Command::Hello);
        assert_eq!(
            reader.read_frame().unwrap(),
            frame(Command::Pong, 1, device_payload)
        );
        assert_eq!(reader.read_frame().unwrap(), frame(Command::GoAway, 2, &[]));
    }
}

#[test]
fn hello_and_directional_payload_mismatches_fail_closed() {
    let limits = DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD);
    let invalid_hello = frame(Command::Hello, 0, b"kindlebridge-usb-bench/0.1")
        .encode(limits)
        .unwrap();
    assert!(matches!(
        run_probe_session(Cursor::new(invalid_hello), Vec::<u8>::new()),
        Err(FunctionFsError::InvalidHello(_))
    ));

    let mut input = Vec::new();
    input.extend_from_slice(&probe_hello("device-to-host", 4, 1).encode(limits).unwrap());
    input.extend_from_slice(
        &frame(Command::Ping, 1, b"unexpected")
            .encode(limits)
            .unwrap(),
    );
    assert!(matches!(
        run_probe_session(Cursor::new(input), Vec::<u8>::new()),
        Err(FunctionFsError::MismatchedPayload {
            mode: "device-to-host",
            sequence: 1,
            expected: 0,
            actual: 10,
        })
    ));
}

#[test]
fn disconnect_and_unexpected_frame_exit_deterministically() {
    assert_eq!(
        run_probe_session(Cursor::new(Vec::<u8>::new()), Vec::<u8>::new()).unwrap(),
        SessionOutcome::Disconnected
    );

    let input = frame(Command::Ping, 0, b"wrong")
        .encode(DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD))
        .unwrap();
    assert!(matches!(
        run_probe_session(Cursor::new(input), Vec::<u8>::new()),
        Err(FunctionFsError::UnexpectedFrame {
            expected: Command::Hello,
            command: Command::Ping,
            ..
        })
    ));
}

#[test]
fn frame_budget_and_one_mib_payload_limit_are_enforced() {
    let config = TransportConfig::new(DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD));
    let input = frame(Command::Hello, 0, &[]).encode(config.limits).unwrap();
    let mut reader = FrameReader::new(Cursor::new(input), config).unwrap();
    let mut count = MAX_FRAME_COUNT;
    assert!(matches!(
        receive_expected(&mut reader, &mut count, Command::Hello, 0, 0),
        Err(FunctionFsError::FrameLimit(MAX_FRAME_COUNT))
    ));

    let mut oversized = Header::new(Command::Hello, 0, 0);
    oversized.payload_length = MAX_PAYLOAD + 1;
    let header = oversized
        .encode(DecodeLimits::new(MAX_PAYLOAD + 1, MAX_PAYLOAD))
        .unwrap();
    assert!(matches!(
        run_probe_session(Cursor::new(header), Vec::<u8>::new()),
        Err(FunctionFsError::Transport(
            kindlebridge_transport_tcp::TransportError::Wire(WireError::PayloadTooLarge {
                length,
                maximum: MAX_PAYLOAD,
            })
        )) if length == MAX_PAYLOAD + 1
    ));
}
