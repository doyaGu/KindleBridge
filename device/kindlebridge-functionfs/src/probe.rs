use std::{
    fmt,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use rustix::{
    event::{poll, PollFd, PollFlags},
    fs::{open, Mode, OFlags},
};

use kindlebridge_transport_tcp::{
    FrameIo, FrameReader, FrameWriter, IoOperation, TransportConfig, TransportError,
};
use kindlebridge_wire::{Command, DecodeLimits, Frame, Header, MAGIC};

use crate::{descriptor_bytes, string_bytes, wait_for_enable, EventError, WaitOutcome};

pub const MAX_PAYLOAD: u32 = 1024 * 1024;
pub const MAX_FRAME_COUNT: u64 = 100_000;
// Keep each FunctionFS request at order 2 (four 4 KiB pages). The KT6 4.9
// kernel tries to allocate the whole request as physically contiguous memory;
// 63 KiB requests become order-4 allocations and fail once normal uptime has
// fragmented memory, even when tens of MiB remain free.
pub const MAX_FUNCTIONFS_IO: usize = 16 * 1024;
const MAX_ROUNDS: u32 = 99_998;
pub(crate) const MAX_RESYNCHRONIZE_BYTES: usize = 16 * 1024 * 1024;

pub(crate) fn buffer_functionfs_reader<R: Read>(reader: R) -> BufReader<R> {
    BufReader::with_capacity(MAX_FUNCTIONFS_IO, reader)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbeMode {
    Duplex,
    HostToDevice,
    DeviceToHost,
}

impl ProbeMode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "duplex" => Some(Self::Duplex),
            "host-to-device" => Some(Self::HostToDevice),
            "device-to-host" => Some(Self::DeviceToHost),
            _ => None,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Duplex => "duplex",
            Self::HostToDevice => "host-to-device",
            Self::DeviceToHost => "device-to-host",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProbeSessionConfig {
    mode: ProbeMode,
    payload_size: u32,
    rounds: u32,
}

/// FunctionFS endpoint adapter that keeps kernel requests within the stable
/// order-2 allocation size while allowing larger logical KBP frames.
#[derive(Debug)]
pub struct FunctionFsIo<T>(T);

impl<T> FunctionFsIo<T> {
    #[must_use]
    pub const fn new(inner: T) -> Self {
        Self(inner)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Read> Read for FunctionFsIo<T> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let length = buffer.len().min(MAX_FUNCTIONFS_IO);
        self.0.read(&mut buffer[..length])
    }
}

impl<T: BufRead> BufRead for FunctionFsIo<T> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        let buffer = self.0.fill_buf()?;
        Ok(&buffer[..buffer.len().min(MAX_FUNCTIONFS_IO)])
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

impl<T: Write> Write for FunctionFsIo<T> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.write(&buffer[..buffer.len().min(MAX_FUNCTIONFS_IO)])
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

#[derive(Debug)]
pub struct FunctionFsEndpoints {
    pub bulk_out: FunctionFsIo<BufReader<FunctionFsReadEndpoint>>,
    pub bulk_in: FunctionFsIo<FunctionFsWriteEndpoint>,
}

#[derive(Debug)]
pub(crate) struct ResynchronizingReader<R> {
    inner: R,
    magic_offset: usize,
}

impl<R> ResynchronizingReader<R> {
    pub(crate) const fn new(inner: R) -> Self {
        Self {
            inner,
            magic_offset: MAGIC.len(),
        }
    }
}

impl<R: BufRead> ResynchronizingReader<R> {
    pub(crate) fn resynchronize(&mut self) -> std::io::Result<()> {
        self.magic_offset = MAGIC.len();
        let mut matched = 0_usize;
        let mut scanned = 0_usize;
        while scanned < MAX_RESYNCHRONIZE_BYTES {
            let (consumed, found) = {
                let available = self.inner.fill_buf()?;
                if available.is_empty() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "FunctionFS endpoint ended during KBP resynchronization",
                    ));
                }
                let length = available.len().min(MAX_RESYNCHRONIZE_BYTES - scanned);
                let mut consumed = 0_usize;
                let mut found = false;
                for byte in &available[..length] {
                    consumed += 1;
                    if *byte == MAGIC[matched] {
                        matched += 1;
                        if matched == MAGIC.len() {
                            found = true;
                            break;
                        }
                    } else {
                        matched = usize::from(*byte == MAGIC[0]);
                    }
                }
                (consumed, found)
            };
            self.inner.consume(consumed);
            scanned += consumed;
            if found {
                self.magic_offset = 0;
                return Ok(());
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "KBP frame boundary was not found within the USB recovery limit",
        ))
    }
}

impl<R: Read> Read for ResynchronizingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.magic_offset < MAGIC.len() {
            let length = buffer.len().min(MAGIC.len() - self.magic_offset);
            buffer[..length].copy_from_slice(&MAGIC[self.magic_offset..self.magic_offset + length]);
            self.magic_offset += length;
            return Ok(length);
        }
        self.inner.read(buffer)
    }
}

/// KBP framing over one configured FunctionFS endpoint pair. Unlike a TCP
/// stream, a cancelled USB transfer may leave the tail of its payload queued;
/// resynchronization scans to the next frame magic before admitting a new host.
#[derive(Debug)]
pub struct FunctionFsFrameStream {
    reader: FrameReader<ResynchronizingReader<FunctionFsIo<BufReader<FunctionFsReadEndpoint>>>>,
    writer: FrameWriter<FunctionFsIo<FunctionFsWriteEndpoint>>,
}

impl FunctionFsFrameStream {
    pub fn new(
        endpoints: FunctionFsEndpoints,
        config: TransportConfig,
    ) -> Result<Self, TransportError> {
        let (bulk_out, bulk_in) = endpoints.split();
        Ok(Self {
            reader: FrameReader::new(ResynchronizingReader::new(bulk_out), config)?,
            writer: FrameWriter::new(bulk_in, config)?,
        })
    }
}

impl FrameIo for FunctionFsFrameStream {
    fn read_frame(&mut self) -> Result<Frame, TransportError> {
        self.reader.read_frame()
    }

    fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
        self.writer.write_frame_contiguous(frame)
    }

    fn flush(&mut self) -> Result<(), TransportError> {
        self.writer.flush()
    }

    fn resynchronize(&mut self) -> Result<(), TransportError> {
        self.reader
            .get_mut()
            .resynchronize()
            .map_err(|source| TransportError::Io {
                operation: IoOperation::ReadHeader,
                source,
            })
    }
}

impl FunctionFsEndpoints {
    #[must_use]
    pub fn split(
        self,
    ) -> (
        FunctionFsIo<BufReader<FunctionFsReadEndpoint>>,
        FunctionFsIo<FunctionFsWriteEndpoint>,
    ) {
        (self.bulk_out, self.bulk_in)
    }
}

#[derive(Debug)]
pub struct FunctionFsReadEndpoint {
    file: File,
}

impl Read for FunctionFsReadEndpoint {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        {
            loop {
                wait_until_ready(&self.file, PollFlags::IN)?;
                match self.file.read(buffer) {
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    outcome => return outcome,
                }
            }
        }
        #[cfg(not(unix))]
        self.file.read(buffer)
    }
}

#[derive(Debug)]
pub struct FunctionFsWriteEndpoint {
    file: File,
}

impl Write for FunctionFsWriteEndpoint {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        {
            loop {
                wait_until_ready(&self.file, PollFlags::OUT)?;
                match self.file.write(buffer) {
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    outcome => return outcome,
                }
            }
        }
        #[cfg(not(unix))]
        self.file.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

#[cfg(unix)]
fn wait_until_ready(file: &File, expected: PollFlags) -> std::io::Result<()> {
    loop {
        let mut fds = [PollFd::new(file, expected)];
        match poll(&mut fds, None) {
            Ok(0) => continue,
            Ok(_) => {
                let ready = fds[0].revents();
                if ready.intersects(PollFlags::ERR | PollFlags::HUP | PollFlags::NVAL) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "FunctionFS endpoint disconnected",
                    ));
                }
                if ready.contains(expected) {
                    return Ok(());
                }
            }
            Err(error) => {
                let error = std::io::Error::from(error);
                if error.kind() != std::io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

/// An initialized FunctionFS control endpoint. The caller is responsible only
/// for preparing/mounting the gadget; this type never touches configfs or UDC.
#[derive(Debug)]
pub struct FunctionFsDevice {
    functionfs_dir: PathBuf,
    ep0: File,
}

impl FunctionFsDevice {
    pub fn open(functionfs_dir: &Path) -> Result<Self, FunctionFsError> {
        let ep0_path = functionfs_dir.join("ep0");
        let mut ep0 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&ep0_path)
            .map_err(|source| FunctionFsError::OpenEndpoint {
                path: ep0_path,
                source,
            })?;
        write_control_block(&mut ep0, "descriptor", &descriptor_bytes())?;
        write_control_block(&mut ep0, "string", &string_bytes())?;
        Ok(Self {
            functionfs_dir: functionfs_dir.to_path_buf(),
            ep0,
        })
    }

    /// Wait for the next host configuration and open one full-duplex endpoint pair.
    /// `None` means FunctionFS was unbound; a service manager may reopen it after
    /// the gadget is prepared again.
    pub fn accept(&mut self) -> Result<Option<FunctionFsEndpoints>, FunctionFsError> {
        if wait_for_enable(&mut self.ep0)? == WaitOutcome::Disconnected {
            return Ok(None);
        }
        Ok(Some(FunctionFsEndpoints {
            bulk_out: FunctionFsIo::new(buffer_functionfs_reader(open_read_endpoint(
                &self.functionfs_dir.join("ep1"),
            )?)),
            bulk_in: FunctionFsIo::new(open_write_endpoint(&self.functionfs_dir.join("ep2"))?),
        }))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionOutcome {
    Completed,
    Disconnected,
}

#[derive(Debug)]
pub enum FunctionFsError {
    OpenEndpoint {
        path: PathBuf,
        source: std::io::Error,
    },
    ControlWrite {
        block: &'static str,
        source: std::io::Error,
    },
    PartialControlWrite {
        block: &'static str,
        expected: usize,
        actual: usize,
    },
    Event(EventError),
    Transport(TransportError),
    UnexpectedFrame {
        expected: Command,
        command: Command,
        stream_id: u32,
        sequence: u32,
    },
    InvalidHello(String),
    MismatchedPayload {
        mode: &'static str,
        sequence: u32,
        expected: usize,
        actual: usize,
    },
    FrameLimit(u64),
}

impl fmt::Display for FunctionFsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenEndpoint { path, source } => {
                write!(formatter, "cannot open {}: {source}", path.display())
            }
            Self::ControlWrite { block, source } => {
                write!(
                    formatter,
                    "writing FunctionFS {block} block failed: {source}"
                )
            }
            Self::PartialControlWrite {
                block,
                expected,
                actual,
            } => write!(
                formatter,
                "FunctionFS {block} block accepted {actual} of {expected} bytes"
            ),
            Self::Event(error) => error.fmt(formatter),
            Self::Transport(error) => error.fmt(formatter),
            Self::UnexpectedFrame {
                expected,
                command,
                stream_id,
                sequence,
            } => write!(
                formatter,
                "expected {expected:?}, got {command:?} stream={stream_id} sequence={sequence}"
            ),
            Self::InvalidHello(reason) => {
                write!(formatter, "invalid USB probe HELLO: {reason}")
            }
            Self::MismatchedPayload {
                mode,
                sequence,
                expected,
                actual,
            } => write!(
                formatter,
                "{mode} payload mismatch at sequence {sequence}: expected {expected} bytes, got {actual}"
            ),
            Self::FrameLimit(limit) => write!(formatter, "KBP frame limit {limit} exceeded"),
        }
    }
}

impl std::error::Error for FunctionFsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OpenEndpoint { source, .. } | Self::ControlWrite { source, .. } => Some(source),
            Self::Event(error) => Some(error),
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

impl From<EventError> for FunctionFsError {
    fn from(value: EventError) -> Self {
        Self::Event(value)
    }
}

impl From<TransportError> for FunctionFsError {
    fn from(value: TransportError) -> Self {
        Self::Transport(value)
    }
}

/// Run the probe against an externally prepared FunctionFS mount directory.
pub fn run(functionfs_dir: &Path) -> Result<SessionOutcome, FunctionFsError> {
    let mut device = FunctionFsDevice::open(functionfs_dir)?;
    let Some(endpoints) = device.accept()? else {
        return Ok(SessionOutcome::Disconnected);
    };
    let (bulk_out, bulk_in) = endpoints.split();
    run_probe_session(bulk_out, bulk_in)
}

fn open_read_endpoint(path: &Path) -> Result<FunctionFsReadEndpoint, FunctionFsError> {
    open_endpoint(path, true).map(|file| FunctionFsReadEndpoint { file })
}

fn open_write_endpoint(path: &Path) -> Result<FunctionFsWriteEndpoint, FunctionFsError> {
    open_endpoint(path, false).map(|file| FunctionFsWriteEndpoint { file })
}

fn open_endpoint(path: &Path, read: bool) -> Result<File, FunctionFsError> {
    #[cfg(unix)]
    let opened = open(
        path,
        if read {
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC
        } else {
            OFlags::WRONLY | OFlags::NONBLOCK | OFlags::CLOEXEC
        },
        Mode::empty(),
    )
    .map(File::from)
    .map_err(std::io::Error::from);
    #[cfg(not(unix))]
    let opened = OpenOptions::new().read(read).write(!read).open(path);
    opened.map_err(|source| FunctionFsError::OpenEndpoint {
        path: path.to_path_buf(),
        source,
    })
}

pub(crate) fn write_control_block<W: Write>(
    writer: &mut W,
    block: &'static str,
    bytes: &[u8],
) -> Result<(), FunctionFsError> {
    // FunctionFS consumes each control block in one write. Retrying an already
    // partial block would be interpreted as the next control-plane object.
    let actual = loop {
        match writer.write(bytes) {
            Ok(actual) => break actual,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(source) => return Err(FunctionFsError::ControlWrite { block, source }),
        }
    };
    if actual != bytes.len() {
        return Err(FunctionFsError::PartialControlWrite {
            block,
            expected: bytes.len(),
            actual,
        });
    }
    Ok(())
}

/// Bounded device-side throughput exchange:
/// host HELLO -> device HELLO, ordered PING/PONG pairs, then symmetric GOAWAY.
pub fn run_probe_session<R: Read, W: Write>(
    bulk_out: R,
    bulk_in: W,
) -> Result<SessionOutcome, FunctionFsError> {
    let config = TransportConfig::new(DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD));
    let mut reader = FrameReader::new(FunctionFsIo::new(bulk_out), config)?;
    let mut writer = FrameWriter::new(FunctionFsIo::new(bulk_in), config)?;
    let mut received = 0_u64;

    let Some(host_hello) = receive_expected(&mut reader, &mut received, Command::Hello, 0, 0)?
    else {
        return Ok(SessionOutcome::Disconnected);
    };
    let session = parse_hello(&host_hello.payload)?;
    if send_frame(
        &mut writer,
        &Frame::new(Header::new(Command::Hello, 0, 0), hello_payload())?,
    )? == SessionOutcome::Disconnected
    {
        return Ok(SessionOutcome::Disconnected);
    }

    let expected_payload = test_payload(usize::try_from(session.payload_size).map_err(|_| {
        FunctionFsError::InvalidHello("payload size does not fit usize".to_owned())
    })?);
    let mut generated_pong = Frame::new(
        Header::new(Command::Pong, 0, 1),
        if session.mode == ProbeMode::DeviceToHost {
            expected_payload.clone()
        } else {
            Vec::new()
        },
    )?;

    for sequence in 1..=session.rounds {
        let Some(frame) = receive_expected(&mut reader, &mut received, Command::Ping, 0, sequence)?
        else {
            return Ok(SessionOutcome::Disconnected);
        };
        let expected_host_payload = if session.mode == ProbeMode::DeviceToHost {
            &[][..]
        } else {
            expected_payload.as_slice()
        };
        if frame.payload != expected_host_payload {
            return Err(FunctionFsError::MismatchedPayload {
                mode: session.mode.name(),
                sequence,
                expected: expected_host_payload.len(),
                actual: frame.payload.len(),
            });
        }

        let outcome = if session.mode == ProbeMode::Duplex {
            let response = Frame::new(Header::new(Command::Pong, 0, sequence), frame.payload)?;
            send_frame(&mut writer, &response)?
        } else {
            generated_pong.header.sequence = sequence;
            send_frame(&mut writer, &generated_pong)?
        };
        if outcome == SessionOutcome::Disconnected {
            return Ok(SessionOutcome::Disconnected);
        }
    }

    let goaway_sequence = session.rounds + 1;
    let Some(goaway) = receive_expected(
        &mut reader,
        &mut received,
        Command::GoAway,
        0,
        goaway_sequence,
    )?
    else {
        return Ok(SessionOutcome::Disconnected);
    };
    if !goaway.payload.is_empty() {
        return Err(FunctionFsError::MismatchedPayload {
            mode: session.mode.name(),
            sequence: goaway_sequence,
            expected: 0,
            actual: goaway.payload.len(),
        });
    }
    let response = Frame::new(Header::new(Command::GoAway, 0, goaway_sequence), Vec::new())?;
    send_frame(&mut writer, &response)
}

pub(crate) fn receive_expected<R: Read>(
    reader: &mut FrameReader<R>,
    received: &mut u64,
    expected: Command,
    stream_id: u32,
    sequence: u32,
) -> Result<Option<Frame>, FunctionFsError> {
    if *received >= MAX_FRAME_COUNT {
        return Err(FunctionFsError::FrameLimit(MAX_FRAME_COUNT));
    }
    let frame = match reader.read_frame() {
        Ok(frame) => frame,
        Err(error) if transport_disconnected(&error) => return Ok(None),
        Err(error) => return Err(FunctionFsError::Transport(error)),
    };
    *received += 1;
    if frame.header.command != expected
        || frame.header.stream_id != stream_id
        || frame.header.sequence != sequence
    {
        return Err(FunctionFsError::UnexpectedFrame {
            expected,
            command: frame.header.command,
            stream_id: frame.header.stream_id,
            sequence: frame.header.sequence,
        });
    }
    Ok(Some(frame))
}

fn send_frame<W: Write>(
    writer: &mut FrameWriter<W>,
    frame: &Frame,
) -> Result<SessionOutcome, FunctionFsError> {
    if let Err(error) = writer.write_frame(frame) {
        if transport_disconnected(&error) {
            return Ok(SessionOutcome::Disconnected);
        }
        return Err(FunctionFsError::Transport(error));
    }
    if let Err(error) = writer.flush() {
        if transport_disconnected(&error) {
            return Ok(SessionOutcome::Disconnected);
        }
        return Err(FunctionFsError::Transport(error));
    }
    Ok(SessionOutcome::Completed)
}

fn parse_hello(payload: &[u8]) -> Result<ProbeSessionConfig, FunctionFsError> {
    let value = std::str::from_utf8(payload)
        .map_err(|_| FunctionFsError::InvalidHello("payload is not UTF-8".to_owned()))?;
    let mut fields = value.split(';');
    if fields.next() != Some("kindlebridge-usb-bench/0.2") {
        return Err(FunctionFsError::InvalidHello(
            "unsupported probe protocol version".to_owned(),
        ));
    }
    let mode = parse_field(&mut fields, "mode")
        .and_then(ProbeMode::parse)
        .ok_or_else(|| FunctionFsError::InvalidHello("invalid mode".to_owned()))?;
    let payload_size = parse_field(&mut fields, "payload")
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| (1..=MAX_PAYLOAD).contains(value))
        .ok_or_else(|| FunctionFsError::InvalidHello("invalid payload size".to_owned()))?;
    let rounds = parse_field(&mut fields, "rounds")
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| (1..=MAX_ROUNDS).contains(value))
        .ok_or_else(|| FunctionFsError::InvalidHello("invalid round count".to_owned()))?;
    if fields.next().is_some() {
        return Err(FunctionFsError::InvalidHello("unexpected field".to_owned()));
    }
    Ok(ProbeSessionConfig {
        mode,
        payload_size,
        rounds,
    })
}

fn parse_field<'a>(fields: &mut impl Iterator<Item = &'a str>, name: &str) -> Option<&'a str> {
    fields.next()?.strip_prefix(name)?.strip_prefix('=')
}

fn test_payload(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| u8::try_from(index % 251).expect("modulo result fits in u8"))
        .collect()
}

fn transport_disconnected(error: &TransportError) -> bool {
    match error {
        TransportError::EndOfStream => true,
        TransportError::Io { source, .. } => {
            matches!(
                source.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::UnexpectedEof
            ) || matches!(source.raw_os_error(), Some(5 | 19 | 108))
        }
        _ => false,
    }
}

fn hello_payload() -> Vec<u8> {
    let mut entries = vec![
        cbor_entry("abi", cbor_text("arm-kindlehf-linux-gnueabihf")),
        cbor_entry("clock_monotonic_ns", cbor_uint(0)),
        cbor_entry("clock_wall_ns", cbor_uint(0)),
        cbor_entry("features", vec![0x80]),
        cbor_entry("firmware", cbor_text("probe")),
        cbor_entry("identity_fingerprint", cbor_bytes(&[0; 32])),
        cbor_entry("initial_connection_window", cbor_uint(MAX_PAYLOAD.into())),
        cbor_entry("kernel", cbor_text("linux")),
        cbor_entry("limits", vec![0xa0]),
        cbor_entry("max_frame", cbor_uint(MAX_PAYLOAD.into())),
        cbor_entry("model", cbor_text("kindlehf")),
        cbor_entry("protocol_max", cbor_uint(1)),
        cbor_entry("protocol_min", cbor_uint(1)),
        cbor_entry("serial", cbor_text("ffs-probe")),
    ];
    entries.sort_by(|left, right| {
        left.0
            .len()
            .cmp(&right.0.len())
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut output = Vec::new();
    cbor_head(&mut output, 5, entries.len() as u64);
    for (key, value) in entries {
        output.extend_from_slice(&key);
        output.extend_from_slice(&value);
    }
    output
}

fn cbor_entry(key: &str, value: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    (cbor_text(key), value)
}

fn cbor_text(value: &str) -> Vec<u8> {
    let mut output = Vec::new();
    cbor_head(&mut output, 3, value.len() as u64);
    output.extend_from_slice(value.as_bytes());
    output
}

fn cbor_bytes(value: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    cbor_head(&mut output, 2, value.len() as u64);
    output.extend_from_slice(value);
    output
}

fn cbor_uint(value: u64) -> Vec<u8> {
    let mut output = Vec::new();
    cbor_head(&mut output, 0, value);
    output
}

fn cbor_head(output: &mut Vec<u8>, major: u8, value: u64) {
    let prefix = major << 5;
    match value {
        0..=23 => output.push(prefix | value as u8),
        24..=0xff => output.extend_from_slice(&[prefix | 24, value as u8]),
        0x100..=0xffff => {
            output.push(prefix | 25);
            output.extend_from_slice(&(value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            output.push(prefix | 26);
            output.extend_from_slice(&(value as u32).to_be_bytes());
        }
        _ => {
            output.push(prefix | 27);
            output.extend_from_slice(&value.to_be_bytes());
        }
    }
}

impl From<kindlebridge_wire::WireError> for FunctionFsError {
    fn from(value: kindlebridge_wire::WireError) -> Self {
        Self::Transport(TransportError::Wire(value))
    }
}
