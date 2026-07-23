use std::io::{self, BufRead, BufReader, BufWriter, IsTerminal, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use crossterm::event::{
    self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers,
};
use crossterm::{execute, terminal};
use interprocess::local_socket::{prelude::*, SendHalf, Stream};
use kindlebridge::{CommandOutput, ShellArgs};
use kindlebridge_schema::device_protocol::{ShellMode, ShellOpen, TerminalSize, SHELL_V2_FEATURE};
use kindlebridge_schema::{
    methods, read_json_frame, write_json_frame, DeviceFeatures, RequestId, RpcClient, RpcRequest,
    RpcResponse, ShellOpenParams, ShellOpenResult, StreamChannel, StreamClosedParams,
    StreamCreditParams, StreamDataParams, StreamExitParams, StreamIdParams, StreamResizeParams,
    StreamWriteParams, DEFAULT_MAX_CONTENT_LENGTH,
};
use serde::Serialize;
use serde_json::{json, Value};

const INPUT_POLL: Duration = Duration::from_millis(50);
const MAX_INPUT_PACKET: usize =
    kindlebridge_schema::shell_protocol::USB_ALIGNED_SHELL_PACKET_PAYLOAD;

#[derive(Debug)]
pub enum Error {
    Arguments(String),
    Connection(String),
    Message(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arguments(message) | Self::Connection(message) | Self::Message(message) => {
                formatter.write_str(message)
            }
        }
    }
}

pub fn run<F>(args: &ShellArgs, json_output: bool, mut connect: F) -> Result<CommandOutput, Error>
where
    F: FnMut() -> Result<Stream, String>,
{
    if json_output {
        return Err(Error::Arguments(
            "streaming shell does not support --json; use --ndjson or exec".to_owned(),
        ));
    }
    let escape = parse_escape(&args.escape)?;
    let features = device_features(connect().map_err(Error::Connection)?, &args.serial)?;
    if !features
        .features
        .iter()
        .any(|feature| feature == SHELL_V2_FEATURE)
    {
        return Err(Error::Message(
            "incompatible device daemon: shell.v2 is required; install the matching KindleBridge package"
                .to_owned(),
        ));
    }
    run_shell_v2(connect().map_err(Error::Connection)?, args, escape)
}

fn device_features(stream: Stream, serial: &str) -> Result<DeviceFeatures, Error> {
    let (reader, writer) = stream.split();
    let mut client = RpcClient::new(BufReader::new(reader), BufWriter::new(writer));
    let value = client
        .call(methods::DEVICE_FEATURES, Some(json!({ "serial": serial })))
        .map_err(|error| Error::Message(error.to_string()))?;
    serde_json::from_value(value)
        .map_err(|_| Error::Message("server returned invalid device features".to_owned()))
}

fn run_shell_v2(
    stream: Stream,
    args: &ShellArgs,
    escape: Option<u8>,
) -> Result<CommandOutput, Error> {
    let stdin_is_terminal = io::stdin().is_terminal();
    let pty = !args.no_tty && (stdin_is_terminal || args.tty >= 2);
    let mode = if pty { ShellMode::Pty } else { ShellMode::Raw };
    let terminal_size = pty.then(current_terminal_size).transpose()?;
    let argv = match &args.command {
        Some(command) => vec!["/bin/sh".to_owned(), "-lc".to_owned(), command.clone()],
        None => vec!["/bin/sh".to_owned(), "-l".to_owned()],
    };
    let open = ShellOpen {
        mode,
        argv,
        terminal_size,
        cwd: "/tmp/root".to_owned(),
        term: "linux".to_owned(),
    };
    let (reader, writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(Mutex::new(BufWriter::new(writer)));
    send_message(
        &writer,
        &RpcRequest::call(
            RequestId::Number(1),
            methods::SHELL_OPEN,
            Some(
                serde_json::to_value(ShellOpenParams {
                    serial: args.serial.clone(),
                    open,
                })
                .map_err(|error| Error::Message(error.to_string()))?,
            ),
        ),
    )?;
    let open_result = read_open_response(&mut reader)?;

    let stopped = Arc::new(AtomicBool::new(false));
    let input_credit = Arc::new(InputCredit::new(open_result.send_credit));
    let _terminal = RawTerminalGuard::new(pty && stdin_is_terminal)?;
    start_input(InputTask {
        writer: Arc::clone(&writer),
        stream_id: open_result.stream_id.clone(),
        mode,
        stdin_is_terminal,
        no_stdin: args.no_stdin,
        escape,
        stopped: Arc::clone(&stopped),
        input_credit: Arc::clone(&input_credit),
    })?;

    let mut exit_status = None;
    let closed_by_escape = loop {
        let value = read_json_frame(&mut reader, DEFAULT_MAX_CONTENT_LENGTH)
            .map_err(|error| Error::Message(error.to_string()))?
            .ok_or_else(|| Error::Message("local server closed the shell stream".to_owned()))?;
        if args.ndjson {
            println!(
                "{}",
                serde_json::to_string(&value).map_err(|error| Error::Message(error.to_string()))?
            );
            io::stdout()
                .flush()
                .map_err(|error| Error::Message(error.to_string()))?;
        }
        let notification: RpcRequest = serde_json::from_value(value)
            .map_err(|_| Error::Message("invalid stream notification".to_owned()))?;
        let params = notification.params.unwrap_or(Value::Null);
        match notification.method.as_str() {
            methods::STREAM_DATA => {
                let params: StreamDataParams = serde_json::from_value(params)
                    .map_err(|_| Error::Message("invalid stream data event".to_owned()))?;
                if params.stream_id != open_result.stream_id {
                    continue;
                }
                let data = BASE64
                    .decode(params.data)
                    .map_err(|_| Error::Message("invalid base64 stream data".to_owned()))?;
                if !args.ndjson {
                    match params.channel {
                        StreamChannel::Stdout => io::stdout()
                            .write_all(&data)
                            .and_then(|()| io::stdout().flush()),
                        StreamChannel::Stderr => io::stderr()
                            .write_all(&data)
                            .and_then(|()| io::stderr().flush()),
                    }
                    .map_err(|error| Error::Message(error.to_string()))?;
                }
            }
            methods::STREAM_EXIT => {
                let params: StreamExitParams = serde_json::from_value(params)
                    .map_err(|_| Error::Message("invalid stream exit event".to_owned()))?;
                if params.stream_id == open_result.stream_id {
                    exit_status = Some(if params.signal == 0 {
                        params.exit_code
                    } else {
                        i32::try_from(128_u32.saturating_add(params.signal)).unwrap_or(255)
                    });
                }
            }
            methods::STREAM_CLOSED => {
                let params: StreamClosedParams = serde_json::from_value(params)
                    .map_err(|_| Error::Message("invalid stream closed event".to_owned()))?;
                if params.stream_id == open_result.stream_id {
                    break params.reason.as_deref() == Some("closed by client");
                }
            }
            methods::STREAM_CREDIT => {
                let params: StreamCreditParams = serde_json::from_value(params)
                    .map_err(|_| Error::Message("invalid stream credit event".to_owned()))?;
                if params.stream_id == open_result.stream_id {
                    input_credit.restore(params.bytes);
                }
            }
            _ => {}
        }
    };
    stopped.store(true, Ordering::Release);
    input_credit.stop();
    if exit_status.is_none() && !closed_by_escape {
        return Err(Error::Message(
            "shell connection closed before an exit status was received".to_owned(),
        ));
    }
    Ok(CommandOutput {
        output: String::new(),
        exit_code: exit_status.unwrap_or(0),
    })
}

fn read_open_response<R: BufRead>(reader: &mut R) -> Result<ShellOpenResult, Error> {
    let value = read_json_frame(reader, DEFAULT_MAX_CONTENT_LENGTH)
        .map_err(|error| Error::Message(error.to_string()))?
        .ok_or_else(|| Error::Message("local server closed during shell open".to_owned()))?;
    let response: RpcResponse = serde_json::from_value(value)
        .map_err(|_| Error::Message("invalid shell open response".to_owned()))?;
    let value = response
        .into_result()
        .map_err(|error| Error::Message(error.to_string()))?;
    serde_json::from_value(value)
        .map_err(|_| Error::Message("invalid shell open result".to_owned()))
}

struct InputTask {
    writer: Arc<Mutex<BufWriter<SendHalf>>>,
    stream_id: String,
    mode: ShellMode,
    stdin_is_terminal: bool,
    no_stdin: bool,
    escape: Option<u8>,
    stopped: Arc<AtomicBool>,
    input_credit: Arc<InputCredit>,
}

fn start_input(task: InputTask) -> Result<(), Error> {
    thread::Builder::new()
        .name("kindlebridge-shell-input".to_owned())
        .spawn(move || {
            if task.no_stdin {
                let _ = send_notification(
                    &task.writer,
                    methods::STREAM_CLOSE_INPUT,
                    &StreamIdParams {
                        stream_id: task.stream_id,
                    },
                );
                return;
            }
            if task.mode == ShellMode::Pty && task.stdin_is_terminal {
                run_terminal_input(
                    &task.writer,
                    &task.stream_id,
                    task.escape,
                    &task.stopped,
                    &task.input_credit,
                );
            } else {
                run_stream_input(
                    &task.writer,
                    &task.stream_id,
                    &task.stopped,
                    &task.input_credit,
                );
            }
        })
        .map(|_| ())
        .map_err(|error| Error::Message(format!("could not start shell input worker: {error}")))
}

fn run_terminal_input(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    stream_id: &str,
    escape: Option<u8>,
    stopped: &AtomicBool,
    input_credit: &InputCredit,
) {
    let mut filter = EscapeFilter::new(escape);
    while !stopped.load(Ordering::Acquire) {
        match event::poll(INPUT_POLL) {
            Ok(false) => continue,
            Ok(true) => {}
            Err(_) => break,
        }
        let Ok(event) = event::read() else { break };
        match event {
            Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
                let bytes = encode_key(key);
                let filtered = filter.push(&bytes);
                if !filtered.data.is_empty()
                    && send_input(writer, stream_id, &filtered.data, input_credit).is_err()
                {
                    break;
                }
                if filtered.close {
                    let _ = send_notification(
                        writer,
                        methods::STREAM_CLOSE,
                        &StreamIdParams {
                            stream_id: stream_id.to_owned(),
                        },
                    );
                    break;
                }
            }
            Event::Paste(data) => {
                let filtered = filter.push(data.as_bytes());
                if !filtered.data.is_empty() {
                    let _ = send_input(writer, stream_id, &filtered.data, input_credit);
                }
                if filtered.close {
                    let _ = send_notification(
                        writer,
                        methods::STREAM_CLOSE,
                        &StreamIdParams {
                            stream_id: stream_id.to_owned(),
                        },
                    );
                    break;
                }
            }
            Event::Resize(columns, rows) => {
                let _ = send_notification(
                    writer,
                    methods::STREAM_RESIZE,
                    &StreamResizeParams {
                        stream_id: stream_id.to_owned(),
                        size: TerminalSize {
                            rows: rows.max(1),
                            columns: columns.max(1),
                            pixel_width: 0,
                            pixel_height: 0,
                        },
                    },
                );
            }
            _ => {}
        }
    }
}

fn run_stream_input(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    stream_id: &str,
    stopped: &AtomicBool,
    input_credit: &InputCredit,
) {
    let mut stdin = io::stdin();
    let mut buffer = [0_u8; MAX_INPUT_PACKET];
    while !stopped.load(Ordering::Acquire) {
        match stdin.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                if send_input(writer, stream_id, &buffer[..count], input_credit).is_err() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
    let _ = send_notification(
        writer,
        methods::STREAM_CLOSE_INPUT,
        &StreamIdParams {
            stream_id: stream_id.to_owned(),
        },
    );
}

fn send_input(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    stream_id: &str,
    data: &[u8],
    input_credit: &InputCredit,
) -> Result<(), Error> {
    for chunk in data.chunks(MAX_INPUT_PACKET) {
        if !input_credit.take(u32::try_from(chunk.len()).unwrap_or(u32::MAX)) {
            return Err(Error::Message("shell input stream was closed".to_owned()));
        }
        if let Err(error) = send_notification(
            writer,
            methods::STREAM_WRITE,
            &StreamWriteParams {
                stream_id: stream_id.to_owned(),
                data: BASE64.encode(chunk),
            },
        ) {
            input_credit.restore(u32::try_from(chunk.len()).unwrap_or(0));
            return Err(error);
        }
    }
    Ok(())
}

struct InputCredit {
    maximum: u32,
    state: Mutex<InputCreditState>,
    available: Condvar,
}

struct InputCreditState {
    bytes: u32,
    stopped: bool,
}

impl InputCredit {
    fn new(initial: u32) -> Self {
        Self {
            maximum: initial,
            state: Mutex::new(InputCreditState {
                bytes: initial,
                stopped: false,
            }),
            available: Condvar::new(),
        }
    }

    fn take(&self, bytes: u32) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        while state.bytes < bytes && !state.stopped {
            let Ok(next) = self.available.wait(state) else {
                return false;
            };
            state = next;
        }
        if state.stopped {
            return false;
        }
        state.bytes -= bytes;
        true
    }

    fn restore(&self, bytes: u32) {
        if let Ok(mut state) = self.state.lock() {
            state.bytes = state.bytes.saturating_add(bytes).min(self.maximum);
            self.available.notify_all();
        }
    }

    fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.stopped = true;
            self.available.notify_all();
        }
    }
}

fn send_notification<T: Serialize>(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    method: &str,
    params: &T,
) -> Result<(), Error> {
    let params = serde_json::to_value(params).map_err(|error| Error::Message(error.to_string()))?;
    send_message(writer, &RpcRequest::notification(method, Some(params)))
}

fn send_message<T: Serialize>(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    value: &T,
) -> Result<(), Error> {
    let mut writer = writer
        .lock()
        .map_err(|_| Error::Message("local RPC writer is unavailable".to_owned()))?;
    write_json_frame(&mut *writer, value).map_err(|error| Error::Message(error.to_string()))
}

fn current_terminal_size() -> Result<TerminalSize, Error> {
    let (columns, rows) = terminal::size()
        .map_err(|error| Error::Message(format!("could not read terminal size: {error}")))?;
    Ok(TerminalSize {
        rows: rows.max(1),
        columns: columns.max(1),
        pixel_width: 0,
        pixel_height: 0,
    })
}

struct RawTerminalGuard {
    enabled: bool,
}

impl RawTerminalGuard {
    fn new(enabled: bool) -> Result<Self, Error> {
        if enabled {
            terminal::enable_raw_mode().map_err(|error| {
                Error::Message(format!("could not enable terminal raw mode: {error}"))
            })?;
            if let Err(error) = execute!(io::stdout(), EnableBracketedPaste) {
                let _ = terminal::disable_raw_mode();
                return Err(Error::Message(format!(
                    "could not enable terminal paste handling: {error}"
                )));
            }
        }
        Ok(Self { enabled })
    }
}

impl Drop for RawTerminalGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = execute!(io::stdout(), DisableBracketedPaste);
            let _ = terminal::disable_raw_mode();
        }
    }
}

fn encode_key(key: KeyEvent) -> Vec<u8> {
    let mut bytes = Vec::new();
    if key.modifiers.contains(KeyModifiers::ALT) {
        bytes.push(0x1b);
    }
    match key.code {
        KeyCode::Char(character) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let value = character as u32;
            if value <= 0x7f {
                let byte = value as u8;
                bytes.push(if byte == b'?' { 0x7f } else { byte & 0x1f });
            }
        }
        KeyCode::Char(character) => {
            let mut encoded = [0_u8; 4];
            bytes.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
        }
        KeyCode::Enter => bytes.push(b'\r'),
        KeyCode::Tab => bytes.push(b'\t'),
        KeyCode::BackTab => bytes.extend_from_slice(b"\x1b[Z"),
        KeyCode::Backspace => bytes.push(0x7f),
        KeyCode::Esc => bytes.push(0x1b),
        KeyCode::Up => bytes.extend_from_slice(b"\x1b[A"),
        KeyCode::Down => bytes.extend_from_slice(b"\x1b[B"),
        KeyCode::Right => bytes.extend_from_slice(b"\x1b[C"),
        KeyCode::Left => bytes.extend_from_slice(b"\x1b[D"),
        KeyCode::Home => bytes.extend_from_slice(b"\x1b[H"),
        KeyCode::End => bytes.extend_from_slice(b"\x1b[F"),
        KeyCode::Delete => bytes.extend_from_slice(b"\x1b[3~"),
        KeyCode::Insert => bytes.extend_from_slice(b"\x1b[2~"),
        KeyCode::PageUp => bytes.extend_from_slice(b"\x1b[5~"),
        KeyCode::PageDown => bytes.extend_from_slice(b"\x1b[6~"),
        KeyCode::F(number) => bytes.extend_from_slice(function_key(number)),
        _ => {}
    }
    bytes
}

fn function_key(number: u8) -> &'static [u8] {
    match number {
        1 => b"\x1bOP",
        2 => b"\x1bOQ",
        3 => b"\x1bOR",
        4 => b"\x1bOS",
        5 => b"\x1b[15~",
        6 => b"\x1b[17~",
        7 => b"\x1b[18~",
        8 => b"\x1b[19~",
        9 => b"\x1b[20~",
        10 => b"\x1b[21~",
        11 => b"\x1b[23~",
        12 => b"\x1b[24~",
        _ => b"",
    }
}

struct EscapeFilter {
    escape: Option<u8>,
    at_line_start: bool,
    pending: bool,
}

struct FilteredInput {
    data: Vec<u8>,
    close: bool,
}

impl EscapeFilter {
    const fn new(escape: Option<u8>) -> Self {
        Self {
            escape,
            at_line_start: true,
            pending: false,
        }
    }

    fn push(&mut self, input: &[u8]) -> FilteredInput {
        let mut data = Vec::with_capacity(input.len() + 1);
        let mut close = false;
        for &byte in input {
            let Some(escape) = self.escape else {
                data.push(byte);
                self.at_line_start = matches!(byte, b'\r' | b'\n');
                continue;
            };
            if self.pending {
                self.pending = false;
                if byte == b'.' {
                    close = true;
                    break;
                }
                data.push(escape);
                if byte == escape {
                    self.at_line_start = false;
                    continue;
                }
                data.push(byte);
                self.at_line_start = matches!(byte, b'\r' | b'\n');
            } else if self.at_line_start && byte == escape {
                self.pending = true;
            } else {
                data.push(byte);
                self.at_line_start = matches!(byte, b'\r' | b'\n');
            }
        }
        FilteredInput { data, close }
    }
}

fn parse_escape(value: &str) -> Result<Option<u8>, Error> {
    if value.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    let bytes = value.as_bytes();
    if bytes.len() == 1 && bytes[0].is_ascii() {
        Ok(Some(bytes[0]))
    } else {
        Err(Error::Arguments(
            "-e expects one ASCII character or 'none'".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_filter_closes_only_for_line_leading_tilde_dot() {
        let mut filter = EscapeFilter::new(Some(b'~'));
        assert_eq!(filter.push(b"echo ~.\r").data, b"echo ~.\r");
        let first = filter.push(b"~");
        assert!(first.data.is_empty());
        assert!(!first.close);
        assert!(filter.push(b".").close);
    }

    #[test]
    fn doubled_escape_sends_one_literal_escape() {
        let mut filter = EscapeFilter::new(Some(b'~'));
        let filtered = filter.push(b"~~hello");
        assert_eq!(filtered.data, b"~hello");
        assert!(!filtered.close);
    }

    #[test]
    fn disabled_escape_forwards_line_leading_tilde_dot() {
        let mut filter = EscapeFilter::new(None);
        let filtered = filter.push(b"~.\r");
        assert_eq!(filtered.data, b"~.\r");
        assert!(!filtered.close);
    }

    #[test]
    fn escape_parser_accepts_one_ascii_byte_or_none() {
        assert_eq!(parse_escape("none").unwrap(), None);
        assert_eq!(parse_escape("NONE").unwrap(), None);
        assert_eq!(parse_escape("^").unwrap(), Some(b'^'));
        assert!(matches!(parse_escape("~~"), Err(Error::Arguments(_))));
        assert!(matches!(parse_escape("λ"), Err(Error::Arguments(_))));
    }

    #[test]
    fn key_encoder_preserves_unicode_and_control_keys() {
        assert_eq!(
            encode_key(KeyEvent::new(KeyCode::Char('你'), KeyModifiers::NONE)),
            "你".as_bytes()
        );
        assert_eq!(
            encode_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            b"\x03"
        );
        assert_eq!(
            encode_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE)),
            b"\x1b[A"
        );
    }

    #[test]
    fn input_credit_blocks_until_the_server_consumes_bytes() {
        let credit = Arc::new(InputCredit::new(4));
        assert!(credit.take(4));
        let waiting = Arc::clone(&credit);
        let worker = thread::spawn(move || waiting.take(1));
        thread::sleep(Duration::from_millis(20));
        assert!(!worker.is_finished());
        credit.restore(1);
        assert!(worker.join().unwrap());
    }

    #[test]
    fn stopping_input_credit_unblocks_a_waiting_reader() {
        let credit = Arc::new(InputCredit::new(0));
        let waiting = Arc::clone(&credit);
        let worker = thread::spawn(move || waiting.take(1));
        thread::sleep(Duration::from_millis(20));
        credit.stop();
        assert!(!worker.join().unwrap());
    }
}
