use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use kindlebridge_schema::device_protocol::{ServiceAccept, ServiceOpen, SHELL_STREAM_WINDOW};
use kindlebridge_transport::actor::{Connection, FrameSink, FrameSource, RestartedSession};
use kindlebridge_transport::TrafficClass;
use kindlebridge_wire::{
    Command, DecodeLimits, EndpointRole, Frame, FrameContext, Header, SessionConfig, SessionState,
    FLAG_END_STREAM,
};

struct ChannelSource(Receiver<Frame>);

impl FrameSource for ChannelSource {
    fn read_frame(&mut self) -> Result<Frame, String> {
        self.0.recv().map_err(|_| "source disconnected".to_owned())
    }
}

struct ChannelSink(Sender<Frame>);

impl FrameSink for ChannelSink {
    fn write_frame(&mut self, frame: &Frame) -> Result<(), String> {
        self.0
            .send(frame.clone())
            .map_err(|_| "sink disconnected".to_owned())
    }
}

struct FailOnceSink {
    output: Sender<Frame>,
    fail_next: Arc<AtomicBool>,
}

struct DataGateSink {
    output: Sender<Frame>,
    data_started: Sender<()>,
    data_gate: Receiver<()>,
}

impl FrameSink for DataGateSink {
    fn write_frame(&mut self, frame: &Frame) -> Result<(), String> {
        if frame.header.command == Command::Data {
            self.data_started
                .send(())
                .map_err(|_| "data observer disconnected".to_owned())?;
            self.data_gate
                .recv()
                .map_err(|_| "data write gate disconnected".to_owned())?;
        }
        self.output
            .send(frame.clone())
            .map_err(|_| "sink disconnected".to_owned())
    }
}

impl FrameSink for FailOnceSink {
    fn write_frame(&mut self, frame: &Frame) -> Result<(), String> {
        if self.fail_next.swap(false, Ordering::AcqRel) {
            return Err("injected cancelled USB write".to_owned());
        }
        self.output
            .send(frame.clone())
            .map_err(|_| "sink disconnected".to_owned())
    }
}

#[test]
fn ping_waits_for_the_matching_control_pong() {
    let (device_tx, device_rx) = mpsc::channel();
    let (host_tx, host_rx) = mpsc::channel();
    let (connection, _incoming) = Connection::start(
        online_host_state(),
        ChannelSource(device_rx),
        ChannelSink(host_tx),
    );

    let ping = {
        let connection = connection.clone();
        thread::spawn(move || connection.ping())
    };
    let request = host_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(request.header.command, Command::Ping);
    assert_eq!(request.header.stream_id, 0);
    assert_eq!(request.payload.len(), 16);

    device_tx
        .send(
            Frame::new(
                Header::new(Command::Pong, 0, 1),
                b"not-the-request-token".to_vec(),
            )
            .unwrap(),
        )
        .unwrap();
    assert!(!ping.is_finished());
    device_tx
        .send(Frame::new(Header::new(Command::Pong, 0, 2), request.payload).unwrap())
        .unwrap();
    ping.join().unwrap().unwrap();
}

#[test]
fn missing_pong_times_out_without_disconnecting_other_work() {
    let (_device_tx, device_rx) = mpsc::channel();
    let (host_tx, host_rx) = mpsc::channel();
    let (connection, _incoming) = Connection::start(
        online_host_state(),
        ChannelSource(device_rx),
        ChannelSink(host_tx),
    );

    assert_eq!(
        connection.ping_timeout(Duration::from_millis(20)),
        Err(kindlebridge_transport::actor::ConnectionError::PingTimedOut)
    );
    assert_eq!(
        host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Ping
    );
    assert!(connection.is_online());
}

#[test]
fn received_credit_is_returned_only_after_the_worker_consumes_data() {
    let (device_tx, device_rx) = mpsc::channel();
    let (host_tx, host_rx) = mpsc::channel();
    let (connection, _incoming) = Connection::start(
        online_host_state(),
        ChannelSource(device_rx),
        ChannelSink(host_tx),
    );

    let opener = {
        let connection = connection.clone();
        thread::spawn(move || {
            connection.open("shell.v2", SHELL_STREAM_WINDOW, TrafficClass::Interactive)
        })
    };
    let open = host_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(open.header.command, Command::Open);
    let stream_id = open.header.stream_id;
    device_tx
        .send(json_frame(
            Command::Accept,
            stream_id,
            0,
            &ServiceAccept {
                initial_stream_window: SHELL_STREAM_WINDOW,
            },
        ))
        .unwrap();

    let stream = opener.join().unwrap().unwrap();
    let initial_credit = host_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(initial_credit.header.command, Command::Credit);
    assert_eq!(initial_credit.header.stream_id, stream_id);

    device_tx
        .send(
            Frame::new(
                Header::new(Command::Data, stream_id, 1),
                b"queued output".to_vec(),
            )
            .unwrap(),
        )
        .unwrap();
    assert!(host_rx.recv_timeout(Duration::from_millis(50)).is_err());

    let received = stream.recv().unwrap();
    assert_eq!(received.payload, b"queued output");
    let returned: Vec<_> = (0..2)
        .map(|_| host_rx.recv_timeout(Duration::from_secs(1)).unwrap())
        .collect();
    assert!(returned
        .iter()
        .all(|frame| frame.header.command == Command::Credit));
    assert!(returned.iter().any(|frame| frame.header.stream_id == 0));
    assert!(returned
        .iter()
        .any(|frame| frame.header.stream_id == stream_id));
}

#[test]
fn consuming_a_terminal_frame_retires_only_that_actor_stream() {
    let (device_tx, device_rx) = mpsc::channel();
    let (host_tx, host_rx) = mpsc::channel();
    let (connection, _incoming) = Connection::start(
        online_host_state(),
        ChannelSource(device_rx),
        ChannelSink(host_tx),
    );

    let opening = {
        let connection = connection.clone();
        thread::spawn(move || {
            connection.open("shell.v2", SHELL_STREAM_WINDOW, TrafficClass::Interactive)
        })
    };
    let open = host_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let stream_id = open.header.stream_id;
    device_tx
        .send(json_frame(
            Command::Accept,
            stream_id,
            0,
            &ServiceAccept {
                initial_stream_window: SHELL_STREAM_WINDOW,
            },
        ))
        .unwrap();
    let stream = opening.join().unwrap().unwrap();
    assert_eq!(
        host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Credit
    );

    device_tx
        .send(Frame::new(Header::new(Command::Close, stream_id, 1), Vec::new()).unwrap())
        .unwrap();
    assert_eq!(stream.recv().unwrap().header.command, Command::Close);

    let (result_tx, result_rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = result_tx.send(stream.recv());
    });
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("terminal actor stream was retained"),
        Err(kindlebridge_transport::actor::ConnectionError::Disconnected)
    );

    let next_open = {
        let connection = connection.clone();
        thread::spawn(move || connection.open("rpc.v1", SHELL_STREAM_WINDOW, TrafficClass::Bulk))
    };
    let open = host_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    device_tx
        .send(json_frame(
            Command::Accept,
            open.header.stream_id,
            0,
            &ServiceAccept {
                initial_stream_window: SHELL_STREAM_WINDOW,
            },
        ))
        .unwrap();
    assert!(next_open.join().unwrap().is_ok());
    assert!(connection.is_online());
}

#[test]
fn data_from_an_idle_worker_is_submitted_before_close() {
    let (host_to_device_tx, host_to_device_rx) = mpsc::channel();
    let (device_to_host_tx, device_to_host_rx) = mpsc::channel();
    let (_connection, incoming) = Connection::start(
        online_device_state(),
        ChannelSource(host_to_device_rx),
        ChannelSink(device_to_host_tx),
    );

    host_to_device_tx
        .send(json_frame(
            Command::Open,
            1,
            0,
            &ServiceOpen {
                service: "rpc.v1".to_owned(),
            },
        ))
        .unwrap();
    let stream = incoming
        .recv()
        .unwrap()
        .accept(SHELL_STREAM_WINDOW, TrafficClass::Bulk)
        .unwrap();
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Accept
    );

    let mut credit = Header::new(Command::Credit, 1, 1);
    credit.credit_delta = SHELL_STREAM_WINDOW;
    host_to_device_tx
        .send(Frame::new(credit, Vec::new()).unwrap())
        .unwrap();
    thread::sleep(Duration::from_millis(20));

    stream.send_data(b"reply".to_vec(), true).unwrap();
    stream.close().unwrap();
    let data = device_to_host_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    let close = device_to_host_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(data.header.command, Command::Data);
    assert_eq!(data.header.sequence, 1);
    assert_eq!(close.header.command, Command::Close);
    assert_eq!(close.header.sequence, 2);
}

#[test]
fn late_shell_input_after_close_does_not_stop_an_unrelated_stream() {
    let (host_to_device_tx, host_to_device_rx) = mpsc::channel();
    let (device_to_host_tx, device_to_host_rx) = mpsc::channel();
    let (connection, incoming) = Connection::start(
        online_device_state(),
        ChannelSource(host_to_device_rx),
        ChannelSink(device_to_host_tx),
    );

    host_to_device_tx
        .send(json_frame(
            Command::Open,
            1,
            0,
            &ServiceOpen {
                service: "shell.v2".to_owned(),
            },
        ))
        .unwrap();
    let shell = incoming
        .recv()
        .unwrap()
        .accept(SHELL_STREAM_WINDOW, TrafficClass::Interactive)
        .unwrap();
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Accept
    );
    let mut shell_credit = Header::new(Command::Credit, 1, 1);
    shell_credit.credit_delta = SHELL_STREAM_WINDOW;
    host_to_device_tx
        .send(Frame::new(shell_credit, Vec::new()).unwrap())
        .unwrap();

    host_to_device_tx
        .send(json_frame(
            Command::Open,
            3,
            0,
            &ServiceOpen {
                service: "sync.v1".to_owned(),
            },
        ))
        .unwrap();
    let sync = incoming
        .recv()
        .unwrap()
        .accept(SHELL_STREAM_WINDOW, TrafficClass::Bulk)
        .unwrap();
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Accept
    );

    shell.send_data(b"exit".to_vec(), true).unwrap();
    shell.close().unwrap();
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Data
    );
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Close
    );

    // close_input was already queued by the host before it observed the
    // responder's CLOSE, so it can legitimately arrive after the terminal
    // transition in the opposite direction.
    host_to_device_tx
        .send(Frame::new(Header::new(Command::Data, 1, 2), b"close-input".to_vec()).unwrap())
        .unwrap();
    let waiting = thread::spawn(move || sync.recv());
    host_to_device_tx
        .send(Frame::new(Header::new(Command::Data, 3, 1), b"bulk".to_vec()).unwrap())
        .unwrap();

    assert_eq!(
        waiting
            .join()
            .unwrap()
            .expect("unrelated stream was stopped by late shell input")
            .payload,
        b"bulk"
    );
    assert!(connection.is_online());
}

#[test]
fn worker_can_reply_after_consuming_an_end_stream_request() {
    let (host_to_device_tx, host_to_device_rx) = mpsc::channel();
    let (device_to_host_tx, device_to_host_rx) = mpsc::channel();
    let (_connection, incoming) = Connection::start(
        online_device_state(),
        ChannelSource(host_to_device_rx),
        ChannelSink(device_to_host_tx),
    );
    let (worker_result_tx, worker_result_rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = (|| {
            let stream = incoming
                .recv()?
                .accept(SHELL_STREAM_WINDOW, TrafficClass::Bulk)?;
            let request = stream.recv()?;
            assert_eq!(request.payload, b"request");
            assert_ne!(request.header.flags & FLAG_END_STREAM, 0);
            stream.send_data(b"reply".to_vec(), true)?;
            stream.close()
        })();
        let _ = worker_result_tx.send(result);
    });

    host_to_device_tx
        .send(json_frame(
            Command::Open,
            1,
            0,
            &ServiceOpen {
                service: "rpc.v1".to_owned(),
            },
        ))
        .unwrap();
    let accept = device_to_host_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(accept.header.command, Command::Accept);

    let mut credit = Header::new(Command::Credit, 1, 1);
    credit.credit_delta = SHELL_STREAM_WINDOW;
    host_to_device_tx
        .send(Frame::new(credit, Vec::new()).unwrap())
        .unwrap();
    let mut request = Frame::new(Header::new(Command::Data, 1, 2), b"request".to_vec()).unwrap();
    request.header.flags = FLAG_END_STREAM;
    host_to_device_tx.send(request).unwrap();

    let connection_credit = device_to_host_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(connection_credit.header.command, Command::Credit);
    assert_eq!(connection_credit.header.stream_id, 0);
    let response = device_to_host_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(response.header.command, Command::Data);
    assert_eq!(response.payload, b"reply");
    assert_ne!(response.header.flags & FLAG_END_STREAM, 0);
    assert_eq!(
        worker_result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        Ok(())
    );
}

#[test]
fn send_data_completes_only_after_the_sink_commits_the_frame() {
    let (host_to_device_tx, host_to_device_rx) = mpsc::channel();
    let (device_to_host_tx, device_to_host_rx) = mpsc::channel();
    let (data_started_tx, data_started_rx) = mpsc::channel();
    let (data_gate_tx, data_gate_rx) = mpsc::channel();
    let (_connection, incoming) = Connection::start(
        online_device_state(),
        ChannelSource(host_to_device_rx),
        DataGateSink {
            output: device_to_host_tx,
            data_started: data_started_tx,
            data_gate: data_gate_rx,
        },
    );

    host_to_device_tx
        .send(json_frame(
            Command::Open,
            1,
            0,
            &ServiceOpen {
                service: "rpc.v1".to_owned(),
            },
        ))
        .unwrap();
    let stream = incoming
        .recv()
        .unwrap()
        .accept(SHELL_STREAM_WINDOW, TrafficClass::Bulk)
        .unwrap();
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Accept
    );

    let mut credit = Header::new(Command::Credit, 1, 1);
    credit.credit_delta = SHELL_STREAM_WINDOW;
    host_to_device_tx
        .send(Frame::new(credit, Vec::new()).unwrap())
        .unwrap();
    let (result_tx, result_rx) = mpsc::channel();
    let sending = stream.clone();
    thread::spawn(move || {
        let _ = result_tx.send(sending.send_data(b"reply".to_vec(), true));
    });

    data_started_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(result_rx.recv_timeout(Duration::from_millis(50)).is_err());
    data_gate_tx.send(()).unwrap();
    assert_eq!(
        result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ok(())
    );
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Data
    );

    stream.close().unwrap();
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Close
    );
}

#[test]
fn fresh_hello_restarts_the_session_without_reopening_the_transport() {
    let (host_to_device_tx, host_to_device_rx) = mpsc::channel();
    let (device_to_host_tx, device_to_host_rx) = mpsc::channel();
    let (connection, incoming) = Connection::start_restartable(
        online_device_state(),
        ChannelSource(host_to_device_rx),
        ChannelSink(device_to_host_tx),
        |hello| {
            assert_eq!(hello.header.command, Command::Hello);
            assert_eq!(hello.header.stream_id, 0);
            Ok(RestartedSession {
                state: online_device_state(),
                hello_response: Frame::new(
                    Header::new(Command::Hello, 0, 0),
                    b"replacement session".to_vec(),
                )
                .unwrap(),
            })
        },
    );

    host_to_device_tx
        .send(json_frame(
            Command::Open,
            1,
            0,
            &ServiceOpen {
                service: "shell.v2".to_owned(),
            },
        ))
        .unwrap();
    let first = incoming.recv().unwrap();
    let first = first
        .accept(SHELL_STREAM_WINDOW, TrafficClass::Interactive)
        .unwrap();
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Accept
    );

    let stale = first.clone();
    let waiting = thread::spawn(move || first.recv());
    host_to_device_tx
        .send(Frame::new(Header::new(Command::Hello, 0, 0), b"new host".to_vec()).unwrap())
        .unwrap();
    assert_eq!(
        waiting.join().unwrap(),
        Err(kindlebridge_transport::actor::ConnectionError::Disconnected)
    );
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Hello
    );

    // Stream ids and sequences restart from the beginning, while the same
    // Connection and IncomingStreams objects continue owning the endpoints.
    host_to_device_tx
        .send(json_frame(
            Command::Open,
            1,
            0,
            &ServiceOpen {
                service: "shell.v2".to_owned(),
            },
        ))
        .unwrap();
    let second = incoming.recv().unwrap();
    assert_eq!(second.service, "shell.v2");
    second
        .accept(SHELL_STREAM_WINDOW, TrafficClass::Interactive)
        .unwrap();
    assert_eq!(
        stale.send_data(b"old process output".to_vec(), false),
        Err(kindlebridge_transport::actor::ConnectionError::Disconnected)
    );
    assert!(connection.is_online());
}

#[test]
fn failed_usb_write_waits_for_a_fresh_hello_and_then_recovers() {
    let (host_to_device_tx, host_to_device_rx) = mpsc::channel();
    let (device_to_host_tx, device_to_host_rx) = mpsc::channel();
    let fail_next = Arc::new(AtomicBool::new(false));
    let (connection, incoming) = Connection::start_restartable(
        online_device_state(),
        ChannelSource(host_to_device_rx),
        FailOnceSink {
            output: device_to_host_tx,
            fail_next: Arc::clone(&fail_next),
        },
        |hello| {
            assert_eq!(hello.header.command, Command::Hello);
            assert_eq!(hello.header.stream_id, 0);
            Ok(RestartedSession {
                state: online_device_state(),
                hello_response: Frame::new(
                    Header::new(Command::Hello, 0, 0),
                    b"replacement session".to_vec(),
                )
                .unwrap(),
            })
        },
    );

    host_to_device_tx
        .send(json_frame(
            Command::Open,
            1,
            0,
            &ServiceOpen {
                service: "shell.v2".to_owned(),
            },
        ))
        .unwrap();
    let incoming_stream = incoming.recv().unwrap();
    fail_next.store(true, Ordering::Release);
    let stale = incoming_stream
        .accept(SHELL_STREAM_WINDOW, TrafficClass::Interactive)
        .unwrap();

    // The ACCEPT write is cancelled when the host releases WinUSB. The actor
    // must abandon the old session without terminating its endpoint owners.
    assert_eq!(
        stale.recv(),
        Err(kindlebridge_transport::actor::ConnectionError::Disconnected)
    );
    assert!(matches!(
        connection.open("shell.v2", SHELL_STREAM_WINDOW, TrafficClass::Interactive),
        Err(kindlebridge_transport::actor::ConnectionError::Disconnected)
    ));
    assert!(device_to_host_rx
        .recv_timeout(Duration::from_millis(50))
        .is_err());

    // Frames still queued from the abandoned session are ignored until the
    // replacement host starts a new KBP handshake.
    host_to_device_tx
        .send(Frame::new(Header::new(Command::Data, 1, 1), b"stale input".to_vec()).unwrap())
        .unwrap();
    host_to_device_tx
        .send(Frame::new(Header::new(Command::Hello, 0, 0), b"new host".to_vec()).unwrap())
        .unwrap();
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Hello
    );

    host_to_device_tx
        .send(json_frame(
            Command::Open,
            1,
            0,
            &ServiceOpen {
                service: "shell.v2".to_owned(),
            },
        ))
        .unwrap();
    let replacement = incoming.recv().unwrap();
    assert_eq!(replacement.service, "shell.v2");
    replacement
        .accept(SHELL_STREAM_WINDOW, TrafficClass::Interactive)
        .unwrap();
    assert_eq!(
        device_to_host_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .header
            .command,
        Command::Accept
    );
    assert!(connection.is_online());
}

fn online_host_state() -> SessionState {
    let limits = DecodeLimits::new(16 * 1024 * 1024, 16 * 1024 * 1024);
    let mut state = SessionState::new(SessionConfig::new(EndpointRole::Host, limits));
    let local = Header::new(Command::Hello, 0, 0);
    state
        .process_outbound(&local, FrameContext::hello(16 * 1024 * 1024))
        .unwrap();
    let peer = Header::new(Command::Hello, 0, 0);
    state
        .process_inbound(&peer, FrameContext::hello(16 * 1024 * 1024))
        .unwrap();
    state
}

fn online_device_state() -> SessionState {
    let limits = DecodeLimits::new(16 * 1024 * 1024, 16 * 1024 * 1024);
    let mut state = SessionState::new(SessionConfig::new(EndpointRole::Device, limits));
    let peer = Header::new(Command::Hello, 0, 0);
    state
        .process_inbound(&peer, FrameContext::hello(16 * 1024 * 1024))
        .unwrap();
    let local = Header::new(Command::Hello, 0, 0);
    state
        .process_outbound(&local, FrameContext::hello(16 * 1024 * 1024))
        .unwrap();
    state
}

fn json_frame(
    command: Command,
    stream_id: u32,
    sequence: u32,
    value: &impl serde::Serialize,
) -> Frame {
    Frame::new(
        Header::new(command, stream_id, sequence),
        serde_json::to_vec(value).unwrap(),
    )
    .unwrap()
}
