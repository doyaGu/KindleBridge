use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use kindlebridge_schema::device_protocol::{ServiceAccept, SHELL_STREAM_WINDOW};
use kindlebridge_transport::actor::{Connection, FrameSink, FrameSource};
use kindlebridge_transport::TrafficClass;
use kindlebridge_wire::{
    Command, DecodeLimits, EndpointRole, Frame, FrameContext, Header, SessionConfig, SessionState,
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

    let mut stream = opener.join().unwrap().unwrap();
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
