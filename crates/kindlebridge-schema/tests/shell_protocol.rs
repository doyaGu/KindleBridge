use kindlebridge_schema::device_protocol::{
    ShellMode, ShellOpen, TerminalSize, SHELL_STREAM_WINDOW, SHELL_V2_SERVICE,
};
use kindlebridge_schema::shell_protocol::{
    PacketSource, ShellExit, ShellPacket, ShellPacketError, ShellStreamError, ShellStreamState,
    MAX_SHELL_PACKET_PAYLOAD, USB_ALIGNED_SHELL_PACKET_PAYLOAD,
};

#[test]
fn preferred_shell_payload_fills_one_safe_functionfs_request() {
    assert_eq!(
        kindlebridge_wire::HEADER_LEN + 5 + USB_ALIGNED_SHELL_PACKET_PAYLOAD,
        16 * 1024
    );
}

#[test]
fn shell_packets_have_stable_adb_compatible_golden_bytes() {
    assert_eq!(
        ShellPacket::Stdin(vec![b'a', 0, b'b']).encode().unwrap(),
        vec![0, 3, 0, 0, 0, b'a', 0, b'b']
    );
    assert_eq!(
        ShellPacket::Exit(ShellExit {
            exit_code: -1,
            signal: 9,
        })
        .encode()
        .unwrap(),
        vec![3, 8, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 9, 0, 0, 0]
    );
    assert_eq!(
        ShellPacket::Resize(TerminalSize {
            rows: 24,
            columns: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .encode()
        .unwrap(),
        vec![5, 8, 0, 0, 0, 24, 0, 80, 0, 0, 0, 0, 0]
    );
}

#[test]
fn shell_packet_round_trips_binary_data_by_direction() {
    let stdin = ShellPacket::Stdin(vec![0, 1, 0xff, b'\n']);
    let stdout = ShellPacket::Stdout(vec![0, 2, 0xfe, b'\n']);

    assert_eq!(
        ShellPacket::decode(&stdin.encode().unwrap(), PacketSource::Host).unwrap(),
        stdin
    );
    assert_eq!(
        ShellPacket::decode(&stdout.encode().unwrap(), PacketSource::Device).unwrap(),
        stdout
    );
}

#[test]
fn shell_packet_rejects_wrong_direction_unknown_kind_and_bad_length() {
    let stdout = ShellPacket::Stdout(b"no".to_vec()).encode().unwrap();
    assert_eq!(
        ShellPacket::decode(&stdout, PacketSource::Host).unwrap_err(),
        ShellPacketError::InvalidDirection { kind: 1 }
    );
    assert_eq!(
        ShellPacket::decode(&[99, 0, 0, 0, 0], PacketSource::Device).unwrap_err(),
        ShellPacketError::UnknownKind(99)
    );
    assert_eq!(
        ShellPacket::decode(&[0, 2, 0, 0, 0, b'x'], PacketSource::Host).unwrap_err(),
        ShellPacketError::LengthMismatch {
            declared: 2,
            actual: 1,
        }
    );
}

#[test]
fn shell_packet_enforces_payload_and_control_shapes() {
    assert_eq!(
        ShellPacket::Stdin(vec![0; MAX_SHELL_PACKET_PAYLOAD + 1])
            .encode()
            .unwrap_err(),
        ShellPacketError::PayloadTooLarge {
            length: MAX_SHELL_PACKET_PAYLOAD + 1,
            maximum: MAX_SHELL_PACKET_PAYLOAD,
        }
    );
    assert_eq!(
        ShellPacket::decode(&[4, 1, 0, 0, 0, 0], PacketSource::Host).unwrap_err(),
        ShellPacketError::InvalidPayloadLength {
            kind: 4,
            expected: 0,
            actual: 1,
        }
    );
    assert_eq!(
        ShellPacket::Resize(TerminalSize {
            rows: 0,
            columns: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .encode()
        .unwrap_err(),
        ShellPacketError::InvalidTerminalSize
    );
}

#[test]
fn shell_open_defaults_are_predictable_and_keep_kbp_v2() {
    assert_eq!(SHELL_V2_SERVICE, "shell.v2");

    let interactive = ShellOpen::interactive(TerminalSize {
        rows: 24,
        columns: 80,
        pixel_width: 0,
        pixel_height: 0,
    });
    assert_eq!(interactive.mode, ShellMode::Pty);
    assert_eq!(interactive.argv, ["/bin/sh", "-l"]);
    assert_eq!(interactive.cwd, "/tmp/root");
    assert_eq!(interactive.term, "linux");

    let command = ShellOpen::command("printf hi");
    assert_eq!(command.mode, ShellMode::Raw);
    assert_eq!(command.argv, ["/bin/sh", "-lc", "printf hi"]);
    assert!(command.terminal_size.is_none());
}

#[test]
fn shell_stream_orders_input_eof_output_and_exit() {
    assert_eq!(SHELL_STREAM_WINDOW, 256 * 1024);
    let mut stream = ShellStreamState::new(ShellMode::Raw);

    stream
        .accept(&ShellPacket::Stdin(b"hello".to_vec()))
        .unwrap();
    stream.accept(&ShellPacket::CloseStdin).unwrap();
    assert_eq!(
        stream
            .accept(&ShellPacket::Stdin(b"too late".to_vec()))
            .unwrap_err(),
        ShellStreamError::InputClosed
    );
    stream
        .accept(&ShellPacket::Stdout(b"goodbye".to_vec()))
        .unwrap();
    stream
        .accept(&ShellPacket::Exit(ShellExit {
            exit_code: 37,
            signal: 0,
        }))
        .unwrap();
    assert_eq!(
        stream
            .accept(&ShellPacket::Stderr(b"too late".to_vec()))
            .unwrap_err(),
        ShellStreamError::AfterExit
    );
}
