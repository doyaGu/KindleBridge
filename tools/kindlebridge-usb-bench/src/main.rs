//! Internal raw-bulk KBP throughput and descriptor probe.

use std::env;
use std::error::Error;
use std::fmt;
use std::thread;
use std::time::Instant;

use kindlebridge_transport_tcp::{FrameReader, FrameWriter, TransportConfig};
use kindlebridge_transport_usb::{open, BufferConfig, UsbMatch};
use kindlebridge_wire::{Command, DecodeLimits, Frame, Header};
use serde::Serialize;

const KINDLE_VENDOR_ID: u16 = 0x1949;
const KT6_PRODUCT_ID: u16 = 0x9981;
const KBP_SUBCLASS: u8 = 0x4b;
const KBP_PROTOCOL: u8 = 0x01;
const MAX_PAYLOAD: u32 = 1024 * 1024;
const DEFAULT_PAYLOAD: u32 = 256 * 1024;
const DEFAULT_ROUNDS: u32 = 128;
const MAX_ROUNDS: u32 = 99_998;
const KT6_SAFE_TRANSFER_SIZE: usize = 16 * 1024;
const KT6_SAFE_QUEUE_DEPTH: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbeMode {
    Duplex,
    HostToDevice,
    DeviceToHost,
}

impl ProbeMode {
    fn parse(value: &str) -> Result<Self, ProbeError> {
        match value {
            "duplex" => Ok(Self::Duplex),
            "host-to-device" | "h2d" => Ok(Self::HostToDevice),
            "device-to-host" | "d2h" => Ok(Self::DeviceToHost),
            value => Err(ProbeError::InvalidMode(value.to_owned())),
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

#[derive(Debug, Eq, PartialEq)]
struct Arguments {
    vendor_id: u16,
    product_id: u16,
    serial: Option<String>,
    mode: ProbeMode,
    payload_size: u32,
    rounds: u32,
}

#[derive(Debug, Serialize)]
struct Report {
    vendor_id: u16,
    product_id: u16,
    interface_number: u8,
    alternate_setting: u8,
    bulk_in_address: u8,
    bulk_out_address: u8,
    max_packet_size: usize,
    queue_depth_each_direction: usize,
    transfer_size: usize,
    mode: &'static str,
    rounds: u32,
    payload_size: u32,
    host_to_device_bytes: u64,
    device_to_host_bytes: u64,
    elapsed_millis: u64,
    host_to_device_mib_per_second: f64,
    device_to_host_mib_per_second: f64,
    aggregate_mib_per_second: f64,
}

fn main() {
    if let Err(error) = run(env::args().skip(1).collect()) {
        eprintln!("kindlebridge-usb-bench: {error}");
        std::process::exit(1);
    }
}

fn run(raw_arguments: Vec<String>) -> Result<(), Box<dyn Error>> {
    let arguments = parse_arguments(&raw_arguments)?;
    let buffers = probe_buffer_config();
    let transport = open(
        &UsbMatch {
            vendor_id: arguments.vendor_id,
            product_id: arguments.product_id,
            interface_subclass: KBP_SUBCLASS,
            interface_protocol: KBP_PROTOCOL,
            serial_number: arguments.serial.clone(),
        },
        buffers,
    )?;
    let selection = transport.selection;
    let (usb_reader, usb_writer) = transport.split();
    let config = TransportConfig::new(DecodeLimits::new(MAX_PAYLOAD, MAX_PAYLOAD));
    let mut reader = FrameReader::new(usb_reader, config)?;
    let mut writer = FrameWriter::new(usb_writer, config)?;

    let hello_payload = format!(
        "kindlebridge-usb-bench/0.2;mode={};payload={};rounds={}",
        arguments.mode.name(),
        arguments.payload_size,
        arguments.rounds
    );
    writer.write_frame(&Frame::new(
        Header::new(Command::Hello, 0, 0),
        hello_payload.into_bytes(),
    )?)?;
    writer.flush()?;
    let hello = reader.read_frame()?;
    if hello.header.command != Command::Hello
        || hello.header.stream_id != 0
        || hello.header.sequence != 0
    {
        return Err(ProbeError::UnexpectedHello.into());
    }

    let payload_len = usize::try_from(arguments.payload_size)?;
    let expected_payload = test_payload(payload_len);
    let (writer_payload, reader_payload) = match arguments.mode {
        ProbeMode::Duplex => (expected_payload.clone(), expected_payload),
        ProbeMode::HostToDevice => (expected_payload, Vec::new()),
        ProbeMode::DeviceToHost => (Vec::new(), expected_payload),
    };
    let rounds = arguments.rounds;
    let started = Instant::now();
    let writer_thread = thread::spawn(move || -> Result<_, String> {
        let mut ping = Frame::new(Header::new(Command::Ping, 0, 1), writer_payload)
            .map_err(|error| error.to_string())?;
        for round in 0..rounds {
            ping.header.sequence = round
                .checked_add(1)
                .ok_or_else(|| "probe sequence exhausted".to_owned())?;
            writer
                .write_frame(&ping)
                .map_err(|error| error.to_string())?;
        }
        writer.flush().map_err(|error| error.to_string())?;
        Ok(writer)
    });

    for round in 0..arguments.rounds {
        let sequence = round.checked_add(1).ok_or(ProbeError::SequenceExhausted)?;
        let pong = reader.read_frame()?;
        if pong.header.command != Command::Pong
            || pong.header.stream_id != 0
            || pong.header.sequence != sequence
            || pong.payload != reader_payload
        {
            return Err(ProbeError::MismatchedPong(sequence).into());
        }
    }
    let mut writer = writer_thread
        .join()
        .map_err(|_| ProbeError::WriterPanicked)?
        .map_err(ProbeError::Writer)?;
    let elapsed = started.elapsed();

    let goaway_sequence = arguments
        .rounds
        .checked_add(1)
        .ok_or(ProbeError::SequenceExhausted)?;
    writer.write_frame(&Frame::new(
        Header::new(Command::GoAway, 0, goaway_sequence),
        Vec::new(),
    )?)?;
    writer.flush()?;
    let goaway = reader.read_frame()?;
    if goaway.header.command != Command::GoAway
        || goaway.header.stream_id != 0
        || goaway.header.sequence != goaway_sequence
    {
        return Err(ProbeError::UnexpectedGoAway.into());
    }

    let payload_bytes = u64::from(arguments.payload_size)
        .checked_mul(u64::from(arguments.rounds))
        .ok_or(ProbeError::ByteCountOverflow)?;
    let (host_to_device_bytes, device_to_host_bytes) = match arguments.mode {
        ProbeMode::Duplex => (payload_bytes, payload_bytes),
        ProbeMode::HostToDevice => (payload_bytes, 0),
        ProbeMode::DeviceToHost => (0, payload_bytes),
    };
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
    let host_to_device_rate = host_to_device_bytes as f64 / (1024.0 * 1024.0) / seconds;
    let device_to_host_rate = device_to_host_bytes as f64 / (1024.0 * 1024.0) / seconds;
    let report = Report {
        vendor_id: arguments.vendor_id,
        product_id: arguments.product_id,
        interface_number: selection.interface_number,
        alternate_setting: selection.alternate_setting,
        bulk_in_address: selection.bulk_in_address,
        bulk_out_address: selection.bulk_out_address,
        max_packet_size: selection
            .bulk_in_max_packet_size
            .min(selection.bulk_out_max_packet_size),
        queue_depth_each_direction: buffers.read_queue_depth.min(buffers.write_queue_depth),
        transfer_size: buffers.transfer_size,
        mode: arguments.mode.name(),
        rounds: arguments.rounds,
        payload_size: arguments.payload_size,
        host_to_device_bytes,
        device_to_host_bytes,
        elapsed_millis: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        host_to_device_mib_per_second: host_to_device_rate,
        device_to_host_mib_per_second: device_to_host_rate,
        aggregate_mib_per_second: host_to_device_rate + device_to_host_rate,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn probe_buffer_config() -> BufferConfig {
    BufferConfig {
        transfer_size: KT6_SAFE_TRANSFER_SIZE,
        read_queue_depth: KT6_SAFE_QUEUE_DEPTH,
        write_queue_depth: KT6_SAFE_QUEUE_DEPTH,
        ..BufferConfig::default()
    }
}

fn test_payload(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| u8::try_from(index % 251).expect("modulo result fits in u8"))
        .collect()
}

fn parse_arguments(arguments: &[String]) -> Result<Arguments, ProbeError> {
    let vendor_id = parse_hex_option(arguments, "--vid", KINDLE_VENDOR_ID)?;
    let product_id = parse_hex_option(arguments, "--pid", KT6_PRODUCT_ID)?;
    let serial = option_value(arguments, "--serial").map(str::to_owned);
    let mode = ProbeMode::parse(option_value(arguments, "--mode").unwrap_or("duplex"))?;
    let payload_size = parse_u32_option(arguments, "--payload-size", DEFAULT_PAYLOAD)?;
    let rounds = parse_u32_option(arguments, "--rounds", DEFAULT_ROUNDS)?;
    if payload_size == 0 || payload_size > MAX_PAYLOAD {
        return Err(ProbeError::InvalidPayloadSize(payload_size));
    }
    if rounds == 0 || rounds > MAX_ROUNDS {
        return Err(ProbeError::InvalidRounds(rounds));
    }
    Ok(Arguments {
        vendor_id,
        product_id,
        serial,
        mode,
        payload_size,
        rounds,
    })
}

fn parse_hex_option(arguments: &[String], name: &str, default: u16) -> Result<u16, ProbeError> {
    let Some(value) = option_value(arguments, name) else {
        return Ok(default);
    };
    let value = value.strip_prefix("0x").unwrap_or(value);
    u16::from_str_radix(value, 16).map_err(|_| ProbeError::InvalidOption(name.to_owned()))
}

fn parse_u32_option(arguments: &[String], name: &str, default: u32) -> Result<u32, ProbeError> {
    option_value(arguments, name)
        .map(str::parse::<u32>)
        .transpose()
        .map_err(|_| ProbeError::InvalidOption(name.to_owned()))
        .map(|value| value.unwrap_or(default))
}

fn option_value<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    arguments
        .windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
}

#[derive(Debug, Eq, PartialEq)]
enum ProbeError {
    InvalidOption(String),
    InvalidMode(String),
    InvalidPayloadSize(u32),
    InvalidRounds(u32),
    UnexpectedHello,
    MismatchedPong(u32),
    UnexpectedGoAway,
    SequenceExhausted,
    ByteCountOverflow,
    WriterPanicked,
    Writer(String),
}

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOption(name) => write!(formatter, "invalid or missing {name} value"),
            Self::InvalidMode(value) => write!(
                formatter,
                "invalid probe mode {value:?}; expected duplex, host-to-device, or device-to-host"
            ),
            Self::InvalidPayloadSize(value) => {
                write!(
                    formatter,
                    "payload size {value} is outside 1..={MAX_PAYLOAD}"
                )
            }
            Self::InvalidRounds(value) => {
                write!(formatter, "round count {value} is outside 1..={MAX_ROUNDS}")
            }
            Self::UnexpectedHello => formatter.write_str("device returned an invalid HELLO"),
            Self::MismatchedPong(sequence) => {
                write!(
                    formatter,
                    "device returned a mismatched PONG for {sequence}"
                )
            }
            Self::UnexpectedGoAway => formatter.write_str("device returned an invalid GOAWAY"),
            Self::SequenceExhausted => formatter.write_str("probe sequence exhausted"),
            Self::ByteCountOverflow => formatter.write_str("probe byte count overflowed"),
            Self::WriterPanicked => formatter.write_str("USB writer thread panicked"),
            Self::Writer(error) => write!(formatter, "USB writer failed: {error}"),
        }
    }
}

impl Error for ProbeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_frozen_kt6_probe_profile() {
        assert_eq!(
            parse_arguments(&[]).unwrap(),
            Arguments {
                vendor_id: 0x1949,
                product_id: 0x9981,
                serial: None,
                mode: ProbeMode::Duplex,
                payload_size: 256 * 1024,
                rounds: 128,
            }
        );
        let buffers = probe_buffer_config();
        assert_eq!(buffers.transfer_size, 16 * 1024);
        assert_eq!(buffers.read_queue_depth, 4);
        assert_eq!(buffers.write_queue_depth, 4);
    }

    #[test]
    fn rejects_unbounded_probe_work() {
        assert_eq!(
            parse_arguments(&["--rounds".to_owned(), "99999".to_owned()]),
            Err(ProbeError::InvalidRounds(99_999))
        );
    }

    #[test]
    fn accepts_readable_direction_names_and_short_aliases() {
        for (value, expected) in [
            ("duplex", ProbeMode::Duplex),
            ("host-to-device", ProbeMode::HostToDevice),
            ("h2d", ProbeMode::HostToDevice),
            ("device-to-host", ProbeMode::DeviceToHost),
            ("d2h", ProbeMode::DeviceToHost),
        ] {
            let arguments = parse_arguments(&["--mode".to_owned(), value.to_owned()]).unwrap();
            assert_eq!(arguments.mode, expected);
        }
    }
}
