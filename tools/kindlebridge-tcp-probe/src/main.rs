//! Internal client for the one-shot, non-production KT6 transport probe.

use std::env;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::thread;
use std::time::Instant;

use kindlebridge_transport_tcp::{TcpFrameStream, TransportConfig};
use kindlebridge_wire::{Command, DecodeLimits, Frame, Header};
use serde::Serialize;
use serde_json::Value;

const MAX_PROBE_PAYLOAD: u32 = 1024 * 1024;
const DEFAULT_PAYLOAD_SIZE: u32 = 256 * 1024;
const DEFAULT_ROUNDS: u32 = 64;
const MAX_ROUNDS: u32 = 100_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Arguments {
    address: SocketAddr,
    payload_size: u32,
    rounds: u32,
}

#[derive(Debug, Serialize)]
struct Report {
    address: SocketAddr,
    hello: Value,
    rounds: u32,
    payload_size: u32,
    bytes_each_direction: u64,
    elapsed_millis: u64,
    upload_mib_per_second: f64,
    download_mib_per_second: f64,
    aggregate_mib_per_second: f64,
}

fn main() {
    if let Err(error) = run(env::args().skip(1).collect()) {
        eprintln!("kindlebridge-tcp-probe: {error}");
        std::process::exit(1);
    }
}

fn run(raw_arguments: Vec<String>) -> Result<(), Box<dyn Error>> {
    let arguments = parse_arguments(&raw_arguments)?;
    let limits = DecodeLimits::new(MAX_PROBE_PAYLOAD, MAX_PROBE_PAYLOAD);
    let mut stream = TcpFrameStream::connect(arguments.address, TransportConfig::new(limits))?;

    stream.write_frame(&Frame::new(
        Header::new(Command::Hello, 0, 0),
        b"kindlebridge-tcp-probe/0.1".to_vec(),
    )?)?;
    let hello_frame = stream.read_frame()?;
    if hello_frame.header.command != Command::Hello
        || hello_frame.header.stream_id != 0
        || hello_frame.header.sequence != 0
    {
        return Err(ProbeClientError::UnexpectedHello.into());
    }
    let hello: Value = serde_json::from_slice(&hello_frame.payload)?;

    let payload_len = usize::try_from(arguments.payload_size)?;
    let payload = (0..payload_len)
        .map(|index| u8::try_from(index % 251).expect("modulo result fits in u8"))
        .collect::<Vec<_>>();
    let started = Instant::now();
    let mut writer = stream.try_clone()?;
    let rounds = arguments.rounds;
    let writer_thread = thread::spawn(move || -> Result<(), ProbeClientError> {
        let mut ping = Frame::new(Header::new(Command::Ping, 0, 1), payload)
            .map_err(|_| ProbeClientError::FrameConstruction)?;
        for round in 0..rounds {
            ping.header.sequence = round
                .checked_add(1)
                .ok_or(ProbeClientError::SequenceExhausted)?;
            writer
                .write_frame(&ping)
                .map_err(|_| ProbeClientError::TransportWrite)?;
        }
        Ok(())
    });

    for round in 0..arguments.rounds {
        let sequence = round
            .checked_add(1)
            .ok_or(ProbeClientError::SequenceExhausted)?;
        let pong = stream.read_frame()?;
        if pong.header.command != Command::Pong
            || pong.header.stream_id != 0
            || pong.header.sequence != sequence
            || pong.payload.len() != payload_len
            || pong
                .payload
                .iter()
                .enumerate()
                .any(|(index, byte)| *byte != u8::try_from(index % 251).expect("modulo fits"))
        {
            return Err(ProbeClientError::MismatchedPong(sequence).into());
        }
    }
    writer_thread
        .join()
        .map_err(|_| ProbeClientError::WriterPanicked)??;

    let elapsed = started.elapsed();
    let go_away_sequence = arguments
        .rounds
        .checked_add(1)
        .ok_or(ProbeClientError::SequenceExhausted)?;
    stream.write_frame(&Frame::new(
        Header::new(Command::GoAway, 0, go_away_sequence),
        Vec::new(),
    )?)?;

    let bytes_each_direction = u64::from(arguments.payload_size)
        .checked_mul(u64::from(arguments.rounds))
        .ok_or(ProbeClientError::ByteCountOverflow)?;
    let elapsed_seconds = elapsed.as_secs_f64().max(f64::EPSILON);
    let mib = bytes_each_direction as f64 / (1024.0 * 1024.0);
    let report = Report {
        address: arguments.address,
        hello,
        rounds: arguments.rounds,
        payload_size: arguments.payload_size,
        bytes_each_direction,
        elapsed_millis: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        upload_mib_per_second: mib / elapsed_seconds,
        download_mib_per_second: mib / elapsed_seconds,
        aggregate_mib_per_second: (2.0 * mib) / elapsed_seconds,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn parse_arguments(arguments: &[String]) -> Result<Arguments, ProbeClientError> {
    let Some(address) = arguments.first() else {
        return Err(ProbeClientError::Usage);
    };
    let address = address
        .parse::<SocketAddr>()
        .map_err(|_| ProbeClientError::InvalidAddress)?;
    let payload_size = parse_u32_option(arguments, "--payload-size", DEFAULT_PAYLOAD_SIZE)?;
    let rounds = parse_u32_option(arguments, "--rounds", DEFAULT_ROUNDS)?;
    if payload_size == 0 || payload_size > MAX_PROBE_PAYLOAD {
        return Err(ProbeClientError::InvalidPayloadSize(payload_size));
    }
    if rounds == 0 || rounds > MAX_ROUNDS {
        return Err(ProbeClientError::InvalidRounds(rounds));
    }
    Ok(Arguments {
        address,
        payload_size,
        rounds,
    })
}

fn parse_u32_option(
    arguments: &[String],
    option: &str,
    default: u32,
) -> Result<u32, ProbeClientError> {
    let Some(index) = arguments.iter().position(|argument| argument == option) else {
        return Ok(default);
    };
    arguments
        .get(index + 1)
        .ok_or_else(|| ProbeClientError::MissingOptionValue(option.to_owned()))?
        .parse::<u32>()
        .map_err(|_| ProbeClientError::InvalidOptionValue(option.to_owned()))
}

#[derive(Debug, Eq, PartialEq)]
enum ProbeClientError {
    Usage,
    InvalidAddress,
    MissingOptionValue(String),
    InvalidOptionValue(String),
    InvalidPayloadSize(u32),
    InvalidRounds(u32),
    UnexpectedHello,
    MismatchedPong(u32),
    SequenceExhausted,
    ByteCountOverflow,
    FrameConstruction,
    TransportWrite,
    WriterPanicked,
}

impl fmt::Display for ProbeClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => formatter.write_str(
                "usage: kindlebridge-tcp-probe IP:PORT [--payload-size BYTES] [--rounds N]",
            ),
            Self::InvalidAddress => formatter.write_str("address must be an IP address and port"),
            Self::MissingOptionValue(option) => write!(formatter, "{option} requires a value"),
            Self::InvalidOptionValue(option) => write!(formatter, "{option} must be an integer"),
            Self::InvalidPayloadSize(size) => {
                write!(
                    formatter,
                    "payload size {size} is outside 1..={MAX_PROBE_PAYLOAD}"
                )
            }
            Self::InvalidRounds(rounds) => {
                write!(
                    formatter,
                    "round count {rounds} is outside 1..={MAX_ROUNDS}"
                )
            }
            Self::UnexpectedHello => formatter.write_str("device returned an invalid probe HELLO"),
            Self::MismatchedPong(sequence) => {
                write!(
                    formatter,
                    "device returned a mismatched PONG for sequence {sequence}"
                )
            }
            Self::SequenceExhausted => formatter.write_str("probe sequence space exhausted"),
            Self::ByteCountOverflow => formatter.write_str("probe byte count overflowed"),
            Self::FrameConstruction => formatter.write_str("failed to construct probe frame"),
            Self::TransportWrite => formatter.write_str("probe writer transport failed"),
            Self::WriterPanicked => formatter.write_str("probe writer thread panicked"),
        }
    }
}

impl Error for ProbeClientError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bounded_probe_arguments() {
        let arguments = vec![
            "127.0.0.1:7788".to_owned(),
            "--payload-size".to_owned(),
            "4096".to_owned(),
            "--rounds".to_owned(),
            "10".to_owned(),
        ];
        assert_eq!(
            parse_arguments(&arguments).unwrap(),
            Arguments {
                address: "127.0.0.1:7788".parse().unwrap(),
                payload_size: 4096,
                rounds: 10,
            }
        );
    }

    #[test]
    fn rejects_unbounded_work() {
        let arguments = vec![
            "127.0.0.1:7788".to_owned(),
            "--rounds".to_owned(),
            "100001".to_owned(),
        ];
        assert_eq!(
            parse_arguments(&arguments),
            Err(ProbeClientError::InvalidRounds(100_001))
        );
    }
}
