//! One-shot, non-privileged hardware bring-up server.
//!
//! This is intentionally not the production KindleBridge listener. It accepts
//! one explicitly allowlisted peer, supports only HELLO/PING/GOAWAY, and exits.

use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use kindlebridge_transport_tcp::{
    TcpFrameListener, TcpFrameStream, TransportConfig, TransportError,
};
use kindlebridge_wire::{Command, DecodeLimits, Frame, Header, WireError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::DeviceInfo;

pub const DEFAULT_PROBE_MAX_PAYLOAD: u32 = 1024 * 1024;
pub const DEFAULT_PROBE_MAX_FRAMES: u64 = 100_000;

#[derive(Clone, Debug)]
pub struct ProbeConfig {
    pub allowed_peer: IpAddr,
    pub max_payload: u32,
    pub max_frames: u64,
    pub io_timeout: Duration,
    pub device: DeviceInfo,
}

impl ProbeConfig {
    #[must_use]
    pub fn kt6(allowed_peer: IpAddr) -> Self {
        Self {
            allowed_peer,
            max_payload: DEFAULT_PROBE_MAX_PAYLOAD,
            max_frames: DEFAULT_PROBE_MAX_FRAMES,
            io_timeout: Duration::from_secs(15),
            device: DeviceInfo::kt6("UNPROVISIONED-PROBE"),
        }
    }

    fn limits(&self) -> DecodeLimits {
        DecodeLimits::new(self.max_payload, self.max_payload)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProbeHello {
    pub mode: String,
    pub device: DeviceInfo,
    pub max_payload: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProbeReport {
    pub peer: SocketAddr,
    pub frames_received: u64,
    pub payload_bytes_received: u64,
    pub payload_bytes_sent: u64,
    pub elapsed_millis: u64,
}

pub struct ProbeServer {
    listener: TcpFrameListener,
    config: ProbeConfig,
}

impl ProbeServer {
    pub fn bind(address: SocketAddr, config: ProbeConfig) -> Result<Self, ProbeError> {
        let listener = TcpFrameListener::bind(address, transport_config(&config))?;
        Ok(Self { listener, config })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ProbeError> {
        Ok(self.listener.local_addr()?)
    }

    pub fn serve_once(self) -> Result<ProbeReport, ProbeError> {
        let (mut stream, peer) = self.listener.accept()?;
        if peer.ip() != self.config.allowed_peer {
            return Err(ProbeError::PeerNotAllowed(peer.ip()));
        }
        serve_stream(&mut stream, peer, &self.config)
    }
}

fn serve_stream(
    stream: &mut TcpFrameStream,
    peer: SocketAddr,
    config: &ProbeConfig,
) -> Result<ProbeReport, ProbeError> {
    let started = Instant::now();
    let hello = stream.read_frame()?;
    if hello.header.command != Command::Hello
        || hello.header.stream_id != 0
        || hello.header.sequence != 0
    {
        return Err(ProbeError::ExpectedHello);
    }

    let response = ProbeHello {
        mode: "one-shot-hardware-probe".to_owned(),
        device: config.device.clone(),
        max_payload: config.max_payload,
    };
    stream.write_frame(&Frame::new(
        Header::new(Command::Hello, 0, 0),
        serde_json::to_vec(&response)?,
    )?)?;
    stream.flush()?;

    let mut expected_sequence = 1_u32;
    let mut response_sequence = 1_u32;
    let mut frames_received = 1_u64;
    let mut payload_bytes_received = u64::try_from(hello.payload.len()).unwrap_or(u64::MAX);
    let mut payload_bytes_sent =
        u64::try_from(serde_json::to_vec(&response)?.len()).unwrap_or(u64::MAX);

    loop {
        if frames_received >= config.max_frames {
            return Err(ProbeError::FrameLimit(config.max_frames));
        }
        let frame = stream.read_frame()?;
        if frame.header.stream_id != 0 || frame.header.sequence != expected_sequence {
            return Err(ProbeError::UnexpectedSequence {
                expected: expected_sequence,
                actual: frame.header.sequence,
            });
        }
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(ProbeError::SequenceExhausted)?;
        frames_received += 1;
        payload_bytes_received = payload_bytes_received
            .checked_add(u64::try_from(frame.payload.len()).unwrap_or(u64::MAX))
            .ok_or(ProbeError::CounterOverflow)?;

        match frame.header.command {
            Command::Ping => {
                let payload_len = u64::try_from(frame.payload.len()).unwrap_or(u64::MAX);
                let reply = Frame::new(
                    Header::new(Command::Pong, 0, response_sequence),
                    frame.payload,
                )?;
                stream.write_frame(&reply)?;
                response_sequence = response_sequence
                    .checked_add(1)
                    .ok_or(ProbeError::SequenceExhausted)?;
                payload_bytes_sent = payload_bytes_sent
                    .checked_add(payload_len)
                    .ok_or(ProbeError::CounterOverflow)?;
            }
            Command::GoAway => break,
            command => return Err(ProbeError::UnsupportedCommand(command)),
        }
    }

    let elapsed_millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(ProbeReport {
        peer,
        frames_received,
        payload_bytes_received,
        payload_bytes_sent,
        elapsed_millis,
    })
}

fn transport_config(config: &ProbeConfig) -> TransportConfig {
    TransportConfig {
        limits: config.limits(),
        read_timeout: Some(config.io_timeout),
        write_timeout: Some(config.io_timeout),
        ..TransportConfig::new(config.limits())
    }
}

#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("probe transport failed: {0}")]
    Transport(#[from] TransportError),
    #[error("invalid KBP frame: {0}")]
    Wire(#[from] WireError),
    #[error("probe metadata serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("peer {0} is not allowlisted")]
    PeerNotAllowed(IpAddr),
    #[error("first probe frame must be HELLO sequence 0 on stream 0")]
    ExpectedHello,
    #[error("unexpected stream-0 sequence: expected {expected}, got {actual}")]
    UnexpectedSequence { expected: u32, actual: u32 },
    #[error("probe command {0:?} is not supported")]
    UnsupportedCommand(Command),
    #[error("probe frame limit {0} reached")]
    FrameLimit(u64),
    #[error("probe sequence space exhausted")]
    SequenceExhausted,
    #[error("probe byte counter overflowed")]
    CounterOverflow,
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::thread;

    use super::*;

    #[test]
    fn one_shot_probe_round_trips_payload_and_exits() {
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server =
            ProbeServer::bind(SocketAddr::new(loopback, 0), ProbeConfig::kt6(loopback)).unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || server.serve_once().unwrap());

        let limits = DecodeLimits::new(DEFAULT_PROBE_MAX_PAYLOAD, DEFAULT_PROBE_MAX_PAYLOAD);
        let mut client = TcpFrameStream::connect(address, TransportConfig::new(limits)).unwrap();
        client
            .write_frame(
                &Frame::new(Header::new(Command::Hello, 0, 0), b"host-probe".to_vec()).unwrap(),
            )
            .unwrap();
        let hello = client.read_frame().unwrap();
        assert_eq!(hello.header.command, Command::Hello);
        let decoded: ProbeHello = serde_json::from_slice(&hello.payload).unwrap();
        assert_eq!(decoded.device.model, "KT6");

        client
            .write_frame(
                &Frame::new(Header::new(Command::Ping, 0, 1), b"payload".to_vec()).unwrap(),
            )
            .unwrap();
        let pong = client.read_frame().unwrap();
        assert_eq!(pong.header.command, Command::Pong);
        assert_eq!(pong.payload, b"payload");

        client
            .write_frame(&Frame::new(Header::new(Command::GoAway, 0, 2), Vec::new()).unwrap())
            .unwrap();
        let report = worker.join().unwrap();
        assert_eq!(report.frames_received, 3);
        assert_eq!(report.payload_bytes_received, 17);
    }

    #[test]
    fn probe_rejects_commands_before_hello() {
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server =
            ProbeServer::bind(SocketAddr::new(loopback, 0), ProbeConfig::kt6(loopback)).unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || server.serve_once().unwrap_err());
        let limits = DecodeLimits::new(DEFAULT_PROBE_MAX_PAYLOAD, DEFAULT_PROBE_MAX_PAYLOAD);
        let mut client = TcpFrameStream::connect(address, TransportConfig::new(limits)).unwrap();
        client
            .write_frame(&Frame::new(Header::new(Command::Ping, 0, 0), Vec::new()).unwrap())
            .unwrap();
        assert!(matches!(worker.join().unwrap(), ProbeError::ExpectedHello));
    }
}
