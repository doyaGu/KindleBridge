//! One `shell.v2` stream from opening metadata through process cleanup.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use kindlebridge_schema::device_protocol::{ShellOpen, SHELL_STREAM_WINDOW};
use kindlebridge_schema::shell_protocol::{
    PacketSource, ShellPacket, ShellPacketError, ShellStreamState,
};
use kindlebridge_transport::actor::{
    ConnectionError, IncomingStream as ActorIncomingStream, Stream as ActorStream,
};
use kindlebridge_transport::TrafficClass;
use kindlebridge_wire::{Command, FLAG_END_STREAM};
use thiserror::Error;

use crate::shell::{ShellEvent, ShellWorker, ShellWorkerError};

const MAX_CONCURRENT_SHELLS: usize = 4;

#[derive(Clone, Debug)]
pub(crate) struct ShellStreams {
    active: Arc<AtomicUsize>,
}

impl ShellStreams {
    pub(crate) fn new() -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn serve(&self, incoming: ActorIncomingStream) -> Result<(), ShellStreamError> {
        let Some(_slot) = ShellSlot::reserve(Arc::clone(&self.active)) else {
            incoming.reject("at most four Shell Streams may be active")?;
            return Ok(());
        };
        serve_stream(incoming)
    }
}

struct ShellSlot(Arc<AtomicUsize>);

impl ShellSlot {
    fn reserve(active: Arc<AtomicUsize>) -> Option<Self> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                (value < MAX_CONCURRENT_SHELLS).then_some(value + 1)
            })
            .ok()
            .map(|_| Self(active))
    }
}

impl Drop for ShellSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn serve_stream(incoming: ActorIncomingStream) -> Result<(), ShellStreamError> {
    let stream = incoming.accept(SHELL_STREAM_WINDOW, TrafficClass::Interactive)?;
    let open_frame = stream.recv()?;
    if open_frame.header.command != Command::Data {
        return Err(ShellStreamError::UnexpectedFrame(
            "expected DATA on actor service stream",
        ));
    }
    if open_frame.header.flags & FLAG_END_STREAM != 0 {
        stream.reset("shell open metadata must not end the stream")?;
        return Ok(());
    }
    let open: ShellOpen = match serde_json::from_slice(&open_frame.payload) {
        Ok(open) => open,
        Err(source) => {
            stream.reset(
                ShellStreamError::InvalidOpen {
                    label: "shell open",
                    source,
                }
                .to_string(),
            )?;
            return Ok(());
        }
    };
    let mut worker = match ShellWorker::spawn(open.clone()) {
        Ok(worker) => worker,
        Err(error) => {
            stream.reset(error.to_string())?;
            return Ok(());
        }
    };
    let input = worker.input();
    let input_stream = stream.clone();
    let stream_stopped = Arc::new(AtomicBool::new(false));
    let input_stopped = Arc::clone(&stream_stopped);
    let input_thread = thread::spawn(move || {
        let mut protocol = ShellStreamState::new(open.mode);
        loop {
            let frame = match input_stream.recv() {
                Ok(frame) => frame,
                Err(_) => {
                    stop_input(&input_stopped, &input);
                    break;
                }
            };
            match frame.header.command {
                Command::Data => {
                    let packet = match ShellPacket::decode(&frame.payload, PacketSource::Host) {
                        Ok(packet) => packet,
                        Err(error) => {
                            input_stopped.store(true, Ordering::Release);
                            let _ = input_stream.reset(error.to_string());
                            let _ = input.hangup();
                            break;
                        }
                    };
                    if let Err(error) = protocol.accept(&packet) {
                        input_stopped.store(true, Ordering::Release);
                        let _ = input_stream.reset(error.to_string());
                        let _ = input.hangup();
                        break;
                    }
                    let result = match packet {
                        ShellPacket::Stdin(bytes) => input.write_stdin(bytes),
                        ShellPacket::CloseStdin => input.close_input(),
                        ShellPacket::Resize(size) => input.resize(size),
                        _ => unreachable!("host packet direction was validated"),
                    };
                    if result.is_err() {
                        input_stopped.store(true, Ordering::Release);
                        let _ = input_stream.reset("shell process input stopped");
                        let _ = input.hangup();
                        break;
                    }
                }
                Command::Reset | Command::Close => {
                    stop_input(&input_stopped, &input);
                    break;
                }
                _ => {
                    input_stopped.store(true, Ordering::Release);
                    let _ = input_stream.reset("unexpected shell stream frame");
                    let _ = input.hangup();
                    break;
                }
            }
        }
    });

    let mut terminal_error = None;
    loop {
        match worker.recv_timeout(Duration::from_secs(1)) {
            Ok(ShellEvent::Stdout(bytes)) => {
                if stream_stopped.load(Ordering::Acquire) {
                    continue;
                }
                if let Err(error) = send_packet(&stream, ShellPacket::Stdout(bytes), false) {
                    terminal_error = Some(error);
                    stop_output(&stream_stopped, &worker, &stream);
                }
            }
            Ok(ShellEvent::Stderr(bytes)) => {
                if stream_stopped.load(Ordering::Acquire) {
                    continue;
                }
                if let Err(error) = send_packet(&stream, ShellPacket::Stderr(bytes), false) {
                    terminal_error = Some(error);
                    stop_output(&stream_stopped, &worker, &stream);
                }
            }
            Ok(ShellEvent::Exit(status)) => {
                if !stream_stopped.load(Ordering::Acquire) {
                    let result = send_packet(&stream, ShellPacket::Exit(status), true);
                    let _ = stream.cancel_receive();
                    let result =
                        result.and_then(|()| stream.close().map_err(ShellStreamError::from));
                    if let Err(error) = result {
                        terminal_error = Some(error);
                        stop_output(&stream_stopped, &worker, &stream);
                    }
                }
                break;
            }
            Err(ShellWorkerError::ReceiveTimeout) => {}
            Err(error) => {
                if !stream_stopped.load(Ordering::Acquire) {
                    let _ = stream.cancel_receive();
                    if let Err(reset_error) = stream.reset(error.to_string()) {
                        terminal_error = Some(reset_error.into());
                    }
                }
                break;
            }
        }
    }
    let input_result = input_thread
        .join()
        .map_err(|_| ShellStreamError::InputPanicked);
    let worker_result = worker.join().map_err(ShellStreamError::from);
    input_result?;
    worker_result?;
    terminal_error.map_or(Ok(()), Err)
}

fn send_packet(
    stream: &ActorStream,
    packet: ShellPacket,
    end_stream: bool,
) -> Result<(), ShellStreamError> {
    stream.send_data(packet.encode()?, end_stream)?;
    Ok(())
}

fn stop_input(stopped: &AtomicBool, input: &crate::shell::ShellInput) {
    if !stopped.swap(true, Ordering::AcqRel) {
        let _ = input.hangup();
    }
}

fn stop_output(stopped: &AtomicBool, worker: &ShellWorker, stream: &ActorStream) {
    if !stopped.swap(true, Ordering::AcqRel) {
        let _ = worker.hangup();
    }
    let _ = stream.cancel_receive();
}

#[derive(Debug, Error)]
pub enum ShellStreamError {
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    #[error(transparent)]
    Worker(#[from] ShellWorkerError),
    #[error(transparent)]
    Packet(#[from] ShellPacketError),
    #[error("invalid {label} payload: {source}")]
    InvalidOpen {
        label: &'static str,
        source: serde_json::Error,
    },
    #[error("shell input worker panicked")]
    InputPanicked,
    #[error("{0}")]
    UnexpectedFrame(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_allows_four_streams_across_clones_and_releases_slots() {
        let streams = ShellStreams::new();
        let mut slots = Vec::new();
        for _ in 0..MAX_CONCURRENT_SHELLS {
            slots.push(ShellSlot::reserve(Arc::clone(&streams.active)).unwrap());
        }
        let clone = streams.clone();
        assert!(ShellSlot::reserve(Arc::clone(&clone.active)).is_none());
        slots.pop();
        assert!(ShellSlot::reserve(Arc::clone(&clone.active)).is_some());
    }
}
