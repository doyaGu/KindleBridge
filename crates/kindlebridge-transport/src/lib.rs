//! Bounded scheduling and backend selection for `KindleBridge` transports.

use std::array;
use std::collections::{BTreeMap, VecDeque};

use kindlebridge_wire::{Command, Frame};
use thiserror::Error;

pub mod actor;

const CLASS_COUNT: usize = 5;
const DEFAULT_QUANTA: [usize; CLASS_COUNT] = [65_536, 32_768, 16_384, 8_192, 4_096];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(usize)]
pub enum TrafficClass {
    Control = 0,
    Interactive = 1,
    Events = 2,
    Forwarding = 3,
    Bulk = 4,
}

impl TrafficClass {
    #[must_use]
    pub const fn for_command(command: Command) -> Self {
        match command {
            Command::Hello
            | Command::PairingFinish
            | Command::Accept
            | Command::Reject
            | Command::Credit
            | Command::Close
            | Command::Reset
            | Command::Ping
            | Command::Pong
            | Command::GoAway
            | Command::Error => Self::Control,
            Command::Open | Command::Data => Self::Bulk,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledFrame {
    pub class: TrafficClass,
    pub frame: Frame,
}

impl ScheduledFrame {
    #[must_use]
    pub fn wire_len(&self) -> usize {
        kindlebridge_wire::HEADER_LEN.saturating_add(self.frame.payload.len())
    }
}

#[derive(Debug)]
pub struct FrameScheduler {
    queues: [VecDeque<ScheduledFrame>; CLASS_COUNT],
    deficits: [usize; CLASS_COUNT],
    quanta: [usize; CLASS_COUNT],
    cursor: usize,
    queued_bytes: usize,
    max_queued_bytes: usize,
}

impl FrameScheduler {
    #[must_use]
    pub fn new(max_queued_bytes: usize) -> Self {
        Self {
            queues: array::from_fn(|_| VecDeque::new()),
            deficits: [0; CLASS_COUNT],
            quanta: DEFAULT_QUANTA,
            cursor: 0,
            queued_bytes: 0,
            max_queued_bytes,
        }
    }

    pub fn enqueue(&mut self, item: ScheduledFrame) -> Result<(), SchedulerError> {
        let wire_len = item.wire_len();
        let next = self
            .queued_bytes
            .checked_add(wire_len)
            .ok_or(SchedulerError::QueueFull)?;
        if next > self.max_queued_bytes {
            return Err(SchedulerError::QueueFull);
        }
        self.queues[item.class as usize].push_back(item);
        self.queued_bytes = next;
        Ok(())
    }

    pub fn dequeue(&mut self) -> Option<ScheduledFrame> {
        if self.queued_bytes == 0 {
            return None;
        }

        // A frame can be larger than its class quantum, so permit enough visits for
        // every class to accumulate one maximum-sized frame worth of deficit.
        let attempts = CLASS_COUNT.saturating_mul(512);
        for _ in 0..attempts {
            let index = self.cursor;
            let Some(front) = self.queues[index].front() else {
                self.deficits[index] = 0;
                self.advance();
                continue;
            };

            let cost = front.wire_len();
            if self.deficits[index] < cost {
                self.deficits[index] = self.deficits[index].saturating_add(self.quanta[index]);
            }
            if self.deficits[index] < cost {
                self.advance();
                continue;
            }

            self.deficits[index] -= cost;
            let frame = self.queues[index]
                .pop_front()
                .expect("front was checked above");
            self.queued_bytes -= cost;
            // Advance after every frame so a stream of tiny control frames cannot
            // monopolize the transport. Deficit is retained for the next round.
            self.advance();
            return Some(frame);
        }
        None
    }

    #[must_use]
    pub const fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queued_bytes == 0
    }

    fn advance(&mut self) {
        self.cursor = (self.cursor + 1) % CLASS_COUNT;
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SchedulerError {
    #[error("bounded transport queue is full")]
    QueueFull,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportKind {
    UsbBulk,
    Tcp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportCandidate {
    pub id: String,
    pub kind: TransportKind,
    pub online: bool,
    pub round_trip_micros: u32,
    pub estimated_bytes_per_second: u64,
}

#[derive(Debug, Default)]
pub struct TransportRegistry {
    by_device: BTreeMap<String, Vec<TransportCandidate>>,
}

impl TransportRegistry {
    pub fn register(&mut self, serial: impl Into<String>, candidate: TransportCandidate) {
        let candidates = self.by_device.entry(serial.into()).or_default();
        if let Some(existing) = candidates
            .iter_mut()
            .find(|existing| existing.id == candidate.id)
        {
            *existing = candidate;
        } else {
            candidates.push(candidate);
        }
    }

    #[must_use]
    pub fn select(&self, serial: &str, class: TrafficClass) -> Option<&TransportCandidate> {
        let candidates = self.by_device.get(serial)?;
        candidates
            .iter()
            .filter(|candidate| candidate.online)
            .min_by_key(|candidate| match class {
                TrafficClass::Bulk => (
                    candidate.kind != TransportKind::UsbBulk,
                    u64::MAX.saturating_sub(candidate.estimated_bytes_per_second),
                ),
                _ => (false, u64::from(candidate.round_trip_micros)),
            })
    }

    pub fn mark_offline(&mut self, serial: &str, transport_id: &str) {
        if let Some(candidate) = self
            .by_device
            .get_mut(serial)
            .and_then(|items| items.iter_mut().find(|item| item.id == transport_id))
        {
            candidate.online = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use kindlebridge_wire::Header;

    use super::*;

    fn scheduled(class: TrafficClass, payload_len: usize, sequence: u32) -> ScheduledFrame {
        ScheduledFrame {
            class,
            frame: Frame {
                header: Header::new(Command::Data, 1, sequence),
                payload: vec![0; payload_len],
            },
        }
    }

    #[test]
    fn queue_is_strictly_bounded() {
        let mut scheduler = FrameScheduler::new(100);
        scheduler
            .enqueue(scheduled(TrafficClass::Bulk, 50, 0))
            .unwrap();
        assert_eq!(
            scheduler.enqueue(scheduled(TrafficClass::Bulk, 50, 1)),
            Err(SchedulerError::QueueFull)
        );
    }

    #[test]
    fn control_is_served_before_bulk_but_bulk_is_not_starved() {
        let mut scheduler = FrameScheduler::new(2 * 1024 * 1024);
        scheduler
            .enqueue(scheduled(TrafficClass::Bulk, 4_000, 0))
            .unwrap();
        for sequence in 0..100 {
            scheduler
                .enqueue(scheduled(TrafficClass::Control, 100, sequence))
                .unwrap();
        }
        assert_eq!(scheduler.dequeue().unwrap().class, TrafficClass::Control);
        let mut saw_bulk = false;
        for _ in 0..32 {
            if scheduler.dequeue().unwrap().class == TrafficClass::Bulk {
                saw_bulk = true;
                break;
            }
        }
        assert!(saw_bulk, "weighted scheduling starved the bulk queue");
    }

    #[test]
    fn bulk_prefers_usb_and_interactive_prefers_latency() {
        let mut registry = TransportRegistry::default();
        registry.register(
            "KT6",
            TransportCandidate {
                id: "wifi".to_owned(),
                kind: TransportKind::Tcp,
                online: true,
                round_trip_micros: 500,
                estimated_bytes_per_second: 80_000_000,
            },
        );
        registry.register(
            "KT6",
            TransportCandidate {
                id: "usb".to_owned(),
                kind: TransportKind::UsbBulk,
                online: true,
                round_trip_micros: 2_000,
                estimated_bytes_per_second: 40_000_000,
            },
        );
        assert_eq!(
            registry.select("KT6", TrafficClass::Bulk).unwrap().id,
            "usb"
        );
        assert_eq!(
            registry
                .select("KT6", TrafficClass::Interactive)
                .unwrap()
                .id,
            "wifi"
        );
    }
}
