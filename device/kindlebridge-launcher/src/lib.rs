//! Minimal, root-confined KindleBridge A/B launcher and watchdog.
//!
//! This crate does not install or invoke an init-system adapter. An external,
//! separately reviewed adapter may invoke the binary with `--root
//! /var/local/kindlebridge`.

mod fs_safe;
mod manifest;
mod system;
mod update;
mod watchdog;

pub use manifest::{Slot, SlotManifest};
pub use system::{FilesystemDisableFlag, SystemChildRunner, SystemClock};
pub use update::{active_slot, rollback_daemon, stage_daemon, StagedUpdate};
pub use watchdog::{
    encode_heartbeat, ChildRunner, ChildStatus, Clock, DisableFlag, Launcher, SpawnRequest,
    StepOutcome,
};

use std::fmt;
use std::io;

pub const PRODUCTION_DISABLE_FLAG: &str = "/mnt/us/KINDLEBRIDGE_DISABLE";
pub const CHILD_PID_FILE: &str = "run/daemon.pid";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Io,
    InvalidRoot,
    UnsafePath,
    InvalidManifest,
    InvalidState,
    Child,
}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
    source: Option<io::Error>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn io(error: io::Error) -> Self {
        Self {
            kind: ErrorKind::Io,
            message: error.to_string(),
            source: Some(error),
        }
    }

    pub(crate) fn is_not_found(&self) -> bool {
        self.source
            .as_ref()
            .is_some_and(|source| source.kind() == io::ErrorKind::NotFound)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::io(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
