//! FunctionFS endpoint support for the Kindle Bridge Protocol (KBP).
//!
//! This crate only consumes an already mounted and configured FunctionFS
//! instance. It never mounts filesystems, changes configfs, or selects a UDC.

mod descriptors;
mod event;
mod probe;

pub use descriptors::{
    descriptor_bytes, string_bytes, DESCRIPTOR_LENGTH, DEVICE_INTERFACE_GUID, STRING_LENGTH,
};
pub use event::{
    wait_for_active, Event, EventError, EventKind, SetupPacket, WaitOutcome, EVENT_SIZE,
};
pub use probe::{
    run, run_probe_session, FunctionFsDevice, FunctionFsEndpoints, FunctionFsError,
    FunctionFsFrameStream, FunctionFsIo, SessionOutcome, MAX_FRAME_COUNT, MAX_FUNCTIONFS_IO,
    MAX_PAYLOAD,
};

#[cfg(test)]
mod tests;
