//! Deterministic fake device used by host API tests and local development.

use kindlebridge_schema::device_protocol::PROTOCOL_VERSION;
use kindlebridge_schema::{DeviceState, DeviceSummary};
use kindlebridge_server::{DeviceRecord, MemoryDeviceProvider};

pub const SERIAL: &str = "KT6-FAKE-0001";

#[must_use]
pub fn provider() -> MemoryDeviceProvider {
    MemoryDeviceProvider::new(vec![DeviceRecord {
        summary: DeviceSummary {
            serial: SERIAL.to_owned(),
            model: "KT6".to_owned(),
            state: DeviceState::Online,
            transport: "fake-stdio".to_owned(),
        },
        protocol_version: PROTOCOL_VERSION,
        features: vec![
            "exec.v1".to_owned(),
            "app.v1".to_owned(),
            "log.v1".to_owned(),
            "process.v1".to_owned(),
            "shell.v1".to_owned(),
            "sync.v1".to_owned(),
        ],
    }])
}
