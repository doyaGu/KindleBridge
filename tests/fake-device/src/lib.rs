//! Deterministic fake device used by host API tests and local development.

use kindlebridge_schema::device_protocol::{
    APP_INSTALL_FEATURE, APP_LIST_FEATURE, APP_RESTART_FEATURE, APP_ROLLBACK_FEATURE,
    APP_START_FEATURE, APP_STOP_FEATURE, APP_UNINSTALL_FEATURE, EXEC_FEATURE, LOG_TAIL_FEATURE,
    PROCESS_LIST_FEATURE, PROCESS_SIGNAL_FEATURE, PROTOCOL_VERSION, RPC_SERVICE, SYNC_FEATURE,
};
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
            APP_INSTALL_FEATURE.to_owned(),
            APP_LIST_FEATURE.to_owned(),
            APP_RESTART_FEATURE.to_owned(),
            APP_ROLLBACK_FEATURE.to_owned(),
            APP_START_FEATURE.to_owned(),
            APP_STOP_FEATURE.to_owned(),
            APP_UNINSTALL_FEATURE.to_owned(),
            EXEC_FEATURE.to_owned(),
            RPC_SERVICE.to_owned(),
            LOG_TAIL_FEATURE.to_owned(),
            PROCESS_LIST_FEATURE.to_owned(),
            PROCESS_SIGNAL_FEATURE.to_owned(),
            SYNC_FEATURE.to_owned(),
        ],
    }])
}
