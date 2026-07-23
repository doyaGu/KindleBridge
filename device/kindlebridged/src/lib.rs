//! Unprivileged KindleBridge device service catalog and admission policy.

use kindlebridge_broker::{AuthenticatedSession, Grant, SUPPORTED_BUNDLE_PROFILE};
use kindlebridge_schema::device_protocol::{
    APP_LIST_FEATURE, LOG_TAIL_FEATURE, PROCESS_LIST_FEATURE, SYNC_TREE_FEATURE,
};
use kindlebridge_wire::{PROTOCOL_MAJOR as KBP_MAJOR, PROTOCOL_MINOR as KBP_MINOR};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod app;
pub mod application;
pub mod exec;
pub mod probe;
pub mod server;
mod services;
pub mod shell;
pub mod sync;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeviceInfo {
    pub serial: String,
    pub model: String,
    pub firmware: String,
    pub target: String,
    pub bundle_profile: String,
    pub kbp_major: u16,
    pub kbp_minor: u16,
}

impl DeviceInfo {
    #[must_use]
    pub fn kt6(serial: impl Into<String>) -> Self {
        Self {
            serial: serial.into(),
            model: "KT6".to_owned(),
            firmware: "5.17.1.0.4".to_owned(),
            target: "kindlehf".to_owned(),
            bundle_profile: SUPPORTED_BUNDLE_PROFILE.to_owned(),
            kbp_major: KBP_MAJOR,
            kbp_minor: KBP_MINOR,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceStatus {
    Supported,
    Unauthorized,
    Disabled,
    Unavailable,
    Degraded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceDefinition {
    pub name: &'static str,
    pub required_grant: Grant,
    pub available_on_kindlehf: bool,
}

pub const SERVICES: &[ServiceDefinition] = &[
    ServiceDefinition {
        name: "device.v1",
        required_grant: Grant::DeviceRead,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: "rpc.v1",
        required_grant: Grant::ShellUser,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: "sync.v1",
        required_grant: Grant::FsApp,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: SYNC_TREE_FEATURE,
        required_grant: Grant::FsApp,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: "bundle.v1",
        required_grant: Grant::BundleInstall,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: APP_LIST_FEATURE,
        required_grant: Grant::DeviceRead,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: LOG_TAIL_FEATURE,
        required_grant: Grant::DeviceRead,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: "forward.v1",
        required_grant: Grant::NetworkForward,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: PROCESS_LIST_FEATURE,
        required_grant: Grant::DeviceRead,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: "debug.gdb.v1",
        required_grant: Grant::DebugNative,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: "perf.v1",
        required_grant: Grant::PerfRead,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: "diagnostics.v1",
        required_grant: Grant::DeviceRead,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: "device.admin.v1",
        required_grant: Grant::DeviceAdmin,
        available_on_kindlehf: true,
    },
    ServiceDefinition {
        name: "screenshot.v1",
        required_grant: Grant::UiCapture,
        available_on_kindlehf: true,
    },
];

#[derive(Clone, Debug)]
pub struct DaemonState {
    info: DeviceInfo,
    safe_mode: bool,
}

impl DaemonState {
    #[must_use]
    pub const fn new(info: DeviceInfo) -> Self {
        Self {
            info,
            safe_mode: false,
        }
    }

    #[must_use]
    pub const fn info(&self) -> &DeviceInfo {
        &self.info
    }

    pub const fn set_safe_mode(&mut self, safe_mode: bool) {
        self.safe_mode = safe_mode;
    }

    #[must_use]
    pub fn service_status(
        &self,
        session: &AuthenticatedSession,
        service_name: &str,
    ) -> Option<ServiceStatus> {
        let service = SERVICES
            .iter()
            .find(|service| service.name == service_name)?;
        if !service.available_on_kindlehf || self.info.target != "kindlehf" {
            return Some(ServiceStatus::Unavailable);
        }
        if self.safe_mode && !matches!(service.name, "device.v1" | "diagnostics.v1") {
            return Some(ServiceStatus::Disabled);
        }
        if !session.has(service.required_grant) {
            return Some(ServiceStatus::Unauthorized);
        }
        Some(ServiceStatus::Supported)
    }

    pub fn open_service(
        &self,
        session: &AuthenticatedSession,
        service_name: &str,
    ) -> Result<(), OpenServiceError> {
        match self.service_status(session, service_name) {
            Some(ServiceStatus::Supported | ServiceStatus::Degraded) => Ok(()),
            Some(ServiceStatus::Unauthorized) => Err(OpenServiceError::Unauthorized),
            Some(ServiceStatus::Disabled) => Err(OpenServiceError::Disabled),
            Some(ServiceStatus::Unavailable) => Err(OpenServiceError::Unavailable),
            None => Err(OpenServiceError::UnknownService(service_name.to_owned())),
        }
    }

    #[must_use]
    pub fn service_report(&self, session: &AuthenticatedSession) -> Vec<ServiceReport> {
        SERVICES
            .iter()
            .map(|service| ServiceReport {
                name: service.name,
                status: self
                    .service_status(session, service.name)
                    .expect("catalog entry must be discoverable"),
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ServiceReport {
    pub name: &'static str,
    pub status: ServiceStatus,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum OpenServiceError {
    #[error("unknown service: {0}")]
    UnknownService(String),
    #[error("host is not authorized for this service")]
    Unauthorized,
    #[error("service is disabled by the device")]
    Disabled,
    #[error("service is unavailable on this device profile")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn session(grants: impl IntoIterator<Item = Grant>) -> AuthenticatedSession {
        AuthenticatedSession {
            host_key_id: [7; 32],
            session_id: [8; 32],
            grants: grants.into_iter().collect::<BTreeSet<_>>(),
        }
    }

    #[test]
    fn reports_unauthorized_instead_of_hiding_supported_service() {
        let daemon = DaemonState::new(DeviceInfo::kt6("TEST"));
        assert_eq!(
            daemon.service_status(&session([]), "debug.gdb.v1"),
            Some(ServiceStatus::Unauthorized)
        );
    }

    #[test]
    fn safe_mode_leaves_diagnostics_available() {
        let mut daemon = DaemonState::new(DeviceInfo::kt6("TEST"));
        daemon.set_safe_mode(true);
        let session = session([Grant::DeviceRead, Grant::ShellUser]);
        assert_eq!(
            daemon.service_status(&session, "diagnostics.v1"),
            Some(ServiceStatus::Supported)
        );
        assert_eq!(
            daemon.service_status(&session, "rpc.v1"),
            Some(ServiceStatus::Disabled)
        );
    }

    #[test]
    fn unknown_services_are_rejected_explicitly() {
        let daemon = DaemonState::new(DeviceInfo::kt6("TEST"));
        assert_eq!(
            daemon.open_service(&session([Grant::DeviceRead]), "test-v1"),
            Err(OpenServiceError::UnknownService("test-v1".to_owned()))
        );
    }
}
