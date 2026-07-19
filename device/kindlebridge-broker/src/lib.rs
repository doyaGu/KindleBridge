//! Typed authorization boundary for privileged KindleBridge operations.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const SUPPORTED_BUNDLE_PROFILE: &str = "kindlebridge.bundle.v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Grant {
    DeviceRead,
    FsApp,
    FsUser,
    BundleInstall,
    BundlePublishDev,
    ProcessApp,
    ShellUser,
    NetworkForward,
    DebugNative,
    PerfRead,
    UiCapture,
    UiInspect,
    UiInject,
    DeviceAdmin,
    ShellRoot,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthenticatedSession {
    pub host_key_id: [u8; 32],
    pub session_id: [u8; 32],
    pub grants: BTreeSet<Grant>,
}

impl AuthenticatedSession {
    #[must_use]
    pub fn has(&self, grant: Grant) -> bool {
        self.grants.contains(&grant)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum BrokerOperation {
    OpenRootShell,
    CommitActivation {
        bundle_profile: String,
        generation_id: String,
        activation_relative_path: String,
    },
    AddDevelopmentPublisher {
        publisher_key: [u8; 32],
    },
    RemoveDevelopmentPublisher {
        publisher_key: [u8; 32],
    },
    RestartDaemon,
    RebootDevice,
}

impl BrokerOperation {
    #[must_use]
    pub const fn required_grant(&self) -> Grant {
        match self {
            Self::OpenRootShell => Grant::ShellRoot,
            Self::CommitActivation { .. } => Grant::BundleInstall,
            Self::AddDevelopmentPublisher { .. } | Self::RemoveDevelopmentPublisher { .. } => {
                Grant::BundlePublishDev
            }
            Self::RestartDaemon | Self::RebootDevice => Grant::DeviceAdmin,
        }
    }

    #[must_use]
    pub const fn mutates_device(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrokerRequest {
    pub session: AuthenticatedSession,
    pub operation: BrokerOperation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrokerPolicy {
    pub safe_mode: bool,
    pub active_bundle_profile: String,
}

impl Default for BrokerPolicy {
    fn default() -> Self {
        Self {
            safe_mode: false,
            active_bundle_profile: SUPPORTED_BUNDLE_PROFILE.to_owned(),
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum AuthorizationError {
    #[error("operation is disabled while KindleBridge is in safe mode")]
    SafeMode,
    #[error("the authenticated host does not have grant {0:?}")]
    MissingGrant(Grant),
    #[error("unsupported bundle profile: {0}")]
    UnsupportedProfile(String),
    #[error("invalid activation generation id")]
    InvalidGenerationId,
    #[error("activation path is not the canonical path for the generation")]
    InvalidActivationPath,
}

impl BrokerPolicy {
    pub fn authorize(&self, request: &BrokerRequest) -> Result<(), AuthorizationError> {
        if self.safe_mode && request.operation.mutates_device() {
            return Err(AuthorizationError::SafeMode);
        }

        let required = request.operation.required_grant();
        if !request.session.has(required) {
            return Err(AuthorizationError::MissingGrant(required));
        }

        if let BrokerOperation::CommitActivation {
            bundle_profile,
            generation_id,
            activation_relative_path,
        } = &request.operation
        {
            if bundle_profile != SUPPORTED_BUNDLE_PROFILE
                || bundle_profile != &self.active_bundle_profile
            {
                return Err(AuthorizationError::UnsupportedProfile(
                    bundle_profile.clone(),
                ));
            }
            if !is_generation_id(generation_id) {
                return Err(AuthorizationError::InvalidGenerationId);
            }
            let expected = format!("activations/generations/{generation_id}/activation.cbor");
            if activation_relative_path != &expected {
                return Err(AuthorizationError::InvalidActivationPath);
            }
        }

        Ok(())
    }
}

fn is_generation_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(grants: impl IntoIterator<Item = Grant>) -> AuthenticatedSession {
        AuthenticatedSession {
            host_key_id: [1; 32],
            session_id: [2; 32],
            grants: grants.into_iter().collect(),
        }
    }

    #[test]
    fn root_shell_needs_only_the_persistent_root_grant() {
        let request = BrokerRequest {
            session: session([Grant::ShellRoot]),
            operation: BrokerOperation::OpenRootShell,
        };
        assert_eq!(BrokerPolicy::default().authorize(&request), Ok(()));
    }

    #[test]
    fn transport_trust_does_not_imply_development_publisher_trust() {
        let request = BrokerRequest {
            session: session([Grant::DeviceRead, Grant::BundleInstall]),
            operation: BrokerOperation::AddDevelopmentPublisher {
                publisher_key: [3; 32],
            },
        };
        assert_eq!(
            BrokerPolicy::default().authorize(&request),
            Err(AuthorizationError::MissingGrant(Grant::BundlePublishDev))
        );
    }

    #[test]
    fn activation_path_is_reconstructed_not_trusted() {
        let request = BrokerRequest {
            session: session([Grant::BundleInstall]),
            operation: BrokerOperation::CommitActivation {
                bundle_profile: SUPPORTED_BUNDLE_PROFILE.to_owned(),
                generation_id: "0123456789abcdef0123456789abcdef".to_owned(),
                activation_relative_path: "../../etc/shadow".to_owned(),
            },
        };
        assert_eq!(
            BrokerPolicy::default().authorize(&request),
            Err(AuthorizationError::InvalidActivationPath)
        );
    }

    #[test]
    fn safe_mode_denies_privileged_mutation_even_with_grant() {
        let request = BrokerRequest {
            session: session([Grant::DeviceAdmin]),
            operation: BrokerOperation::RestartDaemon,
        };
        let policy = BrokerPolicy {
            safe_mode: true,
            ..BrokerPolicy::default()
        };
        assert_eq!(
            policy.authorize(&request),
            Err(AuthorizationError::SafeMode)
        );
    }
}
