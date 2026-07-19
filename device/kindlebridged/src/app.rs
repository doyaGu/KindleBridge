//! Deterministic application lifecycle state machine.

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestartPolicy {
    Never,
    OnFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppState {
    Installed,
    Starting,
    Running,
    Stopping,
    Stopped,
    Crashed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppInstance {
    pub logical_id: String,
    pub channel: String,
    pub generation_id: String,
    pub restart_policy: RestartPolicy,
    pub state: AppState,
    pub restart_count: u32,
}

impl AppInstance {
    pub fn new(
        logical_id: impl Into<String>,
        channel: impl Into<String>,
        generation_id: impl Into<String>,
        restart_policy: RestartPolicy,
    ) -> Result<Self, AppError> {
        let logical_id = logical_id.into();
        let channel = channel.into();
        if !valid_logical_id(&logical_id) {
            return Err(AppError::InvalidLogicalId);
        }
        if !valid_channel(&channel) {
            return Err(AppError::InvalidChannel);
        }
        Ok(Self {
            logical_id,
            channel,
            generation_id: generation_id.into(),
            restart_policy,
            state: AppState::Installed,
            restart_count: 0,
        })
    }

    pub fn request_start(&mut self) -> Result<(), AppError> {
        if !matches!(
            self.state,
            AppState::Installed | AppState::Stopped | AppState::Crashed
        ) {
            return Err(AppError::InvalidTransition {
                from: self.state,
                action: "start",
            });
        }
        self.state = AppState::Starting;
        Ok(())
    }

    pub fn mark_started(&mut self) -> Result<(), AppError> {
        self.transition(AppState::Starting, AppState::Running, "mark-started")
    }

    pub fn request_stop(&mut self) -> Result<(), AppError> {
        self.transition(AppState::Running, AppState::Stopping, "stop")
    }

    pub fn mark_stopped(&mut self) -> Result<(), AppError> {
        self.transition(AppState::Stopping, AppState::Stopped, "mark-stopped")
    }

    pub fn mark_exited(&mut self, successful: bool) -> Result<bool, AppError> {
        if !matches!(self.state, AppState::Starting | AppState::Running) {
            return Err(AppError::InvalidTransition {
                from: self.state,
                action: "process-exit",
            });
        }
        if successful {
            self.state = AppState::Stopped;
            return Ok(false);
        }
        self.state = AppState::Crashed;
        if self.restart_policy == RestartPolicy::OnFailure {
            self.restart_count = self.restart_count.saturating_add(1);
            return Ok(true);
        }
        Ok(false)
    }

    fn transition(
        &mut self,
        expected: AppState,
        target: AppState,
        action: &'static str,
    ) -> Result<(), AppError> {
        if self.state != expected {
            return Err(AppError::InvalidTransition {
                from: self.state,
                action,
            });
        }
        self.state = target;
        Ok(())
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum AppError {
    #[error("invalid application logical id")]
    InvalidLogicalId,
    #[error("invalid application channel")]
    InvalidChannel,
    #[error("cannot perform {action} while application is {from:?}")]
    InvalidTransition {
        from: AppState,
        action: &'static str,
    },
}

fn valid_logical_id(value: &str) -> bool {
    let mut components = value.split('.');
    let Some(first) = components.next() else {
        return false;
    };
    let mut count = 1;
    if !valid_identifier_component(first) {
        return false;
    }
    for component in components {
        count += 1;
        if !valid_identifier_component(component) {
            return false;
        }
    }
    count >= 2 && value.len() <= 255
}

fn valid_identifier_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .as_bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

fn valid_channel(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restarts_only_failed_processes_when_requested() {
        let mut app = AppInstance::new(
            "org.example.reader",
            "dev",
            "generation",
            RestartPolicy::OnFailure,
        )
        .unwrap();
        app.request_start().unwrap();
        app.mark_started().unwrap();
        assert!(app.mark_exited(false).unwrap());
        assert_eq!(app.state, AppState::Crashed);
        assert_eq!(app.restart_count, 1);
    }

    #[test]
    fn invalid_state_transitions_are_rejected() {
        let mut app = AppInstance::new(
            "org.example.reader",
            "stable",
            "generation",
            RestartPolicy::Never,
        )
        .unwrap();
        assert!(matches!(
            app.request_stop(),
            Err(AppError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn ids_are_deliberately_conservative() {
        assert!(AppInstance::new(
            "org.Example.reader",
            "dev",
            "generation",
            RestartPolicy::Never
        )
        .is_err());
        assert!(AppInstance::new(
            "org.example.reader",
            "feature/foo",
            "generation",
            RestartPolicy::Never
        )
        .is_err());
    }
}
