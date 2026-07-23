//! Application installation, activation, and runtime ownership.
//!
//! This is the daemon's single application-domain boundary. Clones share the
//! same operation lock and process supervisor, so activation changes cannot
//! race lifecycle operations across KBP connections.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use kindlebridge_schema::{
    error_codes, AppList, AppLogParams, AppLogSnapshot, AppSummary, ProcessList, RpcError,
};

use crate::app::AppSupervisor;
mod backend;

#[derive(Clone, Debug)]
pub struct ApplicationManager {
    inner: Arc<ApplicationManagerInner>,
}

#[derive(Debug)]
struct ApplicationManagerInner {
    activation_root: PathBuf,
    profile: ApplicationProfile,
    operations: Mutex<()>,
    supervisor: AppSupervisor,
}

#[derive(Debug)]
struct ApplicationProfile {
    target: String,
    firmware: String,
    available_features: Vec<&'static str>,
}

impl ApplicationManager {
    #[must_use]
    pub fn new(
        activation_root: impl Into<PathBuf>,
        target: impl Into<String>,
        firmware: impl Into<String>,
        available_features: &[&'static str],
    ) -> Self {
        Self {
            inner: Arc::new(ApplicationManagerInner {
                activation_root: activation_root.into(),
                profile: ApplicationProfile {
                    target: target.into(),
                    firmware: firmware.into(),
                    available_features: available_features.to_vec(),
                },
                operations: Mutex::new(()),
                supervisor: AppSupervisor::new(),
            }),
        }
    }

    #[must_use]
    pub fn activation_root(&self) -> &Path {
        &self.inner.activation_root
    }

    pub fn install(
        &self,
        bundle: &mut File,
        expected_file_hash: &str,
    ) -> Result<AppSummary, RpcError> {
        let _operation = self.lock_install_operation()?;
        backend::app_install(
            bundle,
            expected_file_hash,
            self.activation_root(),
            &self.inner.profile.target,
            &self.inner.profile.firmware,
            &self.inner.profile.available_features,
            &self.inner.supervisor,
        )
    }

    pub fn list(&self) -> Result<AppList, RpcError> {
        let _operation = self.lock_operation()?;
        backend::app_list(self.activation_root(), &self.inner.supervisor)
    }

    pub fn log(&self, params: &AppLogParams) -> Result<AppLogSnapshot, RpcError> {
        let _operation = self.lock_operation()?;
        backend::app_log(self.activation_root(), &self.inner.supervisor, params)
    }

    pub fn start(&self, app_id: &str) -> Result<AppSummary, RpcError> {
        let _operation = self.lock_operation()?;
        backend::app_start(self.activation_root(), &self.inner.supervisor, app_id)
    }

    pub fn stop(&self, app_id: &str) -> Result<AppSummary, RpcError> {
        let _operation = self.lock_operation()?;
        backend::app_stop(self.activation_root(), &self.inner.supervisor, app_id)
    }

    pub fn restart(&self, app_id: &str) -> Result<AppSummary, RpcError> {
        let _operation = self.lock_operation()?;
        backend::app_restart(self.activation_root(), &self.inner.supervisor, app_id)
    }

    pub fn rollback(&self, app_id: &str) -> Result<AppSummary, RpcError> {
        let _operation = self.lock_operation()?;
        backend::app_rollback(self.activation_root(), &self.inner.supervisor, app_id)
    }

    pub fn uninstall(&self, app_id: &str) -> Result<AppSummary, RpcError> {
        let _operation = self.lock_operation()?;
        backend::app_uninstall(self.activation_root(), &self.inner.supervisor, app_id)
    }

    pub fn annotate_processes(&self, processes: &mut ProcessList) -> Result<(), RpcError> {
        let managed = self
            .inner
            .supervisor
            .managed_processes()
            .map_err(supervisor_unavailable)?;
        for process in &mut processes.processes {
            process.app_id = managed.get(&process.pid).cloned();
        }
        Ok(())
    }

    pub fn reject_managed_process_signal(&self, pid: u32) -> Result<(), RpcError> {
        let app_id = self
            .inner
            .supervisor
            .app_id_for_pid(pid)
            .map_err(supervisor_unavailable)?;
        if let Some(app_id) = app_id {
            return Err(RpcError::invalid_params(format!(
                "PID {pid} is managed by {app_id}; use app stop or app restart"
            )));
        }
        Ok(())
    }

    fn lock_operation(&self) -> Result<MutexGuard<'_, ()>, RpcError> {
        self.inner
            .operations
            .lock()
            .map_err(|_| invalid_operation_lock())
    }

    fn lock_install_operation(&self) -> Result<MutexGuard<'_, ()>, RpcError> {
        self.inner.operations.lock().map_err(|_| {
            RpcError::new(
                error_codes::APP_INSTALL_FAILED,
                "Application install failed",
            )
            .with_data(serde_json::json!({
                "stage": "lock",
                "reason": "internal_state",
                "detail": "application install lock is unavailable",
            }))
        })
    }
}

fn invalid_operation_lock() -> RpcError {
    RpcError::new(error_codes::INVALID_STATE, "Invalid device state").with_data(serde_json::json!({
        "detail": "application operation lock is unavailable",
    }))
}

fn supervisor_unavailable(error: crate::app::RuntimeError) -> RpcError {
    RpcError::new(
        error_codes::INVALID_STATE,
        "Application supervisor unavailable",
    )
    .with_data(serde_json::json!({ "detail": error.to_string() }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_application_domain_state() {
        let manager = ApplicationManager::new("/tmp/kindlebridge-apps", "kindlehf", "5.17.1", &[]);
        let clone = manager.clone();

        assert_eq!(clone.activation_root(), manager.activation_root());
        assert!(Arc::ptr_eq(&clone.inner, &manager.inner));
    }
}
