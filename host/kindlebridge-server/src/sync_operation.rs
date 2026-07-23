//! Progress and cancellation state for one Host Sync Operation.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use kindlebridge_schema::{SyncProgress, SyncProgressPhase, TransferDirection};

pub struct HostSyncOperation {
    cancelled: AtomicBool,
    transferred: AtomicU64,
    total: AtomicU64,
    phase: Mutex<SyncProgressPhase>,
    transfer_id: Mutex<Option<String>>,
    cancel_hooks: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
}

impl Default for HostSyncOperation {
    fn default() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            transferred: AtomicU64::new(0),
            total: AtomicU64::new(0),
            phase: Mutex::new(SyncProgressPhase::Hashing),
            transfer_id: Mutex::new(None),
            cancel_hooks: Mutex::new(Vec::new()),
        }
    }
}

impl std::fmt::Debug for HostSyncOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostSyncOperation")
            .field("cancelled", &self.is_cancelled())
            .field("transferred", &self.transferred.load(Ordering::Acquire))
            .field("total", &self.total.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl HostSyncOperation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let hooks = self
            .cancel_hooks
            .lock()
            .map(|mut hooks| std::mem::take(&mut *hooks))
            .unwrap_or_default();
        for hook in hooks {
            hook();
        }
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn phase(&self, phase: SyncProgressPhase, transferred: u64, total: u64) {
        self.total.store(total, Ordering::Release);
        self.transferred.store(transferred, Ordering::Release);
        if let Ok(mut current) = self.phase.lock() {
            *current = phase;
        }
    }

    pub fn transferred(&self, transferred: u64) {
        self.transferred.store(transferred, Ordering::Release);
    }

    pub fn transfer_id(&self, transfer_id: impl Into<String>) {
        if let Ok(mut current) = self.transfer_id.lock() {
            *current = Some(transfer_id.into());
        }
    }

    pub fn on_cancel(&self, hook: impl FnOnce() + Send + 'static) {
        if self.is_cancelled() {
            hook();
            return;
        }
        let Ok(mut hooks) = self.cancel_hooks.lock() else {
            hook();
            return;
        };
        if self.is_cancelled() {
            drop(hooks);
            hook();
        } else {
            hooks.push(Box::new(hook));
        }
    }

    pub(crate) fn snapshot(
        &self,
        operation_id: String,
        direction: TransferDirection,
        remote_path: String,
    ) -> SyncProgress {
        SyncProgress {
            operation_id,
            transfer_id: self.transfer_id.lock().ok().and_then(|id| id.clone()),
            direction,
            remote_path,
            phase: self
                .phase
                .lock()
                .map_or(SyncProgressPhase::Transferring, |phase| phase.clone()),
            transferred: self.transferred.load(Ordering::Acquire),
            total: self.total.load(Ordering::Acquire),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn cancellation_hooks_run_once_and_late_hooks_run_immediately() {
        let operation = HostSyncOperation::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let first = Arc::clone(&calls);
        operation.on_cancel(move || {
            first.fetch_add(1, Ordering::AcqRel);
        });

        operation.cancel();
        operation.cancel();
        let late = Arc::clone(&calls);
        operation.on_cancel(move || {
            late.fetch_add(1, Ordering::AcqRel);
        });

        assert_eq!(calls.load(Ordering::Acquire), 2);
    }
}
