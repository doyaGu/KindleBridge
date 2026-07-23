//! Device inventory and connected-session ownership.
//!
//! The registry is the only module that knows whether sessions are static or
//! rediscovered. Callers execute an operation against a leased provider; USB
//! session invalidation and reconnect policy stay behind this interface.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use kindlebridge_schema::{error_codes, RpcError};
use kindlebridge_transport_usb::UsbMatch;

use crate::device_session::ConnectedDeviceProvider;
use crate::{provider_rpc_error, DeviceProvider, ProviderError};

pub struct DeviceRegistry {
    source: RegistrySource,
}

enum RegistrySource {
    Direct(Arc<dyn DeviceProvider>),
    Usb {
        criteria: UsbMatch,
        sessions: SessionCache<ConnectedDeviceProvider>,
    },
}

enum ProviderLease {
    Direct(Arc<dyn DeviceProvider>),
    Usb(SessionLease<ConnectedDeviceProvider>),
}

struct SessionCache<T> {
    state: Mutex<SessionCacheState<T>>,
}

struct SessionCacheState<T> {
    generation: u64,
    current: Option<SessionLease<T>>,
}

struct SessionLease<T> {
    generation: u64,
    value: Arc<T>,
}

impl<T> Clone for SessionLease<T> {
    fn clone(&self) -> Self {
        Self {
            generation: self.generation,
            value: Arc::clone(&self.value),
        }
    }
}

impl DeviceRegistry {
    #[must_use]
    pub fn direct(provider: Arc<dyn DeviceProvider>) -> Self {
        Self {
            source: RegistrySource::Direct(provider),
        }
    }

    pub fn connect_tcp(addresses: &[SocketAddr]) -> Result<Self, ProviderError> {
        ConnectedDeviceProvider::connect(addresses).map(|provider| Self::direct(Arc::new(provider)))
    }

    #[must_use]
    pub fn connect_usb(criteria: UsbMatch) -> Self {
        Self {
            source: RegistrySource::Usb {
                criteria,
                sessions: SessionCache::new(),
            },
        }
    }

    pub(crate) fn rpc<T>(
        &self,
        operation: impl FnOnce(&dyn DeviceProvider) -> Result<T, RpcError>,
    ) -> Result<T, RpcError> {
        let lease = self.acquire().map_err(provider_rpc_error)?;
        let result = operation(lease.provider());
        if result.as_ref().is_err_and(is_link_unavailable) {
            self.invalidate(&lease);
        }
        result
    }

    fn acquire(&self) -> Result<ProviderLease, ProviderError> {
        match &self.source {
            RegistrySource::Direct(provider) => Ok(ProviderLease::Direct(Arc::clone(provider))),
            RegistrySource::Usb { criteria, sessions } => sessions
                .acquire(
                    || ConnectedDeviceProvider::connect_usb(criteria),
                    ConnectedDeviceProvider::is_online,
                )
                .map(ProviderLease::Usb),
        }
    }

    fn invalidate(&self, lease: &ProviderLease) {
        let (RegistrySource::Usb { sessions, .. }, ProviderLease::Usb(failed)) =
            (&self.source, lease)
        else {
            return;
        };
        sessions.invalidate(failed);
    }
}

impl ProviderLease {
    fn provider(&self) -> &dyn DeviceProvider {
        match self {
            Self::Direct(provider) => provider.as_ref(),
            Self::Usb(lease) => lease.value.as_ref(),
        }
    }
}

impl<T> SessionCache<T> {
    fn new() -> Self {
        Self {
            state: Mutex::new(SessionCacheState {
                generation: 0,
                current: None,
            }),
        }
    }

    fn acquire<E>(
        &self,
        connect: impl FnOnce() -> Result<T, E>,
        is_online: impl Fn(&T) -> bool,
    ) -> Result<SessionLease<T>, E> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(current) = state
            .current
            .as_ref()
            .filter(|current| is_online(&current.value))
        {
            return Ok(current.clone());
        }
        state.current = None;

        // Connect while holding the state lock. WinUSB has exclusive ownership;
        // releasing it here would let concurrent clients claim the interface
        // and perform duplicate HELLO handshakes.
        let value = Arc::new(connect()?);
        state.generation = state.generation.wrapping_add(1);
        let lease = SessionLease {
            generation: state.generation,
            value,
        };
        if is_online(&lease.value) {
            state.current = Some(lease.clone());
        }
        Ok(lease)
    }

    fn invalidate(&self, failed: &SessionLease<T>) {
        if let Ok(mut state) = self.state.lock() {
            if state.current.as_ref().is_some_and(|current| {
                current.generation == failed.generation
                    && Arc::ptr_eq(&current.value, &failed.value)
            }) {
                state.current = None;
            }
        }
    }
}

fn is_link_unavailable(error: &RpcError) -> bool {
    error.code == error_codes::SERVER_NOT_READY && error.message == "Device link is unavailable"
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;

    use super::*;
    use crate::MemoryDeviceProvider;

    struct FakeSession {
        online: AtomicBool,
        id: usize,
    }

    #[test]
    fn concurrent_first_acquire_connects_once_and_shares_the_generation() {
        let cache = Arc::new(SessionCache::new());
        let connects = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let connects = Arc::clone(&connects);
            workers.push(thread::spawn(move || {
                cache
                    .acquire(
                        || {
                            let id = connects.fetch_add(1, Ordering::AcqRel);
                            Ok::<_, ()>(FakeSession {
                                online: AtomicBool::new(true),
                                id,
                            })
                        },
                        |session| session.online.load(Ordering::Acquire),
                    )
                    .unwrap()
            }));
        }
        let leases: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();

        assert_eq!(connects.load(Ordering::Acquire), 1);
        assert!(leases
            .iter()
            .all(|lease| lease.generation == leases[0].generation));
        assert!(leases.iter().all(|lease| lease.value.id == 0));
    }

    #[test]
    fn stale_failure_cannot_invalidate_a_new_generation() {
        let cache = SessionCache::new();
        let connects = AtomicUsize::new(0);
        let connect = || {
            let id = connects.fetch_add(1, Ordering::AcqRel);
            Ok::<_, ()>(FakeSession {
                online: AtomicBool::new(true),
                id,
            })
        };
        let first = cache
            .acquire(connect, |session| session.online.load(Ordering::Acquire))
            .unwrap();
        cache.invalidate(&first);
        let second = cache
            .acquire(connect, |session| session.online.load(Ordering::Acquire))
            .unwrap();

        cache.invalidate(&first);
        let current = cache
            .acquire(connect, |session| session.online.load(Ordering::Acquire))
            .unwrap();

        assert_eq!(current.generation, second.generation);
        assert_eq!(current.value.id, second.value.id);
        assert_eq!(connects.load(Ordering::Acquire), 2);
    }

    #[test]
    fn offline_discovery_is_not_cached() {
        let cache = SessionCache::new();
        let connects = AtomicUsize::new(0);
        let connect = || {
            let id = connects.fetch_add(1, Ordering::AcqRel);
            Ok::<_, ()>(FakeSession {
                online: AtomicBool::new(id > 0),
                id,
            })
        };

        assert_eq!(
            cache
                .acquire(connect, |session| session.online.load(Ordering::Acquire))
                .unwrap()
                .value
                .id,
            0
        );
        assert_eq!(
            cache
                .acquire(connect, |session| session.online.load(Ordering::Acquire))
                .unwrap()
                .value
                .id,
            1
        );
        assert_eq!(connects.load(Ordering::Acquire), 2);
    }

    #[test]
    fn failed_operation_is_not_replayed_and_remote_error_is_not_a_link_failure() {
        let registry = DeviceRegistry::direct(Arc::new(MemoryDeviceProvider::default()));
        let calls = AtomicUsize::new(0);
        let remote = RpcError::new(error_codes::SERVER_NOT_READY, "Remote daemon is busy");

        let result = registry.rpc::<()>(|_| {
            calls.fetch_add(1, Ordering::AcqRel);
            Err(remote.clone())
        });

        assert_eq!(result, Err(remote));
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert!(!is_link_unavailable(result.as_ref().unwrap_err()));
    }
}
