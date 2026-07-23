//! Current-user host rendezvous for the KindleBridge CLI and server.
//!
//! This module owns the platform-specific endpoint name, client connection,
//! listener creation, stale Unix socket replacement, and Unix socket permissions.

use std::io;
#[cfg(any(unix, test))]
use std::path::{Path, PathBuf};

#[cfg(unix)]
use interprocess::local_socket::GenericFilePath;
#[cfg(windows)]
use interprocess::local_socket::GenericNamespaced;
use interprocess::local_socket::{
    prelude::*, Listener, ListenerNonblockingMode, ListenerOptions, Stream,
};

mod lifecycle;

pub use lifecycle::{acquire, wait_for_shutdown, LifecycleError, ServerContract};

/// A nonblocking listener for the current user's KindleBridge server.
pub struct LocalListener {
    inner: Listener,
}

impl LocalListener {
    /// Accepts one client without blocking.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] when no client is waiting.
    pub fn accept(&self) -> io::Result<Stream> {
        self.inner.accept()
    }
}

/// Connects to the current user's KindleBridge server.
pub fn connect() -> io::Result<Stream> {
    let endpoint = endpoint();
    connect_to(&endpoint)
}

/// Creates the current user's nonblocking KindleBridge server listener.
pub fn bind() -> io::Result<LocalListener> {
    let endpoint = endpoint();
    let inner = listen_on(&endpoint).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("could not listen on {endpoint}: {error}"),
        )
    })?;
    secure_unix_socket(&endpoint)?;
    Ok(LocalListener { inner })
}

#[cfg(windows)]
fn connect_to(endpoint: &str) -> io::Result<Stream> {
    Stream::connect(endpoint.to_ns_name::<GenericNamespaced>()?)
}

#[cfg(unix)]
fn connect_to(endpoint: &str) -> io::Result<Stream> {
    Stream::connect(endpoint.to_fs_name::<GenericFilePath>()?)
}

#[cfg(windows)]
fn listen_on(endpoint: &str) -> io::Result<Listener> {
    ListenerOptions::new()
        .name(endpoint.to_ns_name::<GenericNamespaced>()?)
        .nonblocking(ListenerNonblockingMode::Accept)
        .create_sync()
}

#[cfg(unix)]
fn listen_on(endpoint: &str) -> io::Result<Listener> {
    ListenerOptions::new()
        .name(endpoint.to_fs_name::<GenericFilePath>()?)
        .try_overwrite(true)
        .nonblocking(ListenerNonblockingMode::Accept)
        .create_sync()
}

#[cfg(windows)]
fn endpoint() -> String {
    namespaced_endpoint(&std::env::var("USERNAME").unwrap_or_else(|_| "user".to_owned()))
}

#[cfg(unix)]
fn endpoint() -> String {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    filesystem_endpoint(
        &base,
        &std::env::var("USER").unwrap_or_else(|_| "user".to_owned()),
    )
    .to_string_lossy()
    .into_owned()
}

#[cfg(any(windows, test))]
fn namespaced_endpoint(user: &str) -> String {
    format!("kindlebridge-{}", sanitize_endpoint_component(user))
}

#[cfg(any(unix, test))]
fn filesystem_endpoint(base: &Path, user: &str) -> PathBuf {
    base.join(format!(
        "kindlebridge-{}.sock",
        sanitize_endpoint_component(user)
    ))
}

fn sanitize_endpoint_component(value: &str) -> String {
    let value: String = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
        .take(64)
        .collect();
    if value.is_empty() {
        "user".to_owned()
    } else {
        value
    }
}

#[cfg(unix)]
fn secure_unix_socket(endpoint: &str) -> io::Result<()> {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(endpoint, fs::Permissions::from_mode(0o600)).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("could not secure local socket {endpoint}: {error}"),
        )
    })
}

#[cfg(windows)]
fn secure_unix_socket(_endpoint: &str) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_component_keeps_only_portable_characters() {
        assert_eq!(sanitize_endpoint_component("Jane.Doe + 开发"), "JaneDoe");
        assert_eq!(sanitize_endpoint_component("user-name_2"), "user-name_2");
        assert_eq!(sanitize_endpoint_component("..."), "user");
    }

    #[test]
    fn endpoint_component_is_bounded() {
        assert_eq!(
            sanitize_endpoint_component(&"a".repeat(100)),
            "a".repeat(64)
        );
    }

    #[test]
    fn endpoint_forms_preserve_the_existing_contract() {
        assert_eq!(namespaced_endpoint("Jane Doe"), "kindlebridge-JaneDoe");
        assert_eq!(
            filesystem_endpoint(Path::new("/run/user/1000"), "jane"),
            Path::new("/run/user/1000/kindlebridge-jane.sock")
        );
    }
}
