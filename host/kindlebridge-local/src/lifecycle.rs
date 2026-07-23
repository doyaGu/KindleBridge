use std::fmt;
use std::io::{BufReader, BufWriter};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use interprocess::local_socket::{prelude::*, Stream};
use kindlebridge_schema::{methods, ClientError, RpcClient, ServerVersion, API_VERSION};

use crate::connect;

const SERVER_START_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_POLL_INTERVAL: Duration = Duration::from_millis(25);
const SERVER_ENDPOINT_QUIET_PERIOD: Duration = Duration::from_millis(100);
const SERVER_COMPETITOR_GRACE: Duration = Duration::from_millis(500);

/// The exact server identity accepted by a CLI build.
#[derive(Clone, Copy, Debug)]
pub struct ServerContract<'a> {
    pub name: &'a str,
    pub version: &'a str,
    pub api_version: &'a str,
}

impl ServerContract<'static> {
    /// Returns the server contract for the current KindleBridge build.
    pub const fn current() -> Self {
        Self {
            name: "kindlebridge-server",
            version: env!("CARGO_PKG_VERSION"),
            api_version: API_VERSION,
        }
    }
}

/// A verified local-server acquisition or shutdown failure.
#[derive(Debug)]
pub struct LifecycleError {
    message: String,
}

impl LifecycleError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LifecycleError {}

/// Acquires a compatible shared server, replacing an outdated verified server
/// or spawning the supplied command when no server is available.
pub fn acquire(
    command: Command,
    contract: ServerContract<'_>,
    mut on_replacing: impl FnMut(&ServerVersion),
) -> Result<Stream, LifecycleError> {
    if let Ok(stream) = connect() {
        match probe_server_version(stream) {
            Ok(version) => match classify_server(&version, contract) {
                ServerCompatibility::Compatible => {
                    if let Ok(stream) = connect() {
                        return Ok(stream);
                    }
                }
                ServerCompatibility::Replace => {
                    on_replacing(&version);
                    match stop_if_incompatible(contract)? {
                        StopOutcome::MatchingServer => {
                            if let Ok(stream) = connect() {
                                return Ok(stream);
                            }
                        }
                        StopOutcome::StopRequested | StopOutcome::EndpointGone => {}
                    }
                    match wait_for_replacement_window(contract)? {
                        ReplacementWindow::MatchingServer(stream) => return Ok(stream),
                        ReplacementWindow::EndpointGone => {}
                    }
                }
                ServerCompatibility::Foreign => {
                    return Err(foreign_server_error(&version, contract));
                }
            },
            Err(_) => {
                // A server can close between connect and the version call. Only
                // proceed when its endpoint actually disappears; never stop an
                // unverified process that happens to own the same local name.
                wait_for_shutdown()?;
            }
        }
    }
    spawn_server_and_connect(command, contract)
}

fn spawn_server_and_connect(
    mut command: Command,
    contract: ServerContract<'_>,
) -> Result<Stream, LifecycleError> {
    let program = command.get_program().to_string_lossy().into_owned();
    prepare_server_command(&mut command);
    let mut child = command
        .spawn()
        .map_err(|error| LifecycleError::new(format!("could not start {program}: {error}")))?;
    let started = Instant::now();
    let mut child_exit = None;
    let mut incompatible = None;
    loop {
        if let Ok(stream) = connect() {
            if let Ok(version) = probe_server_version(stream) {
                match classify_server(&version, contract) {
                    ServerCompatibility::Compatible => {
                        if let Ok(stream) = connect() {
                            return Ok(stream);
                        }
                    }
                    ServerCompatibility::Replace => incompatible = Some(version),
                    ServerCompatibility::Foreign => {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(foreign_server_error(&version, contract));
                    }
                }
            }
        }
        if child_exit.is_none() {
            child_exit = child
                .try_wait()
                .map_err(|error| LifecycleError::new(format!("could not inspect server: {error}")))?
                .map(|status| (Instant::now(), status));
        }
        if let Some((exited_at, status)) = &child_exit {
            if exited_at.elapsed() < SERVER_COMPETITOR_GRACE {
                thread::sleep(SERVER_POLL_INTERVAL);
                continue;
            }
            return Err(LifecycleError::new(format!(
                "server exited during startup with {status}"
            )));
        }
        if started.elapsed() >= SERVER_START_TIMEOUT {
            let _ = child.kill();
            let _ = child.wait();
            return Err(LifecycleError::new(match incompatible {
                Some(version) => format!(
                    "{program} launched {}, but this CLI requires {} (API {})",
                    format_server_identity(&version),
                    contract.version,
                    contract.api_version
                ),
                None => "shared local server startup timed out".to_owned(),
            }));
        }
        thread::sleep(SERVER_POLL_INTERVAL);
    }
}

fn prepare_server_command(command: &mut Command) {
    use std::process::Stdio;

    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    hide_server_window(command);
}

#[cfg(windows)]
fn hide_server_window(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW);
}

#[cfg(not(windows))]
fn hide_server_window(_command: &mut Command) {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServerCompatibility {
    Compatible,
    Replace,
    Foreign,
}

fn classify_server(version: &ServerVersion, contract: ServerContract<'_>) -> ServerCompatibility {
    if version.name != contract.name {
        ServerCompatibility::Foreign
    } else if version.version == contract.version && version.api_version == contract.api_version {
        ServerCompatibility::Compatible
    } else {
        ServerCompatibility::Replace
    }
}

fn probe_server_version(stream: Stream) -> Result<ServerVersion, ClientError> {
    let (reader, writer) = stream.split();
    let mut client = RpcClient::new(BufReader::new(reader), BufWriter::new(writer));
    let value = client.call(methods::SERVER_VERSION, None)?;
    serde_json::from_value(value).map_err(|_| ClientError::InvalidResponse)
}

enum StopOutcome {
    StopRequested,
    MatchingServer,
    EndpointGone,
}

fn stop_if_incompatible(contract: ServerContract<'_>) -> Result<StopOutcome, LifecycleError> {
    let Ok(stream) = connect() else {
        return Ok(StopOutcome::EndpointGone);
    };
    let (reader, writer) = stream.split();
    let mut client = RpcClient::new(BufReader::new(reader), BufWriter::new(writer));
    let value = match client.call(methods::SERVER_VERSION, None) {
        Ok(value) => value,
        Err(error) => return stop_race_outcome(error, contract),
    };
    let version: ServerVersion = serde_json::from_value(value).map_err(|_| {
        LifecycleError::new("the existing local server returned invalid version information")
    })?;
    match classify_server(&version, contract) {
        ServerCompatibility::Compatible => Ok(StopOutcome::MatchingServer),
        ServerCompatibility::Foreign => Err(foreign_server_error(&version, contract)),
        ServerCompatibility::Replace => {
            if let Err(error) = client.call(methods::SERVER_STOP, None) {
                return stop_race_outcome(error, contract);
            }
            Ok(StopOutcome::StopRequested)
        }
    }
}

fn stop_race_outcome(
    _error: ClientError,
    contract: ServerContract<'_>,
) -> Result<StopOutcome, LifecycleError> {
    let Ok(stream) = connect() else {
        return Ok(StopOutcome::EndpointGone);
    };
    match probe_server_version(stream) {
        Ok(version) => match classify_server(&version, contract) {
            ServerCompatibility::Compatible => Ok(StopOutcome::MatchingServer),
            ServerCompatibility::Foreign => Err(foreign_server_error(&version, contract)),
            // Another CLI may already have delivered STOP while the old
            // listener is still accepting its final connections. Enter the
            // bounded replacement wait instead of racing a second STOP.
            ServerCompatibility::Replace => Ok(StopOutcome::StopRequested),
        },
        Err(_) => Ok(StopOutcome::EndpointGone),
    }
}

enum ReplacementWindow {
    MatchingServer(Stream),
    EndpointGone,
}

fn wait_for_replacement_window(
    contract: ServerContract<'_>,
) -> Result<ReplacementWindow, LifecycleError> {
    let started = Instant::now();
    let mut unavailable_since = None;
    loop {
        match connect() {
            Ok(stream) => {
                unavailable_since = None;
                if let Ok(version) = probe_server_version(stream) {
                    match classify_server(&version, contract) {
                        ServerCompatibility::Compatible => {
                            if let Ok(stream) = connect() {
                                return Ok(ReplacementWindow::MatchingServer(stream));
                            }
                        }
                        ServerCompatibility::Foreign => {
                            return Err(foreign_server_error(&version, contract));
                        }
                        ServerCompatibility::Replace => {}
                    }
                }
            }
            Err(_) => {
                let unavailable_since = unavailable_since.get_or_insert_with(Instant::now);
                if unavailable_since.elapsed() >= SERVER_ENDPOINT_QUIET_PERIOD {
                    return Ok(ReplacementWindow::EndpointGone);
                }
            }
        }
        if started.elapsed() >= SERVER_STOP_TIMEOUT {
            return Err(LifecycleError::new(
                "old local server did not stop within 5 seconds",
            ));
        }
        thread::sleep(SERVER_POLL_INTERVAL);
    }
}

fn foreign_server_error(version: &ServerVersion, contract: ServerContract<'_>) -> LifecycleError {
    LifecycleError::new(format!(
        "local endpoint belongs to {}, not {}; stop it manually",
        format_server_identity(version),
        contract.name
    ))
}

fn format_server_identity(version: &ServerVersion) -> String {
    format!(
        "{} {} (API {})",
        version.name, version.version, version.api_version
    )
}

/// Waits until the shared endpoint has remained unavailable long enough that a
/// replacement cannot connect to the old listener's final accept window.
pub fn wait_for_shutdown() -> Result<(), LifecycleError> {
    // On Windows, a named-pipe connect can fail with ERROR_PIPE_BUSY while the old
    // listener still exists. Require a short, continuous unavailable period so
    // the next CLI cannot land in the server's final accept/exit window.
    if wait_until_stable(SERVER_STOP_TIMEOUT, SERVER_ENDPOINT_QUIET_PERIOD, || {
        connect().is_err()
    }) {
        Ok(())
    } else {
        Err(LifecycleError::new(
            "shared local server did not stop within 5 seconds",
        ))
    }
}

fn wait_until_stable(
    timeout: Duration,
    stable_for: Duration,
    mut condition: impl FnMut() -> bool,
) -> bool {
    let started = Instant::now();
    let mut stable_since = None;
    loop {
        if condition() {
            let stable_since = stable_since.get_or_insert_with(Instant::now);
            if stable_since.elapsed() >= stable_for {
                return true;
            }
        } else {
            stable_since = None;
        }
        if started.elapsed() >= timeout {
            return false;
        }
        thread::sleep(SERVER_POLL_INTERVAL.min(timeout.saturating_sub(started.elapsed())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_version(name: &str, version: &str, api_version: &str) -> ServerVersion {
        ServerVersion {
            name: name.to_owned(),
            version: version.to_owned(),
            api_version: api_version.to_owned(),
        }
    }

    #[test]
    fn compatibility_requires_exact_name_build_and_api() {
        let contract = ServerContract::current();
        assert_eq!(
            classify_server(
                &server_version(contract.name, contract.version, contract.api_version),
                contract
            ),
            ServerCompatibility::Compatible
        );
        assert_eq!(
            classify_server(
                &server_version(contract.name, "0.1.0-dev.40", contract.api_version),
                contract
            ),
            ServerCompatibility::Replace
        );
        assert_eq!(
            classify_server(
                &server_version(contract.name, contract.version, "v0"),
                contract
            ),
            ServerCompatibility::Replace
        );
        assert_eq!(
            classify_server(
                &server_version("unrelated-server", contract.version, contract.api_version),
                contract
            ),
            ServerCompatibility::Foreign
        );
    }

    #[test]
    fn shutdown_wait_resets_when_the_endpoint_reappears() {
        let mut attempts = 0;
        assert!(wait_until_stable(
            Duration::from_secs(1),
            Duration::ZERO,
            || {
                attempts += 1;
                attempts >= 3
            }
        ));
        assert_eq!(attempts, 3);
    }

    #[test]
    fn shutdown_wait_is_bounded() {
        assert!(!wait_until_stable(Duration::ZERO, Duration::ZERO, || false));
    }

    #[test]
    fn foreign_owner_error_names_both_sides() {
        let contract = ServerContract::current();
        let error = foreign_server_error(&server_version("other", "9", "v9"), contract);
        assert_eq!(
            error.to_string(),
            "local endpoint belongs to other 9 (API v9), not kindlebridge-server; stop it manually"
        );
    }
}
