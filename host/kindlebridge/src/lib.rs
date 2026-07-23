//! Command implementation for the `KindleBridge` CLI.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use clap::{Args, Parser, Subcommand};
use kindlebridge_bundle::{build_project_bundle, read_project_manifest};
use kindlebridge_schema::{
    methods, AppInstallParams, AppList, AppLogParams, AppLogSnapshot, AppState, AppSummary,
    AppTargetParams, ClientError, DeviceFeatures, DeviceList, DeviceState, ExecParams, ExecResult,
    LogSnapshot, LogTailParams, ProcessList, ProcessSignalParams, ProcessSummary, RpcClient,
    SerialParams, ServerVersion, SyncPullParams, SyncPullResult, SyncPushParams, SyncPushResult,
    SyncStatus, SyncStatusParams, TransferState, DEFAULT_SYNC_BLOCK_SIZE, MAX_SYNC_BLOCK_SIZE,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(name = "kindlebridge", about = "High-speed Kindle development bridge")]
pub struct Cli {
    /// Emit machine-readable JSON.
    #[arg(long, global = true)]
    pub json: bool,

    /// Path or name of kindlebridge-server.
    #[arg(
        long,
        global = true,
        env = "KINDLEBRIDGE_SERVER",
        default_value = "kindlebridge-server"
    )]
    pub server: String,

    /// Use one private stdio server instead of the shared local service.
    #[arg(long, global = true, hide = true)]
    pub server_stdio: bool,

    /// Pass a development device inventory to the stdio server.
    #[arg(long, global = true, env = "KINDLEBRIDGE_DEVICES_FILE")]
    pub devices_file: Option<String>,

    /// Connect the spawned server to a development device daemon. Repeatable.
    #[arg(
        long,
        global = true,
        env = "KINDLEBRIDGE_TCP_DEVICE",
        value_delimiter = ',',
        action = clap::ArgAction::Append
    )]
    pub tcp_device: Vec<String>,

    /// Disable automatic USB discovery when no explicit TCP/test device is selected.
    #[arg(long, global = true, env = "KINDLEBRIDGE_NO_USB")]
    pub no_usb: bool,

    /// Select one USB Kindle by its exact USB serial number.
    #[arg(long, global = true, env = "KINDLEBRIDGE_USB_SERIAL")]
    pub usb_serial: Option<String>,

    #[command(subcommand)]
    pub command: TopLevelCommand,
}

#[derive(Debug, Subcommand)]
pub enum TopLevelCommand {
    /// Inspect the host server.
    Server(ServerArgs),
    /// Inspect connected Kindles.
    Device(DeviceArgs),
    /// Run one non-interactive process on a device.
    Exec(ExecArgs),
    /// Open a persistent PTY/raw shell. Without -c, starts an interactive terminal.
    Shell(ShellArgs),
    /// Transfer files with resumable block checksums.
    Sync(SyncArgs),
    /// Stage device daemon updates for offline A/B activation.
    Daemon(DaemonArgs),
    /// Install and control applications.
    App(AppArgs),
    /// Inspect and signal device processes.
    Process(ProcessArgs),
    /// Read a bounded log snapshot.
    Log(LogArgs),
    /// Build, deploy, and start the current application project.
    Run(RunArgs),
}

#[derive(Clone, Debug, Args)]
pub struct RunArgs {
    /// Stable device serial from `device list`.
    pub serial: String,
    /// Project KBB manifest containing a [development] section.
    #[arg(long, default_value = "kindlebridge.toml")]
    pub manifest: PathBuf,
    /// Rebuild and redeploy when configured watch paths change.
    #[arg(long)]
    pub watch: bool,
}

#[derive(Debug, Args)]
pub struct DaemonArgs {
    #[command(subcommand)]
    pub command: DaemonCommand,
}

#[derive(Debug, Subcommand)]
pub enum DaemonCommand {
    /// Upload and verify a daemon in the inactive slot; activation stays offline.
    Stage {
        serial: String,
        device_binary: String,
    },
}

#[derive(Debug, Args)]
pub struct ExecArgs {
    /// Stable device serial from `device list`.
    pub serial: String,
    /// Process timeout in milliseconds.
    #[arg(long, default_value_t = 30_000)]
    pub timeout_ms: u64,
    /// Command and arguments; place them after `--`.
    #[arg(required = true, trailing_var_arg = true)]
    pub argv: Vec<String>,
}

#[derive(Debug, Args)]
pub struct ShellArgs {
    /// Stable device serial from `device list`.
    pub serial: String,
    /// Execute one shell command instead of opening the REPL.
    #[arg(short = 'c', long)]
    pub command: Option<String>,
    /// Request a PTY; repeat as -tt to force one for redirected stdin.
    #[arg(short = 't', action = clap::ArgAction::Count, conflicts_with = "no_tty")]
    pub tty: u8,
    /// Disable PTY allocation.
    #[arg(short = 'T', long)]
    pub no_tty: bool,
    /// Do not read local stdin.
    #[arg(short = 'n', long)]
    pub no_stdin: bool,
    /// Line-leading local escape character, or `none`.
    #[arg(short = 'e', default_value = "~")]
    pub escape: String,
    /// Emit one JSON object per stream event.
    #[arg(long, conflicts_with = "json")]
    pub ndjson: bool,
}

#[derive(Debug, Args)]
pub struct SyncArgs {
    #[command(subcommand)]
    pub command: SyncCommand,
}

#[derive(Debug, Subcommand)]
pub enum SyncCommand {
    /// Push a local file to a device.
    Push {
        /// Stable device serial from `device list`.
        serial: String,
        /// Local source file.
        local_path: String,
        /// Relative device path, or an absolute path below `/mnt/us/kindlebridge-data`.
        remote_path: String,
        /// Transfer frame size; the 256 KiB default balances USB throughput and interactive latency. Values below 64 KiB are for diagnostics.
        #[arg(long, default_value_t = DEFAULT_SYNC_BLOCK_SIZE as usize)]
        block_size: usize,
        /// Continue a previously interrupted transfer by its transfer ID.
        #[arg(long)]
        resume: Option<String>,
    },
    /// Pull a device file to the host.
    Pull {
        /// Stable device serial from `device list`.
        serial: String,
        /// Relative device path, or an absolute path below `/mnt/us/kindlebridge-data`.
        remote_path: String,
        /// Local destination file, which must not already exist.
        local_path: String,
        /// Transfer frame size; the 256 KiB default balances USB throughput and interactive latency. Values below 64 KiB are for diagnostics.
        #[arg(long, default_value_t = DEFAULT_SYNC_BLOCK_SIZE)]
        block_size: u32,
        /// Continue a previously interrupted transfer by its transfer ID.
        #[arg(long)]
        resume: Option<String>,
    },
    /// Inspect a resumable transfer.
    Status { serial: String, transfer_id: String },
}

#[derive(Debug, Args)]
pub struct AppArgs {
    #[command(subcommand)]
    pub command: AppCommand,
}

#[derive(Debug, Subcommand)]
pub enum AppCommand {
    /// Verify, upload, and atomically install a local KBB application bundle.
    Install {
        serial: String,
        bundle_path: String,
    },
    Start {
        serial: String,
        app_id: String,
    },
    Stop {
        serial: String,
        app_id: String,
    },
    Restart {
        serial: String,
        app_id: String,
    },
    Rollback {
        serial: String,
        app_id: String,
    },
    Uninstall {
        serial: String,
        app_id: String,
    },
    /// Print captured application stdout and stderr.
    Log {
        serial: String,
        app_id: String,
        /// Keep printing new output and follow application restarts.
        #[arg(long, short = 'f')]
        follow: bool,
        /// Maximum bytes to fetch from each output stream per request.
        #[arg(long, default_value_t = 16 * 1024, value_parser = clap::value_parser!(u32).range(1..=64 * 1024))]
        max_bytes: u32,
    },
    List {
        serial: String,
    },
}

#[derive(Debug, Args)]
pub struct ProcessArgs {
    #[command(subcommand)]
    pub command: ProcessCommand,
}

#[derive(Debug, Subcommand)]
pub enum ProcessCommand {
    List {
        serial: String,
    },
    Signal {
        serial: String,
        pid: u32,
        signal: String,
    },
}

#[derive(Debug, Args)]
pub struct LogArgs {
    #[command(subcommand)]
    pub command: LogCommand,
}

#[derive(Debug, Subcommand)]
pub enum LogCommand {
    Tail {
        serial: String,
        #[arg(long)]
        cursor: Option<u64>,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
}

#[derive(Debug, Args)]
pub struct ServerArgs {
    #[command(subcommand)]
    pub command: ServerCommand,
}

#[derive(Debug, Subcommand)]
pub enum ServerCommand {
    /// Check whether the server responds.
    Ping,
    /// Print server and API versions.
    Version,
    /// Show the shared local server process.
    Status,
    /// Ask the shared local server to exit.
    Stop,
}

#[derive(Debug, Args)]
pub struct DeviceArgs {
    #[command(subcommand)]
    pub command: DeviceCommand,
}

#[derive(Debug, Subcommand)]
pub enum DeviceCommand {
    /// List known devices.
    List,
    /// Round-trip one KBP control frame through a device.
    Ping {
        /// Stable device serial from `device list`.
        serial: String,
    },
    /// Print negotiated features for one device.
    Features {
        /// Stable device serial from `device list`.
        serial: String,
    },
}

pub trait RpcCaller {
    fn call(&mut self, method: &str, params: Option<Value>) -> Result<Value, ClientError>;
}

impl<R: BufRead, W: Write> RpcCaller for RpcClient<R, W> {
    fn call(&mut self, method: &str, params: Option<Value>) -> Result<Value, ClientError> {
        RpcClient::call(self, method, params)
    }
}

#[derive(Debug, Error)]
pub enum CliError {
    #[error(transparent)]
    Rpc(#[from] ClientError),
    #[error("server returned an invalid {kind} result")]
    InvalidResult { kind: &'static str },
    #[error("block size must be between 1 and 1048576 bytes; omit --block-size for the latency-safe USB default")]
    InvalidBlockSize,
    #[error("device path must be relative or below {DEVICE_SYNC_ROOT}: {0}")]
    RemotePathOutsideSyncRoot(String),
    #[error("could not resolve the current host directory: {0}")]
    CurrentDirectory(#[source] std::io::Error),
    #[error("invalid update binary: {0}")]
    InvalidUpdateBinary(String),
    #[error("shell requires the streaming shell.v2 path through the shared local server")]
    StreamingShellRequired,
    #[error("device rejected the {step} step: {message}")]
    UpdateRejected { step: &'static str, message: String },
    #[error("could not load the development project: {0}")]
    Project(String),
    #[error("build command failed with exit code {exit_code}{detail}")]
    BuildFailed { exit_code: i32, detail: String },
}

const DEVICE_LAUNCHER: &str = "/var/local/kindlebridge/control/bin/kindlebridge-launcher";
const DEVICE_RUNTIME_ROOT: &str = "/var/local/kindlebridge/control/runtime";
const DEVICE_SYNC_ROOT: &str = "/mnt/us/kindlebridge-data";
const MAX_UPDATE_BINARY_SIZE: u64 = 32 * 1024 * 1024;

#[derive(Debug, Eq, PartialEq)]
pub struct CommandOutput {
    pub output: String,
    pub exit_code: i32,
}

impl CommandOutput {
    fn success(output: String) -> Self {
        Self {
            output,
            exit_code: 0,
        }
    }
}

pub fn execute_with_status<C: RpcCaller>(
    caller: &mut C,
    command: &TopLevelCommand,
    json_output: bool,
) -> Result<CommandOutput, CliError> {
    match command {
        TopLevelCommand::Exec(args) => {
            execute_exec(caller, &args.serial, args.argv.clone(), 30_000, json_output)
        }
        TopLevelCommand::Shell(_) => Err(CliError::StreamingShellRequired),
        _ => execute(caller, command, json_output).map(CommandOutput::success),
    }
}

pub fn execute<C: RpcCaller>(
    caller: &mut C,
    command: &TopLevelCommand,
    json_output: bool,
) -> Result<String, CliError> {
    match command {
        TopLevelCommand::Server(args) => match args.command {
            ServerCommand::Ping => {
                let result = caller.call(methods::SERVER_PING, None)?;
                require_ping(&result)?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok("pong".to_owned())
                }
            }
            ServerCommand::Version => {
                let result = caller.call(methods::SERVER_VERSION, None)?;
                let version: ServerVersion = serde_json::from_value(result.clone())
                    .map_err(|_| CliError::InvalidResult { kind: "version" })?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok(format!(
                        "{} {} (API {})",
                        version.name, version.version, version.api_version
                    ))
                }
            }
            ServerCommand::Status => {
                let result = caller.call(methods::SERVER_STATUS, None)?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok(format!(
                        "running (pid {})",
                        result["pid"].as_u64().unwrap_or_default()
                    ))
                }
            }
            ServerCommand::Stop => {
                let result = caller.call(methods::SERVER_STOP, None)?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok("stopping".to_owned())
                }
            }
        },
        TopLevelCommand::Device(args) => match &args.command {
            DeviceCommand::List => {
                let result = caller.call(methods::DEVICE_LIST, None)?;
                let list: DeviceList = serde_json::from_value(result.clone()).map_err(|_| {
                    CliError::InvalidResult {
                        kind: "device list",
                    }
                })?;
                if json_output {
                    pretty_json(&result)
                } else if list.devices.is_empty() {
                    Ok("No devices.".to_owned())
                } else {
                    let lines = list.devices.into_iter().map(|device| {
                        let state = match device.state {
                            DeviceState::Online => "online",
                            DeviceState::Offline => "offline",
                            DeviceState::Unauthorized => "unauthorized",
                        };
                        format!(
                            "{}\t{}\t{}\t{}",
                            device.serial, device.model, state, device.transport
                        )
                    });
                    Ok(
                        std::iter::once("SERIAL\tMODEL\tSTATE\tTRANSPORT".to_owned())
                            .chain(lines)
                            .collect::<Vec<_>>()
                            .join("\n"),
                    )
                }
            }
            DeviceCommand::Ping { serial } => {
                let result =
                    caller.call(methods::DEVICE_PING, Some(json!({ "serial": serial })))?;
                require_ping(&result)?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok("pong".to_owned())
                }
            }
            DeviceCommand::Features { serial } => {
                let result =
                    caller.call(methods::DEVICE_FEATURES, Some(json!({ "serial": serial })))?;
                let features: DeviceFeatures =
                    serde_json::from_value(result.clone()).map_err(|_| {
                        CliError::InvalidResult {
                            kind: "device features",
                        }
                    })?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok(format!(
                        "{} (protocol {})\n{}",
                        features.serial,
                        features.protocol_version,
                        features.features.join("\n")
                    ))
                }
            }
        },
        TopLevelCommand::Exec(args) => execute_exec(
            caller,
            &args.serial,
            args.argv.clone(),
            args.timeout_ms,
            json_output,
        )
        .map(|result| result.output),
        TopLevelCommand::Shell(_) => Err(CliError::StreamingShellRequired),
        TopLevelCommand::Sync(args) => execute_sync(caller, &args.command, json_output),
        TopLevelCommand::Daemon(args) => execute_daemon(caller, &args.command, json_output),
        TopLevelCommand::App(args) => execute_app(caller, &args.command, json_output),
        TopLevelCommand::Process(args) => execute_process(caller, &args.command, json_output),
        TopLevelCommand::Log(args) => execute_log(caller, &args.command, json_output),
        TopLevelCommand::Run(args) => run_project_once(caller, args, json_output),
    }
}

pub fn run_project_once<C: RpcCaller>(
    caller: &mut C,
    args: &RunArgs,
    json_output: bool,
) -> Result<String, CliError> {
    run_project(caller, args, json_output, true)
}

pub fn deploy_project_after_build<C: RpcCaller>(
    caller: &mut C,
    args: &RunArgs,
    json_output: bool,
) -> Result<String, CliError> {
    run_project(caller, args, json_output, false)
}

fn run_project<C: RpcCaller>(
    caller: &mut C,
    args: &RunArgs,
    json_output: bool,
    execute_build: bool,
) -> Result<String, CliError> {
    let manifest_path = absolute_path(&args.manifest)?;
    let project_root = manifest_path.parent().ok_or_else(|| {
        CliError::Project(format!(
            "manifest has no parent directory: {}",
            manifest_path.display()
        ))
    })?;
    let manifest = read_project_manifest(&manifest_path)
        .map_err(|error| CliError::Project(error.to_string()))?;
    let development = manifest.development.as_ref().ok_or_else(|| {
        CliError::Project(format!(
            "{} is missing [development]",
            manifest_path.display()
        ))
    })?;
    if execute_build {
        if let Some((program, arguments)) = development.build.split_first() {
            run_project_build(program, arguments, project_root, json_output)?;
        }
    }

    let input = resolve_project_path(project_root, &development.input);
    let signing_key = resolve_project_path(project_root, &development.signing_key);
    let development_root = project_root.join(".kindlebridge");
    let output = development_root.join("run.kbb");
    let release = next_development_release(&development_root, manifest.release)?;
    let built = build_project_bundle(&manifest_path, &input, &signing_key, &output, Some(release))
        .map_err(|error| CliError::Project(error.to_string()))?;

    let bundle_path = normalize_host_path(output.to_string_lossy().as_ref())?;
    let (_, installed): (_, AppSummary) = call_typed(
        caller,
        methods::APP_INSTALL,
        &AppInstallParams {
            serial: args.serial.clone(),
            bundle_path,
        },
        "run install",
    )?;
    let (started_value, started): (_, AppSummary) = call_typed(
        caller,
        methods::APP_START,
        &AppTargetParams {
            serial: args.serial.clone(),
            app_id: built.id.clone(),
        },
        "run start",
    )?;
    if json_output {
        Ok(json!({
            "bundle": {
                "path": output,
                "bytes": built.bytes,
                "id": built.id,
                "version": built.version,
                "release": built.release,
                "bundle_root": format!("{:?}", built.bundle_root),
            },
            "installed": installed,
            "app": started,
        })
        .to_string())
    } else {
        Ok(format!(
            "built {} {} ({} bytes)\n{}",
            built.id,
            built.version,
            built.bytes,
            format_app_result(started_value, &started, false)?
        ))
    }
}

fn run_project_build(
    program: &str,
    arguments: &[String],
    project_root: &Path,
    json_output: bool,
) -> Result<(), CliError> {
    let mut command = Command::new(program);
    command.args(arguments).current_dir(project_root);
    if json_output {
        let output = command.output().map_err(|error| {
            CliError::Project(format!("could not start build command {program}: {error}"))
        })?;
        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let combined = format!("{stdout}{stderr}").trim().to_owned();
            return Err(CliError::BuildFailed {
                exit_code: output.status.code().unwrap_or(1),
                detail: if combined.is_empty() {
                    String::new()
                } else {
                    format!(": {combined}")
                },
            });
        }
    } else {
        let status = command.status().map_err(|error| {
            CliError::Project(format!("could not start build command {program}: {error}"))
        })?;
        if !status.success() {
            return Err(CliError::BuildFailed {
                exit_code: status.code().unwrap_or(1),
                detail: String::new(),
            });
        }
    }
    Ok(())
}

fn next_development_release(root: &Path, manifest_release: u64) -> Result<u64, CliError> {
    fs::create_dir_all(root).map_err(|error| {
        CliError::Project(format!(
            "could not create development state {}: {error}",
            root.display()
        ))
    })?;
    let state = root.join("run-release");
    let previous = match fs::read_to_string(&state) {
        Ok(value) => value.trim().parse::<u64>().map_err(|_| {
            CliError::Project(format!(
                "development release state is invalid: {}",
                state.display()
            ))
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            return Err(CliError::Project(format!(
                "could not read development release state {}: {error}",
                state.display()
            )));
        }
    };
    let clock: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CliError::Project(format!("system clock is before Unix epoch: {error}")))?
        .as_millis()
        .try_into()
        .map_err(|_| CliError::Project("system time does not fit a KBB release".to_owned()))?;
    let release = clock.max(manifest_release).max(previous.saturating_add(1));
    fs::write(&state, format!("{release}\n")).map_err(|error| {
        CliError::Project(format!(
            "could not update development release state {}: {error}",
            state.display()
        ))
    })?;
    Ok(release)
}

fn absolute_path(path: &Path) -> Result<PathBuf, CliError> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .map_err(CliError::CurrentDirectory)
    }
}

fn resolve_project_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        root.join(path)
    }
}

fn execute_daemon<C: RpcCaller>(
    caller: &mut C,
    command: &DaemonCommand,
    json_output: bool,
) -> Result<String, CliError> {
    let DaemonCommand::Stage {
        serial,
        device_binary,
    } = command;
    let (digest, size) = hash_update_binary(device_binary)?;
    let remote_path = format!("staging/daemon/{digest}/kindlebridged");
    let (_, pushed): (_, SyncPushResult) = call_typed(
        caller,
        methods::SYNC_PUSH,
        &SyncPushParams {
            serial: serial.clone(),
            local_path: device_binary.clone(),
            remote_path: remote_path.clone(),
            transfer_id: None,
            block_size: DEFAULT_SYNC_BLOCK_SIZE,
        },
        "daemon stage upload",
    )?;
    if pushed.state != TransferState::Complete || pushed.accepted_offset != size {
        return Err(CliError::UpdateRejected {
            step: "upload",
            message: format!(
                "transfer {} stopped at {}/{} bytes",
                pushed.transfer_id, pushed.accepted_offset, size
            ),
        });
    }

    let staged = call_exec_checked(
        caller,
        serial,
        vec![
            DEVICE_LAUNCHER.to_owned(),
            "stage".to_owned(),
            "--root".to_owned(),
            DEVICE_RUNTIME_ROOT.to_owned(),
            "--source".to_owned(),
            format!("{DEVICE_SYNC_ROOT}/{remote_path}"),
            "--blake3".to_owned(),
            digest.clone(),
        ],
        "staging",
    )?;
    let slot = parse_staged_update(&staged.stdout, &digest, size)?;

    let result = json!({
        "serial": serial,
        "slot": slot,
        "blake3": digest,
        "size": size,
        "transfer_id": pushed.transfer_id,
        "state": "staged"
    });
    if json_output {
        pretty_json(&result)
    } else {
        Ok(format!(
            "uploaded and verified {size} bytes in slot {}; apply from KUAL while USB is unplugged",
            result["slot"].as_str().unwrap_or("unknown")
        ))
    }
}

fn hash_update_binary(path: &str) -> Result<(String, u64), CliError> {
    let path = Path::new(path);
    if !path.is_absolute() {
        return Err(CliError::InvalidUpdateBinary(
            "path must be absolute".to_owned(),
        ));
    }
    let metadata = path
        .metadata()
        .map_err(|error| CliError::InvalidUpdateBinary(error.to_string()))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_UPDATE_BINARY_SIZE {
        return Err(CliError::InvalidUpdateBinary(format!(
            "size must be between 1 and {MAX_UPDATE_BINARY_SIZE} bytes"
        )));
    }
    let mut file =
        File::open(path).map_err(|error| CliError::InvalidUpdateBinary(error.to_string()))?;
    let mut hasher = blake3::Hasher::new();
    // Windows CLI main threads have a smaller stack than Rust test threads.
    // Keep the full USB transfer batch on the heap.
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut observed_size = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| CliError::InvalidUpdateBinary(error.to_string()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        observed_size = observed_size.saturating_add(read as u64);
    }
    if observed_size != metadata.len() {
        return Err(CliError::InvalidUpdateBinary(
            "file changed while being read".to_owned(),
        ));
    }
    Ok((hasher.finalize().to_hex().to_string(), observed_size))
}

fn call_exec_checked<C: RpcCaller>(
    caller: &mut C,
    serial: &str,
    argv: Vec<String>,
    step: &'static str,
) -> Result<ExecResult, CliError> {
    let (_, result): (_, ExecResult) = call_typed(
        caller,
        methods::EXEC_RUN,
        &ExecParams {
            serial: serial.to_owned(),
            argv,
            cwd: None,
            environment: BTreeMap::new(),
            timeout_ms: 30_000,
        },
        "daemon stage exec",
    )?;
    if result.exit_code != 0 {
        let message = if result.stderr.trim().is_empty() {
            format!("command exited with {}", result.exit_code)
        } else {
            result.stderr.trim().to_owned()
        };
        return Err(CliError::UpdateRejected { step, message });
    }
    Ok(result)
}

fn parse_staged_update(
    stdout: &str,
    expected_digest: &str,
    expected_size: u64,
) -> Result<String, CliError> {
    let fields = stdout.trim().split('\t').collect::<Vec<_>>();
    if fields.len() != 3 || !matches!(fields[0], "A" | "B") {
        return Err(CliError::UpdateRejected {
            step: "staging",
            message: "launcher returned an invalid slot record".to_owned(),
        });
    }
    let size = fields[2]
        .parse::<u64>()
        .map_err(|_| CliError::UpdateRejected {
            step: "staging",
            message: "launcher returned an invalid binary size".to_owned(),
        })?;
    if fields[1] != expected_digest || size != expected_size {
        return Err(CliError::UpdateRejected {
            step: "staging",
            message: "launcher verification result did not match the upload".to_owned(),
        });
    }
    Ok(fields[0].to_owned())
}

fn execute_exec<C: RpcCaller>(
    caller: &mut C,
    serial: &str,
    argv: Vec<String>,
    timeout_ms: u64,
    json_output: bool,
) -> Result<CommandOutput, CliError> {
    let result = caller.call(
        methods::EXEC_RUN,
        Some(
            serde_json::to_value(ExecParams {
                serial: serial.to_owned(),
                argv,
                cwd: None,
                environment: BTreeMap::new(),
                timeout_ms,
            })
            .map_err(|_| CliError::InvalidResult {
                kind: "exec params",
            })?,
        ),
    )?;
    let exec: ExecResult = serde_json::from_value(result.clone())
        .map_err(|_| CliError::InvalidResult { kind: "exec" })?;
    let exit_code = exec.exit_code;
    let output = if json_output {
        pretty_json(&result)
    } else {
        let mut output = exec.stdout;
        if !exec.stderr.is_empty() {
            output.push_str(&exec.stderr);
        }
        if exec.exit_code != 0 {
            output.push_str(&format!("[exit {}]\n", exec.exit_code));
        }
        Ok(output.trim_end_matches('\n').to_owned())
    }?;
    Ok(CommandOutput { output, exit_code })
}

fn execute_sync<C: RpcCaller>(
    caller: &mut C,
    command: &SyncCommand,
    json_output: bool,
) -> Result<String, CliError> {
    match command {
        SyncCommand::Push {
            serial,
            local_path,
            remote_path,
            block_size,
            resume,
        } => {
            validate_block_size(*block_size)?;
            let block_size = u32::try_from(*block_size).map_err(|_| CliError::InvalidBlockSize)?;
            let local_path = normalize_host_path(local_path)?;
            let remote_path = normalize_remote_path(remote_path)?;
            let started = Instant::now();
            let (value, result): (_, SyncPushResult) = call_typed(
                caller,
                methods::SYNC_PUSH,
                &SyncPushParams {
                    serial: serial.clone(),
                    local_path: local_path.clone(),
                    remote_path: remote_path.clone(),
                    transfer_id: resume.clone(),
                    block_size,
                },
                "sync push",
            )?;
            if json_output {
                pretty_json(&value)
            } else {
                Ok(format_transfer_summary(
                    "pushed",
                    result.accepted_offset,
                    "to",
                    &remote_path,
                    &result.transfer_id,
                    started.elapsed(),
                    resume.is_some(),
                ))
            }
        }
        SyncCommand::Pull {
            serial,
            remote_path,
            local_path,
            block_size,
            resume,
        } => {
            validate_block_size(usize::try_from(*block_size).unwrap_or(usize::MAX))?;
            let remote_path = normalize_remote_path(remote_path)?;
            let local_path = normalize_host_path(local_path)?;
            let started = Instant::now();
            let (value, result): (_, SyncPullResult) = call_typed(
                caller,
                methods::SYNC_PULL,
                &SyncPullParams {
                    serial: serial.clone(),
                    remote_path: remote_path.clone(),
                    local_path: local_path.clone(),
                    transfer_id: resume.clone(),
                    block_size: *block_size,
                },
                "sync pull",
            )?;
            if json_output {
                pretty_json(&value)
            } else {
                Ok(format_transfer_summary(
                    "pulled",
                    result.received_size,
                    "to",
                    &local_path,
                    &result.transfer_id,
                    started.elapsed(),
                    resume.is_some(),
                ))
            }
        }
        SyncCommand::Status {
            serial,
            transfer_id,
        } => {
            let (value, status): (_, SyncStatus) = call_typed(
                caller,
                methods::SYNC_STATUS,
                &SyncStatusParams {
                    serial: serial.clone(),
                    transfer_id: transfer_id.clone(),
                },
                "sync status",
            )?;
            if json_output {
                pretty_json(&value)
            } else {
                Ok(format!(
                    "{} {:?} {}/{} {:?}",
                    status.transfer_id,
                    status.direction,
                    status.next_offset,
                    status.total_size,
                    status.state
                )
                .to_lowercase())
            }
        }
    }
}

fn execute_app<C: RpcCaller>(
    caller: &mut C,
    command: &AppCommand,
    json_output: bool,
) -> Result<String, CliError> {
    match command {
        AppCommand::Install {
            serial,
            bundle_path,
        } => {
            let bundle_path = normalize_host_path(bundle_path)?;
            let (value, app): (_, AppSummary) = call_typed(
                caller,
                methods::APP_INSTALL,
                &AppInstallParams {
                    serial: serial.clone(),
                    bundle_path,
                },
                "app install",
            )?;
            format_app_result(value, &app, json_output)
        }
        AppCommand::List { serial } => {
            let (value, list): (_, AppList) = call_typed(
                caller,
                methods::APP_LIST,
                &SerialParams {
                    serial: serial.clone(),
                },
                "app list",
            )?;
            if json_output {
                pretty_json(&value)
            } else if list.apps.is_empty() {
                Ok("No apps.".to_owned())
            } else {
                Ok(list
                    .apps
                    .iter()
                    .map(format_app)
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
        }
        AppCommand::Log {
            serial,
            app_id,
            follow,
            max_bytes,
        } => {
            if *follow {
                return Err(CliError::Project(
                    "app log --follow requires the streaming CLI path".to_owned(),
                ));
            }
            let (value, snapshot): (_, AppLogSnapshot) = call_typed(
                caller,
                methods::APP_LOG,
                &AppLogParams {
                    serial: serial.clone(),
                    app_id: app_id.clone(),
                    run_id: None,
                    stdout_cursor: 0,
                    stderr_cursor: 0,
                    max_bytes: Some(*max_bytes),
                },
                "app log",
            )?;
            if json_output {
                pretty_json(&value)
            } else {
                let stdout = decode_app_log(&snapshot.stdout.data_base64)?;
                let stderr = decode_app_log(&snapshot.stderr.data_base64)?;
                Ok(format!(
                    "{}{}",
                    String::from_utf8_lossy(&stdout),
                    String::from_utf8_lossy(&stderr)
                ))
            }
        }
        AppCommand::Start { serial, app_id }
        | AppCommand::Stop { serial, app_id }
        | AppCommand::Restart { serial, app_id }
        | AppCommand::Rollback { serial, app_id }
        | AppCommand::Uninstall { serial, app_id } => {
            let method = match command {
                AppCommand::Start { .. } => methods::APP_START,
                AppCommand::Stop { .. } => methods::APP_STOP,
                AppCommand::Restart { .. } => methods::APP_RESTART,
                AppCommand::Rollback { .. } => methods::APP_ROLLBACK,
                AppCommand::Uninstall { .. } => methods::APP_UNINSTALL,
                AppCommand::Install { .. } | AppCommand::Log { .. } | AppCommand::List { .. } => {
                    unreachable!()
                }
            };
            let (value, app): (_, AppSummary) = call_typed(
                caller,
                method,
                &AppTargetParams {
                    serial: serial.clone(),
                    app_id: app_id.clone(),
                },
                "app operation",
            )?;
            format_app_result(value, &app, json_output)
        }
    }
}

fn decode_app_log(encoded: &str) -> Result<Vec<u8>, CliError> {
    BASE64
        .decode(encoded)
        .map_err(|_| CliError::InvalidResult { kind: "app log" })
}

fn execute_process<C: RpcCaller>(
    caller: &mut C,
    command: &ProcessCommand,
    json_output: bool,
) -> Result<String, CliError> {
    match command {
        ProcessCommand::List { serial } => {
            let (value, list): (_, ProcessList) = call_typed(
                caller,
                methods::PROCESS_LIST,
                &SerialParams {
                    serial: serial.clone(),
                },
                "process list",
            )?;
            if json_output {
                pretty_json(&value)
            } else if list.processes.is_empty() {
                Ok("No processes.".to_owned())
            } else {
                Ok(list
                    .processes
                    .iter()
                    .map(|process| format!("{}\t{}", process.pid, process.name))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
        }
        ProcessCommand::Signal {
            serial,
            pid,
            signal,
        } => {
            let (value, process): (_, ProcessSummary) = call_typed(
                caller,
                methods::PROCESS_SIGNAL,
                &ProcessSignalParams {
                    serial: serial.clone(),
                    pid: *pid,
                    signal: signal.clone(),
                },
                "process signal",
            )?;
            if json_output {
                pretty_json(&value)
            } else {
                Ok(format!(
                    "sent {} to {} ({})",
                    signal, process.pid, process.name
                ))
            }
        }
    }
}

fn execute_log<C: RpcCaller>(
    caller: &mut C,
    command: &LogCommand,
    json_output: bool,
) -> Result<String, CliError> {
    match command {
        LogCommand::Tail {
            serial,
            cursor,
            limit,
        } => {
            let (value, snapshot): (_, LogSnapshot) = call_typed(
                caller,
                methods::LOG_TAIL,
                &LogTailParams {
                    serial: serial.clone(),
                    cursor: *cursor,
                    limit: Some(*limit),
                },
                "log tail",
            )?;
            if json_output {
                pretty_json(&value)
            } else {
                Ok(snapshot
                    .entries
                    .iter()
                    .map(|entry| {
                        format!(
                            "{}\t{}\t{}\t{}",
                            entry.cursor, entry.level, entry.source, entry.message
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
        }
    }
}

fn call_typed<C: RpcCaller, P: Serialize, T: DeserializeOwned>(
    caller: &mut C,
    method: &str,
    params: &P,
    kind: &'static str,
) -> Result<(Value, T), CliError> {
    let params = serde_json::to_value(params).map_err(|_| CliError::InvalidResult { kind })?;
    let value = caller.call(method, Some(params))?;
    let typed =
        serde_json::from_value(value.clone()).map_err(|_| CliError::InvalidResult { kind })?;
    Ok((value, typed))
}

fn validate_block_size(block_size: usize) -> Result<(), CliError> {
    if (1..=usize::try_from(MAX_SYNC_BLOCK_SIZE).unwrap()).contains(&block_size) {
        Ok(())
    } else {
        Err(CliError::InvalidBlockSize)
    }
}

fn normalize_remote_path(path: &str) -> Result<String, CliError> {
    let path = path.replace('\\', "/");
    if let Some(relative) = path.strip_prefix(&format!("{DEVICE_SYNC_ROOT}/")) {
        if !relative.is_empty() {
            return Ok(relative.to_owned());
        }
    }
    if path.starts_with('/') {
        return Err(CliError::RemotePathOutsideSyncRoot(path));
    }
    Ok(path)
}

fn normalize_host_path(path: &str) -> Result<String, CliError> {
    let path = Path::new(path);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(CliError::CurrentDirectory)?
            .join(path)
    };
    Ok(absolute.to_string_lossy().into_owned())
}

fn format_transfer_summary(
    action: &str,
    bytes: u64,
    preposition: &str,
    path: &str,
    transfer_id: &str,
    elapsed: Duration,
    resumed: bool,
) -> String {
    let seconds = elapsed.as_secs_f64();
    let timing = if resumed || seconds <= f64::EPSILON {
        format!("in {seconds:.2} s")
    } else {
        let mib_per_second = bytes as f64 / (1024.0 * 1024.0) / seconds;
        format!("in {seconds:.2} s ({mib_per_second:.2} MiB/s)")
    };
    format!("{action} {bytes} bytes {preposition} {path} {timing} ({transfer_id})")
}

fn format_app_result(
    value: Value,
    app: &AppSummary,
    json_output: bool,
) -> Result<String, CliError> {
    if json_output {
        pretty_json(&value)
    } else {
        Ok(format_app(app))
    }
}

fn format_app(app: &AppSummary) -> String {
    let state = match app.state {
        AppState::Stopped => "stopped".to_owned(),
        AppState::Running => format!("running pid={}", app.pid.unwrap_or(0)),
        AppState::Failed => "failed".to_owned(),
    };
    format!("{}\t{}\t{}", app.app_id, app.version, state)
}

fn require_ping(value: &Value) -> Result<(), CliError> {
    if value.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(CliError::InvalidResult { kind: "ping" })
    }
}

fn pretty_json(value: &Value) -> Result<String, CliError> {
    serde_json::to_string_pretty(value).map_err(|_| CliError::InvalidResult { kind: "JSON" })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use kindlebridge_schema::DeviceState;

    use super::*;

    struct RecordingCaller {
        calls: Vec<(String, Option<Value>)>,
        replies: VecDeque<Value>,
    }

    impl RpcCaller for RecordingCaller {
        fn call(&mut self, method: &str, params: Option<Value>) -> Result<Value, ClientError> {
            self.calls.push((method.to_owned(), params));
            Ok(self.replies.pop_front().unwrap())
        }
    }

    #[test]
    fn features_command_uses_the_public_v1_method() {
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([json!({
                "serial": "KT6-TEST",
                "protocol_version": kindlebridge_schema::device_protocol::PROTOCOL_VERSION,
                "features": ["exec.v1", "sync.v1"]
            })]),
        };
        let command = TopLevelCommand::Device(DeviceArgs {
            command: DeviceCommand::Features {
                serial: "KT6-TEST".to_owned(),
            },
        });

        let output = execute(&mut caller, &command, false).unwrap();
        assert_eq!(
            caller.calls,
            vec![(
                methods::DEVICE_FEATURES.to_owned(),
                Some(json!({ "serial": "KT6-TEST" }))
            )]
        );
        assert!(output.contains("exec.v1"));
    }

    #[test]
    fn device_ping_uses_the_device_control_method() {
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([json!({ "ok": true })]),
        };
        let command = TopLevelCommand::Device(DeviceArgs {
            command: DeviceCommand::Ping {
                serial: "KT6-TEST".to_owned(),
            },
        });

        assert_eq!(execute(&mut caller, &command, false).unwrap(), "pong");
        assert_eq!(
            caller.calls,
            vec![(
                methods::DEVICE_PING.to_owned(),
                Some(json!({ "serial": "KT6-TEST" }))
            )]
        );
    }

    #[test]
    fn device_list_json_is_stable_and_typed() {
        let summary = kindlebridge_schema::DeviceSummary {
            serial: "KT6-TEST".to_owned(),
            model: "KT6".to_owned(),
            state: DeviceState::Online,
            transport: "usb".to_owned(),
        };
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([serde_json::to_value(DeviceList {
                devices: vec![summary],
            })
            .unwrap()]),
        };
        let command = TopLevelCommand::Device(DeviceArgs {
            command: DeviceCommand::List,
        });
        let output = execute(&mut caller, &command, true).unwrap();
        assert_eq!(caller.calls[0].0, methods::DEVICE_LIST);
        assert_eq!(
            serde_json::from_str::<Value>(&output).unwrap()["devices"][0]["state"],
            "online"
        );
    }

    #[test]
    fn exec_uses_public_rpc_and_preserves_argv() {
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([json!({
                "exit_code": 0,
                "stdout": "hello kindle\n",
                "stderr": "",
                "duration_ms": 1
            })]),
        };
        let output = execute(
            &mut caller,
            &TopLevelCommand::Exec(ExecArgs {
                serial: "KT6-TEST".to_owned(),
                timeout_ms: 1000,
                argv: vec!["echo".to_owned(), "hello kindle".to_owned()],
            }),
            false,
        )
        .unwrap();
        assert_eq!(output, "hello kindle");
        assert_eq!(caller.calls[0].0, methods::EXEC_RUN);
        assert_eq!(
            caller.calls[0].1.as_ref().unwrap()["argv"][1],
            "hello kindle"
        );
    }

    #[test]
    fn shell_command_cannot_fall_back_to_captured_exec() {
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::new(),
        };
        let error = execute(
            &mut caller,
            &TopLevelCommand::Shell(ShellArgs {
                serial: "KT6-TEST".to_owned(),
                command: Some("printf shell-ok".to_owned()),
                tty: 0,
                no_tty: true,
                no_stdin: true,
                escape: "none".to_owned(),
                ndjson: false,
            }),
            false,
        )
        .unwrap_err();
        assert!(matches!(error, CliError::StreamingShellRequired));
        assert!(caller.calls.is_empty());
    }

    #[test]
    fn tcp_device_is_repeatable_and_global() {
        let cli = Cli::try_parse_from([
            "kindlebridge",
            "--tcp-device",
            "127.0.0.1:4765",
            "--tcp-device",
            "192.168.15.244:4765",
            "device",
            "list",
        ])
        .unwrap();
        assert_eq!(
            cli.tcp_device,
            vec!["127.0.0.1:4765", "192.168.15.244:4765"]
        );
        assert!(!cli.no_usb);
    }

    #[test]
    fn usb_discovery_is_on_by_default_and_can_be_disabled() {
        let automatic = Cli::try_parse_from(["kindlebridge", "device", "list"]).unwrap();
        assert!(!automatic.no_usb);
        let disabled = Cli::try_parse_from(["kindlebridge", "--no-usb", "device", "list"]).unwrap();
        assert!(disabled.no_usb);
    }

    #[test]
    fn sync_uses_the_latency_safe_block_size_by_default() {
        let cli = Cli::try_parse_from([
            "kindlebridge",
            "sync",
            "push",
            "KT6-TEST",
            "C:\\payload.bin",
            "apps/payload.bin",
        ])
        .unwrap();
        let TopLevelCommand::Sync(SyncArgs {
            command: SyncCommand::Push { block_size, .. },
        }) = cli.command
        else {
            panic!("expected sync push command");
        };
        assert_eq!(block_size, 256 * 1024);
    }

    #[test]
    fn app_install_cli_takes_one_bundle_instead_of_claimed_identity_fields() {
        let cli = Cli::try_parse_from(["kindlebridge", "app", "install", "KT6-TEST", "reader.kbb"])
            .unwrap();
        let TopLevelCommand::App(AppArgs {
            command:
                AppCommand::Install {
                    serial,
                    bundle_path,
                },
        }) = cli.command
        else {
            panic!("expected app install command");
        };
        assert_eq!(serial, "KT6-TEST");
        assert_eq!(bundle_path, "reader.kbb");
        assert!(Cli::try_parse_from([
            "kindlebridge",
            "app",
            "install",
            "KT6-TEST",
            "org.example.forged",
            "99.0.0",
        ])
        .is_err());
    }

    #[test]
    fn app_log_cli_has_bounded_one_shot_and_follow_modes() {
        let cli = Cli::try_parse_from([
            "kindlebridge",
            "app",
            "log",
            "KT6-TEST",
            "org.example.reader",
            "--follow",
            "--max-bytes",
            "4096",
        ])
        .unwrap();
        let TopLevelCommand::App(AppArgs {
            command:
                AppCommand::Log {
                    serial,
                    app_id,
                    follow,
                    max_bytes,
                },
        }) = cli.command
        else {
            panic!("expected app log command");
        };
        assert_eq!(serial, "KT6-TEST");
        assert_eq!(app_id, "org.example.reader");
        assert!(follow);
        assert_eq!(max_bytes, 4096);

        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([json!({
                "app_id": "org.example.reader",
                "run_id": "run-1",
                "reset": true,
                "state": "running",
                "pid": 2048,
                "stdout": {
                    "cursor": 0,
                    "next_cursor": 6,
                    "data_base64": BASE64.encode(b"hello\n"),
                    "capped": false
                },
                "stderr": {
                    "cursor": 0,
                    "next_cursor": 5,
                    "data_base64": BASE64.encode(b"oops\n"),
                    "capped": false
                }
            })]),
        };
        let output = execute(
            &mut caller,
            &TopLevelCommand::App(AppArgs {
                command: AppCommand::Log {
                    serial: "KT6-TEST".to_owned(),
                    app_id: "org.example.reader".to_owned(),
                    follow: false,
                    max_bytes: 16 * 1024,
                },
            }),
            false,
        )
        .unwrap();
        assert_eq!(output, "hello\noops\n");
        assert_eq!(caller.calls[0].0, methods::APP_LOG);
        assert_eq!(caller.calls[0].1.as_ref().unwrap()["stdout_cursor"], 0);
    }

    #[test]
    fn run_cli_defaults_to_one_shot_and_accepts_watch() {
        let one_shot = Cli::try_parse_from(["kindlebridge", "run", "KT6-TEST"]).expect("parse run");
        let TopLevelCommand::Run(one_shot) = one_shot.command else {
            panic!("expected run command");
        };
        assert_eq!(one_shot.serial, "KT6-TEST");
        assert_eq!(one_shot.manifest, PathBuf::from("kindlebridge.toml"));
        assert!(!one_shot.watch);

        let watch = Cli::try_parse_from([
            "kindlebridge",
            "run",
            "KT6-TEST",
            "--manifest",
            "demo.toml",
            "--watch",
        ])
        .expect("parse run --watch");
        let TopLevelCommand::Run(watch) = watch.command else {
            panic!("expected run command");
        };
        assert_eq!(watch.manifest, PathBuf::from("demo.toml"));
        assert!(watch.watch);
    }

    #[test]
    fn sync_rpc_carries_paths_and_never_file_bytes() {
        let local_path = std::env::current_dir()
            .unwrap()
            .join("payload.bin")
            .to_string_lossy()
            .into_owned();
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([json!({
                "transfer_id": "push-test",
                "accepted_offset": 123,
                "state": "complete"
            })]),
        };
        execute(
            &mut caller,
            &TopLevelCommand::Sync(SyncArgs {
                command: SyncCommand::Push {
                    serial: "KT6-TEST".to_owned(),
                    local_path: local_path.clone(),
                    remote_path: "apps/payload.bin".to_owned(),
                    block_size: 65_536,
                    resume: None,
                },
            }),
            true,
        )
        .unwrap();
        let params = caller.calls[0].1.as_ref().unwrap();
        assert_eq!(params["local_path"], local_path);
        assert_eq!(params["remote_path"], "apps/payload.bin");
        assert!(params.get("data_base64").is_none());
        assert!(params.get("block_hash").is_none());
    }

    #[test]
    fn sync_normalizes_developer_friendly_host_and_device_paths() {
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([json!({
                "transfer_id": "push-normalized",
                "accepted_offset": 1,
                "state": "complete"
            })]),
        };
        execute(
            &mut caller,
            &TopLevelCommand::Sync(SyncArgs {
                command: SyncCommand::Push {
                    serial: "KT6-TEST".to_owned(),
                    local_path: "relative-source.bin".to_owned(),
                    remote_path: "/mnt/us/kindlebridge-data/apps/payload.bin".to_owned(),
                    block_size: 65_536,
                    resume: None,
                },
            }),
            true,
        )
        .unwrap();

        let params = caller.calls[0].1.as_ref().unwrap();
        let expected_local = std::env::current_dir()
            .unwrap()
            .join("relative-source.bin")
            .to_string_lossy()
            .into_owned();
        assert_eq!(params["local_path"], expected_local);
        assert_eq!(params["remote_path"], "apps/payload.bin");
    }

    #[test]
    fn sync_human_summary_reports_elapsed_time_and_throughput() {
        assert_eq!(
            format_transfer_summary(
                "pushed",
                32 * 1024 * 1024,
                "to",
                "apps/payload.bin",
                "push-test",
                Duration::from_secs(2),
                false,
            ),
            "pushed 33554432 bytes to apps/payload.bin in 2.00 s (16.00 MiB/s) (push-test)"
        );
    }

    #[test]
    fn daemon_stage_never_activates_a_slot_on_the_live_transport() {
        let path = std::env::temp_dir().join(format!(
            "kindlebridge-daemon-stage-test-{}",
            std::process::id()
        ));
        let bytes = b"test kindlehf daemon";
        std::fs::write(&path, bytes).unwrap();
        let digest = blake3::hash(bytes).to_hex().to_string();
        let size = bytes.len() as u64;
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([
                json!({
                    "transfer_id": "push-update",
                    "accepted_offset": size,
                    "state": "complete"
                }),
                json!({
                    "exit_code": 0,
                    "stdout": format!("B\t{digest}\t{size}\n"),
                    "stderr": "",
                    "duration_ms": 5
                }),
            ]),
        };

        let output = execute(
            &mut caller,
            &TopLevelCommand::Daemon(DaemonArgs {
                command: DaemonCommand::Stage {
                    serial: "KT6-TEST".to_owned(),
                    device_binary: path.to_string_lossy().into_owned(),
                },
            }),
            false,
        )
        .unwrap();

        assert!(output.contains("slot B"));
        assert!(output.contains("KUAL"));
        assert_eq!(
            caller
                .calls
                .iter()
                .map(|call| call.0.as_str())
                .collect::<Vec<_>>(),
            [methods::SYNC_PUSH, methods::EXEC_RUN]
        );
        let push = caller.calls[0].1.as_ref().unwrap();
        assert_eq!(
            push["remote_path"],
            format!("staging/daemon/{digest}/kindlebridged")
        );
        let stage = &caller.calls[1].1.as_ref().unwrap()["argv"];
        assert_eq!(stage[0], DEVICE_LAUNCHER);
        assert_eq!(stage[1], "stage");
        assert_eq!(stage[7], digest);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn daemon_stage_requires_an_absolute_local_binary_path() {
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::new(),
        };
        let error = execute(
            &mut caller,
            &TopLevelCommand::Daemon(DaemonArgs {
                command: DaemonCommand::Stage {
                    serial: "KT6-TEST".to_owned(),
                    device_binary: "relative/kindlebridged".to_owned(),
                },
            }),
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("path must be absolute"));
        assert!(caller.calls.is_empty());
    }
}
