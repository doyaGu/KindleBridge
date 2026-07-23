//! Command implementation for the `KindleBridge` CLI.

mod app;
mod development;
mod sync;

pub use app::{AppArgs, AppCommand};
pub use development::{deploy_project_after_build, run_project_once, RunArgs};
pub use sync::{SyncArgs, SyncCommand};

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, Read, Write};
use std::path::Path;
#[cfg(test)]
use std::{fs, path::PathBuf};

#[cfg(test)]
use base64::engine::general_purpose::STANDARD as BASE64;
#[cfg(test)]
use base64::Engine;
use clap::{Args, Parser, Subcommand};
use kindlebridge_schema::host_rpc::{self, RpcMethod as HostRpcMethod};
#[cfg(test)]
use kindlebridge_schema::{methods, DeviceList};
use kindlebridge_schema::{
    ClientError, DeviceState, ExecParams, ExecResult, LogSnapshot, LogTailParams, ProcessList,
    ProcessSignalParams, ProcessSummary, RpcClient, SerialParams, SyncPushParams, SyncPushResult,
    TransferState, DEFAULT_SYNC_BLOCK_SIZE,
};
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
    /// Transfer files and directory trees with resumable block checksums.
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
    #[error(
        "directory sync cannot use one --resume ID; rerun the directory command without --resume"
    )]
    DirectoryResumeUnsupported,
    #[error("could not sync local directory: {0}")]
    LocalTree(String),
}

const DEVICE_LAUNCHER: &str = "/var/local/kindlebridge/control/bin/kindlebridge-launcher";
const DEVICE_RUNTIME_ROOT: &str = "/var/local/kindlebridge/control/runtime";
const DEVICE_SYNC_ROOT: &str = "/mnt/us/kindlebridge-data";
const MAX_UPDATE_BINARY_SIZE: u64 = 32 * 1024 * 1024;
const MAX_SYNC_TREE_ENTRIES: usize = 100_000;

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
                let (result, ping) = call_empty::<_, host_rpc::ServerPing>(caller, "server ping")?;
                require_ping(ping.ok)?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok("pong".to_owned())
                }
            }
            ServerCommand::Version => {
                let (result, version) =
                    call_empty::<_, host_rpc::ServerVersion>(caller, "version")?;
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
                let (result, status) =
                    call_empty::<_, host_rpc::ServerStatus>(caller, "server status")?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok(format!("running (pid {})", status.pid))
                }
            }
            ServerCommand::Stop => {
                let (result, _) = call_empty::<_, host_rpc::ServerStop>(caller, "server stop")?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok("stopping".to_owned())
                }
            }
        },
        TopLevelCommand::Device(args) => match &args.command {
            DeviceCommand::List => {
                let (result, list) = call_empty::<_, host_rpc::DeviceList>(caller, "device list")?;
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
                let (result, ping) = call_method::<_, host_rpc::DevicePing>(
                    caller,
                    &SerialParams {
                        serial: serial.clone(),
                    },
                    "device ping",
                )?;
                require_ping(ping.ok)?;
                if json_output {
                    pretty_json(&result)
                } else {
                    Ok("pong".to_owned())
                }
            }
            DeviceCommand::Features { serial } => {
                let (result, features) = call_method::<_, host_rpc::DeviceFeatures>(
                    caller,
                    &kindlebridge_schema::DeviceFeaturesParams {
                        serial: serial.clone(),
                    },
                    "device features",
                )?;
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
        TopLevelCommand::Sync(args) => sync::execute(caller, &args.command, json_output),
        TopLevelCommand::Daemon(args) => execute_daemon(caller, &args.command, json_output),
        TopLevelCommand::App(args) => app::execute(caller, &args.command, json_output),
        TopLevelCommand::Process(args) => execute_process(caller, &args.command, json_output),
        TopLevelCommand::Log(args) => execute_log(caller, &args.command, json_output),
        TopLevelCommand::Run(args) => run_project_once(caller, args, json_output),
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
    let (_, pushed): (_, SyncPushResult) = call_method::<_, host_rpc::SyncPush>(
        caller,
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
    let (_, result): (_, ExecResult) = call_method::<_, host_rpc::ExecRun>(
        caller,
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
    let (result, exec) = call_method::<_, host_rpc::ExecRun>(
        caller,
        &ExecParams {
            serial: serial.to_owned(),
            argv,
            cwd: None,
            environment: BTreeMap::new(),
            timeout_ms,
        },
        "exec",
    )?;
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

fn execute_process<C: RpcCaller>(
    caller: &mut C,
    command: &ProcessCommand,
    json_output: bool,
) -> Result<String, CliError> {
    match command {
        ProcessCommand::List { serial } => {
            let (value, list): (_, ProcessList) = call_method::<_, host_rpc::ProcessList>(
                caller,
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
            let (value, process): (_, ProcessSummary) = call_method::<_, host_rpc::ProcessSignal>(
                caller,
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
            let (value, snapshot): (_, LogSnapshot) = call_method::<_, host_rpc::LogTail>(
                caller,
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

fn call_method<C: RpcCaller, M: HostRpcMethod>(
    caller: &mut C,
    params: &M::Params,
    kind: &'static str,
) -> Result<(Value, M::Result), CliError> {
    let params = serde_json::to_value(params).map_err(|_| CliError::InvalidResult { kind })?;
    let value = caller.call(M::METHOD, Some(params))?;
    let typed =
        serde_json::from_value(value.clone()).map_err(|_| CliError::InvalidResult { kind })?;
    Ok((value, typed))
}

fn call_empty<C: RpcCaller, M>(
    caller: &mut C,
    kind: &'static str,
) -> Result<(Value, M::Result), CliError>
where
    M: HostRpcMethod<Params = kindlebridge_schema::EmptyParams>,
{
    let value = caller.call(M::METHOD, None)?;
    let typed =
        serde_json::from_value(value.clone()).map_err(|_| CliError::InvalidResult { kind })?;
    Ok((value, typed))
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

fn require_ping(ok: bool) -> Result<(), CliError> {
    if ok {
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
    fn directory_push_creates_empty_directories_and_pushes_files_in_stable_order() {
        let root =
            std::env::temp_dir().join(format!("kindlebridge-cli-tree-push-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("empty")).unwrap();
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(root.join("a.txt"), b"a").unwrap();
        fs::write(root.join("nested/b.txt"), b"bb").unwrap();
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([
                json!({"remote_path":"tree","created":true}),
                json!({"remote_path":"tree/empty","created":true}),
                json!({"remote_path":"tree/nested","created":true}),
                json!({"transfer_id":"push-a","accepted_offset":1,"state":"complete"}),
                json!({"transfer_id":"push-b","accepted_offset":2,"state":"complete"}),
            ]),
        };
        let output = execute(
            &mut caller,
            &TopLevelCommand::Sync(SyncArgs {
                command: SyncCommand::Push {
                    serial: "KT6-TEST".to_owned(),
                    local_path: root.to_string_lossy().into_owned(),
                    remote_path: "tree".to_owned(),
                    block_size: 65_536,
                    resume: None,
                },
            }),
            true,
        )
        .unwrap();
        let result: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(result["files"], 2);
        assert_eq!(result["directories"], 3);
        assert_eq!(
            caller
                .calls
                .iter()
                .map(|call| call.0.as_str())
                .collect::<Vec<_>>(),
            [
                methods::SYNC_MKDIR,
                methods::SYNC_MKDIR,
                methods::SYNC_MKDIR,
                methods::SYNC_PUSH,
                methods::SYNC_PUSH,
            ]
        );
        assert_eq!(
            caller.calls[3].1.as_ref().unwrap()["remote_path"],
            "tree/a.txt"
        );
        assert_eq!(
            caller.calls[4].1.as_ref().unwrap()["remote_path"],
            "tree/nested/b.txt"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recursive_pull_materializes_files_and_empty_directories() {
        let root =
            std::env::temp_dir().join(format!("kindlebridge-cli-tree-pull-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let mut caller = RecordingCaller {
            calls: Vec::new(),
            replies: VecDeque::from([
                json!({
                    "remote_path":"tree",
                    "entries":[
                        {"name":"a.txt","kind":"file","size":1},
                        {"name":"empty","kind":"directory","size":0}
                    ]
                }),
                json!({
                    "transfer_id":"pull-a",
                    "total_size":1,
                    "received_size":1,
                    "state":"complete"
                }),
                json!({"remote_path":"tree/empty","entries":[]}),
            ]),
        };
        let output = execute(
            &mut caller,
            &TopLevelCommand::Sync(SyncArgs {
                command: SyncCommand::Pull {
                    serial: "KT6-TEST".to_owned(),
                    remote_path: "tree".to_owned(),
                    local_path: root.to_string_lossy().into_owned(),
                    block_size: 65_536,
                    resume: None,
                    recursive: true,
                },
            }),
            true,
        )
        .unwrap();
        let result: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(result["files"], 1);
        assert!(root.join("empty").is_dir());
        assert_eq!(
            caller
                .calls
                .iter()
                .map(|call| call.0.as_str())
                .collect::<Vec<_>>(),
            [methods::SYNC_LIST, methods::SYNC_PULL, methods::SYNC_LIST]
        );
        fs::remove_dir_all(root).unwrap();
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
