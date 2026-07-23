mod shell;

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::thread;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use clap::Parser;
use interprocess::local_socket::{prelude::*, Stream};
use kindlebridge::{
    deploy_project_after_build, execute_with_status, AppCommand, Cli, CliError, CommandOutput,
    RpcCaller, RunArgs, ServerCommand, TopLevelCommand,
};
use kindlebridge_bundle::read_project_manifest;
use kindlebridge_schema::host_rpc::{self, RpcMethod as HostRpcMethod};
use kindlebridge_schema::{
    error_codes, methods, AppLogParams, AppLogSnapshot, AppState, ClientError, RpcClient,
    SyncProgress, SyncProgressPhase,
};
#[cfg(test)]
use kindlebridge_schema::{
    read_json_frame, write_json_frame, RequestId, RpcRequest, RpcResponse,
    DEFAULT_MAX_CONTENT_LENGTH,
};
use serde_json::{json, Value};
use walkdir::WalkDir;

fn main() -> ExitCode {
    let json_requested = std::env::args_os().any(|argument| argument == "--json");
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = error.exit_code();
            if json_requested && exit_code != 0 {
                eprintln!(
                    "{}",
                    json!({
                        "error": {
                            "kind": "arguments",
                            "code": "INVALID_ARGUMENTS",
                            "message": error.to_string(),
                        }
                    })
                );
            } else {
                let _ = error.print();
            }
            return ExitCode::from(u8::try_from(exit_code).unwrap_or(1));
        }
    };
    match run(&cli) {
        Ok(output) => {
            if !output.output.is_empty() {
                println!("{}", output.output);
            }
            process_exit_code(output.exit_code)
        }
        Err(error) => {
            print_error(cli.json, &error);
            ExitCode::FAILURE
        }
    }
}

fn process_exit_code(exit_code: i32) -> ExitCode {
    ExitCode::from(u8::try_from(exit_code).unwrap_or(1))
}

#[derive(Debug)]
enum RunError {
    Command(CliError),
    Arguments(String),
    Startup(String),
    Message(String),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Command(error) => error.fmt(formatter),
            Self::Arguments(message) | Self::Startup(message) | Self::Message(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl From<String> for RunError {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}

fn run(cli: &Cli) -> Result<CommandOutput, RunError> {
    if cli.server_stdio {
        return run_stdio_rpc(cli);
    }
    if let TopLevelCommand::Server(args) = &cli.command {
        if matches!(args.command, ServerCommand::Status | ServerCommand::Stop) {
            let Ok(stream) = kindlebridge_local::connect() else {
                return Ok(CommandOutput {
                    output: "not running".to_owned(),
                    exit_code: 0,
                });
            };
            let output = run_rpc(stream, cli)?;
            if matches!(args.command, ServerCommand::Stop) {
                kindlebridge_local::wait_for_shutdown()
                    .map_err(|error| RunError::Startup(error.to_string()))?;
            }
            return Ok(output);
        }
    }

    if let TopLevelCommand::Shell(args) = &cli.command {
        return shell::run(args, cli.json, || {
            connect_or_start(cli).map_err(|error| error.to_string())
        })
        .map_err(|error| match error {
            shell::Error::Arguments(message) => RunError::Arguments(message),
            shell::Error::Connection(message) => RunError::Startup(message),
            shell::Error::Message(message) => RunError::Message(message),
        });
    }
    if let TopLevelCommand::App(args) = &cli.command {
        if let AppCommand::Log {
            serial,
            app_id,
            follow,
            max_bytes,
        } = &args.command
        {
            if cli.json && *follow {
                return Err(RunError::Arguments(
                    "app log --follow streams raw bytes and does not support --json".to_owned(),
                ));
            }
            if !cli.json {
                return run_app_log(
                    connect_or_start(cli)?,
                    serial.clone(),
                    app_id.clone(),
                    *max_bytes,
                    *follow,
                );
            }
        }
    }
    if let TopLevelCommand::Run(args) = &cli.command {
        if args.watch {
            return run_project_watch(cli, args);
        }
    }
    if matches!(cli.command, TopLevelCommand::Sync(_)) {
        return run_sync(connect_or_start(cli)?, cli);
    }
    run_rpc(connect_or_start(cli)?, cli)
}

fn run_project_watch(cli: &Cli, args: &RunArgs) -> Result<CommandOutput, RunError> {
    if cli.json {
        return Err(RunError::Arguments(
            "run --watch does not support --json yet".to_owned(),
        ));
    }
    let manifest_path = if args.manifest.is_absolute() {
        args.manifest.clone()
    } else {
        std::env::current_dir()
            .map_err(|error| RunError::Message(error.to_string()))?
            .join(&args.manifest)
    };
    let project_root = manifest_path.parent().ok_or_else(|| {
        RunError::Message(format!(
            "manifest has no parent directory: {}",
            manifest_path.display()
        ))
    })?;
    let manifest = read_project_manifest(&manifest_path)
        .map_err(|error| RunError::Message(format!("could not load project: {error}")))?;
    let app_id = manifest.id.clone();
    let development = manifest.development.ok_or_else(|| {
        RunError::Message(format!(
            "{} is missing [development]",
            manifest_path.display()
        ))
    })?;
    if development.watch.is_empty() {
        return Err(RunError::Arguments(
            "[development].watch must list source paths for run --watch".to_owned(),
        ));
    }
    let mut watch_paths = resolve_watch_paths(project_root, &development.watch);
    let mut previous = watch_snapshot(&watch_paths)?;
    previous = run_current_build(
        &development.build,
        Some(&manifest_path),
        project_root,
        &watch_paths,
        previous,
    )?;
    print_run_result(deploy_project_connected(cli, args))?;
    let log_serial = args.serial.clone();
    let log_stream = kindlebridge_local::connect().map_err(|error| {
        RunError::Message(format!("could not follow application logs: {error}"))
    })?;
    thread::spawn(move || {
        if let Err(error) = run_app_log(log_stream, log_serial, app_id, 16 * 1024, true) {
            eprintln!("application log stream stopped: {error}");
        }
    });
    eprintln!(
        "watching {} (press Ctrl-C to stop)",
        watch_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    loop {
        thread::sleep(Duration::from_millis(100));
        let current = watch_snapshot(&watch_paths)?;
        if current == previous {
            continue;
        }
        let mut stable = current;
        loop {
            thread::sleep(Duration::from_millis(250));
            let next = watch_snapshot(&watch_paths)?;
            if next == stable {
                break;
            }
            stable = next;
        }
        eprintln!("change detected; rebuilding...");
        let refreshed = read_project_manifest(&manifest_path)
            .map_err(|error| RunError::Message(format!("could not reload project: {error}")))?;
        let refreshed = refreshed.development.ok_or_else(|| {
            RunError::Message(format!(
                "{} is missing [development]",
                manifest_path.display()
            ))
        })?;
        let refreshed_paths = resolve_watch_paths(project_root, &refreshed.watch);
        if refreshed_paths.is_empty() {
            return Err(RunError::Arguments(
                "[development].watch must list source paths for run --watch".to_owned(),
            ));
        }
        if refreshed_paths != watch_paths {
            watch_paths = refreshed_paths;
            stable = watch_snapshot(&watch_paths)?;
            eprintln!("watch path configuration changed; reloaded project");
        }
        previous = match run_current_build(
            &refreshed.build,
            Some(&manifest_path),
            project_root,
            &watch_paths,
            stable,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                previous = watch_snapshot(&watch_paths)?;
                eprintln!("build failed; waiting for another change: {error}");
                continue;
            }
        };
        match deploy_project_connected(cli, args) {
            Ok(output) => println!("{output}"),
            Err(error) => {
                eprintln!(
                    "redeploy failed; use `kindlebridge app list` to inspect current state: {error}"
                )
            }
        }
    }
}

fn run_app_log(
    stream: Stream,
    serial: String,
    app_id: String,
    max_bytes: u32,
    follow: bool,
) -> Result<CommandOutput, RunError> {
    let (reader, writer) = stream.split();
    let mut client = RpcClient::new(BufReader::new(reader), BufWriter::new(writer));
    let mut run_id = None;
    let mut stdout_cursor = 0;
    let mut stderr_cursor = 0;
    let mut warned_stdout_cap = false;
    let mut warned_stderr_cap = false;
    let mut waiting_for_device = false;
    let mut previous_runtime = None;

    loop {
        let params = serde_json::to_value(AppLogParams {
            serial: serial.clone(),
            app_id: app_id.clone(),
            run_id: run_id.clone(),
            stdout_cursor,
            stderr_cursor,
            max_bytes: Some(max_bytes),
        })
        .map_err(|_| RunError::Message("could not encode app log request".to_owned()))?;
        let value = match client.call(host_rpc::AppLog::METHOD, Some(params)) {
            Ok(value) => {
                if waiting_for_device {
                    eprintln!("--- {app_id} log connection restored ---");
                    waiting_for_device = false;
                }
                value
            }
            Err(error) if follow && is_retryable_app_log_error(&error) => {
                if !waiting_for_device {
                    eprintln!("--- {app_id} is offline; waiting for KindleBridge ---");
                    waiting_for_device = true;
                }
                thread::sleep(Duration::from_millis(250));
                continue;
            }
            Err(error) => {
                return Err(RunError::Message(format!(
                    "app log request failed: {error}"
                )))
            }
        };
        let snapshot: AppLogSnapshot = serde_json::from_value(value)
            .map_err(|_| RunError::Message("server returned invalid app log data".to_owned()))?;
        let restarted = run_id.is_some() && snapshot.reset;
        let runtime_message = follow.then(|| {
            app_runtime_message(
                &app_id,
                previous_runtime.as_ref(),
                &snapshot.state,
                snapshot.pid,
                restarted,
            )
        });
        if snapshot.reset {
            warned_stdout_cap = false;
            warned_stderr_cap = false;
        }
        if restarted {
            eprintln!("\n--- {app_id} restarted ({}) ---", snapshot.run_id);
        }
        run_id = Some(snapshot.run_id);

        let stdout = BASE64
            .decode(&snapshot.stdout.data_base64)
            .map_err(|_| RunError::Message("server returned invalid stdout log data".to_owned()))?;
        let stderr = BASE64
            .decode(&snapshot.stderr.data_base64)
            .map_err(|_| RunError::Message("server returned invalid stderr log data".to_owned()))?;
        if !stdout.is_empty() {
            let mut output = io::stdout().lock();
            output
                .write_all(&stdout)
                .and_then(|()| output.flush())
                .map_err(|error| RunError::Message(format!("could not write stdout: {error}")))?;
        }
        if !stderr.is_empty() {
            let mut output = io::stderr().lock();
            output
                .write_all(&stderr)
                .and_then(|()| output.flush())
                .map_err(|error| RunError::Message(format!("could not write stderr: {error}")))?;
        }
        stdout_cursor = snapshot.stdout.next_cursor;
        stderr_cursor = snapshot.stderr.next_cursor;
        if snapshot.stdout.capped && !warned_stdout_cap {
            eprintln!("\nwarning: {app_id} stdout capture reached its 4 MiB limit");
            warned_stdout_cap = true;
        }
        if snapshot.stderr.capped && !warned_stderr_cap {
            eprintln!("\nwarning: {app_id} stderr capture reached its 4 MiB limit");
            warned_stderr_cap = true;
        }
        if let Some(Some(message)) = runtime_message {
            eprintln!("--- {message} ---");
        }
        previous_runtime = Some((snapshot.state, snapshot.pid));
        if !follow {
            return Ok(CommandOutput {
                output: String::new(),
                exit_code: 0,
            });
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn app_runtime_message(
    app_id: &str,
    previous: Option<&(AppState, Option<u32>)>,
    state: &AppState,
    pid: Option<u32>,
    restarted: bool,
) -> Option<String> {
    let changed = match previous {
        Some(previous) => previous.0 != *state || previous.1 != pid,
        None => true,
    };
    if !changed {
        return None;
    }
    match state {
        AppState::Running if previous.is_none() || restarted => None,
        AppState::Running => Some(pid.map_or_else(
            || format!("{app_id} is running"),
            |pid| format!("{app_id} is running (pid {pid})"),
        )),
        AppState::Stopped if previous.is_none() => Some(format!("{app_id} is stopped")),
        AppState::Stopped => Some(format!("{app_id} exited")),
        AppState::Failed => Some(format!("{app_id} failed")),
    }
}

fn is_retryable_app_log_error(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Rpc(error)
            if matches!(
                error.code,
                error_codes::SERVER_NOT_READY | error_codes::DEVICE_NOT_FOUND
            )
    )
}

fn resolve_watch_paths(project_root: &Path, configured: &[PathBuf]) -> Vec<PathBuf> {
    configured
        .iter()
        .map(|path| {
            if path.is_absolute() {
                path.clone()
            } else {
                project_root.join(path)
            }
        })
        .collect()
}

fn deploy_project_connected(cli: &Cli, args: &RunArgs) -> Result<String, CliError> {
    let stream = connect_or_start(cli).map_err(|error| {
        CliError::Project(format!("could not connect to the host server: {error}"))
    })?;
    let (reader, writer) = stream.split();
    let mut client = RpcClient::new(BufReader::new(reader), BufWriter::new(writer));
    deploy_project_after_build(&mut client, args, false)
}

fn run_current_build(
    build: &[String],
    manifest_path: Option<&Path>,
    project_root: &Path,
    watch_paths: &[PathBuf],
    mut snapshot: BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
) -> Result<BTreeMap<PathBuf, (u64, std::time::SystemTime)>, RunError> {
    let mut current_build = build.to_vec();
    loop {
        let Some((program, arguments)) = current_build.split_first() else {
            return Ok(snapshot);
        };
        let mut command = Command::new(program);
        command.args(arguments).current_dir(project_root);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            command.process_group(0);
        }
        let mut child = command.spawn().map_err(|error| {
            RunError::Command(CliError::Project(format!(
                "could not start build command {program}: {error}"
            )))
        })?;
        loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|error| RunError::Message(format!("could not wait for build: {error}")))?
            {
                if status.success() {
                    return watch_snapshot(watch_paths);
                }
                return Err(RunError::Command(CliError::BuildFailed {
                    exit_code: status.code().unwrap_or(1),
                    detail: String::new(),
                }));
            }
            thread::sleep(Duration::from_millis(100));
            let current = watch_snapshot(watch_paths)?;
            if current == snapshot {
                continue;
            }
            let stable = debounce_watch_paths(watch_paths, current)?;
            terminate_build_tree(&mut child)?;
            eprintln!("source changed during build; cancelled obsolete build");
            snapshot = stable;
            if let Some(manifest_path) = manifest_path {
                let refreshed = read_project_manifest(manifest_path).map_err(|error| {
                    RunError::Message(format!("could not reload project: {error}"))
                })?;
                current_build = refreshed
                    .development
                    .ok_or_else(|| {
                        RunError::Message(format!(
                            "{} is missing [development]",
                            manifest_path.display()
                        ))
                    })?
                    .build;
            }
            break;
        }
    }
}

fn debounce_watch_paths(
    watch_paths: &[PathBuf],
    mut snapshot: BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
) -> Result<BTreeMap<PathBuf, (u64, std::time::SystemTime)>, RunError> {
    loop {
        thread::sleep(Duration::from_millis(250));
        let next = watch_snapshot(watch_paths)?;
        if next == snapshot {
            return Ok(snapshot);
        }
        snapshot = next;
    }
}

fn terminate_build_tree(child: &mut std::process::Child) -> Result<(), RunError> {
    #[cfg(windows)]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &child.id().to_string(), "/T", "/F"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| {
                RunError::Message(format!("could not terminate obsolete build tree: {error}"))
            })?;
        if !status.success() {
            child.kill().map_err(|error| {
                RunError::Message(format!("could not terminate obsolete build: {error}"))
            })?;
        }
    }
    #[cfg(unix)]
    {
        let group = format!("-{}", child.id());
        let _ = Command::new("kill").args(["-TERM", "--", &group]).status();
        for _ in 0..10 {
            if child
                .try_wait()
                .map_err(|error| RunError::Message(format!("could not wait for build: {error}")))?
                .is_some()
            {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
        let _ = Command::new("kill").args(["-KILL", "--", &group]).status();
    }
    child
        .wait()
        .map_err(|error| RunError::Message(format!("could not reap obsolete build: {error}")))?;
    Ok(())
}

fn print_run_result(result: Result<String, CliError>) -> Result<(), RunError> {
    let output = result.map_err(RunError::Command)?;
    if !output.is_empty() {
        println!("{output}");
    }
    Ok(())
}

fn watch_snapshot(
    paths: &[PathBuf],
) -> Result<BTreeMap<PathBuf, (u64, std::time::SystemTime)>, RunError> {
    let mut snapshot = BTreeMap::new();
    for path in paths {
        if path.is_file() {
            add_watch_entry(&mut snapshot, path)?;
            continue;
        }
        if !path.is_dir() {
            return Err(RunError::Message(format!(
                "watch path does not exist: {}",
                path.display()
            )));
        }
        for entry in WalkDir::new(path)
            .follow_links(false)
            .sort_by_file_name()
            .into_iter()
            .filter_entry(|entry| entry.file_name() != ".kindlebridge")
        {
            let entry = entry.map_err(|error| RunError::Message(error.to_string()))?;
            if entry.file_type().is_file() {
                add_watch_entry(&mut snapshot, entry.path())?;
            }
        }
    }
    Ok(snapshot)
}

fn add_watch_entry(
    snapshot: &mut BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
    path: &Path,
) -> Result<(), RunError> {
    let metadata = fs::metadata(path).map_err(|error| {
        RunError::Message(format!("could not inspect {}: {error}", path.display()))
    })?;
    let modified = metadata.modified().map_err(|error| {
        RunError::Message(format!(
            "could not read modification time for {}: {error}",
            path.display()
        ))
    })?;
    snapshot.insert(path.to_owned(), (metadata.len(), modified));
    Ok(())
}

fn run_stdio_rpc(cli: &Cli) -> Result<CommandOutput, RunError> {
    if matches!(cli.command, TopLevelCommand::Shell(_)) {
        return Err(RunError::Arguments(
            "--server-stdio does not support streaming shell; use the shared local server"
                .to_owned(),
        ));
    }
    if matches!(&cli.command, TopLevelCommand::Run(args) if args.watch) {
        return Err(RunError::Arguments(
            "--server-stdio does not support run --watch; use the shared local server".to_owned(),
        ));
    }
    let mut command = Command::new(&cli.server);
    append_device_arguments(&mut command, cli)?;
    command
        .arg("--stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|error| RunError::Startup(format!("could not start {}: {error}", cli.server)))?;
    let reader = child
        .stdout
        .take()
        .ok_or_else(|| RunError::Startup("server stdout was not piped".to_owned()))?;
    let writer = child
        .stdin
        .take()
        .ok_or_else(|| RunError::Startup("server stdin was not piped".to_owned()))?;
    let result = {
        let mut client = RpcClient::new(BufReader::new(reader), writer);
        execute_with_status(&mut client, &cli.command, cli.json).map_err(RunError::Command)
    };
    let status = child
        .wait()
        .map_err(|error| RunError::Startup(format!("could not wait for server: {error}")))?;
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    if !status.success() {
        let detail = stderr.trim();
        return Err(RunError::Startup(if detail.is_empty() {
            format!("server exited with {status}")
        } else {
            format!("server exited with {status}: {detail}")
        }));
    }
    if std::env::var_os("KINDLEBRIDGE_TRACE_SERVER_STDERR").is_some() && !stderr.is_empty() {
        eprint!("{stderr}");
    }
    result
}

fn run_rpc(stream: Stream, cli: &Cli) -> Result<CommandOutput, RunError> {
    let (reader, writer) = stream.split();
    let mut client = RpcClient::new(BufReader::new(reader), BufWriter::new(writer));
    execute_with_status(&mut client, &cli.command, cli.json).map_err(RunError::Command)
}

fn run_sync(stream: Stream, cli: &Cli) -> Result<CommandOutput, RunError> {
    let (reader, writer) = stream.split();
    let client = RpcClient::new(BufReader::new(reader), BufWriter::new(writer));
    let mut caller = SyncProgressCaller::new(client, io::stderr().is_terminal(), !cli.json);
    let result =
        execute_with_status(&mut caller, &cli.command, cli.json).map_err(RunError::Command);
    caller.finish();
    result
}

struct SyncProgressCaller<R, W> {
    client: RpcClient<R, W>,
    display: SyncProgressDisplay,
}

struct SyncProgressDisplay {
    enabled: bool,
    terminal: bool,
    displayed: bool,
    last_operation: Option<String>,
    last_transfer: Option<String>,
    last_phase: Option<SyncProgressPhase>,
    last_bucket: Option<u64>,
}

impl<R: BufRead, W: Write> SyncProgressCaller<R, W> {
    fn new(client: RpcClient<R, W>, terminal: bool, enabled: bool) -> Self {
        Self {
            client,
            display: SyncProgressDisplay {
                enabled,
                terminal,
                displayed: false,
                last_operation: None,
                last_transfer: None,
                last_phase: None,
                last_bucket: None,
            },
        }
    }

    fn finish(&mut self) {
        self.display.finish();
    }
}

impl SyncProgressDisplay {
    fn show(&mut self, progress: &SyncProgress) {
        if !self.enabled {
            return;
        }
        if let Some(transfer_id) = progress
            .transfer_id
            .as_deref()
            .filter(|_| progress.total != 0 && progress.transferred < progress.total)
        {
            if self.last_transfer.as_deref() != Some(transfer_id) {
                if self.terminal && self.displayed {
                    eprintln!();
                }
                eprintln!(
                    "resume token for {}: {transfer_id} (use --resume {transfer_id})",
                    progress.remote_path
                );
                self.displayed = false;
                self.last_transfer = Some(transfer_id.to_owned());
            }
        }
        let bucket = progress
            .transferred
            .saturating_mul(10)
            .checked_div(progress.total)
            .unwrap_or(0);
        let changed_operation = self.last_operation.as_deref() != Some(&progress.operation_id);
        let changed_phase = self.last_phase.as_ref() != Some(&progress.phase);
        let should_print =
            self.terminal || changed_operation || changed_phase || self.last_bucket != Some(bucket);
        if !should_print {
            return;
        }
        let phase = match progress.phase {
            SyncProgressPhase::Hashing => "hashing",
            SyncProgressPhase::Transferring => "transferring",
        };
        let status = match progress
            .transferred
            .saturating_mul(100)
            .checked_div(progress.total)
        {
            Some(percentage) => format!(
                "{} / {} ({percentage}%)",
                format_bytes(progress.transferred),
                format_bytes(progress.total)
            ),
            None => format!("{} transferred", format_bytes(progress.transferred)),
        };
        if self.terminal {
            eprint!("\r{phase} {}: {status}\u{1b}[K", progress.remote_path);
            let _ = io::stderr().flush();
        } else {
            eprintln!("{phase} {}: {status}", progress.remote_path);
        }
        self.displayed = true;
        self.last_operation = Some(progress.operation_id.clone());
        self.last_phase = Some(progress.phase.clone());
        self.last_bucket = Some(bucket);
    }

    fn finish(&mut self) {
        if self.terminal && self.displayed {
            eprintln!();
        }
        self.displayed = false;
    }
}

impl<R: BufRead, W: Write> RpcCaller for SyncProgressCaller<R, W> {
    fn call(&mut self, method: &str, params: Option<Value>) -> Result<Value, ClientError> {
        let method = match method {
            methods::SYNC_PUSH => methods::SYNC_PUSH_STREAM,
            methods::SYNC_PULL => methods::SYNC_PULL_STREAM,
            _ => return self.client.call(method, params),
        };
        let display = &mut self.display;
        self.client
            .call_with_notifications(method, params, |notification| {
                if notification.method != methods::SYNC_PROGRESS {
                    return Err(ClientError::InvalidResponse);
                }
                let progress = notification
                    .params
                    .clone()
                    .ok_or(ClientError::InvalidResponse)
                    .and_then(|value| {
                        serde_json::from_value(value).map_err(|_| ClientError::InvalidResponse)
                    })?;
                display.show(&progress);
                Ok(())
            })
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn connect_or_start(cli: &Cli) -> Result<Stream, RunError> {
    let contract = kindlebridge_local::ServerContract::current();
    kindlebridge_local::acquire(server_command(cli)?, contract, |version| {
        if !cli.json {
            eprintln!(
                "updating local {} {} (API {}) to {} (API {})",
                contract.name,
                version.version,
                version.api_version,
                contract.version,
                contract.api_version
            );
        }
    })
    .map_err(|error| RunError::Startup(error.to_string()))
}

fn server_command(cli: &Cli) -> Result<Command, RunError> {
    let mut command = Command::new(&cli.server);
    append_device_arguments(&mut command, cli)?;
    Ok(command)
}

fn append_device_arguments(command: &mut Command, cli: &Cli) -> Result<(), RunError> {
    if let Some(devices_file) = &cli.devices_file {
        command.arg("--devices-file").arg(devices_file);
    }
    for address in &cli.tcp_device {
        command.arg("--tcp-device").arg(address);
    }
    let automatic_usb = !cli.no_usb && cli.devices_file.is_none() && cli.tcp_device.is_empty();
    if automatic_usb {
        command.arg("--usb");
        if let Some(serial) = &cli.usb_serial {
            command.arg("--usb-serial").arg(serial);
        }
    } else if cli.usb_serial.is_some() {
        return Err(RunError::Arguments(
            "--usb-serial requires automatic USB mode (remove --no-usb/--tcp-device)".to_owned(),
        ));
    }
    Ok(())
}

fn print_error(json_output: bool, error: &RunError) {
    if json_output {
        eprintln!("{}", json_error(error));
    } else {
        eprintln!("kindlebridge: {error}");
    }
}

fn json_error(error: &RunError) -> Value {
    match error {
        RunError::Command(CliError::Rpc(ClientError::Rpc(error))) => json!({
            "error": {
                "kind": "rpc",
                "code": error.code,
                "message": error.message,
                "data": error.data,
            }
        }),
        RunError::Command(CliError::Rpc(error)) => json!({
            "error": {
                "kind": "transport",
                "code": "RPC_TRANSPORT_ERROR",
                "message": error.to_string(),
            }
        }),
        RunError::Command(error) => json!({
            "error": {
                "kind": "command",
                "code": command_error_code(error),
                "message": error.to_string(),
            }
        }),
        RunError::Arguments(message) => json!({
            "error": {
                "kind": "arguments",
                "code": "INVALID_ARGUMENTS",
                "message": message,
            }
        }),
        RunError::Startup(message) => json!({
            "error": {
                "kind": "startup",
                "code": "SERVER_START_FAILED",
                "message": message,
            }
        }),
        RunError::Message(message) => json!({
            "error": {
                "kind": "cli",
                "code": "COMMAND_FAILED",
                "message": message,
            }
        }),
    }
}

fn command_error_code(error: &CliError) -> &'static str {
    match error {
        CliError::Rpc(_) => "RPC_TRANSPORT_ERROR",
        CliError::InvalidResult { .. } => "INVALID_SERVER_RESULT",
        CliError::InvalidBlockSize => "INVALID_BLOCK_SIZE",
        CliError::InvalidRemotePath { .. } => "INVALID_REMOTE_PATH",
        CliError::RemotePathCollision { .. } => "INVALID_REMOTE_PATH",
        CliError::InvalidDeviceSyncPath { .. } | CliError::DevicePathCollision { .. } => {
            "INVALID_SERVER_RESULT"
        }
        CliError::RemoteTreeChanged(_) => "REMOTE_TREE_CHANGED",
        CliError::RemoteTreeTooLarge(_) => "INVALID_SERVER_RESULT",
        CliError::CurrentDirectory(_) => "HOST_IO_ERROR",
        CliError::InvalidUpdateBinary(_) => "INVALID_UPDATE_BINARY",
        CliError::StreamingShellRequired => "STREAMING_SHELL_REQUIRED",
        CliError::UpdateRejected { .. } => "UPDATE_REJECTED",
        CliError::Project(_) => "INVALID_PROJECT",
        CliError::BuildFailed { .. } => "BUILD_FAILED",
        CliError::DirectoryResumeUnsupported => "INVALID_RESUME",
        CliError::LocalTree(_) => "HOST_IO_ERROR",
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn sync_cli_upgrades_file_transfers_to_the_progress_rpc() {
        let mut input = Vec::new();
        write_json_frame(
            &mut input,
            &RpcRequest::notification(
                methods::SYNC_PROGRESS,
                Some(json!({
                    "operation_id": "op",
                    "direction": "push",
                    "remote_path": "payload.bin",
                    "phase": "transferring",
                    "transferred": 7,
                    "total": 7
                })),
            ),
        )
        .unwrap();
        write_json_frame(
            &mut input,
            &RpcResponse::success(RequestId::Number(1), json!({ "accepted_offset": 7 })),
        )
        .unwrap();
        let client = RpcClient::new(BufReader::new(Cursor::new(input)), Vec::new());
        let mut caller = SyncProgressCaller::new(client, false, false);

        let result = caller
            .call(methods::SYNC_PUSH, Some(json!({ "file": "parameters" })))
            .unwrap();

        assert_eq!(result["accepted_offset"], 7);
        let (_, output) = caller.client.into_parts();
        let request: RpcRequest = serde_json::from_value(
            read_json_frame(
                &mut BufReader::new(Cursor::new(output)),
                DEFAULT_MAX_CONTENT_LENGTH,
            )
            .unwrap()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(request.method, methods::SYNC_PUSH_STREAM);
    }

    #[test]
    fn watch_snapshot_detects_file_changes_additions_and_removals() {
        let root = std::env::temp_dir().join(format!(
            "kindlebridge-watch-snapshot-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let first = root.join("first.txt");
        fs::write(&first, b"a").unwrap();
        let initial = watch_snapshot(std::slice::from_ref(&root)).unwrap();

        fs::create_dir_all(root.join(".kindlebridge")).unwrap();
        fs::write(root.join(".kindlebridge/run.kbb"), b"generated").unwrap();
        assert_eq!(
            watch_snapshot(std::slice::from_ref(&root)).unwrap(),
            initial
        );

        fs::write(&first, b"longer").unwrap();
        let changed = watch_snapshot(std::slice::from_ref(&root)).unwrap();
        assert_ne!(changed, initial);

        let second = root.join("second.txt");
        fs::write(&second, b"new").unwrap();
        let added = watch_snapshot(std::slice::from_ref(&root)).unwrap();
        assert_ne!(added, changed);

        fs::remove_file(first).unwrap();
        let removed = watch_snapshot(std::slice::from_ref(&root)).unwrap();
        assert_ne!(removed, added);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn watch_cancels_an_obsolete_build_and_runs_the_latest_source() {
        let root =
            std::env::temp_dir().join(format!("kindlebridge-watch-cancel-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let source = root.join("source.txt");
        fs::write(&source, b"old").unwrap();
        let marker = root.join("marker.txt");
        let build = if cfg!(windows) {
            vec![
                "powershell.exe".to_owned(),
                "-NoProfile".to_owned(),
                "-Command".to_owned(),
                "if ((Get-Content source.txt) -eq 'old') { Start-Sleep -Seconds 5 }; Set-Content marker.txt done".to_owned(),
            ]
        } else {
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "if [ \"$(cat source.txt)\" = old ]; then sleep 5; fi; printf done > marker.txt"
                    .to_owned(),
            ]
        };
        let paths = vec![source.clone()];
        let initial = watch_snapshot(&paths).unwrap();
        let source_for_thread = source.clone();
        let editor = thread::spawn(move || {
            thread::sleep(Duration::from_millis(300));
            fs::write(source_for_thread, b"newer source").unwrap();
        });
        let started = Instant::now();
        let final_snapshot = run_current_build(&build, None, &root, &paths, initial).unwrap();
        editor.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(4));
        assert_eq!(fs::read_to_string(marker).unwrap().trim(), "done");
        assert_eq!(final_snapshot, watch_snapshot(&paths).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn app_log_follow_retries_only_transient_device_link_errors() {
        for code in [error_codes::SERVER_NOT_READY, error_codes::DEVICE_NOT_FOUND] {
            assert!(is_retryable_app_log_error(&ClientError::Rpc(
                kindlebridge_schema::RpcError::new(code, "offline")
            )));
        }
        assert!(!is_retryable_app_log_error(&ClientError::Rpc(
            kindlebridge_schema::RpcError::feature_unavailable("KT6-TEST", "app.log.v2")
        )));
        assert!(!is_retryable_app_log_error(&ClientError::InvalidResponse));
    }

    #[test]
    fn app_log_follow_reports_terminal_state_without_duplicating_restart_markers() {
        assert_eq!(
            app_runtime_message("reader", None, &AppState::Running, Some(10), false),
            None
        );
        assert_eq!(
            app_runtime_message(
                "reader",
                Some(&(AppState::Running, Some(10))),
                &AppState::Failed,
                None,
                false,
            )
            .as_deref(),
            Some("reader failed")
        );
        assert_eq!(
            app_runtime_message(
                "reader",
                Some(&(AppState::Running, Some(10))),
                &AppState::Stopped,
                None,
                false,
            )
            .as_deref(),
            Some("reader exited")
        );
        assert_eq!(
            app_runtime_message(
                "reader",
                Some(&(AppState::Running, Some(10))),
                &AppState::Running,
                Some(11),
                true,
            ),
            None
        );
    }
}
