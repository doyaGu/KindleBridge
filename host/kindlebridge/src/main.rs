use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use clap::Parser;
use crossterm::event::{
    self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers,
};
use crossterm::{execute, terminal};
use interprocess::local_socket::{
    prelude::*, GenericFilePath, GenericNamespaced, SendHalf, Stream,
};
use kindlebridge::{
    deploy_project_after_build, execute_with_status, AppCommand, Cli, CliError, CommandOutput,
    RunArgs, ServerCommand, ShellArgs, TopLevelCommand,
};
use kindlebridge_bundle::read_project_manifest;
use kindlebridge_schema::device_protocol::{ShellMode, ShellOpen, TerminalSize, SHELL_V2_FEATURE};
use kindlebridge_schema::{
    methods, read_json_frame, write_json_frame, AppLogParams, AppLogSnapshot, ClientError,
    DeviceFeatures, RequestId, RpcClient, RpcRequest, RpcResponse, ShellOpenParams,
    ShellOpenResult, StreamChannel, StreamClosedParams, StreamCreditParams, StreamDataParams,
    StreamExitParams, StreamIdParams, StreamResizeParams, StreamWriteParams,
    DEFAULT_MAX_CONTENT_LENGTH,
};
use serde::Serialize;
use serde_json::{json, Value};
use walkdir::WalkDir;

const SERVER_START_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_POLL_INTERVAL: Duration = Duration::from_millis(25);
const SERVER_ENDPOINT_QUIET_PERIOD: Duration = Duration::from_millis(100);
const INPUT_POLL: Duration = Duration::from_millis(50);
const MAX_INPUT_PACKET: usize =
    kindlebridge_schema::shell_protocol::USB_ALIGNED_SHELL_PACKET_PAYLOAD;

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
            let Ok(stream) = connect_local() else {
                return Ok(CommandOutput {
                    output: "not running".to_owned(),
                    exit_code: 0,
                });
            };
            let output = run_rpc(stream, cli)?;
            if matches!(args.command, ServerCommand::Stop) {
                wait_for_local_server_shutdown()?;
            }
            return Ok(output);
        }
    }

    if let TopLevelCommand::Shell(args) = &cli.command {
        return run_shell(cli, args);
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
    let log_stream = connect_local().map_err(|error| {
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

    loop {
        let value = client
            .call(
                methods::APP_LOG,
                Some(
                    serde_json::to_value(AppLogParams {
                        serial: serial.clone(),
                        app_id: app_id.clone(),
                        run_id: run_id.clone(),
                        stdout_cursor,
                        stderr_cursor,
                        max_bytes: Some(max_bytes),
                    })
                    .map_err(|_| {
                        RunError::Message("could not encode app log request".to_owned())
                    })?,
                ),
            )
            .map_err(|error| RunError::Message(format!("app log request failed: {error}")))?;
        let snapshot: AppLogSnapshot = serde_json::from_value(value)
            .map_err(|_| RunError::Message("server returned invalid app log data".to_owned()))?;
        let restarted = run_id.is_some() && snapshot.reset;
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
        if !follow {
            return Ok(CommandOutput {
                output: String::new(),
                exit_code: 0,
            });
        }
        thread::sleep(Duration::from_millis(100));
    }
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

fn run_shell(cli: &Cli, args: &ShellArgs) -> Result<CommandOutput, RunError> {
    let features = device_features(connect_or_start(cli)?, &args.serial)?;
    if !features
        .features
        .iter()
        .any(|feature| feature == SHELL_V2_FEATURE)
    {
        return Err(RunError::Message(
            "incompatible device daemon: shell.v2 is required; install the matching KindleBridge package"
                .to_owned(),
        ));
    }
    if cli.json {
        return Err(RunError::Arguments(
            "streaming shell does not support --json; use --ndjson or exec".to_owned(),
        ));
    }
    run_shell_v2(connect_or_start(cli)?, args)
}

fn device_features(stream: Stream, serial: &str) -> Result<DeviceFeatures, RunError> {
    let (reader, writer) = stream.split();
    let mut client = RpcClient::new(BufReader::new(reader), BufWriter::new(writer));
    let value = client
        .call(methods::DEVICE_FEATURES, Some(json!({ "serial": serial })))
        .map_err(|error| RunError::Message(error.to_string()))?;
    serde_json::from_value(value)
        .map_err(|_| RunError::Message("server returned invalid device features".to_owned()))
}

fn run_shell_v2(stream: Stream, args: &ShellArgs) -> Result<CommandOutput, RunError> {
    let stdin_is_terminal = io::stdin().is_terminal();
    let pty = !args.no_tty && (stdin_is_terminal || args.tty >= 2);
    let mode = if pty { ShellMode::Pty } else { ShellMode::Raw };
    let terminal_size = pty.then(current_terminal_size).transpose()?;
    let argv = match &args.command {
        Some(command) => vec!["/bin/sh".to_owned(), "-lc".to_owned(), command.clone()],
        None => vec!["/bin/sh".to_owned(), "-l".to_owned()],
    };
    let open = ShellOpen {
        mode,
        argv,
        terminal_size,
        cwd: "/tmp/root".to_owned(),
        term: "linux".to_owned(),
    };
    let (reader, writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(Mutex::new(BufWriter::new(writer)));
    send_message(
        &writer,
        &RpcRequest::call(
            RequestId::Number(1),
            methods::SHELL_OPEN,
            Some(
                serde_json::to_value(ShellOpenParams {
                    serial: args.serial.clone(),
                    open,
                })
                .map_err(|error| RunError::Message(error.to_string()))?,
            ),
        ),
    )?;
    let open_result = read_open_response(&mut reader)?;

    let escape = parse_escape(&args.escape)?;
    let stopped = Arc::new(AtomicBool::new(false));
    let input_credit = Arc::new(InputCredit::new(open_result.send_credit));
    let _terminal = RawTerminalGuard::new(pty && stdin_is_terminal)?;
    start_input(InputTask {
        writer: Arc::clone(&writer),
        stream_id: open_result.stream_id.clone(),
        mode,
        stdin_is_terminal,
        no_stdin: args.no_stdin,
        escape,
        stopped: Arc::clone(&stopped),
        input_credit: Arc::clone(&input_credit),
    });

    let mut exit_status = None;
    let closed_by_escape = loop {
        let value = read_json_frame(&mut reader, DEFAULT_MAX_CONTENT_LENGTH)
            .map_err(|error| RunError::Message(error.to_string()))?
            .ok_or_else(|| RunError::Message("local server closed the shell stream".to_owned()))?;
        if args.ndjson {
            println!(
                "{}",
                serde_json::to_string(&value)
                    .map_err(|error| RunError::Message(error.to_string()))?
            );
            io::stdout()
                .flush()
                .map_err(|error| RunError::Message(error.to_string()))?;
        }
        let notification: RpcRequest = serde_json::from_value(value)
            .map_err(|_| RunError::Message("invalid stream notification".to_owned()))?;
        let params = notification.params.unwrap_or(Value::Null);
        match notification.method.as_str() {
            methods::STREAM_DATA => {
                let params: StreamDataParams = serde_json::from_value(params)
                    .map_err(|_| RunError::Message("invalid stream data event".to_owned()))?;
                if params.stream_id != open_result.stream_id {
                    continue;
                }
                let data = BASE64
                    .decode(params.data)
                    .map_err(|_| RunError::Message("invalid base64 stream data".to_owned()))?;
                if !args.ndjson {
                    match params.channel {
                        StreamChannel::Stdout => io::stdout()
                            .write_all(&data)
                            .and_then(|()| io::stdout().flush()),
                        StreamChannel::Stderr => io::stderr()
                            .write_all(&data)
                            .and_then(|()| io::stderr().flush()),
                    }
                    .map_err(|error| RunError::Message(error.to_string()))?;
                }
            }
            methods::STREAM_EXIT => {
                let params: StreamExitParams = serde_json::from_value(params)
                    .map_err(|_| RunError::Message("invalid stream exit event".to_owned()))?;
                if params.stream_id == open_result.stream_id {
                    exit_status = Some(if params.signal == 0 {
                        params.exit_code
                    } else {
                        i32::try_from(128_u32.saturating_add(params.signal)).unwrap_or(255)
                    });
                }
            }
            methods::STREAM_CLOSED => {
                let params: StreamClosedParams = serde_json::from_value(params)
                    .map_err(|_| RunError::Message("invalid stream closed event".to_owned()))?;
                if params.stream_id == open_result.stream_id {
                    break params.reason.as_deref() == Some("closed by client");
                }
            }
            methods::STREAM_CREDIT => {
                let params: StreamCreditParams = serde_json::from_value(params)
                    .map_err(|_| RunError::Message("invalid stream credit event".to_owned()))?;
                if params.stream_id == open_result.stream_id {
                    input_credit.restore(params.bytes);
                }
            }
            _ => {}
        }
    };
    stopped.store(true, Ordering::Release);
    input_credit.stop();
    if exit_status.is_none() && !closed_by_escape {
        return Err(RunError::Message(
            "shell connection closed before an exit status was received".to_owned(),
        ));
    }
    Ok(CommandOutput {
        output: String::new(),
        exit_code: exit_status.unwrap_or(0),
    })
}

fn read_open_response<R: BufRead>(reader: &mut R) -> Result<ShellOpenResult, RunError> {
    let value = read_json_frame(reader, DEFAULT_MAX_CONTENT_LENGTH)
        .map_err(|error| RunError::Message(error.to_string()))?
        .ok_or_else(|| RunError::Message("local server closed during shell open".to_owned()))?;
    let response: RpcResponse = serde_json::from_value(value)
        .map_err(|_| RunError::Message("invalid shell open response".to_owned()))?;
    let value = response
        .into_result()
        .map_err(|error| RunError::Message(error.to_string()))?;
    serde_json::from_value(value)
        .map_err(|_| RunError::Message("invalid shell open result".to_owned()))
}

struct InputTask {
    writer: Arc<Mutex<BufWriter<SendHalf>>>,
    stream_id: String,
    mode: ShellMode,
    stdin_is_terminal: bool,
    no_stdin: bool,
    escape: Option<u8>,
    stopped: Arc<AtomicBool>,
    input_credit: Arc<InputCredit>,
}

fn start_input(task: InputTask) {
    thread::Builder::new()
        .name("kindlebridge-shell-input".to_owned())
        .spawn(move || {
            if task.no_stdin {
                let _ = send_notification(
                    &task.writer,
                    methods::STREAM_CLOSE_INPUT,
                    &StreamIdParams {
                        stream_id: task.stream_id,
                    },
                );
                return;
            }
            if task.mode == ShellMode::Pty && task.stdin_is_terminal {
                run_terminal_input(
                    &task.writer,
                    &task.stream_id,
                    task.escape,
                    &task.stopped,
                    &task.input_credit,
                );
            } else {
                run_stream_input(
                    &task.writer,
                    &task.stream_id,
                    &task.stopped,
                    &task.input_credit,
                );
            }
        })
        .expect("could not start shell input worker");
}

fn run_terminal_input(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    stream_id: &str,
    escape: Option<u8>,
    stopped: &AtomicBool,
    input_credit: &InputCredit,
) {
    let mut filter = EscapeFilter::new(escape);
    while !stopped.load(Ordering::Acquire) {
        match event::poll(INPUT_POLL) {
            Ok(false) => continue,
            Ok(true) => {}
            Err(_) => break,
        }
        let Ok(event) = event::read() else { break };
        match event {
            Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
                let bytes = encode_key(key);
                let filtered = filter.push(&bytes);
                if !filtered.data.is_empty()
                    && send_input(writer, stream_id, &filtered.data, input_credit).is_err()
                {
                    break;
                }
                if filtered.close {
                    let _ = send_notification(
                        writer,
                        methods::STREAM_CLOSE,
                        &StreamIdParams {
                            stream_id: stream_id.to_owned(),
                        },
                    );
                    break;
                }
            }
            Event::Paste(data) => {
                let filtered = filter.push(data.as_bytes());
                if !filtered.data.is_empty() {
                    let _ = send_input(writer, stream_id, &filtered.data, input_credit);
                }
                if filtered.close {
                    let _ = send_notification(
                        writer,
                        methods::STREAM_CLOSE,
                        &StreamIdParams {
                            stream_id: stream_id.to_owned(),
                        },
                    );
                    break;
                }
            }
            Event::Resize(columns, rows) => {
                let _ = send_notification(
                    writer,
                    methods::STREAM_RESIZE,
                    &StreamResizeParams {
                        stream_id: stream_id.to_owned(),
                        size: TerminalSize {
                            rows: rows.max(1),
                            columns: columns.max(1),
                            pixel_width: 0,
                            pixel_height: 0,
                        },
                    },
                );
            }
            _ => {}
        }
    }
}

fn run_stream_input(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    stream_id: &str,
    stopped: &AtomicBool,
    input_credit: &InputCredit,
) {
    let mut stdin = io::stdin();
    let mut buffer = [0_u8; MAX_INPUT_PACKET];
    while !stopped.load(Ordering::Acquire) {
        match stdin.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                if send_input(writer, stream_id, &buffer[..count], input_credit).is_err() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
    let _ = send_notification(
        writer,
        methods::STREAM_CLOSE_INPUT,
        &StreamIdParams {
            stream_id: stream_id.to_owned(),
        },
    );
}

fn send_input(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    stream_id: &str,
    data: &[u8],
    input_credit: &InputCredit,
) -> Result<(), RunError> {
    for chunk in data.chunks(MAX_INPUT_PACKET) {
        if !input_credit.take(u32::try_from(chunk.len()).unwrap_or(u32::MAX)) {
            return Err(RunError::Message(
                "shell input stream was closed".to_owned(),
            ));
        }
        if let Err(error) = send_notification(
            writer,
            methods::STREAM_WRITE,
            &StreamWriteParams {
                stream_id: stream_id.to_owned(),
                data: BASE64.encode(chunk),
            },
        ) {
            input_credit.restore(u32::try_from(chunk.len()).unwrap_or(0));
            return Err(error);
        }
    }
    Ok(())
}

struct InputCredit {
    maximum: u32,
    state: Mutex<InputCreditState>,
    available: Condvar,
}

struct InputCreditState {
    bytes: u32,
    stopped: bool,
}

impl InputCredit {
    fn new(initial: u32) -> Self {
        Self {
            maximum: initial,
            state: Mutex::new(InputCreditState {
                bytes: initial,
                stopped: false,
            }),
            available: Condvar::new(),
        }
    }

    fn take(&self, bytes: u32) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        while state.bytes < bytes && !state.stopped {
            let Ok(next) = self.available.wait(state) else {
                return false;
            };
            state = next;
        }
        if state.stopped {
            return false;
        }
        state.bytes -= bytes;
        true
    }

    fn restore(&self, bytes: u32) {
        if let Ok(mut state) = self.state.lock() {
            state.bytes = state.bytes.saturating_add(bytes).min(self.maximum);
            self.available.notify_all();
        }
    }

    fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.stopped = true;
            self.available.notify_all();
        }
    }
}

fn send_notification<T: Serialize>(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    method: &str,
    params: &T,
) -> Result<(), RunError> {
    let params =
        serde_json::to_value(params).map_err(|error| RunError::Message(error.to_string()))?;
    send_message(writer, &RpcRequest::notification(method, Some(params)))
}

fn send_message<T: Serialize>(
    writer: &Arc<Mutex<BufWriter<SendHalf>>>,
    value: &T,
) -> Result<(), RunError> {
    let mut writer = writer
        .lock()
        .map_err(|_| RunError::Message("local RPC writer is unavailable".to_owned()))?;
    write_json_frame(&mut *writer, value).map_err(|error| RunError::Message(error.to_string()))
}

fn current_terminal_size() -> Result<TerminalSize, RunError> {
    let (columns, rows) = terminal::size()
        .map_err(|error| RunError::Message(format!("could not read terminal size: {error}")))?;
    Ok(TerminalSize {
        rows: rows.max(1),
        columns: columns.max(1),
        pixel_width: 0,
        pixel_height: 0,
    })
}

struct RawTerminalGuard {
    enabled: bool,
}

impl RawTerminalGuard {
    fn new(enabled: bool) -> Result<Self, RunError> {
        if enabled {
            terminal::enable_raw_mode().map_err(|error| {
                RunError::Message(format!("could not enable terminal raw mode: {error}"))
            })?;
            if let Err(error) = execute!(io::stdout(), EnableBracketedPaste) {
                let _ = terminal::disable_raw_mode();
                return Err(RunError::Message(format!(
                    "could not enable terminal paste handling: {error}"
                )));
            }
        }
        Ok(Self { enabled })
    }
}

impl Drop for RawTerminalGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = execute!(io::stdout(), DisableBracketedPaste);
            let _ = terminal::disable_raw_mode();
        }
    }
}

fn encode_key(key: KeyEvent) -> Vec<u8> {
    let mut bytes = Vec::new();
    if key.modifiers.contains(KeyModifiers::ALT) {
        bytes.push(0x1b);
    }
    match key.code {
        KeyCode::Char(character) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let value = character as u32;
            if value <= 0x7f {
                let byte = value as u8;
                bytes.push(if byte == b'?' { 0x7f } else { byte & 0x1f });
            }
        }
        KeyCode::Char(character) => {
            let mut encoded = [0_u8; 4];
            bytes.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
        }
        KeyCode::Enter => bytes.push(b'\r'),
        KeyCode::Tab => bytes.push(b'\t'),
        KeyCode::BackTab => bytes.extend_from_slice(b"\x1b[Z"),
        KeyCode::Backspace => bytes.push(0x7f),
        KeyCode::Esc => bytes.push(0x1b),
        KeyCode::Up => bytes.extend_from_slice(b"\x1b[A"),
        KeyCode::Down => bytes.extend_from_slice(b"\x1b[B"),
        KeyCode::Right => bytes.extend_from_slice(b"\x1b[C"),
        KeyCode::Left => bytes.extend_from_slice(b"\x1b[D"),
        KeyCode::Home => bytes.extend_from_slice(b"\x1b[H"),
        KeyCode::End => bytes.extend_from_slice(b"\x1b[F"),
        KeyCode::Delete => bytes.extend_from_slice(b"\x1b[3~"),
        KeyCode::Insert => bytes.extend_from_slice(b"\x1b[2~"),
        KeyCode::PageUp => bytes.extend_from_slice(b"\x1b[5~"),
        KeyCode::PageDown => bytes.extend_from_slice(b"\x1b[6~"),
        KeyCode::F(number) => bytes.extend_from_slice(function_key(number)),
        _ => {}
    }
    bytes
}

fn function_key(number: u8) -> &'static [u8] {
    match number {
        1 => b"\x1bOP",
        2 => b"\x1bOQ",
        3 => b"\x1bOR",
        4 => b"\x1bOS",
        5 => b"\x1b[15~",
        6 => b"\x1b[17~",
        7 => b"\x1b[18~",
        8 => b"\x1b[19~",
        9 => b"\x1b[20~",
        10 => b"\x1b[21~",
        11 => b"\x1b[23~",
        12 => b"\x1b[24~",
        _ => b"",
    }
}

struct EscapeFilter {
    escape: Option<u8>,
    at_line_start: bool,
    pending: bool,
}

struct FilteredInput {
    data: Vec<u8>,
    close: bool,
}

impl EscapeFilter {
    const fn new(escape: Option<u8>) -> Self {
        Self {
            escape,
            at_line_start: true,
            pending: false,
        }
    }

    fn push(&mut self, input: &[u8]) -> FilteredInput {
        let mut data = Vec::with_capacity(input.len() + 1);
        let mut close = false;
        for &byte in input {
            let Some(escape) = self.escape else {
                data.push(byte);
                self.at_line_start = matches!(byte, b'\r' | b'\n');
                continue;
            };
            if self.pending {
                self.pending = false;
                if byte == b'.' {
                    close = true;
                    break;
                }
                data.push(escape);
                if byte == escape {
                    self.at_line_start = false;
                    continue;
                }
                data.push(byte);
                self.at_line_start = matches!(byte, b'\r' | b'\n');
            } else if self.at_line_start && byte == escape {
                self.pending = true;
            } else {
                data.push(byte);
                self.at_line_start = matches!(byte, b'\r' | b'\n');
            }
        }
        FilteredInput { data, close }
    }
}

fn parse_escape(value: &str) -> Result<Option<u8>, RunError> {
    if value.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    let bytes = value.as_bytes();
    if bytes.len() == 1 && bytes[0].is_ascii() {
        Ok(Some(bytes[0]))
    } else {
        Err(RunError::Arguments(
            "-e expects one ASCII character or 'none'".to_owned(),
        ))
    }
}

fn connect_or_start(cli: &Cli) -> Result<Stream, RunError> {
    if let Ok(stream) = connect_local() {
        return Ok(stream);
    }
    let mut command = server_command(cli)?;
    let mut child = command
        .spawn()
        .map_err(|error| RunError::Startup(format!("could not start {}: {error}", cli.server)))?;
    let started = Instant::now();
    loop {
        if let Ok(stream) = connect_local() {
            return Ok(stream);
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|error| RunError::Startup(format!("could not inspect server: {error}")))?
        {
            return Err(RunError::Startup(format!(
                "server exited during startup with {status}"
            )));
        }
        if started.elapsed() >= SERVER_START_TIMEOUT {
            let _ = child.kill();
            let _ = child.wait();
            return Err(RunError::Startup(
                "shared local server startup timed out".to_owned(),
            ));
        }
        thread::sleep(SERVER_POLL_INTERVAL);
    }
}

fn wait_for_local_server_shutdown() -> Result<(), RunError> {
    // On Windows, a named-pipe connect can fail with ERROR_PIPE_BUSY while the old
    // listener still exists. Require a short, continuous unavailable period so
    // the next CLI cannot land in the server's final accept/exit window.
    if wait_until_stable(SERVER_STOP_TIMEOUT, SERVER_ENDPOINT_QUIET_PERIOD, || {
        connect_local().is_err()
    }) {
        Ok(())
    } else {
        Err(RunError::Startup(
            "shared local server did not stop within 5 seconds".to_owned(),
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

fn server_command(cli: &Cli) -> Result<Command, RunError> {
    let mut command = Command::new(&cli.server);
    append_device_arguments(&mut command, cli)?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    hide_server_window(&mut command);
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

#[cfg(windows)]
fn hide_server_window(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW);
}

#[cfg(not(windows))]
fn hide_server_window(_command: &mut Command) {}

fn connect_local() -> io::Result<Stream> {
    let endpoint = local_endpoint();
    if GenericNamespaced::is_supported() {
        Stream::connect(endpoint.as_str().to_ns_name::<GenericNamespaced>()?)
    } else {
        Stream::connect(endpoint.as_str().to_fs_name::<GenericFilePath>()?)
    }
}

fn local_endpoint() -> String {
    if GenericNamespaced::is_supported() {
        let user = std::env::var("USERNAME").unwrap_or_else(|_| "user".to_owned());
        format!("kindlebridge-{}", sanitize_endpoint_component(&user))
    } else {
        let base = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let user = std::env::var("USER").unwrap_or_else(|_| "user".to_owned());
        base.join(format!(
            "kindlebridge-{}.sock",
            sanitize_endpoint_component(&user)
        ))
        .to_string_lossy()
        .into_owned()
    }
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
        CliError::RemotePathOutsideSyncRoot(_) => "INVALID_REMOTE_PATH",
        CliError::CurrentDirectory(_) => "HOST_IO_ERROR",
        CliError::InvalidUpdateBinary(_) => "INVALID_UPDATE_BINARY",
        CliError::StreamingShellRequired => "STREAMING_SHELL_REQUIRED",
        CliError::UpdateRejected { .. } => "UPDATE_REJECTED",
        CliError::Project(_) => "INVALID_PROJECT",
        CliError::BuildFailed { .. } => "BUILD_FAILED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn shutdown_wait_does_not_return_while_the_old_endpoint_accepts_connections() {
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
    fn escape_filter_closes_only_for_line_leading_tilde_dot() {
        let mut filter = EscapeFilter::new(Some(b'~'));
        assert_eq!(filter.push(b"echo ~.\r").data, b"echo ~.\r");
        let first = filter.push(b"~");
        assert!(first.data.is_empty());
        assert!(!first.close);
        assert!(filter.push(b".").close);
    }

    #[test]
    fn doubled_escape_sends_one_literal_escape() {
        let mut filter = EscapeFilter::new(Some(b'~'));
        let filtered = filter.push(b"~~hello");
        assert_eq!(filtered.data, b"~hello");
        assert!(!filtered.close);
    }

    #[test]
    fn input_credit_blocks_until_the_server_consumes_bytes() {
        let credit = Arc::new(InputCredit::new(4));
        assert!(credit.take(4));
        let waiting = Arc::clone(&credit);
        let worker = thread::spawn(move || waiting.take(1));
        thread::sleep(Duration::from_millis(20));
        assert!(!worker.is_finished());
        credit.restore(1);
        assert!(worker.join().unwrap());
    }

    #[test]
    fn stopping_input_credit_unblocks_a_waiting_reader() {
        let credit = Arc::new(InputCredit::new(0));
        let waiting = Arc::clone(&credit);
        let worker = thread::spawn(move || waiting.take(1));
        thread::sleep(Duration::from_millis(20));
        credit.stop();
        assert!(!worker.join().unwrap());
    }
}
