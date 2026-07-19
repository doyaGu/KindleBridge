use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::process::{Child, ChildStderr, Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use kindlebridge::{execute, Cli, CliError, ShellArgs, TopLevelCommand};
use kindlebridge_schema::{ClientError, RpcClient};
use serde_json::{json, Value};

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
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            print_error(cli.json, &error);
            ExitCode::FAILURE
        }
    }
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

fn run(cli: &Cli) -> Result<String, RunError> {
    let watchdog_listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .map_err(|error| RunError::Startup(format!("could not create server watchdog: {error}")))?;
    watchdog_listener.set_nonblocking(true).map_err(|error| {
        RunError::Startup(format!("could not configure server watchdog: {error}"))
    })?;
    let mut command = Command::new(&cli.server);
    command.arg("--stdio").arg("--parent-watchdog").arg(
        watchdog_listener
            .local_addr()
            .map_err(|error| {
                RunError::Startup(format!("could not inspect server watchdog: {error}"))
            })?
            .to_string(),
    );
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
    let stderr = if cli.json {
        Stdio::piped()
    } else {
        Stdio::inherit()
    };
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(stderr)
        .spawn()
        .map_err(|error| RunError::Startup(format!("could not start {}: {error}", cli.server)))?;
    let server_stderr = child.stderr.take().map(capture_server_stderr);
    let watchdog = match accept_server_watchdog(&watchdog_listener, &mut child) {
        Ok(watchdog) => watchdog,
        Err(message) => {
            return Err(RunError::Startup(append_server_stderr(
                message,
                server_stderr,
            )))
        }
    };
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "server stdout was not piped".to_owned())?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "server stdin was not piped".to_owned())?;

    let result = {
        let mut client = RpcClient::new(BufReader::new(stdout), stdin);
        match &cli.command {
            TopLevelCommand::Shell(args) if args.command.is_none() => {
                run_shell_repl(&mut client, args, cli.json).map_err(RunError::Message)
            }
            _ => execute(&mut client, &cli.command, cli.json).map_err(RunError::Command),
        }
    };

    let status = child
        .wait()
        .map_err(|error| format!("could not wait for server: {error}"))?;
    drop(watchdog);
    let server_stderr = collect_server_stderr(server_stderr);
    if !status.success() {
        return Err(append_server_stderr_text(
            format!("server exited with {status}"),
            server_stderr,
        )
        .into());
    }
    result
}

type ServerStderrCapture = thread::JoinHandle<std::io::Result<String>>;

fn capture_server_stderr(mut stderr: ChildStderr) -> ServerStderrCapture {
    thread::spawn(move || {
        let mut output = String::new();
        stderr.read_to_string(&mut output)?;
        Ok(output)
    })
}

fn collect_server_stderr(capture: Option<ServerStderrCapture>) -> Option<String> {
    let capture = capture?;
    match capture.join() {
        Ok(Ok(output)) if !output.trim().is_empty() => Some(output),
        Ok(Ok(_)) => None,
        Ok(Err(error)) => Some(format!("could not capture server stderr: {error}")),
        Err(_) => Some("server stderr capture thread panicked".to_owned()),
    }
}

fn append_server_stderr(message: String, capture: Option<ServerStderrCapture>) -> String {
    append_server_stderr_text(message, collect_server_stderr(capture))
}

fn append_server_stderr_text(message: String, stderr: Option<String>) -> String {
    match stderr {
        Some(stderr) => format!("{message}: {}", stderr.trim()),
        None => message,
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
        _ => json!({
            "error": {
                "kind": "cli",
                "code": "COMMAND_FAILED",
                "message": error.to_string(),
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
        CliError::UpdateRejected { .. } => "UPDATE_REJECTED",
    }
}

fn accept_server_watchdog(listener: &TcpListener, child: &mut Child) -> Result<TcpStream, String> {
    const START_TIMEOUT: Duration = Duration::from_secs(5);
    let started = Instant::now();
    loop {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_read_timeout(Some(START_TIMEOUT))
                    .map_err(|error| watchdog_error(child, "configure", error))?;
                let mut announced_pid = [0_u8; size_of::<u32>()];
                stream
                    .read_exact(&mut announced_pid)
                    .map_err(|error| watchdog_error(child, "handshake with", error))?;
                if u32::from_le_bytes(announced_pid) != child.id() {
                    terminate_child(child);
                    return Err("server watchdog connected from an unexpected process".to_owned());
                }
                stream
                    .set_read_timeout(None)
                    .map_err(|error| watchdog_error(child, "configure", error))?;
                return Ok(stream);
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if let Some(status) = child
                    .try_wait()
                    .map_err(|error| format!("could not inspect server startup: {error}"))?
                {
                    return Err(format!("server exited during startup with {status}"));
                }
                if started.elapsed() >= START_TIMEOUT {
                    terminate_child(child);
                    return Err("server watchdog startup timed out".to_owned());
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => {
                terminate_child(child);
                return Err(format!("could not accept server watchdog: {error}"));
            }
        }
    }
}

fn watchdog_error(child: &mut Child, operation: &str, error: std::io::Error) -> String {
    terminate_child(child);
    format!("could not {operation} server watchdog: {error}")
}

fn terminate_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn run_shell_repl<R: BufRead, W: Write>(
    client: &mut RpcClient<R, W>,
    args: &ShellArgs,
    json: bool,
) -> Result<String, String> {
    if json {
        return Err("interactive shell does not support --json; use shell -c".to_owned());
    }
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let mut line = String::new();
    loop {
        print!("kindlebridge:{}$ ", args.serial);
        std::io::stdout()
            .flush()
            .map_err(|error| error.to_string())?;
        line.clear();
        if input
            .read_line(&mut line)
            .map_err(|error| error.to_string())?
            == 0
        {
            break;
        }
        let command = line.trim_end_matches(['\r', '\n']);
        if matches!(command.trim(), "exit" | "quit") {
            break;
        }
        if command.trim().is_empty() {
            continue;
        }
        let output = execute(
            client,
            &TopLevelCommand::Shell(ShellArgs {
                serial: args.serial.clone(),
                command: Some(command.to_owned()),
                timeout_ms: args.timeout_ms,
            }),
            false,
        )
        .map_err(|error| error.to_string())?;
        if !output.is_empty() {
            println!("{output}");
        }
    }
    Ok("shell closed".to_owned())
}
