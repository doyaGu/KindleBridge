use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::process::{Child, Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use kindlebridge::{execute, Cli, ShellArgs, TopLevelCommand};
use kindlebridge_schema::RpcClient;

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(output) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("kindlebridge: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> Result<String, String> {
    let watchdog_listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .map_err(|error| format!("could not create server watchdog: {error}"))?;
    watchdog_listener
        .set_nonblocking(true)
        .map_err(|error| format!("could not configure server watchdog: {error}"))?;
    let mut command = Command::new(&cli.server);
    command.arg("--stdio").arg("--parent-watchdog").arg(
        watchdog_listener
            .local_addr()
            .map_err(|error| format!("could not inspect server watchdog: {error}"))?
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
        return Err(
            "--usb-serial requires automatic USB mode (remove --no-usb/--tcp-device)".to_owned(),
        );
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| format!("could not start {}: {error}", cli.server))?;
    let watchdog = accept_server_watchdog(&watchdog_listener, &mut child)?;
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
                run_shell_repl(&mut client, args, cli.json)
            }
            _ => execute(&mut client, &cli.command, cli.json).map_err(|error| error.to_string()),
        }
    };

    let status = child
        .wait()
        .map_err(|error| format!("could not wait for server: {error}"))?;
    drop(watchdog);
    if !status.success() {
        return Err(format!("server exited with {status}"));
    }
    result
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
