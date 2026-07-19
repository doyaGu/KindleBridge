use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::process::{Command, Output};

use kindlebridge_schema::{
    read_json_frame, write_json_frame, RequestId, RpcError, RpcResponse, DEFAULT_MAX_CONTENT_LENGTH,
};
use serde_json::{json, Value};

const FAKE_SERVER_ENV: &str = "KINDLEBRIDGE_JSON_CONTRACT_FAKE_SERVER";

fn main() {
    if let Some(mode) = std::env::var_os(FAKE_SERVER_ENV) {
        if mode == "startup-error" {
            eprintln!("fake server startup diagnostic");
            std::process::exit(17);
        } else if mode == "exec-exit-7" {
            serve_one_exec_result(7);
            return;
        } else {
            serve_one_rpc_error();
            return;
        }
    }

    rpc_error_is_structured_json();
    parameter_error_is_structured_json();
    startup_error_is_structured_json();
    clap_error_is_structured_json_and_keeps_exit_code();
    command_validation_error_is_structured_json();
    non_json_errors_keep_the_human_readable_contract();
    server_stderr_does_not_corrupt_json_errors();
    exec_preserves_the_remote_exit_code();
}

fn exec_preserves_the_remote_exit_code() {
    let server = current_executable();
    let output = run_cli_with_server_mode(
        [
            "--json",
            "--server",
            server.to_str().expect("UTF-8 test path"),
            "exec",
            "KT6-TEST",
            "--",
            "/bin/false",
        ],
        "exec-exit-7",
    );

    assert_eq!(output.status.code(), Some(7));
    assert!(output.stderr.is_empty());
    let document: Value = serde_json::from_slice(&output.stdout).expect("valid JSON stdout");
    assert_eq!(document["exit_code"], 7);
}

fn server_stderr_does_not_corrupt_json_errors() {
    let server = current_executable();
    let output = run_cli_with_server_mode(
        [
            "--json",
            "--server",
            server.to_str().expect("UTF-8 test path"),
            "device",
            "list",
        ],
        "startup-error",
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let document = parse_stderr(&output);
    assert_eq!(document["error"]["kind"], "startup");
    assert_eq!(document["error"]["code"], "SERVER_START_FAILED");
    assert!(document["error"]["message"]
        .as_str()
        .expect("startup error message")
        .contains("fake server startup diagnostic"));
}

fn command_validation_error_is_structured_json() {
    let server = current_executable();
    let output = run_cli([
        "--json",
        "--server",
        server.to_str().expect("UTF-8 test path"),
        "sync",
        "push",
        "KT6-TEST",
        "local.bin",
        "remote.bin",
        "--block-size",
        "0",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let document = parse_stderr(&output);
    assert_eq!(document["error"]["kind"], "command");
    assert_eq!(document["error"]["code"], "INVALID_BLOCK_SIZE");
}

fn non_json_errors_keep_the_human_readable_contract() {
    let server = current_executable();
    let output = run_cli([
        "--server",
        server.to_str().expect("UTF-8 test path"),
        "process",
        "list",
        "KT6-TEST",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 CLI stderr");
    assert_eq!(
        stderr,
        "kindlebridge: Feature unavailable (-32005): {\"feature\":\"process.v1\",\"serial\":\"KT6-TEST\"}\n"
    );
}

fn clap_error_is_structured_json_and_keeps_exit_code() {
    let output = run_cli(["--json", "device", "features"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let document = parse_stderr(&output);
    assert_eq!(document["error"]["kind"], "arguments");
    assert_eq!(document["error"]["code"], "INVALID_ARGUMENTS");
    assert!(document["error"]["message"]
        .as_str()
        .expect("argument error message")
        .contains("<SERIAL>"));
}

fn startup_error_is_structured_json() {
    let missing_server = std::env::temp_dir()
        .join("kindlebridge-json-contract-missing-server")
        .to_string_lossy()
        .into_owned();
    let output = run_cli(["--json", "--server", &missing_server, "device", "list"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let document = parse_stderr(&output);
    assert_eq!(document["error"]["kind"], "startup");
    assert_eq!(document["error"]["code"], "SERVER_START_FAILED");
    assert!(document["error"]["message"]
        .as_str()
        .expect("startup error message")
        .starts_with("could not start "));
}

fn parameter_error_is_structured_json() {
    let output = run_cli([
        "--json",
        "--no-usb",
        "--usb-serial",
        "KT6-TEST",
        "device",
        "list",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let document = parse_stderr(&output);
    assert_eq!(document["error"]["kind"], "arguments");
    assert_eq!(document["error"]["code"], "INVALID_ARGUMENTS");
    assert_eq!(
        document["error"]["message"],
        "--usb-serial requires automatic USB mode (remove --no-usb/--tcp-device)"
    );
}

fn rpc_error_is_structured_json() {
    let output = run_cli([
        "--json",
        "--server",
        current_executable().to_str().expect("UTF-8 test path"),
        "process",
        "list",
        "KT6-TEST",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let document = parse_stderr(&output);
    assert_eq!(document["error"]["kind"], "rpc");
    assert_eq!(document["error"]["code"], -32_005);
    assert_eq!(document["error"]["message"], "Feature unavailable");
    assert_eq!(document["error"]["data"]["serial"], "KT6-TEST");
    assert_eq!(document["error"]["data"]["feature"], "process.v1");
}

fn run_cli<const N: usize>(args: [&str; N]) -> Output {
    run_cli_with_server_mode(args, "rpc-error")
}

fn run_cli_with_server_mode<const N: usize>(args: [&str; N], mode: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kindlebridge"))
        .args(args)
        .env(FAKE_SERVER_ENV, mode)
        .output()
        .expect("run KindleBridge CLI")
}

fn parse_stderr(output: &Output) -> Value {
    serde_json::from_slice(&output.stderr).unwrap_or_else(|error| {
        panic!(
            "stderr is not valid JSON: {error}; stderr={}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn current_executable() -> std::path::PathBuf {
    std::env::current_exe().expect("locate JSON contract test executable")
}

fn serve_one_rpc_error() {
    let address = argument_value("--parent-watchdog");
    let mut watchdog = TcpStream::connect(address).expect("connect CLI watchdog");
    watchdog
        .write_all(&std::process::id().to_le_bytes())
        .expect("announce fake server PID");

    let stdin = std::io::stdin();
    let Some(request) = read_json_frame(
        &mut BufReader::new(stdin.lock()),
        DEFAULT_MAX_CONTENT_LENGTH,
    )
    .expect("read CLI RPC request") else {
        return;
    };
    assert_eq!(request["method"], "v1.process.list");

    let response = RpcResponse::failure(
        RequestId::Number(request["id"].as_i64().expect("numeric request ID")),
        RpcError::feature_unavailable("KT6-TEST", "process.v1"),
    );
    let stdout = std::io::stdout();
    write_json_frame(&mut stdout.lock(), &response).expect("write fake RPC response");
}

fn serve_one_exec_result(exit_code: i32) {
    let address = argument_value("--parent-watchdog");
    let mut watchdog = TcpStream::connect(address).expect("connect CLI watchdog");
    watchdog
        .write_all(&std::process::id().to_le_bytes())
        .expect("announce fake server PID");

    let stdin = std::io::stdin();
    let request = read_json_frame(
        &mut BufReader::new(stdin.lock()),
        DEFAULT_MAX_CONTENT_LENGTH,
    )
    .expect("read CLI RPC request")
    .expect("exec RPC request");
    assert_eq!(request["method"], "v1.exec.run");

    let response = RpcResponse::success(
        RequestId::Number(request["id"].as_i64().expect("numeric request ID")),
        json!({
            "exit_code": exit_code,
            "stdout": "",
            "stderr": "",
            "duration_ms": 1
        }),
    );
    let stdout = std::io::stdout();
    write_json_frame(&mut stdout.lock(), &response).expect("write fake RPC response");
}

fn argument_value(name: &str) -> String {
    let mut arguments = std::env::args();
    while let Some(argument) = arguments.next() {
        if argument == name {
            return arguments.next().expect("argument value");
        }
    }
    panic!("missing {name}");
}
