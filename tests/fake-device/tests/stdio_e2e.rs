use std::collections::BTreeMap;
use std::fs;
use std::io::BufReader;
use std::process::{Command, Stdio};

use kindlebridge::{
    execute, run_project_once, AppArgs, AppCommand, CliError, DeviceArgs, DeviceCommand, LogArgs,
    LogCommand, ProcessArgs, ProcessCommand, RunArgs, ServerArgs, ServerCommand, SyncArgs,
    SyncCommand, TopLevelCommand,
};
use kindlebridge_bundle::{BuildConfig, BundleBuilder, BundleKind};
use kindlebridge_fake_device::SERIAL;
use kindlebridge_schema::{
    error_codes, methods, AppList, AppState, AppSummary, ClientError, DeviceFeatures, DeviceList,
    ExecResult, LogSnapshot, ProcessList, RpcClient, ServerVersion, SyncStatus,
};
use serde_json::{json, Value};

#[test]
fn run_builds_installs_and_starts_a_development_project() {
    let root = std::env::temp_dir().join(format!("kindlebridge-run-e2e-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("root/bin")).unwrap();
    fs::write(root.join("root/bin/app"), b"#!/bin/sh\nsleep 60\n").unwrap();
    fs::write(root.join("dev.key"), [0x42; 32]).unwrap();
    let build = if cfg!(windows) {
        r#"["cmd", "/d", "/c", "exit", "0"]"#
    } else {
        r#"["sh", "-c", "true"]"#
    };
    let manifest = format!(
        r#"
kind = "application"
id = "org.example.run"
version = "0.1.0"
release = 1
entrypoints = {{ main = "bin/app" }}

[process]
restart = "never"
stop_timeout_ms = 100

[development]
build = {build}
input = "root"
signing_key = "dev.key"
watch = ["src"]
"#
    );
    fs::write(root.join("kindlebridge.toml"), &manifest).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_kindlebridge-fake-device"))
        .arg("--stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut client = RpcClient::new(
        BufReader::new(child.stdout.take().unwrap()),
        child.stdin.take().unwrap(),
    );
    let output = run_project_once(
        &mut client,
        &RunArgs {
            serial: SERIAL.to_owned(),
            manifest: root.join("kindlebridge.toml"),
            watch: false,
        },
        false,
    )
    .unwrap();
    assert!(output.contains("org.example.run"));
    assert!(output.contains("running"));

    let apps = execute(
        &mut client,
        &TopLevelCommand::App(AppArgs {
            command: AppCommand::List {
                serial: SERIAL.to_owned(),
            },
        }),
        true,
    )
    .unwrap();
    let apps: AppList = serde_json::from_str(&apps).unwrap();
    assert_eq!(apps.apps.len(), 1);
    assert_eq!(apps.apps[0].app_id, "org.example.run");
    assert_eq!(apps.apps[0].state, AppState::Running);

    fs::write(root.join("root/bin/app"), b"#!/bin/sh\nsleep 59\n").unwrap();
    run_project_once(
        &mut client,
        &RunArgs {
            serial: SERIAL.to_owned(),
            manifest: root.join("kindlebridge.toml"),
            watch: false,
        },
        false,
    )
    .unwrap();
    let apps = execute(
        &mut client,
        &TopLevelCommand::App(AppArgs {
            command: AppCommand::List {
                serial: SERIAL.to_owned(),
            },
        }),
        true,
    )
    .unwrap();
    let apps: AppList = serde_json::from_str(&apps).unwrap();
    assert_eq!(apps.apps[0].state, AppState::Running);
    assert!(apps.apps[0].rollback_available);

    let failing_build = if cfg!(windows) {
        r#"["cmd", "/d", "/c", "exit", "23"]"#
    } else {
        r#"["sh", "-c", "exit 23"]"#
    };
    fs::write(
        root.join("kindlebridge.toml"),
        manifest.replace(
            &format!("build = {build}"),
            &format!("build = {failing_build}"),
        ),
    )
    .unwrap();
    let error = run_project_once(
        &mut client,
        &RunArgs {
            serial: SERIAL.to_owned(),
            manifest: root.join("kindlebridge.toml"),
            watch: false,
        },
        false,
    )
    .unwrap_err();
    assert!(matches!(error, CliError::BuildFailed { exit_code: 23, .. }));
    let apps = execute(
        &mut client,
        &TopLevelCommand::App(AppArgs {
            command: AppCommand::List {
                serial: SERIAL.to_owned(),
            },
        }),
        true,
    )
    .unwrap();
    let apps: AppList = serde_json::from_str(&apps).unwrap();
    assert_eq!(apps.apps[0].state, AppState::Running);

    drop(client);
    assert!(child.wait().unwrap().success());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn all_v1_discovery_methods_work_over_stdio_and_cli_rpc() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_kindlebridge-fake-device"))
        .arg("--stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut client = RpcClient::new(BufReader::new(stdout), stdin);

    let ping = client.call(methods::SERVER_PING, None).unwrap();
    assert_eq!(ping, json!({ "ok": true }));

    let version: ServerVersion =
        serde_json::from_value(client.call(methods::SERVER_VERSION, None).unwrap()).unwrap();
    assert_eq!(version.api_version, "v1");

    let devices: DeviceList =
        serde_json::from_value(client.call(methods::DEVICE_LIST, None).unwrap()).unwrap();
    assert_eq!(devices.devices.len(), 1);
    assert_eq!(devices.devices[0].serial, SERIAL);

    let device_ping = client
        .call(methods::DEVICE_PING, Some(json!({ "serial": SERIAL })))
        .unwrap();
    assert_eq!(device_ping, json!({ "ok": true }));

    let features: DeviceFeatures = serde_json::from_value(
        client
            .call(methods::DEVICE_FEATURES, Some(json!({ "serial": SERIAL })))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        features.features,
        [
            "app.install.v1",
            "app.list.v1",
            "app.restart.v1",
            "app.rollback.v1",
            "app.start.v1",
            "app.stop.v1",
            "app.uninstall.v1",
            "exec.v1",
            "log.tail.v1",
            "process.list.v1",
            "process.signal.v1",
            "rpc.v1",
            "sync.tree.v1",
            "sync.v1"
        ]
    );

    let exec: ExecResult = serde_json::from_value(
        client
            .call(
                methods::EXEC_RUN,
                Some(json!({
                    "serial": SERIAL,
                    "argv": ["echo", "hello", "kindle"],
                    "environment": {},
                    "timeout_ms": 1000
                })),
            )
            .unwrap(),
    )
    .unwrap();
    assert_eq!(exec.exit_code, 0);
    assert_eq!(exec.stdout, "hello kindle\n");

    // Exercise the CLI command layer against the same public RPC connection.
    let output = execute(
        &mut client,
        &TopLevelCommand::Device(DeviceArgs {
            command: DeviceCommand::Features {
                serial: SERIAL.to_owned(),
            },
        }),
        true,
    )
    .unwrap();
    let output: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(output["serial"], SERIAL);

    let output = execute(
        &mut client,
        &TopLevelCommand::Device(DeviceArgs {
            command: DeviceCommand::Ping {
                serial: SERIAL.to_owned(),
            },
        }),
        false,
    )
    .unwrap();
    assert_eq!(output, "pong");

    let output = execute(
        &mut client,
        &TopLevelCommand::Server(ServerArgs {
            command: ServerCommand::Ping,
        }),
        false,
    )
    .unwrap();
    assert_eq!(output, "pong");

    drop(client);
    assert!(child.wait().unwrap().success());
}

#[test]
fn stateful_sync_app_process_and_log_flow_works_over_stdio() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_kindlebridge-fake-device"))
        .arg("--stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut client = RpcClient::new(BufReader::new(stdout), stdin);

    let unique = format!("{}-{}", std::process::id(), SERIAL);
    let source = std::env::temp_dir().join(format!("kindlebridge-source-{unique}.bin"));
    let destination = std::env::temp_dir().join(format!("kindlebridge-pull-{unique}.bin"));
    let tree_source = std::env::temp_dir().join(format!("kindlebridge-tree-source-{unique}"));
    let tree_destination = std::env::temp_dir().join(format!("kindlebridge-tree-pull-{unique}"));
    let app_v1 = std::env::temp_dir().join(format!("kindlebridge-app-v1-{unique}.kbb"));
    let app_v2 = std::env::temp_dir().join(format!("kindlebridge-app-v2-{unique}.kbb"));
    let payload: Vec<u8> = (0_u16..4096).flat_map(u16::to_le_bytes).collect();
    fs::write(&source, &payload).unwrap();
    fs::create_dir_all(tree_source.join("empty")).unwrap();
    fs::create_dir_all(tree_source.join("nested")).unwrap();
    fs::write(tree_source.join("root.txt"), b"root").unwrap();
    fs::write(tree_source.join("nested/child.bin"), [0_u8, 1, 2, 3]).unwrap();

    let pushed = execute(
        &mut client,
        &TopLevelCommand::Sync(SyncArgs {
            command: SyncCommand::Push {
                serial: SERIAL.to_owned(),
                local_path: source.to_string_lossy().into_owned(),
                remote_path: "dev/payload.bin".to_owned(),
                block_size: 257,
                resume: None,
            },
        }),
        true,
    )
    .unwrap();
    let pushed: Value = serde_json::from_str(&pushed).unwrap();
    let transfer_id = pushed["transfer_id"].as_str().unwrap();
    let status: SyncStatus = serde_json::from_value(
        client
            .call(
                methods::SYNC_STATUS,
                Some(json!({ "serial": SERIAL, "transfer_id": transfer_id })),
            )
            .unwrap(),
    )
    .unwrap();
    assert_eq!(status.next_offset, payload.len() as u64);

    execute(
        &mut client,
        &TopLevelCommand::Sync(SyncArgs {
            command: SyncCommand::Pull {
                serial: SERIAL.to_owned(),
                remote_path: "dev/payload.bin".to_owned(),
                local_path: destination.to_string_lossy().into_owned(),
                block_size: 193,
                resume: None,
                recursive: false,
            },
        }),
        false,
    )
    .unwrap();
    assert_eq!(fs::read(&destination).unwrap(), payload);

    execute(
        &mut client,
        &TopLevelCommand::Sync(SyncArgs {
            command: SyncCommand::Push {
                serial: SERIAL.to_owned(),
                local_path: tree_source.to_string_lossy().into_owned(),
                remote_path: "dev/tree".to_owned(),
                block_size: 193,
                resume: None,
            },
        }),
        false,
    )
    .unwrap();
    execute(
        &mut client,
        &TopLevelCommand::Sync(SyncArgs {
            command: SyncCommand::Pull {
                serial: SERIAL.to_owned(),
                remote_path: "dev/tree".to_owned(),
                local_path: tree_destination.to_string_lossy().into_owned(),
                block_size: 193,
                resume: None,
                recursive: true,
            },
        }),
        false,
    )
    .unwrap();
    assert_eq!(
        fs::read(tree_destination.join("root.txt")).unwrap(),
        b"root"
    );
    assert_eq!(
        fs::read(tree_destination.join("nested/child.bin")).unwrap(),
        [0_u8, 1, 2, 3]
    );
    assert!(tree_destination.join("empty").is_dir());

    let app_id = "org.kindlebridge.e2e";
    write_test_bundle(&app_v1, app_id, "1.0.0", 1);
    write_test_bundle(&app_v2, app_id, "2.0.0", 2);
    let installed = app_command(
        &mut client,
        AppCommand::Install {
            serial: SERIAL.to_owned(),
            bundle_path: app_v1.to_string_lossy().into_owned(),
        },
    );
    assert_eq!(installed.state, AppState::Stopped);
    let first_pid = app_command(
        &mut client,
        AppCommand::Start {
            serial: SERIAL.to_owned(),
            app_id: app_id.to_owned(),
        },
    )
    .pid
    .unwrap();
    let restarted = app_command(
        &mut client,
        AppCommand::Restart {
            serial: SERIAL.to_owned(),
            app_id: app_id.to_owned(),
        },
    );
    assert_ne!(restarted.pid, Some(first_pid));
    assert_eq!(
        app_command(
            &mut client,
            AppCommand::Stop {
                serial: SERIAL.to_owned(),
                app_id: app_id.to_owned(),
            },
        )
        .state,
        AppState::Stopped
    );
    app_command(
        &mut client,
        AppCommand::Install {
            serial: SERIAL.to_owned(),
            bundle_path: app_v2.to_string_lossy().into_owned(),
        },
    );
    assert_eq!(
        app_command(
            &mut client,
            AppCommand::Rollback {
                serial: SERIAL.to_owned(),
                app_id: app_id.to_owned(),
            },
        )
        .version,
        "1.0.0"
    );
    let pid = app_command(
        &mut client,
        AppCommand::Start {
            serial: SERIAL.to_owned(),
            app_id: app_id.to_owned(),
        },
    )
    .pid
    .unwrap();

    let processes = execute(
        &mut client,
        &TopLevelCommand::Process(ProcessArgs {
            command: ProcessCommand::List {
                serial: SERIAL.to_owned(),
            },
        }),
        true,
    )
    .unwrap();
    assert_eq!(
        serde_json::from_str::<ProcessList>(&processes)
            .unwrap()
            .processes[0]
            .pid,
        pid
    );
    execute(
        &mut client,
        &TopLevelCommand::Process(ProcessArgs {
            command: ProcessCommand::Signal {
                serial: SERIAL.to_owned(),
                pid,
                signal: "KILL".to_owned(),
            },
        }),
        false,
    )
    .unwrap();
    let error = client
        .call(
            methods::PROCESS_SIGNAL,
            Some(json!({ "serial": SERIAL, "pid": pid, "signal": "KILL" })),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Rpc(error) if error.code == error_codes::PROCESS_NOT_FOUND
    ));

    app_command(
        &mut client,
        AppCommand::Uninstall {
            serial: SERIAL.to_owned(),
            app_id: app_id.to_owned(),
        },
    );
    let apps = execute(
        &mut client,
        &TopLevelCommand::App(AppArgs {
            command: AppCommand::List {
                serial: SERIAL.to_owned(),
            },
        }),
        true,
    )
    .unwrap();
    assert!(serde_json::from_str::<AppList>(&apps)
        .unwrap()
        .apps
        .is_empty());

    let logs = execute(
        &mut client,
        &TopLevelCommand::Log(LogArgs {
            command: LogCommand::Tail {
                serial: SERIAL.to_owned(),
                cursor: None,
                limit: 3,
            },
        }),
        true,
    )
    .unwrap();
    let logs: LogSnapshot = serde_json::from_str(&logs).unwrap();
    assert!(!logs.entries.is_empty());
    assert!(logs.entries.len() <= 3);

    fs::remove_file(source).unwrap();
    fs::remove_file(destination).unwrap();
    fs::remove_dir_all(tree_source).unwrap();
    fs::remove_dir_all(tree_destination).unwrap();
    fs::remove_file(app_v1).unwrap();
    fs::remove_file(app_v2).unwrap();
    drop(client);
    assert!(child.wait().unwrap().success());
}

fn write_test_bundle(path: &std::path::Path, app_id: &str, version: &str, release: u64) {
    let mut config = BuildConfig::new(
        BundleKind::Application,
        app_id,
        version,
        release,
        "kindlehf",
    );
    config.entrypoints = BTreeMap::from([("main".to_owned(), "bin/app".to_owned())]);
    let mut builder = BundleBuilder::new(config);
    builder
        .add_file(
            "bin/app",
            format!("#!/bin/sh\necho {version}\n").into_bytes(),
            true,
        )
        .unwrap();
    let key = ed25519_dalek::SigningKey::from_bytes(&[0x42; 32]);
    fs::write(path, builder.build(&key).unwrap()).unwrap();
}

fn app_command(
    client: &mut RpcClient<BufReader<std::process::ChildStdout>, std::process::ChildStdin>,
    command: AppCommand,
) -> AppSummary {
    let output = execute(client, &TopLevelCommand::App(AppArgs { command }), true).unwrap();
    serde_json::from_str(&output).unwrap()
}
