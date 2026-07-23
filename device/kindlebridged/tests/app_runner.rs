#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use kindlebridge_bundle::{
    DataPolicy, Digest, MaterializedApplication, ProcessPolicy, RestartPolicy,
};
use kindlebridged::app::{AppSupervisor, RuntimeStatus};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

#[test]
fn application_outlives_the_rpc_style_thread_that_requested_start() {
    let root = temporary_directory();
    fs::create_dir(&root).unwrap();
    let entrypoint = root.join("app.sh");
    let count_file = root.join("data").join("org.example.actor").join("count");
    fs::write(
        &entrypoint,
        b"#!/bin/sh\ncount=0\ntest ! -f \"$KINDLEBRIDGE_DATA/count\" || count=$(cat \"$KINDLEBRIDGE_DATA/count\")\ncount=$((count + 1))\necho \"$count\" > \"$KINDLEBRIDGE_DATA/count\"\necho \"stdout attempt $count\"\necho \"stderr attempt $count\" >&2\ntest \"$count\" -ge 3 || exit 42\ntrap 'exit 0' HUP INT TERM\nwhile :; do sleep 1; done\n",
    )
    .unwrap();
    fs::set_permissions(&entrypoint, fs::Permissions::from_mode(0o755)).unwrap();

    let bundle_root = Digest::of(b"actor-owned-application");
    let application = MaterializedApplication {
        app_id: "org.example.actor".to_owned(),
        version: "1.0.0".to_owned(),
        bundle_root,
        image_root: root.clone(),
        main: entrypoint,
        process: ProcessPolicy {
            restart: RestartPolicy::OnFailure,
            stop_timeout_ms: 500,
            working_dir: None,
            environment: None,
        },
        data: DataPolicy::default(),
    };
    let supervisor = Arc::new(AppSupervisor::with_runner_executable_for_tests(
        PathBuf::from(env!("CARGO_BIN_EXE_kindlebridged")),
    ));
    let caller = Arc::clone(&supervisor);
    let data_root = root.join("data");
    let runner_pid = thread::spawn(move || caller.start(&application, &data_root).unwrap())
        .join()
        .unwrap();

    wait_until(Duration::from_secs(3), || {
        fs::read_to_string(&count_file).is_ok_and(|value| value.trim() == "3")
    });
    let logs = root
        .join("data")
        .join("org.example.actor")
        .join(".kindlebridge-logs");
    wait_until(Duration::from_secs(3), || {
        fs::read_to_string(logs.join("stdout.log"))
            .is_ok_and(|value| value.contains("stdout attempt 3"))
            && fs::read_to_string(logs.join("stderr.log"))
                .is_ok_and(|value| value.contains("stderr attempt 3"))
    });
    assert_eq!(
        fs::read_to_string(logs.join("stdout.log"))
            .unwrap()
            .lines()
            .count(),
        3
    );
    assert_eq!(
        fs::read_to_string(logs.join("stderr.log"))
            .unwrap()
            .lines()
            .count(),
        3
    );
    assert_eq!(
        supervisor.status("org.example.actor", bundle_root).unwrap(),
        RuntimeStatus::Running(runner_pid)
    );

    supervisor
        .stop("org.example.actor", Duration::from_millis(500))
        .unwrap();
    assert_eq!(
        supervisor.status("org.example.actor", bundle_root).unwrap(),
        RuntimeStatus::Stopped
    );
    drop(supervisor);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn runner_terminates_and_reaps_the_application_process_group() {
    let root = temporary_directory();
    fs::create_dir(&root).unwrap();
    let entrypoint = root.join("app.sh");
    let pid_file = root.join("app.pid");
    fs::write(
        &entrypoint,
        b"#!/bin/sh\n: \"${KINDLEBRIDGE_TEST_PID_FILE:?}\"\necho \"$$\" > \"$KINDLEBRIDGE_TEST_PID_FILE\"\ntrap 'exit 0' HUP INT TERM\nwhile :; do sleep 1; done\n",
    )
    .unwrap();
    fs::set_permissions(&entrypoint, fs::Permissions::from_mode(0o755)).unwrap();

    let mut runner = Command::new(env!("CARGO_BIN_EXE_kindlebridged"))
        .args([
            "run-app-supervisor",
            "--entrypoint",
            entrypoint.to_str().unwrap(),
            "--stop-timeout-ms",
            "500",
        ])
        .env("KINDLEBRIDGE_TEST_PID_FILE", &pid_file)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until(Duration::from_secs(3), || pid_file.exists());
    let app_pid: i32 = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    kill(Pid::from_raw(runner.id() as i32), Signal::SIGTERM).unwrap();
    let _ = runner.wait().unwrap();
    wait_until(Duration::from_secs(3), || !process_exists(app_pid));

    fs::remove_file(pid_file).unwrap();
    fs::remove_file(entrypoint).unwrap();
    fs::remove_dir(root).unwrap();
}

#[test]
fn runner_restarts_only_failures_and_stops_after_the_bounded_budget() {
    let root = temporary_directory();
    fs::create_dir(&root).unwrap();
    let entrypoint = root.join("fail.sh");
    let count_file = root.join("count");
    fs::write(
        &entrypoint,
        format!(
            "#!/bin/sh\ncount=0\ntest ! -f '{0}' || count=$(cat '{0}')\ncount=$((count + 1))\necho \"$count\" > '{0}'\nexit 42\n",
            count_file.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&entrypoint, fs::Permissions::from_mode(0o755)).unwrap();

    let started = Instant::now();
    let status = Command::new(env!("CARGO_BIN_EXE_kindlebridged"))
        .args([
            "run-app-supervisor",
            "--entrypoint",
            entrypoint.to_str().unwrap(),
            "--stop-timeout-ms",
            "500",
            "--restart-on-failure",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();

    assert!(!status.success());
    assert_eq!(fs::read_to_string(&count_file).unwrap().trim(), "4");
    assert!(started.elapsed() < Duration::from_secs(3));

    let success_count = root.join("success-count");
    fs::write(
        &entrypoint,
        format!(
            "#!/bin/sh\necho 1 > '{}'\nexit 0\n",
            success_count.display()
        ),
    )
    .unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_kindlebridged"))
        .args([
            "run-app-supervisor",
            "--entrypoint",
            entrypoint.to_str().unwrap(),
            "--stop-timeout-ms",
            "500",
            "--restart-on-failure",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(fs::read_to_string(&success_count).unwrap().trim(), "1");

    fs::remove_file(count_file).unwrap();
    fs::remove_file(success_count).unwrap();
    fs::remove_file(entrypoint).unwrap();
    fs::remove_dir(root).unwrap();
}

#[test]
fn runner_recovers_from_transient_failures_and_remains_stoppable() {
    let root = temporary_directory();
    fs::create_dir(&root).unwrap();
    let entrypoint = root.join("transient.sh");
    let count_file = root.join("count");
    fs::write(
        &entrypoint,
        format!(
            "#!/bin/sh\ncount=0\ntest ! -f '{0}' || count=$(cat '{0}')\ncount=$((count + 1))\necho \"$count\" > '{0}'\ntest \"$count\" -ge 3 || exit 42\ntrap 'exit 0' HUP INT TERM\nwhile :; do sleep 1; done\n",
            count_file.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&entrypoint, fs::Permissions::from_mode(0o755)).unwrap();

    let mut runner = Command::new(env!("CARGO_BIN_EXE_kindlebridged"))
        .args([
            "run-app-supervisor",
            "--entrypoint",
            entrypoint.to_str().unwrap(),
            "--stop-timeout-ms",
            "500",
            "--restart-on-failure",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until(Duration::from_secs(3), || {
        fs::read_to_string(&count_file).is_ok_and(|value| value.trim() == "3")
    });
    assert!(runner.try_wait().unwrap().is_none());

    kill(Pid::from_raw(runner.id() as i32), Signal::SIGTERM).unwrap();
    assert!(runner.wait().unwrap().success());

    fs::remove_file(count_file).unwrap();
    fs::remove_file(entrypoint).unwrap();
    fs::remove_dir(root).unwrap();
}

fn wait_until(timeout: Duration, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "condition did not become true");
        thread::sleep(Duration::from_millis(20));
    }
}

fn process_exists(pid: i32) -> bool {
    kill(Pid::from_raw(pid), None).is_ok()
}

fn temporary_directory() -> PathBuf {
    std::env::temp_dir().join(format!(
        "kindlebridge-app-runner-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
