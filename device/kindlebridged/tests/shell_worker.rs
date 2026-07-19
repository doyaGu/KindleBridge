use kindlebridge_schema::device_protocol::{ShellMode, ShellOpen, TerminalSize};
use std::time::Duration;

use kindlebridge_schema::shell_protocol::ShellExit;
use kindlebridged::shell::{ShellEvent, ShellWorker, ShellWorkerError};

#[test]
fn shell_worker_rejects_an_empty_command_before_spawning() {
    let open = ShellOpen {
        mode: ShellMode::Raw,
        argv: Vec::new(),
        terminal_size: None,
        cwd: "/tmp/root".to_owned(),
        term: "linux".to_owned(),
    };

    assert!(matches!(
        ShellWorker::spawn(open),
        Err(ShellWorkerError::EmptyArgv)
    ));
}

#[test]
fn raw_shell_streams_stdout_stderr_and_exit_separately() {
    let mut worker = ShellWorker::spawn(raw_test_command()).unwrap();
    worker.close_input().unwrap();

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = loop {
        match worker.recv_timeout(Duration::from_secs(5)).unwrap() {
            ShellEvent::Stdout(bytes) => stdout.extend(bytes),
            ShellEvent::Stderr(bytes) => stderr.extend(bytes),
            ShellEvent::Exit(status) => break status,
        }
    };

    assert!(String::from_utf8_lossy(&stdout).contains("out"));
    assert!(String::from_utf8_lossy(&stderr).contains("err"));
    assert_eq!(
        exit,
        ShellExit {
            exit_code: 37,
            signal: 0,
        }
    );
}

#[test]
fn raw_shell_preserves_binary_stdin_and_closes_it_explicitly() {
    let mut worker = ShellWorker::spawn(raw_copy_command()).unwrap();
    let input = vec![0, 1, 2, b'\n', 0xff, 0, b'z'];
    worker.write_stdin(input.clone()).unwrap();
    worker.close_input().unwrap();

    let mut output = Vec::new();
    loop {
        match worker.recv_timeout(Duration::from_secs(5)).unwrap() {
            ShellEvent::Stdout(bytes) => output.extend(bytes),
            ShellEvent::Stderr(bytes) => panic!("unexpected stderr: {bytes:?}"),
            ShellEvent::Exit(status) => {
                assert_eq!(status.exit_code, 0);
                break;
            }
        }
    }
    assert_eq!(output, input);
}

#[test]
fn raw_shell_rejects_terminal_resize() {
    let worker = ShellWorker::spawn(raw_copy_command()).unwrap();
    assert!(matches!(
        worker.resize(TerminalSize {
            rows: 40,
            columns: 120,
            pixel_width: 0,
            pixel_height: 0,
        }),
        Err(ShellWorkerError::ResizeForRaw)
    ));
}

#[cfg(unix)]
#[test]
fn pty_shell_is_persistent_has_ttys_and_resizes() {
    let initial_size = TerminalSize {
        rows: 24,
        columns: 80,
        pixel_width: 0,
        pixel_height: 0,
    };
    let mut worker = ShellWorker::spawn(ShellOpen {
        mode: ShellMode::Pty,
        argv: vec!["/bin/sh".to_owned()],
        terminal_size: Some(initial_size),
        cwd: std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        term: "linux".to_owned(),
    })
    .unwrap();

    worker
        .write_stdin(
            b"test -t 0 && test -t 1 && test -t 2 && echo TTY-OK; cd /; export KB_TEST=persistent; echo STATE:$PWD:$KB_TEST; echo READY\n"
                .to_vec(),
        )
        .unwrap();
    let startup = recv_stdout_until(&mut worker, b"READY", Duration::from_secs(5));
    let startup = String::from_utf8_lossy(&startup);
    assert!(startup.contains("TTY-OK"), "{startup}");
    assert!(startup.contains("STATE:/:persistent"), "{startup}");

    worker
        .resize(TerminalSize {
            rows: 41,
            columns: 119,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    worker
        .write_stdin(b"stty size; echo RESIZED; exit 37\n".to_vec())
        .unwrap();
    let resized = recv_stdout_until(&mut worker, b"RESIZED", Duration::from_secs(5));
    assert!(
        String::from_utf8_lossy(&resized).contains("41 119"),
        "{}",
        String::from_utf8_lossy(&resized)
    );
    loop {
        if let ShellEvent::Exit(status) = worker.recv_timeout(Duration::from_secs(5)).unwrap() {
            assert_eq!(status.exit_code, 37);
            break;
        }
    }
}

#[cfg(unix)]
fn recv_stdout_until(worker: &mut ShellWorker, marker: &[u8], timeout: Duration) -> Vec<u8> {
    let deadline = std::time::Instant::now() + timeout;
    let mut output = Vec::new();
    while !output.windows(marker.len()).any(|window| window == marker) {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for PTY marker");
        match worker.recv_timeout(remaining).unwrap() {
            ShellEvent::Stdout(bytes) => output.extend(bytes),
            ShellEvent::Stderr(bytes) => panic!("PTY produced separate stderr: {bytes:?}"),
            ShellEvent::Exit(status) => panic!("PTY exited early: {status:?}"),
        }
    }
    output
}

fn raw_test_command() -> ShellOpen {
    #[cfg(windows)]
    let argv = vec![
        std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".to_owned()),
        "/D".to_owned(),
        "/C".to_owned(),
        "echo out& echo err 1>&2& exit /b 37".to_owned(),
    ];
    #[cfg(not(windows))]
    let argv = vec![
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        "printf out; printf err >&2; exit 37".to_owned(),
    ];
    ShellOpen {
        mode: ShellMode::Raw,
        argv,
        terminal_size: None,
        cwd: std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        term: "linux".to_owned(),
    }
}

fn raw_copy_command() -> ShellOpen {
    #[cfg(windows)]
    let argv = vec![
        std::path::Path::new(&std::env::var("SYSTEMROOT").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe")
            .to_string_lossy()
            .into_owned(),
        "-NoProfile".to_owned(),
        "-NonInteractive".to_owned(),
        "-Command".to_owned(),
        "$i=[Console]::OpenStandardInput();$o=[Console]::OpenStandardOutput();$i.CopyTo($o)"
            .to_owned(),
    ];
    #[cfg(not(windows))]
    let argv = vec!["/bin/cat".to_owned()];
    ShellOpen {
        mode: ShellMode::Raw,
        argv,
        terminal_size: None,
        cwd: std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        term: "linux".to_owned(),
    }
}
