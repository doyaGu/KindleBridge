use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use clap::{Args, Subcommand};
use kindlebridge_schema::{
    methods, AppInstallParams, AppList, AppLogParams, AppLogSnapshot, AppState, AppSummary,
    AppTargetParams, SerialParams,
};
use serde_json::Value;

use super::{call_typed, normalize_host_path, pretty_json, CliError, RpcCaller};

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

pub(super) fn execute<C: RpcCaller>(
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
            format_result(value, &app, json_output)
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
                let stdout = decode_log(&snapshot.stdout.data_base64)?;
                let stderr = decode_log(&snapshot.stderr.data_base64)?;
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
            format_result(value, &app, json_output)
        }
    }
}

fn decode_log(encoded: &str) -> Result<Vec<u8>, CliError> {
    BASE64
        .decode(encoded)
        .map_err(|_| CliError::InvalidResult { kind: "app log" })
}

pub(super) fn format_result(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn app(state: AppState, pid: Option<u32>) -> AppSummary {
        AppSummary {
            app_id: "org.example.reader".to_owned(),
            version: "1.2.3".to_owned(),
            state,
            rollback_available: true,
            pid,
        }
    }

    #[test]
    fn human_summary_formats_each_runtime_state() {
        assert_eq!(
            format_app(&app(AppState::Stopped, None)),
            "org.example.reader\t1.2.3\tstopped"
        );
        assert_eq!(
            format_app(&app(AppState::Running, Some(2048))),
            "org.example.reader\t1.2.3\trunning pid=2048"
        );
        assert_eq!(
            format_app(&app(AppState::Failed, None)),
            "org.example.reader\t1.2.3\tfailed"
        );
    }

    #[test]
    fn log_decoder_preserves_binary_and_rejects_malformed_base64() {
        let bytes = b"stdout\0\xff";
        assert_eq!(decode_log(&BASE64.encode(bytes)).unwrap(), bytes);
        assert!(matches!(
            decode_log("not base64!"),
            Err(CliError::InvalidResult { kind: "app log" })
        ));
    }
}
