use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use kindlebridge_broker::{AuthenticatedSession, Grant};
use kindlebridged::{DaemonState, DeviceInfo};
use serde::Serialize;

mod heartbeat;

#[derive(Serialize)]
struct Report<'a> {
    device: &'a DeviceInfo,
    services: Vec<kindlebridged::ServiceReport>,
}

#[derive(Debug, Parser)]
#[command(about = "KindleBridge unprivileged device daemon", version)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve persistent development KBP sessions over TCP.
    ServeTcp {
        #[arg(long, default_value = "127.0.0.1:4765")]
        listen: SocketAddr,
        #[arg(long)]
        serial: String,
        #[arg(long)]
        allow_peer: Option<IpAddr>,
        /// Writable root exposed through sync.v1.
        #[arg(long, default_value = "/mnt/us/kindlebridge-data")]
        sync_root: PathBuf,
    },
    /// Serve persistent KBP sessions over an externally prepared FunctionFS gadget.
    ServeUsb {
        /// Mounted FunctionFS directory containing ep0, ep1 and ep2.
        #[arg(long, default_value = "/dev/usb-ffs/kbp")]
        functionfs_dir: PathBuf,
        #[arg(long)]
        serial: String,
        /// Writable root exposed through sync.v1.
        #[arg(long, default_value = "/mnt/us/kindlebridge-data")]
        sync_root: PathBuf,
    },
    /// Run the bounded hardware bring-up echo probe.
    ProbeTcp {
        #[arg(long)]
        listen: SocketAddr,
        #[arg(long)]
        allow_peer: IpAddr,
    },
    #[command(hide = true)]
    RunAppSupervisor {
        #[arg(long)]
        entrypoint: PathBuf,
        #[arg(long)]
        stop_timeout_ms: u64,
        #[arg(long)]
        restart_on_failure: bool,
    },
    #[command(hide = true)]
    ExecApp {
        #[arg(long)]
        entrypoint: PathBuf,
    },
}

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("kindlebridged: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: Args) -> Result<(), String> {
    match arguments.command {
        Some(Command::ServeTcp {
            listen,
            serial,
            allow_peer,
            sync_root,
        }) => {
            if serial.is_empty() {
                return Err("serial must not be empty".to_owned());
            }
            if !listen.ip().is_loopback() && allow_peer.is_none() {
                return Err("--allow-peer is required when --listen is not loopback".to_owned());
            }
            let mut config = kindlebridged::server::ServerConfig::new(DeviceInfo::kt6(serial))
                .sync_root(sync_root);
            if let Some(peer) = allow_peer {
                config = config.allow_peer(peer);
            }
            let server = kindlebridged::server::TcpServer::bind(listen, config)
                .map_err(|error| error.to_string())?;
            heartbeat::start_from_environment()?;
            eprintln!(
                "KindleBridge device link listening on {}",
                server.local_addr().map_err(|error| error.to_string())?
            );
            server.serve_forever().map_err(|error| error.to_string())
        }
        Some(Command::ServeUsb {
            functionfs_dir,
            serial,
            sync_root,
        }) => {
            if serial.is_empty() {
                return Err("serial must not be empty".to_owned());
            }
            let config = kindlebridged::server::ServerConfig::new(DeviceInfo::kt6(serial))
                .sync_root(sync_root);
            let mut server = kindlebridged::server::UsbServer::open(&functionfs_dir, config)
                .map_err(|error| error.to_string())?;
            // A launcher heartbeat is a readiness signal, not merely proof
            // that argument parsing succeeded. Publish it only after the
            // FunctionFS descriptors and sync store have opened successfully.
            heartbeat::start_from_environment()?;
            eprintln!(
                "KindleBridge USB device link waiting on {}",
                functionfs_dir.display()
            );
            server.serve_forever().map_err(|error| error.to_string())
        }
        Some(Command::ProbeTcp { listen, allow_peer }) => run_probe(listen, allow_peer),
        Some(Command::RunAppSupervisor {
            entrypoint,
            stop_timeout_ms,
            restart_on_failure,
        }) => kindlebridged::app::run_application_supervisor(
            &entrypoint,
            Duration::from_millis(stop_timeout_ms),
            restart_on_failure,
        ),
        Some(Command::ExecApp { entrypoint }) => kindlebridged::app::exec_application(&entrypoint),
        None => report(),
    }
}

fn report() -> Result<(), String> {
    let daemon = DaemonState::new(DeviceInfo::kt6("UNPROVISIONED"));
    let session = AuthenticatedSession {
        host_key_id: [0; 32],
        session_id: [0; 32],
        grants: [Grant::DeviceRead].into_iter().collect::<BTreeSet<_>>(),
    };
    let report = Report {
        device: daemon.info(),
        services: daemon.service_report(&session),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn run_probe(listen: SocketAddr, allowed_peer: IpAddr) -> Result<(), String> {
    let config = kindlebridged::probe::ProbeConfig::kt6(allowed_peer);
    let server = kindlebridged::probe::ProbeServer::bind(listen, config)
        .map_err(|error| error.to_string())?;
    eprintln!(
        "KindleBridge one-shot probe listening on {}",
        server.local_addr().map_err(|error| error.to_string())?
    );
    let report = server.serve_once().map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string(&report).map_err(|error| error.to_string())?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_flag_reports_the_daemon_build() {
        let error = Args::try_parse_from(["kindlebridged", "--version"]).unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::DisplayVersion);
        assert!(error.to_string().contains(env!("CARGO_PKG_VERSION")));
    }
}
