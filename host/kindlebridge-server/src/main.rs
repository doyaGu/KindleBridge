use std::fs;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use interprocess::local_socket::{
    prelude::*, GenericFilePath, GenericNamespaced, ListenerNonblockingMode, ListenerOptions,
};
use kindlebridge_server::{
    reset_server_stop_requested, serve_streaming, server_stop_requested, ConnectedDeviceProvider,
    DeviceProvider, DeviceRecord, MemoryDeviceProvider, ReconnectingUsbProvider,
};
use kindlebridge_transport_usb::UsbMatch;

const AMAZON_VENDOR_ID: u16 = 0x1949;
const KT6_COMPOSITE_PRODUCT_ID: u16 = 0x9981;
const KINDLEBRIDGE_USB_SUBCLASS: u8 = 0x4b;
const KINDLEBRIDGE_USB_PROTOCOL: u8 = 0x01;

#[derive(Debug, Parser)]
#[command(about = "KindleBridge host JSON-RPC server")]
struct Args {
    /// Serve JSON-RPC 2.0 over stdin/stdout with LSP Content-Length framing.
    #[arg(long)]
    stdio: bool,

    /// Exit if the spawning CLI process disappears.
    #[arg(long, value_name = "LOOPBACK_ADDR", hide = true)]
    parent_watchdog: Option<SocketAddr>,

    /// Development/test device inventory JSON.
    #[arg(long, value_name = "PATH")]
    devices_file: Option<PathBuf>,

    /// Connect to a development device daemon over KBP/TCP. Repeat for multiple devices.
    #[arg(long, value_name = "IP:PORT")]
    tcp_device: Vec<SocketAddr>,

    /// Discover the KindleBridge vendor interface through WinUSB/libusb.
    #[arg(long)]
    usb: bool,

    /// Select one USB device by its exact USB serial number.
    #[arg(long, value_name = "SERIAL")]
    usb_serial: Option<String>,

    /// Composite USB product ID (decimal or 0x-prefixed hexadecimal).
    #[arg(
        long,
        value_name = "PID",
        default_value_t = KT6_COMPOSITE_PRODUCT_ID,
        value_parser = parse_usb_id
    )]
    usb_product_id: u16,

    /// Exit after this many idle seconds with no local clients.
    #[arg(long, default_value_t = 600, hide = true)]
    idle_timeout_secs: u64,
}

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("kindlebridge-server: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Args) -> Result<(), String> {
    let selected = usize::from(args.devices_file.is_some())
        + usize::from(!args.tcp_device.is_empty())
        + usize::from(args.usb);
    if selected > 1 {
        return Err("--devices-file, --tcp-device and --usb cannot be combined".to_owned());
    }
    if args.usb_serial.is_some() && !args.usb {
        return Err("--usb-serial requires --usb".to_owned());
    }
    let _parent_watchdog = start_parent_watchdog(args.parent_watchdog)?;
    let provider: Arc<dyn DeviceProvider> = if args.usb {
        Arc::new(ReconnectingUsbProvider::new(UsbMatch {
            vendor_id: AMAZON_VENDOR_ID,
            product_id: args.usb_product_id,
            interface_subclass: KINDLEBRIDGE_USB_SUBCLASS,
            interface_protocol: KINDLEBRIDGE_USB_PROTOCOL,
            serial_number: args.usb_serial,
        }))
    } else if !args.tcp_device.is_empty() {
        Arc::new(
            ConnectedDeviceProvider::connect(&args.tcp_device)
                .map_err(|error| error.to_string())?,
        )
    } else {
        let records = match args.devices_file {
            Some(path) => {
                let bytes = fs::read(&path)
                    .map_err(|error| format!("could not read {}: {error}", path.display()))?;
                serde_json::from_slice::<Vec<DeviceRecord>>(&bytes).map_err(|error| {
                    format!("invalid device inventory {}: {error}", path.display())
                })?
            }
            None => Vec::new(),
        };
        Arc::new(MemoryDeviceProvider::new(records))
    };
    if args.stdio {
        serve_streaming(
            &mut BufReader::new(io::stdin()),
            BufWriter::new(io::stdout()),
            provider,
        )
        .map_err(|error| error.to_string())
    } else {
        run_local_service(provider, Duration::from_secs(args.idle_timeout_secs))
    }
}

fn run_local_service(
    provider: Arc<dyn DeviceProvider>,
    idle_timeout: Duration,
) -> Result<(), String> {
    reset_server_stop_requested();
    let endpoint = local_endpoint();
    let listener = if GenericNamespaced::is_supported() {
        ListenerOptions::new()
            .name(
                endpoint
                    .as_str()
                    .to_ns_name::<GenericNamespaced>()
                    .map_err(|error| format!("invalid local pipe name: {error}"))?,
            )
            .nonblocking(ListenerNonblockingMode::Accept)
            .create_sync()
    } else {
        ListenerOptions::new()
            .name(
                endpoint
                    .as_str()
                    .to_fs_name::<GenericFilePath>()
                    .map_err(|error| format!("invalid local socket path: {error}"))?,
            )
            .try_overwrite(true)
            .nonblocking(ListenerNonblockingMode::Accept)
            .create_sync()
    }
    .map_err(|error| format!("could not listen on {endpoint}: {error}"))?;
    secure_unix_socket(&endpoint)?;

    let active_clients = Arc::new(AtomicUsize::new(0));
    let mut idle_since = Instant::now();
    loop {
        if server_stop_requested() {
            return Ok(());
        }
        match listener.accept() {
            Ok(connection) => {
                active_clients.fetch_add(1, Ordering::AcqRel);
                let provider = Arc::clone(&provider);
                let active_clients = Arc::clone(&active_clients);
                thread::Builder::new()
                    .name("kindlebridge-local-client".to_owned())
                    .spawn(move || {
                        let _guard = ClientGuard(active_clients);
                        let (reader, writer) = connection.split();
                        let _ = serve_streaming(
                            &mut BufReader::new(reader),
                            BufWriter::new(writer),
                            provider,
                        );
                    })
                    .map_err(|error| format!("could not start local client worker: {error}"))?;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if active_clients.load(Ordering::Acquire) == 0 {
                    if idle_since.elapsed() >= idle_timeout {
                        return Ok(());
                    }
                } else {
                    idle_since = Instant::now();
                }
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => return Err(format!("local server accept failed: {error}")),
        }
    }
}

struct ClientGuard(Arc<AtomicUsize>);

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
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

#[cfg(unix)]
fn secure_unix_socket(endpoint: &str) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(endpoint, fs::Permissions::from_mode(0o600))
        .map_err(|error| format!("could not secure local socket {endpoint}: {error}"))
}

#[cfg(not(unix))]
fn secure_unix_socket(_endpoint: &str) -> Result<(), String> {
    Ok(())
}

struct ParentWatchdog {
    normal_exit: Arc<AtomicBool>,
}

impl Drop for ParentWatchdog {
    fn drop(&mut self) {
        self.normal_exit.store(true, Ordering::Release);
    }
}

fn start_parent_watchdog(address: Option<SocketAddr>) -> Result<Option<ParentWatchdog>, String> {
    let Some(address) = address else {
        return Ok(None);
    };
    if !address.ip().is_loopback() {
        return Err("--parent-watchdog must use a loopback address".to_owned());
    }
    let mut stream = TcpStream::connect(address)
        .map_err(|error| format!("could not connect parent watchdog: {error}"))?;
    stream
        .write_all(&std::process::id().to_le_bytes())
        .map_err(|error| format!("could not initialize parent watchdog: {error}"))?;
    let normal_exit = Arc::new(AtomicBool::new(false));
    let watcher_exit = Arc::clone(&normal_exit);
    std::thread::Builder::new()
        .name("kindlebridge-parent-watchdog".to_owned())
        .spawn(move || {
            let mut signal = [0_u8; 1];
            let outcome = stream.read(&mut signal);
            if watcher_exit.load(Ordering::Acquire) {
                return;
            }
            match outcome {
                Ok(0) => eprintln!("kindlebridge-server: spawning CLI disconnected"),
                Ok(_) => eprintln!("kindlebridge-server: spawning CLI requested shutdown"),
                Err(error) => eprintln!("kindlebridge-server: parent watchdog failed: {error}"),
            }
            std::process::exit(1);
        })
        .map_err(|error| format!("could not start parent watchdog: {error}"))?;
    Ok(Some(ParentWatchdog { normal_exit }))
}

fn parse_usb_id(value: &str) -> Result<u16, String> {
    let value = value.trim();
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        u16::from_str_radix(hex, 16).map_err(|error| error.to_string())
    } else {
        value.parse::<u16>().map_err(|error| error.to_string())
    }
}
