//! Host-side provider backed by persistent KBP/TCP device sessions.

mod sync_client;

use std::fs::File;
use std::io::{Read, Write};
use std::net::SocketAddr;
#[cfg(all(test, unix))]
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use kindlebridge_bundle::{verify, BundleKind, VerifyOptions};
#[cfg(test)]
use kindlebridge_schema::device_protocol::SYNC_CREDIT_BATCH_SIZE;
use kindlebridge_schema::device_protocol::{
    is_valid_session_id, DeviceAppInstallParams, DeviceCall, DeviceHello, DeviceReply, HostHello,
    ShellOpen, DEFAULT_CONNECTION_WINDOW, DEFAULT_STREAM_WINDOW, MAX_HOST_TO_DEVICE_PAYLOAD,
    PROTOCOL_VERSION, RPC_SERVICE, SHELL_STREAM_WINDOW, SHELL_V2_FEATURE, SHELL_V2_SERVICE,
    SYNC_FEATURE,
};
#[cfg(test)]
use kindlebridge_schema::device_protocol::{SyncReply, SyncRequest, SYNC_SERVICE};
use kindlebridge_schema::device_rpc::{self as rpc_method, RpcMethod};
use kindlebridge_schema::shell_protocol::{PacketSource, ShellPacket, ShellStreamState};
use kindlebridge_schema::{
    error_codes, AppInstallParams, AppList, AppSummary, AppTargetParams, DeviceFeatures,
    DeviceState, DeviceSummary, ExecParams, ExecResult, LogSnapshot, LogTailParams, ProcessList,
    ProcessSignalParams, ProcessSummary, RpcError, SerialParams, SyncListParams, SyncListResult,
    SyncMkdirParams, SyncMkdirResult, SyncPullParams, SyncPullResult, SyncPushParams,
    SyncPushResult, SyncStatus, SyncStatusParams, TransferState,
};
use kindlebridge_transport::{
    actor::{
        Connection, ConnectionError, FrameSink as ActorFrameSink, FrameSource as ActorFrameSource,
        Stream as ActorStream,
    },
    TrafficClass,
};
use kindlebridge_transport_tcp::{
    ErrorClass, FrameIo, FrameReader, FrameWriter, ShutdownMode, SplitFrameStream, TcpFrameStream,
    TransportConfig, TransportError,
};
use kindlebridge_transport_usb::{
    BufferConfig, TransportError as UsbTransportError, UsbMatch, UsbReader, UsbWriter,
};
use kindlebridge_wire::{
    Command, DecodeLimits, EndpointRole, Frame, FrameContext, Header, ProtocolError, SessionConfig,
    SessionState, WireError, FLAG_END_STREAM,
};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use crate::{DeviceProvider, ProviderError, SyncObserver};
use sync_client::SyncClient;

const SESSION_IO_TIMEOUT: Duration = Duration::from_secs(10 * 60 + 30);
// Match the device's order-2 FunctionFS request size. Larger host transfers
// force the KT6 4.9 gadget stack into fragile high-order page allocations.
const USB_TRANSFER_SIZE: usize = 16 * 1024;
// Four order-2 requests keep 64 KiB in flight without asking the KT6 gadget
// stack for larger, fragile high-order allocations.
const USB_READ_QUEUE_DEPTH: usize = 4;
const USB_WRITE_QUEUE_DEPTH: usize = 4;
const USB_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
const USB_RECOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const USB_RECOVERY_DRAIN_POLL: Duration = Duration::from_millis(250);

struct ConnectedDevice {
    summary: DeviceSummary,
    features: DeviceFeatures,
    session: ActorDeviceSession,
}

pub struct ConnectedDeviceProvider {
    devices: Vec<ConnectedDevice>,
}

impl ConnectedDeviceProvider {
    pub fn connect(addresses: &[SocketAddr]) -> Result<Self, ProviderError> {
        let mut devices = Vec::with_capacity(addresses.len());
        for address in addresses {
            let (session, hello) = ActorDeviceSession::connect(*address)
                .map_err(|error| ProviderError::new(error.to_string()))?;
            if devices
                .iter()
                .any(|device: &ConnectedDevice| device.summary.serial == hello.serial)
            {
                return Err(ProviderError::new(format!(
                    "duplicate device serial {}",
                    hello.serial
                )));
            }
            let mut features = hello.features;
            features.sort();
            features.dedup();
            devices.push(ConnectedDevice {
                summary: DeviceSummary {
                    serial: hello.serial.clone(),
                    model: hello.model,
                    state: DeviceState::Online,
                    transport: "tcp".to_owned(),
                },
                features: DeviceFeatures {
                    serial: hello.serial,
                    protocol_version: hello.protocol_version,
                    features,
                },
                session,
            });
        }
        devices.sort_by(|left, right| left.summary.serial.cmp(&right.summary.serial));
        Ok(Self { devices })
    }

    /// Open the exact KindleBridge WinUSB interface and perform the normal KBP
    /// device handshake. MTP remains owned by its separate composite interface.
    pub fn connect_usb(criteria: &UsbMatch) -> Result<Self, ProviderError> {
        let (session, hello) = match ActorDeviceSession::connect_usb(criteria) {
            Ok(link) => link,
            Err(LinkError::Usb(UsbTransportError::DeviceNotFound)) => {
                return Ok(Self {
                    devices: Vec::new(),
                });
            }
            Err(LinkError::IncompatibleProtocol { device, host }) => {
                return Err(ProviderError::public(format!(
                    "Incompatible KindleBridge daemon protocol {device}; host requires {host}. Install the matching KindleBridge package"
                )));
            }
            Err(_error) => {
                return Err(ProviderError::public(format!(
                    "Could not establish KindleBridge protocol {PROTOCOL_VERSION}. Ensure development mode is active and install the matching KindleBridge package"
                )));
            }
        };
        let mut features = hello.features;
        features.sort();
        features.dedup();
        Ok(Self {
            devices: vec![ConnectedDevice {
                summary: DeviceSummary {
                    serial: hello.serial.clone(),
                    model: hello.model,
                    state: DeviceState::Online,
                    transport: "usb".to_owned(),
                },
                features: DeviceFeatures {
                    serial: hello.serial,
                    protocol_version: hello.protocol_version,
                    features,
                },
                session,
            }],
        })
    }

    /// Open a persistent `shell.v2` stream on one connected device.
    pub fn open_shell(&self, serial: &str, open: ShellOpen) -> Result<DeviceShell, RpcError> {
        let device = self.require_feature(serial, SHELL_V2_FEATURE)?;
        device.session.open_shell(open).map_err(link_rpc_error)
    }

    fn find(&self, serial: &str) -> Option<&ConnectedDevice> {
        self.devices
            .iter()
            .find(|device| device.summary.serial == serial)
    }

    pub(crate) fn is_online(&self) -> bool {
        !self.devices.is_empty()
            && self
                .devices
                .iter()
                .all(|device| device.session.connection.is_online())
    }
}

impl ConnectedDeviceProvider {
    fn list(&self) -> Result<Vec<DeviceSummary>, ProviderError> {
        Ok(self
            .devices
            .iter()
            .map(|device| device.summary.clone())
            .collect())
    }

    fn features(&self, serial: &str) -> Result<Option<DeviceFeatures>, ProviderError> {
        Ok(self.find(serial).map(|device| device.features.clone()))
    }

    fn ping(&self, serial: &str) -> Result<bool, RpcError> {
        let Some(device) = self.find(serial) else {
            return Ok(false);
        };
        device.session.ping().map_err(link_rpc_error)?;
        Ok(true)
    }

    fn exec(&self, params: &ExecParams) -> Result<Option<ExecResult>, RpcError> {
        let Some(device) = self.find(&params.serial) else {
            return Ok(None);
        };
        self.typed_call::<rpc_method::ExecRun>(device, params)
            .map(Some)
    }

    fn sync_push(&self, params: SyncPushParams) -> Result<SyncPushResult, RpcError> {
        self.sync_push_observed(params, &SyncObserver::default())
    }

    fn sync_push_observed(
        &self,
        params: SyncPushParams,
        observer: &SyncObserver,
    ) -> Result<SyncPushResult, RpcError> {
        let device = self.require_feature(&params.serial, SYNC_FEATURE)?;
        device.session.sync_client().push_observed(params, observer)
    }

    fn sync_pull(&self, params: SyncPullParams) -> Result<SyncPullResult, RpcError> {
        self.sync_pull_observed(params, &SyncObserver::default())
    }

    fn sync_pull_observed(
        &self,
        params: SyncPullParams,
        observer: &SyncObserver,
    ) -> Result<SyncPullResult, RpcError> {
        let device = self.require_feature(&params.serial, SYNC_FEATURE)?;
        device.session.sync_client().pull_observed(params, observer)
    }

    fn sync_status(&self, params: &SyncStatusParams) -> Result<SyncStatus, RpcError> {
        self.remote_call::<rpc_method::SyncStatus>(&params.serial, params)
    }

    fn sync_list(&self, params: &SyncListParams) -> Result<SyncListResult, RpcError> {
        self.remote_call::<rpc_method::SyncList>(&params.serial, params)
    }

    fn sync_mkdir(&self, params: &SyncMkdirParams) -> Result<SyncMkdirResult, RpcError> {
        self.remote_call::<rpc_method::SyncMkdir>(&params.serial, params)
    }

    fn app_install(&self, params: AppInstallParams) -> Result<AppSummary, RpcError> {
        let device = self.require_feature(&params.serial, rpc_method::AppInstall::FEATURE)?;
        self.require_feature(&params.serial, SYNC_FEATURE)?;
        sync_client::validate_host_path(&params.bundle_path)?;
        let mut file = File::open(&params.bundle_path)
            .map_err(|error| sync_client::host_file_error("read", &params.bundle_path, &error))?;
        let metadata = file
            .metadata()
            .map_err(|error| sync_client::host_file_error("stat", &params.bundle_path, &error))?;
        if !metadata.is_file() {
            return Err(RpcError::invalid_params("bundle_path must name a file"));
        }
        let total_size = metadata.len();
        verify(&mut file, &VerifyOptions::default())
            .map_err(|error| host_bundle_error("verify", &error))
            .and_then(|verified| {
                if verified.inspection.envelope.kind == BundleKind::Application {
                    Ok(())
                } else {
                    Err(RpcError::new(
                        error_codes::APP_INSTALL_FAILED,
                        "Application install failed",
                    )
                    .with_data(serde_json::json!({
                        "stage": "host_verify",
                        "reason": "bundle_kind",
                        "detail": "app install accepts application bundles only",
                    })))
                }
            })?;
        let file_hash = sync_client::hash_file(&mut file, total_size)?;
        let remote_path = format!("packages/kbb/{file_hash}.kbb");
        let sync_params = SyncPushParams {
            serial: params.serial.clone(),
            local_path: params.bundle_path,
            remote_path: remote_path.clone(),
            transfer_id: None,
            block_size: kindlebridge_schema::DEFAULT_SYNC_BLOCK_SIZE,
        };
        let pushed = device
            .session
            .sync_client()
            .push_open_file(
                &sync_params,
                &mut file,
                total_size,
                &file_hash,
                &SyncObserver::default(),
            )
            .map_err(link_rpc_error)?;
        if pushed.state != TransferState::Complete || pushed.accepted_offset != total_size {
            return Err(RpcError::new(
                error_codes::INVALID_STATE,
                "Bundle upload did not complete",
            )
            .with_data(serde_json::json!({
                "transfer_id": pushed.transfer_id,
                "accepted_offset": pushed.accepted_offset,
                "total_size": total_size,
            })));
        }
        let device_params = DeviceAppInstallParams {
            serial: params.serial,
            remote_path,
            file_hash,
        };
        self.typed_call::<rpc_method::AppInstall>(device, &device_params)
    }

    fn app_start(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call::<rpc_method::AppStart>(&params.serial, params)
    }

    fn app_log(
        &self,
        params: &kindlebridge_schema::AppLogParams,
    ) -> Result<kindlebridge_schema::AppLogSnapshot, RpcError> {
        self.remote_call::<rpc_method::AppLog>(&params.serial, params)
    }

    fn app_stop(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call::<rpc_method::AppStop>(&params.serial, params)
    }

    fn app_restart(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call::<rpc_method::AppRestart>(&params.serial, params)
    }

    fn app_rollback(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call::<rpc_method::AppRollback>(&params.serial, params)
    }

    fn app_uninstall(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call::<rpc_method::AppUninstall>(&params.serial, params)
    }

    fn app_list(&self, params: &SerialParams) -> Result<AppList, RpcError> {
        self.remote_call::<rpc_method::AppList>(&params.serial, params)
    }

    fn process_list(&self, params: &SerialParams) -> Result<ProcessList, RpcError> {
        self.remote_call::<rpc_method::ProcessList>(&params.serial, params)
    }

    fn process_signal(&self, params: &ProcessSignalParams) -> Result<ProcessSummary, RpcError> {
        self.remote_call::<rpc_method::ProcessSignal>(&params.serial, params)
    }

    fn log_tail(&self, params: &LogTailParams) -> Result<LogSnapshot, RpcError> {
        self.remote_call::<rpc_method::LogTail>(&params.serial, params)
    }

    fn shell_open(
        &self,
        params: &kindlebridge_schema::ShellOpenParams,
    ) -> Result<std::sync::Arc<dyn crate::ShellStream>, RpcError> {
        ConnectedDeviceProvider::open_shell(self, &params.serial, params.open.clone())
            .map(|shell| std::sync::Arc::new(shell) as std::sync::Arc<dyn crate::ShellStream>)
    }
}

impl DeviceProvider for ConnectedDeviceProvider {
    fn perform(
        &self,
        operation: crate::DeviceOperation,
    ) -> Result<crate::DeviceOperationResult, RpcError> {
        use crate::{DeviceOperation, DeviceOperationResult};

        Ok(match operation {
            DeviceOperation::List => {
                DeviceOperationResult::Devices(self.list().map_err(crate::provider_rpc_error)?)
            }
            DeviceOperation::Features(serial) => DeviceOperationResult::Features(
                self.features(&serial).map_err(crate::provider_rpc_error)?,
            ),
            DeviceOperation::Ping(serial) => DeviceOperationResult::Ping(self.ping(&serial)?),
            DeviceOperation::Exec(params) => DeviceOperationResult::Exec(self.exec(&params)?),
            DeviceOperation::SyncPush(params) => {
                DeviceOperationResult::SyncPush(self.sync_push(params)?)
            }
            DeviceOperation::SyncPull(params) => {
                DeviceOperationResult::SyncPull(self.sync_pull(params)?)
            }
            DeviceOperation::SyncStatus(params) => {
                DeviceOperationResult::SyncStatus(self.sync_status(&params)?)
            }
            DeviceOperation::SyncList(params) => {
                DeviceOperationResult::SyncList(self.sync_list(&params)?)
            }
            DeviceOperation::SyncMkdir(params) => {
                DeviceOperationResult::SyncMkdir(self.sync_mkdir(&params)?)
            }
            DeviceOperation::AppInstall(params) => {
                DeviceOperationResult::App(self.app_install(params)?)
            }
            DeviceOperation::AppStart(params) => {
                DeviceOperationResult::App(self.app_start(&params)?)
            }
            DeviceOperation::AppStop(params) => DeviceOperationResult::App(self.app_stop(&params)?),
            DeviceOperation::AppRestart(params) => {
                DeviceOperationResult::App(self.app_restart(&params)?)
            }
            DeviceOperation::AppRollback(params) => {
                DeviceOperationResult::App(self.app_rollback(&params)?)
            }
            DeviceOperation::AppUninstall(params) => {
                DeviceOperationResult::App(self.app_uninstall(&params)?)
            }
            DeviceOperation::AppList(params) => {
                DeviceOperationResult::Apps(self.app_list(&params)?)
            }
            DeviceOperation::AppLog(params) => {
                DeviceOperationResult::AppLog(self.app_log(&params)?)
            }
            DeviceOperation::ProcessList(params) => {
                DeviceOperationResult::Processes(self.process_list(&params)?)
            }
            DeviceOperation::ProcessSignal(params) => {
                DeviceOperationResult::Process(self.process_signal(&params)?)
            }
            DeviceOperation::LogTail(params) => DeviceOperationResult::Log(self.log_tail(&params)?),
        })
    }

    fn sync_push_observed(
        &self,
        params: SyncPushParams,
        observer: &SyncObserver,
    ) -> Result<SyncPushResult, RpcError> {
        ConnectedDeviceProvider::sync_push_observed(self, params, observer)
    }

    fn sync_pull_observed(
        &self,
        params: SyncPullParams,
        observer: &SyncObserver,
    ) -> Result<SyncPullResult, RpcError> {
        ConnectedDeviceProvider::sync_pull_observed(self, params, observer)
    }

    fn shell_open(
        &self,
        params: &kindlebridge_schema::ShellOpenParams,
    ) -> Result<std::sync::Arc<dyn crate::ShellStream>, RpcError> {
        ConnectedDeviceProvider::shell_open(self, params)
    }
}

impl ConnectedDeviceProvider {
    fn require_feature(&self, serial: &str, feature: &str) -> Result<&ConnectedDevice, RpcError> {
        let device = self
            .find(serial)
            .ok_or_else(|| RpcError::device_not_found(serial))?;
        if device
            .features
            .features
            .iter()
            .any(|value| value == feature)
        {
            Ok(device)
        } else {
            Err(RpcError::feature_unavailable(serial, feature))
        }
    }

    fn remote_call<M: RpcMethod>(
        &self,
        serial: &str,
        params: &M::Params,
    ) -> Result<M::Result, RpcError> {
        let device = self.require_feature(serial, M::FEATURE)?;
        self.typed_call::<M>(device, params)
    }

    fn typed_call<M: RpcMethod>(
        &self,
        device: &ConnectedDevice,
        params: &M::Params,
    ) -> Result<M::Result, RpcError> {
        let value = device
            .session
            .call(M::METHOD, params)
            .map_err(link_rpc_error)?;
        serde_json::from_value(value).map_err(|_| RpcError::internal_error())
    }
}

fn link_rpc_error(error: LinkError) -> RpcError {
    match error {
        LinkError::Remote(error) => error,
        _ => RpcError::new(error_codes::SERVER_NOT_READY, "Device link is unavailable"),
    }
}

#[derive(Clone, Debug)]
struct ActorDeviceSession {
    connection: Connection,
}

impl ActorDeviceSession {
    fn sync_client(&self) -> SyncClient {
        SyncClient::new(self.connection.clone())
    }

    fn ping(&self) -> Result<(), LinkError> {
        self.connection.ping().map_err(LinkError::Connection)
    }

    fn connect(address: SocketAddr) -> Result<(Self, DeviceHello), LinkError> {
        let (limits, transport) = session_transport_config();
        let mut sink = TcpFrameStream::connect(address, transport)?;
        let source = sink.try_clone()?;
        let session_id = new_session_id()?;
        let (state, hello) = negotiate(&mut sink, limits, false, &session_id)?;
        let (connection, _incoming) =
            Connection::start(state, TcpActorSource(source), TcpActorSink(sink));
        Ok((Self { connection }, hello))
    }

    fn connect_usb(criteria: &UsbMatch) -> Result<(Self, DeviceHello), LinkError> {
        let initial_started = Instant::now();
        match Self::connect_usb_attempt(criteria, false) {
            Err(error) if error.allows_usb_handshake_recovery() => {
                trace_usb_recovery(format_args!(
                    "initial probe failed after {} ms: {error}",
                    initial_started.elapsed().as_millis()
                ));
                let recovery_started = Instant::now();
                let result = Self::connect_usb_attempt(criteria, true);
                trace_usb_recovery(format_args!(
                    "recovery attempt finished after {} ms ({})",
                    recovery_started.elapsed().as_millis(),
                    result
                        .as_ref()
                        .map(|_| "ok".to_owned())
                        .unwrap_or_else(|error| format!("error: {error}"))
                ));
                result
            }
            result => result,
        }
    }

    fn connect_usb_attempt(
        criteria: &UsbMatch,
        recover_abandoned_frame: bool,
    ) -> Result<(Self, DeviceHello), LinkError> {
        let (limits, transport_config) = session_transport_config();
        let transport = kindlebridge_transport_usb::open(criteria, usb_buffer_config())?;
        let (mut reader, mut writer) = transport.split();
        if recover_abandoned_frame {
            trace_usb_recovery(format_args!("recovery transport opened"));
            writer.set_write_timeout(USB_RECOVERY_TIMEOUT);
            write_usb_recovery_exchange(&mut reader, &mut writer, limits)?;
            trace_usb_recovery(format_args!("recovery exchange finished"));
            reader.set_read_timeout(USB_RECOVERY_TIMEOUT);
        }
        let mut stream = SplitFrameStream::new(reader, writer, transport_config)?;
        let session_id = new_session_id()?;
        let (state, hello) = negotiate(&mut stream, limits, recover_abandoned_frame, &session_id)?;
        if recover_abandoned_frame {
            trace_usb_recovery(format_args!("recovery HELLO finished"));
        }
        stream.reader_mut().set_read_timeout(Duration::MAX);
        stream.writer_mut().set_write_timeout(Duration::MAX);
        let (reader, writer) = stream.into_inner();
        let source = UsbActorSource(FrameReader::new(reader, transport_config)?);
        let sink = UsbActorSink(FrameWriter::new(writer, transport_config)?);
        let (connection, _incoming) = Connection::start(state, source, sink);
        Ok((Self { connection }, hello))
    }

    fn call(&self, method: &str, params: &impl Serialize) -> Result<Value, LinkError> {
        let mut stream = self
            .connection
            .open(RPC_SERVICE, DEFAULT_STREAM_WINDOW, TrafficClass::Bulk)
            .map_err(|error| {
                trace_usb_recovery(format_args!("RPC {method} OPEN failed: {error:?}"));
                LinkError::Connection(error)
            })?;
        trace_usb_recovery(format_args!(
            "RPC {method} OPEN accepted as stream {}",
            stream.id()
        ));
        let call = DeviceCall {
            method: method.to_owned(),
            params: serde_json::to_value(params)?,
        };
        stream.send_data(encode(&call)?, true).map_err(|error| {
            trace_usb_recovery(format_args!("RPC {method} DATA failed: {error:?}"));
            LinkError::Connection(error)
        })?;
        let response = actor_data(&mut stream).map_err(|error| {
            trace_usb_recovery(format_args!("RPC {method} response failed: {error:?}"));
            error
        })?;
        if response.header.flags & FLAG_END_STREAM == 0 {
            return Err(LinkError::UnexpectedFrame(
                "device reply did not end the stream",
            ));
        }
        actor_close(&mut stream)?;
        let reply: DeviceReply = decode(&response.payload, "device reply")?;
        reply.into_result().map_err(LinkError::Remote)
    }

    fn open_shell(&self, open: ShellOpen) -> Result<DeviceShell, LinkError> {
        let stream = self.connection.open(
            SHELL_V2_SERVICE,
            SHELL_STREAM_WINDOW,
            TrafficClass::Interactive,
        )?;
        stream.send_data(encode(&open)?, false)?;
        Ok(DeviceShell {
            stream,
            input_state: Mutex::new(ShellStreamState::new(open.mode)),
            output_state: Mutex::new(ShellStreamState::new(open.mode)),
        })
    }
}

/// Concurrent host handle for one persistent device shell stream.
#[derive(Debug)]
pub struct DeviceShell {
    stream: ActorStream,
    input_state: Mutex<ShellStreamState>,
    output_state: Mutex<ShellStreamState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeviceShellEvent {
    Packet(ShellPacket),
    Closed,
}

impl DeviceShell {
    pub fn send(&self, packet: ShellPacket) -> Result<(), ProviderError> {
        if !matches!(
            packet,
            ShellPacket::Stdin(_) | ShellPacket::CloseStdin | ShellPacket::Resize(_)
        ) {
            return Err(ProviderError::new("invalid host shell packet direction"));
        }
        if let Err(error) = self
            .input_state
            .lock()
            .map_err(|_| ProviderError::new("shell input state is unavailable"))?
            .accept(&packet)
        {
            let _ = self.stream.reset(error.to_string());
            return Err(ProviderError::new(error.to_string()));
        }
        let encoded = packet
            .encode()
            .map_err(|error| ProviderError::new(error.to_string()))?;
        self.stream
            .send_data(encoded, false)
            .map_err(|error| ProviderError::new(error.to_string()))
    }

    pub fn recv(&self) -> Result<DeviceShellEvent, ProviderError> {
        let frame = self
            .stream
            .recv()
            .map_err(|error| ProviderError::new(error.to_string()))?;
        match frame.header.command {
            Command::Data => {
                let packet = match ShellPacket::decode(&frame.payload, PacketSource::Device) {
                    Ok(packet) => packet,
                    Err(error) => {
                        let _ = self.stream.reset(error.to_string());
                        return Err(ProviderError::new(error.to_string()));
                    }
                };
                if let Err(error) = self
                    .output_state
                    .lock()
                    .map_err(|_| ProviderError::new("shell output state is unavailable"))?
                    .accept(&packet)
                {
                    let _ = self.stream.reset(error.to_string());
                    return Err(ProviderError::new(error.to_string()));
                }
                Ok(DeviceShellEvent::Packet(packet))
            }
            Command::Close => Ok(DeviceShellEvent::Closed),
            Command::Reset => Err(ProviderError::new(format!(
                "device reset shell stream: {}",
                String::from_utf8_lossy(&frame.payload)
            ))),
            _ => Err(ProviderError::new(format!(
                "unexpected {:?} on shell stream",
                frame.header.command
            ))),
        }
    }

    pub fn close(&self) -> Result<(), ProviderError> {
        self.stream
            .reset("host closed shell")
            .map_err(|error| ProviderError::new(error.to_string()))
    }
}

struct TcpActorSource(TcpFrameStream);

impl ActorFrameSource for TcpActorSource {
    fn read_frame(&mut self) -> Result<Frame, String> {
        self.0.read_frame().map_err(|error| error.to_string())
    }
}

struct TcpActorSink(TcpFrameStream);

impl Drop for TcpActorSink {
    fn drop(&mut self) {
        let _ = self.0.shutdown(ShutdownMode::Both);
    }
}

impl ActorFrameSink for TcpActorSink {
    fn write_frame(&mut self, frame: &Frame) -> Result<(), String> {
        self.0.write_frame(frame).map_err(|error| error.to_string())
    }

    fn flush(&mut self) -> Result<(), String> {
        self.0.flush().map_err(|error| error.to_string())
    }
}

struct UsbActorSource(FrameReader<UsbReader>);

impl ActorFrameSource for UsbActorSource {
    fn read_frame(&mut self) -> Result<Frame, String> {
        self.0
            .read_frame()
            .inspect(|frame| {
                trace_usb_recovery(format_args!(
                    "actor inbound {:?} stream={} sequence={} flags={:#x} bytes={}",
                    frame.header.command,
                    frame.header.stream_id,
                    frame.header.sequence,
                    frame.header.flags,
                    frame.header.payload_length
                ));
            })
            .map_err(|error| {
                trace_usb_recovery(format_args!("actor read failed: {error}"));
                error.to_string()
            })
    }
}

struct UsbActorSink(FrameWriter<UsbWriter>);

impl ActorFrameSink for UsbActorSink {
    fn write_frame(&mut self, frame: &Frame) -> Result<(), String> {
        trace_usb_recovery(format_args!(
            "actor outbound {:?} stream={} sequence={} flags={:#x} bytes={}",
            frame.header.command,
            frame.header.stream_id,
            frame.header.sequence,
            frame.header.flags,
            frame.header.payload_length
        ));
        self.0.write_frame(frame).map_err(|error| {
            trace_usb_recovery(format_args!("actor write failed: {error}"));
            error.to_string()
        })
    }

    fn flush(&mut self) -> Result<(), String> {
        self.0.flush().map_err(|error| {
            trace_usb_recovery(format_args!("actor flush failed: {error}"));
            error.to_string()
        })
    }
}

fn actor_data(stream: &mut ActorStream) -> Result<Frame, LinkError> {
    let frame = stream.recv()?;
    expect(&frame, Command::Data, stream.id())?;
    Ok(frame)
}

fn actor_close(stream: &mut ActorStream) -> Result<(), LinkError> {
    let frame = stream.recv()?;
    expect(&frame, Command::Close, stream.id())
}

fn negotiate(
    stream: &mut dyn FrameIo,
    limits: DecodeLimits,
    discard_stale_frames: bool,
    session_id: &str,
) -> Result<(SessionState, DeviceHello), LinkError> {
    let mut state = SessionState::new(SessionConfig::new(EndpointRole::Host, limits));
    let hello = HostHello {
        protocol_version: PROTOCOL_VERSION,
        session_id: session_id.to_owned(),
        client_name: "kindlebridge-server".to_owned(),
        initial_connection_window: DEFAULT_CONNECTION_WINDOW,
    };
    let hello_frame = frame(Command::Hello, 0, 0, encode(&hello)?)?;
    state.process_outbound(
        &hello_frame.header,
        FrameContext::hello(DEFAULT_CONNECTION_WINDOW),
    )?;
    stream.write_frame(&hello_frame)?;
    stream.flush()?;

    let device_frame = loop {
        let candidate = stream.read_frame()?;
        if !discard_stale_frames {
            break candidate;
        }
        if candidate.header.command == Command::Hello
            && candidate.header.stream_id == 0
            && decode::<DeviceHello>(&candidate.payload, "stale device HELLO")
                .is_ok_and(|hello| hello.session_id == session_id)
        {
            break candidate;
        }
    };
    expect(&device_frame, Command::Hello, 0)?;
    let device: DeviceHello = decode(&device_frame.payload, "device HELLO")?;
    validate_device_hello(&device, session_id)?;
    state.process_inbound(
        &device_frame.header,
        FrameContext::hello(device.initial_connection_window),
    )?;
    Ok((state, device))
}

fn trace_usb_recovery(arguments: std::fmt::Arguments<'_>) {
    if std::env::var_os("KINDLEBRIDGE_TRACE_USB_RECOVERY").is_some() {
        eprintln!("kindlebridge-server: USB recovery: {arguments}");
    }
}

fn usb_buffer_config() -> BufferConfig {
    BufferConfig {
        transfer_size: USB_TRANSFER_SIZE,
        read_queue_depth: USB_READ_QUEUE_DEPTH,
        write_queue_depth: USB_WRITE_QUEUE_DEPTH,
        // Before HELLO, a timeout is safe: dropping the failed transport cancels
        // all partially submitted recovery bytes and no KBP session exists yet.
        // Both endpoint timeouts are restored to MAX immediately after HELLO so
        // application traffic continues to use pure USB backpressure.
        read_timeout: USB_HANDSHAKE_TIMEOUT,
        write_timeout: USB_HANDSHAKE_TIMEOUT,
    }
}

fn session_transport_config() -> (DecodeLimits, TransportConfig) {
    let limits = DecodeLimits::new(DEFAULT_CONNECTION_WINDOW, DEFAULT_CONNECTION_WINDOW);
    let transport = TransportConfig {
        read_timeout: Some(SESSION_IO_TIMEOUT),
        write_timeout: Some(SESSION_IO_TIMEOUT),
        ..TransportConfig::new(limits)
    };
    (limits, transport)
}

fn host_bundle_error(stage: &str, error: &kindlebridge_bundle::Error) -> RpcError {
    RpcError::new(
        error_codes::APP_INSTALL_FAILED,
        "Application install failed",
    )
    .with_data(serde_json::json!({
        "stage": format!("host_{stage}"),
        "reason": format!("{:?}", error.code),
        "detail": error.message,
    }))
}

fn validate_device_hello(hello: &DeviceHello, expected_session_id: &str) -> Result<(), LinkError> {
    if hello.protocol_version != PROTOCOL_VERSION {
        return Err(LinkError::IncompatibleProtocol {
            device: hello.protocol_version,
            host: PROTOCOL_VERSION,
        });
    }
    if !is_valid_session_id(&hello.session_id)
        || hello.session_id != expected_session_id
        || hello.serial.is_empty()
        || hello.model.is_empty()
        || hello.initial_connection_window == 0
        || hello.initial_connection_window > DEFAULT_CONNECTION_WINDOW
    {
        return Err(LinkError::InvalidHello);
    }
    Ok(())
}

fn new_session_id() -> Result<String, LinkError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut bytes = [0_u8; 16];
    getrandom::getrandom(&mut bytes).map_err(|_| LinkError::SessionIdUnavailable)?;
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(value)
}

fn expect(frame: &Frame, command: Command, stream_id: u32) -> Result<(), LinkError> {
    if frame.header.command == command && frame.header.stream_id == stream_id {
        Ok(())
    } else {
        Err(LinkError::UnexpectedFrame(
            "unexpected command or stream identifier",
        ))
    }
}

fn frame(
    command: Command,
    stream_id: u32,
    sequence: u32,
    payload: Vec<u8>,
) -> Result<Frame, WireError> {
    Frame::new(Header::new(command, stream_id, sequence), payload)
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, LinkError> {
    Ok(serde_json::to_vec(value)?)
}

fn decode<T: serde::de::DeserializeOwned>(
    payload: &[u8],
    label: &'static str,
) -> Result<T, LinkError> {
    serde_json::from_slice(payload).map_err(|source| LinkError::InvalidPayload { label, source })
}

fn write_usb_resynchronization_burst(
    writer: &mut impl Write,
    limits: DecodeLimits,
) -> Result<(), LinkError> {
    let mut remaining = usize::try_from(MAX_HOST_TO_DEVICE_PAYLOAD).map_err(|_| {
        LinkError::UnexpectedFrame("USB recovery payload limit does not fit the host")
    })?;
    let zeros = [0_u8; USB_TRANSFER_SIZE];
    while remaining > 0 {
        let length = remaining.min(zeros.len());
        writer.write_all(&zeros[..length])?;
        remaining -= length;
    }

    // If the zero fill ends inside a stale header, this complete marker gives
    // the device resynchronizer a boundary before the real HELLO. At a clean
    // boundary the device simply discards the non-HELLO frame.
    let marker = frame(Command::Ping, 0, 0, Vec::new())?.encode(limits)?;
    writer.write_all(&marker)?;
    writer.flush()?;
    Ok(())
}

trait UsbRecoveryRead: Read {
    fn set_recovery_read_timeout(&mut self, timeout: Duration);
}

impl UsbRecoveryRead for UsbReader {
    fn set_recovery_read_timeout(&mut self, timeout: Duration) {
        self.set_read_timeout(timeout);
    }
}

fn write_usb_recovery_exchange<R: UsbRecoveryRead + Send, W: Write>(
    reader: &mut R,
    writer: &mut W,
    limits: DecodeLimits,
) -> Result<(), LinkError> {
    // Poll IN while OUT is still advancing. Old pull traffic is credit-batched,
    // so a quiet interval does not prove that the abandoned response is fully
    // drained; the device can start another batch after consuming queued credit.
    reader.set_recovery_read_timeout(USB_RECOVERY_DRAIN_POLL);
    let outbound_done = AtomicBool::new(false);
    let inbound_bytes = AtomicU64::new(0);
    let inbound_timeouts = AtomicU64::new(0);
    std::thread::scope(|scope| {
        // A killed pull can leave the device blocked writing old DATA on IN.
        // Drain that direction while the recovery fill advances OUT; doing
        // either operation first would reproduce the cross-endpoint deadlock
        // that KBP credit batching is designed to prevent.
        let inbound = scope.spawn(|| {
            drain_usb_recovery_inbound(reader, &outbound_done, &inbound_bytes, &inbound_timeouts)
        });
        let mut counted = CountingWriter::new(writer);
        let outbound = write_usb_resynchronization_burst(&mut counted, limits);
        let outbound_bytes = counted.bytes_written();
        outbound_done.store(true, std::sync::atomic::Ordering::Release);
        let inbound = inbound
            .join()
            .map_err(|_| LinkError::UnexpectedFrame("USB recovery reader panicked"))?;
        if let Err(source) = outbound {
            return Err(LinkError::UsbRecoveryFailed {
                inbound_bytes: inbound_bytes.load(std::sync::atomic::Ordering::Relaxed),
                inbound_timeouts: inbound_timeouts.load(std::sync::atomic::Ordering::Relaxed),
                outbound_bytes,
                reason: source.to_string(),
            });
        }
        if let Err(source) = inbound {
            return Err(LinkError::UsbRecoveryFailed {
                inbound_bytes: inbound_bytes.load(std::sync::atomic::Ordering::Relaxed),
                inbound_timeouts: inbound_timeouts.load(std::sync::atomic::Ordering::Relaxed),
                outbound_bytes,
                reason: source.to_string(),
            });
        }
        Ok(())
    })
}

struct CountingWriter<'a, W> {
    inner: &'a mut W,
    bytes_written: u64,
}

impl<'a, W> CountingWriter<'a, W> {
    const fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }

    const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl<W: Write> Write for CountingWriter<'_, W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.bytes_written = self
            .bytes_written
            .saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn drain_usb_recovery_inbound(
    reader: &mut impl UsbRecoveryRead,
    outbound_done: &AtomicBool,
    inbound_bytes: &AtomicU64,
    inbound_timeouts: &AtomicU64,
) -> std::io::Result<()> {
    let mut buffer = [0_u8; USB_TRANSFER_SIZE];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(length) => {
                inbound_bytes.fetch_add(
                    u64::try_from(length).unwrap_or(u64::MAX),
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) =>
            {
                inbound_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if outbound_done.load(std::sync::atomic::Ordering::Acquire) {
                    return Ok(());
                }
            }
            Err(error) => return Err(error),
        }
    }
}

#[derive(Debug, Error)]
enum LinkError {
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(
        "USB recovery failed after draining {inbound_bytes} inbound bytes across {inbound_timeouts} idle timeouts and writing {outbound_bytes} outbound bytes: {reason}"
    )]
    UsbRecoveryFailed {
        inbound_bytes: u64,
        inbound_timeouts: u64,
        outbound_bytes: u64,
        reason: String,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Usb(#[from] UsbTransportError),
    #[error("invalid {label} payload: {source}")]
    InvalidPayload {
        label: &'static str,
        source: serde_json::Error,
    },
    #[error("device HELLO is incompatible")]
    InvalidHello,
    #[error("incompatible device protocol {device}; host requires {host}")]
    IncompatibleProtocol { device: u32, host: u32 },
    #[error("a USB session identifier could not be generated")]
    SessionIdUnavailable,
    #[error("{0}")]
    UnexpectedFrame(&'static str),
    #[error("sequence space exhausted")]
    SequenceExhausted,
    #[error("device call failed: {0:?}")]
    Remote(RpcError),
}

impl LinkError {
    fn allows_usb_handshake_recovery(&self) -> bool {
        matches!(
            self,
            Self::Transport(error)
                if matches!(
                    error.class(),
                    ErrorClass::CleanEof
                        | ErrorClass::Timeout
                        | ErrorClass::Truncated
                        | ErrorClass::Protocol
                        | ErrorClass::Io
                )
        ) || matches!(self, Self::UnexpectedFrame(_))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::{self, Cursor, Read, Write};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Condvar, Mutex as StdMutex};
    use std::thread;

    use kindlebridged::server::{ServerConfig, TcpServer};
    use kindlebridged::DeviceInfo;

    use super::*;

    const TEST_SESSION_ID: &str = "000102030405060708090a0b0c0d0e0f";
    const STALE_SESSION_ID: &str = "f0e0d0c0b0a090807060504030201000";

    #[cfg(unix)]
    fn make_test_tree_removable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        let metadata = fs::symlink_metadata(path).unwrap();
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return;
        }
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        for entry in fs::read_dir(path).unwrap() {
            make_test_tree_removable(&entry.unwrap().path());
        }
    }

    #[test]
    fn rejects_a_mismatched_device_protocol() {
        let hello = DeviceHello {
            protocol_version: PROTOCOL_VERSION - 1,
            session_id: TEST_SESSION_ID.to_owned(),
            serial: "KT6-PROTOCOL-TEST".to_owned(),
            model: "KT6".to_owned(),
            firmware: "5.17.1.0.4".to_owned(),
            target: "kindlehf".to_owned(),
            features: Vec::new(),
            initial_connection_window: DEFAULT_CONNECTION_WINDOW,
        };
        assert!(matches!(
            validate_device_hello(&hello, TEST_SESSION_ID),
            Err(LinkError::IncompatibleProtocol {
                device,
                host
            }) if device == PROTOCOL_VERSION - 1 && host == PROTOCOL_VERSION
        ));
    }

    #[test]
    fn usb_bulk_io_bounds_the_complete_initial_handshake() {
        let buffers = usb_buffer_config();
        assert_eq!(buffers.transfer_size, 16 * 1024);
        assert_eq!(buffers.read_queue_depth, 4);
        // Keep four order-2 requests in flight. A single 16 KiB request makes
        // every host-to-device write wait for one USB completion and leaves
        // most of the high-speed bus idle.
        assert_eq!(buffers.write_queue_depth, 4);
        assert_eq!(buffers.read_timeout, USB_HANDSHAKE_TIMEOUT);
        assert_eq!(buffers.write_timeout, USB_HANDSHAKE_TIMEOUT);
        assert!(USB_RECOVERY_TIMEOUT > USB_HANDSHAKE_TIMEOUT);
        assert_ne!(USB_RECOVERY_TIMEOUT, Duration::MAX);
    }

    #[derive(Clone, Debug, Default)]
    struct SharedWriter(Arc<StdMutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecoveryGate {
        state: StdMutex<RecoveryGateState>,
        drained: Condvar,
    }

    #[derive(Default)]
    struct RecoveryGateState {
        inbound_chunks: usize,
        outbound: Vec<u8>,
    }

    struct RecoveryGateReader {
        gate: Arc<RecoveryGate>,
        step: u8,
        timeouts: Vec<Duration>,
    }

    impl Read for RecoveryGateReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.step == 1 {
                self.step = 2;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "quiet interval between pull batches",
                ));
            }
            if self.step >= 3 {
                thread::yield_now();
                return Err(io::Error::new(io::ErrorKind::TimedOut, "drained"));
            }
            self.step += 1;
            let mut state = self.gate.state.lock().unwrap();
            state.inbound_chunks += 1;
            self.gate.drained.notify_all();
            buffer[0] = 0xa5;
            Ok(1)
        }
    }

    impl UsbRecoveryRead for RecoveryGateReader {
        fn set_recovery_read_timeout(&mut self, timeout: Duration) {
            self.timeouts.push(timeout);
        }
    }

    struct RecoveryGateWriter {
        gate: Arc<RecoveryGate>,
    }

    impl Write for RecoveryGateWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let state = self.gate.state.lock().unwrap();
            let required_chunks = usize::from(!state.outbound.is_empty()) + 1;
            let (mut state, timeout) = self
                .gate
                .drained
                .wait_timeout_while(state, Duration::from_millis(250), |state| {
                    state.inbound_chunks < required_chunks
                })
                .unwrap();
            if timeout.timed_out() && state.inbound_chunks < required_chunks {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "OUT remained blocked by stale IN",
                ));
            }
            state.outbound.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn usb_recovery_drains_stale_inbound_while_writing_the_resync_burst() {
        let gate = Arc::new(RecoveryGate::default());
        let mut reader = RecoveryGateReader {
            gate: Arc::clone(&gate),
            step: 0,
            timeouts: Vec::new(),
        };
        let mut writer = RecoveryGateWriter {
            gate: Arc::clone(&gate),
        };
        let (limits, _) = session_transport_config();

        write_usb_recovery_exchange(&mut reader, &mut writer, limits).unwrap();

        let state = gate.state.lock().unwrap();
        assert_eq!(state.inbound_chunks, 2);
        assert!(state.outbound.len() > usize::try_from(MAX_HOST_TO_DEVICE_PAYLOAD).unwrap());
        assert!(state.outbound.len() < usize::try_from(limits.max_payload).unwrap());
        assert_eq!(reader.timeouts, vec![USB_RECOVERY_DRAIN_POLL]);
    }

    struct FlushCountingActorSink {
        inner: TcpActorSink,
        flushes: Arc<AtomicUsize>,
    }

    impl ActorFrameSink for FlushCountingActorSink {
        fn write_frame(&mut self, frame: &Frame) -> Result<(), String> {
            self.inner.write_frame(frame)
        }

        fn flush(&mut self) -> Result<(), String> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            self.inner.flush()
        }
    }

    #[test]
    fn usb_recovery_burst_covers_one_maximum_host_frame_and_ends_on_a_boundary() {
        let (limits, config) = session_transport_config();
        let mut bytes = Vec::new();
        write_usb_resynchronization_burst(&mut bytes, limits).unwrap();
        let fill_length = usize::try_from(MAX_HOST_TO_DEVICE_PAYLOAD).unwrap();
        assert!(MAX_HOST_TO_DEVICE_PAYLOAD < limits.max_payload);
        assert!(bytes[..fill_length].iter().all(|byte| *byte == 0));

        let mut marker = kindlebridge_transport_tcp::FrameReader::new(
            Cursor::new(&bytes[fill_length..]),
            config,
        )
        .unwrap();
        let marker = marker.read_frame().unwrap();
        expect(&marker, Command::Ping, 0).unwrap();
    }

    #[test]
    fn recovered_usb_handshake_discards_stale_inbound_frames_before_hello() {
        let (limits, config) = session_transport_config();
        let mut hello = DeviceHello {
            protocol_version: PROTOCOL_VERSION,
            session_id: STALE_SESSION_ID.to_owned(),
            serial: "KT6-USB-RECOVERED".to_owned(),
            model: "KT6".to_owned(),
            firmware: "5.17.1.0.4".to_owned(),
            target: "kindlehf".to_owned(),
            features: vec![SYNC_FEATURE.to_owned()],
            initial_connection_window: DEFAULT_CONNECTION_WINDOW,
        };
        let mut incoming = frame(Command::Ping, 0, 9, Vec::new())
            .unwrap()
            .encode(limits)
            .unwrap();
        incoming.extend_from_slice(
            &frame(Command::Hello, 0, 0, encode(&hello).unwrap())
                .unwrap()
                .encode(limits)
                .unwrap(),
        );
        hello.session_id = TEST_SESSION_ID.to_owned();
        incoming.extend_from_slice(
            &frame(Command::Hello, 0, 0, encode(&hello).unwrap())
                .unwrap()
                .encode(limits)
                .unwrap(),
        );
        let output = SharedWriter::default();
        let mut stream = SplitFrameStream::new(Cursor::new(incoming), output, config).unwrap();

        let (_, received) = negotiate(&mut stream, limits, true, TEST_SESSION_ID).unwrap();
        assert_eq!(received.serial, "KT6-USB-RECOVERED");
        assert_eq!(received.session_id, TEST_SESSION_ID);
    }

    #[test]
    fn persistent_provider_lists_features_and_executes_on_real_device_server() {
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server = TcpServer::bind(
            SocketAddr::new(loopback, 0),
            ServerConfig::new(DeviceInfo::kt6("KT6-LINK")).allow_peer(loopback),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || server.serve_once());

        let provider = ConnectedDeviceProvider::connect(&[address]).unwrap();
        let devices = provider.list().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].serial, "KT6-LINK");
        assert_eq!(devices[0].transport, "tcp");
        assert_eq!(
            provider.features("KT6-LINK").unwrap().unwrap().features,
            vec![
                kindlebridge_schema::device_protocol::APP_INSTALL_FEATURE,
                kindlebridge_schema::device_protocol::APP_LIST_FEATURE,
                kindlebridge_schema::device_protocol::APP_LOG_FEATURE,
                kindlebridge_schema::device_protocol::APP_RESTART_FEATURE,
                kindlebridge_schema::device_protocol::APP_ROLLBACK_FEATURE,
                kindlebridge_schema::device_protocol::APP_START_FEATURE,
                kindlebridge_schema::device_protocol::APP_STOP_FEATURE,
                kindlebridge_schema::device_protocol::APP_UNINSTALL_FEATURE,
                kindlebridge_schema::device_protocol::EXEC_FEATURE,
                kindlebridge_schema::device_protocol::LOG_TAIL_FEATURE,
                kindlebridge_schema::device_protocol::PROCESS_LIST_FEATURE,
                kindlebridge_schema::device_protocol::PROCESS_SIGNAL_FEATURE,
                kindlebridge_schema::device_protocol::SHELL_V2_FEATURE,
                kindlebridge_schema::device_protocol::SYNC_TREE_FEATURE,
                kindlebridge_schema::device_protocol::SYNC_FEATURE,
            ]
        );

        assert!(provider
            .app_list(&SerialParams {
                serial: "KT6-LINK".to_owned(),
            })
            .unwrap()
            .apps
            .is_empty());
        let _ = provider
            .process_list(&SerialParams {
                serial: "KT6-LINK".to_owned(),
            })
            .unwrap();
        assert!(provider
            .log_tail(&LogTailParams {
                serial: "KT6-LINK".to_owned(),
                cursor: None,
                limit: Some(10),
            })
            .unwrap()
            .entries
            .is_empty());
        let missing = provider
            .app_start(&AppTargetParams {
                serial: "KT6-LINK".to_owned(),
                app_id: "org.example.reader".to_owned(),
            })
            .unwrap_err();
        assert_eq!(missing.code, error_codes::APP_NOT_FOUND);

        let executable = std::env::current_exe().unwrap();
        let successful_params = ExecParams {
            serial: "KT6-LINK".to_owned(),
            argv: vec![
                executable.to_string_lossy().into_owned(),
                "--list".to_owned(),
            ],
            cwd: None,
            environment: BTreeMap::new(),
            timeout_ms: 10_000,
        };
        let result = provider.exec(&successful_params).unwrap().unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result
            .stdout
            .contains("persistent_provider_lists_features_and_executes_on_real_device_server"));

        // A second stream verifies that both endpoints restored connection
        // credit after the first call.
        assert_eq!(
            provider
                .exec(&successful_params)
                .unwrap()
                .unwrap()
                .exit_code,
            0
        );

        let timeout = provider
            .exec(&ExecParams {
                serial: "KT6-LINK".to_owned(),
                argv: vec![
                    executable.to_string_lossy().into_owned(),
                    "--exact".to_owned(),
                    "device_session::tests::child_sleep_helper".to_owned(),
                ],
                cwd: None,
                environment: BTreeMap::from([("KBP_CHILD_SLEEP".to_owned(), "1".to_owned())]),
                timeout_ms: 10,
            })
            .unwrap_err();
        assert_eq!(timeout.code, error_codes::EXEC_TIMEOUT);

        // A remote method failure closes only that stream, not the session.
        assert_eq!(
            provider
                .exec(&successful_params)
                .unwrap()
                .unwrap()
                .exit_code,
            0
        );

        drop(provider);
        worker.join().unwrap().unwrap();
    }

    #[test]
    fn shell_v2_raw_stream_preserves_binary_channels_and_exit_status() {
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server = TcpServer::bind(
            SocketAddr::new(loopback, 0),
            ServerConfig::new(DeviceInfo::kt6("KT6-SHELL")).allow_peer(loopback),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || server.serve_once());
        let provider = ConnectedDeviceProvider::connect(&[address]).unwrap();
        let executable = std::env::current_exe().unwrap();
        let shell = provider
            .open_shell(
                "KT6-SHELL",
                ShellOpen {
                    mode: kindlebridge_schema::device_protocol::ShellMode::Raw,
                    argv: vec![
                        executable.to_string_lossy().into_owned(),
                        "--exact".to_owned(),
                        "device_session::tests::shell_raw_child_helper".to_owned(),
                        "--ignored".to_owned(),
                        "--nocapture".to_owned(),
                    ],
                    terminal_size: None,
                    cwd: std::env::temp_dir().to_string_lossy().into_owned(),
                    term: "linux".to_owned(),
                },
            )
            .unwrap();
        shell.send(ShellPacket::Stdin(vec![0, 1, 2, 255])).unwrap();
        shell.send(ShellPacket::CloseStdin).unwrap();

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = loop {
            match shell.recv().unwrap() {
                DeviceShellEvent::Packet(ShellPacket::Stdout(bytes)) => stdout.extend(bytes),
                DeviceShellEvent::Packet(ShellPacket::Stderr(bytes)) => stderr.extend(bytes),
                DeviceShellEvent::Packet(ShellPacket::Exit(status)) => break status,
                DeviceShellEvent::Packet(packet) => panic!("unexpected packet {packet:?}"),
                DeviceShellEvent::Closed => panic!("shell closed before exit"),
            }
        };
        assert!(stdout.windows(4).any(|window| window == [0, 1, 2, 255]));
        assert!(stderr.windows(4).any(|window| window == b"ERR\0"));
        assert_eq!(exit.exit_code, 37);
        assert_eq!(exit.signal, 0);
        assert_eq!(shell.recv().unwrap(), DeviceShellEvent::Closed);

        drop(shell);
        drop(provider);
        worker.join().unwrap().unwrap();
    }

    #[test]
    fn shell_quota_reopens_only_after_a_shell_is_cleaned_up() {
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server = TcpServer::bind(
            SocketAddr::new(loopback, 0),
            ServerConfig::new(DeviceInfo::kt6("KT6-SHELL-QUOTA")).allow_peer(loopback),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let server_worker = thread::spawn(move || server.serve_once());
        let provider = ConnectedDeviceProvider::connect(&[address]).unwrap();

        let mut shells = (0..4)
            .map(|_| open_echo_shell(&provider, "KT6-SHELL-QUOTA"))
            .collect::<Vec<_>>();
        assert!(provider
            .open_shell("KT6-SHELL-QUOTA", echo_shell_open())
            .is_err());

        shells.pop().unwrap().close().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let replacement = loop {
            match provider.open_shell("KT6-SHELL-QUOTA", echo_shell_open()) {
                Ok(shell) => break shell,
                Err(_) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("shell quota was not released after cleanup: {error}"),
            }
        };
        replacement
            .send(ShellPacket::Stdin(b"replacement\n".to_vec()))
            .unwrap();
        loop {
            match replacement.recv().unwrap() {
                DeviceShellEvent::Packet(ShellPacket::Stdout(bytes))
                    if bytes
                        .windows(b"replacement\n".len())
                        .any(|window| window == b"replacement\n") =>
                {
                    break;
                }
                DeviceShellEvent::Packet(_) => {}
                DeviceShellEvent::Closed => panic!("replacement shell closed before echo"),
            }
        }

        replacement.close().unwrap();
        drop(replacement);
        for shell in shells {
            shell.close().unwrap();
        }
        drop(provider);
        server_worker.join().unwrap().unwrap();
    }

    #[test]
    fn malformed_shell_packet_resets_only_its_stream() {
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server = TcpServer::bind(
            SocketAddr::new(loopback, 0),
            ServerConfig::new(DeviceInfo::kt6("KT6-SHELL-RESET")).allow_peer(loopback),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || server.serve_once());
        let provider = ConnectedDeviceProvider::connect(&[address]).unwrap();
        let executable = std::env::current_exe().unwrap();
        let stream = provider.devices[0]
            .session
            .connection
            .open(
                SHELL_V2_SERVICE,
                SHELL_STREAM_WINDOW,
                TrafficClass::Interactive,
            )
            .unwrap();
        stream
            .send_data(
                encode(&ShellOpen {
                    mode: kindlebridge_schema::device_protocol::ShellMode::Raw,
                    argv: vec![
                        executable.to_string_lossy().into_owned(),
                        "--exact".to_owned(),
                        "device_session::tests::shell_raw_child_helper".to_owned(),
                        "--ignored".to_owned(),
                        "--nocapture".to_owned(),
                    ],
                    terminal_size: None,
                    cwd: std::env::temp_dir().to_string_lossy().into_owned(),
                    term: "linux".to_owned(),
                })
                .unwrap(),
                false,
            )
            .unwrap();
        // Unknown shell packet kind. The device must RESET this stream and
        // keep the shared KBP connection usable by unrelated RPCs.
        stream.send_data(vec![0xff, 0, 0, 0, 0], false).unwrap();
        loop {
            let frame = stream.recv().unwrap();
            if frame.header.command == Command::Reset {
                break;
            }
            assert_eq!(frame.header.command, Command::Data);
        }

        let result = provider
            .exec(&ExecParams {
                serial: "KT6-SHELL-RESET".to_owned(),
                argv: vec![
                    executable.to_string_lossy().into_owned(),
                    "--exact".to_owned(),
                    "device_session::tests::child_sleep_helper".to_owned(),
                ],
                cwd: None,
                environment: BTreeMap::new(),
                timeout_ms: 10_000,
            })
            .unwrap()
            .unwrap();
        assert_eq!(result.exit_code, 0);

        drop(stream);
        drop(provider);
        worker.join().unwrap().unwrap();
    }

    #[test]
    fn two_shells_stay_responsive_during_sync_and_log_traffic() {
        const SYNC_BYTES: usize = 16 * 1024 * 1024;
        const ECHO_ROUNDS: usize = 40;

        let unique = format!("{}-shell-fairness", std::process::id());
        let root = std::env::temp_dir().join(format!("kindlebridge-device-{unique}"));
        let source = std::env::temp_dir().join(format!("kindlebridge-source-{unique}.bin"));
        let log = std::env::temp_dir().join(format!("kindlebridge-log-{unique}.txt"));
        let mut source_file = File::create(&source).unwrap();
        let block = vec![0x5a; 1024 * 1024];
        for _ in 0..(SYNC_BYTES / block.len()) {
            source_file.write_all(&block).unwrap();
        }
        source_file.sync_all().unwrap();
        fs::write(&log, b"kindlebridge test log\n").unwrap();

        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server = TcpServer::bind(
            SocketAddr::new(loopback, 0),
            ServerConfig::new(DeviceInfo::kt6("KT6-FAIR"))
                .allow_peer(loopback)
                .sync_root(&root)
                .log_path(&log),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || server.serve_once());
        let provider = Arc::new(ConnectedDeviceProvider::connect(&[address]).unwrap());
        let executable = std::env::current_exe().unwrap();
        let shell_open = || {
            provider
                .open_shell(
                    "KT6-FAIR",
                    ShellOpen {
                        mode: kindlebridge_schema::device_protocol::ShellMode::Raw,
                        argv: vec![
                            executable.to_string_lossy().into_owned(),
                            "--exact".to_owned(),
                            "device_session::tests::shell_echo_child_helper".to_owned(),
                            "--ignored".to_owned(),
                            "--nocapture".to_owned(),
                        ],
                        terminal_size: None,
                        cwd: std::env::temp_dir().to_string_lossy().into_owned(),
                        term: "linux".to_owned(),
                    },
                )
                .unwrap()
        };
        let shell_a = shell_open();
        let shell_b = shell_open();
        let start = Arc::new(Barrier::new(4));

        let echo_a_start = Arc::clone(&start);
        let echo_a =
            thread::spawn(move || shell_echo_latencies(shell_a, "a", ECHO_ROUNDS, echo_a_start));
        let echo_b_start = Arc::clone(&start);
        let echo_b =
            thread::spawn(move || shell_echo_latencies(shell_b, "b", ECHO_ROUNDS, echo_b_start));

        let sync_provider = Arc::clone(&provider);
        let sync_start = Arc::clone(&start);
        let sync_source = source.to_string_lossy().into_owned();
        let sync_worker = thread::spawn(move || {
            sync_start.wait();
            sync_provider
                .sync_push(SyncPushParams {
                    serial: "KT6-FAIR".to_owned(),
                    local_path: sync_source,
                    remote_path: "fairness/payload.bin".to_owned(),
                    transfer_id: None,
                    block_size: kindlebridge_schema::DEFAULT_SYNC_BLOCK_SIZE,
                })
                .unwrap()
        });

        let log_provider = Arc::clone(&provider);
        let log_start = Arc::clone(&start);
        let log_worker = thread::spawn(move || {
            log_start.wait();
            for _ in 0..64 {
                log_provider
                    .log_tail(&LogTailParams {
                        serial: "KT6-FAIR".to_owned(),
                        cursor: Some(0),
                        limit: Some(16),
                    })
                    .unwrap();
            }
        });

        let mut latencies = echo_a.join().unwrap();
        latencies.extend(echo_b.join().unwrap());
        sync_worker.join().unwrap();
        log_worker.join().unwrap();
        latencies.sort_unstable();
        let p95 = latencies[(latencies.len() * 95).div_ceil(100) - 1];
        assert!(
            p95 <= Duration::from_millis(50),
            "shell echo P95 was {p95:?}"
        );
        assert_eq!(
            fs::metadata(root.join("fairness/payload.bin"))
                .unwrap()
                .len(),
            SYNC_BYTES as u64
        );

        drop(provider);
        worker.join().unwrap().unwrap();
        fs::remove_file(source).unwrap();
        fs::remove_file(log).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[ignore = "runs only as a child process for shell_v2_raw_stream"]
    fn shell_raw_child_helper() {
        let mut input = Vec::new();
        std::io::stdin().read_to_end(&mut input).unwrap();
        std::io::stdout().write_all(&input).unwrap();
        std::io::stdout().flush().unwrap();
        std::io::stderr().write_all(b"ERR\0").unwrap();
        std::io::stderr().flush().unwrap();
        std::process::exit(37);
    }

    #[test]
    #[ignore = "runs only as a child process for the shell fairness test"]
    fn shell_echo_child_helper() {
        let mut input = std::io::stdin();
        let mut output = std::io::stdout();
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            let count = input.read(&mut buffer).unwrap();
            if count == 0 {
                break;
            }
            output.write_all(&buffer[..count]).unwrap();
            output.flush().unwrap();
        }
    }

    fn echo_shell_open() -> ShellOpen {
        let executable = std::env::current_exe().unwrap();
        ShellOpen {
            mode: kindlebridge_schema::device_protocol::ShellMode::Raw,
            argv: vec![
                executable.to_string_lossy().into_owned(),
                "--exact".to_owned(),
                "device_session::tests::shell_echo_child_helper".to_owned(),
                "--ignored".to_owned(),
                "--nocapture".to_owned(),
            ],
            terminal_size: None,
            cwd: std::env::temp_dir().to_string_lossy().into_owned(),
            term: "linux".to_owned(),
        }
    }

    fn open_echo_shell(provider: &ConnectedDeviceProvider, serial: &str) -> DeviceShell {
        provider.open_shell(serial, echo_shell_open()).unwrap()
    }

    fn shell_echo_latencies(
        shell: DeviceShell,
        label: &str,
        rounds: usize,
        start: Arc<Barrier>,
    ) -> Vec<Duration> {
        start.wait();
        let mut latencies = Vec::with_capacity(rounds);
        for round in 0..rounds {
            let token = format!("kb-{label}-{round:04}\n");
            let started = Instant::now();
            shell
                .send(ShellPacket::Stdin(token.as_bytes().to_vec()))
                .unwrap();
            let mut received = Vec::new();
            loop {
                match shell.recv().unwrap() {
                    DeviceShellEvent::Packet(ShellPacket::Stdout(bytes)) => {
                        received.extend(bytes);
                        if received
                            .windows(token.len())
                            .any(|window| window == token.as_bytes())
                        {
                            break;
                        }
                    }
                    DeviceShellEvent::Packet(ShellPacket::Stderr(_)) => {}
                    event => panic!("shell exited during echo test: {event:?}"),
                }
            }
            latencies.push(started.elapsed());
        }
        shell.send(ShellPacket::CloseStdin).unwrap();
        loop {
            match shell.recv().unwrap() {
                DeviceShellEvent::Packet(ShellPacket::Exit(status)) => {
                    assert_eq!(status.exit_code, 0);
                }
                DeviceShellEvent::Closed => break,
                _ => {}
            }
        }
        latencies
    }

    #[test]
    fn child_sleep_helper() {
        if std::env::var_os("KBP_CHILD_SLEEP").is_some() {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    #[test]
    fn app_install_uploads_and_commits_a_real_kbb_over_one_device_session() {
        let unique = format!("{}-app-install-link", std::process::id());
        let root = std::env::temp_dir().join(format!("kindlebridge-device-{unique}"));
        let activation_root = root.join("activations");
        let bundle_path = std::env::temp_dir().join(format!("kindlebridge-{unique}.kbb"));
        let mut bundle_config = kindlebridge_bundle::BuildConfig::new(
            kindlebridge_bundle::BundleKind::Application,
            "org.example.connected",
            "1.2.3",
            4,
            "kindlehf",
        );
        bundle_config.firmware_min = Some(vec![5, 17]);
        bundle_config.required_features = vec![SYNC_FEATURE.to_owned()];
        bundle_config.entrypoints =
            BTreeMap::from([("main".to_owned(), "bin/connected".to_owned())]);
        let mut builder = kindlebridge_bundle::BundleBuilder::new(bundle_config);
        builder
            .add_file("bin/connected", b"#!/bin/sh\nexit 0\n".to_vec(), true)
            .unwrap();
        fs::write(
            &bundle_path,
            builder
                .build(&ed25519_dalek::SigningKey::from_bytes(&[0x21; 32]))
                .unwrap(),
        )
        .unwrap();

        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server = TcpServer::bind(
            SocketAddr::new(loopback, 0),
            ServerConfig::new(DeviceInfo::kt6("KT6-APP"))
                .allow_peer(loopback)
                .sync_root(&root)
                .activation_root(&activation_root),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || server.serve_once());
        let provider = ConnectedDeviceProvider::connect(&[address]).unwrap();

        let installed = provider
            .app_install(AppInstallParams {
                serial: "KT6-APP".to_owned(),
                bundle_path: bundle_path.to_string_lossy().into_owned(),
            })
            .unwrap();
        assert_eq!(installed.app_id, "org.example.connected");
        assert_eq!(installed.version, "1.2.3");
        assert_eq!(installed.state, kindlebridge_schema::AppState::Stopped);
        let listed = provider
            .app_list(&SerialParams {
                serial: "KT6-APP".to_owned(),
            })
            .unwrap();
        assert_eq!(listed.apps, vec![installed]);
        assert!(root.join("packages/kbb").read_dir().unwrap().count() >= 1);
        assert!(activation_root.join("active-generation").is_file());

        drop(provider);
        worker.join().unwrap().unwrap();
        fs::remove_file(bundle_path).unwrap();
        #[cfg(unix)]
        make_test_tree_removable(&root);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn raw_sync_stream_round_trips_beyond_the_flow_control_window() {
        let unique = format!("{}-sync-link", std::process::id());
        let root = std::env::temp_dir().join(format!("kindlebridge-device-{unique}"));
        let source = std::env::temp_dir().join(format!("kindlebridge-source-{unique}.bin"));
        let destination =
            std::env::temp_dir().join(format!("kindlebridge-destination-{unique}.bin"));
        let payload: Vec<u8> = (0..(9 * 1024 * 1024 + 123))
            .map(|index| ((index as u64).wrapping_mul(17) & 0xff) as u8)
            .collect();
        fs::write(&source, &payload).unwrap();

        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server = TcpServer::bind(
            SocketAddr::new(loopback, 0),
            ServerConfig::new(DeviceInfo::kt6("KT6-SYNC"))
                .allow_peer(loopback)
                .sync_root(&root),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || server.serve_once());
        let provider = ConnectedDeviceProvider::connect(&[address]).unwrap();

        let push_result = provider.sync_push(SyncPushParams {
            serial: "KT6-SYNC".to_owned(),
            local_path: source.to_string_lossy().into_owned(),
            remote_path: "apps/demo/payload.bin".to_owned(),
            transfer_id: None,
            block_size: kindlebridge_schema::DEFAULT_SYNC_BLOCK_SIZE,
        });
        let pushed = match push_result {
            Ok(value) => value,
            Err(error) => {
                drop(provider);
                panic!(
                    "push failed: {error:?}; server: {:?}",
                    worker.join().unwrap()
                );
            }
        };
        assert_eq!(pushed.accepted_offset, payload.len() as u64);
        assert_eq!(
            fs::read(root.join("apps/demo/payload.bin")).unwrap(),
            payload
        );
        assert_eq!(
            provider
                .sync_status(&SyncStatusParams {
                    serial: "KT6-SYNC".to_owned(),
                    transfer_id: pushed.transfer_id.clone(),
                })
                .unwrap()
                .state,
            TransferState::Complete
        );

        // If the original COMPLETE reply was lost, the host retries with the
        // same transfer ID. A completed push must acknowledge that retry
        // without reopening a staging file or duplicating file data.
        let replayed = provider
            .sync_push(SyncPushParams {
                serial: "KT6-SYNC".to_owned(),
                local_path: source.to_string_lossy().into_owned(),
                remote_path: "apps/demo/payload.bin".to_owned(),
                transfer_id: Some(pushed.transfer_id.clone()),
                block_size: kindlebridge_schema::DEFAULT_SYNC_BLOCK_SIZE,
            })
            .unwrap();
        assert_eq!(replayed.transfer_id, pushed.transfer_id);
        assert_eq!(replayed.accepted_offset, payload.len() as u64);
        assert_eq!(
            fs::read(root.join("apps/demo/payload.bin")).unwrap(),
            payload
        );

        let pulled = provider
            .sync_pull(SyncPullParams {
                serial: "KT6-SYNC".to_owned(),
                remote_path: "apps/demo/payload.bin".to_owned(),
                local_path: destination.to_string_lossy().into_owned(),
                transfer_id: None,
                block_size: kindlebridge_schema::DEFAULT_SYNC_BLOCK_SIZE,
            })
            .unwrap();
        assert_eq!(pulled.received_size, payload.len() as u64);
        assert_eq!(fs::read(&destination).unwrap(), payload);

        drop(provider);
        worker.join().unwrap().unwrap();
        fs::remove_file(source).unwrap();
        fs::remove_file(destination).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sync_push_flushes_each_data_frame_and_drains_batch_credits() {
        let unique = format!("{}-flush-batching", std::process::id());
        let root = std::env::temp_dir().join(format!("kindlebridge-device-{unique}"));
        let source = std::env::temp_dir().join(format!("kindlebridge-source-{unique}.bin"));
        let payload = vec![0x7b; 2 * SYNC_CREDIT_BATCH_SIZE as usize];
        fs::write(&source, &payload).unwrap();

        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server = TcpServer::bind(
            SocketAddr::new(loopback, 0),
            ServerConfig::new(DeviceInfo::kt6("KT6-FLUSH"))
                .allow_peer(loopback)
                .sync_root(&root),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || server.serve_once());
        let (limits, transport) = session_transport_config();
        let mut sink = TcpFrameStream::connect(address, transport).unwrap();
        let actor_source = sink.try_clone().unwrap();
        let session_id = new_session_id().unwrap();
        let (state, _) = negotiate(&mut sink, limits, false, &session_id).unwrap();
        let flushes = Arc::new(AtomicUsize::new(0));
        let counted = FlushCountingActorSink {
            inner: TcpActorSink(sink),
            flushes: Arc::clone(&flushes),
        };
        let (connection, _incoming) =
            Connection::start(state, TcpActorSource(actor_source), counted);
        let session = ActorDeviceSession { connection };
        let before = flushes.load(Ordering::Relaxed);
        let mut file = File::open(&source).unwrap();
        session
            .sync_client()
            .push_open_file(
                &SyncPushParams {
                    serial: "KT6-FLUSH".to_owned(),
                    local_path: source.to_string_lossy().into_owned(),
                    remote_path: "flush/payload.bin".to_owned(),
                    transfer_id: None,
                    block_size: 64 * 1024,
                },
                &mut file,
                payload.len() as u64,
                blake3::hash(&payload).to_hex().as_ref(),
                &SyncObserver::default(),
            )
            .unwrap();
        let transfer_flushes = flushes.load(Ordering::Relaxed) - before;
        let data_frames = payload.len() / (64 * 1024);
        assert!(
            transfer_flushes >= data_frames,
            "expected at least one flush per DATA frame, observed {transfer_flushes} for {data_frames} frames"
        );

        drop(session);
        worker.join().unwrap().unwrap();
        fs::remove_file(source).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interrupted_push_resumes_after_a_new_device_session() {
        let unique = format!("{}-resume-link", std::process::id());
        let root = std::env::temp_dir().join(format!("kindlebridge-device-{unique}"));
        let source = std::env::temp_dir().join(format!("kindlebridge-source-{unique}.bin"));
        let payload: Vec<u8> = (0..2_500_321_u64)
            .map(|index| (index.wrapping_mul(29) & 0xff) as u8)
            .collect();
        fs::write(&source, &payload).unwrap();
        let file_hash = blake3::hash(&payload).to_hex().to_string();

        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let server = TcpServer::bind(
            SocketAddr::new(loopback, 0),
            ServerConfig::new(DeviceInfo::kt6("KT6-RESUME"))
                .allow_peer(loopback)
                .sync_root(&root),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let worker = thread::spawn(move || {
            server.serve_once()?;
            server.serve_once()
        });

        let (interrupted, _) = ActorDeviceSession::connect(address).unwrap();
        let mut stream = interrupted
            .connection
            .open(SYNC_SERVICE, DEFAULT_STREAM_WINDOW, TrafficClass::Bulk)
            .unwrap();
        let request = SyncRequest::Push {
            transfer_id: None,
            remote_path: "resume/payload.bin".to_owned(),
            total_size: payload.len() as u64,
            file_hash: file_hash.clone(),
            block_size: 256 * 1024,
        };
        stream.send_data(encode(&request).unwrap(), false).unwrap();
        let ready: SyncReply =
            decode(&actor_data(&mut stream).unwrap().payload, "sync READY").unwrap();
        let transfer_id = match ready {
            SyncReply::Ready {
                transfer_id,
                offset: 0,
                ..
            } => transfer_id,
            other => panic!("unexpected resume READY: {other:?}"),
        };
        let split = 1024 * 1024;
        for chunk in payload[..split].chunks(256 * 1024) {
            stream.send_data(chunk.to_vec(), false).unwrap();
        }
        drop(stream);
        drop(interrupted);

        let provider = ConnectedDeviceProvider::connect(&[address]).unwrap();
        let resumed = provider
            .sync_push(SyncPushParams {
                serial: "KT6-RESUME".to_owned(),
                local_path: source.to_string_lossy().into_owned(),
                remote_path: "resume/payload.bin".to_owned(),
                transfer_id: Some(transfer_id.clone()),
                block_size: 256 * 1024,
            })
            .unwrap();
        assert_eq!(resumed.transfer_id, transfer_id);
        assert_eq!(resumed.accepted_offset, payload.len() as u64);
        assert_eq!(fs::read(root.join("resume/payload.bin")).unwrap(), payload);

        drop(provider);
        worker.join().unwrap().unwrap();
        fs::remove_file(source).unwrap();
        fs::remove_dir_all(root).unwrap();
    }
}
