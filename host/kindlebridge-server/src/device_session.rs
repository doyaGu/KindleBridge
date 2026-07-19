//! Host-side provider backed by persistent KBP/TCP device sessions.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use kindlebridge_schema::device_protocol::{
    is_valid_session_id, DeviceCall, DeviceHello, DeviceReply, HostHello, ServiceAccept,
    ServiceOpen, SyncReply, SyncRequest, APP_INSTALL_FEATURE, APP_LIST_FEATURE,
    APP_RESTART_FEATURE, APP_ROLLBACK_FEATURE, APP_START_FEATURE, APP_STOP_FEATURE,
    APP_UNINSTALL_FEATURE, DEFAULT_CONNECTION_WINDOW, DEFAULT_STREAM_WINDOW, LOG_TAIL_FEATURE,
    MAX_HOST_TO_DEVICE_PAYLOAD, PROCESS_LIST_FEATURE, PROCESS_SIGNAL_FEATURE, PROTOCOL_VERSION,
    SHELL_SERVICE, SYNC_CREDIT_BATCH_SIZE, SYNC_FEATURE, SYNC_SERVICE,
};
use kindlebridge_schema::{
    error_codes, AppInstallParams, AppList, AppSummary, AppTargetParams, DeviceFeatures,
    DeviceState, DeviceSummary, ExecParams, ExecResult, LogSnapshot, LogTailParams, ProcessList,
    ProcessSignalParams, ProcessSummary, RpcError, SerialParams, SyncPullParams, SyncPullResult,
    SyncPushParams, SyncPushResult, SyncStatus, SyncStatusParams, TransferState,
    MAX_SYNC_BLOCK_SIZE,
};
use kindlebridge_transport_tcp::{
    ErrorClass, FrameIo, SplitFrameStream, TcpFrameStream, TransportConfig, TransportError,
};
use kindlebridge_transport_usb::{
    BufferConfig, TransportError as UsbTransportError, UsbMatch, UsbReader,
};
use kindlebridge_wire::{
    Command, DecodeLimits, EndpointRole, Frame, FrameContext, Header, ProtocolError, SessionConfig,
    SessionState, WireError, FLAG_END_STREAM,
};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use crate::{DeviceProvider, ProviderError};

const SESSION_IO_TIMEOUT: Duration = Duration::from_secs(10 * 60 + 30);
// Match the device's order-2 FunctionFS request size. Larger host transfers
// force the KT6 4.9 gadget stack into fragile high-order page allocations.
const USB_TRANSFER_SIZE: usize = 16 * 1024;
const USB_READ_QUEUE_DEPTH: usize = 4;
// Four order-2 requests keep 64 KiB in flight without asking the KT6 gadget
// stack for larger, fragile high-order allocations.
const USB_WRITE_QUEUE_DEPTH: usize = 4;
const USB_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
const USB_RECOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const USB_RECOVERY_DRAIN_POLL: Duration = Duration::from_millis(250);

struct ConnectedDevice {
    summary: DeviceSummary,
    features: DeviceFeatures,
    session: Mutex<DeviceSession>,
}

pub struct ConnectedDeviceProvider {
    devices: Vec<ConnectedDevice>,
}

impl ConnectedDeviceProvider {
    pub fn connect(addresses: &[SocketAddr]) -> Result<Self, ProviderError> {
        let mut devices = Vec::with_capacity(addresses.len());
        for address in addresses {
            let (session, hello) = DeviceSession::connect(*address)
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
                session: Mutex::new(session),
            });
        }
        devices.sort_by(|left, right| left.summary.serial.cmp(&right.summary.serial));
        Ok(Self { devices })
    }

    /// Open the exact KindleBridge WinUSB interface and perform the normal KBP
    /// device handshake. MTP remains owned by its separate composite interface.
    pub fn connect_usb(criteria: &UsbMatch) -> Result<Self, ProviderError> {
        let (session, hello) = match DeviceSession::connect_usb(criteria) {
            Ok(link) => link,
            Err(LinkError::Usb(UsbTransportError::DeviceNotFound)) => {
                return Ok(Self {
                    devices: Vec::new(),
                });
            }
            Err(error) => return Err(ProviderError::new(error.to_string())),
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
                session: Mutex::new(session),
            }],
        })
    }

    fn find(&self, serial: &str) -> Option<&ConnectedDevice> {
        self.devices
            .iter()
            .find(|device| device.summary.serial == serial)
    }
}

impl DeviceProvider for ConnectedDeviceProvider {
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

    fn exec(&self, params: &ExecParams) -> Result<Option<ExecResult>, RpcError> {
        let Some(device) = self.find(&params.serial) else {
            return Ok(None);
        };
        let value = device
            .session
            .lock()
            .map_err(|_| RpcError::internal_error())?
            .call(kindlebridge_schema::methods::EXEC_RUN, params)
            .map_err(link_rpc_error)?;
        serde_json::from_value(value)
            .map(Some)
            .map_err(|_| RpcError::internal_error())
    }

    fn sync_push(&self, params: SyncPushParams) -> Result<SyncPushResult, RpcError> {
        let device = self.require_feature(&params.serial, SYNC_FEATURE)?;
        validate_host_path(&params.local_path)?;
        validate_block_size(params.block_size)?;
        let mut file = File::open(&params.local_path)
            .map_err(|error| host_file_error("read", &params.local_path, &error))?;
        if !file
            .metadata()
            .map_err(|error| host_file_error("stat", &params.local_path, &error))?
            .is_file()
        {
            return Err(RpcError::invalid_params("local_path must name a file"));
        }
        let total_size = file
            .metadata()
            .map_err(|error| host_file_error("stat", &params.local_path, &error))?
            .len();
        let file_hash = hash_file(&mut file, total_size)?;
        device
            .session
            .lock()
            .map_err(|_| RpcError::internal_error())?
            .sync_push(&params, &mut file, total_size, &file_hash)
            .map_err(link_rpc_error)
    }

    fn sync_pull(&self, params: SyncPullParams) -> Result<SyncPullResult, RpcError> {
        let device = self.require_feature(&params.serial, SYNC_FEATURE)?;
        validate_host_path(&params.local_path)?;
        validate_block_size(params.block_size)?;
        device
            .session
            .lock()
            .map_err(|_| RpcError::internal_error())?
            .sync_pull(&params)
            .map_err(link_rpc_error)
    }

    fn sync_status(&self, params: &SyncStatusParams) -> Result<SyncStatus, RpcError> {
        let device = self.require_feature(&params.serial, SYNC_FEATURE)?;
        let value = device
            .session
            .lock()
            .map_err(|_| RpcError::internal_error())?
            .call(kindlebridge_schema::methods::SYNC_STATUS, params)
            .map_err(link_rpc_error)?;
        serde_json::from_value(value).map_err(|_| RpcError::internal_error())
    }

    fn app_install(&self, params: AppInstallParams) -> Result<AppSummary, RpcError> {
        self.remote_call(
            &params.serial,
            APP_INSTALL_FEATURE,
            kindlebridge_schema::methods::APP_INSTALL,
            &params,
        )
    }

    fn app_start(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call(
            &params.serial,
            APP_START_FEATURE,
            kindlebridge_schema::methods::APP_START,
            params,
        )
    }

    fn app_stop(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call(
            &params.serial,
            APP_STOP_FEATURE,
            kindlebridge_schema::methods::APP_STOP,
            params,
        )
    }

    fn app_restart(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call(
            &params.serial,
            APP_RESTART_FEATURE,
            kindlebridge_schema::methods::APP_RESTART,
            params,
        )
    }

    fn app_rollback(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call(
            &params.serial,
            APP_ROLLBACK_FEATURE,
            kindlebridge_schema::methods::APP_ROLLBACK,
            params,
        )
    }

    fn app_uninstall(&self, params: &AppTargetParams) -> Result<AppSummary, RpcError> {
        self.remote_call(
            &params.serial,
            APP_UNINSTALL_FEATURE,
            kindlebridge_schema::methods::APP_UNINSTALL,
            params,
        )
    }

    fn app_list(&self, params: &SerialParams) -> Result<AppList, RpcError> {
        self.remote_call(
            &params.serial,
            APP_LIST_FEATURE,
            kindlebridge_schema::methods::APP_LIST,
            params,
        )
    }

    fn process_list(&self, params: &SerialParams) -> Result<ProcessList, RpcError> {
        self.remote_call(
            &params.serial,
            PROCESS_LIST_FEATURE,
            kindlebridge_schema::methods::PROCESS_LIST,
            params,
        )
    }

    fn process_signal(&self, params: &ProcessSignalParams) -> Result<ProcessSummary, RpcError> {
        self.remote_call(
            &params.serial,
            PROCESS_SIGNAL_FEATURE,
            kindlebridge_schema::methods::PROCESS_SIGNAL,
            params,
        )
    }

    fn log_tail(&self, params: &LogTailParams) -> Result<LogSnapshot, RpcError> {
        self.remote_call(
            &params.serial,
            LOG_TAIL_FEATURE,
            kindlebridge_schema::methods::LOG_TAIL,
            params,
        )
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

    fn remote_call<T: serde::de::DeserializeOwned>(
        &self,
        serial: &str,
        feature: &str,
        method: &str,
        params: &impl Serialize,
    ) -> Result<T, RpcError> {
        let device = self.require_feature(serial, feature)?;
        let value = device
            .session
            .lock()
            .map_err(|_| RpcError::internal_error())?
            .call(method, params)
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

struct DeviceSession {
    stream: Box<dyn FrameIo>,
    state: SessionState,
    stream_window: u32,
    control_sequence: u32,
}

impl DeviceSession {
    fn connect(address: SocketAddr) -> Result<(Self, DeviceHello), LinkError> {
        let (limits, transport) = session_transport_config();
        let stream = TcpFrameStream::connect(address, transport)?;
        Self::handshake(Box::new(stream), limits)
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
                    if result.is_ok() { "ok" } else { "error" }
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
        let buffers = usb_buffer_config();
        let open_started = Instant::now();
        let transport = kindlebridge_transport_usb::open(criteria, buffers)?;
        if recover_abandoned_frame {
            trace_usb_recovery(format_args!(
                "recovery transport opened after {} ms",
                open_started.elapsed().as_millis()
            ));
        }
        let (mut reader, mut writer) = transport.split();
        if recover_abandoned_frame {
            // Recovery may have to drain an entire old connection window and
            // scan the maximum payload fill before the device can answer the
            // new HELLO. Keep it bounded, but do not reuse the short abandoned-
            // session detection deadline for that actual recovery work.
            writer.set_write_timeout(USB_RECOVERY_TIMEOUT);
            let exchange_started = Instant::now();
            write_usb_recovery_exchange(&mut reader, &mut writer, limits)?;
            trace_usb_recovery(format_args!(
                "recovery exchange finished after {} ms",
                exchange_started.elapsed().as_millis()
            ));
            reader.set_read_timeout(USB_RECOVERY_TIMEOUT);
        }
        let mut stream = SplitFrameStream::new(reader, writer, transport_config)?;
        let session_id = new_session_id()?;
        let negotiate_started = Instant::now();
        let (state, device) =
            Self::negotiate(&mut stream, limits, recover_abandoned_frame, &session_id)?;
        if recover_abandoned_frame {
            trace_usb_recovery(format_args!(
                "recovery HELLO finished after {} ms",
                negotiate_started.elapsed().as_millis()
            ));
        }
        // The finite timeout exists only to detect an abandoned partial frame
        // or an endpoint with no FunctionFS owner during HELLO. Application
        // traffic returns to pure USB backpressure after both directions have
        // proved live.
        stream.reader_mut().set_read_timeout(Duration::MAX);
        stream.writer_mut().set_write_timeout(Duration::MAX);
        Ok((
            Self {
                stream: Box::new(stream),
                state,
                stream_window: DEFAULT_STREAM_WINDOW,
                control_sequence: 1,
            },
            device,
        ))
    }

    fn handshake(
        stream: Box<dyn FrameIo>,
        limits: DecodeLimits,
    ) -> Result<(Self, DeviceHello), LinkError> {
        let session_id = new_session_id()?;
        Self::handshake_with_session_id(stream, limits, &session_id)
    }

    fn handshake_with_session_id(
        mut stream: Box<dyn FrameIo>,
        limits: DecodeLimits,
        session_id: &str,
    ) -> Result<(Self, DeviceHello), LinkError> {
        let (state, device) = Self::negotiate(stream.as_mut(), limits, false, session_id)?;
        Ok((
            Self {
                stream,
                state,
                stream_window: DEFAULT_STREAM_WINDOW,
                control_sequence: 1,
            },
            device,
        ))
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

    fn call(&mut self, method: &str, params: &impl Serialize) -> Result<Value, LinkError> {
        let stream_id = self.state.allocate_stream_id()?;
        self.send(
            frame(
                Command::Open,
                stream_id,
                0,
                encode(&ServiceOpen {
                    service: SHELL_SERVICE.to_owned(),
                })?,
            )?,
            FrameContext::default(),
        )?;

        let opening = self
            .read_application_frame()?
            .ok_or(LinkError::Disconnected)?;
        if opening.header.command == Command::Reject {
            self.state
                .process_inbound(&opening.header, FrameContext::default())?;
            return Err(LinkError::Rejected(
                String::from_utf8_lossy(&opening.payload).into_owned(),
            ));
        }
        expect(&opening, Command::Accept, stream_id)?;
        let accept: ServiceAccept = decode(&opening.payload, "ACCEPT")?;
        if accept.initial_stream_window == 0 || accept.initial_stream_window > DEFAULT_STREAM_WINDOW
        {
            return Err(LinkError::InvalidHello);
        }
        self.state.process_inbound(
            &opening.header,
            FrameContext::accept(accept.initial_stream_window),
        )?;
        self.stream_window = accept.initial_stream_window;

        self.send_credit(stream_id, 1, self.stream_window)?;
        let call = DeviceCall {
            method: method.to_owned(),
            params: serde_json::to_value(params)?,
        };
        let mut request = frame(Command::Data, stream_id, 2, encode(&call)?)?;
        request.header.flags = FLAG_END_STREAM;
        self.send(request, FrameContext::default())?;

        let response = self
            .read_application_frame()?
            .ok_or(LinkError::Disconnected)?;
        expect(&response, Command::Data, stream_id)?;
        if response.header.flags & FLAG_END_STREAM == 0 {
            return Err(LinkError::UnexpectedFrame(
                "device reply did not end the stream",
            ));
        }
        self.state
            .process_inbound(&response.header, FrameContext::default())?;
        if response.header.payload_length != 0 {
            self.send_credit(0, self.control_sequence, response.header.payload_length)?;
            self.control_sequence = self
                .control_sequence
                .checked_add(1)
                .ok_or(LinkError::SequenceExhausted)?;
        }

        let close = self
            .read_application_frame()?
            .ok_or(LinkError::Disconnected)?;
        expect(&close, Command::Close, stream_id)?;
        self.state
            .process_inbound(&close.header, FrameContext::default())?;

        let reply: DeviceReply = decode(&response.payload, "device reply")?;
        reply.into_result().map_err(LinkError::Remote)
    }

    fn sync_push(
        &mut self,
        params: &SyncPushParams,
        file: &mut File,
        total_size: u64,
        file_hash: &str,
    ) -> Result<SyncPushResult, LinkError> {
        let (stream_id, _) = self.open_service(SYNC_SERVICE)?;
        let mut send_sequence = 2_u32;
        let request = SyncRequest::Push {
            transfer_id: params.transfer_id.clone(),
            remote_path: params.remote_path.clone(),
            total_size,
            file_hash: file_hash.to_owned(),
            block_size: params.block_size,
        };
        self.send_data(stream_id, send_sequence, encode(&request)?, false)?;
        send_sequence = next_sequence(send_sequence)?;

        let ready = self.read_sync_reply(stream_id, &mut send_sequence)?;
        let (transfer_id, offset) = match ready {
            SyncReply::Ready {
                transfer_id,
                offset,
                total_size: remote_size,
                file_hash: remote_hash,
            } if remote_size == total_size && remote_hash == file_hash => (transfer_id, offset),
            SyncReply::Failure { error } => return Err(LinkError::Remote(error)),
            _ => return Err(LinkError::UnexpectedFrame("invalid sync push READY")),
        };
        if params
            .transfer_id
            .as_ref()
            .is_some_and(|expected| expected != &transfer_id)
            || offset > total_size
        {
            return Err(LinkError::UnexpectedFrame("sync push resume mismatch"));
        }
        // READY is followed by credits restoring the metadata DATA frame. Drain
        // them before starting bulk OUT so every FunctionFS IN request is
        // available for flow-control traffic during the transfer.
        self.wait_for_full_send_credit(stream_id)?;
        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![
            0_u8;
            usize::try_from(params.block_size).map_err(|_| {
                LinkError::UnexpectedFrame("sync block size does not fit the host")
            })?
        ];
        let mut sent = offset;
        let mut sent_since_credit_drain = 0_u64;
        if sent == total_size {
            self.send_data(stream_id, send_sequence, Vec::new(), true)?;
            send_sequence = next_sequence(send_sequence)?;
        } else {
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    return Err(LinkError::UnexpectedFrame(
                        "local file ended before its declared size",
                    ));
                }
                let length = u32::try_from(read)
                    .map_err(|_| LinkError::UnexpectedFrame("sync block is too large"))?;
                sent = sent
                    .checked_add(u64::from(length))
                    .ok_or(LinkError::SequenceExhausted)?;
                if sent > total_size {
                    return Err(LinkError::UnexpectedFrame("local file grew during sync"));
                }
                self.wait_for_send_capacity(stream_id, length)?;
                let is_last = sent == total_size;
                // Complete every KBP DATA frame as its own bounded USB write
                // batch. The KT6 MTU3/FunctionFS stack eventually stalls when
                // partial nusb buffers span many protocol frames, even with a
                // shallow transfer queue.
                self.send_data(stream_id, send_sequence, buffer[..read].to_vec(), is_last)?;
                send_sequence = next_sequence(send_sequence)?;
                sent_since_credit_drain = sent_since_credit_drain
                    .checked_add(u64::from(length))
                    .ok_or(LinkError::SequenceExhausted)?;
                if is_last {
                    break;
                }
                if sent_since_credit_drain >= u64::from(SYNC_CREDIT_BATCH_SIZE) {
                    // USB bulk endpoints are independent: while the host is
                    // writing OUT, the device can block trying to return the
                    // stream and connection CREDIT frames on IN. Explicitly
                    // consume both at the device's batching boundary before
                    // queuing more OUT data.
                    self.wait_for_full_send_credit(stream_id)?;
                    sent_since_credit_drain = 0;
                }
            }
        }

        let completion = self.read_sync_reply(stream_id, &mut send_sequence)?;
        let result = match completion {
            SyncReply::Complete {
                transfer_id: completed_id,
                next_offset,
                total_size: completed_size,
            } if completed_id == transfer_id
                && next_offset == total_size
                && completed_size == total_size =>
            {
                SyncPushResult {
                    transfer_id,
                    accepted_offset: next_offset,
                    state: TransferState::Complete,
                }
            }
            SyncReply::Failure { error } => return Err(LinkError::Remote(error)),
            _ => return Err(LinkError::UnexpectedFrame("invalid sync push completion")),
        };
        self.read_close(stream_id)?;
        Ok(result)
    }

    fn sync_pull(&mut self, params: &SyncPullParams) -> Result<SyncPullResult, LinkError> {
        let (stream_id, _) = self.open_service(SYNC_SERVICE)?;
        let mut send_sequence = 2_u32;
        let staging = params
            .transfer_id
            .as_deref()
            .map(|id| staging_path(Path::new(&params.local_path), id));
        let offset = if let Some(path) = &staging {
            fs::metadata(path).map_or(0, |metadata| metadata.len())
        } else {
            0
        };
        let request = SyncRequest::Pull {
            transfer_id: params.transfer_id.clone(),
            remote_path: params.remote_path.clone(),
            offset,
            block_size: params.block_size,
        };
        self.send_data(stream_id, send_sequence, encode(&request)?, true)?;
        send_sequence = next_sequence(send_sequence)?;
        let ready = self.read_sync_reply(stream_id, &mut send_sequence)?;
        let (transfer_id, remote_offset, total_size, file_hash) = match ready {
            SyncReply::Ready {
                transfer_id,
                offset,
                total_size,
                file_hash,
            } => (transfer_id, offset, total_size, file_hash),
            SyncReply::Failure { error } => return Err(LinkError::Remote(error)),
            _ => return Err(LinkError::UnexpectedFrame("invalid sync pull READY")),
        };
        if params
            .transfer_id
            .as_ref()
            .is_some_and(|expected| expected != &transfer_id)
            || remote_offset != offset
            || remote_offset > total_size
        {
            return Err(LinkError::UnexpectedFrame("sync pull resume mismatch"));
        }
        let staging =
            staging.unwrap_or_else(|| staging_path(Path::new(&params.local_path), &transfer_id));
        let mut output = open_staging(&staging, remote_offset)?;
        let mut hasher = hash_prefix(&mut output, remote_offset)?;
        output.seek(SeekFrom::Start(remote_offset))?;
        let mut received = remote_offset;
        let mut received_batch = 0_u64;

        loop {
            let data = self
                .read_sync_data(stream_id)?
                .ok_or(LinkError::Disconnected)?;
            output.write_all(&data.payload)?;
            hasher.update(&data.payload);
            received = received
                .checked_add(u64::from(data.header.payload_length))
                .ok_or(LinkError::SequenceExhausted)?;
            if received > total_size {
                return Err(LinkError::UnexpectedFrame(
                    "sync pull exceeded declared size",
                ));
            }
            received_batch = received_batch
                .checked_add(u64::from(data.header.payload_length))
                .ok_or(LinkError::SequenceExhausted)?;
            let is_last = data.header.flags & FLAG_END_STREAM != 0;
            if received_batch >= u64::from(SYNC_CREDIT_BATCH_SIZE) || is_last {
                let delta = u32::try_from(received_batch)
                    .map_err(|_| LinkError::UnexpectedFrame("sync credit batch is too large"))?;
                self.restore_received_credit(stream_id, &mut send_sequence, delta)?;
                received_batch = 0;
            }
            if is_last {
                break;
            }
        }
        if received != total_size || hasher.finalize().to_hex().as_str() != file_hash {
            output.set_len(0)?;
            output.sync_all()?;
            return Err(LinkError::Remote(
                RpcError::new(error_codes::CHECKSUM_MISMATCH, "Checksum mismatch").with_data(
                    serde_json::json!({
                        "transfer_id": transfer_id,
                        "staging_path": staging,
                        "resume_offset": 0
                    }),
                ),
            ));
        }
        output.flush()?;
        output.sync_all()?;
        drop(output);
        commit_host_file(&staging, Path::new(&params.local_path))?;
        self.read_close(stream_id)?;
        Ok(SyncPullResult {
            transfer_id,
            total_size,
            received_size: received,
            state: TransferState::Complete,
        })
    }

    fn open_service(&mut self, service: &str) -> Result<(u32, u32), LinkError> {
        let stream_id = self.state.allocate_stream_id()?;
        self.send(
            frame(
                Command::Open,
                stream_id,
                0,
                encode(&ServiceOpen {
                    service: service.to_owned(),
                })?,
            )?,
            FrameContext::default(),
        )?;
        let opening = self
            .read_application_frame()?
            .ok_or(LinkError::Disconnected)?;
        if opening.header.command == Command::Reject {
            self.state
                .process_inbound(&opening.header, FrameContext::default())?;
            return Err(LinkError::Rejected(
                String::from_utf8_lossy(&opening.payload).into_owned(),
            ));
        }
        expect(&opening, Command::Accept, stream_id)?;
        let accept: ServiceAccept = decode(&opening.payload, "ACCEPT")?;
        if accept.initial_stream_window == 0 || accept.initial_stream_window > DEFAULT_STREAM_WINDOW
        {
            return Err(LinkError::InvalidHello);
        }
        self.state.process_inbound(
            &opening.header,
            FrameContext::accept(accept.initial_stream_window),
        )?;
        self.send_credit(stream_id, 1, accept.initial_stream_window)?;
        Ok((stream_id, accept.initial_stream_window))
    }

    fn read_sync_reply(
        &mut self,
        stream_id: u32,
        send_sequence: &mut u32,
    ) -> Result<SyncReply, LinkError> {
        let frame = self
            .read_sync_data(stream_id)?
            .ok_or(LinkError::Disconnected)?;
        self.restore_received_credit(stream_id, send_sequence, frame.header.payload_length)?;
        decode(&frame.payload, "sync reply")
    }

    fn read_sync_data(&mut self, stream_id: u32) -> Result<Option<Frame>, LinkError> {
        loop {
            let Some(frame) = self.read_application_frame()? else {
                return Ok(None);
            };
            if frame.header.command == Command::Credit && frame.header.stream_id == stream_id {
                self.state
                    .process_inbound(&frame.header, FrameContext::default())?;
                continue;
            }
            expect(&frame, Command::Data, stream_id)?;
            self.state
                .process_inbound(&frame.header, FrameContext::default())?;
            return Ok(Some(frame));
        }
    }

    fn restore_received_credit(
        &mut self,
        stream_id: u32,
        send_sequence: &mut u32,
        delta: u32,
    ) -> Result<(), LinkError> {
        if delta == 0 {
            return Ok(());
        }
        self.send_credit(stream_id, *send_sequence, delta)?;
        *send_sequence = next_sequence(*send_sequence)?;
        self.send_credit(0, self.control_sequence, delta)?;
        self.control_sequence = next_sequence(self.control_sequence)?;
        Ok(())
    }

    fn wait_for_send_capacity(&mut self, stream_id: u32, needed: u32) -> Result<(), LinkError> {
        loop {
            let stream_credit = self
                .state
                .stream(stream_id)
                .ok_or(LinkError::UnexpectedFrame("sync stream disappeared"))?
                .send_credit;
            if stream_credit >= needed && self.state.snapshot().connection_send_credit >= needed {
                return Ok(());
            }
            self.read_credit(stream_id)?;
        }
    }

    fn wait_for_full_send_credit(&mut self, stream_id: u32) -> Result<(), LinkError> {
        loop {
            let stream = self
                .state
                .stream(stream_id)
                .ok_or(LinkError::UnexpectedFrame("sync stream disappeared"))?;
            let connection = self.state.snapshot();
            if stream.send_credit == stream.send_limit
                && connection.connection_send_credit == connection.connection_send_limit
            {
                return Ok(());
            }
            self.read_credit(stream_id)?;
        }
    }

    fn read_credit(&mut self, stream_id: u32) -> Result<(), LinkError> {
        let frame = match self.stream.read_frame() {
            Ok(frame) => frame,
            Err(TransportError::EndOfStream) => return Err(LinkError::Disconnected),
            Err(error) => return Err(error.into()),
        };
        if frame.header.command != Command::Credit
            || (frame.header.stream_id != 0 && frame.header.stream_id != stream_id)
        {
            return Err(LinkError::UnexpectedFrame(
                "expected sync flow-control credit",
            ));
        }
        self.state
            .process_inbound(&frame.header, FrameContext::default())?;
        Ok(())
    }

    fn send_data(
        &mut self,
        stream_id: u32,
        sequence: u32,
        payload: Vec<u8>,
        end_stream: bool,
    ) -> Result<(), LinkError> {
        let mut data = frame(Command::Data, stream_id, sequence, payload)?;
        if end_stream {
            data.header.flags = FLAG_END_STREAM;
        }
        self.send(data, FrameContext::default())
    }

    fn read_close(&mut self, stream_id: u32) -> Result<(), LinkError> {
        loop {
            let frame = self
                .read_application_frame()?
                .ok_or(LinkError::Disconnected)?;
            if frame.header.command == Command::Credit && frame.header.stream_id == stream_id {
                self.state
                    .process_inbound(&frame.header, FrameContext::default())?;
                continue;
            }
            expect(&frame, Command::Close, stream_id)?;
            self.state
                .process_inbound(&frame.header, FrameContext::default())?;
            return Ok(());
        }
    }

    fn read_application_frame(&mut self) -> Result<Option<Frame>, LinkError> {
        loop {
            let frame = match self.stream.read_frame() {
                Ok(frame) => frame,
                Err(TransportError::EndOfStream) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            if frame.header.command == Command::Credit && frame.header.stream_id == 0 {
                self.state
                    .process_inbound(&frame.header, FrameContext::default())?;
                continue;
            }
            return Ok(Some(frame));
        }
    }

    fn send_credit(&mut self, stream_id: u32, sequence: u32, delta: u32) -> Result<(), LinkError> {
        let mut header = Header::new(Command::Credit, stream_id, sequence);
        header.credit_delta = delta;
        self.send(Frame::new(header, Vec::new())?, FrameContext::default())
    }

    fn send(&mut self, frame: Frame, context: FrameContext) -> Result<(), LinkError> {
        self.send_buffered(frame, context)?;
        self.flush_outbound()
    }

    fn send_buffered(&mut self, frame: Frame, context: FrameContext) -> Result<(), LinkError> {
        if frame.header.payload_length > MAX_HOST_TO_DEVICE_PAYLOAD {
            return Err(LinkError::OutboundFrameTooLarge {
                actual: frame.header.payload_length,
                maximum: MAX_HOST_TO_DEVICE_PAYLOAD,
            });
        }
        self.state.process_outbound(&frame.header, context)?;
        self.stream.write_frame(&frame)?;
        Ok(())
    }

    fn flush_outbound(&mut self) -> Result<(), LinkError> {
        self.stream.flush()?;
        Ok(())
    }
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

impl Drop for DeviceSession {
    fn drop(&mut self) {
        let sequence = self.control_sequence;
        let _ = self.send(
            frame(Command::GoAway, 0, sequence, Vec::new())
                .expect("an empty GOAWAY frame is always valid"),
            FrameContext::default(),
        );
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

fn validate_host_path(path: &str) -> Result<(), RpcError> {
    if Path::new(path).is_absolute() {
        Ok(())
    } else {
        Err(RpcError::invalid_params("local_path must be absolute"))
    }
}

fn validate_block_size(block_size: u32) -> Result<(), RpcError> {
    if (1..=MAX_SYNC_BLOCK_SIZE).contains(&block_size) {
        Ok(())
    } else {
        Err(RpcError::invalid_params(
            "block_size must be between 1 and 1048576",
        ))
    }
}

fn hash_file(file: &mut File, length: u64) -> Result<String, RpcError> {
    hash_prefix(file, length)
        .map(|hasher| hasher.finalize().to_hex().to_string())
        .map_err(|_| RpcError::new(error_codes::INVALID_STATE, "Host file could not be hashed"))
}

fn hash_prefix(file: &mut File, length: u64) -> Result<blake3::Hasher, LinkError> {
    file.seek(SeekFrom::Start(0))?;
    let mut remaining = length;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    while remaining != 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| LinkError::UnexpectedFrame("host file is too large"))?;
        let read = file.read(&mut buffer[..limit])?;
        if read == 0 {
            return Err(LinkError::UnexpectedFrame(
                "host staging file was truncated",
            ));
        }
        hasher.update(&buffer[..read]);
        remaining -= u64::try_from(read)
            .map_err(|_| LinkError::UnexpectedFrame("host file is too large"))?;
    }
    Ok(hasher)
}

fn staging_path(destination: &Path, transfer_id: &str) -> PathBuf {
    PathBuf::from(format!(
        "{}.kindlebridge-{}.part",
        destination.to_string_lossy(),
        transfer_id
    ))
}

fn open_staging(path: &Path, offset: u64) -> Result<File, LinkError> {
    let parent = path
        .parent()
        .ok_or(LinkError::UnexpectedFrame("host destination has no parent"))?;
    fs::create_dir_all(parent)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    let file = options.open(path)?;
    let actual = file.metadata()?.len();
    if actual < offset {
        return Err(LinkError::UnexpectedFrame(
            "host staging file is shorter than resume offset",
        ));
    }
    file.set_len(offset)?;
    Ok(file)
}

fn commit_host_file(staging: &Path, destination: &Path) -> Result<(), LinkError> {
    if destination.exists() {
        let metadata = fs::symlink_metadata(destination)?;
        if metadata.is_dir() {
            return Err(LinkError::UnexpectedFrame(
                "host destination is a directory",
            ));
        }
        fs::remove_file(destination)?;
    }
    fs::rename(staging, destination)?;
    Ok(())
}

fn host_file_error(operation: &str, path: &str, error: &std::io::Error) -> RpcError {
    RpcError::new(error_codes::INVALID_STATE, "Host file operation failed").with_data(
        serde_json::json!({
            "operation": operation,
            "path": path,
            "kind": format!("{:?}", error.kind())
        }),
    )
}

fn next_sequence(sequence: u32) -> Result<u32, LinkError> {
    sequence.checked_add(1).ok_or(LinkError::SequenceExhausted)
}

fn validate_device_hello(hello: &DeviceHello, expected_session_id: &str) -> Result<(), LinkError> {
    if hello.protocol_version != PROTOCOL_VERSION
        || !is_valid_session_id(&hello.session_id)
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
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error("host frame payload is {actual} bytes; maximum is {maximum}")]
    OutboundFrameTooLarge { actual: u32, maximum: u32 },
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
    #[error("a USB session identifier could not be generated")]
    SessionIdUnavailable,
    #[error("device rejected the service: {0}")]
    Rejected(String),
    #[error("device link disconnected")]
    Disconnected,
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
    use std::sync::{Arc, Condvar, Mutex as StdMutex};
    use std::thread;

    use kindlebridged::server::{ServerConfig, TcpServer};
    use kindlebridged::DeviceInfo;

    use super::*;

    const TEST_SESSION_ID: &str = "000102030405060708090a0b0c0d0e0f";
    const STALE_SESSION_ID: &str = "f0e0d0c0b0a090807060504030201000";

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

    #[test]
    fn host_rejects_frames_larger_than_the_recoverable_outbound_limit() {
        let (limits, config) = session_transport_config();
        let stream = SplitFrameStream::new(
            Cursor::new(Vec::<u8>::new()),
            SharedWriter::default(),
            config,
        )
        .unwrap();
        let mut session = DeviceSession {
            stream: Box::new(stream),
            state: SessionState::new(SessionConfig::new(EndpointRole::Host, limits)),
            stream_window: DEFAULT_STREAM_WINDOW,
            control_sequence: 1,
        };
        let payload = vec![0_u8; usize::try_from(MAX_HOST_TO_DEVICE_PAYLOAD).unwrap() + 1];
        let oversized = frame(Command::Data, 1, 0, payload).unwrap();

        assert!(matches!(
            session.send_buffered(oversized, FrameContext::default()),
            Err(LinkError::OutboundFrameTooLarge {
                actual,
                maximum: MAX_HOST_TO_DEVICE_PAYLOAD
            }) if actual == MAX_HOST_TO_DEVICE_PAYLOAD + 1
        ));
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

    struct FlushCountingFrameIo {
        inner: Box<dyn FrameIo>,
        flushes: Arc<AtomicUsize>,
    }

    impl FrameIo for FlushCountingFrameIo {
        fn read_frame(&mut self) -> Result<Frame, TransportError> {
            self.inner.read_frame()
        }

        fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
            self.inner.write_frame(frame)
        }

        fn flush(&mut self) -> Result<(), TransportError> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            self.inner.flush()
        }
    }

    struct RequireBatchCreditDrainFrameIo {
        inner: Box<dyn FrameIo>,
        ready_seen: bool,
        sent_since_drain: u64,
        stream_credit_seen: bool,
        connection_credit_seen: bool,
    }

    impl FrameIo for RequireBatchCreditDrainFrameIo {
        fn read_frame(&mut self) -> Result<Frame, TransportError> {
            let frame = self.inner.read_frame()?;
            if frame.header.command == Command::Data {
                self.ready_seen = true;
            } else if self.ready_seen
                && frame.header.command == Command::Credit
                && frame.header.credit_delta >= SYNC_CREDIT_BATCH_SIZE
            {
                if frame.header.stream_id == 0 {
                    self.connection_credit_seen = true;
                } else {
                    self.stream_credit_seen = true;
                }
                if self.stream_credit_seen && self.connection_credit_seen {
                    self.sent_since_drain = 0;
                    self.stream_credit_seen = false;
                    self.connection_credit_seen = false;
                }
            }
            Ok(frame)
        }

        fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
            if self.ready_seen && frame.header.command == Command::Data {
                if self.sent_since_drain >= u64::from(SYNC_CREDIT_BATCH_SIZE) {
                    return Err(TransportError::EndOfStream);
                }
                self.sent_since_drain += u64::from(frame.header.payload_length);
            }
            self.inner.write_frame(frame)
        }

        fn flush(&mut self) -> Result<(), TransportError> {
            self.inner.flush()
        }
    }

    #[test]
    fn split_endpoint_session_performs_usb_style_handshake() {
        let (limits, config) = session_transport_config();
        let hello = DeviceHello {
            protocol_version: PROTOCOL_VERSION,
            session_id: TEST_SESSION_ID.to_owned(),
            serial: "KT6-USB".to_owned(),
            model: "KT6".to_owned(),
            firmware: "5.17.1.0.4".to_owned(),
            target: "kindlehf".to_owned(),
            features: vec![SYNC_FEATURE.to_owned()],
            initial_connection_window: DEFAULT_CONNECTION_WINDOW,
        };
        let incoming = frame(Command::Hello, 0, 0, encode(&hello).unwrap())
            .unwrap()
            .encode(limits)
            .unwrap();
        let output = SharedWriter::default();
        let captured = Arc::clone(&output.0);
        let stream = SplitFrameStream::new(Cursor::new(incoming), output, config).unwrap();
        let (_session, received) =
            DeviceSession::handshake_with_session_id(Box::new(stream), limits, TEST_SESSION_ID)
                .unwrap();
        assert_eq!(received.serial, "KT6-USB");

        let bytes = captured.lock().unwrap().clone();
        let mut reader =
            kindlebridge_transport_tcp::FrameReader::new(Cursor::new(bytes), config).unwrap();
        let sent = reader.read_frame().unwrap();
        expect(&sent, Command::Hello, 0).unwrap();
        let host: HostHello = decode(&sent.payload, "host HELLO").unwrap();
        assert_eq!(host.protocol_version, PROTOCOL_VERSION);
        assert_eq!(host.session_id, TEST_SESSION_ID);
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

        let (_, received) =
            DeviceSession::negotiate(&mut stream, limits, true, TEST_SESSION_ID).unwrap();
        assert_eq!(received.serial, "KT6-USB-RECOVERED");
        assert_eq!(received.session_id, TEST_SESSION_ID);
    }

    #[test]
    fn usb_style_sync_pull_batches_received_credit() {
        let (limits, config) = session_transport_config();
        let payload = vec![0xa5; SYNC_CREDIT_BATCH_SIZE as usize];
        let transfer_id = "usb-credit-pull".to_owned();
        let destination = std::env::temp_dir().join(format!(
            "kindlebridge-usb-credit-pull-{}.bin",
            std::process::id()
        ));
        let _ = fs::remove_file(&destination);
        let hello = DeviceHello {
            protocol_version: PROTOCOL_VERSION,
            session_id: TEST_SESSION_ID.to_owned(),
            serial: "KT6-USB-CREDIT".to_owned(),
            model: "KT6".to_owned(),
            firmware: "5.17.1.0.4".to_owned(),
            target: "kindlehf".to_owned(),
            features: vec![SYNC_FEATURE.to_owned()],
            initial_connection_window: DEFAULT_CONNECTION_WINDOW,
        };
        let ready = SyncReply::Ready {
            transfer_id: transfer_id.clone(),
            offset: 0,
            total_size: payload.len() as u64,
            file_hash: blake3::hash(&payload).to_hex().to_string(),
        };
        let mut incoming = Vec::new();
        let mut append = |frame: Frame| {
            incoming.extend_from_slice(&frame.encode(limits).unwrap());
        };
        append(frame(Command::Hello, 0, 0, encode(&hello).unwrap()).unwrap());
        append(
            frame(
                Command::Accept,
                1,
                0,
                encode(&ServiceAccept {
                    initial_stream_window: DEFAULT_STREAM_WINDOW,
                })
                .unwrap(),
            )
            .unwrap(),
        );
        append(frame(Command::Data, 1, 1, encode(&ready).unwrap()).unwrap());
        for (index, chunk) in payload.chunks(64 * 1024).enumerate() {
            let mut data = frame(Command::Data, 1, 2 + index as u32, chunk.to_vec()).unwrap();
            if index + 1 == payload.len() / (64 * 1024) {
                data.header.flags = FLAG_END_STREAM;
            }
            append(data);
        }
        let close_sequence = 2 + u32::try_from(payload.len() / (64 * 1024))
            .expect("test payload chunk count fits in u32");
        append(frame(Command::Close, 1, close_sequence, Vec::new()).unwrap());

        let output = SharedWriter::default();
        let captured = Arc::clone(&output.0);
        let stream = SplitFrameStream::new(Cursor::new(incoming), output, config).unwrap();
        let (mut session, _) =
            DeviceSession::handshake_with_session_id(Box::new(stream), limits, TEST_SESSION_ID)
                .unwrap();
        let result = session
            .sync_pull(&SyncPullParams {
                serial: "KT6-USB-CREDIT".to_owned(),
                remote_path: "credit/payload.bin".to_owned(),
                local_path: destination.to_string_lossy().into_owned(),
                transfer_id: None,
                block_size: 64 * 1024,
            })
            .unwrap();
        assert_eq!(result.received_size, payload.len() as u64);
        assert_eq!(fs::read(&destination).unwrap(), payload);
        drop(session);

        let bytes = captured.lock().unwrap().clone();
        let mut reader =
            kindlebridge_transport_tcp::FrameReader::new(Cursor::new(bytes), config).unwrap();
        let mut batched_credits = 0;
        while let Ok(frame) = reader.read_frame() {
            if frame.header.command == Command::Credit
                && frame.header.credit_delta == SYNC_CREDIT_BATCH_SIZE
            {
                batched_credits += 1;
            }
        }
        assert_eq!(batched_credits, 2, "stream and connection credit");
        fs::remove_file(destination).unwrap();
    }

    #[test]
    fn pull_checksum_failure_resets_staging_for_the_same_transfer_id() {
        let (limits, config) = session_transport_config();
        let payload = vec![0xc3; 64 * 1024];
        let transfer_id = "pull-checksum-reset".to_owned();
        let destination = std::env::temp_dir().join(format!(
            "kindlebridge-pull-checksum-reset-{}.bin",
            std::process::id()
        ));
        let staging = staging_path(&destination, &transfer_id);
        let _ = fs::remove_file(&destination);
        let _ = fs::remove_file(&staging);
        let hello = DeviceHello {
            protocol_version: PROTOCOL_VERSION,
            session_id: TEST_SESSION_ID.to_owned(),
            serial: "KT6-PULL-CHECKSUM".to_owned(),
            model: "KT6".to_owned(),
            firmware: "5.17.1.0.4".to_owned(),
            target: "kindlehf".to_owned(),
            features: vec![SYNC_FEATURE.to_owned()],
            initial_connection_window: DEFAULT_CONNECTION_WINDOW,
        };
        let ready = SyncReply::Ready {
            transfer_id: transfer_id.clone(),
            offset: 0,
            total_size: payload.len() as u64,
            file_hash: blake3::hash(b"different payload").to_hex().to_string(),
        };
        let mut incoming = Vec::new();
        let mut append = |frame: Frame| {
            incoming.extend_from_slice(&frame.encode(limits).unwrap());
        };
        append(frame(Command::Hello, 0, 0, encode(&hello).unwrap()).unwrap());
        append(
            frame(
                Command::Accept,
                1,
                0,
                encode(&ServiceAccept {
                    initial_stream_window: DEFAULT_STREAM_WINDOW,
                })
                .unwrap(),
            )
            .unwrap(),
        );
        append(frame(Command::Data, 1, 1, encode(&ready).unwrap()).unwrap());
        let mut data = frame(Command::Data, 1, 2, payload).unwrap();
        data.header.flags = FLAG_END_STREAM;
        append(data);

        let stream = SplitFrameStream::new(Cursor::new(incoming), Vec::new(), config).unwrap();
        let (mut session, _) =
            DeviceSession::handshake_with_session_id(Box::new(stream), limits, TEST_SESSION_ID)
                .unwrap();
        let result = session.sync_pull(&SyncPullParams {
            serial: "KT6-PULL-CHECKSUM".to_owned(),
            remote_path: "checksum/payload.bin".to_owned(),
            local_path: destination.to_string_lossy().into_owned(),
            transfer_id: None,
            block_size: 64 * 1024,
        });

        assert!(matches!(
            result,
            Err(LinkError::Remote(error)) if error.code == error_codes::CHECKSUM_MISMATCH
        ));
        assert_eq!(fs::metadata(&staging).unwrap().len(), 0);
        fs::remove_file(staging).unwrap();
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
                kindlebridge_schema::device_protocol::APP_LIST_FEATURE,
                kindlebridge_schema::device_protocol::EXEC_FEATURE,
                kindlebridge_schema::device_protocol::LOG_TAIL_FEATURE,
                kindlebridge_schema::device_protocol::PROCESS_LIST_FEATURE,
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
        let unsupported = provider
            .app_start(&AppTargetParams {
                serial: "KT6-LINK".to_owned(),
                app_id: "org.example.reader".to_owned(),
            })
            .unwrap_err();
        assert_eq!(unsupported.code, error_codes::FEATURE_UNAVAILABLE);
        assert_eq!(
            unsupported.data.as_ref().unwrap()["feature"],
            kindlebridge_schema::device_protocol::APP_START_FEATURE
        );

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
    fn child_sleep_helper() {
        if std::env::var_os("KBP_CHILD_SLEEP").is_some() {
            std::thread::sleep(Duration::from_secs(1));
        }
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
        let raw = TcpFrameStream::connect(address, transport).unwrap();
        let flushes = Arc::new(AtomicUsize::new(0));
        let counted = FlushCountingFrameIo {
            inner: Box::new(raw),
            flushes: Arc::clone(&flushes),
        };
        let guarded = RequireBatchCreditDrainFrameIo {
            inner: Box::new(counted),
            ready_seen: false,
            sent_since_drain: 0,
            stream_credit_seen: false,
            connection_credit_seen: false,
        };
        let (mut session, _) = DeviceSession::handshake(Box::new(guarded), limits).unwrap();
        let before = flushes.load(Ordering::Relaxed);
        let mut file = File::open(&source).unwrap();
        session
            .sync_push(
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
            assert!(server.serve_once().is_err());
            server.serve_once()
        });

        let (mut interrupted, _) = DeviceSession::connect(address).unwrap();
        let (stream_id, _) = interrupted.open_service(SYNC_SERVICE).unwrap();
        let request = SyncRequest::Push {
            transfer_id: None,
            remote_path: "resume/payload.bin".to_owned(),
            total_size: payload.len() as u64,
            file_hash: file_hash.clone(),
            block_size: 256 * 1024,
        };
        interrupted
            .send_data(stream_id, 2, encode(&request).unwrap(), false)
            .unwrap();
        let mut send_sequence = 3;
        let ready = interrupted
            .read_sync_reply(stream_id, &mut send_sequence)
            .unwrap();
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
            interrupted
                .send_data(stream_id, send_sequence, chunk.to_vec(), false)
                .unwrap();
            send_sequence = next_sequence(send_sequence).unwrap();
        }
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
