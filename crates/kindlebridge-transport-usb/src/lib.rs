//! Blocking USB transport for the KBP byte stream.
//!
//! Discovery is deliberately interface-scoped. The implementation never calls
//! `detach_and_claim_interface`, `set_configuration`, or `reset`: it claims only
//! the exact vendor interface selected from descriptors. On Windows, that
//! interface (not the composite parent or MTP interface) must be bound to
//! WinUSB, preferably through a firmware-provided WCID descriptor.

use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

use nusb::descriptors::TransferType as NusbTransferType;
use nusb::io::{EndpointRead, EndpointWrite};
use nusb::transfer::{Bulk, In, Out};
use nusb::{DeviceInfo, MaybeFuture};
use thiserror::Error;

pub const VENDOR_INTERFACE_CLASS: u8 = 0xff;
pub const MIN_TRANSFER_SIZE: usize = 64;
pub const MAX_TRANSFER_SIZE: usize = 1024 * 1024;
pub const MAX_QUEUE_DEPTH: usize = 32;
pub const MAX_BUFFER_BYTES_PER_DIRECTION: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsbMatch {
    pub vendor_id: u16,
    pub product_id: u16,
    pub interface_subclass: u8,
    pub interface_protocol: u8,
    pub serial_number: Option<String>,
}

impl UsbMatch {
    #[must_use]
    pub fn interface_class(&self) -> u8 {
        VENDOR_INTERFACE_CLASS
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufferConfig {
    pub transfer_size: usize,
    pub read_queue_depth: usize,
    pub write_queue_depth: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

impl BufferConfig {
    pub fn validate(self) -> Result<Self, TransportError> {
        if !(MIN_TRANSFER_SIZE..=MAX_TRANSFER_SIZE).contains(&self.transfer_size) {
            return Err(TransportError::InvalidBufferConfig {
                reason: "transfer_size must be between 64 bytes and 1 MiB",
            });
        }
        validate_queue(self.read_queue_depth, "read_queue_depth")?;
        validate_queue(self.write_queue_depth, "write_queue_depth")?;
        if self.read_buffer_bytes() > MAX_BUFFER_BYTES_PER_DIRECTION {
            return Err(TransportError::InvalidBufferConfig {
                reason: "read buffer pool exceeds 16 MiB",
            });
        }
        if self.write_buffer_bytes() > MAX_BUFFER_BYTES_PER_DIRECTION {
            return Err(TransportError::InvalidBufferConfig {
                reason: "write buffer pool exceeds 16 MiB",
            });
        }
        if self.read_timeout.is_zero() {
            return Err(TransportError::InvalidBufferConfig {
                reason: "read_timeout must be positive",
            });
        }
        if self.write_timeout.is_zero() {
            return Err(TransportError::InvalidBufferConfig {
                reason: "write_timeout must be positive",
            });
        }
        Ok(self)
    }

    #[must_use]
    pub fn read_buffer_bytes(self) -> usize {
        self.transfer_size.saturating_mul(self.read_queue_depth)
    }

    #[must_use]
    pub fn write_buffer_bytes(self) -> usize {
        self.transfer_size.saturating_mul(self.write_queue_depth)
    }
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            transfer_size: 64 * 1024,
            read_queue_depth: 16,
            write_queue_depth: 16,
            // Bulk endpoints use their own flow control. A blocking timeout is
            // unsafe as a general default because EndpointWrite deliberately
            // leaves an already-submitted transfer pending when it returns
            // TimedOut; dropping the adapter then cancels a frame at an unknown
            // byte boundary. Callers that implement a reset/reconnect protocol
            // may opt into a finite timeout explicitly.
            read_timeout: Duration::MAX,
            write_timeout: Duration::MAX,
        }
    }
}

fn validate_queue(depth: usize, name: &'static str) -> Result<(), TransportError> {
    if !(1..=MAX_QUEUE_DEPTH).contains(&depth) {
        return Err(TransportError::InvalidBufferConfig {
            reason: if name == "read_queue_depth" {
                "read_queue_depth must be between 1 and 32"
            } else {
                "write_queue_depth must be between 1 and 32"
            },
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointTransferType {
    Control,
    Isochronous,
    Bulk,
    Interrupt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointSnapshot {
    pub address: u8,
    pub transfer_type: EndpointTransferType,
    pub max_packet_size: usize,
}

impl EndpointSnapshot {
    #[must_use]
    pub fn is_in(self) -> bool {
        self.address & 0x80 != 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InterfaceSnapshot {
    pub interface_number: u8,
    pub alternate_setting: u8,
    pub class: u8,
    pub subclass: u8,
    pub protocol: u8,
    pub endpoints: Vec<EndpointSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceSnapshot {
    pub vendor_id: u16,
    pub product_id: u16,
    pub serial_number: Option<String>,
    pub interfaces: Vec<InterfaceSnapshot>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointSelection {
    pub interface_number: u8,
    pub alternate_setting: u8,
    pub bulk_in_address: u8,
    pub bulk_out_address: u8,
    pub bulk_in_max_packet_size: usize,
    pub bulk_out_max_packet_size: usize,
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("invalid USB buffer configuration: {reason}")]
    InvalidBufferConfig { reason: &'static str },
    #[error("no KindleBridge USB device matched the exact VID/PID, serial and vendor interface")]
    DeviceNotFound,
    #[error(
        "KindleBridge USB interface is busy or access was denied; wait for another command to finish, close any stale kindlebridge-server process, and retry"
    )]
    DeviceBusy,
    #[error("{count} USB devices matched; specify an exact serial number")]
    AmbiguousDevice { count: usize },
    #[error("no exact vendor interface class/subclass/protocol match was found")]
    InterfaceNotFound,
    #[error("{count} alternate interfaces have valid KBP bulk endpoints")]
    AmbiguousInterface { count: usize },
    #[error(
        "matching interface {interface_number} alt {alternate_setting} has no bulk IN endpoint"
    )]
    MissingBulkIn {
        interface_number: u8,
        alternate_setting: u8,
    },
    #[error(
        "matching interface {interface_number} alt {alternate_setting} has no bulk OUT endpoint"
    )]
    MissingBulkOut {
        interface_number: u8,
        alternate_setting: u8,
    },
    #[error("matching interface {interface_number} alt {alternate_setting} has {count} bulk IN endpoints")]
    AmbiguousBulkIn {
        interface_number: u8,
        alternate_setting: u8,
        count: usize,
    },
    #[error("matching interface {interface_number} alt {alternate_setting} has {count} bulk OUT endpoints")]
    AmbiguousBulkOut {
        interface_number: u8,
        alternate_setting: u8,
        count: usize,
    },
    #[error("bulk endpoint 0x{address:02x} reports a zero max packet size")]
    InvalidMaxPacketSize { address: u8 },
    #[error("USB enumeration failed: {0}")]
    Enumerate(#[source] nusb::Error),
    #[error("USB operation {operation} failed: {source}")]
    UsbOperation {
        operation: &'static str,
        #[source]
        source: nusb::Error,
    },
    #[error("could not inspect the active USB configuration: {0}")]
    ActiveConfiguration(String),
    #[error("opened interface descriptors changed after selection")]
    DescriptorChanged,
}

/// Select exactly one device and one alternate interface from descriptor snapshots.
pub fn select_descriptors(
    devices: &[DeviceSnapshot],
    criteria: &UsbMatch,
) -> Result<EndpointSelection, TransportError> {
    let matching_devices: Vec<_> = devices
        .iter()
        .filter(|device| device_matches(device, criteria))
        .collect();
    let device = match matching_devices.as_slice() {
        [] => return Err(TransportError::DeviceNotFound),
        [device] => *device,
        many => return Err(TransportError::AmbiguousDevice { count: many.len() }),
    };
    select_interface(&device.interfaces, criteria)
}

fn device_matches(device: &DeviceSnapshot, criteria: &UsbMatch) -> bool {
    device.vendor_id == criteria.vendor_id
        && device.product_id == criteria.product_id
        && criteria.serial_number.as_deref().map_or(true, |serial| {
            device.serial_number.as_deref() == Some(serial)
        })
        && device.interfaces.iter().any(|interface| {
            interface.class == VENDOR_INTERFACE_CLASS
                && interface.subclass == criteria.interface_subclass
                && interface.protocol == criteria.interface_protocol
        })
}

fn select_interface(
    interfaces: &[InterfaceSnapshot],
    criteria: &UsbMatch,
) -> Result<EndpointSelection, TransportError> {
    let matching: Vec<_> = interfaces
        .iter()
        .filter(|interface| {
            interface.class == VENDOR_INTERFACE_CLASS
                && interface.subclass == criteria.interface_subclass
                && interface.protocol == criteria.interface_protocol
        })
        .collect();
    if matching.is_empty() {
        return Err(TransportError::InterfaceNotFound);
    }

    let mut valid = Vec::new();
    let mut first_error = None;
    for interface in matching {
        match select_endpoints(interface) {
            Ok(selection) => valid.push(selection),
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }
    match valid.as_slice() {
        [selection] => Ok(*selection),
        [] => Err(first_error.unwrap_or(TransportError::InterfaceNotFound)),
        many => Err(TransportError::AmbiguousInterface { count: many.len() }),
    }
}

fn select_endpoints(interface: &InterfaceSnapshot) -> Result<EndpointSelection, TransportError> {
    let bulk_in: Vec<_> = interface
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.transfer_type == EndpointTransferType::Bulk && endpoint.is_in())
        .collect();
    let bulk_out: Vec<_> = interface
        .endpoints
        .iter()
        .filter(|endpoint| {
            endpoint.transfer_type == EndpointTransferType::Bulk && !endpoint.is_in()
        })
        .collect();
    let bulk_in = unique_endpoint(interface, &bulk_in, true)?;
    let bulk_out = unique_endpoint(interface, &bulk_out, false)?;
    if bulk_in.max_packet_size == 0 {
        return Err(TransportError::InvalidMaxPacketSize {
            address: bulk_in.address,
        });
    }
    if bulk_out.max_packet_size == 0 {
        return Err(TransportError::InvalidMaxPacketSize {
            address: bulk_out.address,
        });
    }
    Ok(EndpointSelection {
        interface_number: interface.interface_number,
        alternate_setting: interface.alternate_setting,
        bulk_in_address: bulk_in.address,
        bulk_out_address: bulk_out.address,
        bulk_in_max_packet_size: bulk_in.max_packet_size,
        bulk_out_max_packet_size: bulk_out.max_packet_size,
    })
}

fn unique_endpoint<'a>(
    interface: &InterfaceSnapshot,
    endpoints: &[&'a EndpointSnapshot],
    is_in: bool,
) -> Result<&'a EndpointSnapshot, TransportError> {
    match endpoints {
        [endpoint] => Ok(*endpoint),
        [] if is_in => Err(TransportError::MissingBulkIn {
            interface_number: interface.interface_number,
            alternate_setting: interface.alternate_setting,
        }),
        [] => Err(TransportError::MissingBulkOut {
            interface_number: interface.interface_number,
            alternate_setting: interface.alternate_setting,
        }),
        many if is_in => Err(TransportError::AmbiguousBulkIn {
            interface_number: interface.interface_number,
            alternate_setting: interface.alternate_setting,
            count: many.len(),
        }),
        many => Err(TransportError::AmbiguousBulkOut {
            interface_number: interface.interface_number,
            alternate_setting: interface.alternate_setting,
            count: many.len(),
        }),
    }
}

pub struct UsbReader {
    inner: Option<EndpointRead<Bulk>>,
}

impl UsbReader {
    /// Change how long a blocking read waits for the next USB transfer.
    ///
    /// The host uses a finite value only while establishing a session, then
    /// restores `Duration::MAX` before application traffic begins.
    pub fn set_read_timeout(&mut self, timeout: Duration) {
        self.inner
            .as_mut()
            .expect("USB reader is present until drop")
            .set_read_timeout(timeout);
    }
}

impl Read for UsbReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.inner
            .as_mut()
            .expect("USB reader is present until drop")
            .read(buffer)
    }
}

impl Drop for UsbReader {
    fn drop(&mut self) {
        if let Some(reader) = self.inner.take() {
            let mut endpoint = reader.into_inner();
            endpoint.cancel_all();
            drain_cancelled_transfers(&mut endpoint);
        }
    }
}

pub struct UsbWriter {
    inner: Option<EndpointWrite<Bulk>>,
}

impl UsbWriter {
    /// Change how long a blocking write waits for an in-flight USB transfer.
    ///
    /// A finite value is safe while no KBP session exists: a failed handshake
    /// drops the transport and cancels every partial recovery transfer. Normal
    /// application traffic restores `Duration::MAX` after HELLO.
    pub fn set_write_timeout(&mut self, timeout: Duration) {
        self.inner
            .as_mut()
            .expect("USB writer is present until drop")
            .set_write_timeout(timeout);
    }
}

impl Write for UsbWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.inner
            .as_mut()
            .expect("USB writer is present until drop")
            .write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner
            .as_mut()
            .expect("USB writer is present until drop")
            .flush()
    }
}

impl Drop for UsbWriter {
    fn drop(&mut self) {
        if let Some(writer) = self.inner.take() {
            let mut endpoint = writer.into_inner();
            endpoint.cancel_all();
            drain_cancelled_transfers(&mut endpoint);
        }
    }
}

fn drain_cancelled_transfers<EpType, Direction>(endpoint: &mut nusb::Endpoint<EpType, Direction>)
where
    EpType: nusb::transfer::EndpointType + nusb::transfer::BulkOrInterrupt,
    Direction: nusb::transfer::EndpointDirection,
{
    const MAX_DRAIN: Duration = Duration::from_millis(500);
    const POLL: Duration = Duration::from_millis(25);
    let deadline = Instant::now() + MAX_DRAIN;
    while endpoint.pending() > 0 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let _ = endpoint.wait_next_complete(remaining.min(POLL));
    }
}

pub struct UsbTransport {
    pub selection: EndpointSelection,
    pub reader: UsbReader,
    pub writer: UsbWriter,
}

impl UsbTransport {
    #[must_use]
    pub fn split(self) -> (UsbReader, UsbWriter) {
        (self.reader, self.writer)
    }
}

/// Discover, verify and claim one KBP vendor interface using nusb's blocking API.
pub fn open(criteria: &UsbMatch, buffers: BufferConfig) -> Result<UsbTransport, TransportError> {
    let buffers = buffers.validate()?;
    let device_infos: Vec<DeviceInfo> = nusb::list_devices()
        .wait()
        .map_err(TransportError::Enumerate)?
        .filter(|device| device_info_matches(device, criteria))
        .collect();
    let device_info = match device_infos.as_slice() {
        [] => return Err(TransportError::DeviceNotFound),
        [device] => device,
        many => return Err(TransportError::AmbiguousDevice { count: many.len() }),
    };
    let expected_interfaces: Vec<u8> = device_info
        .interfaces()
        .filter(|interface| interface_info_matches(interface, criteria))
        .map(nusb::InterfaceInfo::interface_number)
        .collect();
    if expected_interfaces.is_empty() {
        return Err(TransportError::InterfaceNotFound);
    }

    let device = device_info
        .open()
        .wait()
        .map_err(|source| usb_operation_error("open device", source))?;
    let configuration = device
        .active_configuration()
        .map_err(|error| TransportError::ActiveConfiguration(error.to_string()))?;
    let interfaces: Vec<_> = configuration
        .interface_alt_settings()
        .filter(|interface| expected_interfaces.contains(&interface.interface_number()))
        .map(|interface| InterfaceSnapshot {
            interface_number: interface.interface_number(),
            alternate_setting: interface.alternate_setting(),
            class: interface.class(),
            subclass: interface.subclass(),
            protocol: interface.protocol(),
            endpoints: interface
                .endpoints()
                .map(|endpoint| EndpointSnapshot {
                    address: endpoint.address(),
                    transfer_type: map_transfer_type(endpoint.transfer_type()),
                    max_packet_size: endpoint.max_packet_size(),
                })
                .collect(),
        })
        .collect();
    let selection = select_interface(&interfaces, criteria)?;
    validate_rounded_pools(buffers, selection)?;

    // Intentionally claim only this interface. Never detach a class driver or claim the parent.
    let interface = device
        .claim_interface(selection.interface_number)
        .wait()
        .map_err(|source| usb_operation_error("claim vendor interface", source))?;
    if interface.get_alt_setting() != selection.alternate_setting {
        interface
            .set_alt_setting(selection.alternate_setting)
            .wait()
            .map_err(|source| TransportError::UsbOperation {
                operation: "select vendor interface alternate setting",
                source,
            })?;
    }
    let current = interface
        .descriptor()
        .ok_or(TransportError::DescriptorChanged)?;
    if current.interface_number() != selection.interface_number
        || current.alternate_setting() != selection.alternate_setting
        || current.class() != VENDOR_INTERFACE_CLASS
        || current.subclass() != criteria.interface_subclass
        || current.protocol() != criteria.interface_protocol
    {
        return Err(TransportError::DescriptorChanged);
    }

    let in_endpoint = interface
        .endpoint::<Bulk, In>(selection.bulk_in_address)
        .map_err(|source| TransportError::UsbOperation {
            operation: "open bulk IN endpoint",
            source,
        })?;
    let out_endpoint = interface
        .endpoint::<Bulk, Out>(selection.bulk_out_address)
        .map_err(|source| TransportError::UsbOperation {
            operation: "open bulk OUT endpoint",
            source,
        })?;
    if in_endpoint.max_packet_size() != selection.bulk_in_max_packet_size
        || out_endpoint.max_packet_size() != selection.bulk_out_max_packet_size
    {
        return Err(TransportError::DescriptorChanged);
    }
    let reader = in_endpoint
        .reader(buffers.transfer_size)
        .with_num_transfers(buffers.read_queue_depth)
        .with_read_timeout(buffers.read_timeout);
    let writer = out_endpoint
        .writer(buffers.transfer_size)
        .with_num_transfers(buffers.write_queue_depth)
        // Flow-control backpressure has no wall-clock deadline. nusb keeps a
        // timed-out transfer submitted, so a finite timeout would make KBP lose
        // track of the exact byte boundary during error unwinding.
        .with_write_timeout(buffers.write_timeout);
    Ok(UsbTransport {
        selection,
        reader: UsbReader {
            inner: Some(reader),
        },
        writer: UsbWriter {
            inner: Some(writer),
        },
    })
}

fn usb_operation_error(operation: &'static str, source: nusb::Error) -> TransportError {
    if is_busy_usb_error(source.kind(), source.os_error()) {
        TransportError::DeviceBusy
    } else {
        TransportError::UsbOperation { operation, source }
    }
}

fn is_busy_usb_error(kind: nusb::ErrorKind, os_error: Option<u32>) -> bool {
    kind == nusb::ErrorKind::Busy || cfg!(windows) && os_error == Some(5)
}

fn validate_rounded_pools(
    buffers: BufferConfig,
    selection: EndpointSelection,
) -> Result<(), TransportError> {
    let read_size = buffers
        .transfer_size
        .div_ceil(selection.bulk_in_max_packet_size)
        .checked_mul(selection.bulk_in_max_packet_size)
        .ok_or(TransportError::InvalidBufferConfig {
            reason: "rounded read transfer size overflow",
        })?;
    let write_size = buffers
        .transfer_size
        .div_ceil(selection.bulk_out_max_packet_size)
        .checked_mul(selection.bulk_out_max_packet_size)
        .ok_or(TransportError::InvalidBufferConfig {
            reason: "rounded write transfer size overflow",
        })?;
    if read_size.saturating_mul(buffers.read_queue_depth) > MAX_BUFFER_BYTES_PER_DIRECTION {
        return Err(TransportError::InvalidBufferConfig {
            reason: "USB packet-rounded read pool exceeds 16 MiB",
        });
    }
    if write_size.saturating_mul(buffers.write_queue_depth) > MAX_BUFFER_BYTES_PER_DIRECTION {
        return Err(TransportError::InvalidBufferConfig {
            reason: "USB packet-rounded write pool exceeds 16 MiB",
        });
    }
    Ok(())
}

fn device_info_matches(device: &DeviceInfo, criteria: &UsbMatch) -> bool {
    device.vendor_id() == criteria.vendor_id
        && device.product_id() == criteria.product_id
        && criteria
            .serial_number
            .as_deref()
            .map_or(true, |serial| device.serial_number() == Some(serial))
        && device
            .interfaces()
            .any(|interface| interface_info_matches(interface, criteria))
}

fn interface_info_matches(interface: &nusb::InterfaceInfo, criteria: &UsbMatch) -> bool {
    interface.class() == VENDOR_INTERFACE_CLASS
        && interface.subclass() == criteria.interface_subclass
        && interface.protocol() == criteria.interface_protocol
}

const fn map_transfer_type(transfer_type: NusbTransferType) -> EndpointTransferType {
    match transfer_type {
        NusbTransferType::Control => EndpointTransferType::Control,
        NusbTransferType::Isochronous => EndpointTransferType::Isochronous,
        NusbTransferType::Bulk => EndpointTransferType::Bulk,
        NusbTransferType::Interrupt => EndpointTransferType::Interrupt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MATCH: UsbMatch = UsbMatch {
        vendor_id: 0x1949,
        product_id: 0x9981,
        interface_subclass: 0x4b,
        interface_protocol: 0x01,
        serial_number: None,
    };

    fn endpoint(address: u8, transfer_type: EndpointTransferType) -> EndpointSnapshot {
        EndpointSnapshot {
            address,
            transfer_type,
            max_packet_size: 512,
        }
    }

    fn kbp_interface(number: u8) -> InterfaceSnapshot {
        InterfaceSnapshot {
            interface_number: number,
            alternate_setting: 0,
            class: VENDOR_INTERFACE_CLASS,
            subclass: MATCH.interface_subclass,
            protocol: MATCH.interface_protocol,
            endpoints: vec![
                endpoint(0x81, EndpointTransferType::Bulk),
                endpoint(0x02, EndpointTransferType::Bulk),
            ],
        }
    }

    fn mtp_interface() -> InterfaceSnapshot {
        InterfaceSnapshot {
            interface_number: 0,
            alternate_setting: 0,
            class: 0x06,
            subclass: 0x01,
            protocol: 0x01,
            endpoints: vec![
                endpoint(0x83, EndpointTransferType::Bulk),
                endpoint(0x04, EndpointTransferType::Bulk),
            ],
        }
    }

    fn device(serial: &str, interfaces: Vec<InterfaceSnapshot>) -> DeviceSnapshot {
        DeviceSnapshot {
            vendor_id: MATCH.vendor_id,
            product_id: MATCH.product_id,
            serial_number: Some(serial.to_owned()),
            interfaces,
        }
    }

    #[test]
    fn selects_only_the_vendor_interface_not_mtp() {
        let selection = select_descriptors(
            &[device("KT6", vec![mtp_interface(), kbp_interface(3)])],
            &MATCH,
        )
        .unwrap();
        assert_eq!(selection.interface_number, 3);
        assert_eq!(selection.bulk_in_address, 0x81);
        assert_eq!(selection.bulk_out_address, 0x02);
    }

    #[test]
    fn optional_serial_is_an_exact_disambiguator() {
        let devices = [
            device("ONE", vec![kbp_interface(3)]),
            device("TWO", vec![kbp_interface(3)]),
        ];
        assert!(matches!(
            select_descriptors(&devices, &MATCH),
            Err(TransportError::AmbiguousDevice { count: 2 })
        ));
        let mut criteria = MATCH.clone();
        criteria.serial_number = Some("TWO".to_owned());
        assert!(select_descriptors(&devices, &criteria).is_ok());
        criteria.serial_number = Some("two".to_owned());
        assert!(matches!(
            select_descriptors(&devices, &criteria),
            Err(TransportError::DeviceNotFound)
        ));
    }

    #[test]
    fn rejects_duplicate_bulk_endpoints() {
        let mut interface = kbp_interface(3);
        interface
            .endpoints
            .push(endpoint(0x82, EndpointTransferType::Bulk));
        assert!(matches!(
            select_descriptors(&[device("KT6", vec![interface])], &MATCH),
            Err(TransportError::AmbiguousBulkIn { count: 2, .. })
        ));
    }

    #[test]
    fn rejects_missing_bulk_direction() {
        let mut interface = kbp_interface(3);
        interface.endpoints.retain(|endpoint| endpoint.is_in());
        assert!(matches!(
            select_descriptors(&[device("KT6", vec![interface])], &MATCH),
            Err(TransportError::MissingBulkOut { .. })
        ));
    }

    #[test]
    fn rejects_more_than_one_valid_vendor_interface() {
        assert!(matches!(
            select_descriptors(
                &[device("KT6", vec![kbp_interface(3), kbp_interface(4)])],
                &MATCH
            ),
            Err(TransportError::AmbiguousInterface { count: 2 })
        ));
    }

    #[test]
    fn ignores_non_bulk_endpoints_but_requires_one_bulk_pair() {
        let mut interface = kbp_interface(3);
        interface
            .endpoints
            .push(endpoint(0x85, EndpointTransferType::Interrupt));
        interface
            .endpoints
            .push(endpoint(0x06, EndpointTransferType::Isochronous));
        assert!(select_descriptors(&[device("KT6", vec![interface])], &MATCH).is_ok());
    }

    #[test]
    fn buffer_pool_is_strictly_bounded() {
        let config = BufferConfig::default().validate().unwrap();
        assert_eq!(config.read_buffer_bytes(), 1024 * 1024);
        assert_eq!(config.write_buffer_bytes(), 1024 * 1024);
        assert_eq!(config.read_timeout, Duration::MAX);
        assert_eq!(config.write_timeout, Duration::MAX);
        assert!(matches!(
            BufferConfig {
                write_timeout: Duration::ZERO,
                ..BufferConfig::default()
            }
            .validate(),
            Err(TransportError::InvalidBufferConfig { .. })
        ));
        let invalid = BufferConfig {
            transfer_size: MAX_TRANSFER_SIZE,
            read_queue_depth: MAX_QUEUE_DEPTH,
            ..BufferConfig::default()
        };
        assert!(matches!(
            invalid.validate(),
            Err(TransportError::InvalidBufferConfig { .. })
        ));

        let packet_rounded = BufferConfig {
            transfer_size: 541_200,
            read_queue_depth: 31,
            write_queue_depth: 1,
            ..BufferConfig::default()
        }
        .validate()
        .unwrap();
        let mut selection =
            select_descriptors(&[device("KT6", vec![kbp_interface(3)])], &MATCH).unwrap();
        selection.bulk_in_max_packet_size = 1024;
        assert!(matches!(
            validate_rounded_pools(packet_rounded, selection),
            Err(TransportError::InvalidBufferConfig { .. })
        ));
    }

    #[test]
    fn adapters_satisfy_standard_kbp_io_bounds() {
        fn accepts_reader<T: Read>() {}
        fn accepts_writer<T: Write>() {}
        accepts_reader::<UsbReader>();
        accepts_writer::<UsbWriter>();
    }

    #[test]
    fn classifies_exclusive_usb_ownership_as_busy() {
        assert!(is_busy_usb_error(nusb::ErrorKind::Busy, None));
        #[cfg(windows)]
        assert!(is_busy_usb_error(nusb::ErrorKind::Other, Some(5)));
        assert!(!is_busy_usb_error(nusb::ErrorKind::Other, None));
    }
}
