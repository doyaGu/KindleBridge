// Values and field ordering are from the local kindlehf sysroot's
// linux/usb/functionfs.h and linux/usb/ch9.h.
const FUNCTIONFS_DESCRIPTORS_MAGIC_V2: u32 = 3;
const FUNCTIONFS_STRINGS_MAGIC: u32 = 2;
const FUNCTIONFS_HAS_FS_DESC: u32 = 1;
const FUNCTIONFS_HAS_HS_DESC: u32 = 2;
const FUNCTIONFS_HAS_MS_OS_DESC: u32 = 8;

const USB_DT_INTERFACE: u8 = 0x04;
const USB_DT_ENDPOINT: u8 = 0x05;
const USB_CLASS_VENDOR_SPEC: u8 = 0xff;
const USB_ENDPOINT_XFER_BULK: u8 = 0x02;

const INTERFACE_STRING: &[u8] = b"KindleBridge\0";
const DEVICE_INTERFACE_GUID_PROPERTY: &[u8] = b"DeviceInterfaceGUIDs\0";
const DEVICE_INTERFACE_GUID_VALUE: &[u8] = b"{3F5EC011-3CD6-4E0D-819C-387BED7DB3B5}\0\0";

pub const DEVICE_INTERFACE_GUID: &str = "{3F5EC011-3CD6-4E0D-819C-387BED7DB3B5}";
pub const DESCRIPTOR_LENGTH: usize = 191;
pub const STRING_LENGTH: usize = 31;

/// FunctionFS v2 descriptors with MS OS 1.0 WINUSB ID and interface GUID.
pub fn descriptor_bytes() -> [u8; DESCRIPTOR_LENGTH] {
    let mut output = [0_u8; DESCRIPTOR_LENGTH];
    let mut offset = 0;

    push_u32(&mut output, &mut offset, FUNCTIONFS_DESCRIPTORS_MAGIC_V2);
    push_u32(&mut output, &mut offset, DESCRIPTOR_LENGTH as u32);
    push_u32(
        &mut output,
        &mut offset,
        FUNCTIONFS_HAS_FS_DESC | FUNCTIONFS_HAS_HS_DESC | FUNCTIONFS_HAS_MS_OS_DESC,
    );
    push_u32(&mut output, &mut offset, 3); // fs_count
    push_u32(&mut output, &mut offset, 3); // hs_count
    push_u32(&mut output, &mut offset, 2); // os_count

    push_interface(&mut output, &mut offset);
    push_endpoint(&mut output, &mut offset, 0x01, 64);
    push_endpoint(&mut output, &mut offset, 0x82, 64);

    push_interface(&mut output, &mut offset);
    push_endpoint(&mut output, &mut offset, 0x01, 512);
    push_endpoint(&mut output, &mut offset, 0x82, 512);

    // FunctionFS-specific MS OS header: the leading interface byte is part of
    // dwLength, so this in-memory feature descriptor is 11 + 24 = 35 bytes.
    push_u8(&mut output, &mut offset, 0); // interface
    push_u32(&mut output, &mut offset, 35); // dwLength
    push_u16(&mut output, &mut offset, 1); // bcdVersion 1.0
    push_u16(&mut output, &mut offset, 4); // Extended Compat ID
    push_u8(&mut output, &mut offset, 1); // bCount
    push_u8(&mut output, &mut offset, 0); // Reserved

    push_u8(&mut output, &mut offset, 0); // bFirstInterfaceNumber
    push_u8(&mut output, &mut offset, 1); // Reserved1, required by MS OS 1.0
    push_bytes(&mut output, &mut offset, b"WINUSB\0\0");
    push_bytes(&mut output, &mut offset, &[0; 8]); // SubCompatibleID
    push_bytes(&mut output, &mut offset, &[0; 6]); // Reserved2

    // MS OS 1.0 Extended Properties descriptor. This registers a stable
    // interface GUID so WinUSB clients can open MI_01 without an INF. The
    // vendor FunctionFS implementation expands input bytes to UTF-16LE.
    push_u8(&mut output, &mut offset, 0); // interface
    push_u32(&mut output, &mut offset, 86); // 11-byte header + 75-byte property
    push_u16(&mut output, &mut offset, 1); // bcdVersion 1.0
    push_u16(&mut output, &mut offset, 5); // Extended Properties
    push_u16(&mut output, &mut offset, 1); // wCount

    push_u32(&mut output, &mut offset, 75); // dwSize
    push_u32(&mut output, &mut offset, 7); // REG_MULTI_SZ
    push_u16(
        &mut output,
        &mut offset,
        DEVICE_INTERFACE_GUID_PROPERTY.len() as u16,
    );
    push_bytes(&mut output, &mut offset, DEVICE_INTERFACE_GUID_PROPERTY);
    push_u32(
        &mut output,
        &mut offset,
        DEVICE_INTERFACE_GUID_VALUE.len() as u32,
    );
    push_bytes(&mut output, &mut offset, DEVICE_INTERFACE_GUID_VALUE);

    debug_assert_eq!(offset, DESCRIPTOR_LENGTH);
    output
}

/// FunctionFS string table with one en-US interface string.
pub fn string_bytes() -> [u8; STRING_LENGTH] {
    let mut output = [0_u8; STRING_LENGTH];
    let mut offset = 0;
    push_u32(&mut output, &mut offset, FUNCTIONFS_STRINGS_MAGIC);
    push_u32(&mut output, &mut offset, STRING_LENGTH as u32);
    push_u32(&mut output, &mut offset, 1); // str_count
    push_u32(&mut output, &mut offset, 1); // lang_count
    push_u16(&mut output, &mut offset, 0x0409); // en-US
    push_bytes(&mut output, &mut offset, INTERFACE_STRING);
    debug_assert_eq!(offset, STRING_LENGTH);
    output
}

fn push_interface(output: &mut [u8], offset: &mut usize) {
    push_bytes(
        output,
        offset,
        &[
            9,
            USB_DT_INTERFACE,
            0, // bInterfaceNumber, rewritten by FunctionFS
            0, // bAlternateSetting
            2, // bNumEndpoints
            USB_CLASS_VENDOR_SPEC,
            0x4b, // bInterfaceSubClass, KindleBridge
            0x01, // bInterfaceProtocol v1
            1,    // iInterface
        ],
    );
}

fn push_endpoint(output: &mut [u8], offset: &mut usize, address: u8, packet_size: u16) {
    push_u8(output, offset, 7);
    push_u8(output, offset, USB_DT_ENDPOINT);
    push_u8(output, offset, address);
    push_u8(output, offset, USB_ENDPOINT_XFER_BULK);
    push_u16(output, offset, packet_size);
    push_u8(output, offset, 0);
}

fn push_u8(output: &mut [u8], offset: &mut usize, value: u8) {
    output[*offset] = value;
    *offset += 1;
}

fn push_u16(output: &mut [u8], offset: &mut usize, value: u16) {
    push_bytes(output, offset, &value.to_le_bytes());
}

fn push_u32(output: &mut [u8], offset: &mut usize, value: u32) {
    push_bytes(output, offset, &value.to_le_bytes());
}

fn push_bytes(output: &mut [u8], offset: &mut usize, value: &[u8]) {
    let end = *offset + value.len();
    output[*offset..end].copy_from_slice(value);
    *offset = end;
}
