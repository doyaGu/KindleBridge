# Naming

KindleBridge names follow ownership boundaries so new transports, services,
and protocol versions do not force repository-wide renames.

## Stable external names

- `KindleBridge` is the product and `kindlebridge` is the host command.
- `kindlebridged` is the device daemon.
- KBP means Kindle Bridge Protocol. `KBP1`, `ffs.kbp`, and `/dev/usb-ffs/kbp`
  identify the versioned wire interface.
- KBB means KindleBridge Bundle. `KBB1`, `.kbb`, and
  `kindlebridge.bundle.v1` identify the versioned bundle format and profile.

These names are compatibility surfaces. A future major version gets a new
wire/format identifier and a versioned decoder; it does not rename the product.

## Code names

Rust packages use the searchable `kindlebridge-*` namespace. Generic packages
describe responsibility (`wire`, `schema`, `transport`, `bundle`); backend
packages append the mechanism (`transport-tcp`, `transport-usb`). KBP and KBB
are not package-name prefixes.

Types use their role in the containing module. Protocol-neutral types stay
neutral (`DeviceSession`, `ConnectedDeviceProvider`, `TcpServer`, `UsbServer`).
A protocol acronym belongs in a type only when that type represents a concrete
wire artifact or version.

Diagnostic executables use product plus purpose, such as
`kindlebridge-tcp-probe`, `kindlebridge-usb-bench`, and
`kindlebridge-ffs-probe`. Numbered milestone labels do not appear in long-lived
paths, commands, environment variables, or type names.
