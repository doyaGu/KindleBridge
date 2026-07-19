# Windows WinUSB onboarding

KindleBridge keeps Amazon's stock `VID_1949:PID_9981` identity and stock MTP
interface. Its KBP function is the separate `MI_01` interface; the stock
firmware's Microsoft compatible-ID descriptor already binds that interface to
Windows' inbox WinUSB service. On the tested KT6 firmware, Windows does not
persist the FunctionFS extended-property descriptor that should publish the
KindleBridge device-interface GUID.

The repository therefore includes a narrowly scoped onboarding script. It
does not install a kernel driver, replace the driver for the composite parent
or MTP `MI_00`, allocate another VID/PID, or depend on pid.codes. It only adds
the stable KindleBridge GUID to `DeviceInterfaceGUIDs` for connected instances
whose instance ID, hardware ID, and active service all match the KBP `MI_01`
interface.

## Install

Start USB Bridge on the Kindle, connect it to Windows, and inspect the proposed
change from a normal PowerShell window:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install-windows-winusb.ps1 -DryRun
```

Then run the installer from an **elevated** PowerShell window:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install-windows-winusb.ps1
```

The script restarts only `MI_01` so the interface path appears immediately.
Pass `-NoRestart` if a current transfer must not be interrupted, then unplug
and reconnect the Kindle later. Existing unrelated interface GUIDs are
preserved.

To remove only the KindleBridge GUID:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install-windows-winusb.ps1 -Remove
```

The interface must be present for installation and removal. The change is
stored in that Windows device instance, so a fresh Windows installation, a
different Kindle, or a connection that produces a new device instance must be
onboarded once.

## Offline validation

CI and maintainers can validate the fail-closed selector without a connected
Kindle or administrative access:

```powershell
pwsh -NoProfile -File scripts/install-windows-winusb.ps1 -Validate
```

A custom INF was deliberately not chosen for this development path. Modern
64-bit Windows requires such a package to be signed even when it selects the
inbox WinUSB binary, adding certificate and release maintenance while the
firmware already selects WinUSB correctly. A signed distribution package or a
firmware descriptor fix can replace this registry onboarding later without
changing the KindleBridge interface GUID.

The registry value and reconnect behavior follow Microsoft's
[`DeviceInterfaceGUIDs` WinUSB guidance](https://learn.microsoft.com/en-us/windows-hardware/drivers/usbcon/winusb-installation),
and the interface-only restart uses the documented
[`pnputil /restart-device` form](https://learn.microsoft.com/en-us/windows-hardware/drivers/devtest/pnputil-examples#restart-device).
