[CmdletBinding(SupportsShouldProcess = $true, DefaultParameterSetName = 'Install')]
param(
    [Parameter(ParameterSetName = 'Install')]
    [Parameter(ParameterSetName = 'Remove')]
    [string[]]$InstanceId,

    [Parameter(Mandatory = $true, ParameterSetName = 'Remove')]
    [switch]$Remove,

    [Parameter(Mandatory = $true, ParameterSetName = 'Validate')]
    [switch]$Validate,

    [Parameter(ParameterSetName = 'Install')]
    [Parameter(ParameterSetName = 'Remove')]
    [switch]$DryRun,

    [Parameter(ParameterSetName = 'Install')]
    [Parameter(ParameterSetName = 'Remove')]
    [switch]$NoRestart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$BridgeGuid = '{3F5EC011-3CD6-4E0D-819C-387BED7DB3B5}'
$BridgeInstancePattern = '^USB\\VID_1949&PID_9981&MI_01\\[^\\]+$'
$BridgeHardwareId = 'USB\VID_1949&PID_9981&MI_01'

function Test-KindleBridgeInstanceId {
    param([Parameter(Mandatory = $true)][string]$Value)

    return [regex]::IsMatch(
        $Value,
        $BridgeInstancePattern,
        [Text.RegularExpressions.RegexOptions]::IgnoreCase
    )
}

function Get-DevicePropertyData {
    param(
        [Parameter(Mandatory = $true)][string]$DeviceInstanceId,
        [Parameter(Mandatory = $true)][string]$KeyName
    )

    return (Get-PnpDeviceProperty `
        -InstanceId $DeviceInstanceId `
        -KeyName $KeyName `
        -ErrorAction Stop).Data
}

function Get-DesiredGuids {
    param(
        [string[]]$Current,
        [Parameter(Mandatory = $true)][bool]$Removing
    )

    $desired = @($Current | Where-Object { $_ })
    if ($Removing) {
        return @($desired | Where-Object { $_ -ine $BridgeGuid })
    }
    if (-not ($desired -icontains $BridgeGuid)) {
        $desired += $BridgeGuid
    }
    return @($desired)
}

function Test-StringSequenceEqual {
    param([string[]]$Left, [string[]]$Right)

    if (@($Left).Count -ne @($Right).Count) {
        return $false
    }
    for ($index = 0; $index -lt @($Left).Count; $index++) {
        if ($Left[$index] -ine $Right[$index]) {
            return $false
        }
    }
    return $true
}

function Test-ScriptConstants {
    $validId = 'USB\VID_1949&PID_9981&MI_01\KINDLEBRIDGE-VALIDATION'
    $invalidIds = @(
        'USB\VID_1949&PID_9981\KINDLE-PARENT',
        'USB\VID_1949&PID_9981&MI_00\KINDLE-MTP',
        'USB\VID_1949&PID_9982&MI_01\OTHER-PRODUCT',
        'USB\VID_0525&PID_A4A2\LEGACY-RNDIS'
    )

    if (-not (Test-KindleBridgeInstanceId $validId)) {
        throw 'The onboarding selector rejects the KindleBridge MI_01 interface.'
    }
    foreach ($invalidId in $invalidIds) {
        if (Test-KindleBridgeInstanceId $invalidId) {
            throw "The onboarding selector is unsafe: it accepts $invalidId"
        }
    }
    if ([guid]::Parse($BridgeGuid).ToString('B').ToUpperInvariant() -ne $BridgeGuid) {
        throw 'The KindleBridge device-interface GUID is not canonical.'
    }

    $otherGuid = '{AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE}'
    $installed = @(Get-DesiredGuids -Current @($otherGuid) -Removing $false)
    if ($installed.Count -ne 2 -or $installed[0] -ne $otherGuid -or $installed[1] -ne $BridgeGuid) {
        throw 'Installation does not preserve existing interface GUIDs.'
    }
    $idempotent = @(Get-DesiredGuids -Current $installed -Removing $false)
    if (-not (Test-StringSequenceEqual $installed $idempotent)) {
        throw 'Repeated installation is not idempotent.'
    }
    $removed = @(Get-DesiredGuids -Current $installed -Removing $true)
    if ($removed.Count -ne 1 -or $removed[0] -ne $otherGuid) {
        throw 'Removal deletes a GUID not owned by KindleBridge.'
    }

    $relativePath = "SYSTEM\CurrentControlSet\Enum\$validId\Device Parameters"
    if ($relativePath -notlike 'SYSTEM\CurrentControlSet\Enum\USB\VID_1949&PID_9981&MI_01\*') {
        throw 'The registry path escaped the exact KindleBridge USB interface.'
    }

    Write-Output 'KindleBridge Windows onboarding validation passed.'
}

if ($Validate) {
    Test-ScriptConstants
    return
}

$IsWindowsHost = $env:OS -eq 'Windows_NT'
if (-not $IsWindowsHost) {
    throw 'Windows WinUSB onboarding can only inspect or modify devices on Windows.'
}

if (-not $DryRun) {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Run this script from an elevated PowerShell window, or use -DryRun to inspect the plan.'
    }
}

if ($InstanceId) {
    $devices = foreach ($id in $InstanceId) {
        if (-not (Test-KindleBridgeInstanceId $id)) {
            throw "Refusing non-KindleBridge interface instance: $id"
        }
        Get-PnpDevice -InstanceId $id -ErrorAction Stop
    }
} else {
    $devices = @(
        Get-PnpDevice -PresentOnly -ErrorAction Stop |
            Where-Object { Test-KindleBridgeInstanceId $_.InstanceId }
    )
}

if (@($devices).Count -eq 0) {
    throw 'No connected KindleBridge MI_01 interface found. Choose Switch to development mode in KUAL, connect USB, and retry.'
}

$changed = @()
foreach ($device in @($devices)) {
    $id = [string]$device.InstanceId
    if (-not (Test-KindleBridgeInstanceId $id)) {
        throw "Refusing non-KindleBridge interface instance: $id"
    }

    $service = [string](Get-DevicePropertyData $id 'DEVPKEY_Device_Service')
    $driverInf = [string](Get-DevicePropertyData $id 'DEVPKEY_Device_DriverInfPath')
    $hardwareIds = @(Get-DevicePropertyData $id 'DEVPKEY_Device_HardwareIds')
    if ($service -ine 'WinUSB') {
        throw "Refusing $id because its active service is '$service', not the inbox WinUSB service."
    }
    if ($driverInf -ine 'winusb.inf') {
        throw "Refusing $id because its driver INF is '$driverInf', not the inbox winusb.inf."
    }
    if (-not ($hardwareIds -icontains $BridgeHardwareId)) {
        throw "Refusing $id because its hardware IDs do not contain $BridgeHardwareId."
    }

    $registryPath = "HKLM:\SYSTEM\CurrentControlSet\Enum\$id\Device Parameters"
    if (-not (Test-Path -LiteralPath $registryPath -PathType Container)) {
        throw "The expected device registry key does not exist: $registryPath"
    }

    $current = @(
        Get-ItemPropertyValue `
            -LiteralPath $registryPath `
            -Name DeviceInterfaceGUIDs `
            -ErrorAction SilentlyContinue
    ) | Where-Object { $_ }

    $desired = @(Get-DesiredGuids -Current $current -Removing ([bool]$Remove))
    if ($Remove) {
        $action = "remove only $BridgeGuid from DeviceInterfaceGUIDs"
    } else {
        $action = "add $BridgeGuid to DeviceInterfaceGUIDs"
    }

    Write-Output "Interface: $id"
    Write-Output "Service:   $service"
    Write-Output "Plan:      $action"

    if (Test-StringSequenceEqual $current $desired) {
        Write-Output 'Result:    already configured'
        continue
    }
    if ($DryRun) {
        Write-Output 'Result:    dry run; no changes made'
        continue
    }
    if (-not $PSCmdlet.ShouldProcess($id, $action)) {
        continue
    }

    if ($desired.Count -eq 0) {
        Remove-ItemProperty `
            -LiteralPath $registryPath `
            -Name DeviceInterfaceGUIDs `
            -ErrorAction Stop
    } else {
        New-ItemProperty `
            -LiteralPath $registryPath `
            -Name DeviceInterfaceGUIDs `
            -PropertyType MultiString `
            -Value $desired `
            -Force | Out-Null
    }
    $changed += $id
    Write-Output 'Result:    registry updated'
}

if ($changed.Count -gt 0 -and -not $NoRestart) {
    $pnputil = Join-Path $env:SystemRoot 'System32\pnputil.exe'
    foreach ($id in $changed) {
        Write-Output "Restarting only the KindleBridge MI_01 interface: $id"
        & $pnputil /restart-device $id
        if ($LASTEXITCODE -ne 0) {
            throw "pnputil could not restart $id. Unplug and reconnect the Kindle before using the CLI."
        }
    }
} elseif ($changed.Count -gt 0) {
    Write-Output 'Reconnect the Kindle before using the CLI so Windows publishes the interface path.'
}
