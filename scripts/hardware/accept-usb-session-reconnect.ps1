param(
    [Parameter(Mandatory = $true)]
    [string]$Serial,
    [ValidateRange(1, 100)]
    [int]$Count = 5,
    [ValidateRange(1000, 30000)]
    [int]$StopTimeoutMs = 5000,
    [string]$Cli,
    [string]$Server
)

$ErrorActionPreference = 'Stop'
if (-not $Cli) {
    $Cli = Join-Path $PSScriptRoot '..\..\target\release\kindlebridge.exe'
}
if (-not $Server) {
    $Server = Join-Path $PSScriptRoot '..\..\target\release\kindlebridge-server.exe'
}
$Cli = (Resolve-Path -LiteralPath $Cli).Path
$Server = (Resolve-Path -LiteralPath $Server).Path

function Assert-UsbInterfaces {
    $escapedSerial = [Regex]::Escape($Serial)
    $devices = @(
        Get-PnpDevice -PresentOnly -ErrorAction Stop |
            Where-Object { $_.InstanceId -match 'VID_1949&PID_9981' }
    )
    $expected = @(
        "^USB\\VID_1949&PID_9981\\$escapedSerial$",
        '^USB\\VID_1949&PID_9981&MI_00\\',
        '^USB\\VID_1949&PID_9981&MI_01\\'
    )
    foreach ($pattern in $expected) {
        $matching = @($devices | Where-Object { $_.InstanceId -match $pattern })
        if ($matching.Count -ne 1) {
            throw "Expected one present USB interface matching $pattern; found $($matching.Count)."
        }
        if ($matching[0].Status -ne 'OK') {
            throw "USB interface $($matching[0].InstanceId) is $($matching[0].Status), not OK."
        }
    }
}

function Invoke-DevicePing {
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $output = & $Cli --server $Server device ping $Serial
    $exitCode = $LASTEXITCODE
    $timer.Stop()
    if ($exitCode -ne 0 -or $output -ne 'pong') {
        throw "Device ping failed with exit code $exitCode and output '$output'."
    }
    return $timer.ElapsedMilliseconds
}

Assert-UsbInterfaces
$initialMs = Invoke-DevicePing
Write-Output "Initial session: $initialMs ms"

for ($cycle = 1; $cycle -le $Count; $cycle++) {
    $servers = @(Get-Process kindlebridge-server -ErrorAction SilentlyContinue)
    if ($servers.Count -ne 1) {
        throw "Expected one shared host server before cycle $cycle; found $($servers.Count)."
    }

    $stopTimer = [Diagnostics.Stopwatch]::StartNew()
    & $Cli --server $Server server stop | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Server stop failed during cycle $cycle."
    }
    while (@(Get-Process kindlebridge-server -ErrorAction SilentlyContinue).Count -ne 0 -and
        $stopTimer.ElapsedMilliseconds -lt $StopTimeoutMs) {
        Start-Sleep -Milliseconds 50
    }
    $stopTimer.Stop()
    if (@(Get-Process kindlebridge-server -ErrorAction SilentlyContinue).Count -ne 0) {
        throw "Shared host server did not exit within $StopTimeoutMs ms during cycle $cycle."
    }

    $reconnectMs = Invoke-DevicePing
    Assert-UsbInterfaces
    Write-Output (
        'Cycle {0}: stop={1} ms reconnect={2} ms USB=OK' -f
        $cycle, $stopTimer.ElapsedMilliseconds, $reconnectMs
    )
}

Write-Output "PASS: $Count graceful server-stop reconnect cycles completed without a USB replug."
