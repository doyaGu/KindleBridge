param(
    [Parameter(Mandatory = $true)]
    [string]$DeviceHost,
    [Parameter(Mandatory = $true)]
    [string]$HostKey,
    [string]$User = 'root',
    [int]$Port = 22,
    [Parameter(Mandatory = $true)]
    [string]$Password,
    [ValidateRange(1, 1000)]
    [int]$Count = 100,
    [string]$Plink = 'plink.exe',
    [string]$Pscp = 'pscp.exe'
)

$ErrorActionPreference = 'Stop'
$gate = Join-Path $PSScriptRoot 'usb-mode-cycle-gate.sh'
if (-not (Test-Path -LiteralPath $gate -PathType Leaf)) {
    throw "Missing device gate: $gate"
}

$plinkPath = (Get-Command $Plink -ErrorAction Stop).Source
$pscpPath = (Get-Command $Pscp -ErrorAction Stop).Source
$remoteGate = "/var/tmp/kindlebridge-usb-mode-cycle-gate-$PID.sh"
$destination = "${User}@${DeviceHost}:$remoteGate"
$connection = @(
    '-batch',
    '-P', $Port,
    '-pw', $Password,
    '-hostkey', $HostKey
)

try {
    & $pscpPath @connection $gate $destination
    if ($LASTEXITCODE -ne 0) {
        throw "Could not copy the USB mode gate to $DeviceHost."
    }

    Write-Output "Running $Count unplugged MTP-to-Development cycles on $DeviceHost."
    Write-Output 'The gate stops on the first failure and preserves the observed USB mode.'
    & $plinkPath @connection "${User}@${DeviceHost}" "chmod 700 '$remoteGate' && '$remoteGate' '$Count'"
    if ($LASTEXITCODE -ne 0) {
        throw "USB mode cycle gate failed with exit code $LASTEXITCODE."
    }
} finally {
    & $plinkPath @connection "${User}@${DeviceHost}" "rm -f '$remoteGate'" 2>$null | Out-Null
}
