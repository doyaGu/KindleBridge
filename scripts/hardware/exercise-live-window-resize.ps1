param(
    [Parameter(Mandatory = $true)]
    [string]$Serial,
    [string]$Cli
)

$ErrorActionPreference = 'Stop'
if (-not $Cli) {
    $Cli = Join-Path (Split-Path -Parent $PSCommandPath) '..\..\target\release\kindlebridge.exe'
}
$Cli = (Resolve-Path $Cli).Path

Write-Host 'KindleBridge live window-resize gate'
Write-Host 'Drag this Windows Terminal window to a different size.'
Write-Host 'Then press Enter once. The second rows/columns value must differ.'
Write-Host ''

$remoteCommand = 'stty size; read line; stty size'
& $Cli shell $Serial -tt -c $remoteCommand
exit $LASTEXITCODE
