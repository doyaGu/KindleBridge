[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$ManifestPath = Join-Path $RepositoryRoot 'Cargo.toml'
$MetadataJson = & cargo metadata --format-version 1 --no-deps --manifest-path $ManifestPath
if ($LASTEXITCODE -ne 0) {
    throw 'cargo metadata failed'
}
$Metadata = $MetadataJson | ConvertFrom-Json
$WorkspaceMembers = @($Metadata.workspace_members)
$WorkspacePackages = @(
    $Metadata.packages | Where-Object { $WorkspaceMembers -contains $_.id }
)
if ($WorkspacePackages.Count -eq 0) {
    throw 'cargo metadata returned no workspace packages'
}

$Versions = @($WorkspacePackages | ForEach-Object { [string]$_.version } | Sort-Object -Unique)
if ($Versions.Count -ne 1) {
    $Details = $WorkspacePackages |
        Sort-Object name |
        ForEach-Object { "$($_.name)=$($_.version)" }
    throw "workspace package versions diverged: $($Details -join ', ')"
}
$Version = $Versions[0]
if ($Version -notmatch '^[0-9A-Za-z][0-9A-Za-z.-]{0,63}$') {
    throw "workspace version is unsafe for MRPI metadata: $Version"
}

$KualConfigPath = Join-Path $RepositoryRoot 'packaging\mrpi\payload\extensions\kindlebridge\config.xml'
$KualConfig = [IO.File]::ReadAllText($KualConfigPath)
$KualVersionToken = '@KINDLEBRIDGE_VERSION@'
$FirstKualVersionToken = $KualConfig.IndexOf($KualVersionToken, [StringComparison]::Ordinal)
$LastKualVersionToken = $KualConfig.LastIndexOf($KualVersionToken, [StringComparison]::Ordinal)
if ($FirstKualVersionToken -lt 0 -or $FirstKualVersionToken -ne $LastKualVersionToken) {
    throw "KUAL config must contain exactly one $KualVersionToken token"
}
if ($KualConfig -match '<version>\s*[0-9]') {
    throw 'KUAL config contains a second hard-coded product version'
}

$PackageScriptPath = Join-Path $RepositoryRoot 'packaging\build-mrpi-dev.ps1'
$PackageScript = [IO.File]::ReadAllText($PackageScriptPath)
if ($PackageScript -notmatch 'cargo metadata') {
    throw 'MRPI builder no longer derives its version from Cargo metadata'
}
if ($PackageScript -match '\[string\]\$Version\s*=') {
    throw 'MRPI builder reintroduced an independent default version'
}

Write-Output "Release contract passed: workspace, MRPI, payload, and KUAL use $Version."
