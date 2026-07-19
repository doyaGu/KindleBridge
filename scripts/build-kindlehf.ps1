[CmdletBinding()]
param(
    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Release',
    [string]$ToolchainRoot,
    [version]$MaximumGlibcVersion = '2.27'
)

$ErrorActionPreference = 'Stop'
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$WorkspaceRoot = Split-Path -Parent $RepositoryRoot

if (-not $ToolchainRoot) {
    if ($env:KINDLEHF_TOOLCHAIN_ROOT) {
        $ToolchainRoot = $env:KINDLEHF_TOOLCHAIN_ROOT
    } else {
        $ToolchainRoot = Join-Path $WorkspaceRoot 'toolchains\arm-kindlehf-linux-gnueabihf'
    }
}

$ToolchainRoot = (Resolve-Path -LiteralPath $ToolchainRoot).Path
$Linker = Join-Path $ToolchainRoot 'bin\arm-kindlehf-linux-gnueabihf-gcc.exe'
$ReadElf = Join-Path $ToolchainRoot 'bin\arm-kindlehf-linux-gnueabihf-readelf.exe'

if (-not (Test-Path -LiteralPath $Linker -PathType Leaf)) {
    throw "kindlehf linker not found: $Linker"
}

$env:CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_LINKER = $Linker
$CargoArguments = @(
    'build',
    '--target', 'armv7-unknown-linux-gnueabihf',
    '--package', 'kindlebridged',
    '--package', 'kindlebridge-broker'
    '--package', 'kindlebridge-device-bench'
    '--package', 'kindlebridge-functionfs'
    '--package', 'kindlebridge-launcher'
)
if ($Configuration -eq 'Release') {
    $CargoArguments += '--release'
}

Push-Location $RepositoryRoot
try {
    & cargo @CargoArguments
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with exit code $LASTEXITCODE"
    }

    if (Test-Path -LiteralPath $ReadElf -PathType Leaf) {
        $ProfileDirectory = if ($Configuration -eq 'Release') { 'release' } else { 'debug' }
        foreach ($Binary in @(
            'kindlebridged',
            'kindlebridge-broker',
            'kindlebridge-device-bench',
            'kindlebridge-ffs-probe',
            'kindlebridge-launcher'
        )) {
            $BinaryPath = Join-Path $RepositoryRoot "target\armv7-unknown-linux-gnueabihf\$ProfileDirectory\$Binary"
            $Header = & $ReadElf '--file-header' $BinaryPath
            if ($LASTEXITCODE -ne 0) {
                throw "ELF header validation failed for $BinaryPath"
            }
            $HeaderText = $Header -join "`n"
            if ($HeaderText -notmatch 'Class:\s+ELF32' -or
                $HeaderText -notmatch 'Machine:\s+ARM' -or
                $HeaderText -notmatch 'hard-float ABI') {
                throw "$BinaryPath is not an ELF32 ARM hard-float binary"
            }

            $VersionInfo = & $ReadElf '--version-info' $BinaryPath
            if ($LASTEXITCODE -ne 0) {
                throw "readelf validation failed for $BinaryPath"
            }
            $VersionText = $VersionInfo -join "`n"
            $RequiredVersions = [regex]::Matches($VersionText, 'GLIBC_(\d+\.\d+)') |
                ForEach-Object { [version]$_.Groups[1].Value }
            $HighestRequired = $RequiredVersions |
                Sort-Object -Descending |
                Select-Object -First 1
            if ($HighestRequired -and $HighestRequired -gt $MaximumGlibcVersion) {
                throw "$BinaryPath requires GLIBC_$HighestRequired, above allowed GLIBC_$MaximumGlibcVersion"
            }
            Write-Output "${Binary}: ELF32 ARM hard-float, maximum required GLIBC_$HighestRequired"
        }
    }
} finally {
    Pop-Location
}
