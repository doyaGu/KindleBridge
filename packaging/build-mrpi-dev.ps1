[CmdletBinding()]
param(
    [string]$KindleTool,
    [switch]$SkipDeviceBuild
)

$ErrorActionPreference = 'Stop'
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$WorkspaceRoot = Split-Path -Parent $RepositoryRoot
$TargetRoot = Join-Path $RepositoryRoot 'target\mrpi-dev'
$StageRoot = Join-Path $TargetRoot 'stage'
$DistRoot = Join-Path $RepositoryRoot 'dist'
$Version = '0.1.0-dev'

$GitBash = Join-Path $env:ProgramFiles 'Git\bin\bash.exe'
if (-not (Test-Path -LiteralPath $GitBash -PathType Leaf)) {
    throw "Git Bash not found: $GitBash"
}
Push-Location $RepositoryRoot
try {
    & $GitBash -lc 'sh scripts/test-shell.sh'
    if ($LASTEXITCODE -ne 0) { throw 'USB lifecycle shell tests failed' }
} finally {
    Pop-Location
}

if (-not $SkipDeviceBuild) {
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $RepositoryRoot 'scripts\build-kindlehf.ps1')
    if ($LASTEXITCODE -ne 0) { throw 'kindlehf build failed' }
}

$ResolvedTarget = [IO.Path]::GetFullPath($TargetRoot)
$ResolvedRepository = [IO.Path]::GetFullPath($RepositoryRoot)
if (-not $ResolvedTarget.StartsWith($ResolvedRepository, [StringComparison]::OrdinalIgnoreCase)) {
    throw "unsafe staging path: $ResolvedTarget"
}
if (Test-Path -LiteralPath $TargetRoot) {
    Remove-Item -LiteralPath $TargetRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $StageRoot,$DistRoot -Force | Out-Null

$PayloadSource = Join-Path $PSScriptRoot 'mrpi\payload'
Copy-Item -LiteralPath $PayloadSource -Destination (Join-Path $StageRoot 'payload') -Recurse
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'mrpi\install.sh') -Destination $StageRoot
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'mrpi\uninstall.sh') -Destination $StageRoot
$DeviceBinary = Join-Path $RepositoryRoot 'target\armv7-unknown-linux-gnueabihf\release\kindlebridged'
if (-not (Test-Path -LiteralPath $DeviceBinary -PathType Leaf)) {
    throw "kindlebridged not found: $DeviceBinary"
}
$LauncherBinary = Join-Path $RepositoryRoot 'target\armv7-unknown-linux-gnueabihf\release\kindlebridge-launcher'
if (-not (Test-Path -LiteralPath $LauncherBinary -PathType Leaf)) {
    throw "kindlebridge-launcher not found: $LauncherBinary"
}
Copy-Item -LiteralPath $LauncherBinary -Destination (Join-Path $StageRoot 'payload\kindlebridge\bin\kindlebridge-launcher')
foreach ($Slot in @('A', 'B')) {
    $SlotBin = Join-Path $StageRoot "payload\kindlebridge\runtime\slots\$Slot\bin"
    New-Item -ItemType Directory -Path $SlotBin -Force | Out-Null
    Copy-Item -LiteralPath $DeviceBinary -Destination (Join-Path $SlotBin 'kindlebridged')
}
$PayloadArchive = Join-Path $StageRoot 'payload.tar'
$Tar = Join-Path $env:SystemRoot 'System32\tar.exe'
& $Tar -cf $PayloadArchive -C (Join-Path $StageRoot 'payload') kindlebridge extensions
if ($LASTEXITCODE -ne 0) { throw 'payload archive creation failed' }

if (-not $KindleTool) {
    $KindleToolRoot = Join-Path $WorkspaceRoot 'KindleTool'
    $KindleToolManifest = Join-Path $KindleToolRoot 'Cargo.toml'
    if (-not (Test-Path -LiteralPath $KindleToolManifest -PathType Leaf)) {
        throw "Rust KindleTool checkout not found: $KindleToolRoot"
    }
    & cargo build --release --locked --package kindletool-cli --manifest-path $KindleToolManifest
    if ($LASTEXITCODE -ne 0) { throw 'Rust KindleTool build failed' }
    $KindleTool = Join-Path $KindleToolRoot 'target\release\kindletool.exe'
}
if (-not (Test-Path -LiteralPath $KindleTool -PathType Leaf)) {
    throw "Rust KindleTool executable not found: $KindleTool"
}
$KindleTool = [IO.Path]::GetFullPath($KindleTool)

$InstallOutput = Join-Path $DistRoot "update_kindlebridge_${Version}_install_khf.bin"
$UninstallOutput = Join-Path $DistRoot "update_kindlebridge_${Version}_uninstall_khf.bin"
Remove-Item -LiteralPath $InstallOutput,$UninstallOutput -Force -ErrorAction SilentlyContinue
$KindleToolInstallOutput = $InstallOutput.Replace('\', '/')
$KindleToolUninstallOutput = $UninstallOutput.Replace('\', '/')
$env:KT_WITH_UNKNOWN_DEVCODES = '1'
$PackageTmp = Join-Path $TargetRoot 'package-tmp'
New-Item -ItemType Directory -Path $PackageTmp -Force | Out-Null
$env:TEMP = $PackageTmp
$env:TMP = $PackageTmp
Push-Location $StageRoot
try {
    & $KindleTool create ota2 '-xPackageName=kindlebridge' "-xPackageVersion=$Version" '-xPackageAuthor=KindleBridge contributors' '-xPackageMaintainer=KindleBridge contributors' -X -d basic5 install.sh payload.tar $KindleToolInstallOutput
    if ($LASTEXITCODE -ne 0) { throw 'install package creation failed' }
    & $KindleTool create ota2 '-xPackageName=kindlebridge' "-xPackageVersion=$Version" '-xPackageAuthor=KindleBridge contributors' '-xPackageMaintainer=KindleBridge contributors' -X -d basic5 uninstall.sh $KindleToolUninstallOutput
    if ($LASTEXITCODE -ne 0) { throw 'uninstall package creation failed' }
} finally {
    Pop-Location
}

Get-FileHash -Algorithm SHA256 $InstallOutput,$UninstallOutput |
    Select-Object Path,Hash
