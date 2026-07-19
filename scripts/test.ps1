[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$RepositoryRoot = Split-Path -Parent $PSScriptRoot

Push-Location $RepositoryRoot
try {
    & (Join-Path $PSScriptRoot 'install-windows-winusb.ps1') -Validate

    $GitBash = Join-Path $env:ProgramFiles 'Git\bin\bash.exe'
    if (-not (Test-Path -LiteralPath $GitBash -PathType Leaf)) {
        throw "Git Bash not found: $GitBash"
    }
    & $GitBash -lc 'sh scripts/test-shell.sh'
    if ($LASTEXITCODE -ne 0) { throw 'USB lifecycle shell tests failed' }

    & cargo fmt --all --check
    if ($LASTEXITCODE -ne 0) { throw 'cargo fmt failed' }

    & cargo test --workspace
    if ($LASTEXITCODE -ne 0) { throw 'cargo test failed' }

    & cargo clippy --workspace --all-targets -- -D warnings
    if ($LASTEXITCODE -ne 0) { throw 'cargo clippy failed' }
} finally {
    Pop-Location
}
