param(
    [Parameter(Mandatory = $true)]
    [string]$Serial,
    [long]$Bytes = 128MB,
    [int]$MinimumSamples = 100,
    [double]$MaxP95Ms = 50,
    [string]$PipeName = "kindlebridge-$env:USERNAME",
    [string]$Cli
)

$ErrorActionPreference = 'Stop'
$utf8 = New-Object System.Text.UTF8Encoding($false)
$ascii = [System.Text.Encoding]::ASCII
$pipe = $null
$jobs = New-Object System.Collections.Generic.List[object]
$remoteMayExist = $false
$token = [Guid]::NewGuid().ToString('N')
$source = Join-Path ([IO.Path]::GetTempPath()) "kindlebridge-control-$token.bin"
$readback = Join-Path ([IO.Path]::GetTempPath()) "kindlebridge-control-$token.readback.bin"
$remote = "hardware-gates/control-$token.bin"

if (-not $Cli) {
    $Cli = Join-Path $PSScriptRoot '..\..\target\release\kindlebridge.exe'
}
$Cli = (Resolve-Path -LiteralPath $Cli).Path

if ($Bytes -lt 32MB -or $Bytes -gt 1GB) {
    throw 'Bytes must be between 32 MiB and 1 GiB.'
}
if ($MinimumSamples -lt 20) {
    throw 'MinimumSamples must be at least 20.'
}

function Write-JsonFrame {
    param([System.IO.Stream]$Stream, [object]$Message)

    $json = $Message | ConvertTo-Json -Compress -Depth 10
    $body = $utf8.GetBytes($json)
    $header = $ascii.GetBytes("Content-Length: $($body.Length)`r`n`r`n")
    $Stream.Write($header, 0, $header.Length)
    $Stream.Write($body, 0, $body.Length)
    $Stream.Flush()
}

function Read-Exact {
    param([System.IO.Stream]$Stream, [int]$Length)

    $buffer = New-Object byte[] $Length
    $offset = 0
    while ($offset -lt $Length) {
        $count = $Stream.Read($buffer, $offset, $Length - $offset)
        if ($count -eq 0) {
            throw 'Local server closed the connection while reading a JSON-RPC frame.'
        }
        $offset += $count
    }
    return $buffer
}

function Read-JsonFrame {
    param([System.IO.Stream]$Stream)

    $headerBytes = New-Object System.Collections.Generic.List[byte]
    while ($true) {
        $value = $Stream.ReadByte()
        if ($value -lt 0) {
            throw 'Local server closed the connection before a JSON-RPC header.'
        }
        $headerBytes.Add([byte]$value)
        $count = $headerBytes.Count
        if ($count -ge 4 -and
            $headerBytes[$count - 4] -eq 13 -and
            $headerBytes[$count - 3] -eq 10 -and
            $headerBytes[$count - 2] -eq 13 -and
            $headerBytes[$count - 1] -eq 10) {
            break
        }
        if ($count -gt 8192) {
            throw 'JSON-RPC header exceeded 8 KiB.'
        }
    }

    $header = $ascii.GetString($headerBytes.ToArray())
    if ($header -notmatch '(?im)^Content-Length:\s*(\d+)\s*$') {
        throw 'JSON-RPC response did not contain Content-Length.'
    }
    $body = Read-Exact -Stream $Stream -Length ([int]$Matches[1])
    return $utf8.GetString($body) | ConvertFrom-Json
}

function Invoke-DevicePing {
    param([System.IO.Stream]$Stream, [int]$Id)

    Write-JsonFrame -Stream $Stream -Message ([ordered]@{
        jsonrpc = '2.0'
        id = $Id
        method = 'v1.device.ping'
        params = [ordered]@{ serial = $Serial }
    })
    while ($true) {
        $message = Read-JsonFrame -Stream $Stream
        if ($null -eq $message.id -or [int]$message.id -ne $Id) {
            continue
        }
        if ($message.error) {
            throw "v1.device.ping failed: $($message.error.message)"
        }
        if (-not $message.result.ok) {
            throw 'v1.device.ping returned an invalid result.'
        }
        return
    }
}

function Get-LatencySummary {
    param([System.Collections.Generic.List[double]]$Latencies)

    $ordered = @($Latencies | Sort-Object)
    return [ordered]@{
        Samples = $ordered.Count
        P50 = $ordered[[Math]::Ceiling($ordered.Count * 0.50) - 1]
        P95 = $ordered[[Math]::Ceiling($ordered.Count * 0.95) - 1]
        Maximum = $ordered[-1]
    }
}

function Measure-Baseline {
    param([System.IO.Stream]$Stream, [ref]$NextId)

    $latencies = New-Object System.Collections.Generic.List[double]
    for ($sample = 0; $sample -lt $MinimumSamples; $sample++) {
        $timer = [Diagnostics.Stopwatch]::StartNew()
        Invoke-DevicePing -Stream $Stream -Id $NextId.Value
        $timer.Stop()
        $NextId.Value++
        $latencies.Add($timer.Elapsed.TotalMilliseconds)
    }
    return Get-LatencySummary -Latencies $latencies
}

function Measure-Transfer {
    param(
        [System.IO.Stream]$Stream,
        [ValidateSet('push', 'pull')]
        [string]$Direction,
        [ref]$NextId
    )

    $localPath = if ($Direction -eq 'push') { $source } else { $readback }
    $job = Start-Job -ScriptBlock {
        param($Executable, $Mode, $DeviceSerial, $LocalPath, $RemotePath)
        if ($Mode -eq 'push') {
            & $Executable sync push $DeviceSerial $LocalPath $RemotePath
        } else {
            & $Executable sync pull $DeviceSerial $RemotePath $LocalPath
        }
        if ($LASTEXITCODE -ne 0) {
            throw "sync $Mode failed with exit code $LASTEXITCODE"
        }
    } -ArgumentList $Cli, $Direction, $Serial, $localPath, $remote
    if ($Direction -eq 'push') {
        $script:remoteMayExist = $true
    }
    $jobs.Add($job)
    $transferTimer = [Diagnostics.Stopwatch]::StartNew()

    # Exclude local process startup and the push source-hash pass from the
    # latency population. A valid 128 MiB hardware run remains active well
    # beyond this warm-up.
    Start-Sleep -Milliseconds 750
    if ($job.State -ne 'Running') {
        Receive-Job -Job $job -Wait | ForEach-Object { Write-Host $_ }
        throw "sync $Direction completed before the sustained-load sample window"
    }

    $latencies = New-Object System.Collections.Generic.List[double]
    while ($job.State -eq 'Running') {
        $timer = [Diagnostics.Stopwatch]::StartNew()
        Invoke-DevicePing -Stream $Stream -Id $NextId.Value
        $timer.Stop()
        $NextId.Value++
        $latencies.Add($timer.Elapsed.TotalMilliseconds)
        Start-Sleep -Milliseconds 10
    }
    $transferTimer.Stop()
    $jobOutput = @(Receive-Job -Job $job -Wait -ErrorAction Stop)
    if ($job.State -ne 'Completed') {
        throw "sync $Direction job ended as $($job.State): $($jobOutput -join '; ')"
    }
    $jobOutput | ForEach-Object { Write-Host $_ }
    if ($latencies.Count -lt $MinimumSamples) {
        throw "sync $Direction produced only $($latencies.Count) overlapping ping samples; expected at least $MinimumSamples"
    }
    $summary = Get-LatencySummary -Latencies $latencies
    $summary['ElapsedSeconds'] = $transferTimer.Elapsed.TotalSeconds
    $summary['MiBPerSecond'] = ($Bytes / 1MB) / $transferTimer.Elapsed.TotalSeconds
    return $summary
}

function Write-Summary {
    param([string]$Name, $Summary)

    $throughput = if ($Summary.Contains('MiBPerSecond')) {
        ' throughput={0:N2} MiB/s' -f $Summary.MiBPerSecond
    } else {
        ''
    }
    Write-Output ('{0}: samples={1} p50={2:N2} ms p95={3:N2} ms max={4:N2} ms{5}' -f
        $Name, $Summary.Samples, $Summary.P50, $Summary.P95, $Summary.Maximum, $throughput)
}

try {
    $file = [IO.File]::Open($source, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    try {
        $file.SetLength($Bytes)
    } finally {
        $file.Dispose()
    }

    & $Cli server ping | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw 'Could not start or reach the shared KindleBridge host server.'
    }

    $pipe = New-Object System.IO.Pipes.NamedPipeClientStream(
        '.',
        $PipeName,
        [System.IO.Pipes.PipeDirection]::InOut,
        [System.IO.Pipes.PipeOptions]::None
    )
    $pipe.Connect(5000)
    $nextId = 1

    $baseline = Measure-Baseline -Stream $pipe -NextId ([ref]$nextId)
    Write-Summary -Name 'Baseline control RTT' -Summary $baseline

    $push = Measure-Transfer -Stream $pipe -Direction push -NextId ([ref]$nextId)
    Write-Summary -Name 'Push-load control RTT' -Summary $push

    $pull = Measure-Transfer -Stream $pipe -Direction pull -NextId ([ref]$nextId)
    Write-Summary -Name 'Pull-load control RTT' -Summary $pull

    $sourceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash
    $readbackHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $readback).Hash
    if ($sourceHash -ne $readbackHash) {
        throw "sync round-trip hash mismatch: source=$sourceHash readback=$readbackHash"
    }
    foreach ($result in @($push, $pull)) {
        if ($result.P95 -gt $MaxP95Ms) {
            throw ('control-frame P95 {0:N2} ms exceeded {1:N2} ms' -f $result.P95, $MaxP95Ms)
        }
    }
    Write-Output ('PASS: push/pull control-frame P95 <= {0:N2} ms; sync SHA-256 {1}' -f
        $MaxP95Ms, $sourceHash.ToLowerInvariant())
} finally {
    foreach ($job in $jobs) {
        if ($job.State -eq 'Running') {
            Stop-Job -Job $job
        }
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
    }
    if ($pipe) {
        $pipe.Dispose()
    }
    Remove-Item -LiteralPath $source, $readback -Force -ErrorAction SilentlyContinue
    if ($remoteMayExist) {
        & $Cli exec $Serial --timeout-ms 5000 -- rm -f "/mnt/us/kindlebridge-data/$remote" 2>$null |
            Out-Null
    }
}
