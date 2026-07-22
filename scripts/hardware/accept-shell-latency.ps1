param(
    [Parameter(Mandatory = $true)]
    [string]$Serial,
    [int]$Samples = 120,
    [double]$MaxP95Ms = 50,
    [string]$PipeName = "kindlebridge-$env:USERNAME"
)

$ErrorActionPreference = 'Stop'
$utf8 = New-Object System.Text.UTF8Encoding($false)
$ascii = [System.Text.Encoding]::ASCII

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

function Open-EchoShell {
    param([System.IO.Stream]$Stream, [int]$Id)

    Write-JsonFrame -Stream $Stream -Message ([ordered]@{
        jsonrpc = '2.0'
        id = $Id
        method = 'v1.shell.open'
        params = [ordered]@{
            serial = $Serial
            mode = 'raw'
            argv = @('/bin/sh', '-c', 'while IFS= read -r line; do printf "%s\n" "$line"; done')
            cwd = '/tmp/root'
            term = 'linux'
        }
    })

    while ($true) {
        $message = Read-JsonFrame -Stream $Stream
        if ($null -ne $message.id -and $message.id -eq $Id) {
            if ($message.error) {
                throw "v1.shell.open failed: $($message.error.message)"
            }
            return [string]$message.result.stream_id
        }
    }
}

if ($Samples -lt 2) {
    throw 'Samples must be at least 2.'
}

$pipe = New-Object System.IO.Pipes.NamedPipeClientStream(
    '.',
    $PipeName,
    [System.IO.Pipes.PipeDirection]::InOut,
    [System.IO.Pipes.PipeOptions]::None
)

try {
    $pipe.Connect(5000)
    $streamIds = @(
        (Open-EchoShell -Stream $pipe -Id 1),
        (Open-EchoShell -Stream $pipe -Id 2)
    )
    $buffers = @{
        $streamIds[0] = ''
        $streamIds[1] = ''
    }
    $latencies = New-Object System.Collections.Generic.List[double]

    for ($sample = 0; $sample -lt $Samples; $sample++) {
        $streamId = $streamIds[$sample % 2]
        $token = "kb-latency-$sample-$([Guid]::NewGuid().ToString('N'))"
        $stopwatch = [Diagnostics.Stopwatch]::StartNew()
        Write-JsonFrame -Stream $pipe -Message ([ordered]@{
            jsonrpc = '2.0'
            method = 'v1.stream.write'
            params = [ordered]@{
                stream_id = $streamId
                data = [Convert]::ToBase64String($utf8.GetBytes("$token`n"))
            }
        })

        while ($buffers[$streamId] -notmatch [regex]::Escape($token)) {
            $message = Read-JsonFrame -Stream $pipe
            if ($message.method -eq 'v1.stream.data' -and $buffers.ContainsKey([string]$message.params.stream_id)) {
                $id = [string]$message.params.stream_id
                $buffers[$id] += $utf8.GetString([Convert]::FromBase64String($message.params.data))
            } elseif ($message.method -eq 'v1.stream.closed' -and
                $buffers.ContainsKey([string]$message.params.stream_id)) {
                throw "Echo stream closed during sample $sample`: $($message.params.reason)"
            }
        }
        $stopwatch.Stop()
        $latencies.Add($stopwatch.Elapsed.TotalMilliseconds)
        $buffers[$streamId] = ''
    }

    foreach ($streamId in $streamIds) {
        Write-JsonFrame -Stream $pipe -Message ([ordered]@{
            jsonrpc = '2.0'
            method = 'v1.stream.close_input'
            params = [ordered]@{ stream_id = $streamId }
        })
    }

    $closing = @{}
    foreach ($streamId in $streamIds) {
        $closing[$streamId] = $true
    }
    while ($closing.Count -gt 0) {
        $message = Read-JsonFrame -Stream $pipe
        $streamId = [string]$message.params.stream_id
        if (-not $closing.ContainsKey($streamId)) {
            continue
        }
        if ($message.method -eq 'v1.stream.exit' -and [int]$message.params.exit_code -ne 0) {
            throw "Echo stream exited with status $($message.params.exit_code)."
        }
        if ($message.method -eq 'v1.stream.closed') {
            if ($message.params.reason) {
                throw "Echo stream closed with error: $($message.params.reason)"
            }
            $closing.Remove($streamId)
        }
    }

    $ordered = $latencies | Sort-Object
    $p50 = $ordered[[Math]::Ceiling($ordered.Count * 0.50) - 1]
    $p95 = $ordered[[Math]::Ceiling($ordered.Count * 0.95) - 1]
    $maximum = $ordered[-1]
    Write-Output ('Shell echo latency: samples={0} p50={1:N2} ms p95={2:N2} ms max={3:N2} ms' -f $Samples, $p50, $p95, $maximum)
    if ($p95 -gt $MaxP95Ms) {
        throw ('Shell echo P95 {0:N2} ms exceeded {1:N2} ms.' -f $p95, $MaxP95Ms)
    }
    Write-Output ('PASS: shell echo P95 <= {0:N2} ms.' -f $MaxP95Ms)
} finally {
    $pipe.Dispose()
}
