param(
    [Parameter(Mandatory = $true)]
    [string]$Serial,
    [string]$PipeName = "kindlebridge-$env:USERNAME"
)

$ErrorActionPreference = 'Stop'
$utf8 = New-Object System.Text.UTF8Encoding($false)
$ascii = [System.Text.Encoding]::ASCII

function Write-JsonFrame {
    param(
        [System.IO.Stream]$Stream,
        [object]$Message
    )

    $json = $Message | ConvertTo-Json -Compress -Depth 10
    $body = $utf8.GetBytes($json)
    $header = $ascii.GetBytes("Content-Length: $($body.Length)`r`n`r`n")
    $Stream.Write($header, 0, $header.Length)
    $Stream.Write($body, 0, $body.Length)
    $Stream.Flush()
}

function Read-Exact {
    param(
        [System.IO.Stream]$Stream,
        [int]$Length
    )

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

$pipe = New-Object System.IO.Pipes.NamedPipeClientStream(
    '.',
    $PipeName,
    [System.IO.Pipes.PipeDirection]::InOut,
    [System.IO.Pipes.PipeOptions]::None
)

try {
    $pipe.Connect(5000)

    Write-JsonFrame -Stream $pipe -Message ([ordered]@{
        jsonrpc = '2.0'
        id = 1
        method = 'v1.shell.open'
        params = [ordered]@{
            serial = $Serial
            mode = 'pty'
            argv = @('/bin/sh', '-lc', 'stty size; read line; stty size')
            terminal_size = [ordered]@{
                rows = 24
                columns = 80
                pixel_width = 0
                pixel_height = 0
            }
            cwd = '/tmp/root'
            term = 'linux'
        }
    })

    $streamId = $null
    $pending = New-Object System.Collections.Generic.List[object]
    while (-not $streamId) {
        $message = Read-JsonFrame -Stream $pipe
        if ($null -ne $message.id -and $message.id -eq 1) {
            if ($message.error) {
                throw "v1.shell.open failed: $($message.error.message)"
            }
            $streamId = [string]$message.result.stream_id
        } else {
            $pending.Add($message)
        }
    }

    Write-JsonFrame -Stream $pipe -Message ([ordered]@{
        jsonrpc = '2.0'
        method = 'v1.stream.resize'
        params = [ordered]@{
            stream_id = $streamId
            rows = 40
            columns = 100
            pixel_width = 0
            pixel_height = 0
        }
    })
    Write-JsonFrame -Stream $pipe -Message ([ordered]@{
        jsonrpc = '2.0'
        method = 'v1.stream.write'
        params = [ordered]@{
            stream_id = $streamId
            data = [Convert]::ToBase64String($utf8.GetBytes("`n"))
        }
    })

    $output = New-Object System.Text.StringBuilder
    $exitCode = $null
    $closed = $false
    foreach ($message in $pending) {
        if ($message.method -eq 'v1.stream.data' -and $message.params.stream_id -eq $streamId) {
            [void]$output.Append($utf8.GetString([Convert]::FromBase64String($message.params.data)))
        }
    }
    while (-not $closed) {
        $message = Read-JsonFrame -Stream $pipe
        if (-not $message.params -or $message.params.stream_id -ne $streamId) {
            continue
        }
        switch ($message.method) {
            'v1.stream.data' {
                [void]$output.Append($utf8.GetString([Convert]::FromBase64String($message.params.data)))
            }
            'v1.stream.exit' {
                $exitCode = [int]$message.params.exit_code
            }
            'v1.stream.closed' {
                if ($message.params.reason) {
                    throw "PTY stream closed with error: $($message.params.reason)"
                }
                $closed = $true
            }
        }
    }

    $text = $output.ToString() -replace "`r", ''
    if ($exitCode -ne 0) {
        throw "Remote resize probe exited with status $exitCode. Output: $text"
    }
    if ($text -notmatch '(?m)^24 80$' -or $text -notmatch '(?m)^40 100$') {
        throw "PTY did not report both requested sizes. Output: $text"
    }

    Write-Output "PASS: PTY resized live from 24x80 to 40x100 (stream $streamId)."
} finally {
    $pipe.Dispose()
}
