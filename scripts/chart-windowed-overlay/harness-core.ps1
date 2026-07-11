Set-StrictMode -Version Latest

function Get-ArrayValue([object]$Value) {
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function Test-SafeRunId([string]$RunId) {
    return -not [string]::IsNullOrWhiteSpace($RunId) -and $RunId -match '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'
}

function Resolve-VerifiedDescendantPath([string]$Root, [string]$Child) {
    if ([string]::IsNullOrWhiteSpace($Root) -or [string]::IsNullOrWhiteSpace($Child)) { throw "Root and child paths are required." }
    $canonicalRoot = [IO.Path]::GetFullPath($Root).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    $canonicalChild = [IO.Path]::GetFullPath((Join-Path $canonicalRoot $Child))
    $prefix = $canonicalRoot + [IO.Path]::DirectorySeparatorChar
    if (-not $canonicalChild.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) { throw "Path escapes configured output root: $Child" }
    return $canonicalChild
}

function Get-EvidenceRunPaths([string]$OutputRoot, [string]$RunId) {
    if (-not (Test-SafeRunId $RunId)) { throw "RunId must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$" }
    $root = [IO.Path]::GetFullPath($OutputRoot)
    return [pscustomobject]@{
        root = $root
        final = Resolve-VerifiedDescendantPath $root $RunId
        stage = Resolve-VerifiedDescendantPath $root ".$RunId.staging"
        lock = Resolve-VerifiedDescendantPath $root ".$RunId.lock"
    }
}

function Enter-EvidenceRunLock([string]$OutputRoot, [string]$RunId) {
    $paths = Get-EvidenceRunPaths $OutputRoot $RunId
    New-Item -ItemType Directory -Force -Path $paths.root | Out-Null
    try {
        $stream = [IO.File]::Open($paths.lock, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
        $bytes = [Text.Encoding]::UTF8.GetBytes("pid=$PID runId=$RunId acquired=$((Get-Date).ToUniversalTime().ToString('o'))")
        $stream.SetLength(0); $stream.Write($bytes, 0, $bytes.Length); $stream.Flush()
        return [pscustomobject]@{ runId = $RunId; paths = $paths; stream = $stream }
    }
    catch [IO.IOException] { throw "RunId is already locked by another harness process: $RunId" }
}

function Exit-EvidenceRunLock([object]$Lock) {
    if ($null -ne $Lock -and $null -ne $Lock.stream) { $Lock.stream.Dispose() }
}

function Try-ParseEpochSeconds([object]$Value, [ref]$Result) {
    $parsed = 0L
    if ($null -eq $Value) { return $false }
    $text = [Convert]::ToString($Value, [Globalization.CultureInfo]::InvariantCulture)
    if (-not [long]::TryParse($text, [Globalization.NumberStyles]::Integer, [Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) { return $false }
    if ($parsed -lt -62135596800L -or $parsed -gt 253402300799L) { return $false }
    $Result.Value = $parsed
    return $true
}

function Try-FormatApiTime([object]$EpochSeconds, [ref]$Result) {
    $parsed = 0L
    if (-not (Try-ParseEpochSeconds $EpochSeconds ([ref]$parsed))) { return $false }
    try { $Result.Value = [DateTimeOffset]::FromUnixTimeSeconds($parsed).ToString("o"); return $true }
    catch { return $false }
}

function ConvertTo-MarkdownCell([object]$Value) {
    $text = if ($null -eq $Value) { "" } else { [string]$Value }
    $text = $text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;")
    $text = $text.Replace("!", "&#33;").Replace("[", "&#91;").Replace("]", "&#93;")
    $text = $text.Replace("(", "&#40;").Replace(")", "&#41;").Replace(":", "&#58;").Replace("/", "&#47;")
    $text = $text.Replace("|", "&#124;").Replace([string][char]96, "&#96;")
    $text = $text -replace "`r`n|`n|`r", "&#10;"
    return "<code>$text</code>"
}

function Invoke-TypeScriptBundleScan {
    param(
        [System.Collections.IDictionary]$Sources,
        [string]$NodePath,
        [string]$TypeScriptPath,
        [string]$ScannerPath,
        [ValidateRange(1, 60)][int]$TimeoutSeconds = 10,
        [switch]$RequireProductionScopes
    )
    $unavailable = [System.Collections.Generic.List[object]]::new()
    if (-not $NodePath -or -not (Test-Path -LiteralPath $NodePath)) { $unavailable.Add([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "Node executable unavailable" }) }
    if (-not $TypeScriptPath -or -not (Test-Path -LiteralPath $TypeScriptPath)) { $unavailable.Add([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "TypeScript compiler unavailable" }) }
    if (-not $ScannerPath -or -not (Test-Path -LiteralPath $ScannerPath)) { $unavailable.Add([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "AST scanner script unavailable" }) }
    if ($unavailable.Count) { return [pscustomobject]@{ findings = @(); uncertainties = @($unavailable); scanner = "typescript-compiler-ast-unavailable" } }

    $sourceBytes = 0L
    foreach ($entry in $Sources.GetEnumerator()) { $sourceBytes += [Text.Encoding]::UTF8.GetByteCount([string]$entry.Value) }
    if ($sourceBytes -gt 4194304) { return [pscustomobject]@{ findings = @(); uncertainties = @([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "TypeScript AST scanner input exceeded 4 MiB" }); scanner = "typescript-compiler-ast-input-oversize" } }

    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $NodePath
    $start.Arguments = ('"' + $ScannerPath.Replace('"', '\"') + '" "' + $TypeScriptPath.Replace('"', '\"') + '"')
    $start.UseShellExecute = $false; $start.CreateNoWindow = $true
    $start.RedirectStandardInput = $true; $start.RedirectStandardOutput = $true; $start.RedirectStandardError = $true
    $process = [Diagnostics.Process]::new(); $process.StartInfo = $start
    try {
        [void]$process.Start()
        $stdoutTask = $process.StandardOutput.ReadToEndAsync(); $stderrTask = $process.StandardError.ReadToEndAsync()
        if ($PSVersionTable.PSVersion.Major -le 5) {
            Add-Type -AssemblyName System.Web.Extensions
            $sourceDictionary = [Collections.Generic.Dictionary[string,string]]::new()
            foreach ($entry in $Sources.GetEnumerator()) { $sourceDictionary[[string]$entry.Key] = [string]$entry.Value }
            $payloadDictionary = [Collections.Generic.Dictionary[string,object]]::new()
            $payloadDictionary["sources"] = $sourceDictionary; $payloadDictionary["requireProductionScopes"] = [bool]$RequireProductionScopes
            $serializer = [Web.Script.Serialization.JavaScriptSerializer]::new(); $serializer.MaxJsonLength = 8388608
            $payload = $serializer.Serialize($payloadDictionary)
        }
        else { $payload = [ordered]@{ sources = $Sources; requireProductionScopes = [bool]$RequireProductionScopes } | ConvertTo-Json -Depth 8 -Compress }
        $deadline = [Diagnostics.Stopwatch]::StartNew()
        $inputTask = $process.StandardInput.WriteAsync($payload)
        if (-not $inputTask.Wait($TimeoutSeconds * 1000)) {
            $process.Kill(); $process.WaitForExit()
            return [pscustomobject]@{ findings = @(); uncertainties = @([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "TypeScript AST scanner input timeout after ${TimeoutSeconds}s" }); scanner = "typescript-compiler-ast-timeout" }
        }
        $process.StandardInput.Close()
        $remainingMs = [math]::Max(1, ($TimeoutSeconds * 1000) - [int]$deadline.ElapsedMilliseconds)
        if (-not $process.WaitForExit($remainingMs)) {
            $process.Kill(); $process.WaitForExit()
            return [pscustomobject]@{ findings = @(); uncertainties = @([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "TypeScript AST scanner timeout after ${TimeoutSeconds}s" }); scanner = "typescript-compiler-ast-timeout" }
        }
        $stdout = $stdoutTask.GetAwaiter().GetResult(); $stderr = $stderrTask.GetAwaiter().GetResult()
        if ($process.ExitCode -ne 0) {
            $detail = if ($stderr.Length -gt 4096) { $stderr.Substring(0, 4096) } else { $stderr }
            return [pscustomobject]@{ findings = @(); uncertainties = @([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "TypeScript AST scanner exit $($process.ExitCode): $detail" }); scanner = "typescript-compiler-ast-error" }
        }
        if ($stdout.Length -gt 4194304) { return [pscustomobject]@{ findings = @(); uncertainties = @([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "TypeScript AST scanner output exceeded 4 MiB" }); scanner = "typescript-compiler-ast-oversize" } }
        try {
            $result = $stdout | ConvertFrom-Json
            return [pscustomobject]@{ findings = @(Get-ArrayValue $result.findings); uncertainties = @(Get-ArrayValue $result.uncertainties); scanner = [string]$result.scanner }
        }
        catch { return [pscustomobject]@{ findings = @(); uncertainties = @([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "Invalid AST scanner JSON: $($_.Exception.Message)" }); scanner = "typescript-compiler-ast-invalid-json" } }
    }
    catch { return [pscustomobject]@{ findings = @(); uncertainties = @([pscustomobject]@{ path = "<scanner>"; scope = "runtime"; reason = "AST scanner process failure: $($_.Exception.Message)" }); scanner = "typescript-compiler-ast-process-failure" } }
    finally { $process.Dispose() }
}

function New-ProbeMatrix([string[]]$Symbols, [string[]]$Timeframes, [string[]]$WindowNames) {
    foreach ($symbol in @($Symbols | Where-Object { $_ } | Select-Object -Unique)) {
        foreach ($timeframe in @($Timeframes | Where-Object { $_ } | Select-Object -Unique)) {
            foreach ($window in @($WindowNames | Where-Object { $_ } | Select-Object -Unique)) {
                [pscustomobject]@{ symbol = $symbol.ToUpperInvariant(); timeframe = $timeframe; window = $window; key = "$($symbol.ToUpperInvariant())|$timeframe|$window" }
            }
        }
    }
}

function Test-ProbeResponse {
    param([object]$Probe, [ValidateSet("bars", "overlay")][string]$Kind, [long]$From = 0, [long]$To = [long]::MaxValue, [string[]]$ExpectedLevels = @())
    $failures = [System.Collections.Generic.List[string]]::new()
    if ($null -eq $Probe -or ($null -ne $Probe.PSObject.Properties["skipped"] -and $Probe.skipped)) { return [pscustomobject]@{ valid = $false; recordLatency = $false; status = "PENDING"; failures = @("probe not executed") } }
    if ($null -ne $Probe.PSObject.Properties["failureKind"] -and $Probe.failureKind) { return [pscustomobject]@{ valid = $false; recordLatency = $false; status = "FAIL"; failures = @("$($Probe.failureKind): $($Probe.text)") } }
    if ($Probe.statusCode -eq 404) { return [pscustomobject]@{ valid = $false; recordLatency = $false; status = "SKIP"; failures = @("sample unavailable") } }
    if ($Probe.statusCode -ne 200) { return [pscustomobject]@{ valid = $false; recordLatency = $false; status = "FAIL"; failures = @("HTTP $($Probe.statusCode)") } }
    if ($null -eq $Probe.body) { return [pscustomobject]@{ valid = $false; recordLatency = $false; status = "FAIL"; failures = @("missing JSON body") } }
    if ($Kind -eq "bars") {
        if ($null -eq $Probe.body.PSObject.Properties["bars"] -or -not ($Probe.body.bars -is [System.Collections.IEnumerable])) { $failures.Add("missing bars array") }
        else {
            $bars = @(Get-ArrayValue $Probe.body.bars)
            if ($bars.Count -eq 0) { return [pscustomobject]@{ valid = $false; recordLatency = $false; status = "SKIP"; failures = @("sample has no bars") } }
            $times = @()
            foreach ($bar in $bars) {
                $time = 0L
                $rawTime = if ($null -ne $bar.PSObject.Properties["time"]) { $bar.time } else { "<missing>" }
                if (-not (Try-ParseEpochSeconds $rawTime ([ref]$time))) { $failures.Add("invalid bar timestamp: $rawTime"); continue }
                $times += $time
            }
            if ($times.Count -eq $bars.Count) {
                if (@($times | Select-Object -Unique).Count -ne $times.Count) { $failures.Add("duplicate bar times") }
                if (($times -join ",") -ne (@($times | Sort-Object) -join ",")) { $failures.Add("bar times are not ascending") }
                if (@($times | Where-Object { $_ -lt $From -or $_ -ge $To }).Count) { $failures.Add("bars outside [from,to)") }
            }
        }
    }
    else {
        if (-not $Probe.body.snapshot_version) { return [pscustomobject]@{ valid = $false; recordLatency = $false; status = "SKIP"; failures = @("no authoritative published snapshot") } }
        if ((@(Get-ArrayValue $Probe.body.levels) -join ",") -ne ($ExpectedLevels -join ",")) { $failures.Add("overlay level mapping mismatch") }
        foreach ($name in @("strokes", "segments", "centers", "signals")) { if ($null -eq $Probe.body.PSObject.Properties[$name] -or -not ($Probe.body.$name -is [System.Collections.IEnumerable])) { $failures.Add("missing $name array") } }
    }
    $valid = $failures.Count -eq 0
    return [pscustomobject]@{ valid = $valid; recordLatency = $valid; status = $(if ($valid) { "PASS" } else { "FAIL" }); failures = @($failures) }
}

function Get-ObjectTimestamp([object]$Item, [string[]]$Names) {
    foreach ($name in $Names) {
        if ($null -ne $Item.PSObject.Properties[$name] -and $null -ne $Item.$name) {
            $parsed = 0L
            if (Try-ParseEpochSeconds $Item.$name ([ref]$parsed)) { return [pscustomobject]@{ valid = $true; value = $parsed } }
            return [pscustomobject]@{ valid = $false; value = 0L; reason = "invalid timestamp in $name" }
        }
    }
    return [pscustomobject]@{ valid = $false; value = 0L; reason = "missing timestamp" }
}

function Get-LineBounds([object]$Item) {
    $start = Get-ObjectTimestamp $Item @("begin_base_ts", "start_time")
    if (-not $start.valid -and $null -ne $Item.PSObject.Properties["start"] -and $Item.start) { $start = Get-ObjectTimestamp $Item.start @("base_ts", "time") }
    $end = Get-ObjectTimestamp $Item @("end_base_ts", "end_time")
    if (-not $end.valid -and $null -ne $Item.PSObject.Properties["end"] -and $Item.end) { $end = Get-ObjectTimestamp $Item.end @("base_ts", "time") }
    if (-not $start.valid -or -not $end.valid) { return [pscustomobject]@{ valid = $false; reason = "$($start.reason); $($end.reason)" } }
    return [pscustomobject]@{ valid = $true; left = [math]::Min($start.value, $end.value); right = [math]::Max($start.value, $end.value) }
}

function Test-OverlayPayload {
    param([object]$Body, [long]$Bytes, [long]$From, [long]$To, [int]$BarCount, [hashtable]$Limits)
    $failures = [System.Collections.Generic.List[string]]::new(); $counts = [ordered]@{}; $total = 0
    foreach ($name in @("strokes", "segments", "centers", "signals")) { $counts[$name] = @(Get-ArrayValue $Body.$name).Count; $total += $counts[$name] }
    if ($Bytes -gt [long]$Limits.MaxBytes) { $failures.Add("payload bytes $Bytes exceed $($Limits.MaxBytes)") }
    if ($total -gt [int]$Limits.MaxObjects) { $failures.Add("object count $total exceeds $($Limits.MaxObjects)") }
    $proportionalLimit = [math]::Ceiling(([math]::Max(1, $BarCount) * [double]$Limits.MaxObjectsPerBar) + [int]$Limits.MinimumObjectAllowance)
    if ($total -gt $proportionalLimit) { $failures.Add("object count $total is not proportional to $BarCount bars (limit $proportionalLimit)") }
    $ids = @{}
    foreach ($name in @("strokes", "segments", "centers", "signals")) {
        foreach ($item in @(Get-ArrayValue $Body.$name)) {
            if (-not $item.id) { $failures.Add("$name object missing stable id"); continue }
            $identity = "$name|$($item.level)|$($item.mode)|$($item.id)"
            if ($ids.ContainsKey($identity)) { $failures.Add("duplicate stable id $identity") } else { $ids[$identity] = $true }
        }
    }
    foreach ($name in @("strokes", "segments")) {
        foreach ($group in @(@(Get-ArrayValue $Body.$name) | Group-Object { "$($_.level)|$($_.mode)" })) {
            $predecessors = 0; $successors = 0
            foreach ($item in $group.Group) {
                $bounds = Get-LineBounds $item
                if (-not $bounds.valid) { $failures.Add("$name $($item.id) $($bounds.reason)"); continue }
                if ($bounds.right -lt $From) { $predecessors++ } elseif ($bounds.left -gt $To) { $successors++ }
            }
            if ($predecessors -gt 1) { $failures.Add("$name group $($group.Name) has $predecessors predecessor rows") }
            if ($successors -gt 1) { $failures.Add("$name group $($group.Name) has $successors successor rows") }
        }
    }
    foreach ($center in @(Get-ArrayValue $Body.centers)) {
        $bounds = Get-LineBounds $center
        if (-not $bounds.valid) { $failures.Add("center $($center.id) $($bounds.reason)") } elseif ($bounds.right -lt $From -or $bounds.left -gt $To) { $failures.Add("center $($center.id) does not overlap window") }
    }
    foreach ($signal in @(Get-ArrayValue $Body.signals)) {
        $point = Get-ObjectTimestamp $signal @("base_ts", "time")
        if (-not $point.valid) { $failures.Add("signal $($signal.id) $($point.reason)") } elseif ($point.value -lt $From -or $point.value -gt $To) { $failures.Add("signal $($signal.id) is outside window") }
    }
    return [pscustomobject]@{ valid = $failures.Count -eq 0; failures = @($failures); bytes = $Bytes; counts = $counts; totalObjects = $total; proportionalLimit = $proportionalLimit }
}

function Test-BarsPayload([object]$Probe, [int]$BarLimit, [long]$MaxBytes) {
    $failures = [System.Collections.Generic.List[string]]::new(); $count = if ($null -ne $Probe.body -and $null -ne $Probe.body.PSObject.Properties["bars"]) { @(Get-ArrayValue $Probe.body.bars).Count } else { 0 }
    if ([long]$Probe.bytes -gt $MaxBytes) { $failures.Add("bars payload $($Probe.bytes) exceeds $MaxBytes bytes") }
    if ($count -gt $BarLimit) { $failures.Add("bars count $count exceeds limit $BarLimit") }
    return [pscustomobject]@{ valid = $failures.Count -eq 0; failures = @($failures); bytes = [long]$Probe.bytes; count = $count }
}

function New-BoundedHttpClient([int]$TimeoutSeconds) {
    Add-Type -AssemblyName System.Net.Http
    $handler = [Net.Http.HttpClientHandler]::new()
    $handler.AutomaticDecompression = [Net.DecompressionMethods]::GZip -bor [Net.DecompressionMethods]::Deflate
    $client = [Net.Http.HttpClient]::new($handler, $true)
    $client.Timeout = [TimeSpan]::FromSeconds($TimeoutSeconds)
    return $client
}

function Invoke-BoundedHttpGet {
    param([Net.Http.HttpClient]$Client, [string]$Uri, [hashtable]$Headers, [int]$TimeoutSeconds, [long]$MaxBytes)
    $watch = [Diagnostics.Stopwatch]::StartNew(); $request = $null; $response = $null; $stream = $null; $memory = $null
    $cts = [Threading.CancellationTokenSource]::new([TimeSpan]::FromSeconds($TimeoutSeconds))
    $transport = if ($PSVersionTable.PSVersion.Major -le 5) { "HttpClient.ResponseHeadersRead.ps51-fallback" } else { "HttpClient.ResponseHeadersRead" }
    try {
        $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Get, $Uri)
        foreach ($key in $Headers.Keys) { [void]$request.Headers.TryAddWithoutValidation($key, [string]$Headers[$key]) }
        $response = $Client.SendAsync($request, [Net.Http.HttpCompletionOption]::ResponseHeadersRead, $cts.Token).GetAwaiter().GetResult()
        $statusCode = [int]$response.StatusCode
        $declared = $response.Content.Headers.ContentLength
        if ($null -ne $declared -and [long]$declared -gt $MaxBytes) {
            $cts.Cancel(); $watch.Stop()
            return [pscustomobject]@{ statusCode = $statusCode; elapsedMs = [math]::Round($watch.Elapsed.TotalMilliseconds, 2); bytes = 0; text = "declared response size $declared exceeds cap $MaxBytes"; failureKind = "response-too-large"; transport = $transport }
        }
        $stream = $response.Content.ReadAsStreamAsync().GetAwaiter().GetResult(); $memory = [IO.MemoryStream]::new(); $buffer = [byte[]]::new(8192); $total = 0L
        while ($true) {
            $read = $stream.ReadAsync($buffer, 0, $buffer.Length, $cts.Token).GetAwaiter().GetResult()
            if ($read -eq 0) { break }
            if ($total + $read -gt $MaxBytes) { $cts.Cancel(); $watch.Stop(); return [pscustomobject]@{ statusCode = $statusCode; elapsedMs = [math]::Round($watch.Elapsed.TotalMilliseconds, 2); bytes = $total; text = "streamed response exceeds cap $MaxBytes"; failureKind = "response-too-large"; transport = $transport } }
            $memory.Write($buffer, 0, $read); $total += $read
        }
        $watch.Stop(); $text = [Text.Encoding]::UTF8.GetString($memory.ToArray())
        return [pscustomobject]@{ statusCode = $statusCode; elapsedMs = [math]::Round($watch.Elapsed.TotalMilliseconds, 2); bytes = $total; text = $text; failureKind = ""; transport = $transport }
    }
    catch {
        $watch.Stop(); $exception = $_.Exception
        while ($exception.InnerException) { $exception = $exception.InnerException }
        $timeout = $exception -is [OperationCanceledException] -or $exception -is [Threading.Tasks.TaskCanceledException] -or $cts.IsCancellationRequested
        return [pscustomobject]@{ statusCode = 0; elapsedMs = [math]::Round($watch.Elapsed.TotalMilliseconds, 2); bytes = 0; text = $(if ($timeout) { "request timeout after ${TimeoutSeconds}s" } else { $exception.Message }); failureKind = $(if ($timeout) { "timeout" } else { "transport" }); transport = $transport }
    }
    finally {
        if ($memory) { $memory.Dispose() }; if ($stream) { $stream.Dispose() }; if ($response) { $response.Dispose() }; if ($request) { $request.Dispose() }; $cts.Dispose()
    }
}

function Write-AtomicTextFile([string]$Path, [string]$Content) {
    $temporary = "$Path.tmp-$([guid]::NewGuid().ToString('N'))"; $Content | Set-Content -LiteralPath $temporary -Encoding UTF8; Move-Item -LiteralPath $temporary -Destination $Path -Force
}

function Test-EvidenceManifest([string]$Directory, [string]$RunId) {
    $manifestPath = Join-Path $Directory "manifest.json"; if (-not (Test-Path -LiteralPath $manifestPath)) { return $false }
    try { $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json } catch { return $false }
    if ($manifest.runId -ne $RunId) { return $false }
    foreach ($name in @("report.json", "report.md")) {
        $path = Join-Path $Directory $name; if (-not (Test-Path -LiteralPath $path)) { return $false }
        $property = $manifest.files.PSObject.Properties[$name]; if ($null -eq $property -or (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash -ne $property.Value) { return $false }
    }
    return $true
}

function Publish-EvidenceSet {
    param([string]$OutputRoot, [string]$RunId, [object]$Report, [string]$Markdown, [object]$RunLock = $null)
    $ownedLock = $null
    if ($null -eq $RunLock) { $ownedLock = Enter-EvidenceRunLock $OutputRoot $RunId; $RunLock = $ownedLock }
    try {
        if ($RunLock.runId -ne $RunId) { throw "Evidence lock does not match RunId." }
        $paths = $RunLock.paths
        if (Test-Path -LiteralPath $paths.final) { if (-not (Test-EvidenceManifest $paths.final $RunId)) { throw "Existing evidence set is incomplete or has invalid hashes: $($paths.final)" }; return [pscustomobject]@{ path = $paths.final; idempotent = $true; recovered = $false } }
        $recovered = Test-Path -LiteralPath $paths.stage; New-Item -ItemType Directory -Force -Path $paths.stage | Out-Null
        $jsonPath = Join-Path $paths.stage "report.json"; $markdownPath = Join-Path $paths.stage "report.md"
        Write-AtomicTextFile $jsonPath ($Report | ConvertTo-Json -Depth 32); Write-AtomicTextFile $markdownPath $Markdown
        $files = [ordered]@{ "report.json" = (Get-FileHash -Algorithm SHA256 -LiteralPath $jsonPath).Hash; "report.md" = (Get-FileHash -Algorithm SHA256 -LiteralPath $markdownPath).Hash }
        Write-AtomicTextFile (Join-Path $paths.stage "manifest.json") ([ordered]@{ schemaVersion = "chart-windowed-overlay-evidence.v1"; runId = $RunId; files = $files } | ConvertTo-Json -Depth 8)
        if (-not (Test-EvidenceManifest $paths.stage $RunId)) { throw "Staged evidence validation failed: $($paths.stage)" }
        Move-Item -LiteralPath $paths.stage -Destination $paths.final
        return [pscustomobject]@{ path = $paths.final; idempotent = $false; recovered = $recovered }
    }
    finally { if ($ownedLock) { Exit-EvidenceRunLock $ownedLock } }
}
