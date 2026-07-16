[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$ApiBaseUrl = "http://127.0.0.1:8001",
    [string]$WebUrl = "",
    [string[]]$Symbols = @("000001.SZ", "600000.SH", "430047.BJ", "000017.SZ"),
    [switch]$NoDefaultSymbols,
    [string]$SparseSymbol = "000017.SZ",
    [string]$EqualPriceSymbol = "",
    [switch]$NoSupplementalSymbols,
    [string[]]$Timeframes = @("5f", "15f", "30f", "1h", "1d", "1w", "1m"),
    [string[]]$WindowNames = @("cold-first-observation", "shifted-overlap"),
    [string]$ApiToken = $env:VITE_API_TOKEN,
    [ValidateRange(20, 5000)][int]$BarLimit = 300,
    [ValidateRange(2, 20)][int]$WarmSamples = 3,
    [ValidateRange(1, 120)][int]$RequestTimeoutSeconds = 10,
    [ValidateRange(1, 500)][int]$MaxMatrixCells = 100,
    [ValidateRange(1024, 104857600)][long]$MaxBarsResponseBytes = 2097152,
    [ValidateRange(1024, 104857600)][long]$MaxOverlayResponseBytes = 4194304,
    [ValidateRange(1, 100000)][int]$MaxOverlayObjects = 6000,
    [ValidateRange(0.1, 100.0)][double]$MaxOverlayObjectsPerBar = 12.0,
    [ValidateRange(0, 10000)][int]$MinimumOverlayObjectAllowance = 32,
    [ValidateSet("Auto", "Run", "Skip")][string]$ManagerContractTests = "Auto",
    [string]$OutputRoot = "",
    [string]$SourceRoot = "",
    [string]$NodePath = "",
    [string]$TypeScriptCompilerPath = "",
    [ValidateRange(1, 60)][int]$SourceScanTimeoutSeconds = 10,
    [string]$RunId = "",
    [switch]$SkipSourceCheck,
    [switch]$SkipResourceCheck
)

# This acceptance client issues GET requests only. It never flushes caches,
# applies migrations, starts processes, or mutates server-side data.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot "chart-windowed-overlay\harness-core.ps1")
$script:RunLock = $null
$script:HttpClient = $null
trap {
    [Console]::Error.WriteLine("Task6 harness failure: $($_.Exception.Message)")
    if ($script:HttpClient) { $script:HttpClient.Dispose(); $script:HttpClient = $null }
    if ($script:RunLock) { Exit-EvidenceRunLock $script:RunLock; $script:RunLock = $null }
    exit 2
}
if (-not $OutputRoot) { $OutputRoot = Join-Path $repoRoot "outputs\chart-windowed-overlay-verification" }
if (-not $SourceRoot) { $SourceRoot = $repoRoot }
if (-not $NodePath) { $nodeCommand = Get-Command node -ErrorAction SilentlyContinue; if ($nodeCommand) { $NodePath = $nodeCommand.Source } }
if (-not $TypeScriptCompilerPath) { $TypeScriptCompilerPath = Join-Path $repoRoot "apps\web\node_modules\typescript\lib\typescript.js" }
if (-not $RunId) { $RunId = "cwo-{0}-{1}" -f (Get-Date -Format "yyyyMMddTHHmmssfff"), ([guid]::NewGuid().ToString("N").Substring(0, 8)) }
if (-not (Test-SafeRunId $RunId)) { throw "Invalid RunId '$RunId'. Use ASCII alphanumeric characters followed only by ASCII alphanumeric, dot, underscore, or hyphen." }

$displayLevels = [ordered]@{
    "5f" = @("5f", "30f", "1d"); "15f" = @("5f", "30f", "1d")
    "30f" = @("30f", "1d"); "1h" = @("30f", "1d")
    "1d" = @("1d", "1w"); "1w" = @("1w", "1m"); "1m" = @("1m")
}
$allowedWindows = @("cold-first-observation", "shifted-overlap")
$baseSymbols = if ($NoDefaultSymbols) { @() } else { @($Symbols) }
$supplementalSymbols = if ($NoSupplementalSymbols) { @() } else { @($SparseSymbol, $EqualPriceSymbol) }
$effectiveSymbols = @($baseSymbols + $supplementalSymbols | Where-Object { $_ } | ForEach-Object { $_.ToUpperInvariant() } | Select-Object -Unique)
$effectiveTimeframes = @($Timeframes | Select-Object -Unique)
$effectiveWindows = @($WindowNames | Select-Object -Unique)
$matrix = @(New-ProbeMatrix -Symbols $effectiveSymbols -Timeframes $effectiveTimeframes -WindowNames $effectiveWindows)
$script:Results = [System.Collections.Generic.List[object]]::new()
$script:MatrixResults = [System.Collections.Generic.List[object]]::new()
$script:Measurements = [System.Collections.Generic.List[object]]::new()

function Add-Result([string]$Name, [string]$Status, [string]$Message, [object]$Evidence = $null) {
    $script:Results.Add([pscustomobject]@{ name = $Name; status = $Status; message = $Message; evidence = $Evidence })
}

function Get-Percentile([double[]]$Values, [double]$Percentile) {
    if ($Values.Count -eq 0) { return $null }
    $ordered = @($Values | Sort-Object)
    $index = [math]::Max(0, [math]::Min([math]::Ceiling($Percentile * $ordered.Count) - 1, $ordered.Count - 1))
    return [math]::Round($ordered[$index], 2)
}

function New-RequestUri([string]$Path, [hashtable]$Parameters) {
    $pairs = foreach ($key in @($Parameters.Keys | Sort-Object)) {
        if ($null -ne $Parameters[$key] -and "$($Parameters[$key])" -ne "") {
            "{0}={1}" -f [uri]::EscapeDataString($key), [uri]::EscapeDataString("$($Parameters[$key])")
        }
    }
    return "{0}/{1}{2}" -f $ApiBaseUrl.TrimEnd("/"), $Path.TrimStart("/"), $(if ($pairs.Count) { "?" + ($pairs -join "&") } else { "" })
}

function Invoke-ReadOnlyGet([string]$Path, [hashtable]$Parameters) {
    $uri = New-RequestUri $Path $Parameters
    if ($WhatIfPreference -or -not $PSCmdlet.ShouldProcess($uri, "GET read-only acceptance probe")) {
        return [pscustomobject]@{ uri = $uri; skipped = $true; statusCode = 0; elapsedMs = $null; bytes = 0; body = $null; text = "" }
    }
    $headers = @{}; if ($ApiToken) { $headers.Authorization = "Bearer $ApiToken" }
    $cap = if ($Path -match '/overlay$') { $MaxOverlayResponseBytes } else { $MaxBarsResponseBytes }
    $bounded = Invoke-BoundedHttpGet -Client $script:HttpClient -Uri $uri -Headers $headers -TimeoutSeconds $RequestTimeoutSeconds -MaxBytes $cap
    $body = $null; $failureKind = $bounded.failureKind; $failureText = $bounded.text
    if (-not $failureKind -and $bounded.text) {
        try { $body = $bounded.text | ConvertFrom-Json }
        catch { $failureKind = "invalid-json"; $failureText = "bounded response JSON parse failed: $($_.Exception.Message)" }
    }
    return [pscustomobject]@{ uri = $uri; skipped = $false; statusCode = $bounded.statusCode; elapsedMs = $bounded.elapsedMs; bytes = $bounded.bytes; body = $body; text = $failureText; failureKind = $failureKind; transport = $bounded.transport }
}

function Find-DebugEvidence([object]$Value, [string[]]$Needles, [string]$Path = "$") {
    $found = [System.Collections.Generic.List[object]]::new()
    if ($null -eq $Value) { return $found }
    if ($Value -is [pscustomobject] -or $Value -is [System.Collections.IDictionary]) {
        foreach ($property in $Value.PSObject.Properties) {
            $childPath = "$Path.$($property.Name)"
            if (@($Needles | Where-Object { $property.Name -match $_ }).Count) { $found.Add([pscustomobject]@{ path = $childPath; value = "$($property.Value)" }) }
            foreach ($entry in @(Find-DebugEvidence $property.Value $Needles $childPath)) { $found.Add($entry) }
        }
    }
    elseif ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
        $index = 0
        foreach ($item in $Value) { foreach ($entry in @(Find-DebugEvidence $item $Needles "$Path[$index]")) { $found.Add($entry) }; $index++ }
    }
    return $found
}

function Get-SeedWindow([string]$Symbol, [string]$Timeframe) {
    $probe = Invoke-ReadOnlyGet "api/v3/chart/bars" @{ symbol = $Symbol; timeframe = $Timeframe; limit = $BarLimit }
    if ($probe.skipped) { return [pscustomobject]@{ status = "PENDING"; message = "WhatIf: discovery GET not executed"; probe = $probe } }
    if ($probe.failureKind) { return [pscustomobject]@{ status = "FAIL"; message = "$($probe.failureKind): $($probe.text)"; probe = $probe } }
    if ($probe.statusCode -eq 404) { return [pscustomobject]@{ status = "SKIP"; message = "sample unavailable"; probe = $probe } }
    if ($probe.statusCode -ne 200 -or $null -eq $probe.body -or $null -eq $probe.body.PSObject.Properties["bars"]) { return [pscustomobject]@{ status = "FAIL"; message = "discovery HTTP/content failure"; probe = $probe } }
    $bars = @(Get-ArrayValue $probe.body.bars)
    if ($bars.Count -eq 0) { return [pscustomobject]@{ status = "SKIP"; message = "sample has no bars"; probe = $probe } }
    $parsedBars = @()
    foreach ($bar in $bars) {
        $time = 0L
        $rawTime = if ($null -ne $bar.PSObject.Properties["time"]) { $bar.time } else { "<missing>" }
        if (-not (Try-ParseEpochSeconds $rawTime ([ref]$time))) { return [pscustomobject]@{ status = "FAIL"; message = "invalid discovery bar timestamp: $rawTime"; probe = $probe } }
        $parsedBars += [pscustomobject]@{ time = $time; bar = $bar }
    }
    $parsedBars = @($parsedBars | Sort-Object time)
    $bars = @($parsedBars | ForEach-Object { $_.bar })
    $times = @($parsedBars | ForEach-Object { $_.time })
    if (@($times | Select-Object -Unique).Count -ne $times.Count) { return [pscustomobject]@{ status = "FAIL"; message = "discovery returned duplicate bar times"; probe = $probe } }
    $interval = if ($times.Count -gt 1) { [math]::Max(1, $times[-1] - $times[-2]) } else { 1 }
    $shiftIndex = [math]::Min($times.Count - 1, [math]::Max(0, [math]::Floor($times.Count / 4)))
    return [pscustomobject]@{
        status = "PASS"; message = "discovery complete"; probe = $probe; bars = $bars
        windows = @{
            "cold-first-observation" = [pscustomobject]@{ from = $times[0]; barsToExclusive = $times[-1] + $interval; overlayToInclusive = $times[-1] }
            "shifted-overlap" = [pscustomobject]@{ from = $times[$shiftIndex]; barsToExclusive = $times[-1] + $interval; overlayToInclusive = $times[-1] }
        }
    }
}

function Invoke-ValidatedSample([string]$Kind, [hashtable]$Request, [long]$From, [long]$To, [string[]]$ExpectedLevels) {
    $path = if ($Kind -eq "bars") { "api/v3/chart/bars" } else { "api/v3/chart/overlay" }
    $probe = Invoke-ReadOnlyGet $path $Request
    $validation = Test-ProbeResponse -Probe $probe -Kind $Kind -From $From -To $To -ExpectedLevels $ExpectedLevels
    return [pscustomobject]@{ probe = $probe; validation = $validation }
}

function Test-MatrixCell([object]$Cell, [object]$Discovery) {
    if ($Discovery.status -ne "PASS") {
        $transport = if ($null -ne $Discovery.probe -and $null -ne $Discovery.probe.PSObject.Properties["transport"]) { $Discovery.probe.transport } else { "" }
        $script:MatrixResults.Add([pscustomobject]@{ key = $Cell.key; symbol = $Cell.symbol; timeframe = $Cell.timeframe; window = $Cell.window; status = $Discovery.status; message = $Discovery.message; transport = $transport })
        return
    }
    $window = $Discovery.windows[$Cell.window]
    if ($null -eq $window) {
        $script:MatrixResults.Add([pscustomobject]@{ key = $Cell.key; symbol = $Cell.symbol; timeframe = $Cell.timeframe; window = $Cell.window; status = "FAIL"; message = "window definition unavailable" })
        return
    }
    $levels = @($displayLevels[$Cell.timeframe])
    $fromText = ""; $barsToText = ""; $overlayToText = ""
    if (-not (Try-FormatApiTime $window.from ([ref]$fromText)) -or -not (Try-FormatApiTime $window.barsToExclusive ([ref]$barsToText)) -or -not (Try-FormatApiTime $window.overlayToInclusive ([ref]$overlayToText))) {
        $script:MatrixResults.Add([pscustomobject]@{ key = $Cell.key; symbol = $Cell.symbol; timeframe = $Cell.timeframe; window = $Cell.window; status = "FAIL"; message = "invalid timestamp while formatting bounded request" })
        return
    }
    $barRequest = @{ symbol = $Cell.symbol; timeframe = $Cell.timeframe; limit = $BarLimit; from = $fromText; to = $barsToText }
    $barSample = Invoke-ValidatedSample bars $barRequest $window.from $window.barsToExclusive @()
    $failures = [System.Collections.Generic.List[string]]::new()
    $barCount = if ($barSample.validation.valid) { @(Get-ArrayValue $barSample.probe.body.bars).Count } else { 0 }
    $barPayload = if ($barSample.validation.valid) { Test-BarsPayload -Probe $barSample.probe -BarLimit $BarLimit -MaxBytes $MaxBarsResponseBytes } else { $null }
    if ($barSample.validation.status -eq "SKIP") {
        $script:MatrixResults.Add([pscustomobject]@{ key = $Cell.key; symbol = $Cell.symbol; timeframe = $Cell.timeframe; window = $Cell.window; status = "SKIP"; message = "bars sample unavailable" })
        return
    }
    if (-not $barSample.validation.valid) { foreach ($failure in $barSample.validation.failures) { $failures.Add("bars: $failure") } }
    elseif (-not $barPayload.valid) { foreach ($failure in $barPayload.failures) { $failures.Add("bars: $failure") } }

    $overlayRequest = @{ symbol = $Cell.symbol; timeframe = $Cell.timeframe; levels = ($levels -join ","); modes = "confirmed,predictive"; limit = $BarLimit; from = $fromText; to = $overlayToText }
    $overlaySample = Invoke-ValidatedSample overlay $overlayRequest $window.from $window.overlayToInclusive $levels
    $payload = $null
    if ($overlaySample.validation.status -eq "SKIP") {
        $script:MatrixResults.Add([pscustomobject]@{ key = $Cell.key; symbol = $Cell.symbol; timeframe = $Cell.timeframe; window = $Cell.window; status = "SKIP"; message = "authoritative overlay unavailable" })
        return
    }
    if (-not $overlaySample.validation.valid) { foreach ($failure in $overlaySample.validation.failures) { $failures.Add("overlay: $failure") } }
    else {
        $payload = Test-OverlayPayload -Body $overlaySample.probe.body -Bytes $overlaySample.probe.bytes -From $window.from -To $window.overlayToInclusive -BarCount $barCount -Limits @{
            MaxBytes = $MaxOverlayResponseBytes; MaxObjects = $MaxOverlayObjects
            MaxObjectsPerBar = $MaxOverlayObjectsPerBar; MinimumObjectAllowance = $MinimumOverlayObjectAllowance
        }
        foreach ($failure in $payload.failures) { $failures.Add("overlay: $failure") }
    }

    $warmBars = @(); $warmOverlay = @()
    $barContractValid = $barSample.validation.valid -and $barPayload.valid
    $overlayContractValid = $overlaySample.validation.valid -and $null -ne $payload -and $payload.valid
    if ($Cell.window -eq "cold-first-observation" -and $barContractValid -and $overlayContractValid) {
        for ($sampleIndex = 1; $sampleIndex -le $WarmSamples; $sampleIndex++) {
            $barWarm = Invoke-ValidatedSample bars $barRequest $window.from $window.barsToExclusive @()
            $barWarmPayload = if ($barWarm.validation.valid) { Test-BarsPayload -Probe $barWarm.probe -BarLimit $BarLimit -MaxBytes $MaxBarsResponseBytes } else { $null }
            if ($barWarm.validation.recordLatency -and $barWarmPayload.valid) { $warmBars += [double]$barWarm.probe.elapsedMs }
            else {
                foreach ($failure in $barWarm.validation.failures) { $failures.Add("warm bars sample ${sampleIndex}: $failure") }
                if ($barWarmPayload) { foreach ($failure in $barWarmPayload.failures) { $failures.Add("warm bars sample ${sampleIndex}: $failure") } }
            }
            $overlayWarm = Invoke-ValidatedSample overlay $overlayRequest $window.from $window.overlayToInclusive $levels
            $overlayWarmPayload = if ($overlayWarm.validation.valid) { Test-OverlayPayload -Body $overlayWarm.probe.body -Bytes $overlayWarm.probe.bytes -From $window.from -To $window.overlayToInclusive -BarCount $barCount -Limits @{
                MaxBytes = $MaxOverlayResponseBytes; MaxObjects = $MaxOverlayObjects
                MaxObjectsPerBar = $MaxOverlayObjectsPerBar; MinimumObjectAllowance = $MinimumOverlayObjectAllowance
            } } else { $null }
            if ($overlayWarm.validation.recordLatency -and $overlayWarmPayload.valid) { $warmOverlay += [double]$overlayWarm.probe.elapsedMs }
            else {
                foreach ($failure in $overlayWarm.validation.failures) { $failures.Add("warm overlay sample ${sampleIndex}: $failure") }
                if ($overlayWarmPayload) { foreach ($failure in $overlayWarmPayload.failures) { $failures.Add("warm overlay sample ${sampleIndex}: $failure") } }
            }
        }
    }
    if ($barContractValid) {
        $script:Measurements.Add([pscustomobject]@{ kind = "bars"; phase = $Cell.window; symbol = $Cell.symbol; timeframe = $Cell.timeframe; elapsedMs = [double]$barSample.probe.elapsedMs; warm = $warmBars })
    }
    if ($overlayContractValid) {
        $script:Measurements.Add([pscustomobject]@{ kind = "overlay"; phase = $Cell.window; symbol = $Cell.symbol; timeframe = $Cell.timeframe; elapsedMs = [double]$overlaySample.probe.elapsedMs; warm = $warmOverlay })
    }
    $status = if ($failures.Count) { "FAIL" } elseif ($barSample.validation.status -eq "SKIP" -or $overlaySample.validation.status -eq "SKIP") { "SKIP" } else { "PASS" }
    $debugEvidence = if ($overlaySample.validation.valid) { @(Find-DebugEvidence $overlaySample.probe.body @("head", "config.*hash", "base.*timeframe", "status", "coverage", "bar_from", "bar_until")) } else { @() }
    $script:MatrixResults.Add([pscustomobject]@{
        key = $Cell.key; symbol = $Cell.symbol; timeframe = $Cell.timeframe; window = $Cell.window; status = $status
        message = $(if ($failures.Count) { $failures -join "; " } else { "bars and overlay contract passed" })
        request = @{ from = $window.from; barsToExclusive = $window.barsToExclusive; overlayToInclusive = $window.overlayToInclusive; expectedLevels = $levels }
        bars = @{ statusCode = $barSample.probe.statusCode; bytes = $barSample.probe.bytes; count = $barCount; transport = $barSample.probe.transport }
        overlay = @{ statusCode = $overlaySample.probe.statusCode; bytes = $overlaySample.probe.bytes; payload = $payload; debugEvidence = $debugEvidence; transport = $overlaySample.probe.transport }
    })
}

function Test-ProductionNoBundlePath {
    if ($WhatIfPreference) { Add-Result "normal-path-no-bundle" "PENDING" "WhatIf: source gate not executed."; return }
    if ($SkipSourceCheck) { Add-Result "normal-path-no-bundle" "PENDING" "Source gate explicitly skipped."; return }
    $relativePaths = @(
        "apps/web/src/api/chartDataManager.ts", "apps/web/src/api/chanOverlayManager.ts",
        "apps/web/src/api/chanRealtimeOverlayBridge.ts", "apps/web/src/api/realtime.ts", "apps/web/src/tradingview/datafeed.ts",
        "apps/web/src/components/ChartWorkspace.tsx"
    )
    $sources = @{}
    $missingSources = @()
    foreach ($relative in $relativePaths) {
        $path = Join-Path $SourceRoot $relative
        if (Test-Path -LiteralPath $path) { $sources[$relative] = Get-Content -Raw -Encoding UTF8 -LiteralPath $path }
        else { $missingSources += $relative }
    }
    $scan = Invoke-TypeScriptBundleScan -Sources $sources -NodePath $NodePath -TypeScriptPath $TypeScriptCompilerPath -ScannerPath (Join-Path $PSScriptRoot "chart-windowed-overlay\source-scan.mjs") -TimeoutSeconds $SourceScanTimeoutSeconds -RequireProductionScopes
    $findings = @($scan.findings); $uncertainties = @($scan.uncertainties) + @($missingSources | ForEach-Object { [pscustomobject]@{ path = $_; scope = "production-entrypoint"; reason = "required source file not found" } })
    $status = if ($findings.Count -or $uncertainties.Count) { "FAIL" } else { "PASS" }
    Add-Result "normal-path-no-bundle" $status $(if ($uncertainties.Count) { "$($findings.Count) active bundle usage(s) and $($uncertainties.Count) scanner uncertainty item(s) found; scanner fails closed." } elseif ($findings.Count) { "$($findings.Count) active production-path bundle usage(s) found." } else { "No active bars/overlay/realtime bundle usage found; TypeScript AST ignored comments, inert literals, and compatibility definitions." }) @{ findings = $findings; uncertainties = $uncertainties; scanner = $scan.scanner }
}

function Test-ManagerContracts {
    if ($WhatIfPreference -or $ManagerContractTests -eq "Skip") { Add-Result "manager-cache-contracts" "PENDING" "Manager contract execution not requested."; return }
    $webRoot = Join-Path $repoRoot "apps\web"
    $testPath = Join-Path $webRoot "src\api\chartDataManager.contract.test.ts"
    $tsx = Join-Path $webRoot "node_modules\.bin\tsx.cmd"
    if (-not (Test-Path -LiteralPath $tsx)) { $tsx = Join-Path $webRoot "node_modules\.bin\tsx" }
    if (-not (Test-Path -LiteralPath $testPath) -or -not (Test-Path -LiteralPath $tsx)) {
        $status = if ($ManagerContractTests -eq "Run") { "FAIL" } else { "PENDING" }
        Add-Result "manager-cache-contracts" $status "Local tsx dependency or focused manager contract test is unavailable; no install was attempted."
        return
    }
    $output = @(& $tsx --test $testPath 2>&1 | ForEach-Object { "$_" })
    $code = $LASTEXITCODE
    Add-Result "manager-cache-contracts" $(if ($code -eq 0) { "PASS" } else { "FAIL" }) $(if ($code -eq 0) { "Focused manager cache/coalescing/revisit contract suite passed." } else { "Focused manager contract suite failed with exit code $code." }) @{ command = "$tsx --test $testPath"; output = @($output | Select-Object -Last 40) }
}

function Test-Resources {
    if ($WhatIfPreference -or $SkipResourceCheck) { Add-Result "local-resource-snapshot" "PENDING" "Local resource snapshot not executed."; return }
    try {
        $memory = Get-CimInstance Win32_OperatingSystem
        $processes = @(Get-Process -ErrorAction SilentlyContinue | Sort-Object WorkingSet64 -Descending | Select-Object -First 5 ProcessName, Id, @{n="workingSetMb";e={[math]::Round($_.WorkingSet64 / 1MB, 1)}})
        Add-Result "local-resource-snapshot" "PASS" "Read-only snapshot captured; no process was started or stopped." @{ freeMemoryMb = [math]::Round($memory.FreePhysicalMemory / 1024, 0); topProcesses = $processes }
    }
    catch { Add-Result "local-resource-snapshot" "PENDING" "Resource snapshot unavailable: $($_.Exception.Message)" }
}

function Add-LatencyGates {
    foreach ($kind in @("bars", "overlay")) {
        $first = @($script:Measurements | Where-Object { $_.kind -eq $kind -and $_.phase -eq "cold-first-observation" } | ForEach-Object { $_.elapsedMs })
        $warm = @($script:Measurements | Where-Object { $_.kind -eq $kind -and $_.phase -eq "cold-first-observation" } | ForEach-Object { @($_.warm) })
        $targetFirst = if ($kind -eq "bars") { 500 } else { 800 }; $targetWarm = if ($kind -eq "bars") { 100 } else { 150 }
        if (-not $first.Count -or -not $warm.Count) { Add-Result "latency-$kind" "PENDING" "No fully validated timing samples were available."; continue }
        $firstP50 = Get-Percentile $first 0.5; $firstP95 = Get-Percentile $first 0.95
        $warmP50 = Get-Percentile $warm 0.5; $warmP95 = Get-Percentile $warm 0.95
        $status = if ($firstP95 -lt $targetFirst -and $warmP95 -lt $targetWarm) { "PASS" } else { "FAIL" }
        Add-Result "latency-$kind" $status "first-observation p50=${firstP50}ms p95=${firstP95}ms (target p95 <$targetFirst); warm p50=${warmP50}ms p95=${warmP95}ms (target p95 <$targetWarm). No server cache flush was attempted or claimed." @{ firstObservationCount = $first.Count; warmCount = $warm.Count; serverCacheFlushed = $false }
    }
}

function New-MarkdownReport([object]$Report) {
    $lines = @("# Chart Windowed Overlay Acceptance", "", "Run: $(ConvertTo-MarkdownCell $RunId)", "", "API: $(ConvertTo-MarkdownCell $ApiBaseUrl)", "", "This run was GET-only. Server caches were not flushed.", "", "| Gate | Status | Result |", "|---|---|---|")
    $lines += $script:Results | ForEach-Object { "| $(ConvertTo-MarkdownCell $_.name) | $(ConvertTo-MarkdownCell $_.status) | $(ConvertTo-MarkdownCell $_.message) |" }
    $lines += "", "## Matrix", "", "| Symbol | Timeframe | Window | Status | Result |", "|---|---|---|---|---|"
    $lines += $script:MatrixResults | ForEach-Object { "| $(ConvertTo-MarkdownCell $_.symbol) | $(ConvertTo-MarkdownCell $_.timeframe) | $(ConvertTo-MarkdownCell $_.window) | $(ConvertTo-MarkdownCell $_.status) | $(ConvertTo-MarkdownCell $_.message) |" }
    $lines += "", "## Pending Browser And Realtime Evidence", "", "- [ ] Five rapid drags: 150ms debounce, at most one completed paint, and zero superseded overlay HTTP completions.", "- [ ] Revisit an already covered bars and overlay window: zero HTTP requests and no flash-empty redraw.", "- [ ] Rapid symbol/timeframe switch: no stale paint, no visible AbortError noise, and bars paint before overlay.", "- [ ] Endpoint, center, signal, and configured equal-price projection align; overlay UX completion is under 2 seconds.", "- [ ] WebSocket sequence gap triggers one active-window resync; loss preserves the last valid overlay and uses HTTP fallback."
    return $lines -join [Environment]::NewLine
}

if (-not $effectiveSymbols.Count -or -not $effectiveTimeframes.Count -or -not $effectiveWindows.Count -or -not $matrix.Count) { throw "Configured symbol/timeframe/window matrix must be non-empty." }
if ($matrix.Count -gt $MaxMatrixCells) { throw "Configured matrix has $($matrix.Count) cells, exceeding MaxMatrixCells=$MaxMatrixCells. Increase the explicit safety override; the harness will not truncate the matrix." }
if (@($effectiveTimeframes | Where-Object { -not $displayLevels.Contains($_) }).Count) { throw "Unsupported timeframe configured. Allowed: $($displayLevels.Keys -join ', ')" }
if (@($effectiveWindows | Where-Object { $_ -notin $allowedWindows }).Count) { throw "Unsupported window configured. Allowed: $($allowedWindows -join ', ')" }

if (-not $WhatIfPreference) {
    $script:RunLock = Enter-EvidenceRunLock $OutputRoot $RunId
    $existing = $script:RunLock.paths.final
    if (Test-Path -LiteralPath $existing) {
        if (-not (Test-EvidenceManifest $existing $RunId)) { throw "Existing run is incomplete or corrupt: $existing" }
        $existingReport = Get-Content -Raw -LiteralPath (Join-Path $existing "report.json") | ConvertFrom-Json
        Write-Host "Evidence already complete: $existing"
        $existingCode = if ([int]$existingReport.summary.fail -gt 0) { 2 } else { 0 }
        Exit-EvidenceRunLock $script:RunLock; $script:RunLock = $null
        exit $existingCode
    }
    $script:HttpClient = New-BoundedHttpClient $RequestTimeoutSeconds
}

Test-Resources
Test-ProductionNoBundlePath
Test-ManagerContracts
$discoveries = @{}
foreach ($symbol in $effectiveSymbols) {
    foreach ($timeframe in $effectiveTimeframes) { $discoveries["$symbol|$timeframe"] = Get-SeedWindow $symbol $timeframe }
}
foreach ($cell in $matrix) { Test-MatrixCell $cell $discoveries["$($cell.symbol)|$($cell.timeframe)"] }

$expectedCells = $effectiveSymbols.Count * $effectiveTimeframes.Count * $effectiveWindows.Count
$actualKeys = @($script:MatrixResults | Select-Object -ExpandProperty key -Unique)
Add-Result "matrix-completeness" $(if ($actualKeys.Count -eq $expectedCells) { "PASS" } else { "FAIL" }) "$($actualKeys.Count)/$expectedCells configured symbol x timeframe x window cells recorded." @{ symbols = $effectiveSymbols; timeframes = $effectiveTimeframes; windows = $effectiveWindows }
Add-LatencyGates
Add-Result "browser-debounce-cache-revisit" "PENDING" "Browser debounce, zero-HTTP covered-window revisit, stale-paint, projection, and UX <2s require manual evidence."
Add-Result "websocket-gap-resync-fallback" "PENDING" "Realtime gap/resync and HTTP fallback were not executed by this GET-only harness."
if ($EqualPriceSymbol) { Add-Result "equal-price-endpoint" "PENDING" "Configured sample $EqualPriceSymbol is in the API matrix; final last-equal-price paint remains a browser assertion." }
else { Add-Result "equal-price-endpoint" "SKIP" "No equal-price sample was configured." }

$summary = [ordered]@{
    pass = @($script:Results | Where-Object status -eq "PASS").Count + @($script:MatrixResults | Where-Object status -eq "PASS").Count
    fail = @($script:Results | Where-Object status -eq "FAIL").Count + @($script:MatrixResults | Where-Object status -eq "FAIL").Count
    skip = @($script:Results | Where-Object status -eq "SKIP").Count + @($script:MatrixResults | Where-Object status -eq "SKIP").Count
    pending = @($script:Results | Where-Object status -eq "PENDING").Count + @($script:MatrixResults | Where-Object status -eq "PENDING").Count
}
$report = [ordered]@{
    schemaVersion = "chart-windowed-overlay-verification.v2"; runId = $RunId; generatedAt = (Get-Date).ToUniversalTime().ToString("o")
    readOnly = $true; httpMethods = @("GET"); serverCacheFlushed = $false; apiBaseUrl = $ApiBaseUrl; webUrl = $WebUrl; sourceRoot = [IO.Path]::GetFullPath($SourceRoot)
    effectiveConfig = [ordered]@{
        symbols = $effectiveSymbols; sparseSymbol = $SparseSymbol; equalPriceSymbol = $EqualPriceSymbol; timeframes = $effectiveTimeframes; windows = $effectiveWindows
        barLimit = $BarLimit; warmSamples = $WarmSamples; requestTimeoutSeconds = $RequestTimeoutSeconds; sourceScanTimeoutSeconds = $SourceScanTimeoutSeconds; maxMatrixCells = $MaxMatrixCells; managerContractTests = $ManagerContractTests
        thresholds = @{ maxBarsResponseBytes = $MaxBarsResponseBytes; maxOverlayResponseBytes = $MaxOverlayResponseBytes; maxOverlayObjects = $MaxOverlayObjects; maxOverlayObjectsPerBar = $MaxOverlayObjectsPerBar; minimumOverlayObjectAllowance = $MinimumOverlayObjectAllowance; barsFirstObservationP95Ms = 500; barsWarmP95Ms = 100; overlayFirstObservationP95Ms = 800; overlayWarmP95Ms = 150; overlayBrowserUxMs = 2000 }
        resourceSafety = @{ concurrency = 1; serverMutation = $false; databaseMutation = $false; processStartStop = $false; matrixTruncation = $false; responseRead = "HttpClient.ResponseHeadersRead"; ps51Fallback = "HttpClient timeout plus cancellation token and bounded streaming; DNS cancellation may follow .NET Framework timing" }
    }
    results = $script:Results; matrix = $script:MatrixResults; measurements = $script:Measurements; summary = $summary
}

if ($WhatIfPreference) {
    Write-Host "WhatIf: planned $expectedCells matrix cells; no GET, tests, browser, server, database, or evidence writes were performed."
    exit 0
}
$published = Publish-EvidenceSet -OutputRoot $OutputRoot -RunId $RunId -Report $report -Markdown (New-MarkdownReport $report) -RunLock $script:RunLock
Write-Host "Evidence: $($published.path)"
$finalCode = if ([int]$summary.fail -gt 0) { 2 } else { 0 }
if ($script:HttpClient) { $script:HttpClient.Dispose(); $script:HttpClient = $null }
if ($script:RunLock) { Exit-EvidenceRunLock $script:RunLock; $script:RunLock = $null }
exit $finalCode
