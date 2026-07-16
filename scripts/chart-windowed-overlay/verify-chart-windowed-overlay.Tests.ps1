$corePath = Join-Path $PSScriptRoot "harness-core.ps1"
$mainPath = Join-Path (Split-Path -Parent $PSScriptRoot) "verify-chart-windowed-overlay.ps1"
$scannerPath = Join-Path $PSScriptRoot "source-scan.mjs"
$typescriptPath = Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "apps\web\node_modules\typescript\lib\typescript.js"
if (Test-Path -LiteralPath $corePath) {
    . $corePath
}

function Start-RawMockServer([ValidateSet("body", "timeout", "oversize", "chunkedOversize")][string]$Scenario, [string]$Body = "") {
    $probe = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $probe.Start(); $port = ([Net.IPEndPoint]$probe.LocalEndpoint).Port; $probe.Stop()
    $ready = Join-Path $TestDrive "server-$port.ready"
    $bodyPath = Join-Path $TestDrive "server-$port.body"
    $scriptPath = Join-Path $TestDrive "server-$port.ps1"
    $Body | Set-Content -LiteralPath $bodyPath -Encoding UTF8
    $serverScript = @(
        'param($Port, $Scenario, $BodyPath, $Ready)',
        '$Body = Get-Content -Raw -LiteralPath $BodyPath',
        '$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, [int]$Port)',
        '$listener.Start()',
        'Set-Content -LiteralPath $Ready -Value "ready"',
        'try {',
        '  $client = $listener.AcceptTcpClient()',
        '  try {',
        '    $stream = $client.GetStream()',
        '    $reader = [IO.StreamReader]::new($stream, [Text.Encoding]::ASCII, $false, 1024, $true)',
        '    while (($line = $reader.ReadLine()) -ne $null -and $line -ne "") { }',
        '    if ($Scenario -eq "timeout") { Start-Sleep -Seconds 3; return }',
        '    $bytes = if ($Scenario -eq "oversize" -or $Scenario -eq "chunkedOversize") { [byte[]]::new(4096) } else { [Text.Encoding]::UTF8.GetBytes($Body) }',
        '    $header = if ($Scenario -eq "chunkedOversize") { "HTTP/1.1 200 OK`r`nContent-Type: application/json`r`nTransfer-Encoding: chunked`r`nConnection: close`r`n`r`n" } else { "HTTP/1.1 200 OK`r`nContent-Type: application/json`r`nContent-Length: $($bytes.Length)`r`nConnection: close`r`n`r`n" }',
        '    $headerBytes = [Text.Encoding]::ASCII.GetBytes($header)',
        '    $stream.Write($headerBytes, 0, $headerBytes.Length)',
        '    if ($Scenario -eq "chunkedOversize") {',
        '      $chunkHeader = [Text.Encoding]::ASCII.GetBytes(($bytes.Length.ToString("X") + "`r`n"))',
        '      $stream.Write($chunkHeader, 0, $chunkHeader.Length)',
        '      $stream.Write($bytes, 0, $bytes.Length)',
        '      $chunkEnd = [Text.Encoding]::ASCII.GetBytes("`r`n0`r`n`r`n")',
        '      $stream.Write($chunkEnd, 0, $chunkEnd.Length)',
        '    } else { $stream.Write($bytes, 0, $bytes.Length) }',
        '    $stream.Flush()',
        '  } finally { $client.Dispose() }',
        '} catch { } finally { $listener.Stop() }'
    ) -join [Environment]::NewLine
    $serverScript | Set-Content -LiteralPath $scriptPath -Encoding UTF8
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = (Get-Command powershell.exe).Source
    $serverArguments = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $scriptPath, $port, $Scenario, $bodyPath, $ready) | ForEach-Object { '"' + $_ + '"' }
    $start.Arguments = $serverArguments -join " "
    $start.UseShellExecute = $false; $start.CreateNoWindow = $true
    $process = [Diagnostics.Process]::Start($start)
    $deadline = (Get-Date).AddSeconds(5)
    while (-not (Test-Path -LiteralPath $ready) -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 25 }
    if (-not (Test-Path -LiteralPath $ready)) { throw "mock server did not start" }
    return [pscustomobject]@{ port = $port; process = $process }
}

function Stop-RawMockServer([object]$Server) {
    if ($Server -and $Server.process) { if (-not $Server.process.HasExited) { $Server.process.Kill(); $Server.process.WaitForExit() }; $Server.process.Dispose() }
}

function Invoke-HarnessChild([string[]]$ExtraArguments, [switch]$EnableSourceCheck) {
    $base = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $mainPath,
        "-Symbols", "TEST.SZ", "-SparseSymbol", "", "-Timeframes", "5f",
        "-WindowNames", "cold-first-observation", "-WarmSamples", "2",
        "-MaxMatrixCells", "4", "-ManagerContractTests", "Skip",
        "-SkipResourceCheck"
    )
    if (-not $EnableSourceCheck) { $base += "-SkipSourceCheck" }
    $arguments = @($base + $ExtraArguments) | ForEach-Object { '"' + ([string]$_).Replace('"', '\"') + '"' }
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = (Get-Command powershell.exe).Source
    $start.Arguments = $arguments -join " "
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $process = [Diagnostics.Process]::new(); $process.StartInfo = $start
    [void]$process.Start()
    if (-not $process.WaitForExit(15000)) {
        $process.Kill()
        $process.WaitForExit()
        return [pscustomobject]@{ exitCode = 124; output = @("child process timed out") }
    }
    $output = @(($process.StandardOutput.ReadToEnd() + $process.StandardError.ReadToEnd()) -split "`r?`n" | Where-Object { $_ })
    return [pscustomobject]@{ exitCode = $process.ExitCode; output = $output }
}

function New-ScannerFixture([string]$Root, [bool]$ActiveBundle) {
    $files = @(
        "apps/web/src/api/chartDataManager.ts", "apps/web/src/api/chanOverlayManager.ts",
        "apps/web/src/api/chanRealtimeOverlayBridge.ts", "apps/web/src/api/realtime.ts", "apps/web/src/tradingview/datafeed.ts",
        "apps/web/src/components/ChartWorkspace.tsx"
    )
    foreach ($relative in $files) { New-Item -ItemType Directory -Force -Path (Split-Path -Parent (Join-Path $Root $relative)) | Out-Null }
    $barsPath = if ($ActiveBundle) { "const loader = this.compatBundle; return loader();" } else { "return this.loadBars(request);" }
    @"
class ChartWebSocketClient { handleMessage(message) { return message.chan; } }
class ChartDataManager {
async getBars(request) { $barsPath }
async loadBars(request) { return getBarsHttp(request); }
async subscribeChanOverlay(request) { return this.ws.subscribe(request); }
async compatBundle() { const load = getChartBundleHttp; return load(); }
async getChartWindow(request) { return this.loadChartWindow(request); }
async loadChartWindow(request) { return getChartBundleHttp(request); }
}
"@ | Set-Content -LiteralPath (Join-Path $Root "apps/web/src/api/chartDataManager.ts")
    'export function createDatafeed(manager) { return { getBars(request) { return manager.getBars(request); } }; }' | Set-Content -LiteralPath (Join-Path $Root "apps/web/src/tradingview/datafeed.ts")
    'export class ChanOverlayManager { request(value) { return this.consume(value); } fetchFresh(value) { return value; } applyRealtime(value) { return value; } consume(value) { return value; } }' | Set-Content -LiteralPath (Join-Path $Root "apps/web/src/api/chanOverlayManager.ts")
    'export class ChanRealtimeOverlayBridge { hydrateHttp(value) { return value; } apply(value) { return this.requestResync(value); } requestResync(value) { return value; } }' | Set-Content -LiteralPath (Join-Path $Root "apps/web/src/api/chanRealtimeOverlayBridge.ts")
    'export function ChartWorkspace() { const requestOverlay = () => overlayManager.request({}); requestOverlay(); realtimeBridge.apply({}); }' | Set-Content -LiteralPath (Join-Path $Root "apps/web/src/components/ChartWorkspace.tsx")
    'export function createChartSocket() { return createSocket("/ws/v2/chart"); }' | Set-Content -LiteralPath (Join-Path $Root "apps/web/src/api/realtime.ts")
}

Describe "Task 6 chart windowed overlay harness" {
    It "discovers current semantic roots, permits dormant compatibility, and follows active aliases" {
        $current = @{
            "apps/web/src/api/chartDataManager.ts" = @'
class ChartWebSocketClient { handleMessage(message) { return message.chan; } }
class ChartDataManager {
  getBars(request) { return this.loadBars(request); }
  loadBars(request) { return getBarsHttp(request); }
  subscribeChanOverlay(request) { return this.ws.subscribe(request); }
  getChartWindow(request) { return this.loadChartWindow(request); }
  loadChartWindow(request) { return getChartBundleHttp(request); }
}
'@
            "apps/web/src/api/chanOverlayManager.ts" = 'class ChanOverlayManager { request(value) { return this.consume(value); } fetchFresh(value) { return value; } applyRealtime(value) { return value; } consume(value) { return value; } }'
            "apps/web/src/api/chanRealtimeOverlayBridge.ts" = 'class ChanRealtimeOverlayBridge { hydrateHttp(value) { return value; } apply(value) { return this.requestResync(value); } requestResync(value) { return value; } }'
            "apps/web/src/api/realtime.ts" = 'export function createChartSocket() { return createSocket("/ws/v2/chart"); }'
            "apps/web/src/tradingview/datafeed.ts" = 'function createDatafeed(manager) { return { getBars(request) { return manager.getBars(request); } }; }'
            "apps/web/src/components/ChartWorkspace.tsx" = 'function ChartWorkspace() { const requestOverlay = () => overlayManager.request({}); requestOverlay(); realtimeBridge.apply({}); }'
        }
        $safe = Invoke-TypeScriptBundleScan -Sources $current -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5 -RequireProductionScopes
        $safe.findings.Count | Should Be 0
        $safe.uncertainties.Count | Should Be 0

        $renamed = @{}; foreach ($entry in $current.GetEnumerator()) { $renamed[$entry.Key] = $entry.Value }
        $renamed["apps/web/src/api/chartDataManager.ts"] = $renamed["apps/web/src/api/chartDataManager.ts"].Replace("getBars(request)", "loadHistory(request)")
        $renamedScan = Invoke-TypeScriptBundleScan -Sources $renamed -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5 -RequireProductionScopes
        $renamedScan.findings.Count | Should Be 0
        ($renamedScan.uncertainties.reason -join " ") | Should Match "ChartDataManager.getBars"

        $active = @{}; foreach ($entry in $current.GetEnumerator()) { $active[$entry.Key] = $entry.Value }
        $active["apps/web/src/api/chartDataManager.ts"] = $active["apps/web/src/api/chartDataManager.ts"].Replace(
            "getBars(request) { return this.loadBars(request); }",
            "getBars(request) { const run = this.compatBundle; return run(request); } compatBundle(request) { const loader = getChartBundleHttp; return loader(request); }"
        )
        $activeScan = Invoke-TypeScriptBundleScan -Sources $active -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5 -RequireProductionScopes
        $activeScan.findings.Count | Should BeGreaterThan 0
        $activeScan.uncertainties.Count | Should Be 0

        $duplicateVariants = @(
            @{ path = "apps/web/src/api/chartDataManager.ts"; suffix = ' class ChartDataManager { getBars(request) { return getChartBundleHttp(request); } }' },
            @{ path = "apps/web/src/tradingview/datafeed.ts"; suffix = ' function createDatafeed(manager) { return getChartBundleHttp(manager); }' },
            @{ path = "apps/web/src/tradingview/datafeed.ts"; suffix = ' namespace UnsafeModule { export function createDatafeed(manager) { return getChartBundleHttp(manager); } }' }
        )
        foreach ($variant in $duplicateVariants) {
            $duplicate = @{}; foreach ($entry in $current.GetEnumerator()) { $duplicate[$entry.Key] = $entry.Value }
            $duplicate[$variant.path] += $variant.suffix
            $duplicateScan = Invoke-TypeScriptBundleScan -Sources $duplicate -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5 -RequireProductionScopes
            ($duplicateScan.uncertainties.reason -join " ") | Should Match "ambiguous"
        }

        $invocationBodies = @(
            'getBars(request) { return this.compatBundle.call(this, request); } compatBundle(request) { return getChartBundleHttp.call(null, request); }',
            'getBars(request) { const run = this.compatBundle; return run.apply(this, [request]); } compatBundle(request) { const loader = getChartBundleHttp; return loader(request); }',
            'getBars(request) { const first = this.compatBundle; const second = first.bind(this); const third = second; return third(request); } compatBundle(request) { const loader = getChartBundleHttp; return loader(request); }'
        )
        foreach ($body in $invocationBodies) {
            $invoked = @{}; foreach ($entry in $current.GetEnumerator()) { $invoked[$entry.Key] = $entry.Value }
            $invoked["apps/web/src/api/chartDataManager.ts"] = $invoked["apps/web/src/api/chartDataManager.ts"].Replace("getBars(request) { return this.loadBars(request); }", $body)
            $invokedScan = Invoke-TypeScriptBundleScan -Sources $invoked -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5 -RequireProductionScopes
            $invokedScan.findings.Count | Should BeGreaterThan 0
            $invokedScan.uncertainties.Count | Should Be 0
        }

        $dynamicReceiver = @{}; foreach ($entry in $current.GetEnumerator()) { $dynamicReceiver[$entry.Key] = $entry.Value }
        $dynamicReceiver["apps/web/src/api/chartDataManager.ts"] = $dynamicReceiver["apps/web/src/api/chartDataManager.ts"].Replace(
            "getBars(request) { return this.loadBars(request); }",
            "getBars(request, method) { return this[method].call(this, request); }"
        )
        $dynamicReceiverScan = Invoke-TypeScriptBundleScan -Sources $dynamicReceiver -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5 -RequireProductionScopes
        $dynamicReceiverScan.findings.Count | Should Be 0
        ($dynamicReceiverScan.uncertainties.reason -join " ") | Should Match "dynamic computed call receiver"
    }

    It "ignores compatibility definitions but detects active bars overlay and realtime bundle use" {
        $legacyOnly = @{
            "chartDataManager.ts" = @'
class ChartDataManager {
async getChartWindow(request) { return this.loadChartWindow(request); }
private async loadChartWindow(request) { return getChartBundleHttp(request); }
private async loadChartWindowViaWebSocket(request) { return this.ws.request({ type: "get_chart_bundle" }); }
}
'@
            "client.ts" = "export async function getChartBundle() { return '/api/v3/chart/bundle'; }"
        }
        (Invoke-TypeScriptBundleScan -Sources $legacyOnly -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5).findings.Count | Should Be 0

        $active = @{
            "chartDataManager.ts" = @'
class ChartDataManager {
async getBars(request) { return getChartBundleHttp(request); }
async getChanOverlay(request) { return this.getChartWindow(request); }
private handleChanSnapshotMessage(message) { return message.bundle; }
async getChartWindow(request) { return getChartBundleHttp(request); }
}
'@
        }
        $findings = @(Invoke-TypeScriptBundleScan -Sources $active -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5).findings
        $findings.Count | Should Be 3
        ($findings.scope -join ",") | Should Match "getBars"
        ($findings.scope -join ",") | Should Match "getChanOverlay"
        ($findings.scope -join ",") | Should Match "handleChanSnapshotMessage"
    }

    It "ignores comments and literal text, handles layout, and fails closed on interpolated templates" {
        $safe = @{
            "datafeed.ts" = @'
// getChartBundle(); message.bundle
const note = "getChartWindow /api/v3/chart/bundle";
const template = `get_chart_bundle`;
const matcher = /getChartBundle|message\.bundle/;
const localWrapper = () => client.request("/api/v3/chart/bars");
localWrapper();
function getBars(
  request
) {
  return chartDataManager.getBars(request);
}
'@
        }
        $safeScan = Invoke-TypeScriptBundleScan -Sources $safe -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5
        $safeScan.findings.Count | Should Be 0
        $safeScan.uncertainties.Count | Should Be 0

        $activeLayout = @{ "datafeed.ts" = "function getBars(request) {`n return chartDataManager`n  .getChartWindow(request);`n}" }
        (Invoke-TypeScriptBundleScan -Sources $activeLayout -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5).findings.Count | Should Be 1

        $astCases = @{
            "datafeed.ts" = @'
function getBars({ client: { getChartWindow } }) {
  const version = "v3";
  getChartWindow();
  fetch(`/api/${version}/chart/bundle`);
  request({ type: "get_chart_bundle" });
}
'@
        }
        (Invoke-TypeScriptBundleScan -Sources $astCases -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5).findings.Count | Should Be 3

        $aliasCases = @{
            "datafeed.ts" = @'
function getBars(
  { api: { getChartWindow: destructuredLoader } },
  api,
  client
) {
  const base = "/api/" + "v3";
  const suffix = `/chart/bundle`;
  const endpoint = base + suffix;
  const endpointAlias = endpoint;
  const loader = api.getChartWindow;
  const loaderAlias = loader;
  const send = client.request;
  destructuredLoader();
  loaderAlias();
  send(endpointAlias);
  client.get(endpoint);
  // const commented = api.getChartBundle; commented();
  const inert = "api.getChartWindow /api/v3/chart/bundle";
}
'@
        }
        $aliasScan = Invoke-TypeScriptBundleScan -Sources $aliasCases -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5
        $aliasScan.findings.Count | Should Be 4
        $aliasScan.uncertainties.Count | Should Be 0

        $staticTemplate = @{ "datafeed.ts" = 'function getBars(){ const version="v3"; const endpoint=`/api/${version}/chart/bundle`; fetch(endpoint); }' }
        (Invoke-TypeScriptBundleScan -Sources $staticTemplate -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5).findings.Count | Should Be 1

        $dynamic = @{ "datafeed.ts" = 'function getBars(client, version, method){ const endpoint=`/api/${version}/chart/bundle`; const send=client[method]; send(endpoint); }' }
        $dynamicScan = Invoke-TypeScriptBundleScan -Sources $dynamic -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5
        $dynamicScan.findings.Count | Should Be 0
        $dynamicScan.uncertainties.Count | Should BeGreaterThan 0

        $conditionalDynamic = @{ "datafeed.ts" = 'function getBars(api, alternate, flag){ const loader=flag ? api.getChartWindow : alternate; loader(); }' }
        $conditionalScan = Invoke-TypeScriptBundleScan -Sources $conditionalDynamic -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5
        $conditionalScan.findings.Count | Should Be 0
        $conditionalScan.uncertainties.Count | Should BeGreaterThan 0

        $syntax = @{ "datafeed.ts" = "function getBars( {" }
        (Invoke-TypeScriptBundleScan -Sources $syntax -NodePath (Get-Command node).Source -TypeScriptPath $typescriptPath -ScannerPath $scannerPath -TimeoutSeconds 5).uncertainties.Count | Should BeGreaterThan 0
        (Invoke-TypeScriptBundleScan -Sources $safe -NodePath (Get-Command node).Source -TypeScriptPath (Join-Path $TestDrive "missing-typescript.js") -ScannerPath $scannerPath -TimeoutSeconds 1).uncertainties.Count | Should BeGreaterThan 0
    }

    It "rejects failed or malformed probes before latency is accepted" {
        $failed = [pscustomobject]@{ statusCode = 500; elapsedMs = 0; body = $null; bytes = 0; text = "failed" }
        $failedResult = Test-ProbeResponse -Probe $failed -Kind bars -ExpectedLevels @()
        $failedResult.valid | Should Be $false
        $failedResult.recordLatency | Should Be $false

        $duplicateBars = [pscustomobject]@{
            statusCode = 200; elapsedMs = 12; bytes = 100; text = ""
            body = [pscustomobject]@{ bars = @([pscustomobject]@{ time = 10 }, [pscustomobject]@{ time = 10 }) }
        }
        (Test-ProbeResponse -Probe $duplicateBars -Kind bars -From 1 -To 20 -ExpectedLevels @()).valid | Should Be $false

        $wrongOverlay = [pscustomobject]@{
            statusCode = 200; elapsedMs = 9; bytes = 100; text = ""
            body = [pscustomobject]@{ snapshot_version = "v1"; levels = @("5f"); strokes = @(); segments = @(); centers = @(); signals = @() }
        }
        (Test-ProbeResponse -Probe $wrongOverlay -Kind overlay -From 1 -To 20 -ExpectedLevels @("5f", "30f", "1d")).valid | Should Be $false

        $oversizedBars = [pscustomobject]@{ statusCode = 200; elapsedMs = 0; bytes = 500; body = [pscustomobject]@{ bars = @([pscustomobject]@{ time = 10 }) } }
        (Test-BarsPayload -Probe $oversizedBars -BarLimit 1 -MaxBytes 100).valid | Should Be $false
    }

    It "builds every configured symbol timeframe and window combination" {
        $matrix = @(New-ProbeMatrix -Symbols @("A.SZ", "B.SH") -Timeframes @("5f", "1d") -WindowNames @("cold-probe", "shifted-overlap"))
        $matrix.Count | Should Be 8
        @($matrix | Select-Object -ExpandProperty key -Unique).Count | Should Be 8
    }

    It "enforces payload proportionality and line continuity context" {
        $valid = [pscustomobject]@{
            snapshot_version = "v1"; levels = @("5f"); modes = @("confirmed")
            strokes = @(
                [pscustomobject]@{ id = "pre"; level = "5f"; mode = "confirmed"; begin_base_ts = 1; end_base_ts = 5 },
                [pscustomobject]@{ id = "in"; level = "5f"; mode = "confirmed"; begin_base_ts = 9; end_base_ts = 12 },
                [pscustomobject]@{ id = "post"; level = "5f"; mode = "confirmed"; begin_base_ts = 21; end_base_ts = 25 }
            )
            segments = @(); centers = @([pscustomobject]@{ id = "c"; level = "5f"; mode = "confirmed"; begin_base_ts = 8; end_base_ts = 11 })
            signals = @([pscustomobject]@{ id = "s"; level = "5f"; mode = "confirmed"; base_ts = 15 })
        }
        $limits = @{ MaxBytes = 10000; MaxObjects = 20; MaxObjectsPerBar = 2.0; MinimumObjectAllowance = 4 }
        (Test-OverlayPayload -Body $valid -Bytes 800 -From 10 -To 20 -BarCount 10 -Limits $limits).valid | Should Be $true

        $invalid = $valid | ConvertTo-Json -Depth 10 | ConvertFrom-Json
        $invalid.strokes = @($invalid.strokes) + [pscustomobject]@{ id = "pre2"; level = "5f"; mode = "confirmed"; begin_base_ts = 2; end_base_ts = 6 }
        $bad = Test-OverlayPayload -Body $invalid -Bytes 800 -From 10 -To 20 -BarCount 10 -Limits $limits
        $bad.valid | Should Be $false
        ($bad.failures -join " ") | Should Match "predecessor"
    }

    It "promotes JSON Markdown and manifest as one idempotent recoverable set" {
        $report = [ordered]@{ runId = "run-1"; summary = @{ fail = 0 } }
        $first = Publish-EvidenceSet -OutputRoot $TestDrive -RunId "run-1" -Report $report -Markdown "# report"
        $first.idempotent | Should Be $false
        Test-Path (Join-Path $first.path "report.json") | Should Be $true
        Test-Path (Join-Path $first.path "report.md") | Should Be $true
        Test-Path (Join-Path $first.path "manifest.json") | Should Be $true
        Test-Path (Join-Path $TestDrive ".run-1.staging") | Should Be $false

        $second = Publish-EvidenceSet -OutputRoot $TestDrive -RunId "run-1" -Report $report -Markdown "# report"
        $second.idempotent | Should Be $true

        $partialStage = Join-Path $TestDrive ".run-2.staging"
        New-Item -ItemType Directory -Force -Path $partialStage | Out-Null
        '{"partial":true}' | Set-Content -LiteralPath (Join-Path $partialStage "report.json") -Encoding UTF8
        $recovered = Publish-EvidenceSet -OutputRoot $TestDrive -RunId "run-2" -Report ([ordered]@{ runId = "run-2" }) -Markdown "# recovered"
        $recovered.recovered | Should Be $true
        Test-Path (Join-Path $recovered.path "manifest.json") | Should Be $true
        Test-Path $partialStage | Should Be $false
    }

    It "validates RunId descendants locks timestamps and Markdown escaping" {
        (Test-SafeRunId "run_1.2-x") | Should Be $true
        (Test-SafeRunId "../escape") | Should Be $false
        (Test-SafeRunId "...") | Should Be $false
        { Resolve-VerifiedDescendantPath -Root $TestDrive -Child "..\escape" } | Should Throw

        $lock = Enter-EvidenceRunLock -OutputRoot $TestDrive -RunId "locked-run"
        try { { Enter-EvidenceRunLock -OutputRoot $TestDrive -RunId "locked-run" } | Should Throw }
        finally { Exit-EvidenceRunLock $lock }

        $parsed = 0L
        (Try-ParseEpochSeconds "bad" ([ref]$parsed)) | Should Be $false
        (Try-ParseEpochSeconds "10" ([ref]$parsed)) | Should Be $true
        $parsed | Should Be 10
        $encoded = ConvertTo-MarkdownCell '![x](https://evil.example/a)<img src=x> ` [ ] ( ) mailto:a@b'
        $encoded | Should Be '<code>&#33;&#91;x&#93;&#40;https&#58;&#47;&#47;evil.example&#47;a&#41;&lt;img src=x&gt; &#96; &#91; &#93; &#40; &#41; mailto&#58;a@b</code>'
        $encoded | Should Not Match '!\[|\]\(|https:|mailto:|<img'
    }
}

Describe "Task 6 full-script failures" {
    It "records invalid timestamps and recovers partial evidence with exit 2" {
        $outputRoot = Join-Path $TestDrive "invalid-time"
        $runId = "invalid-time"
        $stage = Join-Path $outputRoot ".$runId.staging"
        New-Item -ItemType Directory -Force -Path $stage | Out-Null
        "partial" | Set-Content -LiteralPath (Join-Path $stage "report.json")
        $server = Start-RawMockServer body '{"symbol":"TEST.SZ","timeframe":"5f","bars":[{"time":"bad"}]}'
        try {
            $run = Invoke-HarnessChild @("-ApiBaseUrl", "http://127.0.0.1:$($server.port)", "-OutputRoot", $outputRoot, "-RunId", $runId, "-RequestTimeoutSeconds", "2")
            $run.exitCode | Should Be 2
            $report = Get-Content -Raw (Join-Path $outputRoot "$runId\report.json") | ConvertFrom-Json
            ($report.matrix.message -join " ") | Should Match "timestamp"
            Test-Path (Join-Path $outputRoot "$runId\manifest.json") | Should Be $true
            Test-Path $stage | Should Be $false
        }
        finally { Stop-RawMockServer $server }
    }

    It "records timeout and oversize failures with exit 2" {
        foreach ($scenario in @("timeout", "oversize", "chunkedOversize")) {
            $outputRoot = Join-Path $TestDrive $scenario
            $server = Start-RawMockServer $scenario
            try {
                $run = Invoke-HarnessChild @("-ApiBaseUrl", "http://127.0.0.1:$($server.port)", "-OutputRoot", $outputRoot, "-RunId", $scenario, "-RequestTimeoutSeconds", "1", "-MaxBarsResponseBytes", "1024")
                $run.exitCode | Should Be 2
                $report = Get-Content -Raw (Join-Path $outputRoot "$scenario\report.json") | ConvertFrom-Json
                ($report.matrix.message -join " ") | Should Match $(if ($scenario -eq "timeout") { "timeout" } else { "too-large|size" })
                if ($scenario -eq "chunkedOversize") { $report.matrix.transport | Should Match "ps51-fallback" }
            }
            finally { Stop-RawMockServer $server }
        }
    }

    It "returns exit 2 for traversal concurrent RunId and an empty matrix" {
        (Invoke-HarnessChild @("-OutputRoot", (Join-Path $TestDrive "traversal"), "-RunId", "..\escape", "-WhatIf")).exitCode | Should Be 2

        $lockedRoot = Join-Path $TestDrive "concurrent"
        $lock = Enter-EvidenceRunLock -OutputRoot $lockedRoot -RunId "same-run"
        try { (Invoke-HarnessChild @("-OutputRoot", $lockedRoot, "-RunId", "same-run")).exitCode | Should Be 2 }
        finally { Exit-EvidenceRunLock $lock }

        (Invoke-HarnessChild @("-NoDefaultSymbols", "-NoSupplementalSymbols", "-OutputRoot", (Join-Path $TestDrive "empty"), "-RunId", "empty", "-WhatIf")).exitCode | Should Be 2
    }

    It "runs scanner false-positive and false-negative behavior through the full script" {
        foreach ($active in @($false, $true)) {
            $label = if ($active) { "scanner-active" } else { "scanner-safe" }
            $sourceRoot = Join-Path $TestDrive "$label-source"; $outputRoot = Join-Path $TestDrive "$label-output"
            New-ScannerFixture $sourceRoot $active
            $server = Start-RawMockServer body '{"symbol":"TEST.SZ","timeframe":"5f","bars":[{"time":"bad"}]}'
            try {
                $run = Invoke-HarnessChild -ExtraArguments @("-ApiBaseUrl", "http://127.0.0.1:$($server.port)", "-SourceRoot", $sourceRoot, "-OutputRoot", $outputRoot, "-RunId", $label, "-RequestTimeoutSeconds", "2") -EnableSourceCheck
                $run.exitCode | Should Be 2
                $report = Get-Content -Raw (Join-Path $outputRoot "$label\report.json") | ConvertFrom-Json
                $gate = @($report.results | Where-Object name -eq "normal-path-no-bundle")[0]
                $gate.status | Should Be $(if ($active) { "FAIL" } else { "PASS" })
            }
            finally { Stop-RawMockServer $server }
        }
    }

    It "reads UTF-8 TypeScript sources correctly in Windows PowerShell 5" {
        $sourceRoot = Join-Path $TestDrive "scanner-utf8-source"
        $outputRoot = Join-Path $TestDrive "scanner-utf8-output"
        New-ScannerFixture $sourceRoot $false
        $workspacePath = Join-Path $sourceRoot "apps/web/src/components/ChartWorkspace.tsx"
        $title = -join @([char]0x5207, [char]0x6362, [char]0x5230, [char]0x767D, [char]0x8272, [char]0x4E3B, [char]0x9898)
        $workspaceSource = 'export function ChartWorkspace() { const title = "' + $title + '"; const requestOverlay = () => overlayManager.request({ title }); requestOverlay(); realtimeBridge.apply({}); }'
        [IO.File]::WriteAllText($workspacePath, $workspaceSource, [Text.UTF8Encoding]::new($false))
        $server = Start-RawMockServer body '{"symbol":"TEST.SZ","timeframe":"5f","bars":[{"time":"bad"}]}'
        try {
            $run = Invoke-HarnessChild -ExtraArguments @("-ApiBaseUrl", "http://127.0.0.1:$($server.port)", "-SourceRoot", $sourceRoot, "-OutputRoot", $outputRoot, "-RunId", "scanner-utf8", "-RequestTimeoutSeconds", "2") -EnableSourceCheck
            $run.exitCode | Should Be 2
            $report = Get-Content -Raw (Join-Path $outputRoot "scanner-utf8\report.json") | ConvertFrom-Json
            $gate = @($report.results | Where-Object name -eq "normal-path-no-bundle")[0]
            $gate.status | Should Be "PASS"
            $gate.evidence.uncertainties.Count | Should Be 0
        }
        finally { Stop-RawMockServer $server }
    }
}
