param(
    [string]$Symbols = "",
    [int]$SymbolLimit = 10,
    [string]$BaseTimeframe = "5f",
    [string]$ChanLevels = "5f,30f,1d",
    [string]$Modes = "confirmed,predictive",
    [int]$TaskLimit = 3,
    [int]$Concurrency = 1,
    [double]$Sleep = 0.1,
    [double]$ChanTimeout = 120,
    [switch]$Loop,
    [double]$LoopInterval = 30,
    [switch]$Reset,
    [switch]$ResetRunning,
    [switch]$DryRun,
    [string]$ChanServiceUrl = "http://127.0.0.1:8002",
    [string]$ChanPyPath = "",
    [string]$DatabaseUrl = "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$CollectorDir = Join-Path $Root "services/collector"
$ProtocolDir = Join-Path $Root "libs/protocol/python"
$ApiDir = Join-Path $Root "services/api"

$env:PYTHONPATH = "$CollectorDir;$ProtocolDir;$ApiDir"
$env:DATABASE_URL = $DatabaseUrl
$env:CHAN_SERVICE_URL = $ChanServiceUrl
$env:CHAN_ANALYZE_TIMEOUT = [string]$ChanTimeout
$env:CHAN_ENGINE_MODE = "module_b"

if ($ChanPyPath.Trim().Length -eq 0) {
    $ChanPyPath = Join-Path $Root "work/vendor/chan.py-main"
}

$ArgsList = @(
    "-m", "collector.worker",
    "chan-recompute",
    "--symbol-limit", $SymbolLimit,
    "--base-timeframe", $BaseTimeframe,
    "--chan-levels", $ChanLevels,
    "--modes", $Modes,
    "--task-limit", $TaskLimit,
    "--concurrency", $Concurrency,
    "--sleep", $Sleep,
    "--chan-timeout", $ChanTimeout,
    "--chan-py-path", $ChanPyPath,
    "--chan-service-url", $ChanServiceUrl,
    "--database-url", $DatabaseUrl
)

if ($Symbols.Trim().Length -gt 0) {
    $ArgsList += @("--symbols", $Symbols)
}
if ($Loop) {
    $ArgsList += @("--loop", "--loop-interval", $LoopInterval)
}
if ($Reset) {
    $ArgsList += "--reset"
}
if ($ResetRunning) {
    $ArgsList += "--reset-running"
}
if ($DryRun) {
    $ArgsList += "--dry-run"
}

Set-Location $CollectorDir
python @ArgsList
