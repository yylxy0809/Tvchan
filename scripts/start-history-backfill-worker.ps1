param(
    [string]$Provider = "pytdx",
    [string]$Symbols = "",
    [int]$SymbolLimit = 1,
    [string]$Timeframes = "5f,15f,30f,1h,1d,1w,1m",
    [int]$PageSize = 800,
    [int]$TaskLimit = 3,
    [int]$Concurrency = 1,
    [int]$MaxPagesPerTask = 1,
    [double]$Sleep = 0.25,
    [switch]$Loop,
    [double]$LoopInterval = 30,
    [switch]$Reset,
    [switch]$ResetRunning,
    [switch]$DryRun,
    [string]$TdxHost = "",
    [int]$TdxPort = 7709,
    [int]$TdxTimeout = 10,
    [int]$TdxRetries = 1,
    [string]$DatabaseUrl = "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$CollectorDir = Join-Path $Root "services/collector"
$ProtocolDir = Join-Path $Root "libs/protocol/python"
$ApiDir = Join-Path $Root "services/api"

$env:PYTHONPATH = "$CollectorDir;$ProtocolDir;$ApiDir"
$env:DATABASE_URL = $DatabaseUrl

$ArgsList = @(
    "-m", "collector.history_backfill",
    "--provider", $Provider,
    "--symbol-limit", $SymbolLimit,
    "--timeframes", $Timeframes,
    "--page-size", $PageSize,
    "--task-limit", $TaskLimit,
    "--concurrency", $Concurrency,
    "--max-pages-per-task", $MaxPagesPerTask,
    "--sleep", $Sleep,
    "--tdx-port", $TdxPort,
    "--tdx-timeout", $TdxTimeout,
    "--tdx-retries", $TdxRetries,
    "--database-url", $DatabaseUrl
)

if ($Symbols.Trim().Length -gt 0) {
    $ArgsList += @("--symbols", $Symbols)
}
if ($TdxHost.Trim().Length -gt 0) {
    $ArgsList += @("--tdx-host", $TdxHost)
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
