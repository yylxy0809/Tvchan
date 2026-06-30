param(
    [string]$Root = "",
    [int]$TaskLimit = 1,
    [int]$Concurrency = 1,
    [int]$WriteConcurrency = 1,
    [int]$BatchSize = 50000,
    [switch]$Reset,
    [switch]$ResetRunning,
    [switch]$Loop,
    [double]$LoopInterval = 300,
    [switch]$DryRun,
    [string]$DatabaseUrl = "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$CollectorDir = Join-Path $RepoRoot "services/collector"
$ProtocolDir = Join-Path $RepoRoot "libs/protocol/python"
$ApiDir = Join-Path $RepoRoot "services/api"

if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = "D:\5f$([char]0x6570)$([char]0x636e)\5m_price"
}

$env:PYTHONPATH = "$CollectorDir;$ProtocolDir;$ApiDir"
$env:DATABASE_URL = $DatabaseUrl
$env:PARQUET_5F_ROOT = $Root

$ArgsList = @(
    "-m", "collector.parquet_bootstrap_import",
    "--root", $Root,
    "--task-limit", $TaskLimit,
    "--concurrency", $Concurrency,
    "--write-concurrency", $WriteConcurrency,
    "--batch-size", $BatchSize,
    "--loop-interval", $LoopInterval,
    "--database-url", $DatabaseUrl
)

if ($Reset) {
    $ArgsList += "--reset"
}
if ($ResetRunning) {
    $ArgsList += "--reset-running"
}
if ($Loop) {
    $ArgsList += "--loop"
}
if ($DryRun) {
    $ArgsList += "--dry-run"
}

Set-Location $CollectorDir
python @ArgsList
