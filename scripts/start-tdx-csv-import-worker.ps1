param(
    [string]$Root = "D:\BaiduNetdiskDownload\tdx数据",
    [string]$Timeframes = "5f",
    [string]$Symbols = "",
    [int]$SymbolLimit = 0,
    [string]$Categories = "1",
    [string]$AssetTypes = "stock",
    [string]$Fq = "0",
    [int]$TaskLimit = 1,
    [int]$Concurrency = 1,
    [int]$EntryBatchSize = 20,
    [int]$BarBatchSize = 20000,
    [int]$MaxEntriesPerTask = 0,
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

$env:PYTHONPATH = "$CollectorDir;$ProtocolDir;$ApiDir"
$env:DATABASE_URL = $DatabaseUrl

$ArgsList = @(
    "-m", "collector.tdx_csv_import",
    "--root", $Root,
    "--timeframes", $Timeframes,
    "--symbol-limit", $SymbolLimit,
    "--categories", $Categories,
    "--asset-types", $AssetTypes,
    "--fq", $Fq,
    "--task-limit", $TaskLimit,
    "--concurrency", $Concurrency,
    "--entry-batch-size", $EntryBatchSize,
    "--bar-batch-size", $BarBatchSize,
    "--max-entries-per-task", $MaxEntriesPerTask,
    "--loop-interval", $LoopInterval,
    "--database-url", $DatabaseUrl
)

if ($Symbols.Trim().Length -gt 0) {
    $ArgsList += @("--symbols", $Symbols)
}
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
