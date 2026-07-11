param(
    [string]$Provider = "pytdx",
    [string]$Symbols = "",
    [int]$SymbolLimit = 10,
    [int]$Limit = 300,
    [double]$Sleep = 0.25,
    [switch]$Loop,
    [double]$LoopInterval = 60,
    [switch]$SkipPublish,
    [switch]$DryRun,
    [string]$RedisUrl = "redis://127.0.0.1:6379/0",
    [string]$DatabaseUrl = "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$CollectorDir = Join-Path $Root "services/collector"
$ProtocolDir = Join-Path $Root "libs/protocol/python"
$ApiDir = Join-Path $Root "services/api"

$env:PYTHONPATH = "$CollectorDir;$ProtocolDir;$ApiDir"
$env:DATABASE_URL = $DatabaseUrl
$env:REDIS_URL = $RedisUrl

$ArgsList = @(
    "-m", "collector.market_fill",
    "--provider", $Provider,
    "--symbol-limit", $SymbolLimit,
    "--limit", $Limit,
    "--sleep", $Sleep,
    "--redis-url", $RedisUrl,
    "--database-url", $DatabaseUrl
)

if ($Symbols.Trim().Length -gt 0) {
    $ArgsList += @("--symbols", $Symbols)
}
if ($Loop) {
    $ArgsList += @("--loop", "--loop-interval", $LoopInterval)
}
if ($SkipPublish) {
    $ArgsList += "--skip-publish"
}
if ($DryRun) {
    $ArgsList += "--dry-run"
}

Set-Location $CollectorDir
python @ArgsList
