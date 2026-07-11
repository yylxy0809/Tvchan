param(
    [int]$ShardIndex = 0,
    [int]$ShardCount = 1,
    [int]$TaskLimit = 100,
    [int]$DiscoveryLimit = 500,
    [int]$Concurrency = 1,
    [int]$TailBarLimit = 2000,
    [int]$ContextBars = 64,
    [string]$ChanLevels = "5f,30f,1d,1w,1m",
    [string]$Modes = "confirmed,predictive",
    [switch]$Loop,
    [double]$LoopInterval = 5,
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
$env:PYTHONIOENCODING = "utf-8"
$env:DATABASE_URL = $DatabaseUrl
$env:REDIS_URL = $RedisUrl

Push-Location $CollectorDir
try {
    $argsList = @(
        "-m", "collector.worker",
        "chan-c-stream",
        "--chan-levels", $ChanLevels,
        "--modes", $Modes,
        "--task-limit", $TaskLimit,
        "--discovery-limit", $DiscoveryLimit,
        "--concurrency", $Concurrency,
        "--tail-bar-limit", $TailBarLimit,
        "--context-bars", $ContextBars,
        "--shard-index", $ShardIndex,
        "--shard-count", $ShardCount,
        "--loop-interval", $LoopInterval,
        "--redis-url", $RedisUrl,
        "--database-url", $DatabaseUrl
    )
    if ($Loop) {
        $argsList += "--loop"
    }
    if ($DryRun) {
        $argsList += "--dry-run"
    }
    python $argsList
}
finally {
    Pop-Location
}
