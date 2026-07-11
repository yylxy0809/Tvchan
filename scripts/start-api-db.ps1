param(
    [int]$Port = 8001,
    [string]$HostName = "127.0.0.1",
    [string]$RedisUrl = "redis://127.0.0.1:6379/0"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$ApiDir = Join-Path $Root "services/api"
$ProtocolDir = Join-Path $Root "libs/protocol/python"

$env:USE_SEED_DATA = "false"
if (-not $env:DATABASE_URL) {
    $env:DATABASE_URL = "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local"
}
$env:REDIS_URL = $RedisUrl
$env:API_TOKEN = "dev-local-token"
$env:PYTHONPATH = $ProtocolDir

Set-Location $ApiDir
python -m uvicorn app.main:app --host $HostName --port $Port
