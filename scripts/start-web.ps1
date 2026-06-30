param(
    [int]$Port = 5173,
    [string]$HostName = "127.0.0.1",
    [string]$ApiBaseUrl = "http://127.0.0.1:8001",
    [string]$ApiToken = "dev-local-token"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$WebDir = Join-Path $Root "apps/web"

$env:VITE_API_BASE_URL = $ApiBaseUrl
$env:VITE_API_TOKEN = $ApiToken

Set-Location $WebDir
npm run dev -- --host $HostName --port $Port
