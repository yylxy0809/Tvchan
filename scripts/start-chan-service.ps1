param(
    [int]$Port = 8002,
    [string]$HostName = "127.0.0.1",
    [string]$EngineMode = "module_b"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$ServiceDir = Join-Path $Root "services/chan-service"
$ProtocolDir = Join-Path $Root "libs/protocol/python"
$LegacySchemeName = [string]([char]0x65E7) + [char]0x7248 + [char]0x65B9 + [char]0x6848
$LegacyBackendDir = Join-Path $Root "$LegacySchemeName/backend"

$env:PYTHONPATH = "$ServiceDir;$ProtocolDir"
$env:CHAN_ENGINE_MODE = $EngineMode
$env:CHAN_LEGACY_SCHEME_PATH = $LegacyBackendDir
$env:CHAN_PY_PATH = Join-Path $Root "work/vendor/chan.py-main"

Set-Location $ServiceDir
python -m uvicorn chan_service.main:app --host $HostName --port $Port
