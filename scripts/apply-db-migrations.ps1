param(
    [string]$ContainerName = "tv_local_timescaledb",
    [string]$Database = "tradingview_local",
    [string]$User = "trader"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$SqlDir = Join-Path $Root "db/sql"
$Files = @(
    (Join-Path $SqlDir "001_init.sql"),
    (Join-Path $SqlDir "002_chan.sql"),
    (Join-Path $SqlDir "003_history_backfill.sql"),
    (Join-Path $SqlDir "004_chan_recompute.sql"),
    (Join-Path $SqlDir "005_tdx_csv_import.sql"),
    (Join-Path $SqlDir "006_symbol_identity.sql"),
    (Join-Path $SqlDir "007_kline_source_priority.sql")
)

foreach ($File in $Files) {
    if (-not (Test-Path $File)) {
        throw "Missing migration file: $File"
    }
    Write-Host "Applying $File"
    Get-Content -Path $File | docker exec -i $ContainerName psql -U $User -d $Database
}
