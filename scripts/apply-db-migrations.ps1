param(
    [string]$ContainerName = "tv_local_timescaledb",
    [string]$Database = "tradingview_local",
    [string]$User = "trader"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$SqlDir = Join-Path $Root "db/sql"
$Files = @(Get-ChildItem -Path $SqlDir -Filter "*.sql" -File | Sort-Object Name)

if ($Files.Count -eq 0) {
    throw "No migration files found under: $SqlDir"
}

foreach ($File in $Files) {
    Write-Host "Applying $($File.FullName)"
    Get-Content -Path $File.FullName | docker exec -i $ContainerName psql -U $User -d $Database
}
