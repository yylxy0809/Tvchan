param(
    [string]$ContainerName = "",
    [string]$Database = "tradingview_local",
    [string]$User = "trader",
    [string]$Only = ""
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$SqlDir = Join-Path $Root "db/sql"
$Files = @(Get-ChildItem -Path $SqlDir -Filter "*.sql" -File | Sort-Object Name)
if ($Only.Trim().Length -gt 0) {
    $Files = @($Files | Where-Object { $_.Name -eq $Only -or $_.BaseName -eq $Only })
}

if ($ContainerName.Trim().Length -eq 0) {
    $Candidates = @("tv_backend_timescaledb", "tv_local_timescaledb")
    foreach ($Candidate in $Candidates) {
        docker inspect $Candidate *> $null
        if ($LASTEXITCODE -eq 0) {
            $ContainerName = $Candidate
            break
        }
    }
}

if ($ContainerName.Trim().Length -eq 0) {
    throw "No TimescaleDB container found. Pass -ContainerName explicitly."
}

if ($Files.Count -eq 0) {
    if ($Only.Trim().Length -gt 0) {
        throw "No migration file matched -Only '$Only' under: $SqlDir"
    }
    throw "No migration files found under: $SqlDir"
}

foreach ($File in $Files) {
    Write-Host "Applying $($File.FullName)"
    $ContainerFile = "/tmp/tv-migration-$($File.Name)"
    docker cp $File.FullName "${ContainerName}:$ContainerFile"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to copy migration into container: $($File.Name)"
    }
    docker exec $ContainerName psql -U $User -d $Database -v ON_ERROR_STOP=1 -f $ContainerFile
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to apply migration: $($File.Name)"
    }
}
