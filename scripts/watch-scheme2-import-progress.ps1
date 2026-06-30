param(
    [string]$DbContainer = "tv_backend_timescaledb",
    [string]$PostgresUser = "trader",
    [string]$PostgresDb = "tradingview_local",
    [int]$PollSeconds = 60,
    [string]$SourceProfile = "parquet_5f",
    [string]$LogPath = "",
    [string]$MinFreeDrive = "D",
    [double]$MinFreeGb = 10,
    [switch]$StopImportOnLowDisk
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogDir = Join-Path $RepoRoot "logs\scheme2-import"
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    $LogPath = Join-Path $LogDir "import-progress.log"
}

function Invoke-ImportStateQuery {
    $sql = @"
select
    count(*) filter (where status = 'pending') as pending,
    count(*) filter (where status = 'running') as running,
    count(*) filter (where status = 'failed') as failed,
    count(*) filter (where status = 'success') as success,
    count(*) as total,
    coalesce(sum(imported_rows) filter (where status = 'success'), 0) as imported_rows,
    coalesce(to_char(max(updated_at), 'YYYY-MM-DD HH24:MI:SS'), '') as last_updated
from scheme2_source_member_checkpoints
where source_profile = '$SourceProfile';
"@
    $output = & docker exec $DbContainer psql `
        -U $PostgresUser `
        -d $PostgresDb `
        -X `
        -t `
        -A `
        -F "," `
        -v ON_ERROR_STOP=1 `
        -c $sql 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "psql failed: $($output -join ' ')"
    }
    return @($output | Where-Object { $_.Trim().Length -gt 0 })[0]
}

function Get-FreeGb {
    param([string]$DriveName)
    $drive = Get-PSDrive -Name $DriveName -ErrorAction SilentlyContinue
    if ($null -eq $drive) {
        return ""
    }
    return [math]::Round($drive.Free / 1GB, 2)
}

while ($true) {
    $now = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    try {
        $state = Invoke-ImportStateQuery
        $parts = $state.Split(",")
        $pending = [int64]$parts[0]
        $running = [int64]$parts[1]
        $failed = [int64]$parts[2]
        $success = [int64]$parts[3]
        $total = [int64]$parts[4]
        $rows = [int64]$parts[5]
        $lastUpdated = $parts[6]
        $cFree = Get-FreeGb "C"
        $dFree = Get-FreeGb "D"
        $line = "$now pending=$pending running=$running failed=$failed success=$success total=$total imported_rows=$rows last_updated=$lastUpdated C_free_gb=$cFree D_free_gb=$dFree"
        Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
        Write-Output $line
        if ($StopImportOnLowDisk) {
            $guardFree = Get-FreeGb $MinFreeDrive
            if ($guardFree -ne "" -and [double]$guardFree -lt $MinFreeGb) {
                $guardLine = "$now low_disk_guard drive=$MinFreeDrive free_gb=$guardFree min_free_gb=$MinFreeGb stopping_import=true"
                Add-Content -LiteralPath $LogPath -Value $guardLine -Encoding UTF8
                Write-Output $guardLine
                Get-CimInstance Win32_Process |
                    Where-Object { $_.CommandLine -match "collector.parquet_bootstrap_import|start-parquet-5f-import-worker" } |
                    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
                break
            }
        }
        if (($pending + $running + $failed) -eq 0) {
            Add-Content -LiteralPath $LogPath -Value "$now import_complete" -Encoding UTF8
            break
        }
    }
    catch {
        $line = "$now monitor_error=$($_.Exception.Message)"
        Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
        Write-Output $line
    }
    Start-Sleep -Seconds $PollSeconds
}
