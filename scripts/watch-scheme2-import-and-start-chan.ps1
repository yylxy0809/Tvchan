param(
    [string]$DbContainer = "tv_backend_timescaledb",
    [string]$PostgresUser = "trader",
    [string]$PostgresDb = "tradingview_local",
    [string]$DatabaseUrl = "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local",
    [string]$ChanServiceUrl = "http://127.0.0.1:8002",
    [int]$PollSeconds = 60,
    [int]$ChanTaskLimit = 5,
    [int]$ChanConcurrency = 1,
    [double]$ChanTimeout = 300,
    [double]$ChanLoopInterval = 30,
    [string]$LogDir = ""
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($LogDir)) {
    $LogDir = Join-Path $RepoRoot "logs\scheme2-import"
}
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$LogPath = Join-Path $LogDir "post-import-orchestrator.log"
$ChanOut = Join-Path $LogDir "chan-recompute.out.log"
$ChanErr = Join-Path $LogDir "chan-recompute.err.log"
$ChanServiceOut = Join-Path $LogDir "chan-service.out.log"
$ChanServiceErr = Join-Path $LogDir "chan-service.err.log"

function Write-StepLog {
    param([string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    Add-Content -Path $LogPath -Value $line -Encoding UTF8
    Write-Output $line
}

function Invoke-DbCsv {
    param([string]$Sql)
    $output = & docker exec $DbContainer psql `
        -U $PostgresUser `
        -d $PostgresDb `
        -X `
        -t `
        -A `
        -F "," `
        -v ON_ERROR_STOP=1 `
        -c $Sql 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "psql failed: $($output -join ' ')"
    }
    $lines = @($output | Where-Object { $_.Trim().Length -gt 0 })
    return $lines
}

function Get-ImportState {
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
where source_profile = 'parquet_5f'
  and timeframe = 5;
"@
    $line = @(Invoke-DbCsv $sql)[0]
    $parts = $line.Split(",")
    return [pscustomobject]@{
        Pending = [int64]$parts[0]
        Running = [int64]$parts[1]
        Failed = [int64]$parts[2]
        Success = [int64]$parts[3]
        Total = [int64]$parts[4]
        ImportedRows = [int64]$parts[5]
        LastUpdated = $parts[6]
    }
}

function Invoke-PostImportValidation {
    Write-StepLog "post-import validation started"

    $coverageSql = @"
select
    count(*) as tasks,
    count(*) filter (where status = 'success') as success_tasks,
    coalesce(sum(imported_rows), 0) as imported_rows,
    coalesce(to_char(min(completed_at), 'YYYY-MM-DD HH24:MI:SSOF'), '') as first_completed_at,
    coalesce(to_char(max(completed_at), 'YYYY-MM-DD HH24:MI:SSOF'), '') as last_completed_at
from scheme2_source_member_checkpoints
where source_profile = 'parquet_5f'
  and timeframe = 5;
"@
    $coverage = @(Invoke-DbCsv $coverageSql)[0]
    Write-StepLog "coverage tasks,success_tasks,imported_rows,first_completed_at,last_completed_at = $coverage"
    $coverageParts = $coverage.Split(",")
    if ([int64]$coverageParts[0] -le 0 -or [int64]$coverageParts[1] -ne [int64]$coverageParts[0] -or [int64]$coverageParts[2] -le 0) {
        throw "coverage validation failed: parquet_5f checkpoints are incomplete"
    }

    $watermarkSummarySql = @"
select
    count(*) as symbols,
    coalesce(to_char(min(last_bar_end), 'YYYY-MM-DD HH24:MI:SSOF'), '') as min_last_bar_end,
    coalesce(to_char(max(last_bar_end), 'YYYY-MM-DD HH24:MI:SSOF'), '') as max_last_bar_end
from scheme2_ingest_watermarks
where source = 'parquet_5f'
  and timeframe = 5;
"@
    $watermarkSummary = @(Invoke-DbCsv $watermarkSummarySql)[0]
    Write-StepLog "watermark-summary symbols,min_last_bar_end,max_last_bar_end = $watermarkSummary"
    $watermarkSummaryParts = $watermarkSummary.Split(",")
    if ([int64]$watermarkSummaryParts[0] -le 0) {
        throw "watermark validation failed: no parquet_5f watermarks"
    }

    $invalidSlotSql = @"
select count(*)
from scheme2_ingest_watermarks
where not (
    ((last_bar_end at time zone 'Asia/Shanghai')::time >= time '09:30' and (last_bar_end at time zone 'Asia/Shanghai')::time <= time '11:30' and mod(extract(minute from (last_bar_end at time zone 'Asia/Shanghai')::time)::int, 5) = 0)
    or
    ((last_bar_end at time zone 'Asia/Shanghai')::time >= time '13:05' and (last_bar_end at time zone 'Asia/Shanghai')::time <= time '15:00' and mod(extract(minute from (last_bar_end at time zone 'Asia/Shanghai')::time)::int, 5) = 0)
)
  and source = 'parquet_5f'
  and timeframe = 5;
"@
    $invalidSlotRows = @(Invoke-DbCsv $invalidSlotSql)
    $invalidSlots = [int64]$invalidSlotRows[0]
    Write-StepLog "gap-check invalid_watermark_5f_time_slots = $invalidSlots"
    if ($invalidSlots -gt 0) {
        Write-StepLog "gap-check warning: invalid watermark slots are not blocking chan recompute; inspect symbols separately"
    }

    $watermarkSql = @"
select count(*)
from scheme2_ingest_watermarks w
where w.timeframe = 5
  and w.source <> 'parquet_5f';
"@
    $watermarkRows = @(Invoke-DbCsv $watermarkSql)
    $watermarkMismatches = [int64]$watermarkRows[0]
    Write-StepLog "watermark-check non_parquet_5f_watermarks = $watermarkMismatches"
    if ($watermarkMismatches -gt 0) {
        throw "watermark validation failed: found non parquet_5f 5f watermarks"
    }

    Write-StepLog "deep intraday gap audit skipped inline to avoid blocking on 691M rows; run offline audit separately if needed"

    Write-StepLog "post-import validation passed"
}

function Test-ChanService {
    try {
        $response = Invoke-WebRequest -Uri "$ChanServiceUrl/health" -UseBasicParsing -TimeoutSec 5
        return $response.StatusCode -ge 200 -and $response.StatusCode -lt 300
    } catch {
        return $false
    }
}

function Ensure-ChanService {
    if (Test-ChanService) {
        Write-StepLog "chan service health ok: $ChanServiceUrl"
        return
    }

    $uri = [Uri]$ChanServiceUrl
    $hostName = if ([string]::IsNullOrWhiteSpace($uri.Host)) { "127.0.0.1" } else { $uri.Host }
    $port = if ($uri.Port -gt 0) { $uri.Port } else { 8002 }

    $existing = Get-CimInstance Win32_Process |
        Where-Object { $_.CommandLine -like "*chan_service.main*" }
    if ($existing) {
        Write-StepLog "chan service process exists but health is not ready; waiting for $ChanServiceUrl"
    } else {
        $args = @(
            "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", "scripts\start-chan-service.ps1",
            "-Port", [string]$port,
            "-HostName", $hostName
        )
        $process = Start-Process -FilePath "powershell.exe" `
            -ArgumentList $args `
            -WorkingDirectory $RepoRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput $ChanServiceOut `
            -RedirectStandardError $ChanServiceErr `
            -PassThru
        Write-StepLog "chan service started pid=$($process.Id) out=$ChanServiceOut err=$ChanServiceErr"
    }

    for ($i = 1; $i -le 60; $i++) {
        if (Test-ChanService) {
            Write-StepLog "chan service health ok after wait: $ChanServiceUrl"
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "chan service is not healthy: $ChanServiceUrl"
}

function Start-ChanRecompute {
    Ensure-ChanService

    $existing = Get-CimInstance Win32_Process |
        Where-Object { $_.CommandLine -like "*collector.chan_recompute*" }
    if ($existing) {
        Write-StepLog "chan recompute already running; skip start"
        return
    }

    $args = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", "scripts\start-chan-recompute-worker.ps1",
        "-SymbolLimit", "0",
        "-BaseTimeframe", "5f",
        "-ChanLevels", "5f,30f,1d",
        "-Modes", "confirmed,predictive",
        "-TaskLimit", [string]$ChanTaskLimit,
        "-Concurrency", [string]$ChanConcurrency,
        "-ChanTimeout", [string]$ChanTimeout,
        "-Loop",
        "-LoopInterval", [string]$ChanLoopInterval,
        "-Reset",
        "-ResetRunning",
        "-ChanServiceUrl", $ChanServiceUrl,
        "-DatabaseUrl", $DatabaseUrl
    )
    $process = Start-Process -FilePath "powershell.exe" `
        -ArgumentList $args `
        -WorkingDirectory $RepoRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $ChanOut `
        -RedirectStandardError $ChanErr `
        -PassThru
    Write-StepLog "chan recompute started pid=$($process.Id) out=$ChanOut err=$ChanErr"
}

function Watch-ChanProgress {
    while ($true) {
        $sql = @"
select
    count(*) filter (where status = 'pending') as pending,
    count(*) filter (where status = 'running') as running,
    count(*) filter (where status = 'failed') as failed,
    count(*) filter (where status = 'success') as success,
    count(*) as total,
    coalesce(sum(strokes_count), 0) as strokes,
    coalesce(sum(segments_count), 0) as segments,
    coalesce(sum(centers_count), 0) as centers,
    coalesce(sum(signals_count), 0) as signals
from chan_recompute_tasks;
"@
        try {
            $line = @(Invoke-DbCsv $sql)[0]
            Write-StepLog "chan progress pending,running,failed,success,total,strokes,segments,centers,signals = $line"
            $parts = $line.Split(",")
            if ([int64]$parts[4] -gt 0 -and [int64]$parts[0] -eq 0 -and [int64]$parts[1] -eq 0 -and [int64]$parts[2] -eq 0) {
                Write-StepLog "chan recompute completed"
                return
            }
        } catch {
            Write-StepLog "chan progress query failed: $($_.Exception.Message)"
        }
        Start-Sleep -Seconds $PollSeconds
    }
}

Write-StepLog "orchestrator started"
while ($true) {
    try {
        $state = Get-ImportState
        $pct = if ($state.Total -gt 0) { [math]::Round(100.0 * $state.Success / $state.Total, 2) } else { 0 }
        Write-StepLog "import progress success=$($state.Success)/$($state.Total) pct=$pct pending=$($state.Pending) running=$($state.Running) failed=$($state.Failed) rows=$($state.ImportedRows)"
        if ($state.Total -gt 0 -and $state.Pending -eq 0 -and $state.Running -eq 0 -and $state.Failed -eq 0 -and $state.Success -eq $state.Total) {
            break
        }
    } catch {
        Write-StepLog "import progress query failed: $($_.Exception.Message)"
    }
    Start-Sleep -Seconds $PollSeconds
}

Invoke-PostImportValidation
Start-ChanRecompute
Watch-ChanProgress
