param(
    [string]$DataRoot = "",
    [string]$ApiBaseUrl = "",
    [string]$ApiToken = "",
    [string]$DatabaseUrl = "",
    [string[]]$SampleSymbols = @("000001.SZ", "600000.SH", "300750.SZ", "688981.SH"),
    [string[]]$Timeframes = @("5f", "30f", "1d"),
    [int]$BundleLimit = 300
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$script:Results = New-Object System.Collections.Generic.List[object]

function Write-Section([string]$Title) {
    Write-Host ""
    Write-Host "== $Title ==" -ForegroundColor Cyan
}

function Add-Result(
    [string]$Gate,
    [string]$Status,
    [string]$Message
) {
    $script:Results.Add([pscustomobject]@{
            Gate    = $Gate
            Status  = $Status
            Message = $Message
        })
    $color = switch ($Status) {
        "PASS" { "Green" }
        "WARN" { "Yellow" }
        "FAIL" { "Red" }
        "BLOCKED" { "Magenta" }
        default { "DarkGray" }
    }
    Write-Host ("[{0}] {1} - {2}" -f $Status, $Gate, $Message) -ForegroundColor $color
}

function Test-Tool([string]$Name) {
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Show-IndentedLines([string[]]$Lines) {
    foreach ($line in $Lines) {
        Write-Host ("  {0}" -f $line)
    }
}

function New-QueryString([hashtable]$Pairs) {
    $parts = foreach ($key in $Pairs.Keys) {
        "{0}={1}" -f [uri]::EscapeDataString([string]$key), [uri]::EscapeDataString([string]$Pairs[$key])
    }
    return ($parts -join "&")
}

function Invoke-OptionalPsql([string]$Title, [string]$Sql) {
    if ([string]::IsNullOrWhiteSpace($DatabaseUrl)) {
        Add-Result $Title "BLOCKED" "DatabaseUrl not provided. Manual SQL is required."
        Show-IndentedLines ($Sql -split "`r?`n")
        return $null
    }
    if (-not (Test-Tool "psql")) {
        Add-Result $Title "BLOCKED" "psql is not available in this shell. Run the SQL manually."
        Show-IndentedLines ($Sql -split "`r?`n")
        return $null
    }

    try {
        $output = & psql $DatabaseUrl -X -v ON_ERROR_STOP=1 -P pager=off -c $Sql 2>&1
        $text = ($output | Out-String).Trim()
        Add-Result $Title "PASS" "SQL executed. Review the output below."
        Show-IndentedLines ($text -split "`r?`n")
        return $text
    }
    catch {
        Add-Result $Title "FAIL" $_.Exception.Message
        Show-IndentedLines ($Sql -split "`r?`n")
        return $null
    }
}

function Invoke-OptionalJsonGet(
    [string]$Title,
    [string]$Url,
    [hashtable]$Headers
) {
    if ([string]::IsNullOrWhiteSpace($ApiBaseUrl)) {
        Add-Result $Title "BLOCKED" "ApiBaseUrl not provided."
        return $null
    }
    try {
        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        $response = Invoke-RestMethod -UseBasicParsing -Uri $Url -Headers $Headers -Method Get
        $stopwatch.Stop()
        Add-Result $Title "PASS" ("HTTP ok in {0} ms" -f $stopwatch.ElapsedMilliseconds)
        return [pscustomobject]@{
            Body       = $response
            ElapsedMs  = $stopwatch.ElapsedMilliseconds
            RequestUrl = $Url
        }
    }
    catch {
        Add-Result $Title "FAIL" $_.Exception.Message
        return $null
    }
}

function Get-HeaderValue([object]$Object, [string]$Name) {
    if ($null -eq $Object) {
        return $null
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }
    return $property.Value
}

function Show-BootstrapCommands {
    Write-Section "Scheme 2 Bootstrap Commands"
    $dataRootSetup = '$DataRoot = "D:\5f$([char]0x6570)$([char]0x636e)\5m_price"'

    Add-Result "bootstrap-command-before" "INFO" "Import-before inventory, schema, and runtime-table checks."
    Show-IndentedLines @(
        $dataRootSetup,
        "powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 -DataRoot `$DataRoot",
        "Push-Location services\collector",
        "python -m collector.parquet_bootstrap_audit --root `$DataRoot --sample-size 2",
        "Pop-Location",
        "powershell -ExecutionPolicy Bypass -File scripts\start-parquet-5f-import-worker.ps1 -Root `$DataRoot -DryRun -TaskLimit 2",
        "psql `$env:DATABASE_URL -X -v ON_ERROR_STOP=1 -f db\sql\010_scheme2_runtime.sql"
    )

    Add-Result "bootstrap-command-dry-run" "INFO" "Optional non-destructive parquet import dry-run. This helper prints the command and does not run it."
    Show-IndentedLines @(
        "powershell -ExecutionPolicy Bypass -File scripts\start-parquet-5f-import-worker.ps1 -Root `$DataRoot -DatabaseUrl `$env:DATABASE_URL -TaskLimit 5 -Concurrency 1 -BatchSize 50000 -DryRun"
    )

    Add-Result "bootstrap-command-during" "INFO" "Import-during worker and progress checks."
    Show-IndentedLines @(
        "powershell -ExecutionPolicy Bypass -File scripts\start-parquet-5f-import-worker.ps1 -Root `$DataRoot -DatabaseUrl `$env:DATABASE_URL -TaskLimit 200 -Concurrency 2 -BatchSize 50000 -Loop",
        "powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 -DatabaseUrl `$env:DATABASE_URL"
    )

    Add-Result "bootstrap-command-after" "INFO" "Import-after acceptance and serving-readiness checks."
    Show-IndentedLines @(
        "powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 -DataRoot `$DataRoot -DatabaseUrl `$env:DATABASE_URL",
        "powershell -ExecutionPolicy Bypass -File scripts\verify-scheme2-local.ps1 -DatabaseUrl `$env:DATABASE_URL -ApiBaseUrl 'http://127.0.0.1:8001' -ApiToken 'dev-local-token'"
    )
}

function Test-SourceAudit {
    Write-Section "Gate 1 - Data Source Audit"

    if ([string]::IsNullOrWhiteSpace($DataRoot)) {
        Add-Result "data-source-root" "BLOCKED" "DataRoot not provided. Cannot inspect zip/parquet inventory."
        return
    }
    if (-not (Test-Path -LiteralPath $DataRoot)) {
        Add-Result "data-source-root" "FAIL" "DataRoot does not exist: $DataRoot"
        return
    }
    Add-Result "data-source-root" "PASS" "DataRoot exists: $DataRoot"

    $inventoryErrors = @()
    $files = @(Get-ChildItem -LiteralPath $DataRoot -Recurse -File -ErrorAction SilentlyContinue -ErrorVariable inventoryErrors |
        Where-Object { $_.Extension -in @(".zip", ".parquet") }
    )

    if ($inventoryErrors.Count -gt 0) {
        Add-Result "data-source-readable" "WARN" ("Inventory walk hit {0} read errors. Review permissions or locked files." -f $inventoryErrors.Count)
    }
    else {
        Add-Result "data-source-readable" "PASS" "Inventory walk completed without filesystem read errors."
    }

    if ($files.Count -eq 0) {
        Add-Result "data-source-inventory" "FAIL" "No .zip or .parquet files found under $DataRoot"
        return
    }

    $yearMatches = [System.Collections.Generic.HashSet[string]]::new()
    foreach ($file in $files) {
        foreach ($match in [regex]::Matches($file.FullName, '(19|20)\d{2}')) {
            [void]$yearMatches.Add($match.Value)
        }
    }
    $years = @($yearMatches) | Sort-Object
    Add-Result "data-source-inventory" "PASS" ("Found {0} files and year tokens: {1}" -f $files.Count, ($years -join ", "))
    $requiredYears = 2010..([int](Get-Date).Year) | ForEach-Object { [string]$_ }
    $missingYears = @($requiredYears | Where-Object { $_ -notin $years })
    if ($missingYears.Count -gt 0) {
        Add-Result "data-source-year-coverage" "WARN" ("Missing year tokens in source paths: {0}" -f ($missingYears -join ", "))
    }
    else {
        Add-Result "data-source-year-coverage" "PASS" ("Source path tokens cover {0}-{1}." -f $requiredYears[0], $requiredYears[-1])
    }

    $zipParquetEntryName = $null
    $zipFile = $files | Where-Object { $_.Extension -eq ".zip" } | Select-Object -First 1
    if ($null -ne $zipFile) {
        try {
            Add-Type -AssemblyName System.IO.Compression.FileSystem
            $archive = [System.IO.Compression.ZipFile]::OpenRead($zipFile.FullName)
            try {
                $parquetEntry = $archive.Entries |
                    Where-Object { $_.Length -gt 0 -and ($_.FullName -match '\.parquet$') } |
                    Select-Object -First 1
                if ($null -ne $parquetEntry) {
                    $zipParquetEntryName = $parquetEntry.FullName
                }
                $entry = $archive.Entries |
                    Where-Object { $_.Length -gt 0 -and ($_.FullName -match '\.(csv|txt)$') } |
                    Select-Object -First 1
                if ($null -ne $entry) {
                    $reader = New-Object System.IO.StreamReader($entry.Open())
                    try {
                        $header = $reader.ReadLine()
                        if ([string]::IsNullOrWhiteSpace($header)) {
                            Add-Result "data-source-zip-header" "WARN" "Found a zip sample but could not read a header row."
                        }
                        else {
                            Add-Result "data-source-zip-header" "PASS" ("Sample header from {0}: {1}" -f $entry.FullName, $header)
                        }
                    }
                    finally {
                        $reader.Dispose()
                    }
                }
                else {
                    if ($null -ne $zipParquetEntryName) {
                        Add-Result "data-source-zip-header" "PASS" ("Sampled zip contains parquet member: {0}" -f $zipParquetEntryName)
                    }
                    else {
                        Add-Result "data-source-zip-header" "WARN" "No non-empty CSV/TXT/parquet member found in the sampled zip."
                    }
                }
            }
            finally {
                $archive.Dispose()
            }
        }
        catch {
            Add-Result "data-source-zip-header" "WARN" ("Zip sampling failed: {0}" -f $_.Exception.Message)
        }
    }
    else {
        Add-Result "data-source-zip-header" "WARN" "No zip archive found. Parquet-only source needs a schema check."
    }

    $parquetFile = $files | Where-Object { $_.Extension -eq ".parquet" } | Select-Object -First 1
    if ($null -ne $parquetFile -or ($null -ne $zipFile -and $null -ne $zipParquetEntryName)) {
        if (Test-Tool "python") {
            $code = @'
import json
import sys
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as exc:
    print(json.dumps({'ok': False, 'error': str(exc)}))
    raise SystemExit(0)
try:
    if len(sys.argv) == 3:
        import zipfile
        with zipfile.ZipFile(sys.argv[1]) as zf:
            with zf.open(sys.argv[2]) as fh:
                table = pq.read_schema(pa.BufferReader(fh.read()))
    else:
        table = pq.read_schema(sys.argv[1])
    print(json.dumps({'ok': True, 'fields': [str(f) for f in table]}))
except Exception as exc:
    print(json.dumps({'ok': False, 'error': str(exc)}))
'@
            try {
                if ($null -ne $parquetFile) {
                    $raw = & python -c $code $parquetFile.FullName 2>&1
                }
                else {
                    $raw = & python -c $code $zipFile.FullName $zipParquetEntryName 2>&1
                }
            }
            catch {
                $raw = (@{ ok = $false; error = $_.Exception.Message } | ConvertTo-Json -Compress)
            }
            if (-not [string]::IsNullOrWhiteSpace($raw)) {
                try {
                    $parsed = $raw | ConvertFrom-Json
                    if ($parsed.ok) {
                        Add-Result "data-source-parquet-schema" "PASS" ("Sample fields: {0}" -f (($parsed.fields | Select-Object -First 12) -join "; "))
                    }
                    else {
                        Add-Result "data-source-parquet-schema" "WARN" ("pyarrow not ready: {0}" -f $parsed.error)
                    }
                }
                catch {
                    Add-Result "data-source-parquet-schema" "WARN" "Parquet schema probe returned unreadable output."
                }
            }
            else {
                Add-Result "data-source-parquet-schema" "WARN" "Python parquet probe did not return usable output."
            }
        }
        else {
            Add-Result "data-source-parquet-schema" "WARN" "python is not available. Parquet schema must be inspected manually."
        }
    }
    else {
        Add-Result "data-source-parquet-schema" "WARN" "No parquet file found in the sample root."
    }

    Add-Result "data-source-bar-end" "WARN" "Manual sample still required: write down the source session template. Current parquet source samples include 09:30, 11:30, 13:05, and 15:00 as bar_end timestamps."
    Add-Result "data-source-5f-anomalies" "WARN" "Manual or SQL-backed audit still required: count 5f rows per symbol-day and explain values outside the agreed source template. Current parquet source full days may contain 49 rows."
}

function Test-RuntimeMigration {
    Write-Section "Gate 1b - Runtime Migration"

    $migrationPath = Join-Path $RepoRoot "db\sql\010_scheme2_runtime.sql"
    if (Test-Path -LiteralPath $migrationPath) {
        Add-Result "runtime-migration-file" "PASS" "Migration file exists: db\sql\010_scheme2_runtime.sql"
    }
    else {
        Add-Result "runtime-migration-file" "FAIL" "Missing migration file: db\sql\010_scheme2_runtime.sql"
    }

    $tableSql = @'
with expected(table_name) as (
  values
    ('scheme2_source_member_checkpoints'),
    ('scheme2_ingest_watermarks'),
    ('scheme2_chan_published_heads'),
    ('scheme2_chan_recompute_watermarks')
)
select
  e.table_name,
  to_regclass(format('public.%I', e.table_name)) as regclass,
  case
    when to_regclass(format('public.%I', e.table_name)) is null then 'FAIL: missing runtime table'
    else 'PASS: runtime table exists'
  end as status_hint
from expected e
order by e.table_name;
'@
    Invoke-OptionalPsql "runtime-migration-tables" $tableSql | Out-Null

    $columnSql = @'
with expected(table_name, column_name) as (
  values
    ('scheme2_source_member_checkpoints', 'source_profile'),
    ('scheme2_source_member_checkpoints', 'timeframe'),
    ('scheme2_source_member_checkpoints', 'status'),
    ('scheme2_source_member_checkpoints', 'imported_rows'),
    ('scheme2_source_member_checkpoints', 'updated_at'),
    ('scheme2_ingest_watermarks', 'symbol_id'),
    ('scheme2_ingest_watermarks', 'timeframe'),
    ('scheme2_ingest_watermarks', 'last_bar_end'),
    ('scheme2_ingest_watermarks', 'source'),
    ('scheme2_chan_published_heads', 'symbol_id'),
    ('scheme2_chan_published_heads', 'base_timeframe'),
    ('scheme2_chan_published_heads', 'chan_level'),
    ('scheme2_chan_published_heads', 'mode'),
    ('scheme2_chan_published_heads', 'status'),
    ('scheme2_chan_published_heads', 'base_from_bar_end'),
    ('scheme2_chan_published_heads', 'base_to_bar_end'),
    ('scheme2_chan_published_heads', 'snapshot_version'),
    ('scheme2_chan_recompute_watermarks', 'symbol_id'),
    ('scheme2_chan_recompute_watermarks', 'base_timeframe'),
    ('scheme2_chan_recompute_watermarks', 'chan_level'),
    ('scheme2_chan_recompute_watermarks', 'mode'),
    ('scheme2_chan_recompute_watermarks', 'dirty_from_bar_end'),
    ('scheme2_chan_recompute_watermarks', 'last_computed_bar_end'),
    ('scheme2_chan_recompute_watermarks', 'dirty_reason')
)
select
  e.table_name,
  e.column_name,
  case
    when c.column_name is null then 'FAIL: missing runtime column'
    else 'PASS: runtime column exists'
  end as status_hint
from expected e
left join information_schema.columns c
  on c.table_schema = 'public'
 and c.table_name = e.table_name
 and c.column_name = e.column_name
order by e.table_name, e.column_name;
'@
    Invoke-OptionalPsql "runtime-migration-columns" $columnSql | Out-Null
}

function Test-ImportCompletion {
    Write-Section "Gate 2 - Import Completion"

    $coverageSql = @'
with target as (
  select id
  from symbols
  where asset_type = 'stock'
    and market = 'A_SHARE'
    and is_active
),
source4 as (
  select k.*
  from klines k
  join target t on t.id = k.symbol_id
  where k.timeframe = 5
    and k.source = 4
)
select
  (select count(*) from target) as target_symbols,
  (
    select count(distinct symbol_id)
    from source4
  ) as imported_symbols,
  (
    select count(*)
    from source4
  ) as source4_5f_rows,
  (
    select min(ts)
    from source4
  ) as first_source4_bar_end,
  (
    select max(ts)
    from source4
  ) as last_source4_bar_end,
  case
    when (select count(*) from source4) = 0 then 'FAIL: no source=4 5f rows'
    when (select count(distinct symbol_id) from source4) < (select count(*) from target) then 'WARN: partial target-symbol coverage'
    else 'PASS: source=4 5f rows present for all target symbols'
  end as status_hint;
'@
    Invoke-OptionalPsql "import-source4-coverage" $coverageSql | Out-Null

    $sourceMixSql = @'
select
  timeframe,
  source,
  count(*) as rows,
  count(distinct symbol_id) as symbols,
  min(ts) as first_bar_end,
  max(ts) as last_bar_end
from klines
where timeframe = 5
group by 1, 2
order by timeframe, source;
'@
    Invoke-OptionalPsql "import-source-mix" $sourceMixSql | Out-Null

    $checkpointSql = @'
select
  count(*) as total_members,
  count(*) filter (where status = 'success') as success_members,
  count(*) filter (where status = 'failed') as failed_members,
  count(*) filter (where status = 'running') as running_members,
  count(*) filter (where status = 'pending') as pending_members,
  count(*) filter (where status = 'skipped') as skipped_members,
  coalesce(sum(imported_rows) filter (where status = 'success'), 0) as imported_rows_from_success_members,
  min(created_at) as first_checkpoint_created_at,
  max(updated_at) as last_checkpoint_updated_at,
  case
    when count(*) = 0 then 'BLOCKED: no parquet_5f checkpoints'
    when count(*) filter (where status = 'failed') > 0 then 'FAIL: failed checkpoints exist'
    when count(*) filter (where status = 'running' and updated_at < now() - interval '60 minutes') > 0 then 'WARN: stale running checkpoints exist'
    when count(*) filter (where status in ('pending', 'running')) > 0 then 'WARN: import still incomplete or running'
    else 'PASS: all checkpoints reached terminal success/skipped state'
  end as status_hint
from scheme2_source_member_checkpoints
where source_profile = 'parquet_5f'
  and timeframe = 5;
'@
    Invoke-OptionalPsql "import-checkpoint-status" $checkpointSql | Out-Null

    $checkpointDetailSql = @'
select
  id,
  status,
  imported_rows,
  updated_at,
  zip_path,
  member_path,
  left(coalesce(error_message, ''), 240) as error_sample
from scheme2_source_member_checkpoints
where source_profile = 'parquet_5f'
  and timeframe = 5
  and (
    status in ('failed', 'running')
    or (status = 'pending' and updated_at < now() - interval '60 minutes')
  )
order by
  case status when 'failed' then 1 when 'running' then 2 else 3 end,
  updated_at desc
limit 50;
'@
    Invoke-OptionalPsql "import-checkpoint-problem-samples" $checkpointDetailSql | Out-Null

    $rangeSql = @'
select
  s.code || '.' || s.exchange as symbol,
  min(k.ts) as first_5f_ts,
  max(k.ts) as last_5f_ts,
  count(*) as rows_5f
from klines k
join symbols s on s.id = k.symbol_id
where k.timeframe = 5
  and k.source = 4
group by 1
order by 1
limit 30;
'@
    Invoke-OptionalPsql "import-per-symbol-range" $rangeSql | Out-Null

    $watermarkSummarySql = @'
with imported as (
  select
    k.symbol_id,
    count(*) as rows_5f,
    max(k.ts) as max_bar_end
  from klines k
  where k.timeframe = 5
    and k.source = 4
  group by 1
)
select
  count(*) as imported_symbols,
  count(w.*) as watermark_rows,
  count(*) filter (where w.symbol_id is null) as missing_watermarks,
  count(*) filter (where w.symbol_id is not null and w.last_bar_end = i.max_bar_end) as aligned_watermarks,
  count(*) filter (where w.symbol_id is not null and w.last_bar_end is distinct from i.max_bar_end) as drifted_watermarks,
  min(w.last_bar_end) as min_watermark_bar_end,
  max(w.last_bar_end) as max_watermark_bar_end,
  case
    when count(*) = 0 then 'BLOCKED: no imported source=4 symbols'
    when count(*) filter (where w.symbol_id is null) > 0 then 'FAIL: missing symbol watermarks'
    when count(*) filter (where w.last_bar_end is distinct from i.max_bar_end) > 0 then 'FAIL: watermark drift'
    else 'PASS: every imported symbol watermark aligns to max source=4 bar_end'
  end as status_hint
from imported i
left join scheme2_ingest_watermarks w
  on w.symbol_id = i.symbol_id
 and w.timeframe = 5;
'@
    Invoke-OptionalPsql "import-symbol-watermark-summary" $watermarkSummarySql | Out-Null

    $watermarkSql = @'
select
  s.code || '.' || s.exchange as symbol,
  max(k.ts) as imported_last_5f_ts,
  w.last_bar_end,
  w.source as watermark_source,
  case when max(k.ts) = w.last_bar_end then 'aligned' else 'drift' end as watermark_state
from symbols s
join klines k on k.symbol_id = s.id and k.timeframe = 5 and k.source = 4
left join scheme2_ingest_watermarks w on w.symbol_id = s.id and w.timeframe = 5
group by 1, w.last_bar_end, w.source
having max(k.ts) is distinct from w.last_bar_end
order by 1
limit 30;
'@
    Invoke-OptionalPsql "import-watermark-alignment" $watermarkSql | Out-Null

    $duplicateSql = @'
select
  s.code || '.' || s.exchange as symbol,
  k.timeframe,
  k.ts as bar_end,
  count(*) as duplicate_rows,
  array_agg(k.source order by k.source) as sources
from klines k
join symbols s on s.id = k.symbol_id
where k.timeframe = 5
  and k.source = 4
group by 1, 2, 3
having count(*) > 1
order by duplicate_rows desc, bar_end desc
limit 50;
'@
    Invoke-OptionalPsql "import-duplicate-source4-bars" $duplicateSql | Out-Null

    $dailyCountSql = @'
select
  s.code || '.' || s.exchange as symbol,
  (k.ts at time zone 'Asia/Shanghai')::date as trade_day,
  count(*) as bars_5f
from klines k
join symbols s on s.id = k.symbol_id
where k.timeframe = 5
  and k.source = 4
group by 1, 2
having count(*) not in (48, 49)
order by trade_day desc, symbol
limit 50;
'@
    Invoke-OptionalPsql "import-daily-anomalies" $dailyCountSql | Out-Null

    $dailySummarySql = @'
with daily as (
  select
    k.symbol_id,
    (k.ts at time zone 'Asia/Shanghai')::date as trade_day,
    count(*) as bars_5f
  from klines k
  where k.timeframe = 5
    and k.source = 4
  group by 1, 2
)
select
  count(*) as symbol_days,
  count(*) filter (where bars_5f in (48, 49)) as normal_symbol_days,
  count(*) filter (where bars_5f not in (48, 49)) as anomalous_symbol_days,
  round(100.0 * count(*) filter (where bars_5f not in (48, 49)) / nullif(count(*), 0), 4) as anomaly_percent,
  min(bars_5f) as min_bars_per_day,
  max(bars_5f) as max_bars_per_day,
  case
    when count(*) = 0 then 'BLOCKED: no source=4 symbol-days'
    when 100.0 * count(*) filter (where bars_5f not in (48, 49)) / nullif(count(*), 0) > 0.1 then 'FAIL: anomaly ratio above 0.1%'
    when count(*) filter (where bars_5f not in (48, 49)) > 0 then 'WARN: anomalies require exception list'
    else 'PASS: normal 5f day counts only'
  end as status_hint
from daily;
'@
    Invoke-OptionalPsql "import-daily-anomaly-summary" $dailySummarySql | Out-Null

    $barEndSql = @'
with local_bars as (
  select
    k.symbol_id,
    k.ts,
    k.ts at time zone 'Asia/Shanghai' as local_ts
  from klines k
  where k.timeframe = 5
    and k.source = 4
)
select
  count(*) as source4_5f_rows,
  count(*) filter (where local_ts::time = time '09:30') as rows_labeled_0930,
  count(*) filter (where local_ts::time = time '11:30') as rows_labeled_1130,
  count(*) filter (where local_ts::time = time '13:05') as rows_labeled_1305,
  count(*) filter (where local_ts::time = time '15:00') as rows_labeled_1500,
  count(*) filter (
    where extract(second from local_ts)::int <> 0
       or mod(extract(minute from local_ts)::int, 5) <> 0
  ) as off_5m_grid_rows,
  min(local_ts) as first_local_bar_end,
  max(local_ts) as last_local_bar_end,
  case
    when count(*) = 0 then 'BLOCKED: no source=4 5f rows'
    when count(*) filter (
      where extract(second from local_ts)::int <> 0
         or mod(extract(minute from local_ts)::int, 5) <> 0
    ) > 0 then 'FAIL: off-grid timestamps found'
    when count(*) filter (where local_ts::time = time '15:00') = 0 then 'WARN: no 15:00 bar_end labels found in source=4 rows'
    else 'PASS: source=4 timestamps remain on expected bar_end grid'
  end as status_hint
from local_bars;
'@
    Invoke-OptionalPsql "import-bar-end-semantics" $barEndSql | Out-Null

    $barEndSampleSql = @'
with daily as (
  select
    s.code || '.' || s.exchange as symbol,
    (k.ts at time zone 'Asia/Shanghai')::date as trade_day,
    min((k.ts at time zone 'Asia/Shanghai')::time) as first_local_time,
    max((k.ts at time zone 'Asia/Shanghai')::time) as last_local_time,
    bool_or((k.ts at time zone 'Asia/Shanghai')::time = time '09:30') as has_0930,
    bool_or((k.ts at time zone 'Asia/Shanghai')::time = time '13:05') as has_1305,
    bool_or((k.ts at time zone 'Asia/Shanghai')::time = time '15:00') as has_1500,
    count(*) as bars_5f
  from klines k
  join symbols s on s.id = k.symbol_id
  where k.timeframe = 5
    and k.source = 4
  group by 1, 2
)
select *
from daily
where bars_5f in (48, 49)
order by trade_day desc, symbol
limit 30;
'@
    Invoke-OptionalPsql "import-bar-end-day-samples" $barEndSampleSql | Out-Null
}

function Test-ChanCompletion {
    Write-Section "Gate 3 - Chan Completion"

    $tableSql = @'
select
  to_regclass('public.scheme2_chan_published_heads') as published_heads,
  to_regclass('public.scheme2_chan_recompute_watermarks') as recompute_watermarks;
'@
    Invoke-OptionalPsql "chan-runtime-tables" $tableSql | Out-Null

    $publishedSql = @'
select
  h.chan_level,
  h.mode,
  count(*) as published_heads,
  count(*) filter (where h.status = 'published') as rows_in_published_state
from scheme2_chan_published_heads h
where h.base_timeframe = 5
group by 1, 2
order by 1, 2;
'@
    Invoke-OptionalPsql "chan-published-heads" $publishedSql | Out-Null

    $coverageSql = @'
with imported as (
  select distinct symbol_id
  from klines
  where timeframe = 5
),
expected as (
  select i.symbol_id, level_code.chan_level, mode_name.mode
  from imported i
  cross join (values (5), (30), (1440)) as level_code(chan_level)
  cross join (values ('confirmed'), ('predictive')) as mode_name(mode)
)
select
  count(*) as expected_heads,
  count(h.*) as actual_heads
from expected e
left join scheme2_chan_published_heads h
  on h.symbol_id = e.symbol_id
 and h.chan_level = e.chan_level
 and h.mode = e.mode
 and h.base_timeframe = 5
 and h.status = 'published';
'@
    Invoke-OptionalPsql "chan-head-coverage" $coverageSql | Out-Null

    $dirtySql = @'
select
  w.chan_level,
  w.mode,
  count(*) filter (where w.dirty_from_bar_end is not null) as dirty_rows,
  count(*) filter (
    where w.dirty_from_bar_end is not null
        and coalesce(w.dirty_reason, '') not in ('manual-waiver', 'halted-symbol', 'late-source-repair')
  ) as uncontrolled_dirty_rows
from scheme2_chan_recompute_watermarks w
where w.base_timeframe = 5
group by 1, 2
order by 1, 2;
'@
    Invoke-OptionalPsql "chan-dirty-ranges" $dirtySql | Out-Null
}

function Test-ApiBundle {
    Write-Section "Gate 4 - API Bundle"

    if ([string]::IsNullOrWhiteSpace($ApiBaseUrl)) {
        Add-Result "api-bundle" "BLOCKED" "ApiBaseUrl not provided. Cannot run live bundle checks."
        return
    }

    $trimmedBase = $ApiBaseUrl.TrimEnd("/")
    $token = if ([string]::IsNullOrWhiteSpace($ApiToken)) {
        if ([string]::IsNullOrWhiteSpace($env:API_TOKEN)) { "dev-local-token" } else { $env:API_TOKEN }
    }
    else {
        $ApiToken
    }
    $headers = @{ Authorization = "Bearer $token" }

    foreach ($symbol in ($SampleSymbols | Get-Random -Count ([Math]::Min($SampleSymbols.Count, 3)))) {
        foreach ($timeframe in $Timeframes) {
            $query = New-QueryString @{
                symbol    = $symbol
                timeframe = $timeframe
                limit     = [string]$BundleLimit
            }
            $url = "{0}/api/v3/chart/bundle?{1}" -f $trimmedBase, $query

            $first = Invoke-OptionalJsonGet "bundle-$symbol-$timeframe-1" $url $headers
            $second = Invoke-OptionalJsonGet "bundle-$symbol-$timeframe-2" $url $headers
            if ($null -eq $first -or $null -eq $second) {
                continue
            }

            $firstBody = $first.Body
            $secondBody = $second.Body
            $bars = Get-HeaderValue $firstBody "bars"
            $chan = Get-HeaderValue $firstBody "chan"
            if ($null -eq $bars -or $bars.Count -eq 0) {
                Add-Result "bundle-$symbol-$timeframe-bars" "FAIL" "bars payload is empty."
            }
            else {
                Add-Result "bundle-$symbol-$timeframe-bars" "PASS" ("bars count: {0}" -f $bars.Count)
            }

            if ($null -eq $chan) {
                Add-Result "bundle-$symbol-$timeframe-chan" "FAIL" "chan payload is missing."
                continue
            }

            $schemaVersion = [string](Get-HeaderValue $firstBody "schema_version")
            if ($schemaVersion -ne "chart-bundle.v3") {
                Add-Result "bundle-$symbol-$timeframe-schema" "FAIL" ("expected chart-bundle.v3, got {0}" -f $schemaVersion)
            }
            else {
                Add-Result "bundle-$symbol-$timeframe-schema" "PASS" "schema_version=chart-bundle.v3"
            }

            $analysisLevels = @()
            $rawAnalysisLevels = Get-HeaderValue $firstBody "analysis_levels"
            if ($null -ne $rawAnalysisLevels) {
                $analysisLevels = @($rawAnalysisLevels)
            }
            $chanLevels = Get-HeaderValue $chan "levels"
            $levelNames = @()
            if ($null -ne $chanLevels) {
                $levelNames = @($chanLevels.PSObject.Properties.Name)
            }
            $levels = @($analysisLevels + $levelNames | Select-Object -Unique)
            $missingLevels = @("5f", "30f", "1d") | Where-Object { $_ -notin $levelNames }
            if ($missingLevels.Count -gt 0) {
                Add-Result "bundle-$symbol-$timeframe-levels" "FAIL" ("missing levels: {0}" -f ($missingLevels -join ", "))
            }
            else {
                Add-Result "bundle-$symbol-$timeframe-levels" "PASS" ("analysis levels: {0}; chan levels: {1}" -f (($analysisLevels -join ", ")), (($levelNames -join ", ")))
            }

            $baseTimeframe = [string](Get-HeaderValue $firstBody "base_timeframe")
            $baseSemantics = [string](Get-HeaderValue $firstBody "bar_time_semantics")
            if ($baseTimeframe -ne "5f" -or $baseSemantics -ne "bar_end") {
                Add-Result "bundle-$symbol-$timeframe-base" "FAIL" ("base contract mismatch: {0} / {1}" -f $baseTimeframe, $baseSemantics)
            }
            else {
                Add-Result "bundle-$symbol-$timeframe-base" "PASS" "base_timeframe=5f and bar_time_semantics=bar_end"
            }

            $engine = [string](Get-HeaderValue $chan "engine")
            if ($engine -match "fake|placeholder") {
                Add-Result "bundle-$symbol-$timeframe-engine" "FAIL" ("unexpected engine: {0}" -f $engine)
            }
            else {
                Add-Result "bundle-$symbol-$timeframe-engine" "PASS" ("engine: {0}" -f $engine)
            }

            $firstSnapshotVersion = [string](Get-HeaderValue $firstBody "snapshot_version")
            $secondSnapshotVersion = [string](Get-HeaderValue $secondBody "snapshot_version")
            $firstSnapshotId = [string](Get-HeaderValue $firstBody "snapshot_id")
            $secondSnapshotId = [string](Get-HeaderValue $secondBody "snapshot_id")
            if ($firstSnapshotVersion -ne $secondSnapshotVersion -or $firstSnapshotId -ne $secondSnapshotId) {
                Add-Result "bundle-$symbol-$timeframe-stability" "WARN" ("snapshot drift detected: version {0} -> {1}, id {2} -> {3}" -f $firstSnapshotVersion, $secondSnapshotVersion, $firstSnapshotId, $secondSnapshotId)
            }
            else {
                Add-Result "bundle-$symbol-$timeframe-stability" "PASS" ("snapshot stable: {0}" -f $firstSnapshotVersion)
            }
        }
    }
}

function Test-FrontendBundlePath {
    Write-Section "Gate 5 - Frontend Read-Only Bundle Path"

    $webRoot = Join-Path $RepoRoot "apps\web\src"
    if (-not (Test-Path -LiteralPath $webRoot)) {
        Add-Result "frontend-source-root" "FAIL" "Missing frontend source root: $webRoot"
        return
    }

    $legacyMatches = @()
    if (Test-Tool "rg") {
        $legacyMatches = @(rg -n "/api/v1/chart/window" $webRoot 2>$null)
    }
    else {
        $legacyMatches = @(
            Get-ChildItem -LiteralPath $webRoot -Recurse -File |
            Select-String -Pattern "/api/v1/chart/window" |
            ForEach-Object { "{0}:{1}:{2}" -f $_.Path, $_.LineNumber, $_.Line.Trim() }
        )
    }

    if ($legacyMatches.Count -gt 0) {
        Add-Result "frontend-legacy-window-route" "FAIL" "Direct /api/v1/chart/window read found under apps/web/src."
        Show-IndentedLines $legacyMatches
    }
    else {
        Add-Result "frontend-legacy-window-route" "PASS" "No direct /api/v1/chart/window read found under apps/web/src."
    }

    $bundleMatches = @()
    if (Test-Tool "rg") {
        $bundleMatches = @(rg -n "/api/v3/chart/bundle|get_chart_bundle|chart-bundle.v3|frontend-chart-bundle.v2" $webRoot 2>$null)
    }
    else {
        $bundleMatches = @(
            Get-ChildItem -LiteralPath $webRoot -Recurse -File |
            Select-String -Pattern "/api/v3/chart/bundle|get_chart_bundle|chart-bundle.v3|frontend-chart-bundle.v2" |
            ForEach-Object { "{0}:{1}:{2}" -f $_.Path, $_.LineNumber, $_.Line.Trim() }
        )
    }

    if ($bundleMatches.Count -eq 0) {
        Add-Result "frontend-bundle-route" "WARN" "Bundle route references were not detected automatically."
    }
    else {
        Add-Result "frontend-bundle-route" "PASS" "Bundle route references found in frontend source."
        Show-IndentedLines ($bundleMatches | Select-Object -First 12)
    }
}

function Show-PressureChecklist {
    Write-Section "Gate 6 - Pressure Test Checklist"

    Add-Result "pressure-plan" "WARN" "This script does not run load for you. It prints the minimum scenario and observation set."
    Show-IndentedLines @(
        "Scenario tiers: 5, 10, and 20 concurrent users.",
        "Per user loop: open 5f, drag 5 windows, switch 5f -> 30f -> 1d -> 5f, repeat for 5 minutes after warmup.",
        "Observe /api/v3/chart/bundle latency, WS get_chart_bundle latency, error rate, container restarts, CPU, memory, and disk pressure.",
        "Suggested PASS thresholds: p95 <= 1.0s at 5 users, <= 1.5s at 10 users, <= 2.5s at 20 users, error rate < 1%.",
        "Capture docker stats or NAS monitoring screenshots if live containers exist.",
        "If Docker is not running, prepare the load plan first and attach it to the evidence folder."
    )

    if (Test-Tool "docker") {
        Add-Result "pressure-observation-tools" "PASS" "docker is available. You can use docker stats and docker compose ps during the live run."
        Show-IndentedLines @(
            "docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml ps",
            "docker stats --no-stream"
        )
    }
    else {
        Add-Result "pressure-observation-tools" "WARN" "docker is not available in this shell. Use NAS host monitoring or another shell for live observations."
    }
}

Write-Section "Scheme 2 Verification Gate Skeleton"
Add-Result "repo-root" "PASS" ("Repo root: {0}" -f $RepoRoot)

Show-BootstrapCommands
Test-SourceAudit
Test-RuntimeMigration
Test-ImportCompletion
Test-ChanCompletion
Test-ApiBundle
Test-FrontendBundlePath
Show-PressureChecklist

Write-Section "Summary"
$script:Results |
    Select-Object Gate, Status, Message |
    Format-Table -AutoSize
