param(
    [int]$BatchSize = 50,
    [int]$SymbolLimit = 0,
    [int]$Limit = 300,
    [double]$Sleep = 0,
    [switch]$Loop,
    [double]$LoopInterval = 60,
    [string]$TdxHost = "",
    [string]$TdxHosts = "124.70.199.56,115.238.90.165",
    [int]$TdxPort = 7709,
    [int]$TdxTimeout = 3,
    [int]$TdxRetries = 1,
    [switch]$SkipTdxPreflight,
    [switch]$RecentFirst,
    [string]$TargetBarEndUtc = "",
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

function Test-TdxHost {
    param([string]$HostName)
    $probe = @"
from pytdx.hq import TdxHq_API
api = TdxHq_API()
ok = False
try:
    ok = bool(api.connect('$HostName', $TdxPort, time_out=$TdxTimeout))
except Exception:
    ok = False
finally:
    try:
        api.disconnect()
    except Exception:
        pass
print("1" if ok else "0")
"@
    $result = $probe | & python -
    return ($result | Select-Object -Last 1) -eq "1"
}

function Get-AvailableTdxHost {
    $hosts = @()
    if ($TdxHost -and $TdxHost.Trim().Length -gt 0) {
        $hosts += $TdxHost.Trim()
    } else {
        $hosts += @($TdxHosts.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_.Length -gt 0 })
    }

    if ($SkipTdxPreflight -and $hosts.Count -gt 0) {
        return $hosts[0]
    }

    foreach ($hostName in $hosts) {
        if (Test-TdxHost -HostName $hostName) {
            return $hostName
        }
    }
    return $null
}

function Get-DbSymbols {
    $query = @"
import asyncio
import asyncpg
from datetime import datetime, timezone

async def main():
    conn = await asyncpg.connect('$DatabaseUrl')
    try:
        limit = $SymbolLimit
        recent_first = '''$RecentFirst'''.strip().lower() == 'true'
        target = '''$TargetBarEndUtc'''.strip()
        target_dt = None
        if target:
            target_dt = datetime.fromisoformat(target.replace('Z', '+00:00'))
            if target_dt.tzinfo is None:
                target_dt = target_dt.replace(tzinfo=timezone.utc)
        sql = '''
            select code || '.' || exchange as symbol
            from symbols s
            left join scheme2_ingest_watermarks wm
              on wm.symbol_id = s.id
             and wm.timeframe = 5
            where s.is_active = true
              and (
                `$1::timestamptz is null
                or coalesce(wm.last_bar_end, timestamptz '1970-01-01') < `$1::timestamptz
              )
        '''
        if recent_first:
            sql += '''
                order by coalesce(wm.last_bar_end, timestamptz '1970-01-01') desc, s.code, s.exchange
            '''
        else:
            sql += '''
                order by coalesce(wm.last_bar_end, timestamptz '1970-01-01'), s.code, s.exchange
            '''
        if limit > 0:
            sql += ' limit ' + str(limit)
        rows = await conn.fetch(sql, target_dt)
        for row in rows:
            print(row['symbol'])
    finally:
        await conn.close()

asyncio.run(main())
"@
    $query | & python -
}

do {
    $selectedHost = Get-AvailableTdxHost
    if (-not $selectedHost) {
        Write-Host (@{
            event = "market_fill_db_batches_tdx_unavailable"
            hosts = $(if ($TdxHost) { $TdxHost } else { $TdxHosts })
            time = (Get-Date).ToString("s")
        } | ConvertTo-Json -Compress)
        if (-not $Loop) {
            break
        }
        Start-Sleep -Seconds $LoopInterval
        continue
    }

    $symbols = @(Get-DbSymbols | Where-Object { $_ -and $_.Trim().Length -gt 0 })
    Write-Host (@{
        event = "market_fill_db_batches_started"
        symbols = $symbols.Count
        batch_size = $BatchSize
        time = (Get-Date).ToString("s")
    } | ConvertTo-Json -Compress)

    for ($offset = 0; $offset -lt $symbols.Count; $offset += $BatchSize) {
        $selectedHost = Get-AvailableTdxHost
        if (-not $selectedHost) {
            Write-Host (@{
                event = "market_fill_db_batch_skipped_tdx_unavailable"
                offset = $offset
                hosts = $(if ($TdxHost) { $TdxHost } else { $TdxHosts })
                time = (Get-Date).ToString("s")
            } | ConvertTo-Json -Compress)
            break
        }

        $batch = @($symbols[$offset..([Math]::Min($offset + $BatchSize - 1, $symbols.Count - 1))])
        $joined = [string]::Join(",", $batch)
        $providerHosts = $(if ($TdxHost) { $TdxHost } else { $TdxHosts })
        Write-Host (@{
            event = "market_fill_db_batch_started"
            offset = $offset
            size = $batch.Count
            first = $batch[0]
            host = $selectedHost
            time = (Get-Date).ToString("s")
        } | ConvertTo-Json -Compress)

        python -m collector.market_fill `
            --provider pytdx `
            --symbols $joined `
            --timeframes 5f `
            --limit $Limit `
            --sleep $Sleep `
            --skip-chan `
            --redis-url $RedisUrl `
            --database-url $DatabaseUrl `
            --tdx-host $providerHosts `
            --tdx-port $TdxPort `
            --tdx-timeout $TdxTimeout `
            --tdx-retries $TdxRetries
    }

    Write-Host (@{
        event = "market_fill_db_batches_finished"
        symbols = $symbols.Count
        time = (Get-Date).ToString("s")
    } | ConvertTo-Json -Compress)

    if ($Loop) {
        Start-Sleep -Seconds $LoopInterval
    }
} while ($Loop)
