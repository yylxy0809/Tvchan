param(
    [string]$DatabaseUrl = $env:DATABASE_URL
)

if (-not $DatabaseUrl) {
    $DatabaseUrl = "postgresql://trader:change-me-before-long-running@localhost:5432/tradingview_local"
}

function Invoke-ProjectPsql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Sql
    )

    if (Get-Command psql -ErrorAction SilentlyContinue) {
        psql $DatabaseUrl -c $Sql
        return
    }

    $container = "tv_local_timescaledb"
    $running = docker inspect -f "{{.State.Running}}" $container 2>$null
    if ($running -eq "true") {
        docker exec $container psql -U trader -d tradingview_local -c $Sql
        return
    }

    throw "psql is not installed and Docker container '$container' is not running."
}

$query = @"
select
  relname as relation,
  pg_size_pretty(pg_total_relation_size(relid)) as total_size,
  pg_size_pretty(pg_relation_size(relid)) as table_size,
  pg_size_pretty(pg_indexes_size(relid)) as index_size
from pg_catalog.pg_statio_user_tables
where schemaname = 'public'
order by pg_total_relation_size(relid) desc;
"@

Invoke-ProjectPsql -Sql $query

$hypertableQuery = @"
select
  'klines_hypertable' as relation,
  pg_size_pretty(hypertable_size('klines')) as total_size;
"@

Invoke-ProjectPsql -Sql $hypertableQuery

$countQuery = @"
select
  case timeframe
    when 5 then '5f'
    when 15 then '15f'
    when 30 then '30f'
    when 60 then '1h'
    when 1440 then '1d'
    when 10080 then '1w'
    when 43200 then '1m_month'
    else timeframe::text
  end as timeframe,
  count(*) as rows,
  min(ts) as first_ts,
  max(ts) as last_ts
from klines
group by timeframe
order by min(timeframe);
"@

Invoke-ProjectPsql -Sql $countQuery
