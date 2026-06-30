param(
    [string]$DatabaseUrl = $env:DATABASE_URL,
    [int]$IntervalSeconds = 30,
    [switch]$Once
)

if (-not $DatabaseUrl) {
    $DatabaseUrl = "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local"
}

$ErrorActionPreference = "Stop"
$env:WATCH_CHAN_DATABASE_URL = $DatabaseUrl

do {
    Clear-Host
    @'
import asyncio
import os

import asyncpg


async def main():
    conn = await asyncpg.connect(os.environ["WATCH_CHAN_DATABASE_URL"])
    try:
        print("-- task status")
        rows = await conn.fetch(
            """
            select config_hash, status, count(*)::bigint as tasks,
                   sum(coalesce(last_bar_count, 0))::bigint as bars,
                   sum(coalesce(strokes_count, 0))::bigint as strokes,
                   sum(coalesce(segments_count, 0))::bigint as segments,
                   sum(coalesce(centers_count, 0))::bigint as centers
            from chan_recompute_tasks
            group by config_hash, status
            order by config_hash, status
            """
        )
        for row in rows:
            print(dict(row))

        print("\n-- published heads")
        rows = await conn.fetch(
            """
            select status, count(*)::bigint as heads, count(distinct symbol_id)::bigint as symbols
            from scheme2_chan_published_heads
            group by status
            order by status
            """
        )
        for row in rows:
            print(dict(row))

        print("\n-- latest successes")
        rows = await conn.fetch(
            """
            select s.code || '.' || s.exchange as symbol,
                   t.last_bar_count,
                   t.strokes_count,
                   t.segments_count,
                   t.centers_count,
                   t.finished_at
            from chan_recompute_tasks t
            join symbols s on s.id = t.symbol_id
            where t.status = 'success'
            order by t.finished_at desc nulls last
            limit 10
            """
        )
        for row in rows:
            print(dict(row))

        print("\n-- latest failures")
        rows = await conn.fetch(
            """
            select s.code || '.' || s.exchange as symbol,
                   t.attempts,
                   left(coalesce(t.last_error, ''), 180) as error,
                   t.last_run_at
            from chan_recompute_tasks t
            join symbols s on s.id = t.symbol_id
            where t.status = 'failed'
            order by t.last_run_at desc nulls last
            limit 10
            """
        )
        for row in rows:
            print(dict(row))
    finally:
        await conn.close()


asyncio.run(main())
'@ | python -
    if (-not $Once) {
        Start-Sleep -Seconds $IntervalSeconds
    }
} while (-not $Once)
