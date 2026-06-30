param(
    [string]$DatabaseUrl = $env:DATABASE_URL,
    [switch]$Execute
)

if (-not $DatabaseUrl) {
    $DatabaseUrl = "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local"
}

$ErrorActionPreference = "Stop"
$env:CLEAR_CHAN_DATABASE_URL = $DatabaseUrl
$env:CLEAR_CHAN_EXECUTE = if ($Execute) { "1" } else { "0" }

@'
import asyncio
import os

import asyncpg

TABLES = [
    "scheme2_chan_published_heads",
    "scheme2_chan_recompute_watermarks",
    "chan_recompute_tasks",
    "chan_runs",
    "chan_strokes",
    "chan_segments",
    "chan_centers",
    "chan_signals",
]


async def count_rows(conn, phase):
    for table in TABLES:
        rows = await conn.fetchval(f"select count(*)::bigint from {table}")
        print(f"{phase}\t{table}\t{rows}", flush=True)


async def main():
    database_url = os.environ["CLEAR_CHAN_DATABASE_URL"]
    execute = os.getenv("CLEAR_CHAN_EXECUTE") == "1"
    conn = await asyncpg.connect(database_url)
    try:
        await count_rows(conn, "before")
        if not execute:
            print("dry_run\tset -Execute to clear Chan compute tables", flush=True)
            return
        await conn.execute(
            """
            truncate table
                scheme2_chan_published_heads,
                scheme2_chan_recompute_watermarks,
                chan_recompute_tasks,
                chan_strokes,
                chan_segments,
                chan_centers,
                chan_signals,
                chan_runs
            restart identity
            """
        )
        await count_rows(conn, "after")
    finally:
        await conn.close()


asyncio.run(main())
'@ | python -
