from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Iterable

import asyncpg

from collector.storage.postgres import source_priority_case


TIMEFRAME_CODES = {
    "1w": 10080,
    "1m": 43200,
}

BUCKET_EXPRESSIONS = {
    "1w": "date_trunc('week', canonical_ts AT TIME ZONE 'Asia/Shanghai')",
    "1m": "date_trunc('month', canonical_ts AT TIME ZONE 'Asia/Shanghai')",
}

READABLE_SOURCES = "ARRAY[2,3,4,5,6,7,8,9]::smallint[]"

ACTIVE_SYMBOL_IDS_SQL = """
SELECT id
FROM symbols
WHERE is_active = TRUE
ORDER BY id
"""


def _emit(event: str, **payload: object) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **payload,
    }
    print(json.dumps(record, ensure_ascii=False), flush=True)


def _parse_status_count(status: str) -> int | None:
    match = re.search(r"(\d+)$", status or "")
    return int(match.group(1)) if match else None


def _parse_timeframes(raw: str) -> list[str]:
    values = [item.strip().lower() for item in raw.split(",") if item.strip()]
    invalid = [item for item in values if item not in TIMEFRAME_CODES]
    if invalid:
        raise argparse.ArgumentTypeError(f"unsupported timeframes: {','.join(invalid)}")
    return values


def _chunks(values: Sequence[int], size: int) -> Iterable[tuple[int, list[int]]]:
    for start in range(0, len(values), size):
        yield start // size + 1, list(values[start : start + size])


def _build_aggregate_sql(bucket_expr: str, *, repair_stale_periods: bool = True) -> str:
    period = "week" if "week" in bucket_expr else "month"
    existing_bucket_expr = bucket_expr.replace("canonical_ts", "existing.ts")
    delete_stale_periods = f"""
, deleted as (
    DELETE FROM klines existing
    USING agg
    WHERE existing.symbol_id = agg.symbol_id
      AND existing.timeframe = $1::integer
      AND existing.source = $2::smallint
      AND existing.ts <> agg.ts
      AND {existing_bucket_expr} = agg.bucket
)
""" if repair_stale_periods else ""
    return f"""
WITH daily_candidates AS (
    SELECT
        k.symbol_id,
        (date_trunc('day', k.ts AT TIME ZONE 'Asia/Shanghai') + interval '15 hours')
            AT TIME ZONE 'Asia/Shanghai' AS canonical_ts,
        k.open_x1000,
        k.high_x1000,
        k.low_x1000,
        k.close_x1000,
        k.volume,
        k.amount_x100,
        k.is_complete,
        k.revision,
        k.source,
        k.updated_at
    FROM klines k
    JOIN symbols s ON s.id = k.symbol_id
    WHERE s.is_active = TRUE
      AND k.timeframe = 1440
      AND k.source = ANY({READABLE_SOURCES})
      AND k.symbol_id >= $3::integer
      AND k.symbol_id <= $4::integer
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY symbol_id, canonical_ts
        ORDER BY ({source_priority_case('source')}) DESC,
                 is_complete DESC, revision DESC, updated_at DESC
    ) AS rn
    FROM daily_candidates
), daily AS (
    SELECT
        symbol_id,
        canonical_ts AS ts,
        open_x1000,
        high_x1000,
        low_x1000,
        close_x1000,
        volume,
        amount_x100,
        is_complete,
        revision,
        {bucket_expr} AS bucket
    FROM ranked
    WHERE ranked.rn = 1
),
agg AS (
    SELECT
        symbol_id,
        MAX(ts) AS ts,
        (ARRAY_AGG(open_x1000 ORDER BY ts ASC))[1] AS open_x1000,
        MAX(high_x1000) AS high_x1000,
        MIN(low_x1000) AS low_x1000,
        (ARRAY_AGG(close_x1000 ORDER BY ts DESC))[1] AS close_x1000,
        SUM(volume)::bigint AS volume,
        CASE
            WHEN BOOL_OR(amount_x100 IS NOT NULL)
            THEN SUM(COALESCE(amount_x100, 0))::bigint
            ELSE NULL
        END AS amount_x100,
        BOOL_AND(is_complete) AS is_complete,
        MAX(revision) AS revision,
        bucket
    FROM daily
    WHERE bucket < date_trunc('{period}', now() AT TIME ZONE 'Asia/Shanghai')
    GROUP BY symbol_id, bucket
){delete_stale_periods}
INSERT INTO klines (
    symbol_id,
    timeframe,
    ts,
    open_x1000,
    high_x1000,
    low_x1000,
    close_x1000,
    volume,
    amount_x100,
    is_complete,
    revision,
    source,
    created_at,
    updated_at
)
SELECT
    symbol_id,
    $1::integer AS timeframe,
    ts,
    open_x1000,
    high_x1000,
    low_x1000,
    close_x1000,
    volume,
    amount_x100,
    is_complete,
    revision,
    $2::smallint AS source,
    NOW(),
    NOW()
FROM agg
ON CONFLICT (symbol_id, timeframe, ts) DO UPDATE SET
    open_x1000 = EXCLUDED.open_x1000,
    high_x1000 = EXCLUDED.high_x1000,
    low_x1000 = EXCLUDED.low_x1000,
    close_x1000 = EXCLUDED.close_x1000,
    volume = EXCLUDED.volume,
    amount_x100 = EXCLUDED.amount_x100,
    is_complete = EXCLUDED.is_complete,
    revision = EXCLUDED.revision,
    source = EXCLUDED.source,
    updated_at = NOW()
WHERE ({source_priority_case('EXCLUDED.source')}) > ({source_priority_case('klines.source')})
   OR (
        ({source_priority_case('EXCLUDED.source')}) = ({source_priority_case('klines.source')})
        AND (
            EXCLUDED.revision > klines.revision
            OR (EXCLUDED.is_complete AND NOT klines.is_complete)
            OR klines.open_x1000 IS DISTINCT FROM EXCLUDED.open_x1000
            OR klines.high_x1000 IS DISTINCT FROM EXCLUDED.high_x1000
            OR klines.low_x1000 IS DISTINCT FROM EXCLUDED.low_x1000
            OR klines.close_x1000 IS DISTINCT FROM EXCLUDED.close_x1000
            OR klines.volume IS DISTINCT FROM EXCLUDED.volume
            OR klines.amount_x100 IS DISTINCT FROM EXCLUDED.amount_x100
            OR klines.is_complete IS DISTINCT FROM EXCLUDED.is_complete
        )
   )
"""


WATERMARK_SQL = """
INSERT INTO scheme2_ingest_watermarks (
    symbol_id,
    timeframe,
    last_bar_end,
    source,
    updated_at
)
SELECT
    k.symbol_id,
    $1::integer AS timeframe,
    MAX(k.ts) AS last_bar_end,
    'derived_1d' AS source,
    NOW()
FROM klines k
JOIN symbols s ON s.id = k.symbol_id
WHERE s.is_active = TRUE
  AND k.timeframe = $1::integer
  AND k.source = $2::smallint
  AND k.symbol_id >= $3::integer
  AND k.symbol_id <= $4::integer
GROUP BY k.symbol_id
ON CONFLICT (symbol_id, timeframe) DO UPDATE SET
    last_bar_end = EXCLUDED.last_bar_end,
    source = EXCLUDED.source,
    updated_at = NOW()
"""


COUNT_SQL = """
SELECT
    COUNT(*) FILTER (WHERE wm.last_bar_end = latest.latest_bar_end) AS at_latest,
    COUNT(wm.symbol_id) AS with_watermark,
    COUNT(s.id) AS active_symbols,
    MIN(wm.last_bar_end) AS min_watermark,
    MAX(wm.last_bar_end) AS max_watermark
FROM symbols s
LEFT JOIN scheme2_ingest_watermarks wm
  ON wm.symbol_id = s.id AND wm.timeframe = $1::integer
CROSS JOIN (
    SELECT MAX(ts) AS latest_bar_end
    FROM klines
    WHERE timeframe = $1::integer
) latest
WHERE s.is_active = TRUE
"""

BATCH_WATERMARK_COUNT_SQL = """
WITH daily AS (
    SELECT k.symbol_id, MAX(k.updated_at) AS max_updated_at, MAX(k.revision) AS max_revision
    FROM klines k
    WHERE k.symbol_id = ANY($2::integer[])
      AND k.timeframe = 1440
      AND k.source = ANY(ARRAY[2,3,4,5,6,7,8,9]::smallint[])
    GROUP BY k.symbol_id
), target AS (
    SELECT k.symbol_id, MAX(k.ts) AS max_ts, MAX(k.updated_at) AS max_updated_at, MAX(k.revision) AS max_revision
    FROM klines k
    WHERE k.symbol_id = ANY($2::integer[])
      AND k.timeframe = $1::integer
      AND k.source = 8
    GROUP BY k.symbol_id
)
SELECT COUNT(*)::integer
FROM UNNEST($2::integer[]) AS batch(symbol_id)
JOIN scheme2_ingest_watermarks wm ON wm.symbol_id = batch.symbol_id AND wm.timeframe = $1::integer
JOIN target ON target.symbol_id = batch.symbol_id
JOIN daily ON daily.symbol_id = batch.symbol_id
WHERE wm.last_bar_end = target.max_ts
  AND target.max_updated_at >= daily.max_updated_at
  AND target.max_revision >= daily.max_revision
"""


async def _aggregate_one(
    pool: asyncpg.Pool,
    name: str,
    source_code: int,
    statement_timeout: float | None,
    symbol_ids: Sequence[int],
    batch_size: int,
    concurrency: int,
    skip_complete_batches: bool,
    repair_stale_periods: bool,
) -> None:
    target_code = TIMEFRAME_CODES[name]
    sql = _build_aggregate_sql(BUCKET_EXPRESSIONS[name], repair_stale_periods=repair_stale_periods)

    started = time.perf_counter()
    total_batches = (len(symbol_ids) + batch_size - 1) // batch_size
    semaphore = asyncio.Semaphore(concurrency)
    _emit(
        "aggregate_timeframe_started",
        timeframe=name,
        timeframe_code=target_code,
        active_symbols=len(symbol_ids),
        batch_size=batch_size,
        concurrency=concurrency,
        total_batches=total_batches,
    )

    async def run_batch(batch_index: int, batch_symbol_ids: list[int]) -> int:
        async with semaphore:
            batch_started = time.perf_counter()
            symbol_id_min = min(batch_symbol_ids)
            symbol_id_max = max(batch_symbol_ids)
            if skip_complete_batches:
                async with pool.acquire() as conn:
                    completed = await conn.fetchval(
                        BATCH_WATERMARK_COUNT_SQL,
                        target_code,
                        batch_symbol_ids,
                        timeout=statement_timeout,
                    )
                if completed == len(batch_symbol_ids):
                    _emit(
                        "aggregate_timeframe_batch_skipped",
                        timeframe=name,
                        timeframe_code=target_code,
                        batch_index=batch_index,
                        total_batches=total_batches,
                        symbols=len(batch_symbol_ids),
                        symbol_id_min=symbol_id_min,
                        symbol_id_max=symbol_id_max,
                        reason="complete_watermarks",
                    )
                    return 0

            _emit(
                "aggregate_timeframe_batch_started",
                timeframe=name,
                timeframe_code=target_code,
                batch_index=batch_index,
                total_batches=total_batches,
                symbols=len(batch_symbol_ids),
                symbol_id_min=symbol_id_min,
                symbol_id_max=symbol_id_max,
            )

            async with pool.acquire() as conn:
                async with conn.transaction():
                    status = await conn.execute(
                        sql,
                        target_code,
                        source_code,
                        symbol_id_min,
                        symbol_id_max,
                        timeout=statement_timeout,
                    )
                    rows = _parse_status_count(status) or 0

                    wm_status = await conn.execute(
                        WATERMARK_SQL,
                        target_code,
                        source_code,
                        symbol_id_min,
                        symbol_id_max,
                        timeout=statement_timeout,
                    )

            _emit(
                "aggregate_timeframe_batch_finished",
                timeframe=name,
                timeframe_code=target_code,
                batch_index=batch_index,
                total_batches=total_batches,
                rows=rows,
                watermarks=_parse_status_count(wm_status),
                elapsed_sec=round(time.perf_counter() - batch_started, 3),
            )
            return rows

    tasks = [
        asyncio.create_task(run_batch(batch_index, batch_symbol_ids))
        for batch_index, batch_symbol_ids in _chunks(symbol_ids, batch_size)
    ]
    total_rows = sum(await asyncio.gather(*tasks))

    async with pool.acquire() as conn:
        counts = await conn.fetchrow(COUNT_SQL, target_code, timeout=statement_timeout)
    _emit(
        "aggregate_timeframe_finished",
        timeframe=name,
        timeframe_code=target_code,
        rows=total_rows,
        at_latest=counts["at_latest"],
        with_watermark=counts["with_watermark"],
        active_symbols=counts["active_symbols"],
        min_watermark=str(counts["min_watermark"]) if counts["min_watermark"] else None,
        max_watermark=str(counts["max_watermark"]) if counts["max_watermark"] else None,
        elapsed_sec=round(time.perf_counter() - started, 3),
    )


async def _main(argv: Iterable[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Aggregate full-history weekly/monthly K lines from active-symbol daily K lines."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "postgresql://app:app@127.0.0.1:5432/market"),
    )
    parser.add_argument("--timeframes", type=_parse_timeframes, default=["1w", "1m"])
    parser.add_argument(
        "--source-code",
        type=int,
        default=8,
        help="klines.source code for derived bars; source 8 is already readable by API paths.",
    )
    parser.add_argument(
        "--statement-timeout",
        type=float,
        default=None,
        help="Per SQL statement timeout in seconds. Default disables asyncpg timeout.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=150,
        help="Number of active symbols per aggregation batch.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Number of aggregation batches to run concurrently.",
    )
    parser.add_argument(
        "--skip-complete-batches",
        action="store_true",
        help="Skip a batch when every active symbol in it already has a target timeframe watermark.",
    )
    parser.add_argument(
        "--skip-stale-period-delete",
        action="store_true",
        help="First-build optimization: skip deleting old derived bars; only safe when no target source rows exist.",
    )
    args = parser.parse_args(list(argv))
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if args.concurrency <= 0:
        parser.error("--concurrency must be positive")

    _emit(
        "aggregate_from_daily_started",
        timeframes=args.timeframes,
        source_code=args.source_code,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        skip_complete_batches=args.skip_complete_batches,
    )

    pool = await asyncpg.create_pool(
        args.database_url,
        min_size=1,
        max_size=args.concurrency,
        command_timeout=args.statement_timeout,
    )
    try:
        async with pool.acquire() as conn:
            symbol_ids = [row["id"] for row in await conn.fetch(ACTIVE_SYMBOL_IDS_SQL)]
        _emit("aggregate_active_symbols_loaded", active_symbols=len(symbol_ids))
        for timeframe in args.timeframes:
            await _aggregate_one(
                pool,
                timeframe,
                args.source_code,
                args.statement_timeout,
                symbol_ids,
                args.batch_size,
                args.concurrency,
                args.skip_complete_batches,
                not args.skip_stale_period_delete,
            )
    finally:
        await pool.close()

    _emit("aggregate_from_daily_finished", timeframes=args.timeframes)
    return 0


def main() -> int:
    return asyncio.run(_main(sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
