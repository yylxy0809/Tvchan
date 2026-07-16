"""Fast, read-only canonical K-line gate using database-side aggregates.

The gate never repairs or transfers K-line rows to Python.  Five workers share
one exported repeatable-read snapshot and persist only per-symbol aggregate
checkpoints into the existing audit control tables.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import uuid
from typing import Any, Sequence

import asyncpg


TIMEFRAMES = (5, 30, 1440, 10080, 43200)
_SNAPSHOT_RE = re.compile(r"^[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9]+$")

PRIMARY_KEY_SQL = """
SELECT c.convalidated, pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
WHERE c.conrelid='klines'::regclass AND c.contype='p'
"""


def _session_invalid(timeframe: int) -> str:
    minute = "(extract(hour from lts)::integer * 60 + extract(minute from lts)::integer)"
    clean = "extract(second from lts) = 0"
    if timeframe == 5:
        valid = (
            f"({minute} = 570 OR ({minute} BETWEEN 575 AND 690 AND ({minute} - 570) % 5 = 0) "
            f"OR ({minute} BETWEEN 785 AND 900 AND ({minute} - 780) % 5 = 0))"
        )
    elif timeframe == 30:
        valid = (
            f"({minute} = 570 OR ({minute} BETWEEN 600 AND 690 AND ({minute} - 570) % 30 = 0) "
            f"OR ({minute} BETWEEN 810 AND 900 AND ({minute} - 780) % 30 = 0))"
        )
    else:
        valid = f"{minute} = 900"
    return f"NOT ({clean} AND {valid})"


def _logical_key(timeframe: int) -> str:
    if timeframe == 1440:
        return "lts::date"
    if timeframe == 10080:
        return "date_trunc('week', lts)"
    if timeframe == 43200:
        return "date_trunc('month', lts)"
    return "ts"


def _expected_sources(timeframe: int) -> str:
    return "(2,4,9)" if timeframe in (5, 30, 1440) else "(8)"


def build_gate_sql(timeframe: int) -> str:
    """Return one whole-timeframe, DB-side checkpoint aggregation."""
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    higher = timeframe in (10080, 43200)
    base_materialization = "MATERIALIZED" if higher else "NOT MATERIALIZED"
    bucket = "week" if timeframe == 10080 else "month"
    daily_ctes = ""
    higher_join = ""
    missing_higher_join = ""
    higher_metrics = "0::bigint AS current_open_periods, 0::bigint AS timestamp_mismatches, 0::bigint AS missing_daily_basis, 0::bigint AS missing_higher_periods"
    if higher:
        daily_ctes = f"""
, daily AS MATERIALIZED (
    SELECT k.symbol_id, k.ts, k.ts AT TIME ZONE 'Asia/Shanghai' AS lts
    FROM klines k
    JOIN symbols s ON s.id = k.symbol_id AND s.is_active AND s.market = 'A_SHARE'
    WHERE k.timeframe = 1440 AND k.is_complete
      AND extract(hour FROM k.ts AT TIME ZONE 'Asia/Shanghai') = 15
      AND extract(minute FROM k.ts AT TIME ZONE 'Asia/Shanghai') = 0
      AND extract(second FROM k.ts AT TIME ZONE 'Asia/Shanghai') = 0
      AND k.volume >= 0 AND (k.amount_x100 IS NULL OR k.amount_x100 >= 0)
      AND k.low_x1000 <= least(k.open_x1000, k.close_x1000, k.high_x1000)
      AND k.high_x1000 >= greatest(k.open_x1000, k.close_x1000, k.low_x1000)
), daily_ends AS (
    SELECT symbol_id, date_trunc('{bucket}', lts) AS period_key, max(ts) AS expected_ts
    FROM daily GROUP BY symbol_id, date_trunc('{bucket}', lts)
), missing_higher AS (
    SELECT d.symbol_id, count(*)::bigint AS missing_higher_periods
    FROM daily_ends d
    LEFT JOIN base b ON b.symbol_id=d.symbol_id
      AND date_trunc('{bucket}', b.lts)=d.period_key
    WHERE d.period_key < date_trunc('{bucket}', now() AT TIME ZONE 'Asia/Shanghai')
      AND b.symbol_id IS NULL
    GROUP BY d.symbol_id
)"""
        higher_join = "LEFT JOIN daily_ends d ON d.symbol_id = b.symbol_id AND d.period_key = date_trunc('%s', b.lts)" % bucket
        missing_higher_join = "LEFT JOIN missing_higher mh ON mh.symbol_id = b.symbol_id"
        higher_metrics = f"""count(*) FILTER (WHERE date_trunc('{bucket}', b.lts) >= date_trunc('{bucket}', now() AT TIME ZONE 'Asia/Shanghai'))::bigint AS current_open_periods,
       count(*) FILTER (WHERE d.expected_ts IS NOT NULL AND b.ts IS DISTINCT FROM d.expected_ts)::bigint AS timestamp_mismatches,
       count(*) FILTER (WHERE d.expected_ts IS NULL)::bigint AS missing_daily_basis,
       coalesce(max(mh.missing_higher_periods),0)::bigint AS missing_higher_periods"""
    invalid_session = _session_invalid(timeframe)
    logical_key = _logical_key(timeframe)
    expected_sources = _expected_sources(timeframe)
    if timeframe in (5, 30):
        logical_duplicates_cte = """
, logical_duplicates AS (
    SELECT null::integer AS symbol_id, 0::bigint AS duplicate_rows WHERE false
)"""
    else:
        logical_duplicates_cte = f"""
, logical_duplicates AS (
    SELECT symbol_id, sum(n - 1)::bigint AS duplicate_rows
    FROM (
        SELECT symbol_id, {logical_key} AS logical_key, count(*)::bigint AS n
        FROM base GROUP BY symbol_id, {logical_key} HAVING count(*) > 1
    ) duplicates GROUP BY symbol_id
)"""
    return f"""
WITH base AS {base_materialization} (
    SELECT k.*, k.ts AT TIME ZONE 'Asia/Shanghai' AS lts
    FROM klines k
    JOIN symbols s ON s.id = k.symbol_id AND s.is_active AND s.market = 'A_SHARE'
    WHERE k.timeframe = {timeframe}
), universe AS (
    SELECT id AS symbol_id FROM symbols WHERE is_active AND market='A_SHARE'
){daily_ctes}{logical_duplicates_cte}, metrics AS (
    SELECT b.symbol_id, min(b.ts) AS shard_start, max(b.ts) AS shard_end,
       count(*)::bigint AS rows_scanned,
       count(*) FILTER (WHERE b.low_x1000 > least(b.open_x1000,b.close_x1000,b.high_x1000)
                              OR b.high_x1000 < greatest(b.open_x1000,b.close_x1000,b.low_x1000))::bigint AS invalid_ohlc,
       count(*) FILTER (WHERE b.volume < 0)::bigint AS negative_volume,
       count(*) FILTER (WHERE b.amount_x100 < 0)::bigint AS negative_amount,
       count(*) FILTER (WHERE {invalid_session})::bigint AS illegal_sessions,
       count(*) FILTER (WHERE NOT b.is_complete)::bigint AS incomplete_rows,
       count(*) FILTER (WHERE b.source NOT IN {expected_sources})::bigint AS unexpected_source,
       jsonb_build_object(
         '1',count(*) FILTER (WHERE b.source=1),'2',count(*) FILTER (WHERE b.source=2),
         '3',count(*) FILTER (WHERE b.source=3),'4',count(*) FILTER (WHERE b.source=4),
         '5',count(*) FILTER (WHERE b.source=5),'6',count(*) FILTER (WHERE b.source=6),
         '7',count(*) FILTER (WHERE b.source=7),'8',count(*) FILTER (WHERE b.source=8),
         '9',count(*) FILTER (WHERE b.source=9)) AS sources,
       {higher_metrics}
    FROM base b {higher_join} {missing_higher_join}
    GROUP BY b.symbol_id
), prepared_present AS (
    SELECT m.*, coalesce(d.duplicate_rows,0)::bigint AS logical_duplicate_rows,
       (m.invalid_ohlc + m.negative_volume + m.negative_amount + m.illegal_sessions +
        m.incomplete_rows + m.unexpected_source + m.current_open_periods +
        m.timestamp_mismatches + m.missing_daily_basis + m.missing_higher_periods +
        coalesce(d.duplicate_rows,0))::bigint AS anomaly_total
    FROM metrics m
    LEFT JOIN logical_duplicates d USING (symbol_id)
), prepared (
    symbol_id,shard_start,shard_end,rows_scanned,invalid_ohlc,negative_volume,
    negative_amount,illegal_sessions,incomplete_rows,unexpected_source,sources,
    current_open_periods,timestamp_mismatches,missing_daily_basis,missing_higher_periods,
    logical_duplicate_rows,anomaly_total
) AS (
    SELECT * FROM prepared_present
    UNION ALL
    SELECT u.symbol_id, '-infinity'::timestamptz, '-infinity'::timestamptz,
       0::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint,
       '{{}}'::jsonb, 0::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint, 1::bigint
    FROM universe u LEFT JOIN metrics m USING(symbol_id) WHERE m.symbol_id IS NULL
)
INSERT INTO kline_audit_checkpoints
    (audit_run_id,symbol_id,timeframe,shard_start,shard_end,status,rows_scanned,metadata)
SELECT $1::uuid, symbol_id, {timeframe}, shard_start, shard_end, 'completed', rows_scanned,
       jsonb_build_object(
         'invalid_ohlc',invalid_ohlc,'negative_volume',negative_volume,
         'negative_amount',negative_amount,'illegal_sessions',illegal_sessions,
         'incomplete_rows',incomplete_rows,'logical_duplicate_rows',logical_duplicate_rows,
         'unexpected_source',unexpected_source,'current_open_periods',current_open_periods,
         'timestamp_mismatches',timestamp_mismatches,'missing_daily_basis',missing_daily_basis,
         'missing_higher_periods',missing_higher_periods,
         'missing_rows',CASE WHEN rows_scanned=0 THEN 1 ELSE 0 END,
         'sources',sources,'disposition',CASE WHEN anomaly_total=0 THEN 'eligible' ELSE 'unresolved' END)
FROM prepared
ON CONFLICT (audit_run_id,symbol_id,timeframe,shard_start,shard_end) DO UPDATE SET
 status=excluded.status, rows_scanned=excluded.rows_scanned,
 metadata=excluded.metadata, updated_at=now()
"""


SUMMARY_SQL = """
SELECT count(*)::bigint AS checkpoints, coalesce(sum(rows_scanned),0)::bigint AS rows_scanned,
       count(*) FILTER (WHERE metadata->>'disposition'='eligible')::bigint AS eligible,
       count(*) FILTER (WHERE metadata->>'disposition'='unresolved')::bigint AS unresolved,
       coalesce(sum((metadata->>'invalid_ohlc')::bigint),0)::bigint AS invalid_ohlc,
       coalesce(sum((metadata->>'negative_volume')::bigint),0)::bigint AS negative_volume,
       coalesce(sum((metadata->>'negative_amount')::bigint),0)::bigint AS negative_amount,
       coalesce(sum((metadata->>'illegal_sessions')::bigint),0)::bigint AS illegal_sessions,
       coalesce(sum((metadata->>'incomplete_rows')::bigint),0)::bigint AS incomplete_rows,
       coalesce(sum((metadata->>'logical_duplicate_rows')::bigint),0)::bigint AS logical_duplicate_rows,
       coalesce(sum((metadata->>'unexpected_source')::bigint),0)::bigint AS unexpected_source,
       coalesce(sum((metadata->>'current_open_periods')::bigint),0)::bigint AS current_open_periods,
       coalesce(sum((metadata->>'timestamp_mismatches')::bigint),0)::bigint AS timestamp_mismatches,
       coalesce(sum((metadata->>'missing_daily_basis')::bigint),0)::bigint AS missing_daily_basis
       ,coalesce(sum((metadata->>'missing_higher_periods')::bigint),0)::bigint AS missing_higher_periods
       ,coalesce(sum((metadata->>'missing_rows')::bigint),0)::bigint AS missing_rows
FROM kline_audit_checkpoints WHERE audit_run_id=$1::uuid
"""

ANOMALY_FIELDS = (
    "invalid_ohlc", "negative_volume", "negative_amount", "illegal_sessions",
    "incomplete_rows", "logical_duplicate_rows", "unexpected_source",
    "current_open_periods", "timestamp_mismatches", "missing_daily_basis",
    "missing_higher_periods",
    "missing_rows",
)


def summarize(record: Any) -> tuple[str, dict[str, Any]]:
    summary = {key: int(value or 0) for key, value in dict(record).items()}
    summary["anomaly_total"] = sum(summary[key] for key in ANOMALY_FIELDS)
    summary["gate_pass"] = summary["anomaly_total"] == 0
    return "completed", summary


async def _worker(database_url: str, snapshot: str, run_id: str, timeframe: int) -> None:
    if not _SNAPSHOT_RE.fullmatch(snapshot):
        raise ValueError("invalid PostgreSQL snapshot identifier")
    connection = await asyncpg.connect(database_url)
    try:
        transaction = connection.transaction(isolation="repeatable_read")
        await transaction.start()
        try:
            await connection.execute(f"SET TRANSACTION SNAPSHOT '{snapshot}'")
            await connection.execute("SET LOCAL max_parallel_workers_per_gather = 4")
            await connection.execute(build_gate_sql(timeframe), uuid.UUID(run_id), timeout=None)
        except BaseException:
            await transaction.rollback()
            raise
        else:
            await transaction.commit()
    finally:
        await connection.close()


async def run_gate(
    database_url: str,
    run_id: str | None = None,
    timeframes: Sequence[int] = TIMEFRAMES,
) -> tuple[str, dict[str, Any]]:
    run_id = run_id or str(uuid.uuid4())
    run_uuid = uuid.UUID(run_id)
    setup = await asyncpg.connect(database_url)
    try:
        primary_key = await setup.fetchrow(PRIMARY_KEY_SQL)
        if not primary_key or not primary_key["convalidated"] or "(symbol_id, timeframe, ts)" not in primary_key["definition"]:
            raise RuntimeError("klines canonical primary key is absent or unvalidated")
        await setup.execute(
            "INSERT INTO kline_audit_runs(audit_run_id,status,apply_mode,parameters) "
            "VALUES($1,'running',false,$2::jsonb) "
            "ON CONFLICT (audit_run_id) DO UPDATE SET status='running',completed_at=NULL,"
            "failure=NULL,summary='{}'::jsonb,parameters=excluded.parameters",
            run_uuid, json.dumps({"engine": "sql_gate", "timeframes": list(timeframes)}),
        )
    finally:
        await setup.close()

    coordinator = await asyncpg.connect(database_url)
    coordinator_tx = coordinator.transaction(isolation="repeatable_read", readonly=True)
    coordinator_open = False
    try:
        await coordinator_tx.start()
        coordinator_open = True
        snapshot = await coordinator.fetchval("SELECT pg_export_snapshot()")
        async with asyncio.TaskGroup() as workers:
            for timeframe in timeframes:
                workers.create_task(_worker(database_url, snapshot, run_id, timeframe))
        await coordinator_tx.commit()
        coordinator_open = False
    except BaseException as error:
        if coordinator_open:
            try:
                await coordinator_tx.rollback()
            except Exception:
                pass
        failure = await asyncpg.connect(database_url)
        try:
            await failure.execute(
                "UPDATE kline_audit_runs SET status='failed',completed_at=now(),failure=$2 WHERE audit_run_id=$1",
                run_uuid, str(error),
            )
        finally:
            await failure.close()
        raise
    finally:
        await coordinator.close()

    final = await asyncpg.connect(database_url)
    try:
        status, summary = summarize(await final.fetchrow(SUMMARY_SQL, run_uuid))
        await final.execute(
            "UPDATE kline_audit_runs SET status=$2,completed_at=now(),summary=$3::jsonb,failure=$4 WHERE audit_run_id=$1",
            run_uuid, status, json.dumps(summary), None if status == "completed" else "canonical SQL gate found unresolved rows",
        )
        return run_id, summary
    finally:
        await final.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast database-side canonical K-line gate")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--audit-run-id")
    parser.add_argument("--timeframe", action="append", type=int, choices=TIMEFRAMES)
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    if args.audit_run_id:
        try:
            args.audit_run_id = str(uuid.UUID(args.audit_run_id))
        except ValueError:
            parser.error("--audit-run-id must be a UUID")
    return args


async def _main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    run_id, summary = await run_gate(
        args.database_url,
        args.audit_run_id,
        tuple(args.timeframe) if args.timeframe else TIMEFRAMES,
    )
    print(json.dumps({"audit_run_id": run_id, "summary": summary}, sort_keys=True))
    if not summary["gate_pass"]:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(_main())
