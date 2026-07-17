"""Fast, read-only canonical K-line gate using database-side aggregates.

The gate never repairs or transfers K-line rows to Python.  Five workers share
one exported repeatable-read snapshot and persist only per-symbol aggregate
checkpoints into the existing audit control tables.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import uuid
from datetime import datetime
from typing import Any, Sequence

import asyncpg


TIMEFRAMES = (5, 30, 1440, 10080, 43200)
EVIDENCE_CONTRACT_VERSION = "module-c-strict-audit-v2"
AUDIT_LOCK_PROTOCOL_VERSION = "audit-uuid-advisory-v2"
LOCK_WATCHDOG_INTERVAL_SECONDS = 30.0
LOCK_WATCHDOG_TIMEOUT_SECONDS = 5.0
_SNAPSHOT_RE = re.compile(r"^[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9]+$")

PRIMARY_KEY_SQL = """
SELECT c.convalidated, pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
WHERE c.conrelid='klines'::regclass AND c.contype='p'
"""

EVIDENCE_CLOCK_SQL = """
SELECT transaction_timestamp() AS observed_at,
       pg_current_wal_lsn()::text AS observed_wal_lsn,
       txid_current_snapshot()::text AS transaction_snapshot
"""

ACTIVE_UNIVERSE_SQL = """
SELECT id AS symbol_id, code, exchange
FROM symbols
WHERE is_active AND market='A_SHARE'
ORDER BY id
"""

ACTIVE_CATALOG_GENERATION_SQL = """
SELECT control.active_generation_id AS generation_id,
       control.revision,
       generation.status,
       generation.expected_scope_count,
       generation.symbol_ids,
       generation.timeframes
FROM kline_scope_catalog_control control
JOIN kline_scope_catalog_generations generation
  ON generation.generation_id=control.active_generation_id
WHERE control.control_key='active'
  AND generation.status='complete'
"""

ACTIVE_CATALOG_MANIFEST_SQL = """
SELECT symbol_id, timeframe, state, bounds_complete, min_ts, max_ts, updated_at
FROM kline_scope_catalog
WHERE generation_id=$1
  AND symbol_id=ANY($2::integer[])
  AND timeframe=ANY($3::integer[])
ORDER BY symbol_id, timeframe
"""


class AuditRunAlreadyClaimed(RuntimeError):
    pass


class InvalidAuditEvidence(RuntimeError):
    pass


class AuditLockOwnershipLost(RuntimeError):
    pass


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _manifest_sha256(records: Sequence[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for record in records:
        digest.update(
            json.dumps(
                _json_value(record), ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _normalize_timeframes(timeframes: Sequence[int]) -> tuple[int, ...]:
    values = tuple(int(value) for value in timeframes)
    if len(values) != len(TIMEFRAMES) or set(values) != set(TIMEFRAMES):
        raise ValueError("canonical SQL gate requires the exact five Module C timeframes")
    return TIMEFRAMES


def _advisory_lock_keys(run_id: uuid.UUID) -> tuple[int, int]:
    """Derive distinct claim and writer-fence keys for one audit UUID."""
    def derive(purpose: bytes) -> int:
        digest = hashlib.sha256(purpose + b":" + run_id.bytes).digest()
        return int.from_bytes(digest[:8], byteorder="big", signed=True)

    claim_key = derive(b"claim")
    writer_fence_key = derive(b"writer-fence")
    if claim_key == writer_fence_key:
        raise RuntimeError("audit advisory-lock key derivation collided")
    return claim_key, writer_fence_key


async def _claim_audit_run(
    conn: Any,
    run_id: uuid.UUID,
    lock_owner_id: str,
) -> None:
    """Claim or recover an incomplete run while its UUID advisory lock is held."""
    lock_owner_id = str(uuid.UUID(lock_owner_id))
    pending = json.dumps({
        "contract_version": EVIDENCE_CONTRACT_VERSION,
        "engine": "sql_gate",
        "apply_mode": False,
        "timeframes": list(TIMEFRAMES),
        "evidence_status": "pending",
        "lock_protocol_version": AUDIT_LOCK_PROTOCOL_VERSION,
        "lock_owner_id": lock_owner_id,
    }, sort_keys=True)
    async with conn.transaction():
        existing = await conn.fetchrow(
            "SELECT status,parameters FROM kline_audit_runs "
            "WHERE audit_run_id=$1 FOR UPDATE",
            run_id,
        )
        if existing is None:
            await conn.execute(
                "INSERT INTO kline_audit_runs(audit_run_id,status,apply_mode,parameters) "
                "VALUES($1,'running',false,$2::jsonb)",
                run_id,
                pending,
            )
            return
        status = str(existing["status"])
        if status == "completed":
            raise AuditRunAlreadyClaimed(
                f"canonical audit {run_id} is already {status}; use a new UUID"
            )
        if status == "running":
            raw_parameters = existing["parameters"]
            if isinstance(raw_parameters, str):
                try:
                    decoded_parameters = json.loads(raw_parameters)
                except json.JSONDecodeError:
                    parameters = {}
                else:
                    parameters = (
                        decoded_parameters
                        if isinstance(decoded_parameters, dict)
                        else {}
                    )
            elif isinstance(raw_parameters, dict):
                parameters = raw_parameters
            else:
                parameters = {}
            if parameters.get("lock_protocol_version") != AUDIT_LOCK_PROTOCOL_VERSION:
                raise AuditRunAlreadyClaimed(
                    f"canonical audit {run_id} is legacy running evidence; "
                    "operator confirmation is required before recovery"
                )
        if status not in {"running", "failed"}:
            raise AuditRunAlreadyClaimed(
                f"canonical audit {run_id} has unsupported status {status}"
            )
        await conn.execute(
            "DELETE FROM kline_audit_checkpoints WHERE audit_run_id=$1",
            run_id,
        )
        await conn.execute(
            "UPDATE kline_audit_runs SET started_at=now(),completed_at=NULL,status='running',"
            "apply_mode=false,parameters=$2::jsonb,summary='{}'::jsonb,failure=NULL "
            "WHERE audit_run_id=$1 AND status IN ('running','failed')",
            run_id,
            pending,
        )


async def _capture_snapshot_evidence(conn: Any) -> dict[str, Any]:
    """Capture strict audit evidence from the coordinator's exported snapshot."""
    clock = await conn.fetchrow(EVIDENCE_CLOCK_SQL)
    if (
        clock is None
        or clock["observed_at"] is None
        or not clock["observed_wal_lsn"]
        or not clock["transaction_snapshot"]
    ):
        raise InvalidAuditEvidence("audit snapshot clock/LSN evidence is missing")
    universe_rows = await conn.fetch(ACTIVE_UNIVERSE_SQL)
    universe = [
        {
            "symbol_id": int(row["symbol_id"]),
            "symbol": f"{row['code']}.{str(row['exchange']).upper()}",
        }
        for row in universe_rows
    ]
    if not universe:
        raise InvalidAuditEvidence("active A-share universe is empty")

    generation = await conn.fetchrow(ACTIVE_CATALOG_GENERATION_SQL)
    if generation is None:
        raise InvalidAuditEvidence("active complete K-line scope catalog generation is missing")
    generation_id = generation["generation_id"]
    universe_ids = [record["symbol_id"] for record in universe]
    generation_symbols = {int(value) for value in generation["symbol_ids"]}
    generation_timeframes = {int(value) for value in generation["timeframes"]}
    # A complete generation may retain scopes for symbols deactivated after it
    # was built.  Strict evidence binds only the current active audit universe
    # and requires its five-level scope subset to be exact; inactive extras are
    # deliberately outside the ordered manifest hash.
    if not set(universe_ids).issubset(generation_symbols):
        raise InvalidAuditEvidence("active catalog generation does not cover the active universe")
    if not set(TIMEFRAMES).issubset(generation_timeframes):
        raise InvalidAuditEvidence("active catalog generation does not cover the exact five levels")

    catalog_rows = await conn.fetch(
        ACTIVE_CATALOG_MANIFEST_SQL,
        generation_id,
        universe_ids,
        list(TIMEFRAMES),
    )
    catalog: list[dict[str, Any]] = []
    for row in catalog_rows:
        state = str(row["state"])
        min_ts = row["min_ts"]
        max_ts = row["max_ts"]
        valid = bool(row["bounds_complete"]) and (
            (state == "empty" and min_ts is None and max_ts is None)
            or (
                state == "present"
                and min_ts is not None
                and max_ts is not None
                and min_ts <= max_ts
            )
        )
        if not valid:
            raise InvalidAuditEvidence(
                "active catalog contains unknown or incomplete required scope evidence"
            )
        catalog.append({
            "symbol_id": int(row["symbol_id"]),
            "timeframe": int(row["timeframe"]),
            "state": state,
            "bounds_complete": True,
            "min_ts": _json_value(min_ts),
            "max_ts": _json_value(max_ts),
            "updated_at": _json_value(row["updated_at"]),
        })
    expected_keys = {
        (symbol_id, timeframe)
        for symbol_id in universe_ids
        for timeframe in TIMEFRAMES
    }
    observed_keys = {
        (record["symbol_id"], record["timeframe"])
        for record in catalog
    }
    if len(catalog) != len(expected_keys) or observed_keys != expected_keys:
        raise InvalidAuditEvidence(
            "active catalog scope manifest is not exact for the audit universe"
        )

    evidence: dict[str, Any] = {
        "contract_version": EVIDENCE_CONTRACT_VERSION,
        "engine": "sql_gate",
        "apply_mode": False,
        "timeframes": list(TIMEFRAMES),
        "observed_at": _json_value(clock["observed_at"]),
        # This is diagnostic context, not a snapshot boundary.  The exported
        # snapshot and transaction_snapshot carry the visibility contract.
        "observed_wal_lsn": str(clock["observed_wal_lsn"]),
        "transaction_snapshot": str(clock["transaction_snapshot"]),
        "active_universe_count": len(universe),
        "active_universe_sha256": _manifest_sha256(universe),
        "catalog_generation_id": str(generation_id),
        "catalog_control_revision": int(generation["revision"]),
        "catalog_expected_scope_count": int(generation["expected_scope_count"]),
        "catalog_required_scope_count": len(catalog),
        "catalog_manifest_sha256": _manifest_sha256(catalog),
    }
    evidence["evidence_sha256"] = _manifest_sha256([evidence])
    return evidence


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
    WHERE d.period_key < date_trunc(
        '{bucket}', (SELECT observed_at FROM evidence_context) AT TIME ZONE 'Asia/Shanghai'
    )
      AND b.symbol_id IS NULL
    GROUP BY d.symbol_id
)"""
        higher_join = "LEFT JOIN daily_ends d ON d.symbol_id = b.symbol_id AND d.period_key = date_trunc('%s', b.lts)" % bucket
        missing_higher_join = "LEFT JOIN missing_higher mh ON mh.symbol_id = b.symbol_id"
        higher_metrics = f"""count(*) FILTER (WHERE date_trunc('{bucket}', b.lts) >= date_trunc('{bucket}', (SELECT observed_at FROM evidence_context) AT TIME ZONE 'Asia/Shanghai'))::bigint AS current_open_periods,
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
), catalog_scope AS MATERIALIZED (
    SELECT u.symbol_id, c.state, c.bounds_complete, c.min_ts, c.max_ts,
       CASE WHEN c.symbol_id IS NULL THEN 1 ELSE 0 END::bigint AS catalog_scope_missing,
       CASE WHEN c.symbol_id IS NOT NULL
                  AND c.state IS DISTINCT FROM 'present'
                  AND c.state IS DISTINCT FROM 'empty'
            THEN 1 ELSE 0 END::bigint AS catalog_scope_unknown,
       CASE WHEN c.symbol_id IS NOT NULL AND (
                  c.bounds_complete IS DISTINCT FROM TRUE OR
                  (c.state='empty' AND (c.min_ts IS NOT NULL OR c.max_ts IS NOT NULL)) OR
                  (c.state='present' AND
                    (c.min_ts IS NULL OR c.max_ts IS NULL OR c.min_ts > c.max_ts))
            ) THEN 1 ELSE 0 END::bigint AS catalog_scope_incomplete
    FROM universe u
    LEFT JOIN kline_scope_catalog c
      ON c.generation_id=$3::uuid
     AND c.symbol_id=u.symbol_id
     AND c.timeframe={timeframe}
), evidence_context AS MATERIALIZED (
    SELECT $2::timestamptz AS observed_at
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
), catalog_checked AS (
    SELECT c.symbol_id,
       coalesce(m.shard_start,'-infinity'::timestamptz) AS shard_start,
       coalesce(m.shard_end,'-infinity'::timestamptz) AS shard_end,
       coalesce(m.rows_scanned,0)::bigint AS rows_scanned,
       coalesce(m.invalid_ohlc,0)::bigint AS invalid_ohlc,
       coalesce(m.negative_volume,0)::bigint AS negative_volume,
       coalesce(m.negative_amount,0)::bigint AS negative_amount,
       coalesce(m.illegal_sessions,0)::bigint AS illegal_sessions,
       coalesce(m.incomplete_rows,0)::bigint AS incomplete_rows,
       coalesce(m.unexpected_source,0)::bigint AS unexpected_source,
       coalesce(m.sources,'{{}}'::jsonb) AS sources,
       coalesce(m.current_open_periods,0)::bigint AS current_open_periods,
       coalesce(m.timestamp_mismatches,0)::bigint AS timestamp_mismatches,
       coalesce(m.missing_daily_basis,0)::bigint AS missing_daily_basis,
       coalesce(m.missing_higher_periods,0)::bigint AS missing_higher_periods,
       CASE WHEN c.state='empty' AND m.rows_scanned > 0 THEN 1 ELSE 0 END::bigint
         AS catalog_empty_has_rows,
       CASE WHEN c.state='present' AND coalesce(m.rows_scanned,0)=0
            THEN 1 ELSE 0 END::bigint AS catalog_present_missing_rows,
       CASE WHEN c.state='present' AND coalesce(m.rows_scanned,0)>0 AND
              (c.min_ts IS DISTINCT FROM m.shard_start OR
               c.max_ts IS DISTINCT FROM m.shard_end)
            THEN 1 ELSE 0 END::bigint AS catalog_present_bounds_mismatch,
       c.catalog_scope_missing,
       c.catalog_scope_unknown,
       c.catalog_scope_incomplete
    FROM catalog_scope c
    LEFT JOIN metrics m USING (symbol_id)
), prepared AS (
    SELECT m.*, coalesce(d.duplicate_rows,0)::bigint AS logical_duplicate_rows,
       (m.invalid_ohlc + m.negative_volume + m.negative_amount + m.illegal_sessions +
        m.incomplete_rows + m.unexpected_source + m.current_open_periods +
        m.timestamp_mismatches + m.missing_daily_basis + m.missing_higher_periods +
        m.catalog_empty_has_rows + m.catalog_present_missing_rows +
        m.catalog_present_bounds_mismatch + m.catalog_scope_missing +
        m.catalog_scope_unknown + m.catalog_scope_incomplete +
        CASE WHEN m.rows_scanned=0 THEN 1 ELSE 0 END +
        coalesce(d.duplicate_rows,0))::bigint AS anomaly_total
    FROM catalog_checked m
    LEFT JOIN logical_duplicates d USING (symbol_id)
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
         'catalog_empty_has_rows',catalog_empty_has_rows,
         'catalog_present_missing_rows',catalog_present_missing_rows,
         'catalog_present_bounds_mismatch',catalog_present_bounds_mismatch,
         'catalog_scope_missing',catalog_scope_missing,
         'catalog_scope_unknown',catalog_scope_unknown,
         'catalog_scope_incomplete',catalog_scope_incomplete,
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
       ,coalesce(sum((metadata->>'catalog_empty_has_rows')::bigint),0)::bigint AS catalog_empty_has_rows
       ,coalesce(sum((metadata->>'catalog_present_missing_rows')::bigint),0)::bigint AS catalog_present_missing_rows
       ,coalesce(sum((metadata->>'catalog_present_bounds_mismatch')::bigint),0)::bigint AS catalog_present_bounds_mismatch
       ,coalesce(sum((metadata->>'catalog_scope_missing')::bigint),0)::bigint AS catalog_scope_missing
       ,coalesce(sum((metadata->>'catalog_scope_unknown')::bigint),0)::bigint AS catalog_scope_unknown
       ,coalesce(sum((metadata->>'catalog_scope_incomplete')::bigint),0)::bigint AS catalog_scope_incomplete
       ,coalesce(sum((metadata->>'missing_rows')::bigint),0)::bigint AS missing_rows
FROM kline_audit_checkpoints WHERE audit_run_id=$1::uuid
"""

ANOMALY_FIELDS = (
    "invalid_ohlc", "negative_volume", "negative_amount", "illegal_sessions",
    "incomplete_rows", "logical_duplicate_rows", "unexpected_source",
    "current_open_periods", "timestamp_mismatches", "missing_daily_basis",
    "missing_higher_periods",
    "catalog_empty_has_rows", "catalog_present_missing_rows",
    "catalog_present_bounds_mismatch", "catalog_scope_missing",
    "catalog_scope_unknown", "catalog_scope_incomplete",
    "missing_rows",
)


def summarize(record: Any) -> tuple[str, dict[str, Any]]:
    summary = {key: int(value or 0) for key, value in dict(record).items()}
    summary["anomaly_total"] = sum(summary[key] for key in ANOMALY_FIELDS)
    summary["gate_pass"] = summary["anomaly_total"] == 0
    return "completed", summary


async def _finalize_audit_run(
    conn: Any,
    run_id: uuid.UUID,
    evidence: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    status, summary = summarize(await conn.fetchrow(SUMMARY_SQL, run_id))
    summary.update({
        "contract_version": evidence["contract_version"],
        "observed_at": evidence["observed_at"],
        "observed_wal_lsn": evidence["observed_wal_lsn"],
        "transaction_snapshot": evidence["transaction_snapshot"],
        "evidence_sha256": evidence["evidence_sha256"],
    })
    expected = int(evidence["active_universe_count"]) * len(TIMEFRAMES)
    checkpoints = int(summary["checkpoints"])
    dispositions = int(summary["eligible"]) + int(summary["unresolved"])
    error: str | None = None
    if checkpoints != expected:
        error = f"strict audit checkpoints={checkpoints} expected={expected}"
    elif dispositions != checkpoints:
        error = (
            f"strict audit dispositions={dispositions} checkpoints={checkpoints}"
        )
    if error is not None:
        summary["gate_pass"] = False
        summary["evidence_complete"] = False
        command_tag = await conn.execute(
            "UPDATE kline_audit_runs SET status='failed',completed_at=now(),"
            "summary=$2::jsonb,parameters=$3::jsonb,failure=$4 "
            "WHERE audit_run_id=$1 AND status='running'",
            run_id,
            json.dumps(summary, sort_keys=True),
            json.dumps(evidence, sort_keys=True),
            error,
        )
        if command_tag != "UPDATE 1":
            raise AuditRunAlreadyClaimed(
                f"canonical audit {run_id} lost its running failure fence"
            )
        raise InvalidAuditEvidence(error)

    summary["evidence_complete"] = True
    command_tag = await conn.execute(
        "UPDATE kline_audit_runs SET status=$2,completed_at=now(),summary=$3::jsonb,"
        "failure=$4,parameters=$5::jsonb "
        "WHERE audit_run_id=$1 AND status='running'",
        run_id,
        status,
        json.dumps(summary, sort_keys=True),
        None if status == "completed" else "canonical SQL gate found unresolved rows",
        json.dumps(evidence, sort_keys=True),
    )
    if command_tag != "UPDATE 1":
        raise AuditRunAlreadyClaimed(
            f"canonical audit {run_id} lost its running completion fence"
        )
    return status, summary


async def _worker(
    connection: Any,
    snapshot: str,
    run_id: str,
    timeframe: int,
    observed_at: datetime,
    generation_id: uuid.UUID,
) -> None:
    if not _SNAPSHOT_RE.fullmatch(snapshot):
        raise ValueError("invalid PostgreSQL snapshot identifier")
    transaction = connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        await connection.execute(f"SET TRANSACTION SNAPSHOT '{snapshot}'")
        await connection.execute("SET LOCAL max_parallel_workers_per_gather = 4")
        await connection.execute(
            build_gate_sql(timeframe),
            uuid.UUID(run_id),
            observed_at,
            generation_id,
            timeout=None,
        )
    except BaseException:
        await transaction.rollback()
        raise
    else:
        await transaction.commit()


async def _unlock_advisory(
    connection: Any,
    sql: str,
    lock_key: int,
    description: str,
) -> None:
    try:
        unlocked = await connection.fetchval(sql, lock_key)
    except Exception as error:
        raise AuditLockOwnershipLost(f"{description} unlock failed") from error
    if unlocked is not True:
        raise AuditLockOwnershipLost(f"{description} unlock returned false")


async def _run_fenced_writer(
    database_url: str,
    writer_fence_key: int,
    operation: Any,
) -> Any:
    connection = await asyncpg.connect(database_url)
    lock_acquired = False
    try:
        lock_acquired = bool(
            await connection.fetchval(
                "SELECT pg_try_advisory_lock_shared($1::bigint)",
                writer_fence_key,
            )
        )
        if not lock_acquired:
            raise AuditLockOwnershipLost(
                "audit durable writer could not acquire the shared writer fence"
            )
        return await operation(connection)
    finally:
        try:
            if lock_acquired:
                await _unlock_advisory(
                    connection,
                    "SELECT pg_advisory_unlock_shared($1::bigint)",
                    writer_fence_key,
                    "audit durable writer shared fence",
                )
        finally:
            await connection.close()


async def _run_claimed_gate(
    database_url: str,
    worker_connections: Sequence[Any],
    writer_fence_key: int,
    run_id: str,
    run_uuid: uuid.UUID,
    timeframes: Sequence[int],
) -> tuple[str, dict[str, Any]]:
    coordinator: Any | None = None
    coordinator_tx: Any | None = None
    coordinator_open = False
    evidence: dict[str, Any] | None = None
    try:
        coordinator = await asyncpg.connect(database_url)
        coordinator_tx = coordinator.transaction(isolation="repeatable_read", readonly=True)
        await coordinator_tx.start()
        coordinator_open = True
        snapshot = await coordinator.fetchval("SELECT pg_export_snapshot()")
        evidence = await _capture_snapshot_evidence(coordinator)
        observed_at = datetime.fromisoformat(str(evidence["observed_at"]))
        generation_id = uuid.UUID(str(evidence["catalog_generation_id"]))
        async with asyncio.TaskGroup() as workers:
            for connection, timeframe in zip(
                worker_connections,
                timeframes,
                strict=True,
            ):
                workers.create_task(
                    _worker(
                        connection,
                        snapshot,
                        run_id,
                        timeframe,
                        observed_at,
                        generation_id,
                    )
                )
        await coordinator_tx.commit()
        coordinator_open = False
    except BaseException as error:
        if coordinator_open and coordinator_tx is not None:
            try:
                await coordinator_tx.rollback()
            except Exception:
                pass
        async def mark_failure(failure: Any) -> None:
            await failure.execute(
                "UPDATE kline_audit_runs SET status='failed',completed_at=now(),failure=$2,"
                "parameters=coalesce($3::jsonb,parameters) "
                "WHERE audit_run_id=$1 AND status='running'",
                run_uuid,
                str(error),
                json.dumps(evidence, sort_keys=True) if evidence is not None else None,
            )

        await _run_fenced_writer(database_url, writer_fence_key, mark_failure)
        raise
    finally:
        if coordinator is not None:
            await coordinator.close()

    async def finalize(final: Any) -> tuple[str, dict[str, Any]]:
        if evidence is None:
            raise InvalidAuditEvidence("audit snapshot evidence was not captured")
        _status, summary = await _finalize_audit_run(final, run_uuid, evidence)
        return run_id, summary

    return await _run_fenced_writer(database_url, writer_fence_key, finalize)


async def _watch_lock_session(setup: Any, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            heartbeat = await asyncio.wait_for(
                setup.fetchval("SELECT 1"),
                timeout=LOCK_WATCHDOG_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise AuditLockOwnershipLost(
                "audit advisory-lock watchdog heartbeat failed"
            ) from error
        if heartbeat != 1:
            raise AuditLockOwnershipLost(
                "audit advisory-lock watchdog returned an invalid heartbeat"
            )
        try:
            await asyncio.wait_for(
                stop.wait(),
                timeout=LOCK_WATCHDOG_INTERVAL_SECONDS,
            )
        except TimeoutError:
            pass


async def _run_with_lock_watchdog(
    setup: Any,
    database_url: str,
    worker_connections: Sequence[Any],
    writer_fence_key: int,
    run_id: str,
    run_uuid: uuid.UUID,
    timeframes: Sequence[int],
) -> tuple[str, dict[str, Any]]:
    stop = asyncio.Event()
    gate_task = asyncio.create_task(
        _run_claimed_gate(
            database_url,
            worker_connections,
            writer_fence_key,
            run_id,
            run_uuid,
            timeframes,
        )
    )
    watchdog_task = asyncio.create_task(_watch_lock_session(setup, stop))
    try:
        done, _pending = await asyncio.wait(
            {gate_task, watchdog_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if watchdog_task in done:
            watchdog_error = watchdog_task.exception()
            if watchdog_error is not None:
                gate_task.cancel()
                await asyncio.gather(gate_task, return_exceptions=True)
                raise watchdog_error
            if not gate_task.done():
                gate_task.cancel()
                await asyncio.gather(gate_task, return_exceptions=True)
                raise AuditLockOwnershipLost(
                    "audit advisory-lock watchdog stopped unexpectedly"
                )
        stop.set()
        await watchdog_task
        return await gate_task
    finally:
        stop.set()
        for task in (gate_task, watchdog_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(gate_task, watchdog_task, return_exceptions=True)


async def run_gate(
    database_url: str,
    run_id: str | None = None,
    timeframes: Sequence[int] = TIMEFRAMES,
) -> tuple[str, dict[str, Any]]:
    timeframes = _normalize_timeframes(timeframes)
    run_id = run_id or str(uuid.uuid4())
    run_uuid = uuid.UUID(run_id)
    claim_key, writer_fence_key = _advisory_lock_keys(run_uuid)
    lock_owner_id = str(uuid.uuid4())
    setup = await asyncpg.connect(database_url)
    claim_lock_acquired = False
    fence_exclusive_acquired = False
    worker_connections: list[Any] = []
    worker_shared_locks = 0
    try:
        claim_lock_acquired = bool(
            await setup.fetchval("SELECT pg_try_advisory_lock($1::bigint)", claim_key)
        )
        if not claim_lock_acquired:
            raise AuditRunAlreadyClaimed(
                f"canonical audit {run_id} is already owned by another process"
            )
        fence_exclusive_acquired = bool(
            await setup.fetchval(
                "SELECT pg_try_advisory_lock($1::bigint)",
                writer_fence_key,
            )
        )
        if not fence_exclusive_acquired:
            raise AuditLockOwnershipLost(
                "canonical audit takeover is blocked by an existing writer fence"
            )
        for _timeframe in TIMEFRAMES:
            worker_connections.append(await asyncpg.connect(database_url))
        await _unlock_advisory(
            setup,
            "SELECT pg_advisory_unlock($1::bigint)",
            writer_fence_key,
            "audit exclusive writer fence",
        )
        fence_exclusive_acquired = False
        for connection in worker_connections:
            acquired = bool(
                await connection.fetchval(
                    "SELECT pg_try_advisory_lock_shared($1::bigint)",
                    writer_fence_key,
                )
            )
            if not acquired:
                raise AuditLockOwnershipLost(
                    "audit worker could not acquire the shared writer fence"
                )
            worker_shared_locks += 1
        primary_key = await setup.fetchrow(PRIMARY_KEY_SQL)
        if not primary_key or not primary_key["convalidated"] or "(symbol_id, timeframe, ts)" not in primary_key["definition"]:
            raise RuntimeError("klines canonical primary key is absent or unvalidated")
        await _claim_audit_run(setup, run_uuid, lock_owner_id)
        return await _run_with_lock_watchdog(
            setup,
            database_url,
            worker_connections,
            writer_fence_key,
            run_id,
            run_uuid,
            timeframes,
        )
    finally:
        cleanup_errors: list[BaseException] = []
        for index, connection in reversed(list(enumerate(worker_connections))):
            try:
                if index < worker_shared_locks:
                    await _unlock_advisory(
                        connection,
                        "SELECT pg_advisory_unlock_shared($1::bigint)",
                        writer_fence_key,
                        "audit worker shared writer fence",
                    )
            except BaseException as error:
                cleanup_errors.append(error)
            try:
                await connection.close()
            except BaseException as error:
                cleanup_errors.append(error)
        try:
            if fence_exclusive_acquired:
                try:
                    await _unlock_advisory(
                        setup,
                        "SELECT pg_advisory_unlock($1::bigint)",
                        writer_fence_key,
                        "audit exclusive writer fence",
                    )
                except BaseException as error:
                    cleanup_errors.append(error)
            if claim_lock_acquired:
                try:
                    await _unlock_advisory(
                        setup,
                        "SELECT pg_advisory_unlock($1::bigint)",
                        claim_key,
                        "audit claim",
                    )
                except BaseException as error:
                    cleanup_errors.append(error)
        finally:
            try:
                await setup.close()
            except BaseException as error:
                cleanup_errors.append(error)
        if cleanup_errors:
            raise cleanup_errors[0]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Strict exact-five Module C database-side canonical K-line gate"
    )
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
    if args.timeframe is None:
        args.timeframe = list(TIMEFRAMES)
    else:
        try:
            args.timeframe = list(_normalize_timeframes(args.timeframe))
        except ValueError as error:
            parser.error(str(error))
    return args


async def _main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    run_id, summary = await run_gate(
        args.database_url,
        args.audit_run_id,
        tuple(args.timeframe),
    )
    print(json.dumps({"audit_run_id": run_id, "summary": summary}, sort_keys=True))
    if not summary["gate_pass"]:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(_main())
