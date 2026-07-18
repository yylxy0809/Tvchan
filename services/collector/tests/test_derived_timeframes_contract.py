import asyncio
from datetime import datetime, timezone

import pytest

import collector.aggregate_timeframes_from_daily as aggregate_module
import collector.kline_sql_gate as gate_module
from collector.aggregate_timeframes_from_daily import (
    BATCH_WATERMARK_COUNT_SQL,
    BUCKET_EXPRESSIONS,
    FUTURE_DERIVED_ROWS_SQL,
    FUTURE_DERIVED_PREFLIGHT_SQL,
    _aggregate_one,
    _acquire_aggregate_writer_lock,
    _aggregate_writer_lock_key,
    _build_aggregate_sql,
    _release_aggregate_writer_lock,
    _run_with_aggregate_lock_watchdog,
    _validate_authoritative_clock,
    _validate_no_future_derived_rows,
)
from collector.kline_sql_gate import AuditLockOwnershipLost
from collector.module_c_eligibility import (
    FRESHNESS_CONTRACT_VERSION,
    parse_freshness_contract,
)


def _freshness_contract(*, as_of: str = "2026-07-18T08:00:00+08:00"):
    return parse_freshness_contract({
        "contract_version": FRESHNESS_CONTRACT_VERSION,
        "as_of": as_of,
        "trading_calendar": {"id": "sse-szse-2026-v1", "sha256": "a" * 64},
        "expected_closed_watermarks": {
            "5f": "2026-07-17T15:00:00+08:00",
            "30f": "2026-07-17T15:00:00+08:00",
            "1d": "2026-07-17T15:00:00+08:00",
            "1w": "2026-07-17T15:00:00+08:00",
            "1m": "2026-06-30T15:00:00+08:00",
        },
    })


def test_weekly_and_monthly_aggregation_uses_one_canonical_daily_input_and_completed_periods() -> None:
    for timeframe in ("1w", "1m"):
        sql = _build_aggregate_sql(BUCKET_EXPRESSIONS[timeframe]).lower()

        assert "row_number() over" in sql
        assert "partition by symbol_id, canonical_ts" in sql
        assert "where ranked.rn = 1" in sql
        assert "bucket < date_trunc" in sql
        assert "max(revision) as revision" in sql
        assert "max(ts) as ts" in sql


def test_explicit_closed_period_cutoff_uses_authoritative_daily_end_not_database_max() -> None:
    for timeframe in ("1w", "1m"):
        sql = _build_aggregate_sql(BUCKET_EXPRESSIONS[timeframe]).lower()

        assert "$5::timestamptz is null" in sql
        assert "max(ts) <= $5::timestamptz" in sql
        assert "max(ts) from klines" not in sql

    future_sql = " ".join(FUTURE_DERIVED_ROWS_SQL.lower().split())
    assert "source = $2::smallint" in future_sql
    assert "ts > $5::timestamptz" in future_sql
    assert "delete" not in future_sql
    preflight_sql = " ".join(FUTURE_DERIVED_PREFLIGHT_SQL.lower().split())
    assert "source = $1::smallint" in preflight_sql
    assert "timeframe = $2::integer" in preflight_sql
    assert "ts > $4::timestamptz" in preflight_sql
    assert "delete" not in preflight_sql


def test_aggregate_rejects_future_authoritative_as_of_against_database_clock() -> None:
    contract = _freshness_contract(as_of="2026-07-18T10:00:00+08:00")

    class Connection:
        async def fetchval(self, sql: str, **kwargs: object) -> datetime:
            return datetime(2026, 7, 18, 1, tzinfo=timezone.utc)

    try:
        asyncio.run(_validate_authoritative_clock(Connection(), contract, None))
    except RuntimeError as error:
        assert "after the aggregate database observation" in str(error)
    else:
        raise AssertionError("future authoritative as-of must fail closed")


def test_derived_revision_tracks_corrected_daily_input_and_does_not_churn_on_rerun() -> None:
    sql = _build_aggregate_sql(BUCKET_EXPRESSIONS["1w"]).lower()

    assert "max(revision) as revision" in sql
    assert "revision = excluded.revision" in sql
    assert "klines.revision + 1" not in sql


def test_derived_period_repair_deletes_only_stale_source8_rows_and_updates_same_revision_corrections() -> None:
    sql = _build_aggregate_sql(BUCKET_EXPRESSIONS["1w"]).lower()

    assert "deleted as" in sql
    assert "delete from klines existing" in sql
    assert "existing.source = $2::smallint" in sql
    assert "existing.ts <> agg.ts" in sql
    assert "= agg.bucket" in sql
    assert "open_x1000 is distinct from excluded.open_x1000" in sql
    assert "amount_x100 is distinct from excluded.amount_x100" in sql
    assert "is_complete is distinct from excluded.is_complete" in sql


def test_first_build_can_skip_stale_period_delete() -> None:
    sql = _build_aggregate_sql(BUCKET_EXPRESSIONS["1w"], repair_stale_periods=False).lower()

    assert "delete from klines existing" not in sql


def test_skip_complete_batches_requires_target_watermark_and_daily_freshness() -> None:
    sql = BATCH_WATERMARK_COUNT_SQL.lower()

    assert "wm.last_bar_end = target.max_ts" in sql
    assert "target.max_updated_at >= daily.max_updated_at" in sql
    assert "target.max_revision >= daily.max_revision" in sql
    assert "wm.last_bar_end is not null" not in sql


def test_aggregate_refreshes_touched_scopes_exactly_before_watermark(monkeypatch) -> None:
    events: list[str] = []
    refreshed: list[tuple[int, int]] = []

    class Connection:
        async def execute(self, sql: str, *args: object, **kwargs: object) -> str:
            events.append("watermark" if "scheme2_ingest_watermarks" in sql else "aggregate")
            return "INSERT 1"

        async def fetchrow(self, sql: str, *args: object, **kwargs: object) -> dict[str, object]:
            return {
                "at_latest": 1,
                "with_watermark": 1,
                "active_symbols": 1,
                "min_watermark": None,
                "max_watermark": None,
            }

        def transaction(self):
            class Context:
                async def __aenter__(self):
                    events.append("begin")
                    return object()

                async def __aexit__(self, exc_type, exc, tb):
                    events.append("rollback" if exc_type else "commit")
                    return False

            return Context()

    connection = Connection()

    class Pool:
        def acquire(self):
            class Context:
                async def __aenter__(self):
                    return connection

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            return Context()

    async def refresh_scopes_exact(_conn, *, scopes):
        events.append("catalog-refresh")
        refreshed.extend(scopes)
        return {"catalog_rows": len(scopes), "updated": len(scopes), "cas_skipped": 0}

    monkeypatch.setattr(aggregate_module, "refresh_scopes_exact", refresh_scopes_exact)

    asyncio.run(_aggregate_one(
        Pool(), "1w", 8, None, [7], 1, 1,
        skip_complete_batches=False,
        repair_stale_periods=True,
    ))

    assert refreshed == [(7, 10080)]
    assert events.index("aggregate") < events.index("catalog-refresh") < events.index("watermark")
    assert events[:1] == ["begin"]
    assert "commit" in events


def test_aggregate_binds_explicit_closed_week_cutoff(monkeypatch) -> None:
    executed: list[tuple[object, ...]] = []

    class Connection:
        async def fetchval(self, sql: str, *args: object, **kwargs: object) -> bool:
            return False

        async def execute(self, sql: str, *args: object, **kwargs: object) -> str:
            executed.append(args)
            return "INSERT 1"

        async def fetchrow(self, sql: str, *args: object, **kwargs: object) -> dict[str, object]:
            return {
                "at_latest": 1,
                "with_watermark": 1,
                "active_symbols": 1,
                "min_watermark": None,
                "max_watermark": None,
            }

        def transaction(self):
            class Context:
                async def __aenter__(self):
                    return object()

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            return Context()

    connection = Connection()

    class Pool:
        def acquire(self):
            class Context:
                async def __aenter__(self):
                    return connection

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            return Context()

    async def refresh_scopes_exact(_conn, *, scopes):
        return {"catalog_rows": len(scopes), "updated": len(scopes), "cas_skipped": 0}

    monkeypatch.setattr(aggregate_module, "refresh_scopes_exact", refresh_scopes_exact)
    cutoff = datetime(2026, 7, 17, 7, tzinfo=timezone.utc)

    asyncio.run(_aggregate_one(
        Pool(), "1w", 8, None, [7], 1, 1,
        skip_complete_batches=False,
        repair_stale_periods=True,
        closed_period_cutoff=cutoff,
    ))

    aggregate_args = executed[0]
    assert aggregate_args[-1] == cutoff


def test_authoritative_cutoff_disables_legacy_watermark_skip() -> None:
    with pytest.raises(ValueError, match="skip-complete-batches"):
        asyncio.run(_aggregate_one(
            object(), "1w", 8, None, [7], 1, 1,
            skip_complete_batches=True,
            repair_stale_periods=True,
            closed_period_cutoff=datetime(2026, 7, 17, 7, tzinfo=timezone.utc),
        ))


def test_future_source8_derived_row_fails_before_any_write(monkeypatch) -> None:
    events: list[str] = []

    class Connection:
        async def fetchval(self, sql: str, *args: object, **kwargs: object) -> bool:
            events.append("future-check")
            return True

        async def execute(self, sql: str, *args: object, **kwargs: object) -> str:
            events.append("write")
            return "INSERT 1"

        async def fetchrow(self, sql: str, *args: object, **kwargs: object):
            raise AssertionError("summary query must not run")

        def transaction(self):
            class Context:
                async def __aenter__(self):
                    events.append("begin")

                async def __aexit__(self, exc_type, exc, tb):
                    events.append("rollback" if exc_type else "commit")
                    return False

            return Context()

    connection = Connection()

    class Pool:
        def acquire(self):
            class Context:
                async def __aenter__(self):
                    return connection

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            return Context()

    with pytest.raises(RuntimeError, match="after the authoritative"):
        asyncio.run(_aggregate_one(
            Pool(), "1w", 8, None, [7], 1, 1,
            skip_complete_batches=False,
            repair_stale_periods=True,
            closed_period_cutoff=datetime(2026, 7, 17, 7, tzinfo=timezone.utc),
        ))

    assert events == ["begin", "future-check", "rollback"]


def test_global_future_source8_preflight_fails_before_first_batch() -> None:
    calls: list[tuple[object, ...]] = []

    class Connection:
        async def fetchval(self, sql: str, *args: object, **kwargs: object) -> bool:
            calls.append(args)
            return True

    with pytest.raises(RuntimeError, match="1w source=8"):
        asyncio.run(_validate_no_future_derived_rows(
            Connection(),
            timeframes=["1w", "1m"],
            source_code=8,
            symbol_ids=[7, 9],
            freshness_contract=_freshness_contract(),
            statement_timeout=None,
        ))

    assert calls == [(
        8,
        10080,
        [7, 9],
        datetime(2026, 7, 17, 7, tzinfo=timezone.utc),
    )]


def test_different_cutoffs_cannot_hold_same_source_writer_lock(monkeypatch) -> None:
    old_contract = _freshness_contract()
    new_payload = dict(old_contract.normalized)
    new_payload["expected_closed_watermarks"] = {
        **old_contract.normalized["expected_closed_watermarks"],
        "1w": "2026-07-24T07:00:00+00:00",
    }
    new_payload["as_of"] = "2026-07-25T00:00:00+00:00"
    new_contract = parse_freshness_contract(new_payload)
    assert old_contract.sha256 != new_contract.sha256

    owner: object | None = None

    class Connection:
        def __init__(self) -> None:
            self.closed = False

        async def fetchval(self, sql: str, lock_key: int) -> bool:
            nonlocal owner
            if "try_advisory_lock" in sql:
                if owner is None:
                    owner = self
                    return True
                return owner is self
            if "advisory_unlock" in sql:
                if owner is self:
                    owner = None
                    return True
                return False
            raise AssertionError(sql)

        async def close(self) -> None:
            self.closed = True

    first = Connection()
    second = Connection()
    connections = [first, second]

    async def connect(_database_url: str):
        return connections.pop(0)

    monkeypatch.setattr(aggregate_module.asyncpg, "connect", connect)

    first_session, first_key = asyncio.run(
        _acquire_aggregate_writer_lock("postgresql://test", 8)
    )
    assert first_key == _aggregate_writer_lock_key(8)
    with pytest.raises(RuntimeError, match="another source=8"):
        asyncio.run(_acquire_aggregate_writer_lock("postgresql://test", 8))
    assert second.closed is True
    asyncio.run(_release_aggregate_writer_lock(first_session, first_key))
    assert first.closed is True
    assert owner is None


def test_lock_session_disconnect_cancels_aggregate_before_next_write(
    monkeypatch,
) -> None:
    monkeypatch.setattr(gate_module, "LOCK_WATCHDOG_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(gate_module, "LOCK_WATCHDOG_TIMEOUT_SECONDS", 0.1)
    events: list[str] = []

    class LockSession:
        def __init__(self) -> None:
            self.heartbeats = 0

        async def fetchval(self, sql: str) -> int:
            assert sql == "SELECT 1"
            self.heartbeats += 1
            if self.heartbeats == 1:
                return 1
            raise ConnectionError("lock session disconnected")

    async def aggregate_operation() -> None:
        events.append("batch-1-write")
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            events.append("cancelled")
            raise
        events.append("batch-2-write")

    async def run() -> None:
        with pytest.raises(AuditLockOwnershipLost, match="heartbeat failed"):
            await _run_with_aggregate_lock_watchdog(
                LockSession(),
                aggregate_operation(),
            )

    asyncio.run(run())
    assert events == ["batch-1-write", "cancelled"]
