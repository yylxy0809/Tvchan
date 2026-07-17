import asyncio

import collector.aggregate_timeframes_from_daily as aggregate_module
from collector.aggregate_timeframes_from_daily import (
    BATCH_WATERMARK_COUNT_SQL,
    BUCKET_EXPRESSIONS,
    _aggregate_one,
    _build_aggregate_sql,
)


def test_weekly_and_monthly_aggregation_uses_one_canonical_daily_input_and_completed_periods() -> None:
    for timeframe in ("1w", "1m"):
        sql = _build_aggregate_sql(BUCKET_EXPRESSIONS[timeframe]).lower()

        assert "row_number() over" in sql
        assert "partition by symbol_id, canonical_ts" in sql
        assert "where ranked.rn = 1" in sql
        assert "bucket < date_trunc" in sql
        assert "max(revision) as revision" in sql
        assert "max(ts) as ts" in sql


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
