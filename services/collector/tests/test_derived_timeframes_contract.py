from collector.aggregate_timeframes_from_daily import (
    BATCH_WATERMARK_COUNT_SQL,
    BUCKET_EXPRESSIONS,
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
