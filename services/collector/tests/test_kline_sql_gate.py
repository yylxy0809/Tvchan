from __future__ import annotations

from collector.kline_sql_gate import PRIMARY_KEY_SQL, TIMEFRAMES, build_gate_sql, summarize


def test_gate_has_five_database_side_checkpoint_workers() -> None:
    assert TIMEFRAMES == (5, 30, 1440, 10080, 43200)
    for timeframe in TIMEFRAMES:
        sql = build_gate_sql(timeframe).lower()
        assert "insert into kline_audit_checkpoints" in sql
        assert "join symbols" in sql
        expected_materialization = "materialized" if timeframe in (10080, 43200) else "not materialized"
        assert f"base as {expected_materialization}" in sql
        assert "jsonb_build_object" in sql
        assert "on conflict" in sql
        assert "select k.*" in sql
        assert "from universe" in sql
        assert "'missing_rows'" in sql
    assert "where false" in build_gate_sql(5).lower()
    assert "having count(*) > 1" in build_gate_sql(1440).lower()


def test_physical_duplicate_guarantee_is_validated_from_catalog() -> None:
    assert "pg_constraint" in PRIMARY_KEY_SQL
    assert "contype='p'" in PRIMARY_KEY_SQL


def test_validation_contract_is_aggregated_in_sql() -> None:
    sql = build_gate_sql(5).lower()
    assert "least(b.open_x1000,b.close_x1000,b.high_x1000)" in sql
    assert "greatest(b.open_x1000,b.close_x1000,b.low_x1000)" in sql
    assert "b.volume < 0" in sql
    assert "b.amount_x100 < 0" in sql
    assert "between 575 and 690" in sql
    assert "between 785 and 900" in sql
    assert "unexpected_source" in sql
    assert "source not in (2,4,9)" in sql


def test_30_minute_session_contract_includes_opening_snapshot() -> None:
    sql = build_gate_sql(30)
    assert "= 570" in sql
    assert "BETWEEN 600 AND 690" in sql
    assert "BETWEEN 810 AND 900" in sql
    assert "% 30 = 0" in sql


def test_logical_duplicate_keys_match_timeframe_contract() -> None:
    assert "lts::date AS logical_key" in build_gate_sql(1440)
    assert "date_trunc('week', lts) AS logical_key" in build_gate_sql(10080)
    assert "date_trunc('month', lts) AS logical_key" in build_gate_sql(43200)


def test_higher_periods_require_closed_daily_basis_and_source_8() -> None:
    for timeframe, bucket in ((10080, "week"), (43200, "month")):
        sql = build_gate_sql(timeframe).lower()
        assert "daily_ends" in sql
        assert f"date_trunc('{bucket}', now()" in sql
        assert "b.ts is distinct from d.expected_ts" in sql
        assert "d.expected_ts is null" in sql
        assert "source not in (8)" in sql
        assert "not b.is_complete" in sql


def test_summary_completes_scan_and_reports_gate_result() -> None:
    clean = {
        "checkpoints": 5, "rows_scanned": 100, "eligible": 5, "unresolved": 0,
        "invalid_ohlc": 0, "negative_volume": 0, "negative_amount": 0,
        "illegal_sessions": 0, "incomplete_rows": 0, "logical_duplicate_rows": 0,
        "unexpected_source": 0, "current_open_periods": 0,
        "timestamp_mismatches": 0, "missing_daily_basis": 0,
        "missing_higher_periods": 0,
        "missing_rows": 0,
    }
    status, summary = summarize(clean)
    assert status == "completed"
    assert summary["anomaly_total"] == 0
    assert summary["gate_pass"] is True

    dirty = dict(clean, illegal_sessions=2, unresolved=1)
    status, summary = summarize(dirty)
    assert status == "completed"
    assert summary["anomaly_total"] == 2
    assert summary["gate_pass"] is False
