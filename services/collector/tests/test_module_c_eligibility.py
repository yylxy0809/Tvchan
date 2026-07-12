from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from collector.module_c_eligibility import Symbol, build_summary, evaluate_dispositions


NOW = datetime(2026, 7, 3, 7, tzinfo=timezone.utc)


def _coverage(symbol_id: int) -> dict[tuple[int, str], datetime]:
    return {(symbol_id, timeframe): NOW for timeframe in ("5f", "30f", "1d", "1w", "1m")}


def test_complete_symbol_is_eligible_at_all_five_levels() -> None:
    rows = evaluate_dispositions([Symbol(1, "600000", "SH")], _coverage(1), {}, {})
    assert len(rows) == 5
    assert all(row.eligible and not row.reasons for row in rows)


def test_bj_30f_is_always_excluded() -> None:
    rows = evaluate_dispositions([Symbol(2, "920000", "BJ")], _coverage(2), {}, {})
    bj_30f = next(row for row in rows if row.timeframe == "30f")
    assert not bj_30f.eligible
    assert bj_30f.reasons == ("bj_30f_excluded",)


def test_daily_unresolved_propagates_to_week_and_month() -> None:
    symbol = Symbol(3, "000001", "SZ")
    rows = evaluate_dispositions(
        [symbol], _coverage(3), {(symbol.name, "1d"): 7}, {},
    )
    dispositions = {row.timeframe: row for row in rows}
    assert dispositions["1d"].reasons == ("unresolved_ambiguous_volume_unit",)
    assert dispositions["1w"].reasons == ("daily_unresolved_propagated",)
    assert dispositions["1m"].reasons == ("daily_unresolved_propagated",)
    assert dispositions["1w"].unresolved_rows == 7


def test_missing_source_and_watermark_have_stable_reasons() -> None:
    symbol = Symbol(4, "600001", "SH")
    coverage = _coverage(4)
    coverage.pop((4, "5f"))
    rows = evaluate_dispositions(
        [symbol], coverage, {}, {(symbol.name, "5f"): 1},
    )
    five = next(row for row in rows if row.timeframe == "5f")
    assert five.reasons == ("missing_source_file", "missing_ingest_watermark")
    assert not five.eligible


def test_summary_counts_each_level_instead_of_market_total() -> None:
    symbols = [Symbol(1, "600000", "SH"), Symbol(2, "920000", "BJ")]
    rows = evaluate_dispositions(symbols, {**_coverage(1), **_coverage(2)}, {}, {})
    summary = build_summary(rows)
    assert summary["rows"] == 10
    assert summary["by_timeframe"]["5f"]["eligible"] == 2
    assert summary["by_timeframe"]["30f"]["eligible"] == 1
    assert summary["by_timeframe"]["30f"]["excluded"] == 1


def test_migration_enforces_versioned_append_only_rows() -> None:
    sql = (
        Path(__file__).parents[3] / "db" / "sql" / "031_module_c_eligibility.sql"
    ).read_text(encoding="utf-8")
    assert "manifest_version text not null unique" in sql
    assert "before update or delete on module_c_eligibility_builds" in sql.lower()
    assert "before update or delete on module_c_eligibility" in sql.lower()
    assert "disposition_rows = active_symbols * 5" in sql
