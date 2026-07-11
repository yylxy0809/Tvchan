from __future__ import annotations

from datetime import UTC, datetime

from app.engine.module_c_history_backfill import build_input_signature


def test_backfill_input_signature_is_stable():
    cutoff = datetime(2026, 7, 1, 7, 0, tzinfo=UTC)
    first = build_input_signature(
        profile="research_daily_close",
        symbol="000001.SZ",
        level="1d",
        mode="predictive",
        cutoff_time=cutoff,
        bar_count=200,
        snapshot_version="snap-1",
    )
    second = build_input_signature(
        profile="research_daily_close",
        symbol="000001.SZ",
        level="1d",
        mode="predictive",
        cutoff_time=cutoff,
        bar_count=200,
        snapshot_version="snap-1",
    )

    assert first == second
    assert len(first) == 64


def test_backfill_input_signature_changes_when_cutoff_changes():
    first = build_input_signature(
        profile="research_daily_close",
        symbol="000001.SZ",
        level="1d",
        mode="predictive",
        cutoff_time=datetime(2026, 7, 1, 7, 0, tzinfo=UTC),
        bar_count=200,
        snapshot_version="snap-1",
    )
    second = build_input_signature(
        profile="research_daily_close",
        symbol="000001.SZ",
        level="1d",
        mode="predictive",
        cutoff_time=datetime(2026, 7, 2, 7, 0, tzinfo=UTC),
        bar_count=200,
        snapshot_version="snap-1",
    )

    assert first != second
