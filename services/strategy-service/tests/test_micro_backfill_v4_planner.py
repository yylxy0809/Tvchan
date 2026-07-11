from app.engine.micro_backfill_v4_planner import plan_micro_backfill_v4
from datetime import datetime, timezone


def test_backfill_is_always_plan_only_and_deduplicated():
    result = plan_micro_backfill_v4([{"symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T00:00:00+00:00", "is_complete": True}], [])
    assert result["execute"] is False
    assert result["manifest"][0]["run_group_id"] == "phase_1_22_targeted_entry_window_intraday_v1"
    assert result["raw_expected_episode_cutoff_rows"] == 1
    assert result["deduplicated_rows"] == result["planned_runs"] == 1
    assert "candidate_rows" not in result


def test_backfill_rejects_weekend_and_existing_or_non_expected_rows():
    rows = [{"symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-04T00:00:00+00:00", "is_complete": True}, {"symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-03T00:00:00+00:00", "is_complete": True}]
    result = plan_micro_backfill_v4(rows, [{"symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-03T00:00:00+00:00", "run_id": 5, "run_group_id": "research_daily_close"}])
    assert result["manifest"] == []
    assert result["rejection_counts"]["weekend_or_non_trading"] == 1
    assert result["rejection_counts"]["already_covered"] == 1


def test_aware_datetime_actual_cutoff_prevents_duplicate_backfill_plan():
    expected = [{"symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-03T00:00:00+00:00", "is_complete": True}]
    actual = [{"symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": datetime(2025, 1, 3, tzinfo=timezone.utc)}]
    result = plan_micro_backfill_v4(expected, actual)
    assert result["manifest"] == []
    assert result["rejection_counts"]["already_covered"] == 1
