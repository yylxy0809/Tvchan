from app.engine.intraday_run_coverage_audit_v3 import audit_intraday_run_coverage_v3
from datetime import datetime, timezone


def test_coverage_is_bounded_and_reports_duplicate_and_missing():
    grid = [{"episode_id": "e", "symbol": "x", "level": "30f", "cutoff_bar_end": f"2025-01-01T0{hour}:00:00+00:00"} for hour in (1, 2)]
    runs = [{"symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_id": 1, "run_group_id": "g"}, {"symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_id": 2, "run_group_id": "g"}]
    rows = audit_intraday_run_coverage_v3(grid, runs, episodes=[{"episode_id": "empty", "symbol": "x"}])["rows"]
    row = next(row for row in rows if row["episode_id"] == "e" and row["mode"] == "predictive")
    assert row["coverage_ratio"] == 0.5
    assert row["duplicate_cutoff_count"] == 1
    assert row["missing_cutoff_count"] == 1
    assert row["coverage_classification"] == "partial"
    empty = next(row for row in rows if row["episode_id"] == "empty" and row["level"] == "30f")
    assert empty["coverage_classification"] == "not_applicable"
    unsupported = next(row for row in rows if row["episode_id"] == "e" and row["mode"] == "confirmed")
    assert unsupported["coverage_classification"] == "unsupported_mode"


def test_aware_datetime_run_cutoff_matches_iso_expected_cutoff():
    grid = [{"episode_id": "e", "symbol": "x", "level": "30f", "cutoff_bar_end": "2025-01-01T01:00:00+00:00"}]
    runs = [{"symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": datetime(2025, 1, 1, 1, tzinfo=timezone.utc), "run_id": 1, "run_group_id": "g"}]
    row = next(row for row in audit_intraday_run_coverage_v3(grid, runs)["rows"] if row["mode"] == "predictive")
    assert row["coverage_classification"] == "complete"
    assert row["missing_cutoff_count"] == 0
