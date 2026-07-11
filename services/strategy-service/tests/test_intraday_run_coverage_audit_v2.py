from app.engine.intraday_run_coverage_audit_v2 import audit_intraday_run_coverage_v2


def test_coverage_uses_runs_not_signal_visibility():
    episodes = [{"episode_id": "e", "symbol": "000001.SZ", "daily_setup_first_seen_time": "2025-01-01T00:00:00+00:00", "trigger_window_end": "2025-01-03T00:00:00+00:00"}]
    runs = [{"symbol": "000001.SZ", "level": "30f", "run_id": 8, "cutoff_bar_end": "2025-01-02T00:00:00+00:00", "run_group_id": "phase_1_16_targeted_entry_window_intraday_v2"}]
    row = audit_intraday_run_coverage_v2(episodes, runs)["rows"][0]
    assert row["first_30f_run_after_setup"] == 8
    assert row["30f_run_count_inside_window"] == 1
    assert row["missing_reason"] is None


def test_no_run_has_explicit_missing_reason():
    result = audit_intraday_run_coverage_v2([{"episode_id": "e", "symbol": "x", "daily_setup_first_seen_time": "2025-01-01T00:00:00+00:00", "trigger_window_end": "2025-01-02T00:00:00+00:00"}], [])
    assert result["rows"][0]["missing_reason"] == "no_intraday_runs_for_episode_symbol"
