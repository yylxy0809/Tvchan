from app.engine.micro_backfill_v3_planner import plan_micro_backfill_v3


def test_v3_requires_all_admission_conditions_and_deduplicates():
    decision = plan_micro_backfill_v3([], resource_ok=True)
    assert decision["execute"] is False
    rows = [{"coverage_classification": "partially_covered", "missing_due_to_stale": False, "symbol": "000001.SZ", "missing_cutoffs": [{"level": "30f", "cutoff_bar_end": "2025-01-02T00:00:00+00:00"}]}]
    decision = plan_micro_backfill_v3(rows, resource_ok=True)
    assert decision["execute"] is True
    assert decision["manifest"][0]["run_group_id"] == "phase_1_20r_targeted_entry_window_intraday_v3"
