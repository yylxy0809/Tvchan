from app.engine.signal_lifecycle_ledger import build_signal_lifecycle
import pytest


def test_lifecycle_handles_confirm_disappear_and_reappear():
    runs = [
        {"run_id": 1, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "g", "signals": [{"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": False}]},
        {"run_id": 2, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T02:00:00+00:00", "run_group_id": "g", "signals": [{"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": True}]},
        {"run_id": 3, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T03:00:00+00:00", "run_group_id": "g", "signals": []},
        {"run_id": 4, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T04:00:00+00:00", "run_group_id": "g", "signals": [{"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": True}]},
    ]
    expected = [{"symbol": "x", "level": "30f", "cutoff_bar_end": f"2025-01-01T0{hour}:00:00+00:00"} for hour in range(1, 5)]
    row = build_signal_lifecycle(runs, expected)[0]
    assert row["first_seen_time"] == "2025-01-01T01:00:00+00:00"
    assert row["confirm_time"] == "2025-01-01T02:00:00+00:00"
    assert row["disappear_time"] == "2025-01-01T03:00:00+00:00"
    assert "REAPPEARED" in row["events"]


def test_missing_expected_cutoff_makes_disappearance_indeterminate():
    runs = [
        {"run_id": 1, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "g", "signals": [{"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": True}]},
        {"run_id": 3, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T03:00:00+00:00", "run_group_id": "g", "signals": []},
    ]
    expected = [{"symbol": "x", "level": "30f", "cutoff_bar_end": f"2025-01-01T0{hour}:00:00+00:00"} for hour in (1, 2, 3)]
    row = build_signal_lifecycle(runs, expected)[0]
    assert row["disappear_time"] is None
    assert row["cutoff_gap"] is True


def test_duplicate_runs_at_one_cutoff_preserve_first_confirmation_and_provenance():
    runs = [
        {"run_id": 1, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "a", "signals": [{"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": False}]},
        {"run_id": 2, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "b", "signals": [{"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": True}]},
    ]
    row = build_signal_lifecycle(runs, [{"symbol": "x", "level": "30f", "cutoff_bar_end": "2025-01-01T01:00:00+00:00"}])[0]
    assert row["first_seen_run_id"] == 1
    assert row["confirm_run_id"] == 2
    assert row["source_run_groups"] == ["a", "b"]


def test_runs_outside_expected_grid_cannot_produce_precise_disappearance():
    runs = [
        {"run_id": 1, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "g", "signals": [{"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": True}]},
        {"run_id": 2, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T02:00:00+00:00", "run_group_id": "g", "signals": []},
    ]
    row = build_signal_lifecycle(runs, [{"symbol": "x", "level": "30f", "cutoff_bar_end": "2025-01-01T01:00:00+00:00"}])[0]
    assert row["disappear_time"] is None
    assert row["cutoff_gap"] is True


def test_lifecycle_keeps_explicit_parent_identity_for_five_f_binding():
    run = {"run_id": 1, "symbol": "x", "level": "5f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "g", "signals": [{"side": "buy", "bsp_type": "2", "signal_point_time": "2025-01-01T01:00:00+00:00", "price_x1000": 1, "is_confirmed": True, "parent_30f_identity": "parent"}]}
    row = build_signal_lifecycle([run], [{"symbol": "x", "level": "5f", "cutoff_bar_end": "2025-01-01T01:00:00+00:00"}])[0]
    assert row["parent_30f_identity"] == "parent"


def test_signal_and_empty_duplicate_run_at_same_cutoff_does_not_disappear():
    signal = {"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": False}
    runs = [{"run_id": 1, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "g", "signals": [signal]}, {"run_id": 2, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "g", "signals": []}, {"run_id": 3, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T02:00:00+00:00", "run_group_id": "g", "signals": [signal]}]
    expected = [{"symbol": "x", "level": "30f", "cutoff_bar_end": "2025-01-01T01:00:00+00:00"}, {"symbol": "x", "level": "30f", "cutoff_bar_end": "2025-01-01T02:00:00+00:00"}]
    row = build_signal_lifecycle(runs, expected)[0]
    assert row["disappear_time"] is None
    assert "PERSISTED" in row["events"]


def test_empty_duplicate_seed_does_not_claim_signal_provenance():
    signal = {"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": True}
    runs = [
        {"run_id": 1, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "empty", "signals": []},
        {"run_id": 2, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00+00:00", "run_group_id": "signal", "signals": [signal]},
    ]
    row = build_signal_lifecycle(runs, [{"symbol": "x", "level": "30f", "cutoff_bar_end": "2025-01-01T01:00:00+00:00"}])[0]
    assert row["first_seen_run_id"] == 2
    assert row["last_seen_run_id"] == 2
    assert row["confirm_run_id"] == 2
    assert row["source_run_groups"] == ["empty", "signal"]


def test_lifecycle_rejects_naive_cutoffs_and_treats_equal_offsets_as_one_instant():
    with pytest.raises(ValueError, match="Naive"):
        build_signal_lifecycle([{"run_id": 1, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T01:00:00", "signals": []}])
    signal = {"side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1, "is_confirmed": False}
    rows = build_signal_lifecycle([{"run_id": 1, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T09:00:00+00:00", "signals": [signal]}, {"run_id": 2, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T17:00:00+08:00", "signals": [signal]}])
    assert rows[0]["visible_cutoffs"] == ["2025-01-01T09:00:00+00:00"]


def test_signal_identity_merges_equal_point_times_across_offsets_and_rejects_naive_points():
    base = {"side": "buy", "bsp_type": "1", "price_x1000": 1, "is_confirmed": False}
    runs = [
        {"run_id": 1, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T09:00:00+00:00", "signals": [{**base, "signal_point_time": "2025-01-01T09:00:00+00:00"}]},
        {"run_id": 2, "symbol": "x", "level": "30f", "mode": "predictive", "cutoff_bar_end": "2025-01-01T10:00:00+00:00", "signals": [{**base, "signal_point_time": "2025-01-01T17:00:00+08:00", "is_confirmed": True}]},
    ]
    rows = build_signal_lifecycle(runs, [{"symbol": "x", "level": "30f", "cutoff_bar_end": run["cutoff_bar_end"]} for run in runs])
    assert len(rows) == 1 and rows[0]["confirm_run_id"] == 2
    runs[0]["signals"][0]["signal_point_time"] = "2025-01-01T09:00:00"
    with pytest.raises(ValueError, match="Naive"):
        build_signal_lifecycle(runs)
