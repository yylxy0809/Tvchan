from app.engine.multi_run_group_signal_ledger import build_signal_event_ledger_v2


def test_ledger_uses_fingerprint_and_real_run_cutoffs():
    rows = [
        {"symbol_id": 1, "chan_level": "30f", "mode": "predictive", "side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1000, "run_id": 2, "run_group_id": "research_daily_close", "cutoff_bar_end": "2025-01-03T00:00:00+00:00", "status": "success", "run_kind": "historical_backfill"},
        {"symbol_id": 1, "chan_level": "30f", "mode": "predictive", "side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1000, "run_id": 1, "run_group_id": "phase_1_16_targeted_entry_window_intraday_v2", "cutoff_bar_end": "2025-01-02T00:00:00+00:00", "status": "success", "run_kind": "historical_backfill"},
        {"symbol_id": 1, "chan_level": "30f", "mode": "predictive", "side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-01T00:00:00+00:00", "price_x1000": 1001, "run_id": 3, "run_group_id": "phase_1_16_targeted_entry_window_intraday_v2", "cutoff_bar_end": "2025-01-04T00:00:00+00:00", "status": "success", "run_kind": "historical_backfill"},
    ]
    events = build_signal_event_ledger_v2(rows)
    assert len(events) == 2
    assert events[0]["first_seen_time"] == "2025-01-02T00:00:00+00:00"
    assert events[0]["source_run_ids"] == [1, 2]


def test_ledger_rejects_non_historical_or_non_success_rows():
    assert build_signal_event_ledger_v2([{"status": "failed", "run_kind": "historical_backfill"}]) == []
    assert build_signal_event_ledger_v2([{"status": "success", "run_kind": "historical_backfill", "mode": "legacy"}]) == []


def test_ledger_rejects_signal_visible_after_run_cutoff():
    row = {"symbol_id": 1, "chan_level": "30f", "mode": "predictive", "side": "buy", "bsp_type": "1", "signal_point_time": "2025-01-03T00:00:00+00:00", "price_x1000": 1000, "run_id": 1, "run_group_id": "research_daily_close", "cutoff_bar_end": "2025-01-02T00:00:00+00:00", "status": "success", "run_kind": "historical_backfill"}
    assert build_signal_event_ledger_v2([row]) == []
