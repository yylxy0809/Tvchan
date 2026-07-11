from __future__ import annotations

from app.engine.phase_1_19 import (
    build_candidate_trigger_eligibility_decision,
    build_entry_state_machine_v2,
    build_gate_by_symbol,
    scan_post_daily_30f_refresh,
)


def test_gate_by_symbol_explains_non_000001_filters_and_refresh_gap():
    payload = build_gate_by_symbol(
        target_symbols=["000001.SZ", "000651.SZ", "600519.SH"],
        phase_1_18_universe={
            "rows": [{"sample_id": "s1", "symbol": "000001.SZ", "daily_setup_first_seen_time": "2025-09-23T07:00:00+00:00"}],
            "by_symbol": {
                "000001.SZ": {"candidate_count": 1, "weekly_context_sample_count": 2, "daily_ledger_buy_event_count": 5, "daily_setup_audit_sample_count": 1},
                "000651.SZ": {"candidate_count": 0, "weekly_context_sample_count": 1, "daily_ledger_buy_event_count": 3, "daily_setup_audit_sample_count": 1},
                "600519.SH": {"candidate_count": 0, "weekly_context_sample_count": 0, "daily_ledger_buy_event_count": 2, "daily_setup_audit_sample_count": 0},
            },
        },
        post_daily_refresh_rows=[],
        state_machine_rows=[{"sample_id": "s1", "symbol": "000001.SZ", "confidence_reaches_70": False, "entry_triggered": False, "primary_block_reason": "waiting_for_30f_refresh"}],
    )

    by_symbol = {row["symbol"]: row for row in payload["rows"]}
    assert by_symbol["000001.SZ"]["daily_candidate_setup_accepted"] is True
    assert by_symbol["000001.SZ"]["post_daily_setup_30f_refresh_exists"] is False
    assert by_symbol["000001.SZ"]["primary_block_reason"] == "waiting_for_30f_refresh"
    assert by_symbol["000651.SZ"]["primary_block_reason"] == "daily_candidate_setup_not_accepted"
    assert by_symbol["600519.SH"]["primary_block_reason"] == "weekly_context_missing"


def test_post_daily_30f_refresh_scan_excludes_stale_and_keeps_post_daily_refresh():
    payload = scan_post_daily_30f_refresh(
        candidate_rows=[
            {
                "sample_id": "s1",
                "symbol": "000001.SZ",
                "as_of_time": "2025-09-25T02:00:00+00:00",
                "daily_setup_first_seen_time": "2025-09-23T07:00:00+00:00",
            }
        ],
        thirty_f_events=[
            {"symbol": "000001.SZ", "side": "buy", "bsp_type": "1p", "first_seen_time": "2025-09-22T07:00:00+00:00", "signal_point_time": "2025-09-22T06:30:00+00:00", "price_x1000": 11300},
            {"symbol": "000001.SZ", "side": "buy", "bsp_type": "1", "first_seen_time": "2025-09-24T02:00:00+00:00", "signal_point_time": "2025-09-24T01:30:00+00:00", "price_x1000": 11400},
        ],
    )

    assert payload["summary"]["stale_30f_excluded_count"] == 1
    assert payload["summary"]["post_daily_30f_refresh_count"] == 1
    row = payload["rows"][0]
    assert row["is_post_daily_setup_refresh"] is True
    assert row["price_policy_results"]["signal_price_only"] is True
    assert row["future_leakage_detected"] is False


def test_entry_state_machine_v2_waits_for_post_daily_refresh_before_trigger():
    payload = build_entry_state_machine_v2(
        candidate_rows=[
            {
                "sample_id": "s1",
                "symbol": "000001.SZ",
                "as_of_time": "2025-09-25T02:00:00+00:00",
                "weekly_context_time": "2025-09-19T07:00:00+00:00",
                "daily_setup_first_seen_time": "2025-09-23T07:00:00+00:00",
            }
        ],
        refresh_rows=[],
        bottom_alignment_rows=[{"sample_id": "s1", "bottom_fractal_exists": True, "bottom_fractal_post_setup": True}],
        five_f_alignment_rows=[{"sample_id": "s1", "five_f_confirmation_exists": True, "five_f_post_setup": True}],
    )

    row = payload["rows"][0]
    assert row["states"][-1] == "WAITING_FOR_30F_REFRESH"
    assert row["entry_triggered"] is False
    assert payload["summary"]["entry_state_machine_v2_trigger_count"] == 0
    assert payload["summary"]["primary_zero_trigger_root_cause"] == "waiting_for_post_daily_30f_refresh"


def test_candidate_trigger_eligibility_requires_candidate_trigger_without_future_leakage():
    blocked = build_candidate_trigger_eligibility_decision(entry_trigger_count=0, future_leakage_detected=False)
    assert blocked["candidate_micro_backtest_allowed"] is False
    assert blocked["reason"] == "no_candidate_trigger"

    allowed = build_candidate_trigger_eligibility_decision(entry_trigger_count=1, future_leakage_detected=False)
    assert allowed["candidate_micro_backtest_allowed"] is True
