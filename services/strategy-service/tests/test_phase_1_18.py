from __future__ import annotations

from app.engine.phase_1_18 import (
    build_candidate_micro_backtest_decision_v2,
    build_entry_state_machine_v1,
    build_thirty_f_staleness_policy_matrix,
    classify_thirty_f_staleness,
    rebuild_candidate_universe,
)


def test_rebuild_candidate_universe_marks_other_symbols_filtered_before_candidate_gate():
    daily_rows = [
        {
            "symbol": "000001.SZ",
            "as_of_time": "2025-08-15T07:00:00+00:00",
            "weekly_context_time": "2025-08-15T07:00:00+00:00",
            "candidate_b2_b2s_accept": True,
            "candidate_audit": {
                "selected_daily_b2_or_b2s": {
                    "point_time": "2025-08-15T07:00:00+00:00",
                    "bsp_type": "2",
                    "price": 11.94,
                    "features": {"first_seen_time": "2025-08-15T07:00:00+00:00"},
                }
            },
        },
        {
            "symbol": "000651.SZ",
            "as_of_time": "2025-08-15T07:00:00+00:00",
            "weekly_context_time": "2025-08-15T07:00:00+00:00",
            "candidate_b2_b2s_accept": False,
            "observation_accept": True,
        },
    ]
    daily_ledger_rows = [{"symbol": "000001.SZ"}, {"symbol": "000651.SZ"}, {"symbol": "600519.SH"}]
    weekly_visibility_rows = [{"symbol": "000001.SZ"}, {"symbol": "000651.SZ"}]

    payload = rebuild_candidate_universe(
        daily_setup_rows=daily_rows,
        daily_ledger_rows=daily_ledger_rows,
        weekly_visibility_rows=weekly_visibility_rows,
        phase_1_16_master_rows=[{"sample_id": "000001.SZ|2025-08-15T07:00:00+00:00"}],
        phase_1_17_v6_rows=[],
        target_symbols=["000001.SZ", "000651.SZ", "600519.SH"],
    )

    assert payload["summary"]["candidate_universe_rebuilt"] is True
    assert payload["summary"]["candidate_count"] == 1
    assert payload["summary"]["candidate_symbol_distribution"] == {"000001.SZ": 1}
    assert payload["summary"]["pipeline_bug_detected"] is False
    assert payload["by_symbol"]["000651.SZ"]["missing_stage"] == "filtered_at_candidate_daily_setup_gate"
    assert payload["by_symbol"]["600519.SH"]["missing_stage"] == "daily_signal_exists_but_no_weekly_context_candidate_row"


def test_classify_thirty_f_staleness_prefers_event_timing_subclasses():
    assert classify_thirty_f_staleness(
        daily_setup_first_seen_time="2025-09-23T07:00:00+00:00",
        thirty_f_signal_first_seen_time="2025-09-19T02:00:00+00:00",
        bottom_fractal_first_seen_time="2025-09-24T07:00:00+00:00",
        five_f_confirm_first_seen_time="2025-09-25T01:30:00+00:00",
        thirty_f_price_valid=True,
    ) == "thirty_f_before_daily_setup"

    assert classify_thirty_f_staleness(
        daily_setup_first_seen_time="2025-09-23T07:00:00+00:00",
        thirty_f_signal_first_seen_time=None,
        bottom_fractal_first_seen_time="2025-09-24T07:00:00+00:00",
        five_f_confirm_first_seen_time="2025-09-25T01:30:00+00:00",
        thirty_f_price_valid=True,
    ) == "no_post_daily_setup_30f_refresh"


def test_policy_matrix_keeps_staleness_as_blocker_without_candidate_trigger():
    payload = build_thirty_f_staleness_policy_matrix(
        [
            {
                "sample_id": "s1",
                "staleness_type": "thirty_f_before_daily_setup",
                "future_leakage_detected": False,
                "thirty_f_signal_price_valid": False,
            }
        ]
    )

    assert payload["decision"]["recommended_official_policy"] == "strict_existing"
    assert payload["decision"]["accept_thirty_f_confirmation_stale_as_blocker"] is True
    assert payload["decision"]["candidate_policy_entry_trigger_count"] == 0


def test_entry_state_machine_reaches_confidence_but_not_entry_trigger_for_stale_30f():
    payload = build_entry_state_machine_v1(
        candidate_rows=[
            {
                "sample_id": "s1",
                "symbol": "000001.SZ",
                "as_of_time": "2025-09-25T01:30:00+00:00",
                "visible_30f_B1_or_1p_count": 1,
                "daily_bottom_fractal_visible": True,
                "five_f_B2_confirms_30f": True,
                "v4_confidence": 70.0,
            }
        ],
        staleness_rows=[
            {
                "sample_id": "s1",
                "staleness_type": "thirty_f_before_daily_setup",
                "future_leakage_detected": False,
            }
        ],
    )

    assert payload["summary"]["state_machine_built"] is True
    assert payload["summary"]["samples_reach_confidence_70"] == 1
    assert payload["summary"]["samples_reach_entry_trigger_valid"] == 0
    assert payload["summary"]["primary_rejection_reason"] == "thirty_f_confirmation_stale"


def test_candidate_micro_backtest_v2_requires_real_candidate_trigger():
    decision = build_candidate_micro_backtest_decision_v2(
        candidate_policy_entry_trigger_count=0,
        diagnostic_policy_entry_trigger_count=3,
        future_leakage_detected=False,
    )

    assert decision["candidate_micro_backtest_allowed"] is False
    assert decision["reason"] == "no_candidate_policy_trigger"
