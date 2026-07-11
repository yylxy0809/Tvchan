from __future__ import annotations

from app.engine.phase_1_20 import (
    audit_intraday_run_coverage_gap,
    audit_refresh_visibility_gap,
    build_candidate_micro_backtest_decision_v2,
    build_entry_state_machine_v3,
    build_gate_deep_dive_by_symbol,
)
from app.engine.phase_1_18 import DEFAULT_TARGET_SYMBOLS


def test_visibility_gap_uses_first_seen_not_signal_point_time():
    audit = audit_refresh_visibility_gap(
        refresh_rows=[
            {
                "sample_id": "s1",
                "symbol": "000001.SZ",
                "daily_setup_first_seen_time": "2025-09-23T07:00:00+00:00",
                "daily_setup_point_time": "2025-09-23T07:00:00+00:00",
                "thirty_f_signal_point_time": "2025-09-24T02:00:00+00:00",
                "thirty_f_first_seen_time": "2025-09-26T02:00:00+00:00",
                "as_of_time": "2025-09-25T02:00:00+00:00",
                "price_valid_any_candidate_policy": True,
                "run_group_id": "research_daily_close",
            }
        ],
        trigger_window_days=2,
    )

    row = audit["rows"][0]
    assert row["point_time_after_daily_setup"] is True
    assert row["first_seen_before_as_of"] is False
    assert row["visibility_status"] == "diagnostic_only"
    assert row["visibility_gap_reason"] == "point_after_daily_but_first_seen_after_as_of"
    assert audit["summary"]["visible_eligible_count"] == 0


def test_refresh_must_be_first_seen_after_daily_setup():
    audit = audit_refresh_visibility_gap(
        refresh_rows=[
            {
                "sample_id": "s1",
                "symbol": "000001.SZ",
                "daily_setup_first_seen_time": "2025-09-23T07:00:00+00:00",
                "daily_setup_point_time": "2025-09-23T07:00:00+00:00",
                "thirty_f_signal_point_time": "2025-09-24T02:00:00+00:00",
                "thirty_f_first_seen_time": "2025-09-23T07:00:00+00:00",
                "as_of_time": "2025-09-25T02:00:00+00:00",
                "price_valid_any_candidate_policy": True,
            }
        ]
    )

    row = audit["rows"][0]
    assert row["first_seen_after_daily_setup"] is False
    assert row["visibility_gap_reason"] == "first_seen_equal_or_before_daily_setup"
    assert audit["summary"]["visible_eligible_count"] == 0


def test_future_refresh_cannot_trigger_entry_state_machine_v3():
    visibility = audit_refresh_visibility_gap(
        refresh_rows=[
            {
                "sample_id": "s1",
                "symbol": "000001.SZ",
                "daily_setup_first_seen_time": "2025-09-23T07:00:00+00:00",
                "daily_setup_point_time": "2025-09-23T07:00:00+00:00",
                "thirty_f_signal_point_time": "2025-09-24T02:00:00+00:00",
                "thirty_f_first_seen_time": "2025-09-26T02:00:00+00:00",
                "as_of_time": "2025-09-25T02:00:00+00:00",
                "price_valid_any_candidate_policy": True,
            }
        ]
    )
    state = build_entry_state_machine_v3(
        candidate_rows=[
            {
                "sample_id": "s1",
                "symbol": "000001.SZ",
                "weekly_context_time": "2025-09-19T07:00:00+00:00",
                "daily_setup_first_seen_time": "2025-09-23T07:00:00+00:00",
                "as_of_time": "2025-09-25T02:00:00+00:00",
            }
        ],
        visibility_rows=visibility["rows"],
        bottom_alignment_rows=[{"sample_id": "s1", "bottom_fractal_post_setup": True}],
        five_f_alignment_rows=[{"sample_id": "s1", "five_f_post_setup": True}],
    )

    row = state["rows"][0]
    assert row["entry_triggered"] is False
    assert row["primary_block_reason"] == "waiting_for_post_daily_30f_refresh"
    assert state["summary"]["entry_state_machine_v3_trigger_count"] == 0


def test_gate_deep_dive_covers_default_10_symbols():
    gate = build_gate_deep_dive_by_symbol(
        target_symbols=DEFAULT_TARGET_SYMBOLS,
        phase_1_18_universe={
            "rows": [{"sample_id": "s1", "symbol": "000001.SZ"}],
            "by_symbol": {
                "000001.SZ": {
                    "weekly_context_sample_count": 1,
                    "daily_ledger_buy_event_count": 3,
                    "daily_setup_audit_sample_count": 1,
                    "candidate_count": 1,
                }
            },
        },
        phase_1_19_gate_rows=[],
        visibility_rows=[],
    )

    assert len(gate["rows"]) == 10
    by_symbol = {row["symbol"]: row for row in gate["rows"]}
    assert by_symbol["000001.SZ"]["final_candidate_count"] == 1
    assert by_symbol["000002.SZ"]["primary_block_reason"] == "no_weekly_context"


def test_no_trigger_disables_candidate_micro_backtest_and_recommends_no_writes():
    decision = build_candidate_micro_backtest_decision_v2(
        entry_trigger_count_candidate=0,
        future_leakage_detected=False,
        trigger_sample_trace_complete=True,
    )
    coverage = audit_intraday_run_coverage_gap(
        candidate_rows=[{"sample_id": "s1", "symbol": "000001.SZ", "daily_setup_first_seen_time": "2025-09-23T07:00:00+00:00"}],
        visibility_rows=[],
    )

    assert decision["candidate_micro_backtest_allowed"] is False
    assert decision["reason"] == "no_candidate_trigger"
    assert coverage["summary"]["recommend_micro_backfill_v3_next"] is False
    assert coverage["summary"]["expected_no_published_head_write"] is True
