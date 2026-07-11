from datetime import UTC, datetime, timedelta

from app.engine.phase_1_17 import (
    build_candidate_micro_backtest_decision,
    build_trigger_window,
    classify_v6_timeline_reason,
    confidence_first_seen_time,
    filter_visible_events,
    validate_micro_backfill_isolation,
)


def _dt(day: int, hour: int = 7) -> datetime:
    return datetime(2025, 9, day, hour, tzinfo=UTC)


def test_confidence_first_seen_uses_latest_visible_confirmation_time():
    value = confidence_first_seen_time(
        [
            _dt(18),
            _dt(24),
            _dt(25, 1),
        ]
    )

    assert value == _dt(25, 1)


def test_filter_visible_events_rejects_first_seen_after_as_of():
    events = [
        {"first_seen_time": _dt(18).isoformat(), "signal_point_time": _dt(18).isoformat()},
        {"first_seen_time": _dt(25).isoformat(), "signal_point_time": _dt(18).isoformat()},
    ]

    visible = filter_visible_events(events, _dt(24))

    assert visible == [events[0]]


def test_trigger_window_from_daily_anchor_marks_late_confidence_as_expired():
    window = build_trigger_window(
        anchor=_dt(18),
        evaluation_time=_dt(25),
        confidence_time=_dt(24),
        window=timedelta(days=3),
    )

    assert window["expired"] is True
    assert window["classification"] == "confidence_reached_after_window_end"


def test_v6_timeline_classifies_stale_thirty_f_confirmation_before_generic_expiry():
    reason = classify_v6_timeline_reason(
        has_30f_window_valid=False,
        thirty_f_first_seen=_dt(18),
        confidence_time=_dt(25),
        window_end=_dt(21),
        daily_bottom_first_seen=_dt(24),
        five_f_first_seen=_dt(25),
        evaluation_time=_dt(25),
    )

    assert reason == "thirty_f_confirmation_stale"


def test_micro_backfill_isolation_requires_target_run_group_and_no_published_head_writes():
    payload = validate_micro_backfill_isolation(
        manifest_rows=[
            {"symbol": "000001.SZ", "level": "5f", "run_group_id": "targeted"},
            {"symbol": "000001.SZ", "level": "30f", "run_group_id": "targeted"},
        ],
        run_group_id="targeted",
        published_head_write_count=0,
        overwritten_research_daily_close_count=0,
    )

    assert payload["isolated"] is True


def test_candidate_micro_backtest_disallowed_when_all_triggers_are_diagnostic_only():
    decision = build_candidate_micro_backtest_decision(
        policy_rows=[
            {
                "policy": "diagnostic_record_only_no_trigger_window",
                "entry_trigger_count": 3,
                "label": "diagnostic",
                "future_leakage_detected": False,
            }
        ]
    )

    assert decision["candidate_micro_backtest_allowed"] is False
    assert decision["reason"] == "no_candidate_policy_trigger"
