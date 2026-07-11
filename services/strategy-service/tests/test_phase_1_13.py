from app.engine.phase_1_13 import _build_entry_failure_reason_v2, _build_multi_level_ledger_summary


def test_entry_failure_reason_v2_only_30f_confirmation():
    reason = _build_entry_failure_reason_v2(
        confirmation_30f_b1=True,
        confirmation_daily_bottom_fractal=False,
        confirmation_5f_b2_confirms_30f=False,
        order_invalid=False,
        first_seen_after_as_of=False,
    )
    assert reason == "only_30f_confirmation"


def test_entry_failure_reason_v2_missing_5f_only():
    reason = _build_entry_failure_reason_v2(
        confirmation_30f_b1=True,
        confirmation_daily_bottom_fractal=True,
        confirmation_5f_b2_confirms_30f=False,
        order_invalid=False,
        first_seen_after_as_of=False,
    )
    assert reason == "missing_5f_only"


def test_multi_level_ledger_summary_keeps_both_levels():
    payload = _build_multi_level_ledger_summary(
        payload_30f={"summary": {"level": "30f", "unique_signal_events": 10}},
        payload_5f={"summary": {"level": "5f", "unique_signal_events": 20}},
    )
    assert payload["levels"]["30f"]["unique_signal_events"] == 10
    assert payload["levels"]["5f"]["unique_signal_events"] == 20
