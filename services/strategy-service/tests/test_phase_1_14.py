from datetime import datetime

from app.engine.phase_1_14 import _build_bottom_fractal_events, _build_entry_block_reason, build_entry_confidence_builder_v3
from app.repositories.kline_repo import KlineBar


def test_bottom_fractal_first_seen_uses_right_bar_time():
    bars = [
        KlineBar(ts=datetime(2026, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1),
        KlineBar(ts=datetime(2026, 1, 2), open=9.8, high=10.0, low=8.0, close=8.6, volume=1),
        KlineBar(ts=datetime(2026, 1, 3), open=8.7, high=9.9, low=8.5, close=9.4, volume=1),
    ]
    events = _build_bottom_fractal_events("000001.SZ", bars)
    assert len(events) == 1
    assert events[0]["point_time"] == "2026-01-02T00:00:00"
    assert events[0]["first_seen_time"] == "2026-01-03T00:00:00"


def test_entry_block_reason_bottom_not_first_seen_yet_precedes_5f_logic():
    reason = _build_entry_block_reason(
        window_valid=True,
        price_valid=True,
        bottom_confirmed=False,
        bottom_not_first_seen_yet=True,
        five_f_buy_visible=True,
        five_f_b2_visible=True,
        five_f_confirmed=False,
        confidence=40.0,
        entry_candidate=False,
    )
    assert reason == "bottom_fractal_not_first_seen_yet"


def test_entry_confidence_v3_keeps_bottom_not_first_seen_out_of_confirmation():
    daily_rows = [
        {
            "symbol": "000001.SZ",
            "name": "平安银行",
            "as_of_time": "2026-01-10T00:00:00",
            "candidate_daily_b1_after_weekly_context_strict": True,
        }
    ]
    price_rows = [
        {
            "sample_id": "000001.SZ|2026-01-10T00:00:00",
            "window_valid": True,
            "price_valid": True,
            "price_policy_result": True,
        }
    ]
    bottom_rows = [
        {
            "sample_id": "000001.SZ|2026-01-10T00:00:00",
            "daily_bottom_fractal_visible": False,
            "daily_bottom_fractal_failure_reason": "bottom_fractal_exists_but_not_first_seen_yet",
        }
    ]
    five_f_rows = [
        {
            "sample_id": "000001.SZ|2026-01-10T00:00:00",
            "five_f_B2_confirms_30f": False,
            "five_f_buy_any_visible": True,
            "five_f_B2_or_2s_visible": True,
        }
    ]

    payload = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows,
        bottom_rows=bottom_rows,
        five_f_rows=five_f_rows,
        mode_name="strict_daily_b1_after_weekly_context",
        accepted_field="candidate_daily_b1_after_weekly_context_strict",
        thirty_f_price_policy="thirty_f_price_policy_strict_existing",
        status="candidate",
    )
    row = payload["rows"][0]
    assert row["has_30f_confirmation"] is True
    assert row["has_daily_bottom_fractal_confirmation"] is False
    assert row["has_5f_confirmation"] is False
    assert row["confidence"] == 40.0
    assert row["entry_candidate"] is False
    assert row["entry_triggered"] is False
    assert row["entry_block_reason"] == "bottom_fractal_not_first_seen_yet"


def test_entry_confidence_v3_maps_one_two_three_confirmations():
    base_daily = {
        "symbol": "000001.SZ",
        "name": "平安银行",
        "candidate_daily_b1_after_weekly_context_strict": True,
    }
    daily_rows = [
        dict(base_daily, as_of_time="2026-01-10T00:00:00"),
        dict(base_daily, as_of_time="2026-01-11T00:00:00"),
        dict(base_daily, as_of_time="2026-01-12T00:00:00"),
    ]
    price_rows = [
        {"sample_id": "000001.SZ|2026-01-10T00:00:00", "window_valid": True, "price_valid": True, "price_policy_result": True},
        {"sample_id": "000001.SZ|2026-01-11T00:00:00", "window_valid": True, "price_valid": True, "price_policy_result": True},
        {"sample_id": "000001.SZ|2026-01-12T00:00:00", "window_valid": True, "price_valid": True, "price_policy_result": True},
    ]
    bottom_rows = [
        {"sample_id": "000001.SZ|2026-01-10T00:00:00", "daily_bottom_fractal_visible": False, "daily_bottom_fractal_failure_reason": "bottom_fractal_not_found"},
        {"sample_id": "000001.SZ|2026-01-11T00:00:00", "daily_bottom_fractal_visible": True, "daily_bottom_fractal_failure_reason": "bottom_fractal_confirmed"},
        {"sample_id": "000001.SZ|2026-01-12T00:00:00", "daily_bottom_fractal_visible": True, "daily_bottom_fractal_failure_reason": "bottom_fractal_confirmed"},
    ]
    five_f_rows = [
        {"sample_id": "000001.SZ|2026-01-10T00:00:00", "five_f_B2_confirms_30f": False, "five_f_buy_any_visible": True, "five_f_B2_or_2s_visible": True},
        {"sample_id": "000001.SZ|2026-01-11T00:00:00", "five_f_B2_confirms_30f": False, "five_f_buy_any_visible": True, "five_f_B2_or_2s_visible": True},
        {"sample_id": "000001.SZ|2026-01-12T00:00:00", "five_f_B2_confirms_30f": True, "five_f_buy_any_visible": True, "five_f_B2_or_2s_visible": True},
    ]

    payload = build_entry_confidence_builder_v3(
        daily_rows=daily_rows,
        price_rows=price_rows,
        bottom_rows=bottom_rows,
        five_f_rows=five_f_rows,
        mode_name="strict_daily_b1_after_weekly_context",
        accepted_field="candidate_daily_b1_after_weekly_context_strict",
        thirty_f_price_policy="thirty_f_price_policy_strict_existing",
        status="candidate",
    )

    confidences = [row["confidence"] for row in payload["rows"]]
    assert confidences == [40.0, 70.0, 100.0]
    assert payload["summary"]["confidence_70_count"] == 2
    assert payload["summary"]["confidence_100_count"] == 1
