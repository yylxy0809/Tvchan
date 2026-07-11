from __future__ import annotations

from app.engine.phase_1_10 import (
    _distance_bucket,
    classify_symbol_diff,
    classify_visibility_sample,
)


def test_classify_visibility_sample_when_selected_run_missing():
    result = classify_visibility_sample(
        selected_run_missing=True,
        selected_run_has_signals=False,
        selected_run_has_buy=False,
        visible_daily_buy_count=0,
        nearest_before=None,
        nearest_after=None,
        selected_run_group=None,
        selected_run_kind=None,
    )

    assert result == "selected_daily_run_missing"


def test_classify_visibility_sample_when_buy_exists_only_after_asof():
    result = classify_visibility_sample(
        selected_run_missing=False,
        selected_run_has_signals=True,
        selected_run_has_buy=False,
        visible_daily_buy_count=0,
        nearest_before=None,
        nearest_after={"time": "2026-03-01T00:00:00+00:00"},
        selected_run_group="research_daily_close",
        selected_run_kind="historical_backfill",
    )

    assert result == "buy_signal_exists_only_after_asof"


def test_classify_visibility_sample_when_selected_run_has_buy_but_window_hides_it():
    result = classify_visibility_sample(
        selected_run_missing=False,
        selected_run_has_signals=True,
        selected_run_has_buy=True,
        visible_daily_buy_count=0,
        nearest_before={"time": "2026-02-15T00:00:00+00:00"},
        nearest_after=None,
        selected_run_group="research_daily_close",
        selected_run_kind="historical_backfill",
    )

    assert result == "signal_time_filter_mismatch"


def test_classify_symbol_diff_prefers_mode_or_group_mismatch():
    result = classify_symbol_diff(
        current_daily_buy_signal_count=12,
        historical_final_daily_buy_signal_count=10,
        samples_with_visible_daily_buy_signal=0,
        samples_with_future_daily_buy_signal_after_asof=8,
        samples_with_buy_before_asof_but_not_selected=2,
        samples_with_mode_or_group_mismatch=1,
    )

    assert result == "mode_or_run_group_mismatch"


def test_distance_bucket_prefers_before_then_after_then_empty():
    assert _distance_bucket(before_days=3, after_days=None) == "before_0_5d"
    assert _distance_bucket(before_days=None, after_days=18) == "after_6_20d"
    assert _distance_bucket(before_days=None, after_days=None) == "no_daily_buy_in_symbol_window"
