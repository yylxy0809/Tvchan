from __future__ import annotations

from app.config.strategy_params import (
    DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_STRATEGY_CODE,
    PHASE_1_4_EXPLICIT_B1_THEN_B2_STRATEGY_CODE,
    PHASE_1_4_TRUST_CHAN_SIGNAL_STRATEGY_CODE,
    PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE,
    STRICT_EXPLICIT_B1_STRATEGY_CODE,
    StrategyParams,
)


def test_phase_1_3_strategy_modes_are_mapped():
    strict = StrategyParams.from_strategy_code(STRICT_EXPLICIT_B1_STRATEGY_CODE)
    trust_b2 = StrategyParams.from_strategy_code(DIAG_TRUST_B2_STRATEGY_CODE)
    trust_b2_or_b2s = StrategyParams.from_strategy_code(DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE)
    same_bar = StrategyParams.from_strategy_code(DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE)

    assert strict.weekly_context_mode == "explicit_prior_b1"
    assert strict.weekly_b2_types == ["2"]

    assert trust_b2.weekly_context_mode == "trust_weekly_b2_signal"
    assert trust_b2.weekly_b2_types == ["2"]

    assert trust_b2_or_b2s.weekly_context_mode == "trust_weekly_b2_or_b2s_signal"
    assert trust_b2_or_b2s.weekly_b2_types == ["2", "2s"]

    assert same_bar.weekly_context_mode == "same_bar_b1_b2s_as_candidate"
    assert same_bar.weekly_b2_types == ["2s"]


def test_phase_1_4_strategy_modes_are_mapped():
    explicit = StrategyParams.from_strategy_code(PHASE_1_4_EXPLICIT_B1_THEN_B2_STRATEGY_CODE)
    trust = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_STRATEGY_CODE)
    scored = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE)

    assert explicit.weekly_context_mode == "explicit_b1_then_b2"
    assert explicit.weekly_context_mode_normalized == "explicit_b1_then_b2"
    assert explicit.weekly_b2_types == ["2"]

    assert trust.weekly_context_mode == "trust_chan_signal"
    assert trust.weekly_context_mode_normalized == "trust_chan_signal"
    assert trust.weekly_b2_types == ["2"]

    assert scored.weekly_context_mode == "trust_chan_signal_with_b1_score"
    assert scored.weekly_context_mode_normalized == "trust_chan_signal_with_b1_score"
    assert scored.weekly_b2_types == ["2", "2s"]


def test_daily_setup_mode_overrides_are_applied():
    params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE)
    overridden = params.with_overrides(
        daily_setup_mode="daily_b1_near_weekly_context",
        daily_b1_lookback_trading_days=45,
        daily_b1_lookforward_trading_days=15,
    )

    assert params.daily_setup_mode == "strict_daily_b1_after_weekly_context"
    assert params.is_official_daily_setup_mode is True

    assert overridden.daily_setup_mode == "daily_b1_near_weekly_context"
    assert overridden.daily_b1_lookback_trading_days == 45
    assert overridden.daily_b1_lookforward_trading_days == 15
    assert overridden.is_official_daily_setup_mode is False


def test_diagnostic_only_daily_setup_mode_is_flagged():
    params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE).with_overrides(
        daily_setup_mode="true_trust_daily_b2_or_b2s"
    )

    assert params.is_official_daily_setup_mode is False
    assert params.is_diagnostic_only_daily_setup_mode is True


def test_daily_signal_source_overrides_are_applied():
    params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE)
    overridden = params.with_overrides(daily_signal_source="event_ledger")

    assert params.daily_signal_source == "selected_run"
    assert overridden.daily_signal_source == "event_ledger"
