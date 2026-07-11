from __future__ import annotations

from datetime import datetime, timezone

from app.config.strategy_params import StrategyParams
from app.domain.models import ChanSignal
from app.engine.strategy_diagnoser import StrategyDiagnoser


def _signal(*, bsp_type: str, point_time: str, price: float) -> ChanSignal:
    ts = datetime.fromisoformat(point_time).replace(tzinfo=timezone.utc)
    return ChanSignal(
        signal_id=None,
        level="1w",
        mode="predictive",
        point_time=ts,
        base_time=ts,
        base_seq=None,
        price=price,
        signal_type="bsp",
        side="buy",
        bsp_type=bsp_type,
        confirmed=False,
        features={},
    )


def test_select_weekly_context_explicit_requires_prior_b1():
    b1 = _signal(bsp_type="1", point_time="2026-01-03T00:00:00", price=10.0)
    b2 = _signal(bsp_type="2", point_time="2026-02-03T00:00:00", price=11.0)

    selection = StrategyDiagnoser._select_weekly_context(
        weekly_b1_candidates=[b1],
        weekly_b2_candidates=[b2],
        context_mode="explicit_b1_then_b2",
    )

    assert selection is not None
    assert selection.weekly_b1 is b1
    assert selection.context_score == 100.0
    assert selection.bypass_prior_b1_gate is False


def test_select_weekly_context_trust_mode_accepts_b2_without_prior_b1():
    b2 = _signal(bsp_type="2", point_time="2026-02-03T00:00:00", price=11.0)

    selection = StrategyDiagnoser._select_weekly_context(
        weekly_b1_candidates=[],
        weekly_b2_candidates=[b2],
        context_mode="trust_chan_signal",
    )

    assert selection is not None
    assert selection.weekly_b1 is None
    assert selection.context_score == 60.0
    assert selection.bypass_prior_b1_gate is True
    assert selection.bypass_price_break_gate is True


def test_select_weekly_context_scored_mode_boosts_prior_b1():
    b1 = _signal(bsp_type="1", point_time="2026-01-03T00:00:00", price=10.0)
    b2 = _signal(bsp_type="2s", point_time="2026-02-03T00:00:00", price=10.8)

    selection = StrategyDiagnoser._select_weekly_context(
        weekly_b1_candidates=[b1],
        weekly_b2_candidates=[b2],
        context_mode="trust_chan_signal_with_b1_score",
    )

    assert selection is not None
    assert selection.weekly_b1 is b1
    assert selection.context_score == 90.0
    assert selection.anchor_time == b1.point_time


def test_select_daily_setup_near_mode_can_pick_b1_before_weekly_signal():
    weekly_b2 = _signal(bsp_type="2", point_time="2026-02-10T00:00:00", price=11.0)
    weekly_context = type(
        "WeeklyContextStub",
        (),
        {
            "weekly_b2": weekly_b2,
            "anchor_time": weekly_b2.point_time,
        },
    )()
    daily_b1_before = _signal(bsp_type="1", point_time="2026-02-05T00:00:00", price=10.0)
    daily_signals = [daily_b1_before]
    daily_bars = [type("BarStub", (), {"ts": daily_b1_before.point_time})(), type("BarStub", (), {"ts": weekly_b2.point_time})()]
    params = StrategyParams.from_strategy_code("phase_1_4_trust_chan_signal_with_b1_score").with_overrides(
        daily_setup_mode="daily_b1_near_weekly_context",
        daily_b1_lookback_trading_days=10,
        daily_b1_lookforward_trading_days=5,
    )

    selection = StrategyDiagnoser._select_daily_setup(
        daily_signals=daily_signals,
        weekly_context=weekly_context,
        as_of_time=weekly_b2.point_time,
        params=params,
        daily_bars=daily_bars,
    )

    assert selection is not None
    assert selection.daily_b1 is daily_b1_before
    assert selection.setup_mode == "daily_b1_near_weekly_context"
    assert selection.is_official_strategy is False


def test_select_daily_setup_trust_mode_uses_prior_b1_for_b2():
    weekly_b2 = _signal(bsp_type="2", point_time="2026-02-10T00:00:00", price=11.0)
    weekly_context = type(
        "WeeklyContextStub",
        (),
        {
            "weekly_b2": weekly_b2,
            "anchor_time": weekly_b2.point_time,
        },
    )()
    daily_b1 = _signal(bsp_type="1", point_time="2026-02-11T00:00:00", price=10.0)
    daily_b2 = _signal(bsp_type="2", point_time="2026-02-15T00:00:00", price=10.5)
    daily_signals = [daily_b1, daily_b2]
    daily_bars = [type("BarStub", (), {"ts": daily_b1.point_time})(), type("BarStub", (), {"ts": daily_b2.point_time})()]
    params = StrategyParams.from_strategy_code("phase_1_4_trust_chan_signal_with_b1_score").with_overrides(
        daily_setup_mode="trust_daily_b2_or_b2s_signal"
    )

    selection = StrategyDiagnoser._select_daily_setup(
        daily_signals=daily_signals,
        weekly_context=weekly_context,
        as_of_time=daily_b2.point_time,
        params=params,
        daily_bars=daily_bars,
    )

    assert selection is not None
    assert selection.daily_b1 is daily_b1
    assert selection.daily_b2 is daily_b2
    assert selection.signal_source == "trusted_b2"


def test_audit_daily_setup_semantics_true_trust_accepts_b2_without_prior_b1():
    weekly_b2 = _signal(bsp_type="2", point_time="2026-02-10T00:00:00", price=11.0)
    weekly_context = type(
        "WeeklyContextStub",
        (),
        {
            "weekly_b2": weekly_b2,
            "anchor_time": weekly_b2.point_time,
        },
    )()
    daily_b2 = _signal(bsp_type="2", point_time="2026-02-15T00:00:00", price=10.5)
    params = StrategyParams.from_strategy_code("phase_1_4_trust_chan_signal_with_b1_score").with_overrides(
        daily_setup_mode="true_trust_daily_b2_or_b2s"
    )

    audit = StrategyDiagnoser.audit_daily_setup_semantics(
        daily_signals=[daily_b2],
        weekly_context=weekly_context,
        as_of_time=daily_b2.point_time,
        params=params,
        daily_bars=[type("BarStub", (), {"ts": daily_b2.point_time})()],
    )

    assert audit.daily_signal_any_found is True
    assert audit.daily_b1_found is False
    assert audit.daily_b2_or_b2s_found is True
    assert audit.daily_prior_b1_for_b2_found is False
    assert audit.daily_setup_accepted_by_mode is True
    assert audit.selected_daily_b2_or_b2s is daily_b2
    assert audit.selected_signal_source == "self_contained_daily_b2_or_b2s"


def test_audit_daily_setup_semantics_scored_mode_does_not_block_without_prior_b1():
    weekly_b2 = _signal(bsp_type="2", point_time="2026-02-10T00:00:00", price=11.0)
    weekly_context = type(
        "WeeklyContextStub",
        (),
        {
            "weekly_b2": weekly_b2,
            "anchor_time": weekly_b2.point_time,
        },
    )()
    daily_b2s = _signal(bsp_type="2s", point_time="2026-02-15T00:00:00", price=10.8)
    params = StrategyParams.from_strategy_code("phase_1_4_trust_chan_signal_with_b1_score").with_overrides(
        daily_setup_mode="daily_b2_or_b2s_with_b1_score"
    )

    audit = StrategyDiagnoser.audit_daily_setup_semantics(
        daily_signals=[daily_b2s],
        weekly_context=weekly_context,
        as_of_time=daily_b2s.point_time,
        params=params,
        daily_bars=[type("BarStub", (), {"ts": daily_b2s.point_time})()],
    )

    assert audit.daily_setup_accepted_by_mode is True
    assert audit.daily_prior_b1_for_b2_found is False
    assert audit.selected_signal_kind == "2s"
    assert audit.selected_signal_score == 80.0
