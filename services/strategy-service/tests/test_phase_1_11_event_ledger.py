from __future__ import annotations

from datetime import UTC, datetime

from app.config.strategy_params import PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE, StrategyParams
from app.domain.models import SymbolInfo
from app.engine.phase_1_11 import _aggregate_daily_signal_event_ledger, _event_to_signal
from app.engine.strategy_diagnoser import StrategyDiagnoser


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def _row(*, run_id: int, bar_until: str, cutoff: str, signal_time: str, price_x1000: int, bsp_type: str) -> dict:
    return {
        "run_id": run_id,
        "symbol_id": 1,
        "mode": 2,
        "run_kind": "historical_backfill",
        "run_group_id": "research_daily_close",
        "bar_until": _dt(bar_until),
        "cutoff_bar_end": _dt(cutoff),
        "signal_id": run_id * 10,
        "signal_ts": _dt(signal_time),
        "signal_base_ts": _dt(signal_time),
        "price_x1000": price_x1000,
        "signal_type": "bsp",
        "is_confirmed": False,
        "extra": {"side": "buy", "bsp_type": bsp_type, "features": {}},
    }


def _weekly_context(anchor: str):
    weekly_signal = type("WeeklySignalStub", (), {"point_time": _dt(anchor)})()
    return type("WeeklyContextStub", (), {"weekly_b2": weekly_signal, "anchor_time": _dt(anchor)})()


def test_daily_signal_event_ledger_deduplicates_across_runs():
    payload = _aggregate_daily_signal_event_ledger(
        rows=[
            _row(run_id=1, bar_until="2025-02-01T00:00:00", cutoff="2025-02-01T00:00:00", signal_time="2025-01-30T00:00:00", price_x1000=12340, bsp_type="2"),
            _row(run_id=2, bar_until="2025-02-02T00:00:00", cutoff="2025-02-02T00:00:00", signal_time="2025-01-30T00:00:00", price_x1000=12340, bsp_type="2"),
        ],
        symbol_map={1: SymbolInfo(symbol_id=1, symbol="000001.SZ", code="000001", exchange="SZ", name="平安银行")},
        start_time=_dt("2025-01-01T00:00:00"),
        end_time=_dt("2025-03-01T00:00:00"),
    )

    assert payload["summary"]["raw_signal_rows"] == 2
    assert payload["summary"]["unique_signal_events"] == 1
    assert payload["events"][0]["observed_run_count"] == 2


def test_daily_signal_event_ledger_first_seen_time_uses_run_cutoff():
    payload = _aggregate_daily_signal_event_ledger(
        rows=[
            _row(run_id=5, bar_until="2025-02-03T00:00:00", cutoff="2025-02-01T00:00:00", signal_time="2025-01-30T00:00:00", price_x1000=12340, bsp_type="2"),
        ],
        symbol_map={1: SymbolInfo(symbol_id=1, symbol="000001.SZ", code="000001", exchange="SZ", name="平安银行")},
        start_time=_dt("2025-01-01T00:00:00"),
        end_time=_dt("2025-03-01T00:00:00"),
    )

    assert payload["events"][0]["first_seen_time"] == "2025-02-01T00:00:00+00:00"
    assert payload["events"][0]["first_seen_cutoff_bar_end"] == "2025-02-01T00:00:00+00:00"


def test_daily_signal_event_ledger_does_not_use_future_signal_before_first_seen():
    payload = _aggregate_daily_signal_event_ledger(
        rows=[
            _row(run_id=5, bar_until="2025-02-03T00:00:00", cutoff="2025-02-05T00:00:00", signal_time="2025-01-30T00:00:00", price_x1000=12340, bsp_type="2"),
        ],
        symbol_map={1: SymbolInfo(symbol_id=1, symbol="000001.SZ", code="000001", exchange="SZ", name="平安银行")},
        start_time=_dt("2025-01-01T00:00:00"),
        end_time=_dt("2025-03-01T00:00:00"),
    )
    event = payload["events"][0]

    visible_before = parse_visible(event, _dt("2025-02-04T00:00:00"))
    visible_after = parse_visible(event, _dt("2025-02-05T00:00:00"))

    assert visible_before is False
    assert visible_after is True


def parse_visible(event: dict, as_of_time: datetime) -> bool:
    return _dt(event["first_seen_time"]) <= as_of_time


def test_daily_setup_event_ledger_source_sees_prior_observed_event():
    event = {
        "level": "1d",
        "mode": "predictive",
        "side": "buy",
        "bsp_type": "2",
        "signal_type": "bsp",
        "signal_point_time": "2025-02-15T00:00:00+00:00",
        "signal_ts": "2025-02-15T00:00:00+00:00",
        "signal_base_ts": "2025-02-15T00:00:00+00:00",
        "price_x1000": 10500,
        "is_confirmed": False,
        "first_seen_time": "2025-02-18T00:00:00+00:00",
        "first_seen_run_id": 5,
        "extra_json": {},
    }
    signal = _event_to_signal(event)
    params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE).with_overrides(
        daily_setup_mode="daily_buy_signal_any_observation",
        daily_signal_source="event_ledger",
    )

    audit = StrategyDiagnoser.audit_daily_setup_semantics(
        daily_signals=[signal],
        weekly_context=_weekly_context("2025-02-10T00:00:00"),
        as_of_time=_dt("2025-02-20T00:00:00"),
        params=params,
        daily_bars=[type("BarStub", (), {"ts": _dt("2025-02-15T00:00:00")})()],
    )

    assert audit.daily_setup_accepted_by_mode is True
    assert audit.selected_signal_source == "daily_buy_signal_any"


def test_true_trust_daily_b2_or_b2s_uses_self_contained_ledger_event():
    event = {
        "level": "1d",
        "mode": "predictive",
        "side": "buy",
        "bsp_type": "2s",
        "signal_type": "bsp",
        "signal_point_time": "2025-02-15T00:00:00+00:00",
        "signal_ts": "2025-02-15T00:00:00+00:00",
        "signal_base_ts": "2025-02-15T00:00:00+00:00",
        "price_x1000": 10800,
        "is_confirmed": False,
        "first_seen_time": "2025-02-18T00:00:00+00:00",
        "first_seen_run_id": 8,
        "extra_json": {},
    }
    signal = _event_to_signal(event)
    params = StrategyParams.from_strategy_code(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE).with_overrides(
        daily_setup_mode="true_trust_daily_b2_or_b2s",
        daily_signal_source="event_ledger",
    )

    audit = StrategyDiagnoser.audit_daily_setup_semantics(
        daily_signals=[signal],
        weekly_context=_weekly_context("2025-02-10T00:00:00"),
        as_of_time=_dt("2025-02-20T00:00:00"),
        params=params,
        daily_bars=[type("BarStub", (), {"ts": _dt("2025-02-15T00:00:00")})()],
    )

    assert audit.daily_setup_accepted_by_mode is True
    assert audit.selected_signal_source == "self_contained_daily_b2_or_b2s"
