from __future__ import annotations

from datetime import UTC, datetime

from app.domain.models import ChanSignal, SymbolInfo
from app.engine.module_c_history_backfill import BackfillPerfSample, build_perf_profile
from app.engine.phase_1_9 import _collect_timeline_sample


def _signal(*, bsp_type: str, point_time: str, price: float) -> ChanSignal:
    ts = datetime.fromisoformat(point_time).replace(tzinfo=UTC)
    return ChanSignal(
        signal_id=None,
        level="1d",
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


def test_collect_timeline_sample_separates_future_signals_from_visible_signals():
    symbol = SymbolInfo(symbol_id=1, symbol="000001.SZ", code="000001", exchange="SZ", name="平安银行")
    as_of_time = datetime(2026, 2, 10, tzinfo=UTC)
    weekly_b2 = _signal(bsp_type="2", point_time="2026-02-03T00:00:00", price=11.0)
    weekly_context = type(
        "WeeklyContextStub",
        (),
        {
            "anchor_time": weekly_b2.point_time,
            "weekly_b2": weekly_b2,
            "context_mode": "trust_chan_signal_with_b1_score",
        },
    )()
    visible_b1 = _signal(bsp_type="1", point_time="2026-02-09T00:00:00", price=10.0)
    future_b2 = _signal(bsp_type="2", point_time="2026-02-20T00:00:00", price=10.5)

    row = _collect_timeline_sample(
        symbol=symbol,
        as_of_time=as_of_time,
        weekly_context=weekly_context,
        all_daily_signals=[visible_b1, future_b2],
        future_window_end=datetime(2026, 3, 1, tzinfo=UTC),
        failure_reason_v2="daily_B1_exists_but_after_as_of",
    )

    assert [item["bsp_type"] for item in row["daily_signals_before_as_of"]] == ["1"]
    assert [item["bsp_type"] for item in row["daily_signals_after_as_of_within_window"]] == ["2"]
    assert row["nearest_daily_B1_before"]["bsp_type"] == "1"
    assert row["nearest_daily_B2_or_B2s_before"] is None


def test_build_perf_profile_aggregates_per_symbol_and_per_level():
    samples = [
        BackfillPerfSample(
            symbol="000001.SZ",
            level="1d",
            cutoff_time=datetime(2026, 1, 1, tzinfo=UTC),
            bar_count=100,
            schedule_build_seconds=0.1,
            resume_check_seconds=0.01,
            overlay_build_seconds=0.5,
            db_insert_seconds=0.2,
            total_snapshot_seconds=0.81,
        ),
        BackfillPerfSample(
            symbol="000001.SZ",
            level="1d",
            cutoff_time=datetime(2026, 1, 2, tzinfo=UTC),
            bar_count=101,
            schedule_build_seconds=0.2,
            resume_check_seconds=0.02,
            overlay_build_seconds=0.6,
            db_insert_seconds=0.3,
            total_snapshot_seconds=1.12,
        ),
        BackfillPerfSample(
            symbol="000002.SZ",
            level="30f",
            cutoff_time=datetime(2026, 1, 2, tzinfo=UTC),
            bar_count=60,
            schedule_build_seconds=0.05,
            resume_check_seconds=0.01,
            overlay_build_seconds=0.4,
            db_insert_seconds=0.1,
            total_snapshot_seconds=0.56,
        ),
    ]

    payload = build_perf_profile(samples)

    assert payload["sample_count"] == 3
    assert {row["symbol"] for row in payload["per_symbol"]} == {"000001.SZ", "000002.SZ"}
    assert {row["level"] for row in payload["per_level"]} == {"1d", "30f"}
    assert payload["aggregate"]["sample_count"] == 3
    assert payload["aggregate"]["total_snapshot_seconds"]["sum"] == 2.49
