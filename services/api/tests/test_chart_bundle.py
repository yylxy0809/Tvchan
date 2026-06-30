from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace

from app.core.config import Settings
from app.models import (
    BarResponse,
    BarsResponse,
    ChanOverlayResponse,
    ChanPointResponse,
    ChanStrokeResponse,
)
from app.routes import chart


def test_build_chart_window_keeps_bars_and_chan_on_one_snapshot(monkeypatch) -> None:
    bars = BarsResponse(
        symbol="000001.SZ",
        timeframe="30f",
        bars=[
            BarResponse(
                time=1_780_950_600,
                open=10.5,
                high=10.7,
                low=10.4,
                close=10.6,
                volume=1000,
                amount=10_000.0,
                complete=True,
                revision=2,
            ),
            BarResponse(
                time=1_780_952_400,
                open=10.6,
                high=10.9,
                low=10.55,
                close=10.8,
                volume=1200,
                amount=12_000.0,
                complete=True,
                revision=3,
            ),
        ],
    )
    overlay = ChanOverlayResponse(
        symbol="000001.SZ",
        chart_timeframe="30f",
        levels=["5f", "30f", "1d"],
        modes=["confirmed", "predictive"],
        snapshot_version="duckdb-snapshot-001",
        base_timeframe="5f",
        base_ts_semantics="bar_end",
        engine="chan-service:chan.py",
        requested_bar_count=300,
        bars_by_level={"5f": 300, "30f": 300, "1d": 300},
        strokes=[],
        segments=[],
        centers=[],
        signals=[],
    )

    async def fake_build_bars_response(**kwargs):
        return bars

    async def fake_build_chan_overlay(**kwargs):
        return overlay

    monkeypatch.setattr(chart, "build_bars_response", fake_build_bars_response)
    monkeypatch.setattr(chart, "build_chan_overlay", fake_build_chan_overlay)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    response = asyncio.run(
        chart.build_chart_window(
            request=request,
            symbol="000001.SZ",
            timeframe="30f",
            levels="5f,30f,1d",
            modes="confirmed,predictive",
            from_ts=datetime.fromtimestamp(1_780_950_600),
            to_ts=datetime.fromtimestamp(1_780_952_400),
            limit=300,
            settings=Settings(),
        )
    )

    assert response.symbol == bars.symbol
    assert response.chart_timeframe == bars.timeframe
    assert response.chan.snapshot_version == "duckdb-snapshot-001"
    assert response.snapshot_id == chart._snapshot_id(
        bars,
        overlay.engine,
        overlay.snapshot_version,
    )
    assert response.chan.levels == ["5f", "30f", "1d"]
    assert response.chan.base_timeframe == "5f"


def test_build_chart_bundle_is_stable_for_identical_snapshot_inputs(monkeypatch) -> None:
    bars = BarsResponse(
        symbol="000001.SZ",
        timeframe="30f",
        bars=[
            BarResponse(
                time=1_780_950_600,
                open=10.5,
                high=10.7,
                low=10.4,
                close=10.6,
                volume=1000,
                amount=10_000.0,
                complete=True,
                revision=2,
            ),
            BarResponse(
                time=1_780_952_400,
                open=10.6,
                high=10.9,
                low=10.55,
                close=10.8,
                volume=1200,
                amount=12_000.0,
                complete=True,
                revision=3,
            ),
        ],
    )
    overlay = ChanOverlayResponse(
        symbol="000001.SZ",
        chart_timeframe="30f",
        levels=["5f", "30f", "1d"],
        modes=["confirmed", "predictive"],
        snapshot_version="duckdb-snapshot-001",
        base_timeframe="5f",
        base_ts_semantics="bar_end",
        engine="chan-service:chan.py",
        requested_bar_count=300,
        bars_by_level={"5f": 300, "30f": 300, "1d": 300},
        strokes=[],
        segments=[],
        centers=[],
        signals=[],
    )

    async def fake_build_bars_response(**kwargs):
        return bars

    async def fake_build_chan_overlay(**kwargs):
        return overlay

    monkeypatch.setattr(chart, "build_bars_response", fake_build_bars_response)
    monkeypatch.setattr(chart, "build_chan_overlay", fake_build_chan_overlay)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    first = asyncio.run(
        chart.build_chart_bundle(
            request=request,
            symbol="000001.SZ",
            timeframe="30f",
            levels="5f,30f,1d",
            modes="confirmed,predictive",
            from_ts=datetime.fromtimestamp(1_780_950_600),
            to_ts=datetime.fromtimestamp(1_780_952_400),
            limit=300,
            settings=Settings(),
        )
    )
    second = asyncio.run(
        chart.build_chart_bundle(
            request=request,
            symbol="000001.SZ",
            timeframe="30f",
            levels="5f,30f,1d",
            modes="confirmed,predictive",
            from_ts=datetime.fromtimestamp(1_780_950_600),
            to_ts=datetime.fromtimestamp(1_780_952_400),
            limit=300,
            settings=Settings(),
        )
    )

    assert first.snapshot_id == second.snapshot_id
    assert first.chan.snapshot_version == second.chan.snapshot_version == "duckdb-snapshot-001"


def test_build_chart_bundle_v3_groups_chan_levels_and_emits_source_metadata(monkeypatch) -> None:
    view_bars = BarsResponse(
        symbol="000001.SZ",
        timeframe="30f",
        bars=[
            BarResponse(
                time=1_780_950_600,
                open=10.5,
                high=10.7,
                low=10.4,
                close=10.6,
                volume=1000,
                amount=10_000.0,
                complete=True,
                revision=2,
            ),
            BarResponse(
                time=1_780_952_400,
                open=10.6,
                high=10.9,
                low=10.55,
                close=10.8,
                volume=1200,
                amount=12_000.0,
                complete=False,
                revision=3,
            ),
        ],
    )
    canonical_5f = BarsResponse(
        symbol="000001.SZ",
        timeframe="5f",
        bars=[
            BarResponse(
                time=1_780_949_700,
                open=10.1,
                high=10.2,
                low=10.0,
                close=10.15,
                volume=100,
                amount=None,
                complete=True,
                revision=1,
            ),
            BarResponse(
                time=1_780_950_000,
                open=10.15,
                high=10.3,
                low=10.1,
                close=10.25,
                volume=200,
                amount=None,
                complete=True,
                revision=1,
            ),
        ],
    )
    overlay = ChanOverlayResponse(
        symbol="000001.SZ",
        chart_timeframe="30f",
        levels=["5f", "30f", "1d"],
        modes=["confirmed", "predictive"],
        snapshot_version="canonical-snapshot-001",
        base_timeframe="5f",
        base_ts_semantics="bar_end",
        engine="database:chan-precomputed",
        requested_bar_count=300,
        bars_by_level={"5f": 2, "30f": 1, "1d": 0},
        strokes=[
            ChanStrokeResponse(
                id="5f-stroke",
                level="5f",
                mode="confirmed",
                start=ChanPointResponse(time=1_780_949_700, price=10.1, base_ts=1_780_949_700),
                end=ChanPointResponse(time=1_780_950_000, price=10.25, base_ts=1_780_950_000),
                begin_base_ts=1_780_949_700,
                end_base_ts=1_780_950_000,
                direction="up",
                confirmed=True,
            ),
            ChanStrokeResponse(
                id="30f-stroke",
                level="30f",
                mode="confirmed",
                start=ChanPointResponse(time=1_780_949_700, price=10.1, base_ts=1_780_949_700),
                end=ChanPointResponse(time=1_780_950_000, price=10.25, base_ts=1_780_950_000),
                begin_base_ts=1_780_949_700,
                end_base_ts=1_780_950_000,
                direction="up",
                confirmed=True,
            ),
        ],
        segments=[],
        centers=[],
        signals=[],
    )

    async def fake_build_bars_response(**kwargs):
        if kwargs["timeframe"] == "5f":
            return canonical_5f
        return view_bars

    async def fake_build_chan_overlay(**kwargs):
        assert kwargs["levels"] == "5f,30f,1d"
        assert kwargs["modes"] == "confirmed,predictive"
        return overlay

    monkeypatch.setattr(chart, "build_bars_response", fake_build_bars_response)
    monkeypatch.setattr(chart, "build_chan_overlay", fake_build_chan_overlay)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    response = asyncio.run(
        chart.build_chart_bundle_v3(
            request=request,
            symbol="000001.SZ",
            timeframe="30f",
            from_ts=datetime.fromtimestamp(1_780_950_600),
            to_ts=datetime.fromtimestamp(1_780_952_400),
            limit=300,
            settings=Settings(),
        )
    )

    assert response.schema_version == "chart-bundle.v3"
    assert response.snapshot_version == "canonical-snapshot-001"
    assert response.base_timeframe == "5f"
    assert response.bar_time_semantics == "bar_end"
    assert response.analysis_levels == ["5f", "30f", "1d"]
    assert set(response.chan.levels) == {"5f", "30f", "1d"}
    assert response.chan.levels["5f"].bar_count == 2
    assert response.chan.levels["5f"].strokes[0].id == "5f-stroke"
    assert response.chan.levels["30f"].strokes[0].id == "30f-stroke"
    assert response.chan.levels["1d"].strokes == []
    assert response.source_watermarks.canonical_5f_last_complete_end == 1_780_950_000
    assert response.source_watermarks.canonical_5f_last_seen_end == 1_780_950_000
    assert response.source_watermarks.view_last_complete_end == 1_780_950_600
    assert response.source_watermarks.analysis_source == "precomputed"
    assert response.source_watermarks.aggregation_source == "canonical-5f"
    assert response.warnings[0].code == "VIEW_BAR_INCOMPLETE"


def test_build_chart_bundle_v3_warns_for_unimplemented_week_month_aggregation(monkeypatch) -> None:
    bars = BarsResponse(
        symbol="000001.SZ",
        timeframe="1w",
        bars=[
            BarResponse(
                time=1_780_956_000,
                open=10,
                high=11,
                low=9,
                close=10.5,
                volume=1000,
                amount=None,
                complete=True,
                revision=0,
            )
        ],
    )
    overlay = ChanOverlayResponse(
        symbol="000001.SZ",
        chart_timeframe="1w",
        levels=["5f", "30f", "1d"],
        modes=["confirmed", "predictive"],
        snapshot_version="canonical-snapshot-001",
        base_timeframe="5f",
        base_ts_semantics="bar_end",
        engine="api-fake-overlay",
        requested_bar_count=1,
        bars_by_level={"5f": 1, "30f": 1, "1d": 1},
        strokes=[],
        segments=[],
        centers=[],
        signals=[],
    )

    async def fake_build_bars_response(**kwargs):
        return bars if kwargs["timeframe"] != "5f" else bars.model_copy(update={"timeframe": "5f"})

    async def fake_build_chan_overlay(**kwargs):
        return overlay

    monkeypatch.setattr(chart, "build_bars_response", fake_build_bars_response)
    monkeypatch.setattr(chart, "build_chan_overlay", fake_build_chan_overlay)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    response = asyncio.run(
        chart.build_chart_bundle_v3(
            request=request,
            symbol="000001.SZ",
            timeframe="1w",
            from_ts=None,
            to_ts=None,
            limit=1,
            settings=Settings(),
        )
    )

    assert {warning.code for warning in response.warnings} == {"AGGREGATION_FALLBACK"}
