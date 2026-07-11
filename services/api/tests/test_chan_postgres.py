from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from app.repositories.chan_postgres import (
    _select_windowed_module_c_runs,
    _stroke_row_to_response,
    get_available_precomputed_chan_levels_db,
)
from app.repositories.postgres import TIMEFRAME_TO_DB


class FakePool:
    async def fetch(self, query, *args):
        assert "from scheme2_chan_c_published_heads" in query
        assert "head.status = 'published'" in query
        assert args[2] == [
            TIMEFRAME_TO_DB["5f"],
            TIMEFRAME_TO_DB["30f"],
            TIMEFRAME_TO_DB["1d"],
            TIMEFRAME_TO_DB["1w"],
            TIMEFRAME_TO_DB["1m"],
        ]
        return [
            {"chan_level": TIMEFRAME_TO_DB["1d"]},
            {"chan_level": TIMEFRAME_TO_DB["5f"]},
        ]


def test_available_module_c_levels_preserve_requested_order_and_integer_codes() -> None:
    levels = asyncio.run(get_available_precomputed_chan_levels_db(
        FakePool(),
        symbol="000001.SZ",
        levels=["5f", "30f", "1d", "1w", "1m"],
    ))
    assert levels == ["5f", "1d"]


def test_windowed_module_c_head_selection_requires_native_successful_published_runs() -> None:
    class Conn:
        async def fetch(self, query, *args):
            assert "from scheme2_chan_c_published_heads" in query
            assert "head.base_timeframe = head.chan_level" in query
            assert "head.status = 'published'" in query
            assert "run.status = 'success'" in query
            assert "run.config_hash = any($6::varchar[])" in query
            return []

    selected = asyncio.run(_select_windowed_module_c_runs(
        Conn(),
        symbol_id=1,
        levels=["5f"],
        modes=["confirmed"],
        first_ts=datetime.fromtimestamp(100, UTC),
        last_ts=datetime.fromtimestamp(200, UTC),
    ))
    assert selected is None


def test_higher_level_stroke_points_project_to_chart_bars() -> None:
    end_of_day = int(datetime(2026, 7, 3, 7, 0, tzinfo=UTC).timestamp())
    morning_low_bar = int(datetime(2026, 7, 3, 2, 0, tzinfo=UTC).timestamp())
    morning_high_bar = int(datetime(2026, 7, 3, 2, 30, tzinfo=UTC).timestamp())
    afternoon_bar = int(datetime(2026, 7, 3, 6, 30, tzinfo=UTC).timestamp())
    stroke = _stroke_row_to_response(
        {
            "id": 77, "mode": 1, "seq": 8,
            "start_ts": datetime.fromtimestamp(end_of_day, tz=UTC),
            "end_ts": datetime.fromtimestamp(end_of_day, tz=UTC),
            "begin_base_ts": datetime.fromtimestamp(end_of_day, tz=UTC),
            "end_base_ts": datetime.fromtimestamp(end_of_day, tz=UTC),
            "begin_base_seq": 1, "end_base_seq": 2,
            "start_price_x1000": 9900, "end_price_x1000": 11200,
            "direction": 1, "is_confirmed": True, "extra": {"id": "1d-stroke"},
        },
        "1d",
        chart_timeframe="30f",
        projection_bars=[
            {"time": morning_low_bar, "open": 10.1, "high": 10.2, "low": 9.9, "close": 10.0},
            {"time": morning_high_bar, "open": 10.8, "high": 11.2, "low": 10.7, "close": 11.0},
            {"time": afternoon_bar, "open": 10.5, "high": 10.7, "low": 10.3, "close": 10.6},
        ],
    )

    assert stroke.start.time == morning_low_bar
    assert stroke.start.base_ts == morning_low_bar
    assert stroke.begin_base_ts == morning_low_bar
    assert stroke.end.time == morning_high_bar
    assert stroke.end.base_ts == morning_high_bar
    assert stroke.end_base_ts == morning_high_bar


def test_higher_level_projection_prefers_last_exact_price_match() -> None:
    previous_day = int(datetime(2026, 7, 2, 7, 0, tzinfo=UTC).timestamp())
    end_of_day = int(datetime(2026, 7, 3, 7, 0, tzinfo=UTC).timestamp())
    first_equal_high = int(datetime(2026, 7, 3, 2, 0, tzinfo=UTC).timestamp())
    last_equal_high = int(datetime(2026, 7, 3, 6, 30, tzinfo=UTC).timestamp())
    stroke = _stroke_row_to_response(
        {
            "id": 78, "mode": 1, "seq": 9,
            "start_ts": datetime.fromtimestamp(previous_day, tz=UTC),
            "end_ts": datetime.fromtimestamp(end_of_day, tz=UTC),
            "begin_base_ts": datetime.fromtimestamp(previous_day, tz=UTC),
            "end_base_ts": datetime.fromtimestamp(end_of_day, tz=UTC),
            "begin_base_seq": 1, "end_base_seq": 2,
            "start_price_x1000": 9900, "end_price_x1000": 11200,
            "direction": 1, "is_confirmed": True, "extra": {"id": "1d-stroke-last-exact"},
        },
        "1d",
        chart_timeframe="30f",
        projection_bars=[
            {"time": first_equal_high, "open": 10.8, "high": 11.2, "low": 10.7, "close": 11.0},
            {"time": last_equal_high, "open": 10.9, "high": 11.2, "low": 10.8, "close": 11.1},
        ],
        native_bars=[
            {"time": previous_day, "open": 10.1, "high": 11.0, "low": 9.9, "close": 10.3},
            {"time": end_of_day, "open": 10.3, "high": 11.2, "low": 10.1, "close": 11.0},
        ],
    )

    assert stroke.end.time == last_equal_high
    assert stroke.end.base_ts == last_equal_high
    assert stroke.end_base_ts == last_equal_high
