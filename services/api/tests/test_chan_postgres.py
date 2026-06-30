from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from app.repositories.chan_postgres import get_precomputed_chan_overlay_db
from app.repositories.postgres import TIMEFRAME_TO_DB


class FakeAcquire:
    def __init__(self, conn) -> None:
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakePool:
    def __init__(self, conn) -> None:
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)


class FakeConn:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def fetchval(self, query, *args):
        self.queries.append(query)
        return 1

    async def fetchrow(self, query, *args):
        self.queries.append(query)
        return {
            "id": 10,
            "bar_from": datetime.fromtimestamp(100, tz=UTC),
            "bar_until": datetime.fromtimestamp(3000, tz=UTC),
            "bar_count": 500,
            "snapshot_version": "snapshot-fallback-001",
        }

    async def fetch(self, query, *args):
        self.queries.append(query)
        if "from scheme2_chan_published_heads" in query:
            return [
                {
                    "chan_level": TIMEFRAME_TO_DB["5f"],
                    "mode": "confirmed",
                    "run_id": 10,
                    "snapshot_version": "snapshot-group-001",
                    "base_from_bar_end": datetime.fromtimestamp(100, tz=UTC),
                    "base_to_bar_end": datetime.fromtimestamp(3000, tz=UTC),
                    "published_at": datetime.fromtimestamp(3001, tz=UTC),
                    "updated_at": datetime.fromtimestamp(3001, tz=UTC),
                }
            ]
        if "from chan_runs" in query:
            return [
                {
                    "id": 10,
                    "chan_level": 5,
                    "snapshot_version": "snapshot-group-001",
                    "computed_at": datetime.fromtimestamp(3000, tz=UTC),
                    "bar_from": datetime.fromtimestamp(100, tz=UTC),
                    "bar_until": datetime.fromtimestamp(3000, tz=UTC),
                }
            ]
        if "from chan_strokes" in query:
            return [
                {
                    "id": 1,
                    "mode": 1,
                    "seq": 1,
                    "start_ts": datetime.fromtimestamp(900, tz=UTC),
                    "end_ts": datetime.fromtimestamp(1100, tz=UTC),
                    "begin_base_ts": datetime.fromtimestamp(900, tz=UTC),
                    "end_base_ts": datetime.fromtimestamp(1100, tz=UTC),
                    "begin_base_seq": 101,
                    "end_base_seq": 102,
                    "start_price_x1000": 10000,
                    "end_price_x1000": 10500,
                    "direction": 1,
                    "is_confirmed": True,
                    "extra": {"id": "overlapping-stroke"},
                }
            ]
        return []


def test_precomputed_chan_returns_items_intersecting_requested_window() -> None:
    async def scenario():
        conn = FakeConn()
        response = await get_precomputed_chan_overlay_db(
            FakePool(conn),
            symbol="000001.SZ",
            chart_timeframe="5f",
            levels=["5f"],
            modes=["confirmed"],
            requested_bar_count=2,
            bars_by_level={
                "5f": [
                    {"time": 1000},
                    {"time": 2000},
                ]
            },
        )
        assert response is not None
        assert response.engine == "database:chan-precomputed"
        assert response.base_timeframe == "5f"
        assert response.base_ts_semantics == "bar_end"
        assert response.snapshot_version == "snapshot-group-001"
        assert response.strokes[0].id == "overlapping-stroke"
        assert response.strokes[0].start.time == 900
        assert response.strokes[0].start.base_ts == 900
        assert response.strokes[0].start.base_seq == 101
        assert response.strokes[0].end.time == 1100
        assert response.strokes[0].end.base_ts == 1100
        assert response.strokes[0].end.base_seq == 102
        assert response.strokes[0].begin_base_ts == 900
        assert response.strokes[0].end_base_ts == 1100
        assert response.strokes[0].begin_base_seq == 101
        assert response.strokes[0].end_base_seq == 102
        stroke_query = next(query for query in conn.queries if "from chan_strokes" in query)
        assert "coalesce(begin_base_ts, start_ts) <= $4" in stroke_query
        assert "coalesce(end_base_ts, end_ts) >= $3" in stroke_query
        published_query = next(
            query for query in conn.queries if "from scheme2_chan_published_heads" in query
        )
        assert "status = 'published'" in published_query
        assert not any("from chan_runs" in query for query in conn.queries)

    asyncio.run(scenario())


def test_precomputed_chan_does_not_require_raw_bars_for_each_chan_level() -> None:
    async def scenario():
        conn = FakeConn()
        response = await get_precomputed_chan_overlay_db(
            FakePool(conn),
            symbol="000001.SZ",
            chart_timeframe="1d",
            levels=["5f", "30f", "1d"],
            modes=["confirmed"],
            requested_bar_count=2,
            bars_by_level={
                "5f": [
                    {"time": 1000},
                    {"time": 2000},
                ],
                "30f": [],
                "1d": [],
            },
        )
        assert response is not None
        assert response.engine == "database:chan-precomputed"
        assert response.levels == ["5f", "30f", "1d"]
        assert response.bars_by_level == {"5f": 2, "30f": 0, "1d": 0}

    asyncio.run(scenario())


def test_precomputed_chan_prefers_one_grouped_snapshot_version_for_all_levels() -> None:
    class GroupedConn(FakeConn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            if "from scheme2_chan_published_heads" in query:
                return [
                    {
                        "chan_level": TIMEFRAME_TO_DB["5f"],
                        "mode": "confirmed",
                        "run_id": 101,
                        "snapshot_version": "snapshot-group-xyz",
                        "base_from_bar_end": datetime.fromtimestamp(100, tz=UTC),
                        "base_to_bar_end": datetime.fromtimestamp(3000, tz=UTC),
                        "published_at": datetime.fromtimestamp(4000, tz=UTC),
                        "updated_at": datetime.fromtimestamp(4000, tz=UTC),
                    },
                    {
                        "chan_level": TIMEFRAME_TO_DB["30f"],
                        "mode": "confirmed",
                        "run_id": 102,
                        "snapshot_version": "snapshot-group-xyz",
                        "base_from_bar_end": datetime.fromtimestamp(100, tz=UTC),
                        "base_to_bar_end": datetime.fromtimestamp(3000, tz=UTC),
                        "published_at": datetime.fromtimestamp(4000, tz=UTC),
                        "updated_at": datetime.fromtimestamp(4000, tz=UTC),
                    },
                    {
                        "chan_level": TIMEFRAME_TO_DB["1d"],
                        "mode": "confirmed",
                        "run_id": 103,
                        "snapshot_version": "snapshot-group-xyz",
                        "base_from_bar_end": datetime.fromtimestamp(100, tz=UTC),
                        "base_to_bar_end": datetime.fromtimestamp(3000, tz=UTC),
                        "published_at": datetime.fromtimestamp(4000, tz=UTC),
                        "updated_at": datetime.fromtimestamp(4000, tz=UTC),
                    },
                ]
            return await super().fetch(query, *args)

        async def fetchrow(self, query, *args):
            raise AssertionError("grouped snapshot selection should not fall back to per-level fetchrow")

    async def scenario():
        conn = GroupedConn()
        response = await get_precomputed_chan_overlay_db(
            FakePool(conn),
            symbol="000001.SZ",
            chart_timeframe="1d",
            levels=["5f", "30f", "1d"],
            modes=["confirmed"],
            requested_bar_count=2,
            bars_by_level={
                "5f": [{"time": 1000}, {"time": 2000}],
                "30f": [],
                "1d": [],
            },
        )
        assert response is not None
        assert response.snapshot_version == "snapshot-group-xyz"

    asyncio.run(scenario())
