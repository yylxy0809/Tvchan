from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from app.repositories.postgres import (
    TIMEFRAME_TO_DB,
    _candidate_lower_bounds,
    _fetch_bars_by_symbol_id,
    canonicalize_bar_rows,
    _lookback_days,
    get_bars_db,
)


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def test_missing_watermark_still_bounds_weekly_chunk_scan() -> None:
    async def scenario():
        bounds = await _candidate_lower_bounds(
            FakePool(FakeConn()),
            symbol_id=1,
            timeframe_code=TIMEFRAME_TO_DB["1w"],
            timeframe="1w",
            start=None,
            end=None,
            limit=300,
        )
        assert bounds[0] is not None
        assert bounds[-1] is None
        assert _lookback_days("1w", 300) >= 300 * 7

    asyncio.run(scenario())


def test_monthly_candidate_bounds_do_not_overflow_for_the_api_maximum_limit() -> None:
    async def scenario():
        bounds = await _candidate_lower_bounds(
            FakePool(FakeConn()),
            symbol_id=1,
            timeframe_code=TIMEFRAME_TO_DB["1m"],
            timeframe="1m",
            start=None,
            end=None,
            limit=5000,
        )
        assert all(bound is None or bound.year >= 1 for bound in bounds)

    asyncio.run(scenario())


def test_end_only_history_request_uses_bounded_lookback_candidates() -> None:
    async def scenario():
        end = datetime(2026, 7, 6, 9, 35, tzinfo=SHANGHAI_TZ)
        bounds = await _candidate_lower_bounds(
            FakePool(FakeConn()),
            symbol_id=1,
            timeframe_code=TIMEFRAME_TO_DB["5f"],
            timeframe="5f",
            start=None,
            end=end,
            limit=279,
        )

        assert bounds[0] is not None
        assert bounds[0] < end
        assert bounds[-1] is None

    asyncio.run(scenario())


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

    async def fetch(self, query, *args):
        return await self.conn.fetch(query, *args)

    async def fetchval(self, query, *args):
        if "from symbols" in query:
            return 1
        return None


class FakeConn:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.args: list[tuple] = []

    async def fetch(self, query, *args):
        self.queries.append(query)
        self.args.append(args)
        if "chart_period_bars" in query:
            return []
        return [
            {
                "ts": datetime.fromtimestamp(1000, tz=UTC),
                "open_x1000": 10000,
                "high_x1000": 10100,
                "low_x1000": 9900,
                "close_x1000": 10050,
                "volume": 100,
                "amount_x100": 12345,
                "is_complete": True,
                "revision": 0,
            }
        ]


def test_get_bars_db_excludes_seed_when_real_source_exists_for_stored_timeframe() -> None:
    async def scenario():
        conn = FakeConn()
        rows = await get_bars_db(
            FakePool(conn),
            symbol="000001.SZ",
            timeframe="1w",
            start=None,
            end=None,
            limit=300,
        )
        assert rows[0]["close"] == 10.05
        query_index = next(index for index, query in enumerate(conn.queries) if "source = any($3::smallint[])" in query)
        query = conn.queries[query_index]
        assert "source = any($3::smallint[])" in query
        assert conn.args[query_index][0:3] == (1, TIMEFRAME_TO_DB["1w"], [2, 3, 4, 5, 6, 7, 8, 9])

    asyncio.run(scenario())


def test_period_chart_cache_is_preferred_when_it_covers_the_requested_limit() -> None:
    class CachedPeriodConn(FakeConn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            self.args.append(args)
            if "chart_period_bars" in query:
                return [
                    {
                        "ts": datetime(2026, 6, 26, 15, tzinfo=SHANGHAI_TZ),
                        "open_x1000": 10000,
                        "high_x1000": 10100,
                        "low_x1000": 9900,
                        "close_x1000": 10050,
                        "volume": 100,
                        "amount_x100": 12345,
                        "is_complete": True,
                        "revision": 0,
                    }
                ]
            raise AssertionError("period cache hit must not query the klines hypertable")

    async def scenario():
        conn = CachedPeriodConn()
        rows = await get_bars_db(
            FakePool(conn),
            symbol="000001.SZ",
            timeframe="1m",
            start=None,
            end=None,
            limit=1,
        )
        assert rows[0]["close"] == 10.05
        assert "chart_period_bars" in conn.queries[0]

    asyncio.run(scenario())


def test_bounded_period_chart_cache_serves_its_cached_tail_without_hypertable_fallback() -> None:
    cache_floor = datetime(2020, 1, 31, 15, tzinfo=SHANGHAI_TZ)

    class CachedPeriodConn(FakeConn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            self.args.append(args)
            if "chart_period_bars" in query:
                return [
                    {
                        "ts": datetime(2026, 6, 30, 15, tzinfo=SHANGHAI_TZ),
                        "open_x1000": 10000,
                        "high_x1000": 10100,
                        "low_x1000": 9900,
                        "close_x1000": 10050,
                        "volume": 100,
                        "amount_x100": 12345,
                        "is_complete": True,
                        "revision": 0,
                    }
                ]
            raise AssertionError("cached tail must not query the klines hypertable")

    class CachedPeriodPool(FakePool):
        async def fetchval(self, query, *args):
            if "min(ts)" in query:
                return cache_floor
            return await super().fetchval(query, *args)

    async def scenario():
        rows = await get_bars_db(
            CachedPeriodPool(CachedPeriodConn()),
            symbol="000001.SZ",
            timeframe="1m",
            start=cache_floor,
            end=datetime(2026, 7, 1, 15, tzinfo=SHANGHAI_TZ),
            limit=300,
        )
        assert rows[0]["close"] == 10.05

    asyncio.run(scenario())


def test_reader_dedupes_daily_midnight_fallback_against_pytdx_close() -> None:
    midnight = datetime(2026, 7, 10, tzinfo=SHANGHAI_TZ)
    close = midnight.replace(hour=15)
    rows = canonicalize_bar_rows(
        "1d",
        [
            {
                "ts": midnight,
                "open_x1000": 10000,
                "high_x1000": 11000,
                "low_x1000": 9000,
                "close_x1000": 10500,
                "volume": 10001,
                "amount_x100": None,
                "is_complete": True,
                "revision": 0,
                "source": 6,
                "updated_at": midnight,
            },
            {
                "ts": close,
                "open_x1000": 10000,
                "high_x1000": 11000,
                "low_x1000": 9000,
                "close_x1000": 10500,
                "volume": 10000,
                "amount_x100": None,
                "is_complete": True,
                "revision": 0,
                "source": 2,
                "updated_at": close,
            },
            ],
    )

    assert len(rows) == 1
    assert rows[0]["ts"] == close
    assert rows[0]["source"] == 2


def test_reader_keeps_task_1_canonical_source_2_daily_fixture_over_tencent_midnight_duplicates() -> None:
    canonical_rows = [
        ("2026-03-23", 10.495441, 10.534706, 10.430000, 10.469265),
        ("2026-03-24", 10.626324, 10.665588, 10.560882, 10.600147),
        ("2026-04-30", 11.474000, 11.600000, 11.302549, 11.390000),
        ("2026-05-29", 10.582696, 10.621961, 10.517255, 10.556520),
    ]
    rows = []
    for index, (date, open_, high, low, close) in enumerate(canonical_rows):
        midnight = datetime.fromisoformat(date).replace(tzinfo=SHANGHAI_TZ)
        canonical = midnight.replace(hour=15)
        rows.extend(
            [
                _daily_reader_row(midnight, open_, high, low, close, volume=10_001 + index, source=6),
                _daily_reader_row(canonical, open_, high, low, close, volume=10_000 + index, source=2),
            ]
        )

    result = canonicalize_bar_rows("1d", rows)

    assert len(result) == len(canonical_rows)
    assert all(row["source"] == 2 for row in result)
    assert all(row["ts"].astimezone(SHANGHAI_TZ).strftime("%H:%M") == "15:00" for row in result)
    assert [(row["open_x1000"], row["high_x1000"], row["low_x1000"], row["close_x1000"]) for row in result] == [
        tuple(round(value * 1000) for value in values[1:]) for values in reversed(canonical_rows)
    ]
    assert [row["volume"] for row in result] == [10_003, 10_002, 10_001, 10_000]


def test_api_reader_query_ranks_logical_rows_before_limit() -> None:
    async def scenario():
        conn = FakeConn()
        await get_bars_db(FakePool(conn), "000001.SZ", "1d", None, None, 3)
        assert "row_number() over" in conn.queries[0].lower()
        assert "), scoped as" in conn.queries[0].lower()
        assert "kline_source_coverage" in conn.queries[0].lower()
        assert "ts >= case when $2 in (1440, 10080, 43200)" in conn.queries[0].lower()
        assert "date_trunc('day', $4 at time zone 'asia/shanghai')" in conn.queries[0].lower()
        assert "then 10" in conn.queries[0].lower()
        assert "limit $6" in conn.queries[0].lower()
        assert conn.args[0][-2:] == (35, False)

    asyncio.run(scenario())


def test_reader_oversamples_physical_rows_before_reader_deduplication() -> None:
    async def scenario():
        conn = FakeConn()
        await _fetch_bars_by_symbol_id(
            FakePool(conn),
            symbol_id=1,
            timeframe_code=TIMEFRAME_TO_DB["5f"],
            start=None,
            end=None,
            limit=300,
            source_codes=[2, 3, 4, 5, 6, 7, 8, 9],
            end_exclusive=False,
        )
        # The reader can collapse duplicate physical rows after the SQL limit.
        # Fetch a bounded guard so a 300-bar chart request does not trigger
        # unnecessary widening retries because of one or two duplicates.
        assert conn.args[0][-2:] == (332, False)

    asyncio.run(scenario())


def test_api_reader_can_use_an_exclusive_end_without_changing_legacy_default() -> None:
    async def scenario():
        conn = FakeConn()
        end = datetime.fromtimestamp(1000, tz=UTC)
        await get_bars_db(FakePool(conn), "000001.SZ", "1d", None, end, 3, end_exclusive=True)
        assert "ts < $5" in conn.queries[0]
        assert conn.args[0][-2:] == (35, True)

    asyncio.run(scenario())


def test_reader_ranks_more_than_two_physical_rows_by_updated_at_after_source_revision_ties() -> None:
    timestamp = datetime(2026, 7, 10, 15, tzinfo=SHANGHAI_TZ)
    rows = [
        _daily_reader_row(timestamp, 10, 11, 9, 10.5, volume=100, source=2),
        _daily_reader_row(timestamp.replace(hour=0), 10, 11, 9, 10.5, volume=101, source=6),
        _daily_reader_row(timestamp, 10, 11, 9, 10.5, volume=102, source=2),
    ]
    rows[0]["updated_at"] = timestamp.replace(hour=15, minute=1)
    rows[2]["updated_at"] = timestamp.replace(hour=15, minute=2)

    result = canonicalize_bar_rows("1d", rows)

    assert len(result) == 1
    assert result[0]["volume"] == 102


def test_reader_groups_weekly_and_monthly_rows_by_calendar_period() -> None:
    for timeframe, dates in (("1w", ("2026-07-06", "2026-07-10")), ("1m", ("2026-07-01", "2026-07-31"))):
        rows = [
            _daily_reader_row(datetime.fromisoformat(date).replace(hour=15, tzinfo=SHANGHAI_TZ), 10, 11, 9, 10.5, volume=100 + index, source=2)
            for index, date in enumerate(dates)
        ]
        result = canonicalize_bar_rows(timeframe, rows)

        assert len(result) == 1
        assert result[0]["ts"].date().isoformat() == dates[-1]


def test_higher_timeframe_reader_projects_winner_to_final_period_close_and_excludes_current_period() -> None:
    monday = datetime(2026, 7, 6, 15, tzinfo=SHANGHAI_TZ)
    friday = datetime(2026, 7, 10, 15, tzinfo=SHANGHAI_TZ)
    result = canonicalize_bar_rows(
        "1w",
        [
            _daily_reader_row(monday, 10, 12, 9, 11, volume=100, source=4),
            _daily_reader_row(friday, 10, 11, 9, 10.5, volume=99, source=2),
        ],
        parquet_coverage_end=friday,
    )

    assert len(result) == 1
    assert result[0]["high_x1000"] == 12000
    assert result[0]["ts"] == friday


def test_api_reader_query_projects_period_end_from_daily_and_excludes_current_period() -> None:
    async def scenario():
        conn = FakeConn()
        await get_bars_db(FakePool(conn), "000001.SZ", "1w", None, None, 3)
        sql = next(query.lower() for query in conn.queries if "daily_period_ends" in query.lower())
        assert "daily_period_ends" in sql
        assert "final_ts" in sql
        assert "date_trunc('week', now()" in sql

    asyncio.run(scenario())


def _daily_reader_row(
    ts: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
    *,
    volume: int,
    source: int,
) -> dict:
    return {
        "ts": ts,
        "open_x1000": round(open_ * 1000),
        "high_x1000": round(high * 1000),
        "low_x1000": round(low * 1000),
        "close_x1000": round(close * 1000),
        "volume": volume,
        "amount_x100": None,
        "is_complete": True,
        "revision": 0,
        "source": source,
        "updated_at": ts,
    }


def test_get_bars_db_derives_1d_from_canonical_5f() -> None:
    class DerivedConn(FakeConn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            self.args.append(args)
            if args[1] != TIMEFRAME_TO_DB["5f"]:
                return []
            return [
                _db_bar("2026-04-27 09:35", open=10.0, high=10.2, low=9.9, close=10.1),
                _db_bar("2026-04-27 11:30", open=10.1, high=10.5, low=10.0, close=10.4),
                _db_bar("2026-04-27 13:05", open=10.4, high=10.6, low=10.3, close=10.5),
                _db_bar("2026-04-27 15:00", open=10.5, high=10.8, low=10.4, close=10.7),
            ]

    async def scenario():
        conn = DerivedConn()
        rows = await get_bars_db(
            FakePool(conn),
            symbol="000001.SZ",
            timeframe="1d",
            start=None,
            end=None,
            limit=1,
        )
        assert len(rows) == 1
        assert _format_time(rows[0]["time"]) == "2026-04-27 15:00"
        assert rows[0]["open"] == 10.0
        assert rows[0]["close"] == 10.7
        assert any(
                args[0:3] == (1, TIMEFRAME_TO_DB["5f"], [2, 3, 4, 5, 6, 7, 8, 9])
            for args in conn.args
        )

    asyncio.run(scenario())


def test_get_bars_db_derives_supported_higher_timeframes_from_canonical_5f() -> None:
    class DerivedConn(FakeConn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            self.args.append(args)
            if args[1] != TIMEFRAME_TO_DB["5f"]:
                return []
            return _base_5f_rows()

    async def scenario(timeframe: str):
        conn = DerivedConn()
        rows = await get_bars_db(
            FakePool(conn),
            symbol="000001.SZ",
            timeframe=timeframe,
            start=None,
            end=None,
            limit=1,
        )
        assert rows
        assert any(
                args[0:3] == (1, TIMEFRAME_TO_DB["5f"], [2, 3, 4, 5, 6, 7, 8, 9])
            for args in conn.args
        )

    for timeframe in ("15f", "30f", "1h", "1d"):
        asyncio.run(scenario(timeframe))


def test_get_bars_db_keeps_0930_in_first_derived_slot() -> None:
    class DerivedConn(FakeConn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            self.args.append(args)
            if args[1] != TIMEFRAME_TO_DB["5f"]:
                return []
            return [
                _db_bar("2026-04-27 09:30", open=9.9, high=10.0, low=9.8, close=9.95),
                _db_bar("2026-04-27 09:35", open=10.0, high=10.2, low=9.9, close=10.1),
                _db_bar("2026-04-27 09:40", open=10.1, high=10.3, low=10.0, close=10.2),
                _db_bar("2026-04-27 09:45", open=10.2, high=10.4, low=10.1, close=10.3),
            ]

    async def scenario():
        conn = DerivedConn()
        rows = await get_bars_db(
            FakePool(conn),
            symbol="000001.SZ",
            timeframe="15f",
            start=None,
            end=None,
            limit=2,
        )
        assert len(rows) == 1
        assert _format_time(rows[0]["time"]) == "2026-04-27 09:45"
        assert rows[0]["open"] == 9.9
        assert rows[0]["high"] == 10.4
        assert rows[0]["low"] == 9.8
        assert rows[0]["close"] == 10.3

    asyncio.run(scenario())


def _db_bar(
    value: str,
    *,
    open: float,
    high: float,
    low: float,
    close: float,
) -> dict:
    ts = datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=SHANGHAI_TZ)
    return {
        "ts": ts.astimezone(UTC),
        "open_x1000": int(open * 1000),
        "high_x1000": int(high * 1000),
        "low_x1000": int(low * 1000),
        "close_x1000": int(close * 1000),
        "volume": 100,
        "amount_x100": None,
        "is_complete": True,
        "revision": 0,
    }


def _base_5f_rows() -> list[dict]:
    return [
        _db_bar("2026-04-27 09:35", open=10.0, high=10.2, low=9.9, close=10.1),
        _db_bar("2026-04-27 09:40", open=10.1, high=10.3, low=10.0, close=10.2),
        _db_bar("2026-04-27 09:45", open=10.2, high=10.4, low=10.1, close=10.3),
        _db_bar("2026-04-27 10:00", open=10.3, high=10.5, low=10.2, close=10.4),
        _db_bar("2026-04-27 15:00", open=10.4, high=10.8, low=10.3, close=10.7),
    ]


def _format_time(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M")
