from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from collector.storage.chan_postgres import PostgresChanWriter


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeConn:
    def __init__(self) -> None:
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self._fetchval_results = iter([1, 99])

    async def fetchval(self, query: str, *args):
        self.fetchval_calls.append((query, args))
        return next(self._fetchval_results)

    async def execute(self, query: str, *args):
        self.execute_calls.append((query, args))
        return "OK"

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        return []

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        return None

    async def executemany(self, query: str, rows: list[tuple[object, ...]]):
        self.executemany_calls.append((query, rows))
        return None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()


class FakeAcquire:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeConn:
        return self.conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.conn)


def test_replace_analysis_persists_snapshot_version_and_canonical_endpoint_fields() -> None:
    async def scenario() -> None:
        conn = FakeConn()
        writer = PostgresChanWriter("postgresql://unused")
        writer._pool = FakePool(conn)

        response = {
            "snapshot_version": "snapshot-write-001",
            "strokes": [
                {
                    "id": "stroke-1",
                    "mode": "confirmed",
                    "start": {"time": 1_717_300_600, "price": 10.12},
                    "end": {"time": 1_717_304_200, "price": 10.56},
                    "begin_base_ts": 1_717_300_600,
                    "end_base_ts": 1_717_304_200,
                    "begin_base_seq": 1001,
                    "end_base_seq": 1012,
                    "direction": "up",
                    "confirmed": True,
                }
            ],
            "segments": [],
            "centers": [
                {
                    "id": "center-1",
                    "mode": "predictive",
                    "start_time": 1_717_300_600,
                    "end_time": 1_717_304_200,
                    "begin_base_ts": 1_717_300_600,
                    "end_base_ts": 1_717_304_200,
                    "begin_base_seq": 2001,
                    "end_base_seq": 2009,
                    "low": 10.0,
                    "high": 10.8,
                    "confirmed": False,
                }
            ],
            "signals": [
                {
                    "id": "signal-1",
                    "mode": "confirmed",
                    "time": 1_717_304_200,
                    "price": 10.66,
                    "base_ts": 1_717_304_200,
                    "base_seq": 3007,
                    "signal_type": "2buy",
                    "confirmed": True,
                }
            ],
        }

        counts = await writer.replace_analysis(
            symbol="000001.SZ",
            level="30f",
            modes=["confirmed", "predictive"],
            bar_from=datetime.fromtimestamp(1_717_300_600, tz=UTC),
            bar_until=datetime.fromtimestamp(1_717_304_200, tz=UTC),
            bar_count=300,
            response=response,
        )

        assert counts == {"strokes": 1, "segments": 0, "centers": 1, "signals": 1}

        run_insert_query, run_insert_args = conn.fetchval_calls[1]
        assert "insert into chan_runs" in run_insert_query
        assert "snapshot_version" in run_insert_query
        assert run_insert_args[-1] == "snapshot-write-001"

        stroke_insert = next(
            (query, rows)
            for query, rows in conn.executemany_calls
            if "insert into chan_strokes" in query
        )
        stroke_row = stroke_insert[1][0]
        assert int(stroke_row[11].timestamp()) == 1_717_300_600
        assert int(stroke_row[12].timestamp()) == 1_717_304_200
        assert stroke_row[13] == 1001
        assert stroke_row[14] == 1012

        center_insert = next(
            (query, rows)
            for query, rows in conn.executemany_calls
            if "insert into chan_centers" in query
        )
        center_row = center_insert[1][0]
        assert int(center_row[10].timestamp()) == 1_717_300_600
        assert int(center_row[11].timestamp()) == 1_717_304_200
        assert center_row[12] == 2001
        assert center_row[13] == 2009

        signal_insert = next(
            (query, rows)
            for query, rows in conn.executemany_calls
            if "insert into chan_signals" in query
        )
        signal_row = signal_insert[1][0]
        assert int(signal_row[8].timestamp()) == 1_717_304_200
        assert signal_row[9] == 3007

        published_upserts = [
            rows
            for query, rows in conn.executemany_calls
            if "insert into scheme2_chan_published_heads" in query
        ]
        assert len(published_upserts) == 2
        staged_rows = published_upserts[0]
        published_rows = published_upserts[1]
        assert len(staged_rows) == 2
        assert {row[2] for row in staged_rows} == {"confirmed", "predictive"}
        assert all(row[8] == "staged" for row in staged_rows)
        assert len(published_rows) == 2
        assert all(row[8] == "published" for row in published_rows)

        assert any(
            "update chan_runs" in query and args[0] == 99
            for query, args in conn.execute_calls
        )

    asyncio.run(scenario())
