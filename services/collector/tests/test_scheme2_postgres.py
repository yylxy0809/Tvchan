from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from collector.storage.postgres import code_to_source, source_to_code
from collector.storage.scheme2_postgres import (
    PARQUET_5F_SOURCE_CODE,
    PostgresScheme2KlineWriter,
    PostgresScheme2MemberCheckpointStore,
    Scheme2SourceMember,
    _bar_rows,
)
from trading_protocol import Bar, SymbolInfo


def test_parquet_5f_source_mapping_is_source_4() -> None:
    assert source_to_code("parquet_5f") == 4
    assert code_to_source(4) == "parquet_5f"


def test_bar_rows_keep_bar_end_and_source_code() -> None:
    bar_end = datetime(2024, 1, 2, 9, 35, tzinfo=UTC)
    rows = _bar_rows(
        [
            Bar(
                symbol="000001.SZ",
                timeframe="5f",
                ts=bar_end,
                open=10.111,
                high=10.555,
                low=10.0,
                close=10.333,
                volume=123,
                amount=456.78,
                source="parquet_5f",
            )
        ]
    )

    assert rows == [
        (
            "000001",
            "SZ",
            5,
            bar_end,
            10111,
            10555,
            10000,
            10333,
            123,
            45678,
            True,
            0,
            PARQUET_5F_SOURCE_CODE,
        )
    ]


def test_scheme2_writer_uses_copy_staging_and_watermarks() -> None:
    conn = FakeConnection()
    writer = PostgresScheme2KlineWriter("postgresql://example")
    writer._pool = FakePool(conn)
    bar_end = datetime(2024, 1, 2, 9, 35, tzinfo=UTC)

    result = asyncio.run(
        writer.upsert_5f_bars(
            symbols=[
                SymbolInfo(
                    symbol="000001.SZ",
                    code="000001",
                    exchange="SZ",
                    name="000001",
                )
            ],
            bars=[
                Bar(
                    symbol="000001.SZ",
                    timeframe="5f",
                    ts=bar_end,
                    open=10.0,
                    high=11.0,
                    low=9.5,
                    close=10.5,
                    volume=1000,
                    amount=2000.25,
                    source="parquet_5f",
                )
            ],
        )
    )

    assert result == 1
    assert [copy["table"] for copy in conn.copies] == [
        "_scheme2_symbol_stage",
        "_scheme2_kline_stage",
    ]
    assert conn.copies[1]["records"][0][3] == bar_end
    assert conn.copies[1]["records"][0][-1] == 4
    assert any("scheme2_ingest_watermarks" in sql for sql, _args in conn.executes)
    assert conn.executemany_calls == []


def test_checkpoint_store_resets_running_members() -> None:
    conn = FakeConnection(execute_result="UPDATE 2")
    store = PostgresScheme2MemberCheckpointStore("postgresql://example")
    store._pool = FakePool(conn)

    result = asyncio.run(store.reset_running())

    assert result == 2
    sql, args = conn.executes[0]
    assert "scheme2_source_member_checkpoints" in sql
    assert "status = 'running'" in sql
    assert args == ("parquet_5f",)


def test_checkpoint_store_ensures_members_with_reset_flag() -> None:
    conn = FakeConnection()
    store = PostgresScheme2MemberCheckpointStore("postgresql://example")
    store._pool = FakePool(conn)

    member = Scheme2SourceMember(
        root_path="D:/5f数据/5m_price",
        source_profile="parquet_5f",
        zip_path="D:/5f数据/5m_price/2024.zip",
        member_path="20240102.parquet",
        member_crc32=123,
        member_size_bytes=456,
    )

    result = asyncio.run(store.ensure_member_checkpoints([member], reset=True))

    assert result == 1
    sql, rows = conn.executemany_calls[0]
    assert "scheme2_source_member_checkpoints" in sql
    assert rows == [
        (
            "D:/5f数据/5m_price",
            "parquet_5f",
            "D:/5f数据/5m_price/2024.zip",
            "20240102.parquet",
            123,
            456,
            5,
            True,
        )
    ]


class FakePool:
    def __init__(self, conn) -> None:
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)


class FakeAcquire:
    def __init__(self, conn) -> None:
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeConnection:
    def __init__(self, *, execute_result: str = "UPDATE 1") -> None:
        self.execute_result = execute_result
        self.executes = []
        self.copies = []
        self.executemany_calls = []

    def transaction(self):
        return FakeTransaction()

    async def execute(self, sql, *args):
        self.executes.append((sql, args))
        return self.execute_result

    async def executemany(self, sql, rows):
        self.executemany_calls.append((sql, rows))

    async def copy_records_to_table(self, table, *, records, columns):
        self.copies.append(
            {
                "table": table,
                "records": list(records),
                "columns": list(columns),
            }
        )
