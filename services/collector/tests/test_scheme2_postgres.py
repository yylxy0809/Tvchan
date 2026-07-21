from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from collector.storage import postgres as postgres_storage
from collector.storage import scheme2_postgres as scheme2_storage
from collector.storage.postgres import (
    PostgresKlineWriter,
    _rows_to_canonical_bars,
    bar_to_db_values,
    code_to_source,
    source_to_code,
)
from collector.storage.scheme2_postgres import (
    PARQUET_5F_SOURCE_CODE,
    PostgresScheme2KlineWriter,
    PostgresScheme2MemberCheckpointStore,
    Scheme2SourceMember,
    _bar_rows,
)
from collector.native_parquet_import import NativeParquetWriter
import collector.native_parquet_import as native_import
from trading_protocol import Bar, SymbolInfo


def test_parquet_5f_source_mapping_is_source_4() -> None:
    assert source_to_code("parquet_5f") == 4
    assert code_to_source(4) == "parquet_5f"


def test_bar_rows_keep_bar_end_and_source_code() -> None:
    bar_end = datetime(2024, 1, 2, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai"))
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


def test_scheme2_bar_rows_reject_invalid_5f_endpoint() -> None:
    with pytest.raises(ValueError, match="session bar-end"):
        _bar_rows([
            Bar("000001.SZ", "5f", datetime(2024, 1, 2, 12, 0, tzinfo=UTC), 10, 11, 9, 10.5, 100)
        ])


def test_scheme2_bar_rows_preserve_opening_snapshot_0930() -> None:
    opening_snapshot = datetime(2024, 1, 2, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))

    rows = _bar_rows([
        Bar("000001.SZ", "5f", opening_snapshot, 10, 11, 9, 10.5, 100, source="parquet_5f")
    ])

    assert rows[0][3] == opening_snapshot


def test_daily_midnight_write_is_normalized_to_close() -> None:
    values = bar_to_db_values(
        Bar(
            symbol="000001.SZ",
            timeframe="1d",
            ts=datetime(2026, 7, 10, tzinfo=ZoneInfo("Asia/Shanghai")),
            open=10,
            high=11,
            low=9,
            close=10.5,
            volume=100,
            source="tencent",
        )
    )

    assert values[2].astimezone(ZoneInfo("Asia/Shanghai")).strftime("%H:%M") == "15:00"


def test_upsert_prevents_lower_priority_and_derived_overwrites() -> None:
    async def scenario() -> str:
        conn = FakeConnection()
        writer = PostgresKlineWriter("postgresql://example")
        await writer._upsert_bars_rows(
            conn,
            [
                bar_to_db_values(
                    Bar(
                        symbol="000001.SZ",
                        timeframe="1d",
                        ts=datetime(2026, 7, 10, 15, tzinfo=ZoneInfo("Asia/Shanghai")),
                        open=10,
                        high=11,
                        low=9,
                        close=10.5,
                        volume=100,
                        source="derived_5f",
                    )
                )
            ],
        )
        return conn.executemany_calls[0][0]

    sql = asyncio.run(scenario())
    normalized_sql = " ".join(sql.lower().split())

    assert "where not (klines.is_complete and not excluded.is_complete)" in normalized_sql
    assert "excluded.source" in normalized_sql
    assert "when 2 then 6" in normalized_sql
    assert "when 8 then 2" in normalized_sql


def test_writer_registers_persisted_parquet_coverage_before_upserting_bars() -> None:
    async def scenario():
        conn = FakeConnection()
        writer = PostgresKlineWriter("postgresql://example")
        await writer._upsert_bars_rows(
            conn,
            [bar_to_db_values(Bar("000001.SZ", "5f", datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai")), 10, 11, 9, 10.5, 100, source="parquet_5f"))],
        )
        return conn.executemany_calls

    calls = asyncio.run(scenario())

    assert "kline_source_coverage" in calls[0][0]
    assert "insert into klines" in calls[1][0]


def test_writer_uses_persisted_coverage_not_physical_parquet_rows_for_priority() -> None:
    async def scenario() -> str:
        conn = FakeConnection()
        writer = PostgresKlineWriter("postgresql://example")
        await writer._upsert_bars_rows(
            conn,
            [bar_to_db_values(Bar("000001.SZ", "5f", datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai")), 10, 11, 9, 10.5, 100, source="pytdx"))],
        )
        return next(sql for sql, _rows in conn.executemany_calls if "insert into klines" in sql)

    sql = asyncio.run(scenario()).lower()

    assert "kline_source_coverage" in sql
    assert "from klines covered" not in sql


def test_writer_updates_changed_open_bar_and_never_downgrades_closed_bar() -> None:
    async def scenario() -> str:
        conn = FakeConnection()
        writer = PostgresKlineWriter("postgresql://example")
        await writer._upsert_bars_rows(
            conn,
            [
                bar_to_db_values(
                    Bar(
                        "000001.SZ",
                        "5f",
                        datetime(2026, 7, 21, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai")),
                        10,
                        11,
                        9,
                        10.5,
                        100,
                        complete=False,
                        revision=1,
                        source="pytdx",
                    )
                )
            ],
        )
        return next(sql for sql, _rows in conn.executemany_calls if "insert into klines" in sql)

    normalized = " ".join(asyncio.run(scenario()).lower().split())

    assert "klines.is_complete and not excluded.is_complete" in normalized
    assert "is distinct from" in normalized
    assert "klines.revision + 1" in normalized
    assert "excluded.is_complete and klines.is_complete" in normalized


def test_watermark_changes_only_for_a_new_or_revised_canonical_bar() -> None:
    async def scenario() -> str:
        conn = FakeConnection(symbol_rows=[{"symbol_id": 7, "symbol": "000001.SZ"}])
        writer = PostgresKlineWriter("postgresql://example")
        await writer._upsert_bars_rows(
            conn,
            [
                bar_to_db_values(
                    Bar(
                        "000001.SZ",
                        "5f",
                        datetime(2026, 7, 21, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai")),
                        10,
                        11,
                        9,
                        10.5,
                        100,
                        complete=False,
                        revision=1,
                        source="pytdx",
                    )
                )
            ],
        )
        return next(sql for sql, _args in conn.executes if "scheme2_ingest_watermarks" in sql)

    normalized = " ".join(asyncio.run(scenario()).lower().split())

    assert "from klines kline" in normalized
    assert "max(kline.updated_at)" in normalized
    assert "change_version" in normalized
    assert "first_bar_end" in normalized
    assert "excluded.updated_at > scheme2_ingest_watermarks.updated_at" in normalized


def test_writer_records_distinct_scope_bounds_in_the_kline_transaction(monkeypatch) -> None:
    first = datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai"))
    last = datetime(2026, 7, 10, 9, 40, tzinfo=ZoneInfo("Asia/Shanghai"))
    conn = FakeConnection(symbol_rows=[{"symbol_id": 7, "symbol": "000001.SZ"}])
    writer = PostgresKlineWriter("postgresql://example")
    writer._pool = FakePool(conn)
    recorded: list[tuple[int, int, datetime, datetime]] = []

    async def record_present_scopes(_conn, *, scopes):
        conn.events.append("catalog-present")
        recorded.extend(scopes)
        return len(scopes)

    monkeypatch.setattr(postgres_storage, "record_present_scopes", record_present_scopes)

    result = asyncio.run(writer.upsert_bars([
        Bar("000001.SZ", "5f", last, 10, 11, 9, 10.5, 100),
        Bar("000001.SZ", "5f", first, 10, 11, 9, 10.5, 100),
    ]))

    assert result == 2
    assert recorded == [(7, 5, first, last)]
    assert conn.events.index("klines-upsert") < conn.events.index("catalog-present")
    assert conn.events.index("klines-upsert") < conn.events.index("watermark-upsert")
    assert conn.events.index("watermark-upsert") < conn.events.index("commit")
    assert conn.events[0] == "begin"
    assert conn.events[-1] == "commit"


def test_writer_reads_back_the_exact_canonical_winner_for_publication() -> None:
    timestamp = datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai"))

    class CanonicalConnection:
        def __init__(self) -> None:
            self.args = None

        async def fetchrow(self, sql, *args):
            assert "from klines k" in sql.lower()
            self.args = args
            return {
                "ts": timestamp,
                "open_x1000": 10000,
                "high_x1000": 11000,
                "low_x1000": 9000,
                "close_x1000": 10800,
                "volume": 123,
                "amount_x100": 45600,
                "is_complete": True,
                "revision": 4,
                "source": 4,
            }

    conn = CanonicalConnection()
    writer = PostgresKlineWriter("postgresql://example")
    writer._pool = FakePool(conn)

    bar = asyncio.run(writer.get_canonical_bar("000001.SZ", "5f", timestamp))

    assert conn.args == ("000001.SZ", 5, timestamp)
    assert bar is not None
    assert (bar.close, bar.revision, bar.source) == (10.8, 4, "parquet_5f")


def test_market_claim_rejection_writes_neither_klines_nor_catalog(monkeypatch) -> None:
    conn = FakeConnection(fetchval_result=False)
    writer = PostgresKlineWriter("postgresql://example")
    writer._pool = FakePool(conn)

    async def unexpected_hook(*_args, **_kwargs):
        raise AssertionError("catalog hook must not run for an invalid claim")

    monkeypatch.setattr(postgres_storage, "record_present_scopes", unexpected_hook)

    result = asyncio.run(writer.upsert_bars_for_market_claim(
        task_id=3,
        claim_token="stale",
        bars=[Bar("000001.SZ", "5f", datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai")), 10, 11, 9, 10.5, 100)],
    ))

    assert result == (0, False)
    assert "klines-upsert" not in conn.events
    assert conn.events == ["begin", "claim-check", "commit"]


def test_delete_bars_refreshes_every_resolved_whole_scope_in_one_transaction(monkeypatch) -> None:
    conn = FakeConnection(
        execute_result="DELETE 3",
        symbol_rows=[
            {"symbol_id": 7, "symbol": "000001.SZ"},
            {"symbol_id": 8, "symbol": "000002.SZ"},
        ],
    )
    writer = PostgresKlineWriter("postgresql://example")
    writer._pool = FakePool(conn)
    refreshed: list[tuple[int, int]] = []

    async def refresh_scopes_exact(_conn, *, scopes):
        conn.events.append("catalog-refresh")
        refreshed.extend(scopes)
        return {"catalog_rows": len(scopes), "updated": len(scopes), "cas_skipped": 0}

    monkeypatch.setattr(postgres_storage, "refresh_scopes_exact", refresh_scopes_exact)

    result = asyncio.run(writer.delete_bars(["000001.SZ", "000002.SZ"], ["5f", "1d"]))

    assert result == 3
    assert refreshed == [(7, 5), (7, 1440), (8, 5), (8, 1440)]
    assert conn.events == [
        "begin",
        "resolve-symbols",
        "klines-delete",
        "catalog-refresh",
        "commit",
    ]


def test_delete_bars_rolls_back_when_catalog_refresh_fails(monkeypatch) -> None:
    conn = FakeConnection(
        execute_result="DELETE 1",
        symbol_rows=[{"symbol_id": 7, "symbol": "000001.SZ"}],
    )
    writer = PostgresKlineWriter("postgresql://example")
    writer._pool = FakePool(conn)

    async def refresh_scopes_exact(_conn, *, scopes):
        conn.events.append("catalog-refresh")
        recorded.extend(scopes)
        raise RuntimeError("catalog update failed")

    recorded: list[tuple[int, int]] = []
    monkeypatch.setattr(postgres_storage, "refresh_scopes_exact", refresh_scopes_exact)

    with pytest.raises(RuntimeError, match="catalog update failed"):
        asyncio.run(writer.delete_bars(["000001.SZ"], ["5f"]))

    assert conn.events == [
        "begin",
        "resolve-symbols",
        "klines-delete",
        "catalog-refresh",
        "rollback",
    ]


def test_collector_reader_dedupes_legacy_daily_logical_period() -> None:
    midnight = datetime(2026, 7, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
    close = midnight.replace(hour=15)
    bars = _rows_to_canonical_bars(
        "000001.SZ",
        "1d",
        [
            _reader_row(midnight, source=6, volume=10001),
            _reader_row(close, source=2, volume=10000),
        ],
    )

    assert len(bars) == 1
    assert bars[0].ts == close
    assert bars[0].source == "pytdx"
    assert bars[0].volume == 10000


def test_collector_reader_uses_updated_at_after_source_revision_ties() -> None:
    timestamp = datetime(2026, 7, 10, 15, tzinfo=ZoneInfo("Asia/Shanghai"))
    older = _reader_row(timestamp, source=2, volume=100)
    newer = _reader_row(timestamp, source=2, volume=101)
    older["updated_at"] = timestamp.replace(minute=1)
    newer["updated_at"] = timestamp.replace(minute=2)

    bars = _rows_to_canonical_bars("000001.SZ", "1d", [older, newer, _reader_row(timestamp.replace(hour=0), source=6, volume=102)])

    assert len(bars) == 1
    assert bars[0].volume == 101


def test_collector_chunk_query_ranks_logical_rows_before_limit() -> None:
    conn = FakeConnection()
    writer = PostgresKlineWriter("postgresql://example")

    import asyncio

    asyncio.run(
        writer._fetch_bar_rows_for_sources(
            conn,
            symbol_id=1,
            timeframe_code=1440,
            after_ts=None,
            limit=3,
            sources=[2, 6],
        )
    )

    sql, args = conn.fetch_calls[0]
    assert "row_number() over" in sql.lower()
    assert "kline_source_coverage" in sql.lower()
    assert "then 10" in sql.lower()
    assert args[-1] == 3


def _reader_row(ts: datetime, *, source: int, volume: int) -> dict:
    return {
        "ts": ts,
        "open_x1000": 10000,
        "high_x1000": 11000,
        "low_x1000": 9000,
        "close_x1000": 10500,
        "volume": volume,
        "amount_x100": None,
        "is_complete": True,
        "revision": 0,
        "source": source,
    }


def test_scheme2_writer_uses_copy_staging_and_watermarks() -> None:
    conn = FakeConnection()
    writer = PostgresScheme2KlineWriter("postgresql://example")
    writer._pool = FakePool(conn)
    bar_end = datetime(2024, 1, 2, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai"))

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


def test_scheme2_registers_coverage_before_staged_kline_upsert_in_one_transaction() -> None:
    conn = FakeConnection()
    writer = PostgresScheme2KlineWriter("postgresql://example")
    writer._pool = FakePool(conn)

    asyncio.run(
        writer.upsert_5f_bars(
            symbols=[],
            bars=[Bar("000001.SZ", "5f", datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai")), 10, 11, 9, 10.5, 100)],
        )
    )

    statements = [sql.lower() for sql, _args in conn.executes]
    coverage_index = next(index for index, sql in enumerate(statements) if "kline_source_coverage" in sql)
    kline_index = next(index for index, sql in enumerate(statements) if "insert into klines" in sql)
    assert coverage_index < kline_index


def test_scheme2_records_staged_scope_bounds_between_klines_and_watermark(monkeypatch) -> None:
    first = datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai"))
    last = datetime(2026, 7, 10, 9, 40, tzinfo=ZoneInfo("Asia/Shanghai"))
    conn = FakeConnection(scope_rows=[{
        "symbol_id": 7,
        "timeframe": 5,
        "min_ts": first,
        "max_ts": last,
    }])
    writer = PostgresScheme2KlineWriter("postgresql://example")
    writer._pool = FakePool(conn)
    recorded: list[tuple[int, int, datetime, datetime]] = []

    async def record_present_scopes(_conn, *, scopes):
        conn.events.append("catalog-present")
        recorded.extend(scopes)
        return len(scopes)

    monkeypatch.setattr(scheme2_storage, "record_present_scopes", record_present_scopes)

    asyncio.run(writer.upsert_5f_bars(
        symbols=[],
        bars=[
            Bar("000001.SZ", "5f", last, 10, 11, 9, 10.5, 100),
            Bar("000001.SZ", "5f", first, 10, 11, 9, 10.5, 100),
        ],
    ))

    assert recorded == [(7, 5, first, last)]
    assert conn.events.index("klines-upsert") < conn.events.index("catalog-present")
    assert conn.events.index("catalog-present") < conn.events.index("watermark-upsert")
    assert conn.events[0] == "begin"
    assert conn.events[-1] == "commit"


def test_native_parquet_registers_coverage_before_staged_kline_upsert_in_one_transaction() -> None:
    conn = FakeConnection()
    writer = NativeParquetWriter("postgresql://example")
    writer.pool = FakePool(conn)
    bar = ("000001", "SZ", 5, datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai")), 10000, 11000, 9000, 10500, 100, None, True, 0, 9)

    asyncio.run(writer.upsert(symbols=[], bars=[bar]))

    statements = [sql.lower() for sql, _args in conn.executes]
    coverage_index = next(index for index, sql in enumerate(statements) if "kline_source_coverage" in sql)
    kline_index = next(index for index, sql in enumerate(statements) if "insert into klines" in sql)
    assert coverage_index < kline_index


def test_native_parquet_records_staged_scope_bounds_between_klines_and_watermark(monkeypatch) -> None:
    first = datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai"))
    last = datetime(2026, 7, 10, 9, 40, tzinfo=ZoneInfo("Asia/Shanghai"))
    conn = FakeConnection(scope_rows=[{
        "symbol_id": 7,
        "timeframe": 5,
        "min_ts": first,
        "max_ts": last,
    }])
    writer = NativeParquetWriter("postgresql://example")
    writer.pool = FakePool(conn)
    recorded: list[tuple[int, int, datetime, datetime]] = []

    async def record_present_scopes(_conn, *, scopes):
        conn.events.append("catalog-present")
        recorded.extend(scopes)
        return len(scopes)

    monkeypatch.setattr(native_import, "record_present_scopes", record_present_scopes)

    asyncio.run(writer.upsert(symbols=[], bars=[
        ("000001", "SZ", 5, last, 10000, 11000, 9000, 10500, 100, None, True, 0, 9),
        ("000001", "SZ", 5, first, 10000, 11000, 9000, 10500, 100, None, True, 0, 9),
    ]))

    assert recorded == [(7, 5, first, last)]
    assert conn.events.index("klines-upsert") < conn.events.index("catalog-present")
    assert conn.events.index("catalog-present") < conn.events.index("watermark-upsert")
    assert conn.events[0] == "begin"
    assert conn.events[-1] == "commit"


def test_checkpoint_store_resets_running_members() -> None:
    conn = FakeConnection(execute_result="UPDATE 2")
    store = PostgresScheme2MemberCheckpointStore("postgresql://example")
    store._pool = FakePool(conn)

    result = asyncio.run(store.reset_running())

    assert result == 2
    sql, args = conn.executes[0]
    assert "scheme2_source_member_checkpoints" in sql
    assert "status = 'running'" in sql
    assert "lease_until <= clock_timestamp()" in sql
    assert "attempts = attempts + 1" in sql
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
    assert "coalesce(content_sha256, '')" in sql
    assert rows == [
        (
            "D:/5f数据/5m_price",
            "parquet_5f",
            "D:/5f数据/5m_price/2024.zip",
            "20240102.parquet",
            123,
            456,
            None,
            5,
            True,
        )
    ]


def test_checkpoint_claim_and_terminal_operations_require_exact_owner_and_progress() -> None:
    conn = FakeConnection()
    store = PostgresScheme2MemberCheckpointStore("postgresql://example")
    store._pool = FakePool(conn)

    asyncio.run(store.claim_member_checkpoints(
        limit=2,
        worker_id="worker-a",
        lease_seconds=90,
        max_attempts=4,
    ))
    exhausted_sql, exhausted_args = conn.executes[0]
    assert "set status = 'dead_letter'" in exhausted_sql
    assert "attempts + 1 >= least(max_attempts, $3)" in exhausted_sql
    assert "for update skip locked" in exhausted_sql.lower()
    assert exhausted_args == ("parquet_5f", 5, 4, 2)
    claim_sql, claim_args = conn.fetch_calls[0]
    assert "set status = 'dead_letter'" not in claim_sql
    assert "for update skip locked" in claim_sql.lower()
    assert "lease_version = checkpoint.lease_version + 1" in claim_sql
    assert "lease_until <= clock_timestamp()" in claim_sql
    assert claim_args == ("parquet_5f", 5, 2, "worker-a", 90, 4)

    assert asyncio.run(store.heartbeat(
        checkpoint_id=7,
        claim_token="claim-a",
        lease_version=2,
        lease_seconds=90,
    ))
    assert asyncio.run(store.record_member_success(
        checkpoint_id=7,
        claim_token="claim-a",
        lease_version=2,
        expected_imported_rows=100,
    ))
    assert asyncio.run(store.yield_member(
        checkpoint_id=7,
        claim_token="claim-a",
        lease_version=2,
        expected_imported_rows=100,
    ))
    assert asyncio.run(store.record_member_failure(
        checkpoint_id=7,
        claim_token="claim-a",
        lease_version=2,
        expected_imported_rows=100,
        error="boom",
    ))

    terminal_sql = "\n".join(sql.lower() for sql, _args in conn.fetchrow_calls)
    assert terminal_sql.count("claim_token = $2") == 4
    assert terminal_sql.count("lease_version = $3") == 4
    assert terminal_sql.count("lease_until > clock_timestamp()") == 4
    assert terminal_sql.count("imported_rows = $4") == 3


def test_scheme2_member_batch_fences_identity_and_progress_in_write_transaction(monkeypatch) -> None:
    checkpoint = {
        "id": 7,
        "root_path": "D:/root",
        "source_profile": "parquet_5f",
        "zip_path": "D:/root/2024.zip",
        "member_path": "20240102.parquet",
        "member_crc32": 123,
        "member_size_bytes": 456,
        "content_sha256": None,
        "timeframe": 5,
        "claim_token": "claim-a",
        "lease_version": 2,
    }
    conn = FakeConnection(checkpoint_row=checkpoint)
    writer = PostgresScheme2KlineWriter("postgresql://example")
    writer._pool = FakePool(conn)
    recorded = []

    async def record_scopes(_conn, *, scopes):
        recorded.extend(scopes)
        return len(scopes)

    monkeypatch.setattr(scheme2_storage, "record_present_scopes", record_scopes)
    bar_end = datetime(2026, 7, 10, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai"))
    result = asyncio.run(writer.commit_member_batch(
        task=checkpoint,
        expected_imported_rows=0,
        symbols=[SymbolInfo("000001.SZ", "000001", "SZ", "000001")],
        bars=[Bar("000001.SZ", "5f", bar_end, 10, 11, 9, 10.5, 100)],
        lease_seconds=90,
    ))

    assert result == 1
    assert conn.events[0] == "begin"
    assert "checkpoint-lock" in conn.events
    assert "klines-upsert" in conn.events
    assert conn.events[-1] == "commit"
    lock_sql, _lock_args = conn.fetchrow_calls[0]
    assert "lease_until > clock_timestamp()" in lock_sql
    progress_sql, progress_args = conn.fetchrow_calls[-1]
    assert "lease_until > clock_timestamp()" not in progress_sql
    assert "imported_rows = $4" in progress_sql
    assert progress_args[3:] == (0, 1, 90)


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
    def __init__(self, connection) -> None:
        self.connection = connection

    async def __aenter__(self):
        self.connection.events.append("begin")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.connection.events.append("rollback" if exc_type else "commit")
        return None


class FakeConnection:
    def __init__(
        self,
        *,
        execute_result: str = "UPDATE 1",
        fetchval_result=True,
        symbol_rows=None,
        scope_rows=None,
        checkpoint_row=None,
    ) -> None:
        self.execute_result = execute_result
        self.fetchval_result = fetchval_result
        self.symbol_rows = list(symbol_rows or [])
        self.scope_rows = list(scope_rows or [])
        self.checkpoint_row = checkpoint_row
        self.executes = []
        self.copies = []
        self.executemany_calls = []
        self.fetch_calls = []
        self.fetchrow_calls = []
        self.events = []

    def transaction(self):
        return FakeTransaction(self)

    async def execute(self, sql, *args):
        self.executes.append((sql, args))
        normalized = " ".join(sql.lower().split())
        if "insert into klines" in normalized:
            self.events.append("klines-upsert")
        elif "delete from klines" in normalized:
            self.events.append("klines-delete")
        elif "scheme2_ingest_watermarks" in normalized:
            self.events.append("watermark-upsert")
        return self.execute_result

    async def executemany(self, sql, rows):
        self.executemany_calls.append((sql, rows))
        if "insert into klines" in sql.lower():
            self.events.append("klines-upsert")

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        normalized = " ".join(sql.lower().split())
        if "from _scheme2_kline_stage" in normalized or "from _native_parquet_kline_stage" in normalized:
            self.events.append("resolve-scopes")
            return self.scope_rows
        if "from symbols" in normalized:
            self.events.append("resolve-symbols")
            return self.symbol_rows
        return []

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        normalized = " ".join(sql.lower().split())
        if "from scheme2_source_member_checkpoints" in normalized:
            self.events.append("checkpoint-lock")
            return self.checkpoint_row
        if "update scheme2_source_member_checkpoints" in normalized:
            self.events.append("checkpoint-update")
            return {"id": args[0]}
        assert "kline_scope_catalog_control" in normalized
        assert "for share" in normalized
        return {"control_key": "active"}

    async def fetchval(self, sql, *args):
        self.events.append("claim-check")
        return self.fetchval_result

    async def copy_records_to_table(self, table, *, records, columns):
        self.copies.append(
            {
                "table": table,
                "records": list(records),
                "columns": list(columns),
            }
        )
