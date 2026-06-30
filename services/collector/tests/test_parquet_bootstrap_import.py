from __future__ import annotations

import asyncio
import zipfile
from datetime import datetime
from pathlib import Path

from collector import parquet_bootstrap_import as import_module


def test_discover_parquet_members_records_zip_member_identity(tmp_path: Path) -> None:
    zip_path = tmp_path / "2024.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("20240102.parquet", b"parquet-bytes")
        archive.writestr("README.txt", b"ignored")

    members = import_module.discover_parquet_members(tmp_path)

    assert len(members) == 1
    assert members[0].root_path == str(tmp_path.resolve())
    assert members[0].source_profile == "parquet_5f"
    assert members[0].zip_path == str(zip_path.resolve())
    assert members[0].member_path == "20240102.parquet"
    assert members[0].member_size_bytes == len(b"parquet-bytes")
    assert members[0].timeframe == 5


def test_parse_parquet_rows_keeps_trade_time_as_bar_end() -> None:
    trade_time = datetime(2024, 1, 2, 9, 35, 0)

    parsed = import_module.parse_parquet_rows(
        [
            {
                "code": "000001",
                "trade_time": trade_time,
                "open": 10.12,
                "high": 10.5,
                "low": 10.0,
                "close": 10.3,
                "vol": 12345,
                "amount": 987654.32,
            }
        ]
    )

    assert list(parsed.symbols) == ["000001.SZ"]
    assert len(parsed.bars) == 1
    bar = parsed.bars[0]
    assert bar.symbol == "000001.SZ"
    assert bar.timeframe == "5f"
    assert bar.ts.hour == 9
    assert bar.ts.minute == 35
    assert bar.ts.tzinfo == import_module.SHANGHAI_TZ
    assert bar.source == "parquet_5f"


def test_process_member_task_writes_member_and_records_success(monkeypatch) -> None:
    row = {
        "code": "600519",
        "trade_time": "2024-01-02 09:35:00",
        "open": 100.1,
        "high": 101.2,
        "low": 99.9,
        "close": 100.8,
        "vol": 1200,
        "amount": 1_234_567.89,
    }

    monkeypatch.setattr(
        import_module,
        "_iter_member_rows",
        lambda zip_path, member_path, *, batch_size: iter([[row]]),
    )

    class FakeWriter:
        def __init__(self) -> None:
            self.symbols = []
            self.bars = []

        async def upsert_5f_bars(self, *, symbols, bars):
            self.symbols.extend(symbols)
            self.bars.extend(bars)
            return len(self.bars)

    class FakeCheckpointStore:
        def __init__(self) -> None:
            self.successes = []
            self.failures = []

        async def record_member_success(self, **kwargs):
            self.successes.append(kwargs)

        async def record_member_failure(self, **kwargs):
            self.failures.append(kwargs)

    writer = FakeWriter()
    checkpoint_store = FakeCheckpointStore()

    result = asyncio.run(
        import_module.process_member_task(
            writer=writer,
            checkpoint_store=checkpoint_store,
            task={
                "id": 7,
                "zip_path": "fake.zip",
                "member_path": "20240102.parquet",
            },
            batch_size=1000,
        )
    )

    assert result == {"bars": 1}
    assert [symbol.symbol for symbol in writer.symbols] == ["600519.SH"]
    assert [bar.symbol for bar in writer.bars] == ["600519.SH"]
    assert writer.bars[0].ts.minute == 35
    assert checkpoint_store.successes == [{"checkpoint_id": 7, "imported_rows": 1}]
    assert checkpoint_store.failures == []


def test_process_member_task_records_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        import_module,
        "_iter_member_rows",
        lambda zip_path, member_path, *, batch_size: iter([[{"code": "000001"}]]),
    )

    class FakeWriter:
        async def upsert_5f_bars(self, *, symbols, bars):
            raise AssertionError("missing columns should fail before writes")

    class FakeCheckpointStore:
        def __init__(self) -> None:
            self.failures = []

        async def record_member_success(self, **kwargs):
            raise AssertionError(f"unexpected success: {kwargs}")

        async def record_member_failure(self, **kwargs):
            self.failures.append(kwargs)

    checkpoint_store = FakeCheckpointStore()

    result = asyncio.run(
        import_module.process_member_task(
            writer=FakeWriter(),
            checkpoint_store=checkpoint_store,
            task={
                "id": 8,
                "zip_path": "fake.zip",
                "member_path": "bad.parquet",
            },
            batch_size=1000,
        )
    )

    assert result == {"bars": 0}
    assert checkpoint_store.failures[0]["checkpoint_id"] == 8
    assert checkpoint_store.failures[0]["imported_rows"] == 0
    assert "Missing required parquet columns" in checkpoint_store.failures[0]["error"]


def test_process_tasks_concurrently_serializes_writes(monkeypatch) -> None:
    row = {
        "code": "600519",
        "trade_time": "2024-01-02 09:35:00",
        "open": 100.1,
        "high": 101.2,
        "low": 99.9,
        "close": 100.8,
        "vol": 1200,
        "amount": 1_234_567.89,
    }

    monkeypatch.setattr(
        import_module,
        "_iter_member_rows",
        lambda zip_path, member_path, *, batch_size: iter([[row]]),
    )

    class FakeWriter:
        def __init__(self) -> None:
            self.active_writes = 0
            self.max_active_writes = 0

        async def upsert_5f_bars(self, *, symbols, bars):
            self.active_writes += 1
            self.max_active_writes = max(self.max_active_writes, self.active_writes)
            await asyncio.sleep(0.01)
            self.active_writes -= 1
            return len(bars)

    class FakeCheckpointStore:
        def __init__(self) -> None:
            self.successes = []

        async def record_member_success(self, **kwargs):
            self.successes.append(kwargs)

        async def record_member_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    writer = FakeWriter()
    checkpoint_store = FakeCheckpointStore()
    tasks = [
        {"id": 1, "zip_path": "fake.zip", "member_path": "20240102.parquet"},
        {"id": 2, "zip_path": "fake.zip", "member_path": "20240103.parquet"},
        {"id": 3, "zip_path": "fake.zip", "member_path": "20240104.parquet"},
    ]

    result = asyncio.run(
        import_module.process_tasks_concurrently(
            writer=writer,
            checkpoint_store=checkpoint_store,
            tasks=tasks,
            batch_size=1000,
            concurrency=3,
            write_concurrency=1,
        )
    )

    assert result == {"bars": 3}
    assert writer.max_active_writes == 1
    assert [item["checkpoint_id"] for item in checkpoint_store.successes] == [1, 2, 3]


def test_parse_symbol_accepts_prefixed_and_dotted_codes() -> None:
    assert import_module.parse_symbol("SZ000001").symbol == "000001.SZ"
    assert import_module.parse_symbol("600519.SH").symbol == "600519.SH"
    assert import_module.parse_symbol(1).symbol == "000001.SZ"
