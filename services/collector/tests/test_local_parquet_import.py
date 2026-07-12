from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector import local_parquet_import as import_module
from collector.local_parquet_import import (
    deterministic_import_run_id,
    discover_tasks,
    parse_task,
    run_once,
    static_shard,
    symbol_rows_for_symbols,
)


def _write_intraday(path: Path, symbol: str, *, bad_ohlc: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "ts_code": [symbol], "trade_date": [datetime(2026, 1, 5)], "trade_time": [datetime(2026, 1, 5, 9, 30)],
        "open": [10.0], "high": [9.0 if bad_ohlc else 11.0], "low": [9.0], "close": [10.5],
        "vol": [1000.0], "amount": [10500.0],
    }), path)


def _write_daily(path: Path, symbol: str) -> None:
    pq.write_table(pa.table({
        "ts_code": [symbol], "trade_date": [datetime(2026, 1, 5)], "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
        "vol": [1000.0], "amount": [1050.0],  # 1050 thousand-yuan / 1000 hands => 10.5 yuan/share
    }), path)


def _write_symbol_master(path: Path, rows: list[tuple[str, str]]) -> None:
    pq.write_table(pa.table({
        "ts_code": [symbol for symbol, _status in rows],
        "list_status": [status for _symbol, status in rows],
    }), path)


def test_adapter_parses_local_5f_daily_and_quarantines_bad_ohlc(tmp_path: Path) -> None:
    _write_intraday(tmp_path / "stock_5min" / "000001.SZ.parquet", "000001.SZ")
    _write_intraday(tmp_path / "stock_30min" / "000001.SZ.parquet", "000001.SZ", bad_ohlc=True)
    _write_daily(tmp_path / "stock_daily.parquet", "000001.SZ")
    tasks = discover_tasks(tmp_path, timeframes=["5f", "30f", "1d"], symbols=["000001.SZ"])
    parsed = {task.timeframe: parse_task(task, batch_size=1) for task in tasks}
    assert len(parsed["5f"].bars) == 1
    assert parsed["5f"].bars[0][8] == 1000
    assert len(parsed["1d"].bars) == 1
    assert parsed["1d"].bars[0][8] == 100_000
    assert [item.reason for item in parsed["30f"].quarantines] == ["invalid_ohlc"]


def test_bj_30f_is_explicitly_rejected(tmp_path: Path) -> None:
    _write_intraday(tmp_path / "stock_30min" / "430001.BJ.parquet", "430001.BJ")
    task = discover_tasks(tmp_path, timeframes=["30f"], symbols=["430001.BJ"])[0]
    parsed = parse_task(task)
    assert not parsed.bars
    assert [item.reason for item in parsed.quarantines] == ["rejected_bj_30f_non_native_source"]


def test_missing_intraday_symbol_file_becomes_a_coverage_quarantine_not_a_fatal_error(tmp_path: Path) -> None:
    _write_intraday(tmp_path / "stock_30min" / "920000.BJ.parquet", "920000.BJ")
    _write_daily(tmp_path / "stock_daily.parquet", "920000.BJ")
    tasks = discover_tasks(tmp_path, timeframes=["5f", "30f", "1d"], symbols=["920000.BJ"])
    parsed = {task.timeframe: parse_task(task) for task in tasks}
    assert [item.reason for item in parsed["5f"].quarantines] == ["missing_source_file"]
    assert parsed["5f"].bars == []
    # The missing 5f member cannot prevent the known 1d file from being read.
    assert len(parsed["1d"].bars) == 1
    # Existing BJ source policy remains intact.
    assert [item.reason for item in parsed["30f"].quarantines] == ["rejected_bj_30f_non_native_source"]


def test_zero_turnover_bar_is_accepted_without_guessing_a_volume_unit(tmp_path: Path) -> None:
    _write_intraday(tmp_path / "stock_5min" / "000001.SZ.parquet", "000001.SZ")
    path = tmp_path / "stock_5min" / "000001.SZ.parquet"
    pq.write_table(pa.table({
        "ts_code": ["000001.SZ"], "trade_date": [datetime(2026, 1, 5)], "trade_time": [datetime(2026, 1, 5, 9, 30)],
        "open": [10.0], "high": [10.0], "low": [10.0], "close": [10.0], "vol": [0.0], "amount": [0.0],
    }), path)
    parsed = parse_task(discover_tasks(tmp_path, timeframes=["5f"], symbols=["000001.SZ"])[0])
    assert len(parsed.bars) == 1
    assert parsed.bars[0][8] == 0


def test_write_mode_requires_explicit_symbols(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="--symbols"):
        import asyncio
        asyncio.run(run_once(type("Args", (), {"timeframes": "5f", "symbols": None, "dry_run": False, "root": tmp_path})()))


def test_dry_run_lists_only_explicit_sources_and_never_needs_database(tmp_path: Path) -> None:
    _write_intraday(tmp_path / "stock_5min" / "000001.SZ.parquet", "000001.SZ")
    import asyncio
    result = asyncio.run(run_once(type("Args", (), {
        "timeframes": "5f", "symbols": "000001.SZ", "dry_run": True, "root": tmp_path,
    })()))
    assert result["dry_run"] is True
    assert result["sources"] == ["stock_5min/000001.SZ.parquet"]


def test_explicit_import_run_id_is_reused_across_cli_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_intraday(tmp_path / "stock_5min" / "000001.SZ.parquet", "000001.SZ")
    fixed_run_id = "90c79d5d-f75a-47e6-9b8c-85e3ed190140"

    class FakeWriter:
        created_run_ids: list[object] = []
        batch_run_ids: list[object] = []

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        async def open(self) -> None:
            pass

        async def close(self) -> None:
            pass

        async def create_import_run(self, *, import_run_id, parameters) -> None:
            self.created_run_ids.append(import_run_id)

        async def upsert_symbol_rows(self, rows) -> int:
            return len(list(rows))

        async def completed_import_checkpoint(self, **_kwargs):
            return None

        async def upsert_import_batch(self, *, import_run_id, **_kwargs) -> int:
            self.batch_run_ids.append(import_run_id)
            return 1

    monkeypatch.setattr(import_module, "NativeParquetWriter", FakeWriter)
    args = type("Args", (), {
        "timeframes": "5f", "symbols": "000001.SZ", "dry_run": False,
        "root": tmp_path, "database_url": "postgresql://unused", "batch_size": 10,
        "import_run_id": fixed_run_id,
    })()
    import asyncio
    first = asyncio.run(run_once(args))
    second = asyncio.run(run_once(args))

    assert first["import_run_id"] == fixed_run_id
    assert second["import_run_id"] == fixed_run_id
    assert {str(value) for value in FakeWriter.created_run_ids} == {fixed_run_id}
    assert {str(value) for value in FakeWriter.batch_run_ids} == {fixed_run_id}


def test_symbol_rows_are_canonical_and_stably_sorted_for_parallel_preinitialization() -> None:
    assert symbol_rows_for_symbols(["600000.SH", "000001.SZ", "000001.SZ"]) == [
        ("000001", "SZ", "000001.SZ", "stock", "A_SHARE", True),
        ("600000", "SH", "600000.SH", "stock", "A_SHARE", True),
    ]


def test_completed_checkpoint_skips_reparsing_and_rewriting_on_resume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_intraday(tmp_path / "stock_5min" / "000001.SZ.parquet", "000001.SZ")

    class FakeWriter:
        upserted = 0

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        async def open(self) -> None:
            pass

        async def close(self) -> None:
            pass

        async def create_import_run(self, **_kwargs) -> None:
            pass

        async def upsert_symbol_rows(self, _rows) -> int:
            return 1

        async def completed_import_checkpoint(self, **_kwargs):
            return (12, 3)

        async def upsert_import_batch(self, **_kwargs) -> int:
            self.upserted += 1
            return 1

    monkeypatch.setattr(import_module, "NativeParquetWriter", FakeWriter)
    import asyncio
    result = asyncio.run(run_once(type("Args", (), {
        "timeframes": "5f", "symbols": "000001.SZ", "active_only": False,
        "dry_run": False, "root": tmp_path, "database_url": "postgresql://unused",
        "batch_size": 10, "import_run_id": "90c79d5d-f75a-47e6-9b8c-85e3ed190140",
        "shard_index": 0, "shard_count": 1,
    })()))
    assert result["accepted_rows"] == 12
    assert result["quarantined_rows"] == 3
    assert result["resumed_tasks"] == 1
    assert FakeWriter.upserted == 0


def test_static_symbol_shards_are_stable_disjoint_and_exhaustive() -> None:
    symbols = ["600000.SH", "000002.SZ", "000001.SZ", "430001.BJ", "000001.SZ"]
    shards = [static_shard(symbols, shard_index=index, shard_count=3) for index in range(3)]
    assert shards == [["000001.SZ", "600000.SH"], ["000002.SZ"], ["430001.BJ"]]
    assert set().union(*map(set, shards)) == {"000001.SZ", "000002.SZ", "430001.BJ", "600000.SH"}
    assert not (set(shards[0]) & set(shards[1]) or set(shards[1]) & set(shards[2]))
    assert deterministic_import_run_id(root="F:/data", timeframes=["5f", "1d"], scope="active_only", shard_index=0, shard_count=3) == \
        deterministic_import_run_id(root="F:/data", timeframes=["5f", "1d"], scope="active_only", shard_index=0, shard_count=3)
    assert deterministic_import_run_id(root="F:/data", timeframes=["5f", "1d"], scope="active_only", shard_index=0, shard_count=3) != \
        deterministic_import_run_id(root="F:/data", timeframes=["5f", "1d"], scope="active_only", shard_index=1, shard_count=3)


def test_run_excludes_bj_30f_and_records_static_shard(tmp_path: Path) -> None:
    _write_intraday(tmp_path / "stock_30min" / "000001.SZ.parquet", "000001.SZ")
    _write_intraday(tmp_path / "stock_30min" / "430001.BJ.parquet", "430001.BJ")
    import asyncio
    result = asyncio.run(run_once(type("Args", (), {
        "timeframes": "30f", "symbols": "430001.BJ,000001.SZ", "active_only": False,
        "dry_run": True, "root": tmp_path, "shard_index": 0, "shard_count": 1,
    })()))
    assert result["symbols"] == ["000001.SZ", "430001.BJ"]
    assert result["sources"] == ["stock_30min/000001.SZ.parquet"]
    assert result["excluded_bj_30f_tasks"] == 1


def test_active_only_uses_symbol_master_then_static_shards(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_intraday(tmp_path / "stock_5min" / "000002.SZ.parquet", "000002.SZ")

    class FakeWriter:
        async def open(self) -> None:
            pass

        async def close(self) -> None:
            pass

        async def fetch_active_symbols(self):
            return {"000001.SZ", "000002.SZ", "600000.SH"}

    monkeypatch.setattr(import_module, "NativeParquetWriter", lambda *_args, **_kwargs: FakeWriter())
    import asyncio
    result = asyncio.run(run_once(type("Args", (), {
        "timeframes": "5f", "symbols": None, "active_only": True, "dry_run": True,
        "root": tmp_path, "database_url": "postgresql://unused", "shard_index": 1, "shard_count": 2,
    })()))
    assert result["selection_scope"] == "active_only"
    assert result["active_symbol_source"] == "database"
    assert result["symbols"] == ["000002.SZ"]
    assert result["sources"] == ["stock_5min/000002.SZ.parquet"]


def test_active_only_falls_back_to_local_master_for_fresh_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_intraday(tmp_path / "stock_5min" / "000001.SZ.parquet", "000001.SZ")
    _write_symbol_master(tmp_path / "stock_basic_data.parquet", [
        ("000001.SZ", "L"), ("000003.SZ", "D"), ("600000.SH", "L"),
    ])

    class FakeWriter:
        async def open(self) -> None:
            pass

        async def close(self) -> None:
            pass

        async def fetch_active_symbols(self):
            return set()

    monkeypatch.setattr(import_module, "NativeParquetWriter", lambda *_args, **_kwargs: FakeWriter())
    import asyncio
    result = asyncio.run(run_once(type("Args", (), {
        "timeframes": "5f", "symbols": None, "active_only": True, "dry_run": True,
        "root": tmp_path, "database_url": "postgresql://unused", "shard_index": 0, "shard_count": 1,
    })()))
    assert result["active_symbol_source"] == "master"
    assert result["symbols"] == ["000001.SZ", "600000.SH"]
    assert result["sources"] == ["stock_5min/000001.SZ.parquet", "stock_5min/600000.SH.parquet"]


def test_active_only_prefers_complete_local_master_over_nonempty_database_seed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_intraday(tmp_path / "stock_5min" / "000001.SZ.parquet", "000001.SZ")
    _write_symbol_master(tmp_path / "stock_basic_data.parquet", [
        ("000001.SZ", "L"), ("000002.SZ", "L"), ("000003.SZ", "D"),
    ])

    class FakeWriter:
        def __init__(self) -> None:
            self.fetch_called = False

        async def open(self) -> None:
            pass

        async def close(self) -> None:
            pass

        async def fetch_active_symbols(self):
            self.fetch_called = True
            return {"000001.SZ"}  # migration seed, intentionally incomplete

    writer = FakeWriter()
    monkeypatch.setattr(import_module, "NativeParquetWriter", lambda *_args, **_kwargs: writer)
    import asyncio
    result = asyncio.run(run_once(type("Args", (), {
        "timeframes": "5f", "symbols": None, "active_only": True, "dry_run": True,
        "root": tmp_path, "database_url": "postgresql://unused", "shard_index": 0, "shard_count": 1,
    })()))
    assert result["active_symbol_source"] == "master"
    assert result["symbols"] == ["000001.SZ", "000002.SZ"]
    assert writer.fetch_called is False
