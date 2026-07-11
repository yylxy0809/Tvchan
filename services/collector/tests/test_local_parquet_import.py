from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector import local_parquet_import as import_module
from collector.local_parquet_import import discover_tasks, parse_task, run_once


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
