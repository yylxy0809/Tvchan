from __future__ import annotations

import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from collector.native_parquet_import import discover_tasks, parse_task


def _write_member(path: Path, rows: dict) -> None:
    temporary = path.with_suffix(".parquet")
    pq.write_table(pa.table(rows), temporary)
    with zipfile.ZipFile(path, "w") as archive:
        archive.write(temporary, "bars.parquet")
    temporary.unlink()


def test_native_parser_separates_invalid_ohlc_and_keeps_source_identity(tmp_path: Path) -> None:
    folder = tmp_path / "30m_price"
    folder.mkdir()
    archive = folder / "2024.zip"
    _write_member(
        archive,
        {
            "code": ["000001.SZ", "000001.SZ"],
            "trade_time": ["2024-01-02 09:30:00", "2024-01-02 10:00:00"],
            "open": [10.0, 10.0],
            "high": [11.0, 9.0],
            "low": [9.0, 9.5],
            "close": [10.5, 10.2],
            "vol": [100, 101],
            "amount": [1000.0, 1001.0],
        },
    )

    task = discover_tasks(tmp_path, timeframes=["30f"], years=set())[0]
    parsed = parse_task(task, {}, set(), set())

    assert task.source_ref.startswith("30m_price/2024.zip!bars.parquet#crc32=")
    assert task.source_checksum.startswith("crc32=")
    assert len(parsed.bars) == 1
    assert parsed.last_source_row == 1
    assert [(row.source_row, row.reason, row.symbol_text) for row in parsed.quarantines] == [
        (1, "invalid_ohlc", "000001.SZ")
    ]
    assert parsed.quarantines[0].raw_payload["high"] == 9.0
