from pathlib import Path
import asyncio

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.missing_5f_manifest import PROVIDER_PLAN, build_manifest, probe_rows


def write_master(path: Path) -> None:
    pq.write_table(
        pa.table(
            {
                "ts_code": ["600000.SH", "000001.SZ", "430001.BJ", "000003.SZ"],
                "symbol": ["600000", "000001", "430001", "000003"],
                "name": ["sh", "sz", "bj", "delisted"],
                "market": ["main", "main", "bj", "main"],
                "list_status": ["L", "L", "L", "D"],
                "list_date": ["1", "1", "1", "1"],
            }
        ),
        path,
    )


def test_manifest_is_exact_active_antijoin_and_bj_excludes_tdx(tmp_path: Path):
    master = tmp_path / "master.parquet"
    root = tmp_path / "5f"
    root.mkdir()
    write_master(master)
    pq.write_table(pa.table({"ts": [1]}), root / "600000.SH.parquet")

    rows = build_manifest(master, root)

    assert [row["symbol"] for row in rows] == ["000001.SZ", "430001.BJ"]
    assert rows[0]["missing_reason"] == "file_absent"
    assert rows[1]["provider_plan"] == "tencent,baidu"
    assert "pytdx" not in PROVIDER_PLAN["BJ"]


def test_probe_hard_limits_selection():
    rows = [{"symbol": f"{index:06d}.SZ", "exchange": "SZ"} for index in range(11)]
    with pytest.raises(ValueError, match="1-10"):
        asyncio.run(probe_rows(rows, limit=1, timeout=1))
