from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
COLLECTOR = ROOT / "services" / "collector" / "collector"

KLINE_DML = re.compile(
    r"\b(?:insert\s+into|delete\s+from|update|truncate)\s+(?:public\.)?klines\b",
    re.IGNORECASE,
)

EXPECTED_WRITERS = {
    "aggregate_timeframes_from_daily.py": ("refresh_scopes_exact",),
    "kline_canonical_audit.py": ("invalidate_scopes",),
    "native_parquet_import.py": ("record_present_scopes",),
    "storage/postgres.py": ("record_present_scopes", "refresh_scopes_exact"),
    "storage/scheme2_postgres.py": ("record_present_scopes",),
}


def test_every_direct_canonical_kline_writer_maintains_scope_catalog() -> None:
    direct_writers: dict[str, str] = {}
    for path in COLLECTOR.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        if KLINE_DML.search(source):
            direct_writers[path.relative_to(COLLECTOR).as_posix()] = source

    assert direct_writers.keys() == EXPECTED_WRITERS.keys()
    for relative_path, helpers in EXPECTED_WRITERS.items():
        for helper in helpers:
            assert helper in direct_writers[relative_path], relative_path
