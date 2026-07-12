import asyncio
import csv
import io
import json
from datetime import UTC, datetime

import pytest

from collector.kline_truth_manifest import (
    ACTIVE_COVERAGE_SQL,
    ManifestMetadata,
    build_artifacts,
    collect_truth_snapshot,
)


NOW = datetime(2026, 7, 12, tzinfo=UTC)


def snapshot():
    return {
        "coverage": [
            {"symbol_id": 1, "code": "600000", "exchange": "SH", "timeframe": tf,
             "last_bar_end": NOW.isoformat(), "source": "parquet", "updated_at": NOW.isoformat()}
            for tf in (5, 30, 1440, 10080)
        ] + [
            {"symbol_id": 2, "code": "000001", "exchange": "SZ", "timeframe": 1440,
             "last_bar_end": NOW.isoformat(), "source": "parquet", "updated_at": NOW.isoformat()}
        ],
        "import_runs": [{
            "import_run_id": "run-1", "source_name": "parquet_native", "started_at": NOW.isoformat(),
            "completed_at": NOW.isoformat(), "status": "completed", "parameters": {}, "summary": {},
            "failure": None, "checkpoint_count": 3, "completed_checkpoints": 3,
            "failed_checkpoints": 0, "accepted_rows": 100, "quarantined_rows": 2,
        }],
        "failed_checkpoints": [],
        "import_quarantine": [{"import_run_id": "run-1", "timeframe": "5f", "reason": "bad_ohlc", "rows": 2}],
        "audit_runs": [],
        "audit_quarantine": [],
    }


def metadata():
    return ManifestMetadata(
        git_commit="abc123", image="collector@sha256:123", config_hash="cfg",
        source_roots=("F:/data",), generated_at=NOW,
    )


def test_builds_all_deterministic_truth_artifacts() -> None:
    artifacts = build_artifacts(snapshot(), metadata())
    assert set(artifacts) == {
        "kline_truth_summary.json", "kline_truth_summary.md", "coverage_by_symbol.jsonl",
        "exceptions.csv", "run_manifest.json",
    }
    summary = json.loads(artifacts["kline_truth_summary.json"])
    assert summary["active_symbols"] == 2
    assert summary["symbols_with_watermark"] == {"5": 1, "30": 1, "1440": 2, "10080": 1, "43200": 0}
    manifest = json.loads(artifacts["run_manifest.json"])
    assert manifest["git_commit"] == "abc123"
    assert manifest["source_roots"] == ["F:/data"]
    assert len(manifest["active_universe_hash"]) == 64
    assert "klines was not scanned" in artifacts["kline_truth_summary.md"]


def test_exceptions_include_missing_watermarks_and_quarantine() -> None:
    rows = list(csv.DictReader(io.StringIO(build_artifacts(snapshot(), metadata())["exceptions.csv"])))
    assert sum(row["category"] == "missing_watermark" for row in rows) == 5
    assert any(row["category"] == "import_quarantine" and row["rows"] == "2" for row in rows)


def test_universe_hash_is_independent_of_query_row_order() -> None:
    first = json.loads(build_artifacts(snapshot(), metadata())["run_manifest.json"])["active_universe_hash"]
    reversed_snapshot = snapshot()
    reversed_snapshot["coverage"].reverse()
    second = json.loads(build_artifacts(reversed_snapshot, metadata())["run_manifest.json"])["active_universe_hash"]
    assert first == second


def test_naive_database_timestamp_is_rejected() -> None:
    class Conn:
        async def fetch(self, sql):
            if sql == ACTIVE_COVERAGE_SQL:
                return [{"symbol_id": 1, "code": "x", "exchange": "SH", "timeframe": 5,
                         "last_bar_end": datetime(2026, 1, 1), "source": "x", "updated_at": NOW}]
            return []

    with pytest.raises(ValueError, match="timezone-aware"):
        asyncio.run(collect_truth_snapshot(Conn()))


def test_queries_do_not_read_kline_fact_table() -> None:
    class Conn:
        def __init__(self):
            self.queries = []

        async def fetch(self, sql):
            self.queries.append(sql)
            return []

    conn = Conn()
    asyncio.run(collect_truth_snapshot(conn))
    normalized = " ".join(conn.queries).lower()
    assert " from klines " not in normalized
    assert "scheme2_ingest_watermarks" in normalized
    assert "kline_import_checkpoints" in normalized
