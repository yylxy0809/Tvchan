from pathlib import Path

import pytest

import asyncio

from app.engine.phase_1_21 import _snapshot, build_preflight_manifest


def test_preflight_records_facts_and_requires_sources(tmp_path: Path):
    source = tmp_path / "x.json"
    source.write_text("{}", encoding="utf-8")
    assert len(build_preflight_manifest([source])["artifacts"][0]["sha256"]) == 64
    with pytest.raises(FileNotFoundError):
        build_preflight_manifest([tmp_path / "missing.json"])


def test_preflight_snapshot_contract_lists_readonly_database_facts(tmp_path: Path):
    source = tmp_path / "x.json"
    source.write_text("{}", encoding="utf-8")
    manifest = build_preflight_manifest([source])
    assert "chan_c_runs" in manifest["database_tables"]
    assert any("SELECT" in sql for sql in manifest["readonly_sql_summary"])


def test_readonly_snapshot_records_published_head_and_run_group_counts():
    class Transaction:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return None
    class Connection:
        def transaction(self, *, readonly):
            assert readonly is True
            return Transaction()
        async def fetch(self, sql):
            return [{"run_group_id": "research_daily_close", "count": 9}]
        async def fetchval(self, sql, *args):
            return 7 if "published_heads" in sql else 0
    class Acquire:
        async def __aenter__(self): return Connection()
        async def __aexit__(self, *args): return None
    class Pool:
        def acquire(self): return Acquire()
    snapshot = asyncio.run(_snapshot(Pool()))
    assert snapshot["published_head_row_count"] == 7
    assert snapshot["run_group_counts"] == {"research_daily_close": 9}
