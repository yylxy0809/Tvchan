import asyncio
import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

import app.engine.phase_1_21 as phase


class Transaction:
    def __init__(self, isolation, readonly):
        self.isolation, self.readonly = isolation, readonly
        self.started = self.committed = self.rolled_back = False

    async def start(self): self.started = True
    async def commit(self): self.committed = True
    async def rollback(self): self.rolled_back = True


class ReadOnlyConn:
    def __init__(self, *, fail_on_distinct=False, target_count=0):
        self.calls, self.transactions, self.fail_on_distinct, self.target_count = [], [], fail_on_distinct, target_count

    def transaction(self, *, isolation, readonly):
        tx = Transaction(isolation, readonly)
        self.transactions.append(tx)
        return tx

    @staticmethod
    def _readonly_sql(query):
        normalized = " ".join(query.upper().split())
        forbidden = r"\b(INSERT|UPDATE|DELETE|MERGE|COPY|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|INTO)\b"
        if re.search(forbidden, normalized) or not (normalized.startswith("SELECT ") or normalized.startswith("WITH ")):
            raise AssertionError(f"non-readonly SQL supplied through fetch: {normalized}")

    async def fetch(self, query, *args):
        self._readonly_sql(query)
        self.calls.append(("fetch", query, args))
        if self.fail_on_distinct and "select distinct" in query:
            raise RuntimeError("injected readonly read failure")
        if "coalesce(run_group_id" in query:
            return [{"run_group_id": "research_daily_close", "count": 1}]
        if "min(coalesce(cutoff_bar_end" in query:
            return [{"start_time": datetime(2020, 1, 1, tzinfo=UTC), "end_time": datetime(2025, 1, 1, tzinfo=UTC)}]
        return []

    async def fetchval(self, query, *args):
        self._readonly_sql(query)
        self.calls.append(("fetchval", query, args))
        if "txid_current_snapshot" in query:
            return "42:42:"
        return self.target_count if "where run_group_id" in query else 0

    async def execute(self, *_args): raise AssertionError("readonly pipeline must not execute DML")
    async def executemany(self, *_args): raise AssertionError("readonly pipeline must not executemany")
    async def copy_records_to_table(self, *_args): raise AssertionError("readonly pipeline must not copy")


class ReadOnlyPool:
    def __init__(self, conn): self.conn, self.acquire_count, self.released, self.closed = conn, 0, False, False
    async def acquire(self):
        self.acquire_count += 1
        if self.acquire_count > 1:
            raise AssertionError("Phase 1.21 must use one acquired connection")
        return self.conn
    async def release(self, conn): assert conn is self.conn; self.released = True
    async def close(self): self.closed = True


def test_snapshot_conn_uses_only_read_methods_and_records_contract_fields():
    conn = ReadOnlyConn()
    snapshot = asyncio.run(phase._snapshot_conn(conn, transaction_snapshot="42:42:"))
    assert snapshot["published_head_row_count"] == 0
    assert snapshot["run_group_counts"] == {"research_daily_close": 1}
    assert snapshot["transaction_snapshot"] == "42:42:"
    assert {kind for kind, _, _ in conn.calls} <= {"fetch", "fetchval"}


def test_readonly_fake_rejects_dml_or_ddl_routed_through_fetch():
    conn = ReadOnlyConn()
    for query in ("delete from chan_c_runs", "with changed as (update chan_c_runs set status='x' returning *) select * from changed", "select * into scratch from chan_c_runs"):
        with pytest.raises(AssertionError, match="non-readonly SQL"):
            asyncio.run(conn.fetch(query))


def test_full_pipeline_uses_one_readonly_connection_and_promotes(monkeypatch, tmp_path: Path):
    audit_conn, guard_conn = ReadOnlyConn(), ReadOnlyConn()
    audit_pool, guard_pool = ReadOnlyPool(audit_conn), ReadOnlyPool(guard_conn)
    monkeypatch.setattr(phase, "create_pool", _pool_factory(audit_pool, guard_pool))
    result = asyncio.run(phase.run_phase_1_21(output_dir=tmp_path / "out"))
    assert result["status"] == "DONE"
    assert audit_pool.acquire_count == guard_pool.acquire_count == 1
    assert audit_pool.released and audit_pool.closed and guard_pool.released and guard_pool.closed
    assert len(audit_conn.transactions) == len(guard_conn.transactions) == 1
    assert audit_conn.transactions[0].isolation == guard_conn.transactions[0].isolation == "repeatable_read"
    assert audit_conn.transactions[0].readonly and audit_conn.transactions[0].committed
    assert guard_conn.transactions[0].readonly and guard_conn.transactions[0].committed
    assert {kind for kind, _, _ in audit_conn.calls} <= {"fetch", "fetchval"}
    assert any("min(coalesce(cutoff_bar_end" in query for kind, query, _args in audit_conn.calls if kind == "fetch")
    universe = json.loads((tmp_path / "out" / "observable_research_universe.json").read_text(encoding="utf-8"))
    assert universe["audit_scope"]["scope_mode"] == "full_research_daily_close_history"


def test_full_pipeline_read_failure_rolls_back_and_does_not_promote(monkeypatch, tmp_path: Path):
    target = tmp_path / "out"; target.mkdir(); (target / "old.txt").write_bytes(b"old-output")
    audit_conn = ReadOnlyConn(fail_on_distinct=True)
    pool = ReadOnlyPool(audit_conn)
    monkeypatch.setattr(phase, "create_pool", _pool_factory(pool))
    with pytest.raises(RuntimeError, match="injected readonly read failure"):
        asyncio.run(phase.run_phase_1_21(output_dir=target))
    assert (target / "old.txt").read_bytes() == b"old-output"
    audit_pool = pool
    assert audit_pool.conn.transactions[0].rolled_back and not audit_pool.conn.transactions[0].committed
    assert audit_pool.acquire_count == 1 and audit_pool.released and audit_pool.closed


def test_post_commit_guard_is_a_separate_snapshot_and_detects_target_group_change(monkeypatch, tmp_path: Path):
    audit_pool = ReadOnlyPool(ReadOnlyConn(target_count=0))
    guard_pool = ReadOnlyPool(ReadOnlyConn(target_count=1))
    monkeypatch.setattr(phase, "create_pool", _pool_factory(audit_pool, guard_pool))
    result = asyncio.run(phase.run_phase_1_21(output_dir=tmp_path / "out"))
    after = (tmp_path / "out" / "database_readonly_snapshot_after.json").read_text(encoding="utf-8")
    assert result["database_unchanged"] is False
    assert '"consistency_scope": "post_commit_readonly_guard"' in after
    assert '"in_transaction_after"' in after


async def _async_value(value):
    return value


def _pool_factory(*pools):
    queue = list(pools)
    async def create_pool():
        if not queue:
            raise AssertionError("unexpected additional pool acquisition")
        return queue.pop(0)
    return create_pool
