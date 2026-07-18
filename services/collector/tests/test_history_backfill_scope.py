from __future__ import annotations

import asyncio
from pathlib import Path

import collector.history_backfill as history_backfill
import pytest
from collector.storage.backfill_postgres import PostgresBackfillTaskStore
from collector.market_fill import symbol_info_from_symbol


class _Acquire:
    def __init__(self, connection) -> None:
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args) -> None:
        return None


class _Pool:
    def __init__(self, connection) -> None:
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


class _Transaction:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *_args) -> None:
        return None


class _EnsureConnection:
    def __init__(self) -> None:
        self.rows = []
        self.fetch_args = ()
        self.fetch_query = ""

    def transaction(self):
        return _Transaction()

    async def executemany(self, _query, rows):
        self.rows = list(rows)

    async def fetch(self, query, *args):
        self.fetch_query = query
        self.fetch_args = args
        return [{"id": 11}, {"id": 12}]


def test_backfill_ensure_returns_the_exact_frozen_task_ids() -> None:
    connection = _EnsureConnection()
    store = PostgresBackfillTaskStore("postgresql://unused")
    store._pool = _Pool(connection)

    task_ids = asyncio.run(
        store.ensure_tasks(
            symbols=[symbol_info_from_symbol("000001.SZ")],
            timeframes=["5f", "30f"],
            provider="pytdx",
            page_size=300,
        )
    )

    assert task_ids == [11, 12]
    assert len(connection.rows) == 2
    assert connection.fetch_args == (["000001.SZ"], [5, 30], "pytdx")
    assert "any($2::integer[])" in connection.fetch_query


class _ClaimConnection:
    def __init__(self) -> None:
        self.query = ""
        self.args = ()

    def transaction(self):
        return _Transaction()

    async def execute(self, *_args):
        return "SELECT 1"

    async def fetch(self, query, *args):
        self.query = query
        self.args = args
        return []


def test_backfill_claim_is_limited_to_frozen_task_ids() -> None:
    connection = _ClaimConnection()
    store = PostgresBackfillTaskStore("postgresql://unused")
    store._pool = _Pool(connection)

    result = asyncio.run(
        store.claim_tasks(
            provider="pytdx",
            limit=2,
            worker_id="scoped-worker",
            lease_seconds=300,
            max_attempts=5,
            task_ids=[41, 43],
        )
    )

    assert result == []
    assert connection.args[-1] == [41, 43]
    assert connection.query.count("id = any($7::bigint[])") == 2
    assert connection.query.count("run_id is not distinct from $6::uuid") == 2


class _ResetConnection:
    def __init__(self) -> None:
        self.query = ""
        self.args = ()

    def transaction(self):
        return _Transaction()

    async def execute(self, query, *args):
        if "set_config" in query:
            return "SELECT 1"
        self.query = query
        self.args = args
        return "UPDATE 1"


def test_backfill_reset_running_is_limited_to_frozen_task_ids() -> None:
    connection = _ResetConnection()
    store = PostgresBackfillTaskStore("postgresql://unused")
    store._pool = _Pool(connection)

    count = asyncio.run(store.reset_running(provider="pytdx", task_ids=[7]))

    assert count == 1
    assert connection.args == ("pytdx", None, [7])
    assert "id = any($3::bigint[])" in connection.query


def test_scoped_history_backfill_dry_run_never_opens_database(
    tmp_path, monkeypatch, capsys
) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("000001.SZ\n", encoding="utf-8")

    class _ForbiddenDatabase:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("dry-run must not construct a database client")

    monkeypatch.setattr(history_backfill, "PostgresKlineWriter", _ForbiddenDatabase)
    args = history_backfill.parse_args(
        [
            "--provider", "pytdx",
            "--tdx-host", "127.0.0.1",
            "--symbols-file", str(symbols_file),
            "--timeframes", "5f",
            "--stop-at", "5f=2026-07-10T07:00:00Z",
            "--expected-through", "5f=2026-07-17T07:00:00Z",
            "--freshness-contract-sha256", "a" * 64,
            "--dry-run",
        ]
    )

    asyncio.run(history_backfill.run_once(args))

    output = capsys.readouterr().out
    assert '"history_dry_task"' in output
    assert '"000001.SZ"' in output


def test_scoped_cli_requires_stop_at_and_explicit_endpoint(tmp_path) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("000001.SZ\n", encoding="utf-8")

    with pytest.raises(SystemExit):
        history_backfill.parse_args(
            ["--provider", "pytdx", "--symbols-file", str(symbols_file)]
        )
    with pytest.raises(SystemExit):
        history_backfill.parse_args(
            [
                "--provider", "pytdx", "--symbols-file", str(symbols_file),
                "--timeframes", "5f", "--stop-at", "5f=2026-07-10T07:00:00Z",
            ]
        )

    with pytest.raises(SystemExit):
        history_backfill.parse_args(
            [
                "--provider", "pytdx", "--tdx-host", "127.0.0.1",
                "--symbols-file", str(symbols_file), "--timeframes", "5f",
                "--stop-at", "5f=2026-07-10T07:00:00Z",
            ]
        )

    with pytest.raises(SystemExit):
        history_backfill.parse_args(
            [
                "--provider", "pytdx", "--tdx-host", "127.0.0.1",
                "--symbols-file", str(symbols_file), "--timeframes", "5f",
                "--stop-at", "5f=2026-07-10T07:00:00Z",
                "--expected-through", "5f=2026-07-17T07:00:00Z",
            ]
        )

    args = history_backfill.parse_args(
        [
            "--provider", "pytdx", "--tdx-host", "127.0.0.1",
            "--symbols-file", str(symbols_file), "--timeframes", "5f",
            "--stop-at", "5f=2026-07-10T07:00:00Z",
            "--expected-through", "5f=2026-07-17T07:00:00Z",
            "--freshness-contract-sha256", "a" * 64,
        ]
    )
    assert args.expected_through == "5f=2026-07-17T07:00:00Z"


def test_scoped_run_identity_binds_expected_watermark_and_freshness_contract() -> None:
    common = {
        "provider": "pytdx",
        "manifest_sha256": "b" * 64,
        "symbols": ["000001.SZ"],
        "timeframes": ["5f"],
        "stop_at": {"5f": history_backfill.parse_stop_at(
            "5f=2026-07-10T07:00:00Z", ["5f"], canonical_tail_labels=True
        )["5f"]},
        "page_size": 260,
        "endpoint": "127.0.0.1:7709",
        "source_policy": "primary_failover",
    }
    first = history_backfill.scoped_run_identity(
        **common,
        expected_through={"5f": history_backfill.parse_stop_at(
            "5f=2026-07-17T07:00:00Z", ["5f"], canonical_tail_labels=True
        )["5f"]},
        freshness_contract_sha256="c" * 64,
    )
    changed = history_backfill.scoped_run_identity(
        **common,
        expected_through={"5f": history_backfill.parse_stop_at(
            "5f=2026-07-16T07:00:00Z", ["5f"], canonical_tail_labels=True
        )["5f"]},
        freshness_contract_sha256="d" * 64,
    )
    assert first != changed


def test_scoped_history_backfill_never_upserts_symbol_master(
    tmp_path, monkeypatch
) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("000001.SZ\n", encoding="utf-8")

    class _Writer:
        def __init__(self, *_args):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def upsert_symbols(self, *_args, **_kwargs):
            raise AssertionError("scoped mode must not mutate symbol master")

    class _Store:
        def __init__(self, *_args):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def ensure_scoped_run_tasks(self, **kwargs):
            assert [item.symbol for item in kwargs["symbols"]] == ["000001.SZ"]
            return [101]

        async def claim_tasks(self, **kwargs):
            assert kwargs["task_ids"] == [101]
            assert kwargs["run_id"] is not None
            return []

        async def summarize_scoped_run(self, _run_id):
            return {"success": 1}

    monkeypatch.setattr(history_backfill, "PostgresKlineWriter", _Writer)
    monkeypatch.setattr(history_backfill, "PostgresBackfillTaskStore", _Store)
    args = history_backfill.parse_args(
        [
            "--provider", "pytdx", "--tdx-host", "127.0.0.1",
            "--symbols-file", str(symbols_file), "--timeframes", "5f",
            "--stop-at", "5f=2026-07-10T07:00:00Z",
            "--expected-through", "5f=2026-07-17T07:00:00Z",
            "--freshness-contract-sha256", "a" * 64,
        ]
    )

    asyncio.run(history_backfill.run_once(args))


def test_scoped_zero_claims_with_failed_durable_task_is_not_success(
    tmp_path, monkeypatch
) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("000001.SZ\n", encoding="utf-8")

    class _Context:
        def __init__(self, *_args):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class _Store(_Context):
        async def ensure_scoped_run_tasks(self, **_kwargs):
            return [101]

        async def claim_tasks(self, **_kwargs):
            return []

        async def summarize_scoped_run(self, _run_id):
            return {"dead_letter": 1}

    monkeypatch.setattr(history_backfill, "PostgresKlineWriter", _Context)
    monkeypatch.setattr(history_backfill, "PostgresBackfillTaskStore", _Store)
    args = history_backfill.parse_args(
        [
            "--provider", "pytdx", "--tdx-host", "127.0.0.1",
            "--symbols-file", str(symbols_file), "--timeframes", "5f",
            "--stop-at", "5f=2026-07-10T07:00:00Z",
            "--expected-through", "5f=2026-07-17T07:00:00Z",
            "--freshness-contract-sha256", "a" * 64,
        ]
    )
    with pytest.raises(RuntimeError, match="did not complete cleanly"):
        asyncio.run(history_backfill.run_once(args))


def test_scoped_run_claims_only_concurrency_slots_and_drains_in_batches(
    tmp_path, monkeypatch
) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("000001.SZ\n", encoding="utf-8")
    task_ids = [101, 102, 103]

    class _Context:
        def __init__(self, *_args):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class _Store(_Context):
        claimed_limits = []
        completed = set()

        async def ensure_scoped_run_tasks(self, **_kwargs):
            return task_ids

        async def claim_tasks(self, **kwargs):
            self.claimed_limits.append(kwargs["limit"])
            pending = [task_id for task_id in task_ids if task_id not in self.completed]
            return [{"id": task_id} for task_id in pending[: kwargs["limit"]]]

        async def summarize_scoped_run(self, _run_id):
            return {
                "success": len(self.completed),
                "pending": len(task_ids) - len(self.completed),
            }

    async def _process(**kwargs):
        _Store.completed.update(task["id"] for task in kwargs["tasks"])
        return {"pages": len(kwargs["tasks"]), "bars": 0, "failed": 0, "lease_lost": 0}

    monkeypatch.setattr(history_backfill, "PostgresKlineWriter", _Context)
    monkeypatch.setattr(history_backfill, "PostgresBackfillTaskStore", _Store)
    monkeypatch.setattr(history_backfill, "process_tasks_concurrently", _process)
    args = history_backfill.parse_args(
        [
            "--provider", "pytdx", "--tdx-host", "127.0.0.1",
            "--symbols-file", str(symbols_file), "--timeframes", "5f",
            "--stop-at", "5f=2026-07-10T07:00:00Z",
            "--expected-through", "5f=2026-07-17T07:00:00Z",
            "--freshness-contract-sha256", "a" * 64,
            "--task-limit", "5000", "--concurrency", "1",
        ]
    )
    asyncio.run(history_backfill.run_once(args))
    assert _Store.claimed_limits == [1, 1, 1]
    assert _Store.completed == set(task_ids)


def test_scoped_failed_task_does_not_block_remaining_pending_tasks(
    tmp_path, monkeypatch
) -> None:
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("000001.SZ\n", encoding="utf-8")

    class _Context:
        def __init__(self, *_args):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class _Store(_Context):
        states = {101: "pending", 102: "pending"}
        claims = []

        async def ensure_scoped_run_tasks(self, **_kwargs):
            return [101, 102]

        async def claim_tasks(self, **kwargs):
            candidates = [key for key, value in self.states.items() if value == "pending"]
            selected = candidates[: kwargs["limit"]]
            self.claims.extend(selected)
            return [{"id": task_id} for task_id in selected]

        async def summarize_scoped_run(self, _run_id):
            return {
                status: list(self.states.values()).count(status)
                for status in set(self.states.values())
            }

    async def _process(**kwargs):
        task_id = kwargs["tasks"][0]["id"]
        if task_id == 101:
            _Store.states[task_id] = "dead_letter"
            return {"pages": 0, "bars": 0, "failed": 1, "lease_lost": 0}
        _Store.states[task_id] = "success"
        return {"pages": 1, "bars": 0, "failed": 0, "lease_lost": 0}

    monkeypatch.setattr(history_backfill, "PostgresKlineWriter", _Context)
    monkeypatch.setattr(history_backfill, "PostgresBackfillTaskStore", _Store)
    monkeypatch.setattr(history_backfill, "process_tasks_concurrently", _process)
    args = history_backfill.parse_args([
        "--provider", "pytdx", "--tdx-host", "127.0.0.1",
        "--symbols-file", str(symbols_file), "--timeframes", "5f",
        "--stop-at", "5f=2026-07-10T07:00:00Z",
        "--expected-through", "5f=2026-07-17T07:00:00Z",
        "--freshness-contract-sha256", "a" * 64,
        "--task-limit", "5000", "--concurrency", "1",
    ])
    with pytest.raises(RuntimeError, match="did not complete cleanly"):
        asyncio.run(history_backfill.run_once(args))
    assert _Store.claims == [101, 102]
    assert _Store.states == {101: "dead_letter", 102: "success"}


def test_migration_046_isolates_legacy_workers_and_scoped_identity() -> None:
    migration = (
        Path(__file__).parents[3] / "db/sql/046_history_backfill_scoped_runs.sql"
    ).read_text(encoding="utf-8")

    assert "historical_backfill_scoped_runs" in migration
    assert "where run_id is null" in migration
    assert "where run_id is not null" in migration
    assert "tvchan.history_backfill_scoped_run_id" in migration
    assert "before insert or update or delete" in migration


def test_migration_047_binds_scoped_freshness_evidence() -> None:
    migration = (
        Path(__file__).parents[3]
        / "db/sql/047_history_backfill_scoped_freshness.sql"
    ).read_text(encoding="utf-8")
    assert "expected_through jsonb" in migration
    assert "freshness_contract_sha256 char(64)" in migration
    assert "provider_newest_ts timestamptz" in migration
    assert "old.expected_through is distinct from new.expected_through" in migration
    assert "legacy scoped historical backfill run cannot change task status" in migration
    assert "scoped historical backfill insert requires exact session run fence" in migration
    assert "scoped historical backfill task identity is immutable" in migration
