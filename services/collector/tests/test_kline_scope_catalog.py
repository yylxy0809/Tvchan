from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import re
from uuid import UUID, uuid4

import pytest

from collector import kline_scope_catalog as catalog
from collector.kline_scope_catalog import (
    IncompleteScopeGeneration,
    InvalidScopeGeneration,
    bootstrap_generation_batch,
    create_generation,
    finalize_generation,
    invalidate_scopes,
    parse_args,
    record_empty_scopes,
    record_present_scope,
    record_present_scopes,
    refresh_scopes_exact,
)


ROOT = Path(__file__).resolve().parents[3]


def test_create_generation_serializes_manifest_snapshot_with_catalog_writers() -> None:
    events: list[str] = []
    active_generation_id = uuid4()
    generation_inserts: list[tuple[object, ...]] = []

    class Connection:
        @asynccontextmanager
        async def transaction(self):
            events.append("begin")
            yield
            events.append("commit")

        async def fetchrow(self, sql: str, *args: object):
            normalized = sql.lower()
            if "kline_scope_catalog_control" in normalized:
                assert "for update" in normalized
                events.append("control-lock")
                return {"active_generation_id": active_generation_id, "revision": 7}
            assert "status = 'building'" in normalized
            events.append("building-check")
            return None

        async def fetchval(self, sql: str, *args: object):
            events.append("symbol-snapshot")
            return [7, 8]

        async def execute(self, sql: str, *args: object) -> str:
            events.append("generation" if "catalog_generations" in sql else "manifest")
            if "catalog_generations" in sql:
                generation_inserts.append(args)
            return "INSERT 0 1"

    generation_id = uuid4()
    result = asyncio.run(create_generation(
        Connection(), generation_id=generation_id, timeframes=[5, 30],
    ))

    assert result["expected_scope_count"] == 4
    assert events == [
        "begin", "control-lock", "building-check", "symbol-snapshot",
        "generation", "manifest", "commit",
    ]
    assert generation_inserts == [(
        generation_id, 4, [7, 8], [5, 30], active_generation_id, 7,
    )]


def test_create_generation_rejects_a_second_building_generation() -> None:
    existing_generation_id = uuid4()
    events: list[str] = []

    class Connection:
        @asynccontextmanager
        async def transaction(self):
            events.append("begin")
            yield

        async def fetchrow(self, sql: str, *args: object):
            normalized = sql.lower()
            if "kline_scope_catalog_control" in normalized:
                events.append("control-lock")
                return {"active_generation_id": None, "revision": 0}
            events.append("building-check")
            return {"generation_id": existing_generation_id}

        async def fetchval(self, sql: str, *args: object):
            raise AssertionError("must not snapshot symbols while another generation is building")

        async def execute(self, sql: str, *args: object) -> str:
            raise AssertionError("must not insert a second building generation")

    with pytest.raises(InvalidScopeGeneration, match=str(existing_generation_id)):
        asyncio.run(create_generation(
            Connection(), generation_id=uuid4(), timeframes=[5, 30],
        ))

    assert events == ["begin", "control-lock", "building-check"]


class BootstrapConnection:
    def __init__(self, generation_id: UUID, *, fail_symbol_once: int | None = None) -> None:
        self.generation_id = generation_id
        self.fail_symbol_once = fail_symbol_once
        self.states = {
            (1, 5): {"state": "unknown", "bounds_complete": False, "min_ts": None, "max_ts": None, "updated_at": "v1"},
            (2, 5): {"state": "unknown", "bounds_complete": False, "min_ts": None, "max_ts": None, "updated_at": "v1"},
        }

    @asynccontextmanager
    async def transaction(self):
        yield

    async def fetch(self, sql: str, generation_id: UUID, limit: int):
        assert "not bounds_complete" in sql.lower()
        assert generation_id == self.generation_id
        return [
            {"symbol_id": symbol_id, "timeframe": timeframe, "state": row["state"], "updated_at": row["updated_at"]}
            for (symbol_id, timeframe), row in self.states.items()
            if not row["bounds_complete"]
        ][:limit]

    async def fetchrow(self, sql: str, *args: object):
        if "kline_scope_catalog_control" in sql.lower():
            return {"control_key": "active"}
        symbol_id, timeframe = args
        assert "from klines" in sql.lower()
        if self.fail_symbol_once == symbol_id:
            self.fail_symbol_once = None
            raise RuntimeError("interrupted scan")
        if symbol_id == 1:
            return {"bar_count": 3, "min_ts": "2026-01-01", "max_ts": "2026-01-03"}
        return {"bar_count": 0, "min_ts": None, "max_ts": None}

    async def execute(self, sql: str, *args: object) -> str:
        generation_id, symbol_id, timeframe, state, min_ts, max_ts = args[:6]
        row = self.states[(symbol_id, timeframe)]
        if "state = 'unknown'" in sql.lower():
            matches = (
                row["state"] == "unknown"
                and not row["bounds_complete"]
                and row["updated_at"] == args[6]
            )
        else:
            matches = (
                row["state"] == "present"
                and not row["bounds_complete"]
                and row["updated_at"] == args[6]
            )
        if not matches:
            return "UPDATE 0"
        row.update(state=state, bounds_complete=True, min_ts=min_ts, max_ts=max_ts, updated_at="scan")
        return "UPDATE 1"


def test_bootstrap_is_resumable_after_an_interrupted_short_batch() -> None:
    generation_id = uuid4()
    connection = BootstrapConnection(generation_id, fail_symbol_once=2)

    with pytest.raises(RuntimeError, match="interrupted scan"):
        asyncio.run(bootstrap_generation_batch(connection, generation_id=generation_id, batch_size=2))

    assert connection.states[(1, 5)]["state"] == "present"
    assert connection.states[(2, 5)]["state"] == "unknown"

    result = asyncio.run(bootstrap_generation_batch(connection, generation_id=generation_id, batch_size=2))
    assert result == {"selected": 1, "updated": 1, "cas_skipped": 0}
    assert connection.states[(2, 5)]["state"] == "empty"


def test_bootstrap_cas_never_overwrites_a_concurrent_present_scope() -> None:
    generation_id = uuid4()
    connection = BootstrapConnection(generation_id)

    original_fetchrow = connection.fetchrow

    async def concurrent_write(sql: str, *args: object):
        result = await original_fetchrow(sql, *args)
        if not args:
            return result
        symbol_id, timeframe = args
        connection.states[(symbol_id, timeframe)].update(
            state="present", bounds_complete=False,
            min_ts="writer-min", max_ts="writer-max", updated_at="writer-v2"
        )
        return result

    connection.fetchrow = concurrent_write  # type: ignore[method-assign]
    result = asyncio.run(bootstrap_generation_batch(connection, generation_id=generation_id, batch_size=1))

    assert result == {"selected": 1, "updated": 0, "cas_skipped": 1}
    assert connection.states[(1, 5)] == {
        "state": "present", "bounds_complete": False,
        "min_ts": "writer-min", "max_ts": "writer-max", "updated_at": "writer-v2",
    }

    connection.fetchrow = original_fetchrow  # type: ignore[method-assign]
    resumed = asyncio.run(bootstrap_generation_batch(
        connection, generation_id=generation_id, batch_size=1,
    ))
    assert resumed == {"selected": 1, "updated": 1, "cas_skipped": 0}
    assert connection.states[(1, 5)]["bounds_complete"] is True


def test_writer_hook_marks_unknown_present_without_claiming_complete_bounds() -> None:
    calls: list[tuple[str, tuple[object, ...]]] = []
    locks: list[str] = []

    class Connection:
        async def fetchrow(self, sql: str, *args: object):
            assert "for share" in sql.lower()
            locks.append("control")
            return {"control_key": "active"}

        async def execute(self, sql: str, *args: object) -> str:
            calls.append((sql, args))
            return "UPDATE 1"

    assert asyncio.run(record_present_scope(
        Connection(), symbol_id=7, timeframe=5, timestamp="2026-01-01",
    )) == 1
    sql = calls[0][0].lower()
    assert locks == ["control"]
    assert "when catalog.state = 'unknown' then false" in sql
    assert "generation.status in ('building', 'complete')" in sql


def test_bulk_present_hook_merges_duplicate_ranges_without_claiming_complete() -> None:
    calls: list[tuple[str, tuple[object, ...]]] = []

    class Connection:
        async def fetchrow(self, sql: str, *args: object):
            assert "for share" in sql.lower()
            return {"control_key": "active"}

        async def execute(self, sql: str, *args: object) -> str:
            calls.append((sql, args))
            return "UPDATE 3"

    updated = asyncio.run(record_present_scopes(Connection(), scopes=[
        (7, 5, 3, 5), (7, 5, 1, 4), (8, 30, 10, 12),
    ]))

    assert updated == 3
    _sql, args = calls[0]
    assert args == ([7, 8], [5, 30], [1, 10], [5, 12])


class ExactConnection:
    def __init__(self) -> None:
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.control_locks = 0

    async def fetchrow(self, sql: str, *args: object):
        assert "for share" in sql.lower()
        self.control_locks += 1
        return {"control_key": "active"}

    async def fetch(self, sql: str, *args: object):
        self.fetch_calls.append((sql, args))
        if "from kline_scope_catalog catalog" in sql.lower():
            return [
                {"generation_id": uuid4(), "symbol_id": 7, "timeframe": 5, "updated_at": "v1"},
                {"generation_id": uuid4(), "symbol_id": 8, "timeframe": 30, "updated_at": "v2"},
            ]
        if "left join klines" in sql.lower():
            return [
                {"symbol_id": 7, "timeframe": 5, "bar_count": 2, "min_ts": "lo", "max_ts": "hi"},
                {"symbol_id": 8, "timeframe": 30, "bar_count": 0, "min_ts": None, "max_ts": None},
            ]
        raise AssertionError(sql)

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return "UPDATE 2"


def test_record_empty_scopes_is_bulk_version_fenced_and_never_inserts() -> None:
    connection = ExactConnection()

    updated = asyncio.run(record_empty_scopes(
        connection, scopes=[(7, 5), (8, 30)],
    ))

    assert updated == 2
    assert connection.control_locks == 1
    assert len(connection.fetch_calls) == 1
    sql, args = connection.execute_calls[0]
    assert "catalog.updated_at = target.selected_version" in sql.lower()
    assert "insert" not in sql.lower()
    assert args[3] == ["empty", "empty"]
    assert args[4] == [None, None]
    assert args[5] == [None, None]


def test_record_empty_scopes_invalidates_when_a_concurrent_version_wins() -> None:
    connection = ExactConnection()

    async def execute(sql: str, *args: object) -> str:
        connection.execute_calls.append((sql, args))
        return "UPDATE 1" if "selected_version" in sql.lower() else "UPDATE 4"

    connection.execute = execute  # type: ignore[method-assign]

    updated = asyncio.run(record_empty_scopes(
        connection, scopes=[(7, 5), (8, 30)],
    ))

    assert updated == 1
    assert len(connection.execute_calls) == 2
    assert "set state = 'unknown'" in connection.execute_calls[1][0].lower()


def test_refresh_scopes_exact_reads_klines_once_and_cas_records_present_and_empty() -> None:
    connection = ExactConnection()

    result = asyncio.run(refresh_scopes_exact(
        connection, scopes=[(7, 5), (8, 30)],
    ))

    assert result == {"catalog_rows": 2, "updated": 2, "cas_skipped": 0}
    assert len(connection.fetch_calls) == 2
    assert "left join klines" in connection.fetch_calls[1][0].lower()
    _sql, args = connection.execute_calls[0]
    assert args[3] == ["present", "empty"]
    assert args[4] == ["lo", None]
    assert args[5] == ["hi", None]


def test_refresh_scopes_exact_does_not_probe_klines_without_catalog_membership() -> None:
    class Connection:
        def __init__(self) -> None:
            self.fetch_calls = 0

        async def fetch(self, sql: str, *args: object):
            self.fetch_calls += 1
            assert "from kline_scope_catalog catalog" in sql.lower()
            return []

        async def fetchrow(self, sql: str, *args: object):
            assert "for share" in sql.lower()
            return {"control_key": "active"}

    connection = Connection()

    result = asyncio.run(refresh_scopes_exact(connection, scopes=[(7, 5)]))

    assert result == {"catalog_rows": 0, "updated": 0, "cas_skipped": 0}
    assert connection.fetch_calls == 1


def test_refresh_scopes_exact_invalidates_when_a_concurrent_version_wins() -> None:
    connection = ExactConnection()

    async def execute(sql: str, *args: object) -> str:
        connection.execute_calls.append((sql, args))
        if "selected_version" in sql.lower():
            return "UPDATE 1"
        assert "set state = 'unknown'" in sql.lower()
        return "UPDATE 4"

    connection.execute = execute  # type: ignore[method-assign]

    result = asyncio.run(refresh_scopes_exact(
        connection, scopes=[(7, 5), (8, 30)],
    ))

    assert result == {"catalog_rows": 2, "updated": 1, "cas_skipped": 1}
    assert len(connection.execute_calls) == 2
    invalidation_sql, invalidation_args = connection.execute_calls[1]
    assert "bounds_complete = false" in invalidation_sql.lower()
    assert invalidation_args == ([7, 8], [5, 30])


def test_cli_bootstrap_requires_explicit_opt_in() -> None:
    args = parse_args(["--database-url", "postgresql://catalog"])
    assert args.create_generation is False
    assert args.bootstrap is False
    assert args.finalize is False
    assert args.fail is False


class ManagementSnapshotConnection:
    def __init__(
        self, *, active_generation_id: UUID | None,
        building_generation_id: UUID | None,
        control_revision: int = 7,
    ) -> None:
        self.active_generation_id = active_generation_id
        self.building_generation_id = building_generation_id
        self.control_revision = control_revision
        self.closed = False
        self.queries: list[str] = []
        self.readonly_snapshot = False

    @asynccontextmanager
    async def transaction(self, *, isolation: str, readonly: bool):
        assert isolation == "repeatable_read"
        assert readonly is True
        self.readonly_snapshot = True
        yield

    async def fetchrow(self, sql: str, *args: object):
        normalized = " ".join(sql.lower().split())
        assert normalized.startswith("select")
        self.queries.append(normalized)
        if "from kline_scope_catalog_control" in normalized:
            return {
                "active_generation_id": self.active_generation_id,
                "revision": self.control_revision,
            }
        if "status = 'building'" in normalized and "limit 1" in normalized:
            if self.building_generation_id is None:
                return None
            return {"generation_id": self.building_generation_id}
        if "from kline_scope_catalog_generations" in normalized:
            assert "base_active_generation_id" in normalized
            assert "base_control_revision" in normalized
            generation_id = args[0]
            is_building = generation_id == self.building_generation_id
            return {
                "generation_id": generation_id,
                "status": "building" if is_building else "complete",
                "expected_scope_count": 14,
                "base_active_generation_id": self.active_generation_id,
                "base_control_revision": self.control_revision,
                "created_at": "2026-07-17T00:00:00Z",
                "completed_at": None if is_building else "2026-07-17T00:30:00Z",
                "failed_at": None,
                "failure": None,
            }
        if "count(*)::bigint as scope_count" in normalized:
            return {
                "scope_count": 11,
                "unknown_count": 2,
                "incomplete_count": 3,
            }
        raise AssertionError(sql)

    async def fetchval(self, sql: str, *args: object):
        # Retained only so the pre-snapshot implementation fails on assertions,
        # rather than because the test double lacks its legacy query method.
        assert sql.lower().lstrip().startswith("select")
        self.queries.append(" ".join(sql.lower().split()))
        return self.active_generation_id

    async def execute(self, sql: str, *args: object) -> str:
        raise AssertionError(f"management snapshot must be read-only: {sql}")

    async def close(self) -> None:
        self.closed = True


def _run_management_snapshot(
    monkeypatch: pytest.MonkeyPatch, connection: ManagementSnapshotConnection,
) -> dict[str, object]:
    async def connect(_database_url: str) -> ManagementSnapshotConnection:
        return connection

    monkeypatch.setattr(catalog.asyncpg, "connect", connect)
    return asyncio.run(catalog.run(parse_args(["--database-url", "postgresql://catalog"])))


def test_management_snapshot_discovers_building_generation_without_active_pointer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    building_generation_id = uuid4()
    connection = ManagementSnapshotConnection(
        active_generation_id=None,
        building_generation_id=building_generation_id,
        control_revision=0,
    )

    result = _run_management_snapshot(monkeypatch, connection)

    assert result["active_generation_id"] is None
    assert result["control_revision"] == 0
    assert result["building_generation"] == {
        "generation_id": str(building_generation_id),
        "status": "building",
        "expected_scope_count": 14,
        "base_active_generation_id": None,
        "base_control_revision": 0,
        "created_at": "2026-07-17T00:00:00Z",
        "completed_at": None,
        "failed_at": None,
        "failure": None,
        "scope_count": 11,
        "unknown_count": 2,
        "incomplete_count": 3,
    }
    assert connection.closed is True
    assert connection.readonly_snapshot is True
    assert connection.queries and all(query.startswith("select") for query in connection.queries)


def test_management_snapshot_preserves_active_report_and_exposes_building_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_generation_id = uuid4()
    building_generation_id = uuid4()
    connection = ManagementSnapshotConnection(
        active_generation_id=active_generation_id,
        building_generation_id=building_generation_id,
    )

    result = _run_management_snapshot(monkeypatch, connection)

    assert result["generation_id"] == str(active_generation_id)
    assert result["status"] == "complete"
    assert result["control_revision"] == 7
    building = result["building_generation"]
    assert isinstance(building, dict)
    assert building["generation_id"] == str(building_generation_id)
    assert building["base_active_generation_id"] == str(active_generation_id)
    assert building["base_control_revision"] == 7
    assert building["scope_count"] == 11
    assert building["unknown_count"] == 2
    assert building["incomplete_count"] == 3
    assert connection.closed is True


def test_management_snapshot_reports_no_building_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_generation_id = uuid4()
    connection = ManagementSnapshotConnection(
        active_generation_id=active_generation_id,
        building_generation_id=None,
    )

    result = _run_management_snapshot(monkeypatch, connection)

    assert result["generation_id"] == str(active_generation_id)
    assert result["building_generation"] is None
    assert connection.closed is True


def test_management_snapshot_closes_connection_when_a_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingConnection(ManagementSnapshotConnection):
        async def fetchrow(self, sql: str, *args: object):
            assert sql.lower().lstrip().startswith("select")
            raise RuntimeError("catalog read failed")

    connection = FailingConnection(
        active_generation_id=None,
        building_generation_id=None,
    )

    with pytest.raises(RuntimeError, match="catalog read failed"):
        _run_management_snapshot(monkeypatch, connection)

    assert connection.closed is True


def test_delete_hook_invalidates_existing_scopes_in_one_statement_without_inserting() -> None:
    calls: list[tuple[str, tuple[object, ...]]] = []
    locks: list[str] = []

    class Connection:
        async def fetchrow(self, sql: str, *args: object):
            assert "for share" in sql.lower()
            locks.append("control")
            return {"control_key": "active"}

        async def execute(self, sql: str, *args: object) -> str:
            calls.append((sql, args))
            return "UPDATE 4"

    updated = asyncio.run(invalidate_scopes(
        Connection(), scopes=[(7, 5), (8, 30), (7, 5)],
    ))

    assert updated == 4
    assert len(calls) == 1
    sql, args = calls[0]
    normalized = sql.lower()
    assert "update kline_scope_catalog" in normalized
    assert locks == ["control"]
    assert "set state = 'unknown'" in normalized
    assert "bounds_complete = false" in normalized
    assert "insert" not in normalized
    assert args == ([7, 8], [5, 30])


_UNSET = object()


class FinalizeConnection:
    def __init__(
        self, *, status: str, expected: int, scopes: int, unknown: int, incomplete: int,
        active_generation_id: UUID | None = None,
        control_revision: int = 0,
        base_active_generation_id: UUID | None = None,
        base_control_revision: int | None | object = _UNSET,
    ) -> None:
        self.status = status
        self.expected = expected
        self.scopes = scopes
        self.unknown = unknown
        self.incomplete = incomplete
        self.active_generation_id = active_generation_id
        self.control_revision = control_revision
        self.base_active_generation_id = base_active_generation_id
        self.base_control_revision = (
            control_revision if base_control_revision is _UNSET else base_control_revision
        )
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.lock_order: list[str] = []

    @asynccontextmanager
    async def transaction(self):
        yield

    async def fetchrow(self, sql: str, *args: object):
        normalized = " ".join(sql.lower().split())
        if "from kline_scope_catalog_generations" in normalized and "for update" in normalized:
            self.lock_order.append("generation")
            return {
                "generation_id": args[0], "status": self.status,
                "expected_scope_count": self.expected,
                "base_active_generation_id": self.base_active_generation_id,
                "base_control_revision": self.base_control_revision,
            }
        if "count(*)::bigint as scope_count" in normalized:
            self.lock_order.append("summary")
            return {
                "scope_count": self.scopes,
                "unknown_count": self.unknown,
                "incomplete_count": self.incomplete,
            }
        if "from kline_scope_catalog_control" in normalized:
            self.lock_order.append("control")
            return {
                "active_generation_id": self.active_generation_id,
                "revision": self.control_revision,
            }
        raise AssertionError(sql)

    async def execute(self, sql: str, *args: object) -> str:
        if "lock table kline_scope_catalog in share mode" in sql.lower():
            self.lock_order.append("catalog")
        self.executed.append((sql, args))
        if "update kline_scope_catalog_control" in sql.lower():
            self.active_generation_id = args[0]  # type: ignore[assignment]
            self.control_revision += 1
        return "UPDATE 1"


def test_incomplete_generation_cannot_be_finalized() -> None:
    connection = FinalizeConnection(status="building", expected=2, scopes=2, unknown=1, incomplete=1)

    with pytest.raises(IncompleteScopeGeneration):
        asyncio.run(finalize_generation(connection, generation_id=uuid4()))

    assert not any("kline_scope_catalog_control" in sql for sql, _args in connection.executed)


def test_failed_generation_never_switches_the_active_pointer() -> None:
    connection = FinalizeConnection(status="failed", expected=2, scopes=2, unknown=0, incomplete=0)

    with pytest.raises(InvalidScopeGeneration, match="failed"):
        asyncio.run(finalize_generation(connection, generation_id=uuid4()))

    assert connection.executed == []


def test_complete_generation_switches_pointer_and_supersedes_old_active_atomically() -> None:
    previous_generation_id = uuid4()
    connection = FinalizeConnection(
        status="building", expected=2, scopes=2, unknown=0, incomplete=0,
        active_generation_id=previous_generation_id,
        control_revision=7,
        base_active_generation_id=previous_generation_id,
        base_control_revision=7,
    )
    generation_id = uuid4()

    result = asyncio.run(finalize_generation(connection, generation_id=generation_id))

    assert result["generation_id"] == str(generation_id)
    assert result["status"] == "complete"
    statements = [" ".join(sql.lower().split()) for sql, _args in connection.executed]
    assert any("set status = 'complete'" in sql for sql in statements)
    assert any("update kline_scope_catalog_control" in sql for sql in statements)
    control_sql, control_args = next(
        (sql, args) for sql, args in connection.executed
        if "update kline_scope_catalog_control" in sql.lower()
    )
    assert "revision = revision + 1" in " ".join(control_sql.lower().split())
    assert control_args == (generation_id, 7, previous_generation_id)
    assert connection.control_revision == 8
    assert any("set status = 'superseded'" in sql for sql in statements)
    assert connection.lock_order == ["control", "generation", "summary"]


def test_first_generation_with_null_base_activates_at_revision_zero() -> None:
    connection = FinalizeConnection(
        status="building", expected=2, scopes=2, unknown=0, incomplete=0,
        active_generation_id=None, control_revision=0,
        base_active_generation_id=None, base_control_revision=0,
    )
    generation_id = uuid4()

    result = asyncio.run(finalize_generation(connection, generation_id=generation_id))

    assert result["status"] == "complete"
    control_sql, control_args = next(
        (sql, args) for sql, args in connection.executed
        if "update kline_scope_catalog_control" in sql.lower()
    )
    assert "active_generation_id is not distinct from $3" in " ".join(control_sql.lower().split())
    assert control_args == (generation_id, 0, None)
    assert connection.control_revision == 1


def test_pointer_clear_rejects_stale_building_generation() -> None:
    base_active_generation_id = uuid4()
    connection = FinalizeConnection(
        status="building", expected=2, scopes=2, unknown=0, incomplete=0,
        active_generation_id=None, control_revision=8,
        base_active_generation_id=base_active_generation_id, base_control_revision=7,
    )

    with pytest.raises(InvalidScopeGeneration, match="base active generation"):
        asyncio.run(finalize_generation(connection, generation_id=uuid4()))

    assert connection.lock_order == ["control", "generation"]
    assert connection.executed == []


def test_control_revision_rejects_aba_pointer_reuse() -> None:
    active_generation_id = uuid4()
    connection = FinalizeConnection(
        status="building", expected=2, scopes=2, unknown=0, incomplete=0,
        active_generation_id=active_generation_id, control_revision=9,
        base_active_generation_id=active_generation_id, base_control_revision=7,
    )

    with pytest.raises(InvalidScopeGeneration, match="control revision"):
        asyncio.run(finalize_generation(connection, generation_id=uuid4()))

    assert connection.executed == []


def test_finalize_retry_is_idempotent_only_for_the_active_complete_generation() -> None:
    generation_id = uuid4()
    connection = FinalizeConnection(
        status="complete", expected=2, scopes=2, unknown=0, incomplete=0,
        active_generation_id=generation_id,
        base_control_revision=None,
    )

    result = asyncio.run(finalize_generation(connection, generation_id=generation_id))

    assert result["idempotent"] is True
    assert connection.executed == []


def test_legacy_building_generation_without_base_revision_cannot_finalize() -> None:
    connection = FinalizeConnection(
        status="building", expected=2, scopes=2, unknown=0, incomplete=0,
        active_generation_id=None, control_revision=0,
        base_active_generation_id=None, base_control_revision=None,
    )

    with pytest.raises(InvalidScopeGeneration, match="no base control revision"):
        asyncio.run(finalize_generation(connection, generation_id=uuid4()))

    assert connection.executed == []


def test_migration_enforces_scope_state_bounds_and_safe_active_view() -> None:
    migration = (ROOT / "db" / "sql" / "040_kline_scope_catalog.sql").read_text(encoding="utf-8").lower()

    assert "create table if not exists kline_scope_catalog_generations" in migration
    assert "create table if not exists kline_scope_catalog" in migration
    assert "create table if not exists kline_scope_catalog_control" in migration
    assert "state in ('unknown', 'present', 'empty')" in migration
    assert "state = 'unknown'" in migration and "not bounds_complete" in migration
    assert "state = 'present'" in migration and "min_ts is not null" in migration
    assert "state = 'present'" in migration and "not bounds_complete" in migration
    assert "state = 'empty'" in migration and "min_ts is null" in migration
    assert "symbol_ids integer[] not null" in migration
    assert "timeframes integer[] not null" in migration
    assert "expected_scope_count = cardinality(symbol_ids)::bigint * cardinality(timeframes)::bigint" in migration
    assert "create or replace view active_kline_scope_catalog" in migration
    assert "generation.status = 'complete'" in migration
    assert "create index" not in migration
    assert "create trigger" not in migration
    for relation in (
        "kline_scope_catalog_generations", "kline_scope_catalog",
        "kline_scope_catalog_control", "active_kline_scope_catalog",
    ):
        assert f"revoke all on table {relation} from public" in migration
    forbidden_kline_mutation = (
        r"(?:insert\s+into|update|delete\s+from|alter\s+table|drop\s+table)\s+klines\b",
        r"create\s+(?:unique\s+)?index[\s\S]*?\bon\s+klines\b",
        r"create\s+trigger[\s\S]*?\bon\s+klines\b",
    )
    assert not any(re.search(pattern, migration) for pattern in forbidden_kline_mutation)


def test_migration_041_rejects_conflicts_and_enforces_one_building_generation() -> None:
    migration = (
        ROOT / "db" / "sql" / "041_kline_scope_catalog_generation_fencing.sql"
    ).read_text(encoding="utf-8").lower()

    assert "exists" in migration and "status = 'building'" in migration
    assert "base_control_revision is null" in migration
    assert "raise exception" in migration
    assert "add column if not exists revision bigint not null default 0" in migration
    assert "add column if not exists base_active_generation_id uuid" in migration
    assert "add column if not exists base_control_revision bigint" in migration
    assert "foreign key (base_active_generation_id)" in migration
    assert "check (status <> 'building' or base_control_revision is not null)" in migration
    assert "create or replace function enforce_kline_scope_catalog_control_revision()" in migration
    assert "new.active_generation_id is distinct from old.active_generation_id" in migration
    assert "new.revision <> old.revision + 1" in migration
    assert "new.revision <> old.revision" in migration
    assert "drop trigger if exists trg_kline_scope_catalog_control_revision" in migration
    assert "before update of active_generation_id, revision" in migration
    assert "execute function enforce_kline_scope_catalog_control_revision()" in migration
    assert (
        "revoke all on function enforce_kline_scope_catalog_control_revision() from public"
        in migration
    )
    assert re.search(
        r"create\s+unique\s+index\s+if\s+not\s+exists\s+"
        r"uq_kline_scope_catalog_one_building_generation\s+"
        r"on\s+kline_scope_catalog_generations\s*\(\s*status\s*\)\s+"
        r"where\s+status\s*=\s*'building'",
        migration,
    )
    assert "update kline_scope_catalog_generations" not in migration
    assert "delete from kline_scope_catalog_generations" not in migration
    forbidden_kline_mutation = (
        r"(?:insert\s+into|update|delete\s+from|alter\s+table|drop\s+table)\s+klines\b",
        r"create\s+(?:unique\s+)?index[\s\S]*?\bon\s+klines\b",
        r"create\s+trigger[\s\S]*?\bon\s+klines\b",
    )
    assert not any(re.search(pattern, migration) for pattern in forbidden_kline_mutation)
