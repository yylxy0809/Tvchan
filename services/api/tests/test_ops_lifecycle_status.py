from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from app.routes.ops import _lifecycle_observer_status


class FakeConnection:
    def __init__(self, rows: list[dict | None] | None = None, error: Exception | None = None) -> None:
        self.rows = list(rows or [])
        self.error = error
        self.queries: list[str] = []
        self.query_args: list[tuple[object, ...]] = []

    async def fetchrow(self, query: str, *args: object):
        normalized = " ".join(query.lower().split())
        assert normalized.startswith("select")
        self.queries.append(normalized)
        self.query_args.append(args)
        if self.error is not None:
            raise self.error
        return self.rows.pop(0)


class AcquireContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, *_args) -> None:
        return None


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def acquire(self) -> AcquireContext:
        return AcquireContext(self.connection)


class DatabaseError(RuntimeError):
    def __init__(self, message: str, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


def test_lifecycle_status_exposes_backlog_watermark_and_degraded_health() -> None:
    oldest = datetime(2026, 7, 17, 1, 0, tzinfo=UTC)
    watermark_at = datetime(2026, 7, 17, 1, 1, tzinfo=UTC)
    connection = FakeConnection([
        {
            "pending": 2,
            "processing": 1,
            "failed": 3,
            "dead_letter": 4,
            "oldest_backlog_at": oldest,
            "oldest_backlog_age_seconds": 90,
            "max_outbox_id": 42,
        },
        {
            "observer_name": "chan-lifecycle-v1",
            "last_outbox_id": 37,
            "updated_at": watermark_at,
            "heartbeat_age_seconds": 240,
        },
    ])

    status = asyncio.run(
        _lifecycle_observer_status(FakePool(connection), "chan-lifecycle-v1", stale_after_seconds=120)
    )

    assert status == {
        "status": "degraded",
        "reason": "backlog",
        "deployed": True,
        "expected_observer_name": "chan-lifecycle-v1",
        "heartbeat_age_seconds": 240,
        "heartbeat_stale_after_seconds": 120,
        "counts": {"pending": 2, "processing": 1, "failed": 3, "dead_letter": 4},
        "oldest_backlog_at": oldest,
        "oldest_backlog_age_seconds": 90,
        "max_outbox_id": 42,
        "observer_watermark": {
            "observer_name": "chan-lifecycle-v1",
            "last_outbox_id": 37,
            "updated_at": watermark_at,
            "lag": 5,
        },
    }
    assert any("chan_c_head_outbox" in query for query in connection.queries)
    assert any("chan_lifecycle_observer_watermarks" in query for query in connection.queries)
    assert connection.query_args[-1] == ("chan-lifecycle-v1",)
    assert "where observer_name = $1" in connection.queries[-1]


def test_lifecycle_status_treats_empty_deployed_tables_with_fresh_heartbeat_as_healthy() -> None:
    watermark_at = datetime(2026, 7, 17, 1, 1, tzinfo=UTC)
    connection = FakeConnection([
        {
            "pending": 0,
            "processing": 0,
            "failed": 0,
            "dead_letter": 0,
            "oldest_backlog_at": None,
            "oldest_backlog_age_seconds": None,
            "max_outbox_id": 0,
        },
        {
            "observer_name": "chan-lifecycle-v1",
            "last_outbox_id": 0,
            "updated_at": watermark_at,
            "heartbeat_age_seconds": 30,
        },
    ])

    status = asyncio.run(
        _lifecycle_observer_status(FakePool(connection), "chan-lifecycle-v1", stale_after_seconds=120)
    )

    assert status["status"] == "healthy"
    assert status["deployed"] is True
    assert status["expected_observer_name"] == "chan-lifecycle-v1"
    assert status["counts"] == {"pending": 0, "processing": 0, "failed": 0, "dead_letter": 0}
    assert status["heartbeat_age_seconds"] == 30
    assert status["heartbeat_stale_after_seconds"] == 120
    assert status["observer_watermark"]["updated_at"] == watermark_at


def test_lifecycle_status_degrades_when_idle_observer_heartbeat_is_missing() -> None:
    connection = FakeConnection([
        {
            "pending": 0,
            "processing": 0,
            "failed": 0,
            "dead_letter": 0,
            "oldest_backlog_at": None,
            "oldest_backlog_age_seconds": None,
            "max_outbox_id": 0,
        },
        None,
    ])

    status = asyncio.run(
        _lifecycle_observer_status(FakePool(connection), "chan-lifecycle-v1", stale_after_seconds=120)
    )

    assert status["status"] == "degraded"
    assert status["reason"] == "heartbeat_missing"
    assert status["heartbeat_age_seconds"] is None
    assert status["heartbeat_stale_after_seconds"] == 120


def test_lifecycle_status_degrades_when_idle_observer_heartbeat_is_stale() -> None:
    connection = FakeConnection([
        {
            "pending": 0,
            "processing": 0,
            "failed": 0,
            "dead_letter": 0,
            "oldest_backlog_at": None,
            "oldest_backlog_age_seconds": None,
            "max_outbox_id": 0,
        },
        {
            "observer_name": "chan-lifecycle-v1",
            "last_outbox_id": 0,
            "updated_at": datetime(2026, 7, 17, 1, 1, tzinfo=UTC),
            "heartbeat_age_seconds": 121,
        },
    ])

    status = asyncio.run(
        _lifecycle_observer_status(FakePool(connection), "chan-lifecycle-v1", stale_after_seconds=120)
    )

    assert status["status"] == "degraded"
    assert status["reason"] == "heartbeat_stale"
    assert status["heartbeat_age_seconds"] == 121
    assert "clock_timestamp() - updated_at" in connection.queries[-1]


def test_lifecycle_status_reports_watermark_lag_before_stale_heartbeat() -> None:
    connection = FakeConnection([
        {
            "pending": 0,
            "processing": 0,
            "failed": 0,
            "dead_letter": 0,
            "oldest_backlog_at": None,
            "oldest_backlog_age_seconds": None,
            "max_outbox_id": 42,
        },
        {
            "observer_name": "chan-lifecycle-v1",
            "last_outbox_id": 37,
            "updated_at": datetime(2026, 7, 17, 1, 1, tzinfo=UTC),
            "heartbeat_age_seconds": 121,
        },
    ])

    status = asyncio.run(
        _lifecycle_observer_status(FakePool(connection), "chan-lifecycle-v1", stale_after_seconds=120)
    )

    assert status["status"] == "degraded"
    assert status["reason"] == "watermark_lag"
    assert status["heartbeat_age_seconds"] == 121


def test_lifecycle_status_only_maps_missing_tables_to_rolling_deploy_unavailable() -> None:
    connection = FakeConnection(error=DatabaseError('relation "chan_c_head_outbox" does not exist', "42P01"))

    status = asyncio.run(_lifecycle_observer_status(FakePool(connection), "chan-lifecycle-v1"))

    assert status == {
        "status": "unavailable",
        "deployed": False,
        "expected_observer_name": "chan-lifecycle-v1",
        "reason": "schema_not_deployed",
    }


def test_lifecycle_status_keeps_other_database_errors_fail_visible() -> None:
    connection = FakeConnection(error=DatabaseError("permission denied"))

    status = asyncio.run(_lifecycle_observer_status(FakePool(connection), "chan-lifecycle-v1"))

    assert status["status"] == "degraded"
    assert status["deployed"] is True
    assert status["expected_observer_name"] == "chan-lifecycle-v1"
    assert status["reason"] == "query_failed"
    assert status["error"] == "permission denied"


def test_lifecycle_status_ignores_noncanonical_watermarks() -> None:
    connection = FakeConnection([
        {
            "pending": 0,
            "processing": 0,
            "failed": 0,
            "dead_letter": 0,
            "oldest_backlog_at": None,
            "oldest_backlog_age_seconds": None,
            "max_outbox_id": 42,
        },
        None,
    ])

    status = asyncio.run(_lifecycle_observer_status(FakePool(connection), "chan-lifecycle-v1"))

    assert status["status"] == "degraded"
    assert status["expected_observer_name"] == "chan-lifecycle-v1"
    assert status["observer_watermark"] is None
