from __future__ import annotations

import asyncio

import pytest

from collector import lifecycle_observer_worker


class FakeConnection:
    def __init__(self, *, lock_acquired: bool = True) -> None:
        self.lock_acquired = lock_acquired
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetchval(self, sql: str, *args: object) -> bool:
        self.fetchval_calls.append((sql, args))
        if "pg_try_advisory_lock" in sql:
            return self.lock_acquired
        return True

    async def fetch(self, _sql: str) -> list[dict[str, object]]:
        return []

    async def close(self) -> None:
        self.closed = True


class FakeObserver:
    def __init__(self, stop_event: asyncio.Event | None = None) -> None:
        self.stop_event = stop_event
        self.process_calls = 0
        self.process_kwargs: list[dict[str, object]] = []

    async def process_next(self, _connection: object, **kwargs: object) -> int:
        self.process_calls += 1
        self.process_kwargs.append(kwargs)
        if self.stop_event is not None:
            self.stop_event.set()
        return 1

    async def rebuild_current_projection(self, _connection: object) -> None:
        raise AssertionError("projection rebuild must remain explicit")


def test_parse_args_rejects_invalid_runtime_configuration() -> None:
    with pytest.raises(SystemExit):
        lifecycle_observer_worker.parse_args(["--database-url", "", "--loop"])
    with pytest.raises(SystemExit):
        lifecycle_observer_worker.parse_args(["--database-url", "postgresql://db", "--lease-seconds", "0"])
    with pytest.raises(SystemExit):
        lifecycle_observer_worker.parse_args(["--database-url", "postgresql://db", "--max-attempts", "0"])
    with pytest.raises(SystemExit):
        lifecycle_observer_worker.parse_args(["--database-url", "postgresql://db", "--poll-interval", "0"])


def test_legacy_once_and_poll_arguments_delegate_to_canonical_runtime(monkeypatch) -> None:
    monkeypatch.setenv("LIFECYCLE_LOOP", "1")

    args = lifecycle_observer_worker.parse_args([
        "--database-url", "postgresql://db",
        "--once",
        "--poll-seconds", "2.5",
        "--batch-size", "4",
    ])

    assert args.loop is False
    assert args.poll_interval == 2.5


def test_run_observer_holds_singleton_lock_and_stops_gracefully() -> None:
    stop_event = asyncio.Event()
    connection = FakeConnection()
    observer = FakeObserver(stop_event)
    config = lifecycle_observer_worker.parse_args([
        "--database-url", "postgresql://db",
        "--loop",
        "--lease-seconds", "180",
        "--max-attempts", "7",
        "--retry-delay-seconds", "45",
    ])

    processed = asyncio.run(lifecycle_observer_worker.run_observer(
        config,
        stop_event=stop_event,
        connect=lambda _url: asyncio.sleep(0, result=connection),
        observer_factory=lambda **_kwargs: observer,
        emit_status=False,
    ))

    assert processed == 1
    assert observer.process_calls == 1
    assert observer.process_kwargs == [{
        "lease_seconds": 180,
        "max_attempts": 7,
        "retry_delay_seconds": 45,
    }]
    assert any("pg_try_advisory_lock" in sql for sql, _args in connection.fetchval_calls)
    assert any("pg_advisory_unlock" in sql for sql, _args in connection.fetchval_calls)
    assert connection.closed is True


def test_run_observer_rejects_duplicate_startup_before_claiming() -> None:
    connection = FakeConnection(lock_acquired=False)
    observer = FakeObserver()
    config = lifecycle_observer_worker.parse_args([
        "--database-url", "postgresql://db",
        "--loop",
    ])

    with pytest.raises(lifecycle_observer_worker.DuplicateLifecycleObserver):
        asyncio.run(lifecycle_observer_worker.run_observer(
            config,
            connect=lambda _url: asyncio.sleep(0, result=connection),
            observer_factory=lambda **_kwargs: observer,
            emit_status=False,
        ))

    assert observer.process_calls == 0
    assert connection.closed is True
