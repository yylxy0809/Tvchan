from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from uuid import uuid4

import pytest

from collector.kline_import_quarantine import ImportCheckpoint, QuarantineRecord, commit_import_batch


class _Transaction(AbstractAsyncContextManager):
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self.snapshot = None

    async def __aenter__(self):
        self.snapshot = (list(self.connection.accepted), list(self.connection.quarantines), list(self.connection.checkpoints))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type:
            self.connection.accepted, self.connection.quarantines, self.connection.checkpoints = self.snapshot
        return False


class FakeConnection:
    def __init__(self) -> None:
        self.accepted: list[str] = []
        self.quarantines: list[tuple] = []
        self.checkpoints: list[tuple] = []
        self.events: list[str] = []
        self.fail_checkpoint = False

    def transaction(self):
        return _Transaction(self)

    async def executemany(self, sql, rows):
        self.events.append("quarantine")
        for row in rows:
            key = (row[1], row[2], row[3], row[7])
            if not any((item[1], item[2], item[3], item[7]) == key for item in self.quarantines):
                self.quarantines.append(row)

    async def execute(self, sql, *args):
        if "kline_import_checkpoints" in sql:
            self.events.append("checkpoint")
            if self.fail_checkpoint:
                raise RuntimeError("simulated crash before commit")
            self.checkpoints.append(args)


def _record() -> QuarantineRecord:
    return QuarantineRecord(
        source_name="parquet_native",
        source_ref="30m_price/2024.zip!20240102.parquet#crc32=deadbeef",
        source_row=7,
        symbol_text="000001.SZ",
        timeframe="30f",
        raw_ts="2024-01-02 09:31:00",
        reason="invalid_session_label",
        raw_payload={"trade_time": "2024-01-02 09:31:00"},
    )


def test_crash_before_commit_leaves_no_accepted_rows_quarantine_or_checkpoint() -> None:
    conn = FakeConnection()
    conn.fail_checkpoint = True

    async def write_accepted(_conn):
        conn.events.append("accepted")
        conn.accepted.append("canonical-row")
        return 1

    with pytest.raises(RuntimeError, match="simulated crash"):
        asyncio.run(
            commit_import_batch(
                conn,
                import_run_id=uuid4(),
                checkpoint=ImportCheckpoint(_record().source_ref, "crc32=deadbeef", 7),
                quarantines=[_record()],
                write_accepted=write_accepted,
            )
        )
    assert conn.accepted == []
    assert conn.quarantines == []
    assert conn.checkpoints == []


def test_retry_is_idempotent_for_stable_quarantine_identity_and_advances_checkpoint_last() -> None:
    conn = FakeConnection()
    run_id = uuid4()

    async def write_accepted(_conn):
        conn.events.append("accepted")
        if "canonical-row" not in conn.accepted:  # mirrors canonical upsert identity
            conn.accepted.append("canonical-row")
        return 1

    for _ in range(2):
        assert asyncio.run(
            commit_import_batch(
                conn,
                import_run_id=run_id,
                checkpoint=ImportCheckpoint(_record().source_ref, "crc32=deadbeef", 7),
                quarantines=[_record()],
                write_accepted=write_accepted,
            )
        ) == 1
    assert len(conn.quarantines) == 1
    assert conn.accepted == ["canonical-row"]
    assert len(conn.checkpoints) == 2
    assert conn.checkpoints[-1][5] == 7
    assert conn.events[:3] == ["accepted", "quarantine", "checkpoint"]


def test_retryable_deadlock_retries_the_entire_atomic_batch() -> None:
    class Deadlock(Exception):
        sqlstate = "40P01"

    conn = FakeConnection()
    attempts = 0

    async def write_accepted(_conn):
        nonlocal attempts
        attempts += 1
        conn.events.append("accepted")
        conn.accepted.append("canonical-row")
        if attempts == 1:
            raise Deadlock("simulated deadlock")
        return 1

    assert asyncio.run(
        commit_import_batch(
            conn,
            import_run_id=uuid4(),
            checkpoint=ImportCheckpoint(_record().source_ref, "crc32=deadbeef", 7),
            quarantines=[_record()],
            write_accepted=write_accepted,
            retry_attempts=2,
            retry_base_delay_seconds=0,
        )
    ) == 1
    assert attempts == 2
    assert conn.accepted == ["canonical-row"]
    assert len(conn.quarantines) == 1
    assert len(conn.checkpoints) == 1


def test_non_retryable_error_does_not_retry() -> None:
    conn = FakeConnection()
    attempts = 0

    async def write_accepted(_conn):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("bad row")

    with pytest.raises(RuntimeError, match="bad row"):
        asyncio.run(
            commit_import_batch(
                conn,
                import_run_id=uuid4(),
                checkpoint=ImportCheckpoint(_record().source_ref, "crc32=deadbeef", 7),
                quarantines=[],
                write_accepted=write_accepted,
                retry_attempts=5,
                retry_base_delay_seconds=0,
            )
        )
    assert attempts == 1
