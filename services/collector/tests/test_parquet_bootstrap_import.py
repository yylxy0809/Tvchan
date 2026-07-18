from __future__ import annotations

import asyncio
import threading
import zipfile
from datetime import datetime
from pathlib import Path

import pytest

from collector import parquet_bootstrap_import as import_module
from collector.storage.scheme2_postgres import LostScheme2MemberLease


def test_discover_parquet_members_records_zip_member_identity(tmp_path: Path) -> None:
    zip_path = tmp_path / "2024.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("20240102.parquet", b"parquet-bytes")
        archive.writestr("README.txt", b"ignored")

    members = import_module.discover_parquet_members(tmp_path)

    assert len(members) == 1
    assert members[0].root_path == str(tmp_path.resolve())
    assert members[0].source_profile == "parquet_5f"
    assert members[0].zip_path == str(zip_path.resolve())
    assert members[0].member_path == "20240102.parquet"
    assert members[0].member_size_bytes == len(b"parquet-bytes")
    assert members[0].content_sha256 is None
    assert members[0].timeframe == 5


def test_discover_direct_parquet_uses_content_sha256_for_same_size_replacement(
    tmp_path: Path,
) -> None:
    symbols = tmp_path / "symbols"
    symbols.mkdir()
    parquet_path = symbols / "000001.parquet"
    parquet_path.write_bytes(b"direct-content-a")

    first = import_module.discover_parquet_members(tmp_path)[0]
    parquet_path.write_bytes(b"direct-content-b")
    second = import_module.discover_parquet_members(tmp_path)[0]

    assert first.member_size_bytes == second.member_size_bytes
    assert first.content_sha256 != second.content_sha256
    assert len(first.content_sha256 or "") == 64
    assert len(second.content_sha256 or "") == 64


def test_parse_parquet_rows_keeps_trade_time_as_bar_end() -> None:
    trade_time = datetime(2024, 1, 2, 9, 35, 0)

    parsed = import_module.parse_parquet_rows(
        [
            {
                "code": "000001",
                "trade_time": trade_time,
                "open": 10.12,
                "high": 10.5,
                "low": 10.0,
                "close": 10.3,
                "vol": 12345,
                "amount": 987654.32,
            }
        ]
    )

    assert list(parsed.symbols) == ["000001.SZ"]
    assert len(parsed.bars) == 1
    bar = parsed.bars[0]
    assert bar.symbol == "000001.SZ"
    assert bar.timeframe == "5f"
    assert bar.ts.hour == 9
    assert bar.ts.minute == 35
    assert bar.ts.tzinfo == import_module.SHANGHAI_TZ
    assert bar.source == "parquet_5f"


def test_parse_args_validates_durable_owner_configuration() -> None:
    args = import_module.parse_args([
        "--lease-seconds", "90",
        "--max-attempts", "7",
        "--max-batches-per-member", "3",
        "--worker-id", "scheme2-worker-a",
    ])

    assert args.lease_seconds == 90
    assert args.max_attempts == 7
    assert args.max_batches_per_member == 3
    assert args.worker_id == "scheme2-worker-a"

    with pytest.raises(SystemExit):
        import_module.parse_args(["--lease-seconds", "0"])
    with pytest.raises(SystemExit):
        import_module.parse_args(["--max-attempts", "0"])
    with pytest.raises(SystemExit):
        import_module.parse_args(["--worker-id", "x" * 161])


def test_process_member_task_writes_member_and_records_success(monkeypatch) -> None:
    row = {
        "code": "600519",
        "trade_time": "2024-01-02 09:35:00",
        "open": 100.1,
        "high": 101.2,
        "low": 99.9,
        "close": 100.8,
        "vol": 1200,
        "amount": 1_234_567.89,
    }

    monkeypatch.setattr(
        import_module,
        "_iter_member_rows",
        lambda zip_path, member_path, **_kwargs: iter([[row]]),
    )

    class FakeWriter:
        def __init__(self) -> None:
            self.symbols = []
            self.bars = []

        async def commit_member_batch(self, *, symbols, bars, **_kwargs):
            self.symbols.extend(symbols)
            self.bars.extend(bars)
            return len(self.bars)

    class FakeCheckpointStore:
        def __init__(self) -> None:
            self.successes = []
            self.failures = []

        async def record_member_success(self, **kwargs):
            self.successes.append(kwargs)
            return True

        async def record_member_failure(self, **kwargs):
            self.failures.append(kwargs)
            return True

        async def heartbeat(self, **_kwargs):
            return True

    writer = FakeWriter()
    checkpoint_store = FakeCheckpointStore()

    result = asyncio.run(
        import_module.process_member_task(
            writer=writer,
            checkpoint_store=checkpoint_store,
            task={
                "id": 7,
                "zip_path": "fake.zip",
                "member_path": "20240102.parquet",
                "claim_token": "claim-7",
                "lease_version": 1,
                "imported_rows": 0,
            },
            batch_size=1000,
            lease_seconds=300,
        )
    )

    assert result == {"bars": 1}
    assert [symbol.symbol for symbol in writer.symbols] == ["600519.SH"]
    assert [bar.symbol for bar in writer.bars] == ["600519.SH"]
    assert writer.bars[0].ts.minute == 35
    assert checkpoint_store.successes == [{
        "checkpoint_id": 7,
        "claim_token": "claim-7",
        "lease_version": 1,
        "expected_imported_rows": 1,
    }]
    assert checkpoint_store.failures == []


def test_process_member_task_records_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        import_module,
        "_iter_member_rows",
        lambda zip_path, member_path, **_kwargs: iter([[{"code": "000001"}]]),
    )

    class FakeWriter:
        async def commit_member_batch(self, *, symbols, bars, **_kwargs):
            raise AssertionError("missing columns should fail before writes")

    class FakeCheckpointStore:
        def __init__(self) -> None:
            self.failures = []

        async def record_member_success(self, **kwargs):
            raise AssertionError(f"unexpected success: {kwargs}")

        async def record_member_failure(self, **kwargs):
            self.failures.append(kwargs)
            return True

        async def heartbeat(self, **_kwargs):
            return True

    checkpoint_store = FakeCheckpointStore()

    result = asyncio.run(
        import_module.process_member_task(
            writer=FakeWriter(),
            checkpoint_store=checkpoint_store,
            task={
                "id": 8,
                "zip_path": "fake.zip",
                "member_path": "bad.parquet",
                "claim_token": "claim-8",
                "lease_version": 2,
                "imported_rows": 0,
            },
            batch_size=1000,
            lease_seconds=300,
        )
    )

    assert result == {"bars": 0}
    assert checkpoint_store.failures[0]["checkpoint_id"] == 8
    assert checkpoint_store.failures[0]["expected_imported_rows"] == 0
    assert "Missing required parquet columns" in checkpoint_store.failures[0]["error"]


def test_process_tasks_concurrently_serializes_writes(monkeypatch) -> None:
    row = {
        "code": "600519",
        "trade_time": "2024-01-02 09:35:00",
        "open": 100.1,
        "high": 101.2,
        "low": 99.9,
        "close": 100.8,
        "vol": 1200,
        "amount": 1_234_567.89,
    }

    monkeypatch.setattr(
        import_module,
        "_iter_member_rows",
        lambda zip_path, member_path, **_kwargs: iter([[row]]),
    )

    class FakeWriter:
        def __init__(self) -> None:
            self.active_writes = 0
            self.max_active_writes = 0

        async def commit_member_batch(self, *, symbols, bars, **_kwargs):
            self.active_writes += 1
            self.max_active_writes = max(self.max_active_writes, self.active_writes)
            await asyncio.sleep(0.01)
            self.active_writes -= 1
            return len(bars)

    class FakeCheckpointStore:
        def __init__(self) -> None:
            self.successes = []

        async def record_member_success(self, **kwargs):
            self.successes.append(kwargs)
            return True

        async def record_member_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

        async def heartbeat(self, **_kwargs):
            return True

    writer = FakeWriter()
    checkpoint_store = FakeCheckpointStore()
    tasks = [
        {"id": 1, "zip_path": "fake.zip", "member_path": "20240102.parquet", "claim_token": "c1", "lease_version": 1, "imported_rows": 0},
        {"id": 2, "zip_path": "fake.zip", "member_path": "20240103.parquet", "claim_token": "c2", "lease_version": 1, "imported_rows": 0},
        {"id": 3, "zip_path": "fake.zip", "member_path": "20240104.parquet", "claim_token": "c3", "lease_version": 1, "imported_rows": 0},
    ]

    result = asyncio.run(
        import_module.process_tasks_concurrently(
            writer=writer,
            checkpoint_store=checkpoint_store,
            tasks=tasks,
            batch_size=1000,
            concurrency=3,
            write_concurrency=1,
            lease_seconds=300,
        )
    )

    assert result == {"bars": 3}
    assert writer.max_active_writes == 1
    assert [item["checkpoint_id"] for item in checkpoint_store.successes] == [1, 2, 3]


def test_process_member_resumes_exact_row_progress_and_yields_exact_owner(monkeypatch) -> None:
    rows = [
        {
            "code": code,
            "trade_time": f"2024-01-02 09:{minute}:00",
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "vol": 100,
            "amount": 1000,
        }
        for code, minute in (("000001", "35"), ("000002", "40"), ("000003", "45"))
    ]
    monkeypatch.setattr(
        import_module,
        "_iter_member_rows",
        lambda *_args, **_kwargs: iter([rows[:2], rows[2:]]),
    )

    class FakeWriter:
        def __init__(self) -> None:
            self.calls = []

        async def commit_member_batch(self, **kwargs):
            self.calls.append(kwargs)
            return len(kwargs["bars"])

    class FakeStore:
        def __init__(self) -> None:
            self.yields = []

        async def heartbeat(self, **_kwargs):
            return True

        async def yield_member(self, **kwargs):
            self.yields.append(kwargs)
            return True

        async def record_member_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

        async def record_member_success(self, **kwargs):
            raise AssertionError(f"unexpected success: {kwargs}")

    writer = FakeWriter()
    store = FakeStore()
    result = asyncio.run(import_module.process_member_task(
        writer=writer,
        checkpoint_store=store,
        task={
            "id": 9,
            "zip_path": "fake.zip",
            "member_path": "member.parquet",
            "claim_token": "claim-9",
            "lease_version": 4,
            "imported_rows": 1,
        },
        batch_size=2,
        lease_seconds=300,
        max_batches_per_member=1,
    ))

    assert result == {"bars": 1}
    assert [bar.symbol for bar in writer.calls[0]["bars"]] == ["000002.SZ"]
    assert writer.calls[0]["expected_imported_rows"] == 1
    assert store.yields == [{
        "checkpoint_id": 9,
        "claim_token": "claim-9",
        "lease_version": 4,
        "expected_imported_rows": 2,
    }]


def test_process_member_does_not_fail_checkpoint_after_lease_loss(monkeypatch) -> None:
    row = {
        "code": "000001",
        "trade_time": "2024-01-02 09:35:00",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "vol": 100,
        "amount": 1000,
    }
    monkeypatch.setattr(
        import_module,
        "_iter_member_rows",
        lambda *_args, **_kwargs: iter([[row]]),
    )

    class LostWriter:
        async def commit_member_batch(self, **_kwargs):
            raise LostScheme2MemberLease("lost")

    class FakeStore:
        async def heartbeat(self, **_kwargs):
            return True

        async def record_member_failure(self, **kwargs):
            raise AssertionError(f"stale owner must not record failure: {kwargs}")

    result = asyncio.run(import_module.process_member_task(
        writer=LostWriter(),
        checkpoint_store=FakeStore(),
        task={
            "id": 10,
            "zip_path": "fake.zip",
            "member_path": "member.parquet",
            "claim_token": "claim-10",
            "lease_version": 2,
            "imported_rows": 0,
        },
        batch_size=1,
        lease_seconds=300,
    ))

    assert result == {"bars": 0}


def test_process_member_rejects_changed_zip_identity_before_any_write(tmp_path: Path) -> None:
    zip_path = tmp_path / "changed.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("member.parquet", b"changed parquet bytes")
    with zipfile.ZipFile(zip_path) as archive:
        member = archive.getinfo("member.parquet")

    class NoWrite:
        async def commit_member_batch(self, **kwargs):
            raise AssertionError(f"changed source must not be written: {kwargs}")

    class FakeStore:
        def __init__(self) -> None:
            self.failures = []

        async def heartbeat(self, **_kwargs):
            return True

        async def record_member_failure(self, **kwargs):
            self.failures.append(kwargs)
            return True

    store = FakeStore()
    result = asyncio.run(import_module.process_member_task(
        writer=NoWrite(),
        checkpoint_store=store,
        task={
            "id": 12,
            "zip_path": str(zip_path),
            "member_path": member.filename,
            "member_crc32": member.CRC + 1,
            "member_size_bytes": member.file_size,
            "claim_token": "claim-12",
            "lease_version": 1,
            "imported_rows": 1,
        },
        batch_size=1,
        lease_seconds=300,
    ))

    assert result == {"bars": 0}
    assert len(store.failures) == 1
    assert store.failures[0]["expected_imported_rows"] == 1
    assert "CRC32" in store.failures[0]["error"]


def test_process_member_rejects_same_size_changed_direct_parquet_before_write(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "member.parquet"
    parquet_path.write_bytes(b"direct-content-a")
    expected_sha256 = import_module.hashlib.sha256(b"direct-content-a").hexdigest()
    parquet_path.write_bytes(b"direct-content-b")

    class NoWrite:
        async def commit_member_batch(self, **kwargs):
            raise AssertionError(f"changed direct source must not be written: {kwargs}")

    class FakeStore:
        def __init__(self) -> None:
            self.failures = []

        async def heartbeat(self, **_kwargs):
            return True

        async def record_member_failure(self, **kwargs):
            self.failures.append(kwargs)
            return True

    store = FakeStore()
    result = asyncio.run(import_module.process_member_task(
        writer=NoWrite(),
        checkpoint_store=store,
        task={
            "id": 13,
            "zip_path": str(parquet_path),
            "member_path": "",
            "member_crc32": None,
            "member_size_bytes": len(b"direct-content-a"),
            "content_sha256": expected_sha256,
            "claim_token": "claim-13",
            "lease_version": 1,
            "imported_rows": 1,
        },
        batch_size=1,
        lease_seconds=300,
    ))

    assert result == {"bars": 0}
    assert len(store.failures) == 1
    assert store.failures[0]["expected_imported_rows"] == 1
    assert "SHA-256" in store.failures[0]["error"]


def test_blocking_member_read_runs_off_loop_so_heartbeat_can_progress(monkeypatch) -> None:
    read_started = threading.Event()
    release_read = threading.Event()

    def blocking_rows(*_args, **_kwargs):
        read_started.set()
        assert release_read.wait(timeout=2)
        yield []

    monkeypatch.setattr(import_module, "_iter_member_rows", blocking_rows)

    class FakeStore:
        def __init__(self) -> None:
            self.heartbeats = 0

        async def heartbeat(self, **_kwargs):
            assert read_started.is_set()
            self.heartbeats += 1
            release_read.set()
            return True

        async def record_member_success(self, **_kwargs):
            return True

        async def record_member_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    store = FakeStore()
    result = asyncio.run(import_module.process_member_task(
        writer=object(),
        checkpoint_store=store,
        task={
            "id": 11,
            "zip_path": "fake.zip",
            "member_path": "member.parquet",
            "claim_token": "claim-11",
            "lease_version": 1,
            "imported_rows": 0,
        },
        batch_size=1,
        lease_seconds=0.3,
    ))

    assert result == {"bars": 0}
    assert store.heartbeats >= 1


def test_parse_symbol_accepts_prefixed_and_dotted_codes() -> None:
    assert import_module.parse_symbol("SZ000001").symbol == "000001.SZ"
    assert import_module.parse_symbol("600519.SH").symbol == "600519.SH"
    assert import_module.parse_symbol(1).symbol == "000001.SZ"
