from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from app.history.exports import (
    ExportBuildBusy,
    ExportCapacityExceeded,
    ExportTooLarge,
    InMemoryHistoryExportStore,
)


def make_store(clock, **overrides) -> InMemoryHistoryExportStore:
    limits = {
        "ttl_seconds": 10,
        "max_records": 2,
        "max_records_per_owner": 2,
        "max_stored_bytes": 10_000,
        "max_stored_bytes_per_owner": 10_000,
        "max_uncompressed_bytes": 10_000,
        "max_compressed_bytes": 10_000,
        "max_chunks": 10,
        "max_concurrent_builds": 1,
        "clock": clock,
    }
    limits.update(overrides)
    return InMemoryHistoryExportStore(**limits)


def create(store: InMemoryHistoryExportStore, owner: str, **kwargs):
    with store.reserve_build(owner):
        return store.create_export(owner_key=owner, bars=[], **kwargs)


def test_store_ttl_is_absolute_and_releases_capacity() -> None:
    now = [100.0]
    store = make_store(lambda: now[0], max_records=1, max_records_per_owner=1)
    first = create(store, "owner-a")
    assert store.get_chunk("owner-a", first.request_id, 0) is not None
    now[0] = 110.0

    assert store.get_chunk("owner-a", first.request_id, 0) is None
    second = create(store, "owner-a")
    assert second.request_id != first.request_id


def test_store_ttl_starts_before_serialization_work() -> None:
    calls = 0

    def advancing_clock() -> float:
        nonlocal calls
        calls += 1
        return 100.0 if calls <= 2 else 110.0

    store = make_store(advancing_clock)
    record = create(store, "owner-a", metadata={"note": "x" * 100})

    assert store.get_chunk("owner-a", record.request_id, 0) is None


def test_store_enforces_owner_and_global_capacity_without_eviction() -> None:
    store = make_store(lambda: 100.0, max_records=2, max_records_per_owner=1)
    first = create(store, "owner-a")
    with pytest.raises(ExportCapacityExceeded):
        create(store, "owner-a")
    create(store, "owner-b")
    with pytest.raises(ExportCapacityExceeded):
        create(store, "owner-c")

    assert store.get_chunk("owner-b", first.request_id, 0) is None
    assert store.get_chunk("owner-a", first.request_id, 0) is not None
    assert store.record_count == 2


def test_store_rejects_oversize_and_chunk_amplification_without_partial_record() -> None:
    store = make_store(lambda: 100.0, max_uncompressed_bytes=100, max_chunks=1)
    with pytest.raises(ExportTooLarge):
        create(store, "owner-a", metadata={"note": "x" * 200})
    with pytest.raises(ExportTooLarge):
        create(store, "owner-a", metadata={"note": "x" * 50}, chunk_size_bytes=1)
    assert store.record_count == 0


def test_store_enforces_bar_and_byte_capacity_atomically() -> None:
    store = make_store(lambda: 100.0, max_bars=1)
    with store.reserve_build("owner-a"):
        with pytest.raises(ExportTooLarge):
            store.create_export(owner_key="owner-a", bars=[{}, {}])
    first = create(store, "owner-a", metadata={"note": "a" * 200})
    store.max_stored_bytes_per_owner = first.compressed_size_bytes

    with pytest.raises(ExportCapacityExceeded):
        create(store, "owner-a", metadata={"note": "b" * 200})

    assert store.record_count == 1


def test_store_does_not_retain_uncompressed_metadata() -> None:
    store = make_store(lambda: 100.0, max_uncompressed_bytes=20_000)
    record = create(store, "owner-a", metadata={"note": "x" * 10_000})
    stored = store._records[record.request_id]

    assert not hasattr(stored, "record")
    assert stored.compressed_size_bytes == record.compressed_size_bytes


def test_store_global_byte_capacity_is_atomic_across_owners() -> None:
    store = make_store(lambda: 100.0)
    first = create(store, "owner-a", metadata={"note": "a" * 200})
    store.max_stored_bytes = first.compressed_size_bytes

    with pytest.raises(ExportCapacityExceeded):
        create(store, "owner-b", metadata={"note": "b" * 200})

    assert store.record_count == 1


def test_store_concurrent_capacity_check_commits_only_one_record() -> None:
    store = make_store(
        lambda: 100.0,
        max_records=1,
        max_concurrent_builds=2,
    )
    barrier = Barrier(2)

    def build(owner: str):
        with store.reserve_build(owner):
            barrier.wait()
            return store.create_export(owner_key=owner, bars=[])

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(build, owner) for owner in ("owner-a", "owner-b")]
        outcomes = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except ExportCapacityExceeded as exc:
                outcomes.append(exc)

    assert sum(not isinstance(item, Exception) for item in outcomes) == 1
    assert sum(isinstance(item, ExportCapacityExceeded) for item in outcomes) == 1
    assert store.record_count == 1


def test_store_allows_only_one_build_per_owner_and_global_slot() -> None:
    store = make_store(lambda: 100.0)
    with store.reserve_build("owner-a"):
        with pytest.raises(ExportBuildBusy):
            with store.reserve_build("owner-a"):
                pass
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lambda: store.reserve_build("owner-b").__enter__())
            with pytest.raises(ExportBuildBusy):
                future.result()
    assert store.record_count == 0
