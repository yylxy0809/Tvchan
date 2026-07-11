import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import app.engine.phase_1_21 as phase
from app.engine.time_utils import cutoff_key, iso_utc
from app.repositories.module_c_repo import ModuleCRepository


def _run(run_id: int, cutoff: datetime):
    return {"run_id": run_id, "cutoff_bar_end": cutoff, "signals": [], "strokes": [], "centers": []}


class PagedRepository(ModuleCRepository):
    def __init__(self, pages): self.pages = pages; self.calls = []
    async def fetch_historical_structure_runs(self, **kwargs):
        self.calls.append(kwargs)
        cursor = kwargs.get("cursor")
        return self.pages.get(cursor, [])


class DirectPagedRepository(ModuleCRepository):
    def __init__(self, pages): self.pages = pages; self.calls = []
    async def fetch_historical_runs_with_signals(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages.get(kwargs.get("cursor"), [])


def test_keyset_pagination_two_pages_same_cutoff_is_stable_and_complete():
    cutoff = datetime(2025, 1, 1, tzinfo=UTC)
    first = [_run(1, cutoff), _run(2, cutoff)]
    second = [_run(3, cutoff + timedelta(minutes=5))]
    repo = PagedRepository({None: first, (cutoff, 2): second})
    rows, metrics = asyncio.run(repo.fetch_historical_structure_runs_paged(symbols=["x"], levels=("5f",), run_groups=("g",), conn=object(), start=cutoff, end=cutoff + timedelta(days=1), batch_size=2))
    assert [row["run_id"] for row in rows] == [1, 2, 3]
    assert metrics == {"pages": 2, "runs": 3, "max_batch": 2, "batch_size": 2}
    assert all(call["start"] == cutoff and call["end"] == cutoff + timedelta(days=1) for call in repo.calls)


def test_keyset_pagination_empty_page_terminates_and_never_exceeds_batch_limit():
    cutoff = datetime(2025, 1, 1, tzinfo=UTC)
    repo = PagedRepository({None: []})
    rows, metrics = asyncio.run(repo.fetch_historical_structure_runs_paged(symbols=["x"], levels=("5f",), run_groups=("g",), conn=object(), start=cutoff, end=cutoff, batch_size=2000))
    assert rows == []
    assert metrics["pages"] == 0 and metrics["max_batch"] == 0
    with pytest.raises(ValueError, match="2000"):
        asyncio.run(repo.fetch_historical_structure_runs_paged(symbols=["x"], levels=("5f",), run_groups=("g",), conn=object(), start=cutoff, end=cutoff, batch_size=2001))


def test_keyset_nonadvancing_cursor_raises_instead_of_looping():
    cutoff = datetime(2025, 1, 1, tzinfo=UTC)
    page = [_run(1, cutoff)]
    repo = PagedRepository({None: page, (cutoff, 1): page})
    with pytest.raises(RuntimeError, match="Non-advancing"):
        asyncio.run(repo.fetch_historical_structure_runs_paged(symbols=["x"], levels=("5f",), run_groups=("g",), conn=object(), start=cutoff, end=cutoff, batch_size=1))


def test_direct_intraday_pagination_does_not_truncate_or_lose_same_cutoff_runs():
    cutoff = datetime(2025, 1, 1, tzinfo=UTC)
    first = [_run(1, cutoff), _run(2, cutoff)]
    second = [_run(3, cutoff + timedelta(minutes=5))]
    repo = DirectPagedRepository({None: first, (cutoff, 2): second})
    rows, metrics = asyncio.run(repo.fetch_historical_runs_with_signals_paged(symbols=["x"], levels=("30f", "5f"), run_groups=("g",), conn=object(), start=cutoff, end=cutoff + timedelta(days=1), batch_size=2))
    assert [row["run_id"] for row in rows] == [1, 2, 3]
    assert metrics == {"pages": 2, "runs": 3, "max_batch": 2, "batch_size": 2}
    assert all(call["start"] == cutoff and call["end"] == cutoff + timedelta(days=1) for call in repo.calls)


def test_direct_intraday_pagination_rejects_nonadvancing_cursor():
    cutoff = datetime(2025, 1, 1, tzinfo=UTC)
    page = [_run(1, cutoff)]
    repo = DirectPagedRepository({None: page, (cutoff, 1): page})
    with pytest.raises(RuntimeError, match="Non-advancing"):
        asyncio.run(repo.fetch_historical_runs_with_signals_paged(symbols=["x"], levels=("30f",), run_groups=("g",), conn=object(), start=cutoff, end=cutoff, batch_size=1))


def test_direct_repository_pager_rejects_naive_database_boundaries():
    repo = DirectPagedRepository({None: []})
    with pytest.raises(ValueError, match="Naive"):
        asyncio.run(repo.fetch_historical_runs_with_signals_paged(symbols=["x"], levels=("30f",), run_groups=("g",), conn=object(), start=datetime(2025, 1, 1), end=datetime(2025, 1, 2, tzinfo=UTC)))


def _required(directory: Path, marker: str):
    for name in ("source_artifact_manifest.json", "database_readonly_snapshot_before.json", "database_readonly_snapshot_after.json", "intraday_run_coverage_v3.json", "next_phase_decision.json", "phase_1_21_detailed_completion_report.md"):
        (directory / name).write_text(marker, encoding="utf-8")


def test_atomic_idempotency_and_stale_lock_are_safe(monkeypatch, tmp_path: Path):
    target = tmp_path / "out"
    async def impl(*, output_dir, **_): _required(output_dir, "stable")
    monkeypatch.setattr(phase, "_run_phase_1_21_impl", impl)
    asyncio.run(phase.run_phase_1_21(output_dir=target))
    first = hashlib.sha256((target / "next_phase_decision.json").read_bytes()).hexdigest()
    asyncio.run(phase.run_phase_1_21(output_dir=target))
    assert hashlib.sha256((target / "next_phase_decision.json").read_bytes()).hexdigest() == first
    lock = tmp_path / ".out.lock"; lock.write_text('{"pid": 999999}', encoding="utf-8")
    with pytest.raises(RuntimeError, match="refusing concurrent run"):
        asyncio.run(phase.run_phase_1_21(output_dir=target))
    assert lock.exists()


def test_stale_tokenized_lock_is_safely_reclaimed(monkeypatch, tmp_path: Path):
    target = tmp_path / "out"
    async def impl(*, output_dir, **_): _required(output_dir, "stable")
    monkeypatch.setattr(phase, "_run_phase_1_21_impl", impl)
    lock = tmp_path / ".out.lock"
    lock.write_text('{"pid": 999999, "started_at": "2025-01-01T00:00:00+00:00", "token": "stale", "cleanup_deferred": false}', encoding="utf-8")
    assert asyncio.run(phase.run_phase_1_21(output_dir=target)) is None
    assert not lock.exists()


def test_cross_process_mutex_rejects_competing_run_before_stale_lock_reclamation(monkeypatch, tmp_path: Path):
    class SharedMutex:
        held = False
        def __init__(self, _target): pass
        def acquire(self):
            if SharedMutex.held:
                return False
            SharedMutex.held = True
            return True
        def release(self): SharedMutex.held = False
    started, release = asyncio.Event(), asyncio.Event()
    async def impl(*, output_dir, **_):
        _required(output_dir, "stable")
        started.set()
        await release.wait()
    monkeypatch.setattr(phase, "_OutputMutex", SharedMutex)
    monkeypatch.setattr(phase, "_run_phase_1_21_impl", impl)
    async def race():
        first = asyncio.create_task(phase.run_phase_1_21(output_dir=tmp_path / "out"))
        await started.wait()
        with pytest.raises(RuntimeError, match="output mutex is held"):
            await phase.run_phase_1_21(output_dir=tmp_path / "out")
        release.set()
        await first
    asyncio.run(race())


def test_lifecycle_grid_filter_matches_equal_utc_and_offset_cutoffs():
    expected = [{"symbol": "x", "level": "30f", "cutoff_bar_end": "2025-01-01T09:00:00+00:00"}]
    runs = [{"symbol": "x", "level": "30f", "cutoff_bar_end": "2025-01-01T17:00:00+08:00"}]
    assert phase._lifecycle_runs_on_expected_grid(runs, expected) == runs


def test_windows_pid_probe_treats_only_invalid_parameter_as_confirmed_dead():
    assert phase._windows_pid_alive(123, lambda *_: 0, lambda: 87, lambda *_: None) is False
    assert phase._windows_pid_alive(123, lambda *_: 0, lambda: 5, lambda *_: None) is True
    assert phase._windows_pid_alive(123, lambda *_: 0, lambda: 1234, lambda *_: None) is True


def test_posix_pid_probe_reclaims_only_confirmed_missing_process():
    def raise_error(error):
        def kill(*_):
            raise error

        return kill

    assert phase._posix_pid_alive(123, raise_error(ProcessLookupError())) is False
    assert phase._posix_pid_alive(123, raise_error(PermissionError())) is True
    assert phase._posix_pid_alive(123, raise_error(OSError("unknown"))) is True


def test_output_mutex_holds_posix_advisory_lock_for_entire_run(monkeypatch, tmp_path: Path):
    calls = []
    monkeypatch.setattr(phase, "_is_windows", lambda: False)
    monkeypatch.setattr(phase, "_acquire_posix_mutex", lambda path: calls.append(("acquire", path)) or 42)
    monkeypatch.setattr(phase, "_release_posix_mutex", lambda handle: calls.append(("release", handle)))
    mutex = phase._OutputMutex(tmp_path / "out")
    assert mutex.acquire() is True
    mutex.release()
    assert calls[0][0] == "acquire"
    assert calls[1] == ("release", 42)


def test_output_mutex_serializes_two_instances_across_the_machine(tmp_path: Path):
    first = phase._OutputMutex(tmp_path / "out")
    second = phase._OutputMutex(tmp_path / "out")
    assert first.acquire() is True
    try:
        assert second.acquire() is False
    finally:
        first.release()
    assert second.acquire() is True
    second.release()


def test_execution_bar_must_be_inside_trigger_window():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = [
        {"symbol": "x", "timeframe": 30, "ts": start + timedelta(days=6)},
    ]
    assert phase._has_execution_bar(rows, symbol="x", after=start, window_end=start + timedelta(days=5)) is False
    rows.append({"symbol": "x", "timeframe": 30, "ts": start + timedelta(days=1)})
    assert phase._has_execution_bar(rows, symbol="x", after=start, window_end=start + timedelta(days=5)) is True


def test_cutoff_json_roundtrip_preserves_aware_utc_semantics():
    value = datetime(2025, 1, 1, 8, tzinfo=UTC)
    restored = json.loads(json.dumps({"cutoff": iso_utc(value)}))["cutoff"]
    assert cutoff_key(restored) == value
    with pytest.raises(ValueError, match="Naive"):
        cutoff_key("2025-01-01T08:00:00")
