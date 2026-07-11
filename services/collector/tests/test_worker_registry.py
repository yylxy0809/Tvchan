from __future__ import annotations

import sys
import types
import asyncio

from collector import worker_registry


def test_worker_name_normalization_and_alias() -> None:
    assert worker_registry.get_worker("market_fill").module == "collector.market_fill"
    assert worker_registry.get_worker("backfill").module == "collector.backfill"


def test_list_workers_is_sorted() -> None:
    names = [spec.name for spec in worker_registry.list_workers()]
    assert names == sorted(names)
    assert "chan-c-stream" in names
    assert "chan-module-c-recompute" in names
    assert "chan-recompute" not in names
    assert "chan-tail-publisher" not in names
    assert "realtime-pipeline" not in names
    assert "tdx-csv-import" in names


def test_run_worker_restores_argv(monkeypatch) -> None:
    calls: list[list[str]] = []

    module = types.ModuleType("collector_fake_worker")

    def main() -> int:
        calls.append(sys.argv[:])
        return 7

    module.main = main  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "collector_fake_worker", module)
    monkeypatch.setitem(
        worker_registry._WORKERS,
        "fake",
        worker_registry.WorkerSpec("fake", "collector_fake_worker", "Fake worker"),
    )
    original_argv = sys.argv[:]

    code = asyncio.run(worker_registry.run_worker("fake", ["--dry-run"]))

    assert code == 7
    assert calls == [["python -m collector_fake_worker", "--dry-run"]]
    assert sys.argv == original_argv


def test_run_worker_supports_argv_entrypoint(monkeypatch) -> None:
    calls: list[list[str]] = []

    module = types.ModuleType("collector_fake_argv_worker")

    async def main(argv: list[str]) -> None:
        calls.append(argv)

    module.main = main  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "collector_fake_argv_worker", module)
    monkeypatch.setitem(
        worker_registry._WORKERS,
        "fake-argv",
        worker_registry.WorkerSpec("fake-argv", "collector_fake_argv_worker", "Fake argv worker"),
    )

    code = asyncio.run(worker_registry.run_worker("fake_argv", ["--root", "D:\\data"]))

    assert code == 0
    assert calls == [["--root", "D:\\data"]]
