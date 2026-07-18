from __future__ import annotations

import importlib
import inspect
import sys
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class WorkerSpec:
    name: str
    module: str
    description: str


_WORKERS: dict[str, WorkerSpec] = {
    "market-fill": WorkerSpec(
        name="market-fill",
        module="collector.market_fill",
        description="Realtime and nearline market K-line fill worker.",
    ),
    "symbol-master-refresh": WorkerSpec(
        name="symbol-master-refresh",
        module="collector.symbol_master",
        description="Refresh tradable A-share symbol master from provider discovery.",
    ),
    "history-backfill": WorkerSpec(
        name="history-backfill",
        module="collector.history_backfill",
        description="Recoverable historical K-line backfill worker.",
    ),
    "chan-module-c-recompute": WorkerSpec(
        name="chan-module-c-recompute",
        module="collector.chan_module_c_recompute",
        description="Module C native-timeframe full-history Chan recompute worker.",
    ),
    "module-c-batch-control": WorkerSpec(
        name="module-c-batch-control",
        module="collector.module_c_batch_control",
        description="Create, activate, inspect and seal frozen Module C batches.",
    ),
    "module-c-canary-selection": WorkerSpec(
        name="module-c-canary-selection",
        module="collector.module_c_canary_selection",
        description="Build a read-only deterministic strict-v2 canary selection.",
    ),
    "chan-c-stream": WorkerSpec(
        name="chan-c-stream",
        module="collector.chan_c_stream",
        description="Module C native-timeframe streaming Chan tail worker.",
    ),
    "lifecycle-observer": WorkerSpec(
        name="lifecycle-observer",
        module="collector.lifecycle_observer_worker",
        description="Durable Module C lifecycle publication outbox observer.",
    ),
    "tdx-csv-import": WorkerSpec(
        name="tdx-csv-import",
        module="collector.tdx_csv_import",
        description="Local TDX CSV history import worker.",
    ),
    "parquet-bootstrap-import": WorkerSpec(
        name="parquet-bootstrap-import",
        module="collector.parquet_bootstrap_import",
        description="Scheme2 parquet bootstrap import worker.",
    ),
    "parquet-bootstrap-audit": WorkerSpec(
        name="parquet-bootstrap-audit",
        module="collector.parquet_bootstrap_audit",
        description="Scheme2 parquet bootstrap audit command.",
    ),
    "aggregate-increment-import": WorkerSpec(
        name="aggregate-increment-import",
        module="collector.aggregate_increment_import",
        description="Append approved aggregate 5f/30f/1d incremental Parquet files.",
    ),
    "kline-scope-bootstrap": WorkerSpec(
        name="kline-scope-bootstrap",
        module="collector.kline_scope_catalog",
        description="One-shot generation-fenced K-line scope catalog bootstrap.",
    ),
    "pytdx-5f-spool": WorkerSpec(
        name="pytdx-5f-spool",
        module="collector.pytdx_5f_spool",
        description="Pytdx 5f gap spool worker.",
    ),
    "legacy-backfill": WorkerSpec(
        name="legacy-backfill",
        module="collector.backfill",
        description="Legacy direct pytdx backfill command kept for runbook compatibility.",
    ),
    "iwencai-sidebar-events": WorkerSpec(
        name="iwencai-sidebar-events",
        module="collector.market_data_provider",
        description="Event-driven iWencai sidebar snapshot worker.",
    ),
}


def normalize_worker_name(name: str) -> str:
    return name.strip().lower().replace("_", "-")


def list_workers() -> list[WorkerSpec]:
    return [_WORKERS[key] for key in sorted(_WORKERS)]


def get_worker(name: str) -> WorkerSpec:
    normalized = normalize_worker_name(name)
    if normalized == "backfill":
        normalized = "legacy-backfill"
    try:
        return _WORKERS[normalized]
    except KeyError as exc:
        choices = ", ".join(spec.name for spec in list_workers())
        raise ValueError(f"Unknown collector worker {name!r}. Available workers: {choices}") from exc


async def run_worker(name: str, args: Sequence[str] | None = None) -> int:
    spec = get_worker(name)
    module = importlib.import_module(spec.module)
    entrypoint = getattr(module, "main", None)
    if entrypoint is None:
        raise RuntimeError(f"Collector worker {spec.name!r} has no main() in {spec.module}")

    worker_args = list(args or [])
    original_argv = sys.argv[:]
    sys.argv = [f"python -m {spec.module}", *worker_args]
    try:
        parameters = inspect.signature(entrypoint).parameters
        result = entrypoint(worker_args) if len(parameters) == 1 else entrypoint()
        if inspect.isawaitable(result):
            result = await result
        return 0 if result is None else int(result)
    finally:
        sys.argv = original_argv
