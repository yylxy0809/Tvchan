from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock, RLock

from chan_service.engine_registry import AnalyzerEngine, resolve_engine
from chan_service.models import (
    ChanAnalyzeRequest,
    ChanAnalyzeResponse,
)

_SNAPSHOT_CACHE_MAX_ITEMS = 32
_snapshot_registry_lock = RLock()
_published_snapshots: OrderedDict[tuple[str, str, str, str, str], ChanAnalyzeResponse] = OrderedDict()
_symbol_locks: dict[str, Lock] = {}


@dataclass(frozen=True)
class AnalyzerResult:
    response: ChanAnalyzeResponse
    engine: AnalyzerEngine


def analyze(request: ChanAnalyzeRequest) -> ChanAnalyzeResponse:
    return analyze_with_metadata(request).response


def analyze_with_metadata(request: ChanAnalyzeRequest) -> AnalyzerResult:
    engine = resolve_engine()
    cache_key = _analysis_cache_key(request, engine)
    symbol_lock = _symbol_lock(request.symbol)
    with symbol_lock:
        cached = _load_published_snapshot(cache_key)
        if cached is not None:
            return AnalyzerResult(
                response=cached,
                engine=AnalyzerEngine(name=cached.engine, mode="published_snapshot"),
            )

        response = _compute_analysis_response(request, engine)
        _publish_snapshot(cache_key, response)
        return AnalyzerResult(response=response.model_copy(deep=True), engine=engine)


def _compute_analysis_response(
    request: ChanAnalyzeRequest,
    engine: AnalyzerEngine,
) -> ChanAnalyzeResponse:
    if engine.mode == "chan_py":
        return _analyze_with_chan_py(request, engine)
    if engine.mode == "unsupported":
        raise RuntimeError(f"Unsupported Chan engine mode: {engine.name}")
    raise RuntimeError(f"Unsupported Chan engine mode: {engine.name}")


def get_engine_metadata() -> dict[str, str]:
    engine = resolve_engine()
    snapshot_metrics = _snapshot_registry_metrics()
    if engine.mode == "chan_py":
        return {
            "engine": engine.name,
            "mode": engine.mode,
            "status": "configured",
            "published_snapshots": str(snapshot_metrics["published_snapshots"]),
            "tracked_symbols": str(snapshot_metrics["tracked_symbols"]),
        }
    return {
        "engine": engine.name,
        "mode": engine.mode,
        "status": "fallback",
        "published_snapshots": str(snapshot_metrics["published_snapshots"]),
        "tracked_symbols": str(snapshot_metrics["tracked_symbols"]),
    }


def _analyze_with_chan_py(
    request: ChanAnalyzeRequest,
    engine: AnalyzerEngine,
) -> ChanAnalyzeResponse:
    if not engine.module_path:
        raise RuntimeError("CHAN_PY_PATH is not configured")

    from chan_service.vendor_chan_adapter import build_overlay

    payload = build_overlay(
        {
            **request.model_dump(),
            "chan_py_path": engine.module_path,
        }
    )
    return ChanAnalyzeResponse.model_validate(payload)


def _analysis_cache_key(
    request: ChanAnalyzeRequest,
    engine: AnalyzerEngine,
) -> tuple[str, str, str, str, str]:
    return (
        request.symbol.upper(),
        request.timeframe,
        ",".join(request.chan_levels),
        ",".join(request.modes),
        f"{engine.name}:{_bars_fingerprint(request)}",
    )


def _bars_fingerprint(request: ChanAnalyzeRequest) -> str:
    digest = hashlib.sha256()
    digest.update(request.symbol.upper().encode("utf-8"))
    digest.update(request.timeframe.encode("utf-8"))
    digest.update(json.dumps(request.chan_levels, separators=(",", ":")).encode("utf-8"))
    digest.update(json.dumps(request.modes, separators=(",", ":")).encode("utf-8"))
    for bar in request.bars:
        digest.update(
            (
                f"{bar.time}|{bar.open:.8f}|{bar.high:.8f}|{bar.low:.8f}|"
                f"{bar.close:.8f}|{bar.volume}|"
            ).encode("utf-8")
        )
    return digest.hexdigest()


def _symbol_lock(symbol: str) -> Lock:
    normalized = symbol.upper()
    with _snapshot_registry_lock:
        lock = _symbol_locks.get(normalized)
        if lock is None:
            lock = Lock()
            _symbol_locks[normalized] = lock
        return lock


def _load_published_snapshot(
    cache_key: tuple[str, str, str, str, str],
) -> ChanAnalyzeResponse | None:
    with _snapshot_registry_lock:
        published = _published_snapshots.get(cache_key)
        if published is None:
            return None
        _published_snapshots.move_to_end(cache_key)
        return published.model_copy(deep=True)


def _publish_snapshot(
    cache_key: tuple[str, str, str, str, str],
    response: ChanAnalyzeResponse,
) -> None:
    published = response.model_copy(deep=True)
    with _snapshot_registry_lock:
        _published_snapshots[cache_key] = published
        _published_snapshots.move_to_end(cache_key)
        while len(_published_snapshots) > _SNAPSHOT_CACHE_MAX_ITEMS:
            _published_snapshots.popitem(last=False)


def _snapshot_registry_metrics() -> dict[str, int]:
    with _snapshot_registry_lock:
        return {
            "published_snapshots": len(_published_snapshots),
            "tracked_symbols": len(_symbol_locks),
        }
