from __future__ import annotations

import importlib
import json
import os
import sys
from functools import lru_cache
from pathlib import Path

from chan_service.models import (
    ChanAnalyzeRequest,
    ChanAnalyzeResponse,
    ChanCenter,
    ChanPoint,
    ChanSignal,
    ChanStroke,
)

_REQUEST_TO_LEGACY_LEVEL = {
    "5f": "5f",
    "30f": "30f",
    "1d": "daily",
}

_LEGACY_TO_RESPONSE_LEVEL = {
    "5f": "5f",
    "30f": "30f",
    "daily": "1d",
}

_LEGACY_VENDOR_PREFIXES = (
    "Bi",
    "BuySellPoint",
    "Chan",
    "ChanConfig",
    "Combiner",
    "Common",
    "Config",
    "DataAPI",
    "KLine",
    "Math",
    "Plot",
    "Seg",
    "ZS",
)


def analyze_with_legacy_engine(request: ChanAnalyzeRequest) -> ChanAnalyzeResponse:
    legacy_backend_path = _resolve_legacy_backend_path()
    if legacy_backend_path is None:
        raise RuntimeError("Legacy chan engine path is not configured")

    analyzer_cls = _load_legacy_analyzer(str(legacy_backend_path))
    analyzer = analyzer_cls(user_config=_load_legacy_user_config())
    result = analyzer.analyze_multi_level(
        request.symbol,
        [_normalize_bar(bar) for bar in request.bars],
    )
    if result is None:
        raise RuntimeError(f"Legacy chan engine returned no result for {request.symbol}")

    requested_levels = [
        _REQUEST_TO_LEGACY_LEVEL[level]
        for level in request.chan_levels
        if level in _REQUEST_TO_LEGACY_LEVEL
    ]
    requested_modes = {mode.strip().lower() for mode in request.modes}

    strokes: list[ChanStroke] = []
    segments: list[ChanStroke] = []
    centers: list[ChanCenter] = []
    signals: list[ChanSignal] = []

    for legacy_level in requested_levels:
        level_result = result.levels.get(legacy_level)
        if level_result is None:
            continue

        response_level = _LEGACY_TO_RESPONSE_LEVEL.get(legacy_level, legacy_level)

        for index, item in enumerate(getattr(level_result, "strokes", []) or []):
            normalized_mode = _normalize_mode(bool(getattr(item, "is_sure", True)))
            if normalized_mode not in requested_modes:
                continue
            strokes.append(
                ChanStroke(
                    id=_line_id("stroke", response_level, normalized_mode, index, item.start_time, item.end_time),
                    level=response_level,
                    mode=normalized_mode,
                    start=ChanPoint(time=int(item.start_time), price=float(item.start_price)),
                    end=ChanPoint(time=int(item.end_time), price=float(item.end_price)),
                    direction=str(item.direction),
                    confirmed=bool(getattr(item, "is_sure", True)),
                )
            )

        for index, item in enumerate(getattr(level_result, "segments", []) or []):
            normalized_mode = _normalize_mode(bool(getattr(item, "is_sure", True)))
            if normalized_mode not in requested_modes:
                continue
            segments.append(
                ChanStroke(
                    id=_line_id("segment", response_level, normalized_mode, index, item.start_time, item.end_time),
                    level=response_level,
                    mode=normalized_mode,
                    start=ChanPoint(time=int(item.start_time), price=float(item.start_price)),
                    end=ChanPoint(time=int(item.end_time), price=float(item.end_price)),
                    direction=str(item.direction),
                    confirmed=bool(getattr(item, "is_sure", True)),
                )
            )

        for index, item in enumerate(getattr(level_result, "pivots", []) or []):
            normalized_mode = _normalize_mode(bool(getattr(item, "is_sure", True)))
            if normalized_mode not in requested_modes:
                continue
            centers.append(
                ChanCenter(
                    id=_center_id(response_level, normalized_mode, index, item.start_time, item.end_time),
                    level=response_level,
                    mode=normalized_mode,
                    start_time=int(item.start_time),
                    end_time=int(item.end_time),
                    low=float(item.low),
                    high=float(item.high),
                    confirmed=bool(getattr(item, "is_sure", True)),
                )
            )

        for index, item in enumerate(getattr(level_result, "buy_sell_points", []) or []):
            normalized_mode = _normalize_mode(bool(getattr(item, "is_sure", True)))
            if normalized_mode not in requested_modes:
                continue
            signals.append(
                ChanSignal(
                    id=_signal_id(response_level, normalized_mode, index, item.time, item.bsp_type),
                    level=response_level,
                    mode=normalized_mode,
                    time=int(item.time),
                    price=float(item.price),
                    signal_type=str(item.bsp_type),
                    confirmed=bool(getattr(item, "is_sure", True)),
                )
            )

    return ChanAnalyzeResponse(
        symbol=request.symbol,
        timeframe=request.timeframe,
        engine="legacy-copy",
        strokes=strokes,
        segments=segments,
        centers=centers,
        signals=signals,
    )


def resolve_legacy_backend_path() -> str | None:
    path = _resolve_legacy_backend_path()
    return str(path) if path is not None else None


def _normalize_mode(is_sure: bool) -> str:
    return "confirmed" if is_sure else "predictive"


def _normalize_bar(bar: object) -> dict[str, float | int]:
    if hasattr(bar, "model_dump"):
        payload = bar.model_dump()
    else:
        payload = dict(bar)
    return {
        "timestamp": int(payload["time"]),
        "open": float(payload["open"]),
        "high": float(payload["high"]),
        "low": float(payload["low"]),
        "close": float(payload["close"]),
        "volume": float(payload.get("volume", 0) or 0),
        "amount": float(payload.get("amount", 0) or 0),
    }


def _load_legacy_user_config() -> dict[str, object] | None:
    raw = os.getenv("CHAN_LEGACY_CONFIG_JSON", "").strip()
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("CHAN_LEGACY_CONFIG_JSON is not valid JSON") from exc
    if not isinstance(value, dict):
        raise RuntimeError("CHAN_LEGACY_CONFIG_JSON must be a JSON object")
    return value


def _line_id(
    kind: str,
    level: str,
    mode: str,
    index: int,
    start_time: int,
    end_time: int,
) -> str:
    return f"{kind}:{level}:{mode}:{index}:{int(start_time)}:{int(end_time)}"


def _center_id(level: str, mode: str, index: int, start_time: int, end_time: int) -> str:
    return f"center:{level}:{mode}:{index}:{int(start_time)}:{int(end_time)}"


def _signal_id(level: str, mode: str, index: int, time: int, signal_type: str) -> str:
    return f"signal:{level}:{mode}:{index}:{int(time)}:{signal_type}"


def _resolve_legacy_backend_path() -> Path | None:
    configured = os.getenv("CHAN_LEGACY_SCHEME_PATH", "").strip()
    if configured:
        path = Path(configured).resolve()
    else:
        path = Path(__file__).resolve().parents[3] / "旧版方案" / "backend"
    if (path / "chan_engine" / "analyzer.py").exists():
        return path
    return None


@lru_cache(maxsize=1)
def _load_legacy_analyzer(legacy_backend_path: str):
    _evict_conflicting_vendor_modules()
    if legacy_backend_path not in sys.path:
        sys.path.insert(0, legacy_backend_path)
    module = importlib.import_module("chan_engine")
    return module.ChanAnalyzer


def _evict_conflicting_vendor_modules() -> None:
    for module_name in list(sys.modules):
        if module_name == "chan_engine" or module_name.startswith("chan_engine."):
            del sys.modules[module_name]
            continue
        if any(
            module_name == prefix or module_name.startswith(f"{prefix}.")
            for prefix in _LEGACY_VENDOR_PREFIXES
        ):
            del sys.modules[module_name]
