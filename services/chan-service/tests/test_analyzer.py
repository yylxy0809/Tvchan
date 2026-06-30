from __future__ import annotations

import math
import os
from pathlib import Path

import pytest

from chan_service import analyzer
from chan_service.analyzer import analyze, analyze_with_metadata, get_engine_metadata
from chan_service.models import ChanAnalyzeRequest
from chan_service.vendor_chan_adapter import build_overlay


@pytest.fixture(autouse=True)
def reset_snapshot_registry():
    with analyzer._snapshot_registry_lock:
        analyzer._published_snapshots.clear()
        analyzer._symbol_locks.clear()
    yield
    with analyzer._snapshot_registry_lock:
        analyzer._published_snapshots.clear()
        analyzer._symbol_locks.clear()


def _vendor_chan_py_path() -> str:
    return str(Path(__file__).resolve().parents[3] / "work" / "vendor" / "chan.py-main")


def _bars(count: int = 600) -> list[dict]:
    base = 1_718_000_000
    result = []
    for index in range(count):
        price = 10 + math.sin(index / 3) * 0.8 + math.sin(index / 17) * 1.5 + (index / count) * 2
        result.append(
            {
                "time": base + index * 300,
                "open": price - 0.03,
                "high": price + 0.25,
                "low": price - 0.25,
                "close": price + 0.03,
                "volume": 1000 + index,
            }
        )
    return result


def _deep_recursive_bars(count: int = 2000) -> list[dict]:
    base = 1_718_000_000
    result = []
    for index in range(count):
        price = (
            10
            + math.sin(index / 3) * 0.8
            + math.sin(index / 17) * 1.5
            + math.sin(index / 89) * 4
            + (index / count) * 2
        )
        result.append(
            {
                "time": base + index * 300,
                "open": price - 0.03,
                "high": price + 0.25,
                "low": price - 0.25,
                "close": price + 0.03,
                "volume": 1000 + index,
            }
        )
    return result


def _request(*, bars: list[dict] | None = None) -> ChanAnalyzeRequest:
    return ChanAnalyzeRequest(
        symbol="000001.SZ",
        timeframe="5f",
        chan_levels=["5f", "30f", "1d"],
        modes=["confirmed", "predictive"],
        bars=bars or _bars(),
    )


def test_analyzer_defaults_to_module_b_chan_py_engine() -> None:
    if not Path(_vendor_chan_py_path()).exists():
        pytest.skip("local chan.py source is not available")
    response = analyze(_request())
    assert response.engine == "module-b:chan.py"
    assert response.base_timeframe == "5f"
    assert response.base_ts_semantics == "bar_end"
    assert response.snapshot_version
    assert response.strokes
    assert response.strokes[0].start.base_ts == response.strokes[0].start.time


def test_vendor_chan_adapter_returns_module_b_engine() -> None:
    response = build_overlay(
        {
            "symbol": "000001.SZ",
            "timeframe": "5f",
            "chan_levels": ["5f", "30f", "1d"],
            "bars": _bars(),
            "modes": ["confirmed", "predictive"],
            "chan_py_path": _vendor_chan_py_path(),
        }
    )
    assert response["engine"] == "module-b:chan.py"
    assert response["base_timeframe"] == "5f"
    assert response["base_ts_semantics"] == "bar_end"
    assert response["snapshot_version"]
    assert response["strokes"]
    assert response["channels"] is not None
    first_stroke = response["strokes"][0]
    assert first_stroke["start"]["base_ts"] == first_stroke["start"]["time"]
    assert first_stroke["end"]["base_ts"] == first_stroke["end"]["time"]
    if response["signals"]:
        first_signal = response["signals"][0]
        assert first_signal["base_ts"] == first_signal["time"]
        assert first_signal["side"] in {"buy", "sell"}
        assert "features" in first_signal


def test_module_b_builds_daily_layer_without_relabeling_30f_signals() -> None:
    response = build_overlay(
        {
            "symbol": "000001.SZ",
            "timeframe": "5f",
            "chan_levels": ["5f", "30f", "1d"],
            "bars": _deep_recursive_bars(),
            "modes": ["confirmed", "predictive"],
            "chan_py_path": _vendor_chan_py_path(),
        }
    )

    daily_strokes = [item for item in response["strokes"] if item["level"] == "1d"]
    daily_segments = [item for item in response["segments"] if item["level"] == "1d"]
    daily_centers = [item for item in response["centers"] if item["level"] == "1d"]
    thirty_signals = [item for item in response["signals"] if item["level"] == "30f"]
    daily_signals = [item for item in response["signals"] if item["level"] == "1d"]

    assert daily_strokes
    assert daily_segments
    assert daily_centers
    assert thirty_signals
    assert daily_signals
    assert len(daily_signals) != len(thirty_signals)
    assert {
        (item["time"], item["signal_type"], item["side"]) for item in daily_signals
    } != {
        (item["time"], item["signal_type"], item["side"]) for item in thirty_signals
    }


def test_analyzer_uses_module_b_chan_py_engine(monkeypatch) -> None:
    monkeypatch.setenv("CHAN_ENGINE_MODE", "module_b")
    monkeypatch.setenv("CHAN_PY_PATH", _vendor_chan_py_path())
    response = analyze(_request())
    assert response.engine == "module-b:chan.py"
    assert response.strokes
    metadata = get_engine_metadata()
    assert metadata["engine"] == "module-b:chan.py"
    assert metadata["mode"] == "chan_py"
    assert metadata["status"] == "configured"


def test_module_b_missing_path_does_not_fallback(monkeypatch) -> None:
    monkeypatch.setenv("CHAN_ENGINE_MODE", "module_b")
    monkeypatch.setenv("CHAN_PY_PATH", os.path.join("missing", "chan.py-main"))
    with pytest.raises(RuntimeError, match="chan.py module not found"):
        analyze(_request(bars=_bars()))


def test_unsupported_engine_mode_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("CHAN_ENGINE_MODE", "unsupported")
    with pytest.raises(RuntimeError, match="Unsupported Chan engine mode"):
        analyze(_request(bars=_bars(30)))


def test_published_snapshot_returns_defensive_copy() -> None:
    request = _request(bars=_bars(30))
    first = analyze(request)
    stroke_count = len(first.strokes)
    first.strokes.clear()
    second = analyze(request)
    assert len(second.strokes) == stroke_count


def test_analyze_with_metadata_reports_published_snapshot_mode() -> None:
    request = _request(bars=_bars(30))
    first = analyze_with_metadata(request)
    second = analyze_with_metadata(request)
    assert first.engine.mode == "chan_py"
    assert second.engine.mode == "published_snapshot"
    assert second.response.snapshot_version == first.response.snapshot_version
    assert second.response.model_dump() == first.response.model_dump()


def test_engine_metadata_reports_snapshot_registry_counts() -> None:
    analyze(_request(bars=_bars(30)))
    metadata = get_engine_metadata()
    assert metadata["published_snapshots"] == "1"
    assert metadata["tracked_symbols"] == "1"
