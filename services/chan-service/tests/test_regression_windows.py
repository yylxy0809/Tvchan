from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

from chan_service.vendor_chan_adapter import build_overlay

ROOT = Path(__file__).resolve().parents[3]
DUCKDB_ENV_PATH = os.environ.get("CHAN_REGRESSION_DUCKDB_PATH", "").strip()
DUCKDB_PATH = Path(DUCKDB_ENV_PATH) if DUCKDB_ENV_PATH else Path()
CHAN_PY_PATH = ROOT / "work" / "vendor" / "chan.py-main"
FIXTURE_PATH = ROOT / "services" / "chan-service" / "tests" / "fixtures" / "chan_regression_windows.json"
SYMBOL = "000001.SZ"
SYMBOL_CODE = "sz.000001"


def _require_duckdb_path() -> None:
    if not DUCKDB_ENV_PATH:
        pytest.skip("Regression DuckDB is not configured via CHAN_REGRESSION_DUCKDB_PATH")
    if not DUCKDB_PATH.exists():
        pytest.skip(f"Regression DuckDB is unavailable: {DUCKDB_PATH}")


@lru_cache(maxsize=1)
def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _old_scheme_backend_root() -> Path:
    for child in ROOT.iterdir():
        if not child.is_dir():
            continue
        backend = child / "backend"
        frontend = child / "frontend"
        analyzer = backend / "chan_engine" / "analyzer.py"
        if backend.exists() and frontend.exists() and analyzer.exists():
            return backend
    raise FileNotFoundError("Could not locate old-scheme backend for regression oracle")


@lru_cache(maxsize=1)
def _connect():
    _require_duckdb_path()
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


@lru_cache(maxsize=1)
def _stock_data_max_ts() -> int:
    row = _connect().execute(
        """
        select max(timestamp)
        from kline_data
        where code = ? and frequency = '5'
        """,
        [SYMBOL_CODE],
    ).fetchone()
    assert row and row[0] is not None
    return int(row[0])


@lru_cache(maxsize=1)
def _fetch_5f_bars() -> list[dict]:
    rows = _connect().execute(
        """
        select
          timestamp,
          open,
          high,
          low,
          close,
          cast(volume as bigint) as volume,
          amount
        from kline_data
        where code = ? and frequency = '5'
        order by timestamp asc
        """,
        [SYMBOL_CODE],
    ).fetchall()
    return [
        {
            "time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": int(row[5] or 0),
            "amount": float(row[6] or 0),
        }
        for row in rows
    ]


def _build_current_overlay(bars: list[dict]) -> dict:
    return build_overlay(
        {
            "symbol": SYMBOL,
            "timeframe": "5f",
            "chan_levels": ["5f", "30f", "1d"],
            "modes": ["confirmed", "predictive"],
            "bars": bars,
            "chan_py_path": str(CHAN_PY_PATH),
        }
    )


@lru_cache(maxsize=1)
def _current_overlay() -> dict:
    return _build_current_overlay(_fetch_5f_bars())


@lru_cache(maxsize=3)
def _reference_levels() -> dict:
    _require_duckdb_path()
    backend_root = _old_scheme_backend_root()
    script = f"""
import json
import sys
from pathlib import Path

sys.path.insert(0, {str(backend_root)!r})

from chan_engine.analyzer import ChanAnalyzer
import duckdb

con = duckdb.connect({str(DUCKDB_PATH)!r}, read_only=True)
rows = con.execute(
    '''
    select timestamp, open, high, low, close, cast(volume as bigint) as volume, amount
    from kline_data
    where code = ? and frequency = '5'
    order by timestamp asc
    ''',
    [{SYMBOL_CODE!r}],
).fetchall()
bars = [
    {{
        "timestamp": int(row[0]),
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": int(row[5] or 0),
        "amount": float(row[6] or 0),
    }}
    for row in rows
]
result = ChanAnalyzer().analyze_multi_level({SYMBOL_CODE!r}, bars)
assert result is not None
print(json.dumps(result.to_dict()["levels"], ensure_ascii=True))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(
            "Old-scheme oracle failed:\n"
            f"STDOUT:\n{completed.stdout}\n"
            f"STDERR:\n{completed.stderr}"
        )
    return json.loads(completed.stdout)


@lru_cache(maxsize=3)
def _reference_analysis(frequency: str) -> dict:
    levels = _reference_levels()
    mapping = {
        "5": levels["5f"],
        "30": levels["30f"],
        "1D": levels["daily"],
    }
    return mapping[frequency]


def _parse_ts(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp())


def _normalize_current_strokes(overlay: dict, level: str) -> list[tuple[int, int, str, bool]]:
    return [
        (
            int(item["start"]["time"]),
            int(item["end"]["time"]),
            str(item["direction"]),
            bool(item["confirmed"]),
        )
        for item in overlay["strokes"]
        if item["level"] == level
    ]


def _normalize_reference_strokes(reference: dict) -> list[tuple[int, int, str, bool]]:
    return [
        (
            int(item["start_time"]),
            int(item["end_time"]),
            str(item["direction"]),
            bool(item["is_sure"]),
        )
        for item in reference.get("strokes", [])
    ]


def _covered_windows() -> list[dict]:
    max_ts = _stock_data_max_ts()
    return [
        window
        for window in _load_fixture()["windows"]
        if _parse_ts(window["end"]) <= max_ts
    ]


def test_regression_fixture_is_present_and_records_user_windows() -> None:
    fixture = _load_fixture()
    assert fixture["base_timeframe"] == "5f"
    ids = {window["id"] for window in fixture["windows"]}
    assert "30f-downstroke-2026-06-08-1100_to_2026-06-09-1000" in ids
    assert "5f-alternation-2026-05-22-1000_to_2026-05-26-1000" in ids
    assert "1d-center-non-connection-2026-spring" in ids


def test_stock_data_duckdb_reference_is_available() -> None:
    _require_duckdb_path()
    row = _connect().execute(
        """
        select count(*)
        from kline_data
        where code = ? and frequency = '5'
        """,
        [SYMBOL_CODE],
    ).fetchone()
    assert row and int(row[0]) > 0


def test_current_overlay_is_deterministic_for_same_stock_data_snapshot() -> None:
    _require_duckdb_path()
    bars = _fetch_5f_bars()
    first = _build_current_overlay(bars)
    second = _build_current_overlay(bars)
    assert json.dumps(first, sort_keys=True, ensure_ascii=False) == json.dumps(
        second,
        sort_keys=True,
        ensure_ascii=False,
    )


@pytest.mark.parametrize(
    ("frequency", "expected_level"),
    [("5", "5f"), ("30", "30f"), ("1D", "daily")],
)
def test_reference_analysis_json_has_expected_shape(frequency: str, expected_level: str) -> None:
    reference = _reference_analysis(frequency)
    assert reference["level"] == expected_level
    assert isinstance(reference.get("strokes"), list)
    assert isinstance(reference.get("pivots"), list)
    assert isinstance(reference.get("buy_sell_points"), list)
    if reference["strokes"]:
        sample = reference["strokes"][0]
        assert {
            "start_time",
            "end_time",
            "start_price",
            "end_price",
            "direction",
            "is_sure",
        } <= set(sample)


def test_covered_fixture_windows_have_base_5f_history() -> None:
    _require_duckdb_path()
    covered = _covered_windows()
    assert covered, "Expected at least one regression window to be covered by stock_data.db"
    for window in covered:
        begin = _parse_ts(window["begin"])
        end = _parse_ts(window["end"])
        row = _connect().execute(
            """
            select count(*)
            from kline_data
            where code = ? and frequency = '5' and timestamp between ? and ?
            """,
            [SYMBOL_CODE, begin, end],
        ).fetchone()
        assert row and int(row[0]) > 0, window["id"]


@pytest.mark.skipif(
    os.environ.get("RUN_STRICT_CHAN_REGRESSION") != "1",
    reason="opt-in strict regression against DuckDB reference snapshots",
)
@pytest.mark.parametrize(
    ("frequency", "level", "tail_size"),
    [("5", "5f", 5), ("30", "30f", 5), ("1D", "1d", 5)],
)
def test_current_overlay_matches_reference_suffix_for_common_history(
    frequency: str,
    level: str,
    tail_size: int,
) -> None:
    reference = _reference_analysis(frequency)
    reference_strokes = _normalize_reference_strokes(reference)
    assert reference_strokes, f"No reference strokes for {frequency}"
    cutoff = reference_strokes[-1][1]

    current_strokes = [
        item
        for item in _normalize_current_strokes(_current_overlay(), level)
        if item[1] <= cutoff
    ]
    assert current_strokes, f"No current strokes before cutoff for {level}"

    size = min(tail_size, len(reference_strokes), len(current_strokes))
    assert current_strokes[-size:] == reference_strokes[-size:]
