from __future__ import annotations

from datetime import UTC, datetime

import pytest

from collector.chan_recompute import validate_chan_response


def _valid_response() -> dict:
    return {
        "engine": "module-b:chan.py",
        "strokes": [
            {
                "level": "5f",
                "start": {"time": 1_700_000_000, "price": 10.0},
                "end": {"time": 1_700_000_300, "price": 10.5},
                "begin_base_ts": 1_700_000_000,
                "end_base_ts": 1_700_000_300,
            },
            {
                "level": "30f",
                "start": {"time": 1_700_000_000, "price": 10.0},
                "end": {"time": 1_700_000_300, "price": 10.5},
                "begin_base_ts": 1_700_000_000,
                "end_base_ts": 1_700_000_300,
            },
            {
                "level": "1d",
                "start": {"time": 1_700_000_000, "price": 10.0},
                "end": {"time": 1_700_000_300, "price": 10.5},
                "begin_base_ts": 1_700_000_000,
                "end_base_ts": 1_700_000_300,
            },
        ],
        "segments": [],
        "centers": [
            {
                "level": "5f",
                "start_time": 1_700_000_000,
                "end_time": 1_700_000_300,
                "begin_base_ts": 1_700_000_000,
                "end_base_ts": 1_700_000_300,
                "low": 9.8,
                "high": 10.6,
            }
        ],
        "signals": [],
        "channels": [],
    }


def test_validate_chan_response_accepts_formal_response() -> None:
    validate_chan_response(
        response=_valid_response(),
        symbol="000001.SZ",
        analysis_levels=["5f", "30f", "1d"],
        bar_from=datetime.fromtimestamp(1_700_000_000, tz=UTC),
        bar_until=datetime.fromtimestamp(1_700_000_300, tz=UTC),
        bar_count=10_000,
    )


def test_validate_chan_response_rejects_unsupported_engine_response() -> None:
    response = _valid_response()
    response["engine"] = "unsupported-engine"
    with pytest.raises(RuntimeError, match="Rejected non-formal"):
        validate_chan_response(
            response=response,
            symbol="000001.SZ",
            analysis_levels=["5f", "30f", "1d"],
            bar_from=datetime.fromtimestamp(1_700_000_000, tz=UTC),
            bar_until=datetime.fromtimestamp(1_700_000_300, tz=UTC),
            bar_count=10_000,
        )


def test_validate_chan_response_rejects_fallback_engine() -> None:
    response = _valid_response()
    response["engine"] = "fallback"
    with pytest.raises(RuntimeError, match="Rejected non-formal"):
        validate_chan_response(
            response=response,
            symbol="000001.SZ",
            analysis_levels=["5f", "30f", "1d"],
            bar_from=datetime.fromtimestamp(1_700_000_000, tz=UTC),
            bar_until=datetime.fromtimestamp(1_700_000_300, tz=UTC),
            bar_count=10_000,
        )


def test_validate_chan_response_rejects_excessive_strokes() -> None:
    response = _valid_response()
    response["strokes"] = response["strokes"] * 2000
    with pytest.raises(RuntimeError, match="too many strokes"):
        validate_chan_response(
            response=response,
            symbol="000001.SZ",
            analysis_levels=["5f", "30f", "1d"],
            bar_from=datetime.fromtimestamp(1_700_000_000, tz=UTC),
            bar_until=datetime.fromtimestamp(1_700_000_300, tz=UTC),
            bar_count=10_000,
        )
