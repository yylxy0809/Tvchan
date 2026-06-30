from __future__ import annotations


def build_overlay(request: dict) -> dict:
    """
    Adapter contract for integrating the open-source Vespa314/chan.py project.

    Input:
      request = {
        "symbol": str,
        "timeframe": str,
        "chan_levels": list[str],
        "modes": list[str],
        "bars": list[{
          "time": int,
          "open": float,
          "high": float,
          "low": float,
          "close": float,
          "volume": int,
        }]
      }

    Output:
      {
        "symbol": str,
        "timeframe": str,
        "engine": "chan.py",
        "strokes": list[...],
        "segments": list[...],
        "centers": list[...],
        "signals": list[...],
      }

    This file is intentionally a template. Copy it near your local chan.py repo,
    implement the official object mapping there, and point CHAN_PY_PATH at the
    resulting adapter.py file or its parent directory.
    """
    raise NotImplementedError(
        "Implement build_overlay(request) against your local chan.py checkout"
    )
