from __future__ import annotations

from typing import Any

from app.engine.time_utils import utc_time


def _time(value: Any):
    return utc_time(value)


def _iso(value: Any) -> str:
    return _time(value).isoformat()


def build_expected_intraday_cutoff_grid(episodes: list[dict[str, Any]], klines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map actual completed native intraday bars into each inclusive trigger window."""
    result, seen = [], set()
    for episode in episodes:
        start, end = _time(episode["daily_setup_first_seen_time"]), _time(episode["trigger_window_end"])
        for bar in klines:
            if bar.get("symbol") != episode.get("symbol") or not bar.get("is_complete") or bar.get("timeframe") not in (5, 30):
                continue
            cutoff = _time(bar["ts"])
            if not start <= cutoff <= end:
                continue
            level = f"{bar['timeframe']}f"
            key = (episode["episode_id"], episode["symbol"], level, cutoff)
            if key not in seen:
                seen.add(key)
                result.append({"episode_id": episode["episode_id"], "symbol": episode["symbol"], "level": level, "timeframe": bar["timeframe"], "is_complete": bool(bar["is_complete"]), "cutoff_bar_end": _iso(cutoff), "trigger_window_start": _iso(start), "trigger_window_end": _iso(end)})
    return sorted(result, key=lambda row: (row["episode_id"], row["level"], row["cutoff_bar_end"]))
