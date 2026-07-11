from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any


ALLOWED_RUN_GROUPS = {"research_daily_close", "phase_1_15_targeted_entry_window_intraday", "phase_1_16_targeted_entry_window_intraday_v2", "phase_1_20r_targeted_entry_window_intraday_v3"}


def _fingerprint(row: dict[str, Any]) -> str:
    return "|".join(_iso(row.get(key)) for key in ("symbol_id", "chan_level", "mode", "side", "bsp_type", "signal_point_time", "price_x1000"))


def _iso(value: Any) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value or "")


def _time(value: Any) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def build_signal_event_ledger_v2(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("status") != "success" or row.get("run_kind") != "historical_backfill" or row.get("mode") != "predictive":
            continue
        if row.get("run_group_id") not in ALLOWED_RUN_GROUPS:
            continue
        if not all(row.get(key) is not None for key in ("symbol_id", "chan_level", "mode", "side", "bsp_type", "signal_point_time", "price_x1000", "cutoff_bar_end")):
            continue
        if _time(row["signal_point_time"]) > _time(row["cutoff_bar_end"]):
            continue
        grouped[_fingerprint(row)].append(row)
    events = []
    for fingerprint, matches in grouped.items():
        matches.sort(key=lambda item: (str(item["cutoff_bar_end"]), int(item["run_id"])))
        first, last = matches[0], matches[-1]
        events.append({"fingerprint": fingerprint, "symbol_id": first["symbol_id"], "symbol": first.get("symbol"), "chan_level": first["chan_level"], "mode": first["mode"], "side": first["side"], "bsp_type": first["bsp_type"], "signal_point_time": _iso(first["signal_point_time"]), "price_x1000": first["price_x1000"], "first_seen_time": _iso(first["cutoff_bar_end"]), "last_seen_time": _iso(last["cutoff_bar_end"]), "confirm_time": min((_iso(row["cutoff_bar_end"]) for row in matches if row.get("is_confirmed")), default=None), "source_run_ids": [int(row["run_id"]) for row in matches], "source_run_groups": sorted({str(row["run_group_id"]) for row in matches}), "observed_run_count": len(matches)})
    return sorted(events, key=lambda item: (item["first_seen_time"], item["fingerprint"]))
