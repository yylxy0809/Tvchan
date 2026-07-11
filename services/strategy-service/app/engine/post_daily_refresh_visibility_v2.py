from __future__ import annotations

from datetime import datetime
from typing import Any


def _time(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def audit_post_daily_refresh_visibility_v2(episodes: list[dict[str, Any]], ledger: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for episode in episodes:
        setup, as_of = _time(episode["daily_setup_first_seen_time"]), _time(episode["as_of_time"])
        seen: set[str] = set()
        for signal in ledger:
            fingerprint = str(signal.get("fingerprint"))
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            first_seen = _time(signal["first_seen_time"])
            if first_seen <= setup:
                reason, visible = "stale_signal", False
            elif first_seen > as_of:
                reason, visible = "first_seen_after_as_of", False
            else:
                reason, visible = "visible_refresh", True
            rows.append({"episode_id": episode["episode_id"], "fingerprint": fingerprint, "first_seen_time": signal["first_seen_time"], "signal_point_time": signal.get("signal_point_time"), "historically_visible": visible, "reason": reason, "source_run_ids": signal.get("source_run_ids", []), "source_run_groups": signal.get("source_run_groups", [])})
    return {"rows": rows}
