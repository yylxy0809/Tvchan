from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Any


def _id(prefix: str, *parts: object) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return f"{prefix}_{hashlib.sha256(raw.encode()).hexdigest()[:16]}"


def build_weekly_context_episodes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        fingerprint = row.get("weekly_signal_fingerprint") or row.get("weekly_context_signal_fingerprint") or row.get("weekly_context_signal_time")
        first_seen = row.get("weekly_context_first_seen_time") or row.get("weekly_context_time") or row.get("weekly_context_signal_time")
        grouped[(str(row.get("symbol") or ""), str(fingerprint or ""), str(first_seen or ""))].append(row)
    episodes = []
    for (symbol, fingerprint, first_seen), observations in grouped.items():
        episodes.append({"episode_id": _id("weekly", symbol, fingerprint, first_seen), "symbol": symbol, "weekly_signal_fingerprint": fingerprint, "weekly_context_first_seen_time": first_seen, "observation_count": len(observations)})
    return sorted(episodes, key=lambda item: item["episode_id"])


def build_daily_setup_episodes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        weekly_id = str(row.get("weekly_context_episode_id") or "")
        fingerprint = row.get("daily_setup_signal_fingerprint") or row.get("daily_signal_fingerprint") or row.get("daily_setup_point_time") or row.get("daily_setup_first_seen_time")
        grouped[(weekly_id, str(fingerprint or ""))].append(row)
    episodes = []
    for (weekly_id, fingerprint), observations in grouped.items():
        sample = observations[0]
        first_seen = min((str(item.get("daily_setup_first_seen_time") or item.get("daily_setup_point_time") or "") for item in observations), default="")
        episodes.append({"episode_id": _id("daily", weekly_id, fingerprint), "weekly_context_episode_id": weekly_id, "daily_setup_signal_fingerprint": fingerprint, "daily_setup_first_seen_time": first_seen, "daily_setup_point_time": sample.get("daily_setup_point_time") or first_seen, "as_of_time": max((str(item.get("as_of_time") or "") for item in observations), default=""), "symbol": sample.get("symbol"), "observation_count": len(observations)})
    return sorted(episodes, key=lambda item: item["episode_id"])
