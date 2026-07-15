from __future__ import annotations

import hashlib
import json
from typing import Any


def build_official_historical_gate(snapshot: dict[str, Any]) -> dict[str, Any]:
    counts = snapshot["counts"]
    stages = [
        {"gate": "source_high_level_eligible", "count": int(counts["source_high_level_eligible"])},
        {"gate": "official_high_level_visible", "count": int(counts["official_high_level_visible"])},
        {"gate": "intraday_5f_30f_eligible", "count": int(counts["intraday_eligible"])},
        {"gate": "predictive_weekly_b1_visible", "count": int(counts["predictive_weekly_b1"])},
        {"gate": "predictive_weekly_b2_visible", "count": int(counts["predictive_weekly_b2"])},
        {"gate": "strict_daily_setup_episode", "count": int(counts["strict_daily_episodes"])},
        {"gate": "official_30f_confirmation", "count": int(counts["official_30f_confirmations"])},
        {"gate": "official_5f_parent_bound_confirmation", "count": int(counts["official_5f_confirmations"])},
        {"gate": "official_event_replay_candidate", "count": int(counts["official_candidates"])},
    ]
    monotonic = all(current["count"] <= previous["count"] for previous, current in zip(stages, stages[1:]))
    blockers = []
    if not monotonic:
        blockers.append("non_monotonic_gate_waterfall")
    if not counts["predictive_weekly_b2"]:
        blockers.append("official_predictive_weekly_b2_unavailable")
    if not counts["strict_daily_episodes"]:
        blockers.append("strict_daily_setup_episode_unavailable")
    if int(counts["official_candidates"]) < 3:
        blockers.append("insufficient_complete_official_traces")
    decision = "GO" if not blockers else "NO_GO"
    digest_input = {"as_of_time": snapshot["as_of_time"], "stages": stages, "blockers": blockers}
    return {
        **snapshot,
        "strategy_code": "weekly_daily_b2_resonance_v1",
        "source_contract": "historical_replay_official_lifecycle_only",
        "gate_stages": stages,
        "gate_counts_monotonic": monotonic,
        "blockers": blockers,
        "decision": decision,
        "input_hash": hashlib.sha256(
            json.dumps(digest_input, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
