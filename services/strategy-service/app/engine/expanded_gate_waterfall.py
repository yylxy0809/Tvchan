from __future__ import annotations

from collections import Counter
from typing import Any


GATES = ("weekly_b1", "weekly_b2", "weekly_price_relation_valid", "weekly_dif_gt_zero", "daily_b1", "daily_first_up_strength_ge_70", "daily_b2", "entry_watch", "fresh_30f_b1_appeared", "30f_b1_confirmed", "daily_bottom_fractal", "valid_5f_b2_b2s_confirmation", "official_ge_70_trigger", "next_30f_execution_bar_available")


def build_gate_waterfall(episodes: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for item in episodes:
        row = {"symbol": item.get("symbol"), "episode_id": item.get("episode_id"), "year": str(item.get("daily_setup_first_seen_time", ""))[:4], "gate_status": {}}
        blocked = None
        for gate in GATES:
            if blocked:
                row[gate] = None; row["gate_status"][gate] = "not_evaluated_after_blocker"; continue
            if gate == "weekly_dif_gt_zero" and item.get("weekly_dif_status") != "reconstructed":
                row[gate] = None; row["gate_status"][gate] = "blocked_unreconstructable"; blocked = "weekly_dif_not_reconstructable"; continue
            if gate == "daily_first_up_strength_ge_70" and item.get("daily_first_up_strength_status") != "reconstructed":
                row[gate] = None; row["gate_status"][gate] = "blocked_unreconstructable"; blocked = "daily_first_up_strength_not_reconstructable"; continue
            if gate == "weekly_dif_gt_zero": value = float(item["weekly_dif"]) > 0
            elif gate == "daily_first_up_strength_ge_70": value = float(item["daily_first_up_strength"]) >= 70
            else: value = bool(item.get(gate))
            row[gate] = value; row["gate_status"][gate] = "passed" if value else "failed"
            if not value: blocked = gate
        row["blocker"] = blocked
        rows.append(row)
    return {"rows": rows, "gate_pass_counts": {gate: sum(row[gate] is True for row in rows) for gate in GATES}, "gate_blocked_counts": {gate: sum(row["gate_status"][gate] == "blocked_unreconstructable" for row in rows) for gate in GATES}, "blocker_counts": dict(Counter(row["blocker"] for row in rows))}
