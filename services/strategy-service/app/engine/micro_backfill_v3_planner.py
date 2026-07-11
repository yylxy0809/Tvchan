from __future__ import annotations

from typing import Any


RUN_GROUP_ID = "phase_1_20r_targeted_entry_window_intraday_v3"


def plan_micro_backfill_v3(coverage_rows: list[dict[str, Any]], *, resource_ok: bool) -> dict[str, Any]:
    manifest, seen = [], set()
    eligible = [row for row in coverage_rows if row.get("coverage_classification") in {"partially_covered", "not_covered"} and not row.get("missing_due_to_stale")]
    for row in eligible:
        for missing in row.get("missing_cutoffs", []):
            key = (row.get("symbol"), missing.get("level"), missing.get("cutoff_bar_end"))
            if all(key) and key not in seen:
                seen.add(key)
                manifest.append({"symbol": key[0], "level": key[1], "cutoff_bar_end": key[2], "run_group_id": RUN_GROUP_ID, "run_kind": "historical_backfill", "episode_id": row.get("episode_id")})
    execute = bool(eligible and manifest and resource_ok)
    return {"execute": execute, "decision": "execute" if execute else "do_not_execute", "admission": {"coverage_gap_exists": bool(eligible), "not_stale_only": bool(eligible), "unique_isolated_run_group": True, "published_heads_write": False, "overwrite_existing": False, "resource_ok": resource_ok}, "manifest": manifest, "block_reasons": [] if execute else ["no_deduplicated_non_stale_missing_cutoffs" if not manifest else "resource_estimate_not_approved"]}
