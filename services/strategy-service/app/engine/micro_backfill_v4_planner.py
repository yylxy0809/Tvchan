from __future__ import annotations

from collections import Counter
from typing import Any

from app.engine.time_utils import cutoff_key, iso_utc


RUN_GROUP_ID = "phase_1_22_targeted_entry_window_intraday_v1"


def plan_micro_backfill_v4(expected_rows: list[dict[str, Any]], actual_whitelist_runs: list[dict[str, Any]]) -> dict[str, Any]:
    manifest, seen, rejected = [], set(), Counter()
    actual_keys = {(row.get("symbol"), row.get("level"), row.get("mode"), cutoff_key(row["cutoff_bar_end"])) for row in actual_whitelist_runs if row.get("cutoff_bar_end") is not None}
    evidence = []
    for row in expected_rows:
        cutoff = cutoff_key(row["cutoff_bar_end"]) if row.get("cutoff_bar_end") is not None else None
        key = (row.get("symbol"), row.get("level"), row.get("mode") or "predictive", cutoff)
        if not row.get("is_complete", False):
            rejected["not_expected_kline"] += 1; evidence.append({"key": key, "admitted": False, "reason": "not_complete_expected_kline"}); continue
        if key in actual_keys:
            rejected["already_covered"] += 1; evidence.append({"key": key, "admitted": False, "reason": "covered_by_whitelist_run"}); continue
        if not all(key):
            rejected["incomplete_key"] += 1; evidence.append({"key": key, "admitted": False, "reason": "incomplete_key"}); continue
        if key[3].weekday() >= 5:
            rejected["weekend_or_non_trading"] += 1; evidence.append({"key": key, "admitted": False, "reason": "weekend_or_non_trading"}); continue
        if key not in seen:
            seen.add(key)
            manifest.append({"symbol": key[0], "level": key[1], "mode": key[2], "cutoff_bar_end": iso_utc(key[3]), "run_group_id": RUN_GROUP_ID, "run_kind": "historical_backfill", "execute": False})
            evidence.append({"key": key, "admitted": True, "reason": "complete_expected_kline_uncovered_by_whitelist"})
    counts = Counter(row["level"] for row in manifest)
    for index, item in enumerate(evidence, start=1):
        item["evidence_id"] = f"backfill-evidence-{index:06d}"
    admitted = {tuple(item["key"]): item for item in evidence if item["admitted"]}
    for item in manifest:
        proof = admitted[(item["symbol"], item["level"], item["mode"], cutoff_key(item["cutoff_bar_end"]))]
        item.update({"evidence_id": proof["evidence_id"], "evidence_reason": proof["reason"], "expected_is_complete": True, "whitelist_covered": False})
    estimate = {"planned_runs": len(manifest), "by_level": dict(counts), "estimated_kline_reads": sum(1200 if row["level"] == "5f" else 200 for row in manifest), "estimated_database_rows": len(manifest), "estimated_runtime_seconds": len(manifest) * 2}
    admission = {"expected_kline_cutoff_only": bool(manifest), "no_existing_covered_cutoff": bool(manifest), "exact_symbol_level_mode_cutoff_key": bool(manifest), "published_heads_write": False, "overwrite_existing": False, "execute_hardcoded_false": True}
    return {"execute": False, "decision": "plan_only" if manifest else "no_admissible_gap", "manifest": manifest, "row_evidence": evidence, "admission": admission, "resource_estimate": estimate, "rejection_counts": dict(rejected), "raw_expected_episode_cutoff_rows": len(expected_rows), "deduplicated_rows": len(manifest), "planned_runs": len(manifest)}
