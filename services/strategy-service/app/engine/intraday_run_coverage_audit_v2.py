from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any


def _time(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def audit_intraday_run_coverage_v2(episodes: list[dict[str, Any]], runs: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for episode in episodes:
        start, end = _time(episode["daily_setup_first_seen_time"]), _time(episode["trigger_window_end"])
        related = [run for run in runs if run.get("symbol") == episode.get("symbol") and run.get("level") in {"30f", "5f"}]
        inside = [run for run in related if start <= _time(run["cutoff_bar_end"]) <= end]
        thirties = [run for run in related if run.get("level") == "30f"]
        fives = [run for run in related if run.get("level") == "5f"]
        before = [run for run in thirties if _time(run["cutoff_bar_end"]) <= start]
        after = [run for run in thirties if _time(run["cutoff_bar_end"]) >= start]
        window_after = [run for run in thirties if _time(run["cutoff_bar_end"]) >= end]
        ratio = len(inside) / 2 if inside else 0.0
        classification = "fully_covered" if len([run for run in inside if run.get("level") == "30f"]) and len([run for run in inside if run.get("level") == "5f"]) else ("partially_covered" if inside else "not_covered")
        rows.append({"episode_id": episode["episode_id"], "symbol": episode.get("symbol"), "daily_setup_first_seen_time": episode["daily_setup_first_seen_time"], "trigger_window_start": episode["daily_setup_first_seen_time"], "trigger_window_end": episode["trigger_window_end"], "nearest_30f_run_before_setup": max(before, key=lambda x: _time(x["cutoff_bar_end"]))["run_id"] if before else None, "first_30f_run_after_setup": min(after, key=lambda x: _time(x["cutoff_bar_end"]))["run_id"] if after else None, "last_30f_run_before_window_end": max([run for run in thirties if _time(run["cutoff_bar_end"]) <= end], key=lambda x: _time(x["cutoff_bar_end"]), default={}).get("run_id"), "first_30f_run_after_window_end": min(window_after, key=lambda x: _time(x["cutoff_bar_end"]), default={}).get("run_id"), "30f_run_count_inside_window": len([run for run in inside if run.get("level") == "30f"]), "5f_run_count_inside_window": len([run for run in inside if run.get("level") == "5f"]), "covered_30f_cutoff_count": len(thirties), "covered_5f_cutoff_count": len(fives), "expected_cutoff_count": 2, "coverage_ratio": ratio, "run_groups_seen": sorted({str(run.get("run_group_id")) for run in related}), "coverage_classification": classification, "missing_reason": None if related else "no_intraday_runs_for_episode_symbol", "missing_due_to_stale": False, "missing_cutoffs": []})
    return {"rows": rows, "summary": {"coverage_classification_counts": dict(Counter(row["coverage_classification"] for row in rows))}}
