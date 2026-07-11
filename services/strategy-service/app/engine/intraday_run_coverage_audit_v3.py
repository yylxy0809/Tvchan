from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from app.engine.time_utils import cutoff_key, iso_utc


def audit_intraday_run_coverage_v3(expected_grid: list[dict[str, Any]], runs: list[dict[str, Any]], modes: tuple[str, ...] = ("predictive", "confirmed"), episodes: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    expected: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in expected_grid:
        expected[(row["episode_id"], row["symbol"], row["level"])].append(row)
    for episode in episodes or []:
        for level in ("30f", "5f"):
            expected.setdefault((episode["episode_id"], episode["symbol"], level), [])
    supported_scopes = {(str(run.get("symbol")), str(run.get("level")), str(run.get("mode"))) for run in runs}
    rows, missing, duplicates = [], [], []
    for (episode_id, symbol, level), values in sorted(expected.items()):
        values = sorted(values, key=lambda row: cutoff_key(row["cutoff_bar_end"]))
        cuts = {cutoff_key(row["cutoff_bar_end"]) for row in values}
        for mode in modes:
            matching = [run for run in runs if run.get("symbol") == symbol and run.get("level") == level and run.get("mode") == mode and run.get("cutoff_bar_end") is not None and cutoff_key(run["cutoff_bar_end"]) in cuts]
            by_cutoff: dict[Any, list[dict[str, Any]]] = defaultdict(list)
            for run in matching:
                by_cutoff[cutoff_key(run["cutoff_bar_end"])].append(run)
            covered = sorted(by_cutoff)
            missing_cuts = sorted(cuts - set(covered))
            dupes = {cutoff: group for cutoff, group in by_cutoff.items() if len(group) > 1}
            if not values:
                classification = "not_applicable"
            elif (symbol, level, mode) not in supported_scopes:
                classification = "unsupported_mode"
            elif not covered:
                classification = "none"
            elif len(covered) == len(values):
                classification = "complete"
            else:
                classification = "partial"
            row = {"episode_id": episode_id, "symbol": symbol, "level": level, "mode": mode, "expected_cutoff_count": len(values), "covered_cutoff_count": len(covered), "missing_cutoff_count": len(missing_cuts), "duplicate_cutoff_count": len(dupes), "coverage_ratio": len(covered) / len(values) if values else 0.0, "coverage_classification": classification, "first_expected_cutoff": iso_utc(values[0]["cutoff_bar_end"]) if values else None, "last_expected_cutoff": iso_utc(values[-1]["cutoff_bar_end"]) if values else None, "first_actual_cutoff": iso_utc(covered[0]) if covered else None, "last_actual_cutoff": iso_utc(covered[-1]) if covered else None, "run_groups_seen": sorted({str(run.get("run_group_id")) for run in matching}), "missing_cutoffs": [{"episode_id": episode_id, "symbol": symbol, "level": level, "mode": mode, "cutoff_bar_end": iso_utc(cutoff)} for cutoff in missing_cuts]}
            rows.append(row)
            # Unsupported modes are reported for observability, not admitted as data gaps.
            if classification != "unsupported_mode":
                missing.extend(row["missing_cutoffs"])
            for cutoff, group in dupes.items():
                duplicates.append({"episode_id": episode_id, "symbol": symbol, "level": level, "mode": mode, "cutoff_bar_end": iso_utc(cutoff), "run_ids": [run.get("run_id") for run in group], "run_groups": sorted({str(run.get("run_group_id")) for run in group})})
    counts = Counter(row["coverage_classification"] for row in rows)
    for name in ("complete", "partial", "none", "not_applicable", "unsupported_mode"):
        counts.setdefault(name, 0)
    return {"rows": rows, "missing_cutoffs": missing, "duplicate_cutoffs": duplicates, "summary": {"coverage_classification_counts": dict(counts), "not_applicable_sample_count": counts["not_applicable"], "all_ratios_bounded": all(0 <= row["coverage_ratio"] <= 1 for row in rows)}}
