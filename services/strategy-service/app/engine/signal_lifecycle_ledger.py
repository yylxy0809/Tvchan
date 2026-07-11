from __future__ import annotations

from collections import defaultdict
from bisect import bisect_right
from itertools import groupby
from typing import Any

from app.engine.time_utils import iso_utc, utc_time


def _time(value: Any):
    return utc_time(value)


def _identity(run: dict[str, Any], signal: dict[str, Any]) -> str:
    return "|".join(str(value) for value in (run.get("symbol"), run.get("level"), run.get("mode"), signal.get("side"), signal.get("bsp_type"), iso_utc(signal.get("signal_point_time")), signal.get("price_x1000")))


def build_signal_lifecycle(runs: list[dict[str, Any]], expected_grid: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Build visibility from historical snapshots, using completed-Kline cutoffs for continuity."""
    expected_by_scope: dict[tuple[str, str], set[datetime]] = defaultdict(set)
    for row in expected_grid or []:
        if row.get("cutoff_bar_end"):
            expected_by_scope[(str(row.get("symbol")), str(row.get("level")))].add(_time(row["cutoff_bar_end"]))
    by_scope: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        by_scope[(str(run.get("symbol")), str(run.get("level")), str(run.get("mode")))].append(run)
    expected_times = {scope: sorted(values) for scope, values in expected_by_scope.items()}
    result = []
    for scope_runs in by_scope.values():
        scope_runs.sort(key=lambda row: (_time(row["cutoff_bar_end"]), row.get("run_id", 0)))
        # A cutoff is one observation instant. Merge duplicate runs before lifecycle transitions;
        # an empty duplicate must never manufacture a disappearance beside a signal duplicate.
        merged_runs = []
        for cutoff, duplicate_runs in groupby(scope_runs, key=lambda row: _time(row["cutoff_bar_end"])):
            bucket = list(duplicate_runs)
            seed = dict(bucket[0])
            signals: dict[str, dict[str, Any]] = {}
            for run in bucket:
                for signal in run.get("signals", []):
                    key = _identity(run, signal)
                    existing = signals.get(key)
                    if existing is None:
                        signals[key] = {
                            **signal,
                            "_first_run_id": run.get("run_id"),
                            "_last_run_id": run.get("run_id"),
                            "_confirm_run_id": run.get("run_id") if signal.get("is_confirmed") else None,
                        }
                    else:
                        existing["_last_run_id"] = run.get("run_id")
                        if signal.get("is_confirmed") and not existing.get("is_confirmed"):
                            existing["_confirm_run_id"] = run.get("run_id")
                        existing["is_confirmed"] = bool(existing.get("is_confirmed") or signal.get("is_confirmed"))
            seed["signals"] = list(signals.values())
            seed["duplicate_run_ids"] = [row.get("run_id") for row in bucket]
            seed["duplicate_run_groups"] = [row.get("run_group_id") for row in bucket]
            seed["duplicate_cutoff_conflict"] = len(bucket) > 1 and bool(signals)
            merged_runs.append(seed)
        scope_runs = merged_runs
        identities: dict[str, dict[str, Any]] = {}
        previous: set[str] = set()
        previous_cutoff: datetime | None = None
        for run in scope_runs:
            cutoff = _time(run["cutoff_bar_end"])
            current = {_identity(run, signal): signal for signal in run.get("signals", [])}
            expected_between = expected_times.get((str(run["symbol"]), str(run["level"])), [])
            next_expected_index = bisect_right(expected_between, previous_cutoff) if previous_cutoff else 0
            scope_expected = expected_by_scope.get((str(run["symbol"]), str(run["level"])), set())
            gap = previous_cutoff is not None and (not scope_expected or cutoff not in scope_expected or (next_expected_index < len(expected_between) and expected_between[next_expected_index] < cutoff))
            for key, signal in current.items():
                item = identities.setdefault(key, {"identity": key, "symbol": run["symbol"], "level": run["level"], "mode": run["mode"], "side": signal.get("side"), "bsp_type": signal.get("bsp_type"), "point_time": iso_utc(signal.get("signal_point_time")), "price_x1000": signal.get("price_x1000"), "parent_30f_identity": signal.get("parent_30f_identity"), "first_seen_time": None, "confirm_time": None, "disappear_time": None, "first_seen_run_id": None, "confirm_run_id": None, "last_seen_run_id": None, "last_seen_time": None, "source_run_groups": set(), "events": [], "cutoff_gap": False, "visible_cutoffs": [], "_was_seen": False})
                if key not in previous:
                    event = "REAPPEARED" if item["_was_seen"] else "APPEARED"
                    item["events"].append(event)
                    if item["first_seen_time"] is None:
                        item["first_seen_time"], item["first_seen_run_id"] = cutoff.isoformat(), signal.get("_first_run_id") or run["run_id"]
                else:
                    item["events"].append("PERSISTED")
                if signal.get("is_confirmed") and item["confirm_time"] is None:
                    item["events"].append("CONFIRMED")
                    item["confirm_time"], item["confirm_run_id"] = cutoff.isoformat(), signal.get("_confirm_run_id") or run["run_id"]
                item["last_seen_run_id"], item["last_seen_time"], item["_was_seen"] = signal.get("_last_run_id") or run["run_id"], cutoff.isoformat(), True
                item["visible_cutoffs"].append(cutoff.isoformat())
                item["source_run_groups"].update(str(group) for group in run.get("duplicate_run_groups", [run.get("run_group_id")]))
            for key in previous - set(current):
                item = identities[key]
                item["cutoff_gap"] |= gap
                if gap:
                    item["events"].append("DISAPPEARANCE_INDETERMINATE_GAP")
                elif item["disappear_time"] is None:
                    item["events"].append("DISAPPEARED")
                    item["disappear_time"] = cutoff.isoformat()
            previous, previous_cutoff = set(current), cutoff
        for item in identities.values():
            item["source_run_groups"] = sorted(item["source_run_groups"])
            item.pop("_was_seen")
            result.append(item)
    return sorted(result, key=lambda row: (row["first_seen_time"] or "", row["identity"]))
