from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.engine.time_utils import utc_time


class OfficialLifecycleUnavailable(RuntimeError):
    pass


PROFILE_DATASET = {
    "historical_replay": "official",
    "online": "observable",
    "baseline": "diagnostic",
}


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return utc_time(value).isoformat()
    return value


def _event_record(row: dict[str, Any], dataset_class: str) -> dict[str, Any]:
    event_type = str(row["event_type"])
    return {
        "dataset_class": dataset_class,
        "record_kind": "event",
        "event_id": int(row["id"]),
        "fingerprint": str(row["fingerprint"]),
        "symbol_id": int(row["symbol_id"]),
        "chan_level": int(row["chan_level"]),
        "structure_type": str(row["structure_type"]),
        "event_type": event_type,
        "effective_time": _serialize(row["effective_time"]),
        "observed_time": _serialize(row["observed_time"]),
        "point_time": _serialize(row["point_time"]),
        "first_seen_time": _serialize(row["effective_time"]) if event_type == "first_seen" else None,
        "confirm_time": _serialize(row["effective_time"]) if event_type == "confirmed" else None,
        "current_mode": row.get("current_mode"),
        "run_id": row.get("run_id"),
        "publication_profile": str(row["publication_profile"]),
        "provenance": _mapping(row.get("provenance")),
    }


def _current_record(row: dict[str, Any], dataset_class: str, profile: str) -> dict[str, Any]:
    return {
        "dataset_class": dataset_class,
        "record_kind": "current",
        "fingerprint": str(row["fingerprint"]),
        "symbol_id": int(row["symbol_id"]),
        "chan_level": int(row["chan_level"]),
        "structure_type": str(row["structure_type"]),
        "point_time": _serialize(row["point_time"]),
        "first_seen_time": _serialize(row.get("first_seen_time")),
        "confirm_time": _serialize(row.get("confirm_time")),
        "disappear_time": _serialize(row.get("disappear_time")),
        "current_status": row.get("current_status"),
        "current_mode": row.get("current_mode"),
        "run_id": row.get("last_seen_run_id"),
        "publication_profile": profile,
        "provenance": _mapping(row.get("provenance")),
    }


def build_lifecycle_datasets(
    *,
    events: list[dict[str, Any]],
    current: list[dict[str, Any]],
    as_of_time: datetime,
) -> dict[str, Any]:
    as_of = utc_time(as_of_time)
    datasets: dict[str, list[dict[str, Any]]] = {name: [] for name in ("official", "observable", "diagnostic")}
    future_rows = 0
    for row in events:
        effective_time = utc_time(row["effective_time"])
        observed_time = utc_time(row["observed_time"])
        if effective_time > as_of or observed_time > as_of:
            future_rows += 1
            continue
        profile = str(row["publication_profile"])
        dataset_class = PROFILE_DATASET.get(profile)
        if dataset_class is None:
            raise ValueError(f"Unsupported lifecycle publication profile: {profile}")
        datasets[dataset_class].append(_event_record(row, dataset_class))

    for row in current:
        provenance = _mapping(row.get("provenance"))
        profile = str(provenance.get("publication_profile") or "baseline")
        if profile == "historical_replay":
            continue
        dataset_class = PROFILE_DATASET.get(profile)
        if dataset_class is None:
            raise ValueError(f"Unsupported current lifecycle publication profile: {profile}")
        datasets[dataset_class].append(_current_record(row, dataset_class, profile))

    official_event_count = sum(row["record_kind"] == "event" for row in datasets["official"])
    blockers = [] if official_event_count else ["historical_replay_lifecycle_unavailable"]
    baseline_fake_first_seen = sum(
        row["publication_profile"] == "baseline" and row.get("first_seen_time") is not None
        for row in datasets["diagnostic"]
        if row["record_kind"] == "event"
    )
    if baseline_fake_first_seen:
        blockers.append("baseline_fabricated_first_seen")
    if future_rows:
        blockers.append("future_lifecycle_rows_detected")
    return {
        "as_of_time": as_of.isoformat(),
        "source_contract": "chan_structure_lifecycle_events/current_only",
        "datasets": datasets,
        "counts": {name: len(rows) for name, rows in datasets.items()},
        "official_ready": not blockers,
        "future_rows_rejected": future_rows,
        "blockers": blockers,
        "decision": "GO" if not blockers else "NO_GO",
    }


def require_official_dataset(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not payload.get("official_ready"):
        raise OfficialLifecycleUnavailable(
            ",".join(payload.get("blockers") or ["official_lifecycle_unavailable"])
        )
    return list(payload["datasets"]["official"])
