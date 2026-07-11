from __future__ import annotations

import json
from typing import Any

CHAN_EVENT_SCHEMA_VERSION = "chan-event.v1"
CHAN_SOURCE_EVENT_SCHEMA_VERSION = "chan-head.v1"
OBJECT_GROUPS = ("strokes", "segments", "centers", "signals", "channels")
MAX_OBJECTS_PER_EVENT = 10_000
MAX_EVENT_BYTES = 2_000_000


class ChanEventValidationError(ValueError):
    pass


def validate_chan_event(event: dict[str, Any]) -> dict[str, Any]:
    """Validate the wire format before it is put on the websocket."""
    allowed = {
        "type",
        "schema_version",
        "kind",
        "id",
        "symbol",
        "chart_timeframe",
        "modes",
        "snapshot_version",
        "base_version",
        "sequence",
        "range",
        "upserts",
        "deletes",
    }
    missing = allowed - set(event)
    unknown = set(event) - allowed
    if missing or unknown:
        raise ChanEventValidationError(
            "Chan event envelope fields do not match schema: "
            f"missing={sorted(missing)}, unknown={sorted(unknown)}"
        )
    if (
        event.get("type") != "chan_overlay"
        or event.get("schema_version") != CHAN_EVENT_SCHEMA_VERSION
    ):
        raise ChanEventValidationError("Unsupported Chan event schema")
    kind = event.get("kind")
    if kind not in {"snapshot", "delta"}:
        raise ChanEventValidationError("Chan event kind must be snapshot or delta")
    if not isinstance(event.get("id"), str) or not event["id"].strip():
        raise ChanEventValidationError("Chan event id is required")
    if not isinstance(event.get("symbol"), str) or not event["symbol"].strip():
        raise ChanEventValidationError("Chan event symbol is required")
    if (
        not isinstance(event.get("chart_timeframe"), str)
        or not event["chart_timeframe"].strip()
    ):
        raise ChanEventValidationError("Chan event chart_timeframe is required")
    modes = event.get("modes")
    if not isinstance(modes, list) or not modes or len(set(modes)) != len(modes):
        raise ChanEventValidationError(
            "Chan event modes must be a unique non-empty list"
        )
    if not all(isinstance(mode, str) and mode for mode in modes):
        raise ChanEventValidationError("Chan event modes must be strings")
    if (
        not isinstance(event.get("snapshot_version"), str)
        or not event["snapshot_version"]
    ):
        raise ChanEventValidationError("Chan event snapshot_version is required")
    if kind == "delta":
        if not isinstance(event.get("base_version"), str) or not event["base_version"]:
            raise ChanEventValidationError("Chan delta base_version is required")
        if event["base_version"] == event["snapshot_version"]:
            raise ChanEventValidationError("Chan delta must advance snapshot_version")
    elif event.get("base_version") is not None:
        raise ChanEventValidationError("Chan snapshot cannot have base_version")
    sequence = event.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise ChanEventValidationError("Chan event sequence must be a positive integer")
    _validate_range(event.get("range"))
    object_count = _validate_changes(event.get("upserts"), event.get("deletes"))
    if kind == "delta" and object_count == 0:
        raise ChanEventValidationError("Chan delta must contain real changes")
    if (
        len(json.dumps(event, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
        > MAX_EVENT_BYTES
    ):
        raise ChanEventValidationError("Chan event exceeds the payload limit")
    return event


def validate_chan_source_event(event: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "type",
        "schema_version",
        "id",
        "symbol",
        "level",
        "mode",
        "sequence",
        "snapshot_version",
        "run_id",
        "bar_until",
    }
    if set(event) != allowed:
        raise ChanEventValidationError(
            "Chan source event envelope fields do not match schema: "
            f"missing={sorted(allowed - set(event))}, unknown={sorted(set(event) - allowed)}"
        )
    if (
        event.get("type") != "chan_head_update"
        or event.get("schema_version") != CHAN_SOURCE_EVENT_SCHEMA_VERSION
    ):
        raise ChanEventValidationError("Unsupported Chan source event schema")
    for field in ("id", "symbol", "level", "mode", "snapshot_version", "bar_until"):
        if not isinstance(event.get(field), str) or not event[field].strip():
            raise ChanEventValidationError(f"Chan source event {field} is required")
    for field in ("sequence", "run_id"):
        value = event.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ChanEventValidationError(
                f"Chan source event {field} must be a positive integer"
            )
    return event


def overlay_objects(overlay: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    return {
        group: [dict(item) for item in overlay.get(group, [])]
        for group in OBJECT_GROUPS
    }


def diff_objects(
    previous: dict[str, list[dict[str, Any]]], current: dict[str, list[dict[str, Any]]]
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[str]]]:
    upserts: dict[str, list[dict[str, Any]]] = {}
    deletes: dict[str, list[str]] = {}
    for group in OBJECT_GROUPS:
        before = _by_id(previous.get(group, []), group)
        after = _by_id(current.get(group, []), group)
        upserts[group] = [
            item for stable_id, item in after.items() if before.get(stable_id) != item
        ]
        deletes[group] = [stable_id for stable_id in before if stable_id not in after]
    return upserts, deletes


def _validate_range(value: Any) -> None:
    if not isinstance(value, dict) or set(value) != {"from", "to"}:
        raise ChanEventValidationError(
            "Chan event range must contain inclusive from and to"
        )
    start, end = value["from"], value["to"]
    if (
        any(
            not isinstance(item, int) or isinstance(item, bool) for item in (start, end)
        )
        or start > end
    ):
        raise ChanEventValidationError(
            "Chan event range must be an inclusive integer interval"
        )


def _validate_changes(upserts: Any, deletes: Any) -> int:
    if not isinstance(upserts, dict) or not isinstance(deletes, dict):
        raise ChanEventValidationError("Chan event upserts and deletes are required")
    if set(upserts) != set(OBJECT_GROUPS) or set(deletes) != set(OBJECT_GROUPS):
        raise ChanEventValidationError(
            "Chan event changes must be grouped by stable object type"
        )
    count = 0
    for group in OBJECT_GROUPS:
        if not isinstance(upserts[group], list) or not isinstance(deletes[group], list):
            raise ChanEventValidationError("Chan event change groups must be lists")
        _by_id(upserts[group], group)
        delete_ids = deletes[group]
        if any(not isinstance(item, str) or not item.strip() for item in delete_ids):
            raise ChanEventValidationError("Chan deletes must contain stable IDs")
        if len(set(delete_ids)) != len(delete_ids):
            raise ChanEventValidationError("Chan deletes contain duplicate stable IDs")
        count += len(upserts[group]) + len(delete_ids)
    if count > MAX_OBJECTS_PER_EVENT:
        raise ChanEventValidationError("Chan event exceeds the object limit")
    return count


def _by_id(items: list[dict[str, Any]], group: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            raise ChanEventValidationError(f"Chan {group} upserts must be objects")
        stable_id = item.get("id")
        if not isinstance(stable_id, str) or not stable_id.strip():
            raise ChanEventValidationError(f"Chan {group} requires a stable id")
        if stable_id in result:
            raise ChanEventValidationError(
                f"Chan {group} contains duplicate stable id {stable_id}"
            )
        result[stable_id] = item
    return result
