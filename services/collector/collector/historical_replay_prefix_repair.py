"""Manifest-fenced repair of a broken historical replay predecessor prefix.

This is an exceptional operator tool.  It never edits replay runs, heads,
tasks, batches, or K-lines.  ``apply`` archives the complete before image,
repairs only the declared history/outbox rows, removes their derived events,
and resets their outbox leases for the canonical lifecycle observer to replay.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID

import asyncpg

from collector.historical_replay import finalize_replay_batch
from collector.lifecycle_observer import (
    HISTORY_PAYLOAD_IDENTITY_FIELDS,
    LifecycleObserver,
    plan_events,
)


CONTRACT_VERSION = "historical-replay-prefix-repair-v1"
HASH_LENGTH = 64
TRY_LOCK_SQL = "select pg_try_advisory_lock(hashtext('chan-lifecycle-v1'))"
UNLOCK_SQL = "select pg_advisory_unlock(hashtext('chan-lifecycle-v1'))"

LOCK_PARENT_SQL = """
select id, status, batch_kind, run_group_id, config_hash
  from chan_c_batches where id = $1 for update
"""
LOCK_CHILD_SQL = """
select batch_id, status, contract_hash
  from chan_c_historical_replay_batches where batch_id = $1 for update
"""
LOCK_HISTORIES_SQL = """
select * from chan_c_head_history where id = any($1::bigint[]) order by id for update
"""
LOCK_OUTBOX_SQL = """
select * from chan_c_head_outbox where id = any($1::bigint[]) order by id for update
"""
LOCK_EVENTS_SQL = """
select * from chan_structure_lifecycle_events
 where head_history_id = any($1::bigint[]) order by id for update
"""

PREFIX_ANOMALIES_SQL = """
with actual_heads as materialized (
    select head.batch_id, head.symbol_id, head.chan_level, head.mode,
           head.cutoff_time, head.run_id
      from chan_c_historical_replay_heads head
     where head.batch_id = $1
), ordered as materialized (
    select head.*,
           lag(head.run_id) over (
               partition by head.symbol_id, head.chan_level, head.mode
               order by head.cutoff_time
           ) expected_old_run_id,
           lag(head.cutoff_time) over (
               partition by head.symbol_id, head.chan_level, head.mode
               order by head.cutoff_time
           ) expected_old_cutoff
      from actual_heads head
), evidence as materialized (
    select ordered.*, history.id history_id, history.old_run_id,
           history.old_base_to_bar_end old_cutoff, outbox.id outbox_id,
           outbox.payload,
           predecessor.id predecessor_history_id
      from ordered
      join chan_c_head_history history
        on history.symbol_id = ordered.symbol_id
       and history.chan_level = ordered.chan_level
       and history.mode = ordered.mode
       and history.new_run_id = ordered.run_id
      join chan_c_head_outbox outbox on outbox.head_history_id = history.id
      left join chan_c_head_history predecessor
        on predecessor.symbol_id = ordered.symbol_id
       and predecessor.chan_level = ordered.chan_level
       and predecessor.mode = ordered.mode
       and predecessor.new_run_id = ordered.expected_old_run_id
)
select history_id, outbox_id, run_id new_run_id, cutoff_time new_cutoff,
       old_run_id current_old_run_id, old_cutoff current_old_cutoff,
       predecessor_history_id, expected_old_run_id target_old_run_id,
       expected_old_cutoff target_old_cutoff,
       payload->>'old_run_id' payload_old_run_id,
       payload->>'old_base_to_bar_end' payload_old_cutoff
  from evidence
 where old_run_id is distinct from expected_old_run_id
    or old_cutoff is distinct from expected_old_cutoff
    or (payload->>'old_run_id')::bigint is distinct from old_run_id
    or (payload->>'old_base_to_bar_end')::timestamptz is distinct from old_cutoff
 order by history_id
"""

UPDATE_HISTORY_SQL = """
update chan_c_head_history
   set old_run_id = $2, old_base_to_bar_end = $3
 where id = $1
   and new_run_id = $4
   and old_run_id is not distinct from $5
   and old_base_to_bar_end is not distinct from $6
"""
UPDATE_OUTBOX_SQL = """
update chan_c_head_outbox
   set payload = $3::jsonb,
       status = 'pending',
       lease_version = lease_version + 1,
       lease_token = null,
       lease_until = null,
       next_attempt_at = null,
       processed_at = null,
       last_error = null,
       failed_at = null,
       dead_lettered_at = null,
       updated_at = clock_timestamp()
 where id = $1 and head_history_id = $2
   and status = 'completed' and processed_at is not null
   and payload = $4::jsonb
"""
DELETE_EVENTS_SQL = """
delete from chan_structure_lifecycle_events
 where head_history_id = any($1::bigint[])
"""

# Kept as one inspectable contract for tests/review.  Runtime execution uses
# the individual statements above and checks every command tag.
APPLY_EVIDENCE_SQL = "\n".join((
    LOCK_PARENT_SQL, LOCK_CHILD_SQL, LOCK_HISTORIES_SQL,
    LOCK_OUTBOX_SQL, LOCK_EVENTS_SQL, UPDATE_HISTORY_SQL,
    UPDATE_OUTBOX_SQL, DELETE_EVENTS_SQL,
))

EVENT_COLUMNS = (
    "id", "fingerprint", "head_history_id", "event_type", "effective_time",
    "point_time", "previous_mode", "current_mode", "run_id", "provenance",
    "created_at", "observed_time",
)


class RepairError(RuntimeError):
    pass


class InvalidRepairManifest(RepairError):
    pass


class RepairStateConflict(RepairError):
    pass


@dataclass(frozen=True)
class RepairEntry:
    history_id: int
    outbox_id: int
    new_run_id: int
    new_cutoff: datetime
    current_old_run_id: int | None
    current_old_cutoff: datetime | None
    predecessor_history_id: int
    target_old_run_id: int
    target_old_cutoff: datetime


@dataclass(frozen=True)
class RepairManifest:
    repair_id: UUID
    batch_id: int
    replay_contract_hash: str
    entries: tuple[RepairEntry, ...]
    raw: dict[str, Any]
    sha256: str


def _instant(value: Any, *, field: str, optional: bool = False) -> datetime | None:
    if value is None and optional:
        return None
    if not isinstance(value, (str, datetime)):
        raise InvalidRepairManifest(f"{field} must be an aware ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    except ValueError as exc:
        raise InvalidRepairManifest(f"{field} must be an aware ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidRepairManifest(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        _jsonable(value), ensure_ascii=True, sort_keys=True, separators=(",", ":"),
    )


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def event_set_sha256(events: Iterable[Mapping[str, Any]]) -> str:
    normalized = []
    for event in events:
        item = {
            key: _jsonable(event.get(key))
            for key in (
                "fingerprint", "head_history_id", "event_type", "effective_time",
                "point_time", "previous_mode", "current_mode", "run_id",
                "provenance", "observed_time",
            )
        }
        if isinstance(item["provenance"], str):
            try:
                item["provenance"] = json.loads(item["provenance"])
            except json.JSONDecodeError as exc:
                raise RepairStateConflict("lifecycle event provenance is not JSON") from exc
        normalized.append(item)
    normalized.sort(key=canonical_json)
    return canonical_sha256(normalized)


def event_identity_set_sha256(events: Iterable[Mapping[str, Any]]) -> str:
    """Hash the exact rows that an audited rollback would restore."""
    normalized = []
    for event in events:
        item = {key: _jsonable(event.get(key)) for key in EVENT_COLUMNS}
        if isinstance(item["provenance"], str):
            try:
                item["provenance"] = json.loads(item["provenance"])
            except json.JSONDecodeError as exc:
                raise RepairStateConflict("lifecycle event provenance is not JSON") from exc
        normalized.append(item)
    normalized.sort(key=canonical_json)
    return canonical_sha256(normalized)


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise InvalidRepairManifest(f"{field} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidRepairManifest(f"{field} must be a positive integer") from exc
    if parsed <= 0:
        raise InvalidRepairManifest(f"{field} must be a positive integer")
    return parsed


def _hash(value: Any, field: str) -> str:
    text = str(value or "")
    if len(text) != HASH_LENGTH or any(char not in "0123456789abcdef" for char in text):
        raise InvalidRepairManifest(f"{field} must be a 64-character lowercase hex digest")
    return text


def parse_manifest(
    raw: Mapping[str, Any], *, expected_contract_hash: str,
) -> RepairManifest:
    expected_hash = _hash(expected_contract_hash, "expected_contract_hash")
    if raw.get("contract_version") != CONTRACT_VERSION:
        raise InvalidRepairManifest(f"contract_version must be {CONTRACT_VERSION}")
    try:
        repair_id = UUID(str(raw.get("repair_id", "")))
    except ValueError as exc:
        raise InvalidRepairManifest("repair_id must be a UUID") from exc
    batch_id = _positive_int(raw.get("batch_id"), "batch_id")
    contract_hash = _hash(raw.get("replay_contract_hash"), "replay_contract_hash")
    if contract_hash != expected_hash:
        raise InvalidRepairManifest("replay_contract_hash does not match expected contract hash")
    raw_entries = raw.get("entries")
    if not isinstance(raw_entries, list) or len(raw_entries) != 2:
        raise InvalidRepairManifest("v1 entries must contain exactly 2 rows")
    entries: list[RepairEntry] = []
    for index, item in enumerate(raw_entries):
        if not isinstance(item, Mapping):
            raise InvalidRepairManifest(f"entries[{index}] must be an object")
        current_old_run_id = item.get("current_old_run_id")
        if current_old_run_id is not None:
            raise InvalidRepairManifest(
                f"entries[{index}].current_old_run_id must be null in v1"
            )
        if item.get("current_old_cutoff") is not None:
            raise InvalidRepairManifest(
                f"entries[{index}].current_old_cutoff must be null in v1"
            )
        entry = RepairEntry(
            history_id=_positive_int(item.get("history_id"), f"entries[{index}].history_id"),
            outbox_id=_positive_int(item.get("outbox_id"), f"entries[{index}].outbox_id"),
            new_run_id=_positive_int(item.get("new_run_id"), f"entries[{index}].new_run_id"),
            new_cutoff=_instant(item.get("new_cutoff"), field=f"entries[{index}].new_cutoff"),  # type: ignore[arg-type]
            current_old_run_id=current_old_run_id,
            current_old_cutoff=_instant(
                item.get("current_old_cutoff"),
                field=f"entries[{index}].current_old_cutoff", optional=True,
            ),
            predecessor_history_id=_positive_int(
                item.get("predecessor_history_id"),
                f"entries[{index}].predecessor_history_id",
            ),
            target_old_run_id=_positive_int(
                item.get("target_old_run_id"), f"entries[{index}].target_old_run_id",
            ),
            target_old_cutoff=_instant(
                item.get("target_old_cutoff"),
                field=f"entries[{index}].target_old_cutoff",
            ),  # type: ignore[arg-type]
        )
        entries.append(entry)
    if len({entry.history_id for entry in entries}) != len(entries):
        raise InvalidRepairManifest("duplicate history_id in entries")
    if len({entry.outbox_id for entry in entries}) != len(entries):
        raise InvalidRepairManifest("duplicate outbox_id in entries")
    normalized_raw = dict(_jsonable(raw))
    return RepairManifest(
        repair_id=repair_id, batch_id=batch_id, replay_contract_hash=contract_hash,
        entries=tuple(sorted(entries, key=lambda item: item.history_id)),
        raw=normalized_raw, sha256=canonical_sha256(normalized_raw),
    )


def load_manifest(
    path: Path, *, expected_manifest_sha256: str, expected_contract_hash: str,
) -> RepairManifest:
    expected_manifest = _hash(expected_manifest_sha256, "expected_manifest_sha256")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InvalidRepairManifest(f"cannot load repair manifest: {path}") from exc
    if not isinstance(raw, dict):
        raise InvalidRepairManifest("repair manifest must be a JSON object")
    manifest = parse_manifest(raw, expected_contract_hash=expected_contract_hash)
    if manifest.sha256 != expected_manifest:
        raise InvalidRepairManifest(
            f"manifest sha256 mismatch: expected {expected_manifest}, got {manifest.sha256}"
        )
    return manifest


def _updated_count(command_tag: str) -> int:
    try:
        return int(command_tag.rsplit(" ", 1)[-1])
    except (IndexError, ValueError) as exc:
        raise RepairStateConflict(f"unexpected database command tag: {command_tag!r}") from exc


def _row_json(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for field in ("payload", "provenance", "failure", "verification", "rollback_verification"):
        value = normalized.get(field)
        if isinstance(value, str):
            try:
                normalized[field] = json.loads(value)
            except json.JSONDecodeError:
                pass
    return dict(_jsonable(normalized))


def _entry_signature(entry: RepairEntry) -> dict[str, Any]:
    return {
        "history_id": entry.history_id,
        "outbox_id": entry.outbox_id,
        "new_run_id": entry.new_run_id,
        "new_cutoff": _jsonable(entry.new_cutoff),
        "current_old_run_id": entry.current_old_run_id,
        "current_old_cutoff": _jsonable(entry.current_old_cutoff),
        "predecessor_history_id": entry.predecessor_history_id,
        "target_old_run_id": entry.target_old_run_id,
        "target_old_cutoff": _jsonable(entry.target_old_cutoff),
    }


def _anomaly_signature(row: Mapping[str, Any]) -> dict[str, Any]:
    payload_old_run = row.get("payload_old_run_id")
    payload_old_cutoff = row.get("payload_old_cutoff")
    current_old_run = row.get("current_old_run_id")
    current_old_cutoff = row.get("current_old_cutoff")
    if payload_old_run is not None and int(payload_old_run) != current_old_run:
        raise RepairStateConflict(f"outbox/history old_run_id mismatch: {row['history_id']}")
    if payload_old_cutoff is not None and _instant(
        payload_old_cutoff, field="payload_old_cutoff", optional=True,
    ) != current_old_cutoff:
        raise RepairStateConflict(f"outbox/history old cutoff mismatch: {row['history_id']}")
    return {
        "history_id": int(row["history_id"]),
        "outbox_id": int(row["outbox_id"]),
        "new_run_id": int(row["new_run_id"]),
        "new_cutoff": _jsonable(row["new_cutoff"]),
        "current_old_run_id": (
            int(current_old_run) if current_old_run is not None else None
        ),
        "current_old_cutoff": _jsonable(current_old_cutoff),
        "predecessor_history_id": int(row["predecessor_history_id"]),
        "target_old_run_id": int(row["target_old_run_id"]),
        "target_old_cutoff": _jsonable(row["target_old_cutoff"]),
    }


def validate_pre_repair_finalizer(finalizer: Mapping[str, Any]) -> None:
    counts = finalizer.get("counts", {})
    nonzero = {key: int(value) for key, value in counts.items() if int(value or 0)}
    allowed_nonzero = {
        "total_tasks", "completed_tasks", "excluded_tasks", "invalid_history",
    }
    if (
        list(finalizer.get("blockers", [])) != ["invalid_history"]
        or int(counts.get("invalid_history", 0)) != 2
        or any(key not in allowed_nonzero for key in nonzero)
        or finalizer.get("lifecycle_reconciliation", {}).get("decision") != "PASS"
    ):
        raise RepairStateConflict(
            "pre-repair finalizer blockers/counts are not the exact invalid_history contract"
        )


def validate_post_repair_finalizer(finalizer: Mapping[str, Any]) -> None:
    if not bool(finalizer.get("ready")):
        raise RepairStateConflict(
            f"historical replay finalizer remains blocked: {finalizer.get('blockers', [])}"
        )


async def _validate_batch_and_anomalies(
    conn: Any, manifest: RepairManifest, *, locked: bool,
) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]], list[dict[str, Any]]]:
    if locked:
        parent = await conn.fetchrow(LOCK_PARENT_SQL, manifest.batch_id)
        child = await conn.fetchrow(LOCK_CHILD_SQL, manifest.batch_id)
    else:
        parent = await conn.fetchrow(
            "select id,status,batch_kind,run_group_id,config_hash from chan_c_batches where id=$1",
            manifest.batch_id,
        )
        child = await conn.fetchrow(
            "select batch_id,status,contract_hash from chan_c_historical_replay_batches where batch_id=$1",
            manifest.batch_id,
        )
    if parent is None or child is None:
        raise RepairStateConflict("historical replay parent/child batch is missing")
    if str(parent["status"]) != "sealed" or str(parent["batch_kind"]) != "historical_replay":
        raise RepairStateConflict("repair requires a sealed historical replay parent batch")
    if str(child["status"]) != "running":
        raise RepairStateConflict("plan/apply requires a running replay child batch")
    if str(child["contract_hash"]) != manifest.replay_contract_hash:
        raise RepairStateConflict("stored replay contract hash changed")

    anomaly_rows = [dict(row) for row in await conn.fetch(PREFIX_ANOMALIES_SQL, manifest.batch_id)]
    actual = [_anomaly_signature(row) for row in anomaly_rows]
    expected = [_entry_signature(entry) for entry in manifest.entries]
    if actual != expected:
        raise RepairStateConflict(
            "full-batch predecessor anomaly set does not exactly match manifest: "
            f"expected={canonical_sha256(expected)} actual={canonical_sha256(actual)}"
        )

    history_ids = [entry.history_id for entry in manifest.entries]
    outbox_ids = [entry.outbox_id for entry in manifest.entries]
    if locked:
        histories_raw = await conn.fetch(LOCK_HISTORIES_SQL, history_ids)
        outboxes_raw = await conn.fetch(LOCK_OUTBOX_SQL, outbox_ids)
        events_raw = await conn.fetch(LOCK_EVENTS_SQL, history_ids)
    else:
        histories_raw = await conn.fetch(
            "select * from chan_c_head_history where id=any($1::bigint[]) order by id", history_ids,
        )
        outboxes_raw = await conn.fetch(
            "select * from chan_c_head_outbox where id=any($1::bigint[]) order by id", outbox_ids,
        )
        events_raw = await conn.fetch(
            "select * from chan_structure_lifecycle_events where head_history_id=any($1::bigint[]) order by id",
            history_ids,
        )
    histories = {int(row["id"]): dict(row) for row in histories_raw}
    outboxes = {int(row["id"]): dict(row) for row in outboxes_raw}
    if set(histories) != set(history_ids) or set(outboxes) != set(outbox_ids):
        raise RepairStateConflict("manifest history/outbox rows are not exact")
    for entry in manifest.entries:
        history = histories[entry.history_id]
        outbox = outboxes[entry.outbox_id]
        if int(outbox["head_history_id"]) != entry.history_id:
            raise RepairStateConflict(f"outbox/history identity mismatch: {entry.outbox_id}")
        if (
            str(outbox["status"]) != "completed"
            or outbox["processed_at"] is None
            or outbox["lease_until"] is not None
        ):
            raise RepairStateConflict(f"outbox is not completed without an active lease: {entry.outbox_id}")
        payload = _json_object(outbox["payload"], field=f"outbox[{entry.outbox_id}].payload")
        for field in HISTORY_PAYLOAD_IDENTITY_FIELDS:
            if payload.get(field) != _jsonable(history[field]):
                raise RepairStateConflict(
                    f"outbox/history identity mismatch for {field}: {entry.outbox_id}"
                )
        for field, history_field in (
            ("old_base_to_bar_end", "old_base_to_bar_end"),
            ("new_base_to_bar_end", "new_base_to_bar_end"),
            ("published_at", "published_at"),
        ):
            payload_value = _instant(
                payload.get(field), field=f"payload.{field}", optional=True,
            )
            if payload_value != history[history_field]:
                raise RepairStateConflict(
                    f"outbox/history identity mismatch for {field}: {entry.outbox_id}"
                )

    finalizer = await finalize_replay_batch(
        conn, batch_id=manifest.batch_id, sealed_by="prefix-repair-preflight",
        expected_contract_hash=manifest.replay_contract_hash, dry_run=True,
    )
    validate_pre_repair_finalizer(finalizer)
    return histories, outboxes, [dict(row) for row in events_raw]


def _target_payload(outbox: Mapping[str, Any], entry: RepairEntry) -> dict[str, Any]:
    payload = outbox["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, Mapping):
        raise RepairStateConflict(f"outbox payload is not an object: {entry.outbox_id}")
    target = dict(payload)
    target["old_run_id"] = entry.target_old_run_id
    target["old_base_to_bar_end"] = _jsonable(entry.target_old_cutoff)
    return dict(_jsonable(target))


async def _expected_events(
    conn: Any,
    manifest: RepairManifest,
    histories: Mapping[int, Mapping[str, Any]],
    outboxes: Mapping[int, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[int, str]]:
    observer = LifecycleObserver()
    all_events: list[dict[str, Any]] = []
    by_history: dict[int, str] = {}
    for entry in manifest.entries:
        history = histories[entry.history_id]
        old_config = await conn.fetchval(
            "select config_hash from chan_c_runs where id=$1", entry.target_old_run_id,
        )
        new_config = await conn.fetchval(
            "select config_hash from chan_c_runs where id=$1", entry.new_run_id,
        )
        if str(new_config) != str(history["config_hash"]) or str(old_config) != str(new_config):
            raise RepairStateConflict(f"predecessor/current config mismatch: {entry.history_id}")
        common = {
            "symbol_id": int(history["symbol_id"]),
            "chan_level": int(history["chan_level"]),
            "mode": str(history["mode"]),
        }
        previous = await observer.load_observations(
            conn, run_id=entry.target_old_run_id, config_hash=str(old_config), **common,
        )
        current = await observer.load_observations(
            conn, run_id=entry.new_run_id, config_hash=str(new_config), **common,
        )
        planned = plan_events(
            profile="historical_replay", previous=previous, current=current, states={},
        )
        payload = _target_payload(outboxes[entry.outbox_id], entry)
        rows = [{
            "fingerprint": item.observation.fingerprint,
            "head_history_id": entry.history_id,
            "event_type": item.event_type,
            "effective_time": entry.new_cutoff,
            "point_time": item.observation.point_time,
            "previous_mode": item.previous_mode,
            "current_mode": (
                None if item.event_type == "disappeared" else item.observation.mode
            ),
            "run_id": entry.new_run_id,
            "provenance": payload,
            "observed_time": history["published_at"],
        } for item in planned]
        by_history[entry.history_id] = event_set_sha256(rows)
        all_events.extend(rows)
    return all_events, by_history


async def plan_repair(conn: Any, manifest: RepairManifest) -> dict[str, Any]:
    # The finalizer dry-run uses FOR SHARE locks, which PostgreSQL rejects in a
    # READ ONLY transaction even though it performs no data mutation.
    async with conn.transaction(isolation="repeatable_read"):
        histories, outboxes, events_before = await _validate_batch_and_anomalies(
            conn, manifest, locked=False,
        )
        target_events, by_history = await _expected_events(
            conn, manifest, histories, outboxes,
        )
    return {
        "action": "plan", "repair_id": str(manifest.repair_id),
        "batch_id": manifest.batch_id, "manifest_sha256": manifest.sha256,
        "history_count": len(manifest.entries),
        "before_event_count": len(events_before),
        "before_event_set_sha256": event_set_sha256(events_before),
        "before_event_identity_sha256": event_identity_set_sha256(events_before),
        "target_event_count": len(target_events),
        "target_event_set_sha256": event_set_sha256(target_events),
        "target_event_set_by_history": {str(key): value for key, value in by_history.items()},
        "ready": True,
    }


async def _with_session_lock(conn: Any, operation):
    if not await conn.fetchval(TRY_LOCK_SQL):
        raise RepairStateConflict("canonical lifecycle observer lock is held")
    try:
        return await operation()
    finally:
        await conn.fetchval(UNLOCK_SQL)


async def apply_repair(
    conn: Any, manifest: RepairManifest, *, actor: str,
    expected_before_event_identity_sha256: str,
    expected_target_event_sha256: str,
) -> dict[str, Any]:
    operator = actor.strip()
    if not operator:
        raise ValueError("actor is required")
    expected_before_identity_hash = _hash(
        expected_before_event_identity_sha256, "expected_before_event_identity_sha256",
    )
    expected_target_hash = _hash(
        expected_target_event_sha256, "expected_target_event_sha256",
    )

    async def operation() -> dict[str, Any]:
        async with conn.transaction(isolation="serializable"):
            parent = await conn.fetchrow(LOCK_PARENT_SQL, manifest.batch_id)
            child = await conn.fetchrow(LOCK_CHILD_SQL, manifest.batch_id)
            if (
                parent is None or child is None
                or str(parent["status"]) != "sealed"
                or str(parent["batch_kind"]) != "historical_replay"
                or str(child["status"]) != "running"
                or str(child["contract_hash"]) != manifest.replay_contract_hash
            ):
                raise RepairStateConflict(
                    "first/repeated apply requires sealed parent and running child"
                )
            existing = await conn.fetchrow(
                "select * from chan_c_historical_replay_prefix_repairs where repair_id=$1 for update",
                manifest.repair_id,
            )
            if existing is not None:
                return await _validate_existing_apply(
                    conn, manifest, dict(existing),
                    expected_before_identity_hash=expected_before_identity_hash,
                    expected_target_hash=expected_target_hash,
                )
            histories, outboxes, events_before = await _validate_batch_and_anomalies(
                conn, manifest, locked=True,
            )
            target_events, by_history = await _expected_events(
                conn, manifest, histories, outboxes,
            )
            before_hash = event_set_sha256(events_before)
            before_identity_hash = event_identity_set_sha256(events_before)
            target_hash = event_set_sha256(target_events)
            if (
                before_identity_hash != expected_before_identity_hash
                or target_hash != expected_target_hash
            ):
                raise RepairStateConflict(
                    "apply event hashes no longer match the reviewed plan: "
                    f"before_identity={before_identity_hash} target={target_hash}"
                )
            await conn.execute(
                """insert into chan_c_historical_replay_prefix_repairs(
                       repair_id,batch_id,contract_version,replay_contract_hash,
                       manifest_sha256,manifest,before_event_set_sha256,
                       before_event_identity_sha256,target_event_set_sha256,status,applied_by
                   ) values($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,'applied',$10)""",
                manifest.repair_id, manifest.batch_id, CONTRACT_VERSION,
                manifest.replay_contract_hash, manifest.sha256,
                canonical_json(manifest.raw), before_hash, before_identity_hash,
                target_hash, operator,
            )
            for entry in manifest.entries:
                history = histories[entry.history_id]
                outbox = outboxes[entry.outbox_id]
                row_events = [
                    _row_json(row) for row in events_before
                    if int(row["head_history_id"]) == entry.history_id
                ]
                target_history = {
                    "old_run_id": entry.target_old_run_id,
                    "old_base_to_bar_end": _jsonable(entry.target_old_cutoff),
                }
                target_payload = _target_payload(outbox, entry)
                await conn.execute(
                    """insert into chan_c_historical_replay_prefix_repair_snapshots(
                           repair_id,history_id,outbox_id,history_before,outbox_before,
                           events_before,target_history,target_payload,target_event_set_sha256
                       ) values($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9)""",
                    manifest.repair_id, entry.history_id, entry.outbox_id,
                    canonical_json(_row_json(history)), canonical_json(_row_json(outbox)),
                    canonical_json(row_events), canonical_json(target_history),
                    canonical_json(target_payload), by_history[entry.history_id],
                )
            for entry in manifest.entries:
                history = histories[entry.history_id]
                if _updated_count(await conn.execute(
                    UPDATE_HISTORY_SQL, entry.history_id, entry.target_old_run_id,
                    entry.target_old_cutoff, entry.new_run_id,
                    entry.current_old_run_id, entry.current_old_cutoff,
                )) != 1:
                    raise RepairStateConflict(f"history CAS failed: {entry.history_id}")
                outbox = outboxes[entry.outbox_id]
                if _updated_count(await conn.execute(
                    UPDATE_OUTBOX_SQL, entry.outbox_id, entry.history_id,
                    canonical_json(_target_payload(outbox, entry)),
                    canonical_json(_json_object(
                        outbox["payload"], field=f"outbox[{entry.outbox_id}].payload",
                    )),
                )) != 1:
                    raise RepairStateConflict(f"outbox CAS failed: {entry.outbox_id}")
            deleted = _updated_count(await conn.execute(
                DELETE_EVENTS_SQL, [entry.history_id for entry in manifest.entries],
            ))
            if deleted != len(events_before):
                raise RepairStateConflict(
                    f"event delete count drift: expected {len(events_before)}, got {deleted}"
                )
            return {
                "action": "apply", "repair_id": str(manifest.repair_id),
                "status": "applied", "idempotent": False,
                "reset_outbox_count": len(manifest.entries),
                "archived_event_count": len(events_before),
                "target_event_count": len(target_events),
                "target_event_set_sha256": target_hash,
            }

    return await _with_session_lock(conn, operation)


async def _load_repair_snapshots(conn: Any, repair_id: UUID):
    repair = await conn.fetchrow(
        "select * from chan_c_historical_replay_prefix_repairs where repair_id=$1 for update",
        repair_id,
    )
    snapshots = await conn.fetch(
        """select * from chan_c_historical_replay_prefix_repair_snapshots
            where repair_id=$1 order by history_id""",
        repair_id,
    )
    if repair is None or not snapshots:
        raise RepairStateConflict("repair audit/snapshots are missing")
    return dict(repair), [dict(row) for row in snapshots]


async def _actual_repaired_state(
    conn: Any, manifest: RepairManifest, snapshots: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    history_ids = [entry.history_id for entry in manifest.entries]
    outbox_ids = [entry.outbox_id for entry in manifest.entries]
    histories = [dict(row) for row in await conn.fetch(LOCK_HISTORIES_SQL, history_ids)]
    outboxes = [dict(row) for row in await conn.fetch(LOCK_OUTBOX_SQL, outbox_ids)]
    events = [dict(row) for row in await conn.fetch(LOCK_EVENTS_SQL, history_ids)]
    if len(histories) != len(history_ids) or len(outboxes) != len(outbox_ids):
        raise RepairStateConflict("repaired history/outbox rows are missing")
    snapshot_by_history = {int(row["history_id"]): row for row in snapshots}
    history_by_id = {int(row["id"]): row for row in histories}
    outbox_by_id = {int(row["id"]): row for row in outboxes}
    for entry in manifest.entries:
        history = history_by_id[entry.history_id]
        outbox = outbox_by_id[entry.outbox_id]
        snapshot = snapshot_by_history[entry.history_id]
        target_payload = _json_object(
            snapshot["target_payload"], field="target_payload",
        )
        actual_payload = _json_object(
            outbox["payload"], field=f"outbox[{entry.outbox_id}].payload",
        )
        if (
            int(history["id"]) != entry.history_id
            or history["old_run_id"] != entry.target_old_run_id
            or history["old_base_to_bar_end"] != entry.target_old_cutoff
            or int(outbox["id"]) != entry.outbox_id
            or _jsonable(actual_payload) != _jsonable(target_payload)
        ):
            raise RepairStateConflict(f"target metadata drift: {entry.history_id}")
    return histories, outboxes, events


def _json_object(value: Any, *, field: str) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, Mapping):
        raise RepairStateConflict(f"{field} is not a JSON object")
    return dict(value)


def _json_array(value: Any, *, field: str) -> list[dict[str, Any]]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise RepairStateConflict(f"{field} is not a JSON object array")
    return [dict(item) for item in value]


async def _validate_rolled_back_state(
    conn: Any, manifest: RepairManifest, snapshots: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    history_ids = [entry.history_id for entry in manifest.entries]
    outbox_ids = [entry.outbox_id for entry in manifest.entries]
    histories = {
        int(row["id"]): dict(row)
        for row in await conn.fetch(LOCK_HISTORIES_SQL, history_ids)
    }
    outboxes = {
        int(row["id"]): dict(row)
        for row in await conn.fetch(LOCK_OUTBOX_SQL, outbox_ids)
    }
    events = [dict(row) for row in await conn.fetch(LOCK_EVENTS_SQL, history_ids)]
    before_events: list[dict[str, Any]] = []
    for snapshot in snapshots:
        history_id = int(snapshot["history_id"])
        outbox_id = int(snapshot["outbox_id"])
        history_before = _json_object(
            snapshot["history_before"], field="history_before",
        )
        outbox_before = _json_object(
            snapshot["outbox_before"], field="outbox_before",
        )
        if (
            history_id not in histories
            or histories[history_id]["old_run_id"] != history_before.get("old_run_id")
            or _jsonable(histories[history_id]["old_base_to_bar_end"])
            != history_before.get("old_base_to_bar_end")
        ):
            raise RepairStateConflict(f"rolled-back history drift: {history_id}")
        if outbox_id not in outboxes:
            raise RepairStateConflict(f"rolled-back outbox missing: {outbox_id}")
        for field in (
            "status", "lease_version", "lease_token", "lease_until", "attempts",
            "payload", "processed_at", "next_attempt_at", "last_error", "failed_at",
            "dead_lettered_at",
        ):
            actual_value = outboxes[outbox_id].get(field)
            if field == "payload" and isinstance(actual_value, str):
                actual_value = _json_object(
                    actual_value, field=f"outbox[{outbox_id}].payload",
                )
            if _jsonable(actual_value) != outbox_before.get(field):
                raise RepairStateConflict(
                    f"rolled-back outbox drift for {field}: {outbox_id}"
                )
        before_events.extend(_json_array(
            snapshot["events_before"], field="events_before",
        ))
    if (
        len(events) != len(before_events)
        or event_identity_set_sha256(events) != event_identity_set_sha256(before_events)
    ):
        raise RepairStateConflict("rolled-back lifecycle event set drift")
    return {"history_count": len(histories), "event_count": len(events)}


async def _validate_existing_apply(
    conn: Any,
    manifest: RepairManifest,
    audit: Mapping[str, Any],
    *,
    expected_before_identity_hash: str,
    expected_target_hash: str,
) -> dict[str, Any]:
    if (
        str(audit["manifest_sha256"]) != manifest.sha256
        or str(audit["replay_contract_hash"]) != manifest.replay_contract_hash
        or str(audit["before_event_identity_sha256"]) != expected_before_identity_hash
        or str(audit["target_event_set_sha256"]) != expected_target_hash
    ):
        raise RepairStateConflict("repair_id audit contract/hash mismatch")
    snapshots = [dict(row) for row in await conn.fetch(
        """select * from chan_c_historical_replay_prefix_repair_snapshots
            where repair_id=$1 order by history_id""",
        manifest.repair_id,
    )]
    if len(snapshots) != len(manifest.entries):
        raise RepairStateConflict("existing repair snapshot set is incomplete")
    status = str(audit["status"])
    if status == "rolled_back":
        await _validate_rolled_back_state(conn, manifest, snapshots)
    elif status in {"applied", "verified"}:
        _histories, outboxes, events = await _actual_repaired_state(
            conn, manifest, snapshots,
        )
        statuses = {str(row["status"]) for row in outboxes}
        if status == "verified":
            if statuses != {"completed"} or event_set_sha256(events) != expected_target_hash:
                raise RepairStateConflict("verified repair current state drift")
        else:
            if len(statuses) != 1:
                raise RepairStateConflict("applied repair has mixed outbox states")
            outbox_status = next(iter(statuses))
            if outbox_status == "completed":
                if event_set_sha256(events) != expected_target_hash:
                    raise RepairStateConflict("applied replayed event set drift")
            elif outbox_status in {"pending", "processing", "failed", "dead_letter"}:
                if events:
                    raise RepairStateConflict("unfinished applied repair already has visible events")
            else:
                raise RepairStateConflict(f"unsupported applied outbox state: {outbox_status}")
    else:
        raise RepairStateConflict(f"unsupported repair audit status: {status}")
    return {
        "action": "apply", "repair_id": str(manifest.repair_id),
        "status": status, "idempotent": True,
    }


def validate_rollback_target_state(
    outboxes: Sequence[Mapping[str, Any]],
    current_events: Sequence[Mapping[str, Any]],
    snapshots: Sequence[Mapping[str, Any]],
) -> None:
    allowed_statuses = {"pending", "processing", "failed", "dead_letter", "completed"}
    snapshot_by_history = {int(row["history_id"]): row for row in snapshots}
    events_by_history: dict[int, list[Mapping[str, Any]]] = {
        history_id: [] for history_id in snapshot_by_history
    }
    for event in current_events:
        history_id = int(event["head_history_id"])
        if history_id not in events_by_history:
            raise RepairStateConflict(f"unexpected target event history: {history_id}")
        events_by_history[history_id].append(event)
    for outbox in outboxes:
        status = str(outbox["status"])
        history_id = int(outbox["head_history_id"])
        if status not in allowed_statuses:
            raise RepairStateConflict(f"rollback refuses outbox status: {status}")
        history_events = events_by_history[history_id]
        if status == "completed":
            if outbox["processed_at"] is None or event_set_sha256(history_events) != str(
                snapshot_by_history[history_id]["target_event_set_sha256"]
            ):
                raise RepairStateConflict(f"completed partial replay drift: {history_id}")
        elif history_events:
            raise RepairStateConflict(f"unfinished outbox has committed events: {history_id}")


async def verify_repair(
    conn: Any, manifest: RepairManifest, *, actor: str,
) -> dict[str, Any]:
    operator = actor.strip()
    if not operator:
        raise ValueError("actor is required")

    async def operation() -> dict[str, Any]:
        async with conn.transaction(isolation="serializable"):
            parent = await conn.fetchrow(LOCK_PARENT_SQL, manifest.batch_id)
            child = await conn.fetchrow(LOCK_CHILD_SQL, manifest.batch_id)
            if (
                parent is None or child is None
                or str(parent["status"]) != "sealed"
                or str(parent["batch_kind"]) != "historical_replay"
                or str(child["status"]) not in {"running", "sealed"}
                or str(child["contract_hash"]) != manifest.replay_contract_hash
            ):
                raise RepairStateConflict(
                    "verify requires a manifest-fenced sealed parent and running or sealed child"
                )
            audit, snapshots = await _load_repair_snapshots(conn, manifest.repair_id)
            if audit["manifest_sha256"] != manifest.sha256:
                raise RepairStateConflict("audit manifest hash mismatch")
            if str(audit["status"]) == "rolled_back":
                raise RepairStateConflict("cannot verify a rolled-back repair")
            histories, outboxes, events = await _actual_repaired_state(
                conn, manifest, snapshots,
            )
            if any(str(row["status"]) != "completed" or row["processed_at"] is None for row in outboxes):
                raise RepairStateConflict("canonical observer replay is not completed")
            expected, _ = await _expected_events(
                conn, manifest,
                {int(row["id"]): row for row in histories},
                {int(row["id"]): row for row in outboxes},
            )
            actual_hash = event_set_sha256(events)
            expected_hash = event_set_sha256(expected)
            if actual_hash != expected_hash or expected_hash != audit["target_event_set_sha256"]:
                raise RepairStateConflict(
                    f"canonical event set mismatch: expected={expected_hash} actual={actual_hash}"
                )
            finalizer = await finalize_replay_batch(
                conn, batch_id=manifest.batch_id, sealed_by=operator,
                expected_contract_hash=manifest.replay_contract_hash, dry_run=True,
            )
            validate_post_repair_finalizer(finalizer)
            verification = {
                "event_count": len(events), "event_set_sha256": actual_hash,
                "outbox_completed": len(outboxes), "finalizer_ready": True,
            }
            if str(audit["status"]) != "verified":
                updated = await conn.execute(
                    """update chan_c_historical_replay_prefix_repairs
                          set status='verified', verified_by=$2, verified_at=clock_timestamp(),
                              verification=$3::jsonb
                        where repair_id=$1 and status='applied'""",
                    manifest.repair_id, operator, canonical_json(verification),
                )
                if _updated_count(updated) != 1:
                    raise RepairStateConflict("repair verification CAS failed")
            return {
                "action": "verify", "repair_id": str(manifest.repair_id),
                "status": "verified", **verification,
            }

    return await _with_session_lock(conn, operation)


async def rollback_repair(
    conn: Any, manifest: RepairManifest, *, actor: str,
) -> dict[str, Any]:
    operator = actor.strip()
    if not operator:
        raise ValueError("actor is required")

    async def operation() -> dict[str, Any]:
        async with conn.transaction(isolation="serializable"):
            parent = await conn.fetchrow(LOCK_PARENT_SQL, manifest.batch_id)
            child = await conn.fetchrow(LOCK_CHILD_SQL, manifest.batch_id)
            if (
                parent is None or child is None
                or str(parent["status"]) != "sealed"
                or str(parent["batch_kind"]) != "historical_replay"
                or str(child["status"]) != "running"
                or str(child["contract_hash"]) != manifest.replay_contract_hash
            ):
                raise RepairStateConflict(
                    "rollback requires sealed parent and running child; sealed child evidence is immutable"
                )
            audit, snapshots = await _load_repair_snapshots(conn, manifest.repair_id)
            if audit["manifest_sha256"] != manifest.sha256:
                raise RepairStateConflict("audit manifest hash mismatch")
            if str(audit["status"]) == "rolled_back":
                restored = await _validate_rolled_back_state(conn, manifest, snapshots)
                return {
                    "action": "rollback", "repair_id": str(manifest.repair_id),
                    "status": "rolled_back", "idempotent": True,
                    "restored_history_count": restored["history_count"],
                    "restored_event_count": restored["event_count"],
                }
            if str(audit["status"]) not in {"applied", "verified"}:
                raise RepairStateConflict("rollback audit state is not recoverable")
            _histories, _outboxes, current_events = await _actual_repaired_state(
                conn, manifest, snapshots,
            )
            validate_rollback_target_state(_outboxes, current_events, snapshots)
            current_outbox_status = {
                int(row["id"]): str(row["status"]) for row in _outboxes
            }
            deleted = _updated_count(await conn.execute(
                DELETE_EVENTS_SQL, [entry.history_id for entry in manifest.entries],
            ))
            if deleted != len(current_events):
                raise RepairStateConflict("rollback event delete count drift")

            restored_event_count = 0
            for snapshot in snapshots:
                history_before = snapshot["history_before"]
                outbox_before = snapshot["outbox_before"]
                events_before = snapshot["events_before"]
                if isinstance(history_before, str):
                    history_before = json.loads(history_before)
                if isinstance(outbox_before, str):
                    outbox_before = json.loads(outbox_before)
                if isinstance(events_before, str):
                    events_before = json.loads(events_before)
                history_id = int(snapshot["history_id"])
                restored = await conn.execute(
                    """update chan_c_head_history
                          set old_run_id=$2, old_base_to_bar_end=$3
                        where id=$1 and old_run_id=$4 and old_base_to_bar_end=$5""",
                    history_id, history_before.get("old_run_id"),
                    _instant(history_before.get("old_base_to_bar_end"), field="history_before.old_base_to_bar_end", optional=True),
                    manifest.entries[[item.history_id for item in manifest.entries].index(history_id)].target_old_run_id,
                    manifest.entries[[item.history_id for item in manifest.entries].index(history_id)].target_old_cutoff,
                )
                if _updated_count(restored) != 1:
                    raise RepairStateConflict(f"rollback history CAS failed: {history_id}")
                restored = await conn.execute(
                    """update chan_c_head_outbox
                          set status=$3, lease_version=$4, lease_token=$5,
                              lease_until=$6, attempts=$7, payload=$8::jsonb,
                              processed_at=$9, created_at=$10, updated_at=$11,
                              next_attempt_at=$12, last_error=$13, failed_at=$14,
                              dead_lettered_at=$15
                        where id=$1 and head_history_id=$2 and status=$16""",
                    int(snapshot["outbox_id"]), history_id,
                    outbox_before["status"], int(outbox_before["lease_version"]),
                    outbox_before.get("lease_token"),
                    _instant(outbox_before.get("lease_until"), field="outbox_before.lease_until", optional=True),
                    int(outbox_before["attempts"]), canonical_json(outbox_before["payload"]),
                    _instant(outbox_before.get("processed_at"), field="outbox_before.processed_at", optional=True),
                    _instant(outbox_before["created_at"], field="outbox_before.created_at"),
                    _instant(outbox_before["updated_at"], field="outbox_before.updated_at"),
                    _instant(outbox_before.get("next_attempt_at"), field="outbox_before.next_attempt_at", optional=True),
                    outbox_before.get("last_error"),
                    _instant(outbox_before.get("failed_at"), field="outbox_before.failed_at", optional=True),
                    _instant(outbox_before.get("dead_lettered_at"), field="outbox_before.dead_lettered_at", optional=True),
                    current_outbox_status[int(snapshot["outbox_id"])],
                )
                if _updated_count(restored) != 1:
                    raise RepairStateConflict(f"rollback outbox CAS failed: {snapshot['outbox_id']}")
                for event in events_before:
                    values = [event.get(column) for column in EVENT_COLUMNS]
                    for index in (4, 5, 10, 11):
                        values[index] = _instant(values[index], field=f"event.{EVENT_COLUMNS[index]}")
                    if isinstance(values[9], str):
                        try:
                            values[9] = json.loads(values[9])
                        except json.JSONDecodeError as exc:
                            raise RepairStateConflict(
                                f"archived event provenance is invalid: {values[0]}"
                            ) from exc
                    values[9] = canonical_json(values[9])
                    await conn.execute(
                        """insert into chan_structure_lifecycle_events(
                               id,fingerprint,head_history_id,event_type,effective_time,
                               point_time,previous_mode,current_mode,run_id,provenance,
                               created_at,observed_time
                           ) overriding system value
                           values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12)""",
                        *values,
                    )
                    restored_event_count += 1
            rollback_verification = {
                "restored_history_count": len(snapshots),
                "restored_event_count": restored_event_count,
                "identity_rows_retained": True,
            }
            updated = await conn.execute(
                """update chan_c_historical_replay_prefix_repairs
                      set status='rolled_back', rolled_back_by=$2,
                          rolled_back_at=clock_timestamp(), rollback_verification=$3::jsonb
                    where repair_id=$1 and status in ('applied','verified')""",
                manifest.repair_id, operator, canonical_json(rollback_verification),
            )
            if _updated_count(updated) != 1:
                raise RepairStateConflict("repair rollback CAS failed")
            return {
                "action": "rollback", "repair_id": str(manifest.repair_id),
                "status": "rolled_back", "idempotent": False,
                **rollback_verification,
            }

    return await _with_session_lock(conn, operation)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair a manifest-declared replay prefix")
    parser.add_argument("action", choices=("plan", "apply", "verify", "rollback"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--expected-contract-hash", required=True)
    parser.add_argument("--expected-before-event-identity-sha256")
    parser.add_argument("--expected-target-event-sha256")
    parser.add_argument("--actor")
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    if args.action != "plan" and not str(args.actor or "").strip():
        parser.error("--actor is required for apply, verify and rollback")
    if args.action == "apply" and (
        not args.expected_before_event_identity_sha256
        or not args.expected_target_event_sha256
    ):
        parser.error(
            "apply requires --expected-before-event-identity-sha256 and "
            "--expected-target-event-sha256"
        )
    for field in ("manifest_sha256", "expected_contract_hash"):
        try:
            _hash(getattr(args, field), field)
        except InvalidRepairManifest as exc:
            parser.error(str(exc))
    if args.action == "apply":
        for field in (
            "expected_before_event_identity_sha256", "expected_target_event_sha256",
        ):
            try:
                _hash(getattr(args, field), field)
            except InvalidRepairManifest as exc:
                parser.error(str(exc))
    return args


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    manifest = load_manifest(
        args.manifest, expected_manifest_sha256=args.manifest_sha256,
        expected_contract_hash=args.expected_contract_hash,
    )
    conn = await asyncpg.connect(args.database_url)
    try:
        if args.action == "plan":
            return await plan_repair(conn, manifest)
        if args.action == "apply":
            return await apply_repair(
                conn, manifest, actor=args.actor,
                expected_before_event_identity_sha256=(
                    args.expected_before_event_identity_sha256
                ),
                expected_target_event_sha256=args.expected_target_event_sha256,
            )
        if args.action == "verify":
            return await verify_repair(conn, manifest, actor=args.actor)
        return await rollback_repair(conn, manifest, actor=args.actor)
    finally:
        await conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = asyncio.run(_run(args))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
