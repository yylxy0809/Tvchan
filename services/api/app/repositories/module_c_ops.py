from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import Counter
from datetime import UTC, datetime
from typing import Any, Mapping

from trading_protocol.module_c_canary_selection import (
    evaluate_selection_evidence,
    selection_active_universe_sha256,
)


LEVELS = ((5, "5f"), (30, "30f"), (1440, "1d"), (10080, "1w"), (43200, "1m"))
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
FRESHNESS_CONTRACT_VERSION = "module-c-authoritative-freshness-v1"
AUDIT_CONTRACT_VERSION = "module-c-strict-audit-v2"
FROZEN_CONTRACT = "module-c-native-five-level-v1"
FROZEN_MODES = ("confirmed", "predictive")
ANOMALY_FIELDS = (
    "invalid_ohlc", "negative_volume", "negative_amount", "illegal_sessions",
    "incomplete_rows", "logical_duplicate_rows", "unexpected_source",
    "current_open_periods", "timestamp_mismatches", "missing_daily_basis",
    "missing_higher_periods", "catalog_empty_has_rows",
    "catalog_present_missing_rows", "catalog_present_bounds_mismatch",
    "catalog_scope_missing", "catalog_scope_unknown", "catalog_scope_incomplete",
    "missing_rows",
)
STRICT_PROVENANCE_FIELDS = (
    "canonical_audit_run_id",
    "audit_evidence_sha256",
    "audit_checkpoint_sha256",
    "freshness_contract_version",
    "freshness_contract_sha256",
    "catalog_generation_id",
    "catalog_control_revision",
    "catalog_manifest_sha256",
    "audit_active_universe_sha256",
)
RUNNING_COUNTS_SQL = """
/* module-c-execution:running-counts */
with running as (
    select parent.id as batch_id, parent.created_at
      from chan_c_batches parent
      join chan_c_full_recompute_batches child on child.batch_id=parent.id
     where parent.status='running' or child.status='running'
    union
    select parent.id as batch_id, parent.created_at
      from chan_c_batches parent
      join chan_c_full_recompute_batches child on child.batch_id=parent.id
      join chan_c_full_recompute_tasks task on task.batch_id=parent.id
     where task.status='running'
)
select clock_timestamp() as observed_at,
       (select count(*)::bigint
          from chan_c_batches parent
          join chan_c_full_recompute_batches child on child.batch_id=parent.id
         where parent.status='running')
           as running_parent_batches,
       (select count(*)::bigint from chan_c_full_recompute_batches where status='running')
           as running_child_batches,
       (select count(*)::bigint from chan_c_full_recompute_tasks where status='running')
           as running_tasks,
       coalesce(
           (select array_agg(
                       running.batch_id::text
                       order by running.created_at desc, running.batch_id desc
                   )
              from running),
           array[]::text[]
       ) as running_batch_ids
"""


BATCH_SQL = """
/* module-c-execution:batch */
select parent.id as batch_id, parent.batch_key, parent.batch_kind,
       parent.status as parent_status, child.status as child_status,
       parent.publication_namespace, parent.profile_id, parent.run_group_id,
       child.publication_namespace as child_publication_namespace,
       child.profile_id as child_profile_id,
       child.run_group_id as child_run_group_id,
       parent.code_commit, parent.image_digest, parent.vendor_manifest_sha256,
       parent.config_hash, child.config_hash as child_config_hash,
       parent.created_at, child.started_at, child.finished_at,
       child.updated_at, child.shard_count, child.active_symbols,
       child.disposition_rows,
       (select max(task.updated_at) from chan_c_full_recompute_tasks task
         where task.batch_id=parent.id) as latest_task_update,
       parent.effective_config as frozen_effective_config,
       parent.effective_config->>'contract' as frozen_contract,
       parent.effective_config->'levels' as frozen_levels,
       parent.effective_config->'modes' as frozen_modes,
       parent.effective_config->>'concurrency_per_worker'
           as frozen_concurrency_per_worker,
       parent.effective_config->>'shard_count' as frozen_shard_count,
       parent.effective_config->>'max_attempts' as frozen_max_attempts,
       parent.effective_config->>'eligibility_build_id'
           as frozen_eligibility_build_id,
       build.parameters->>'policy' as policy,
       build.parameters as build_parameters,
       build.active_universe_hash as build_active_universe_sha256,
       build.build_id::text as eligibility_build_id,
       build.manifest_version, parent.eligible_manifest_sha256,
       build.manifest_hash as build_manifest_sha256,
       build.config_hash as build_config_hash,
       build.canonical_audit_run_id::text, build.audit_evidence_sha256,
       build.audit_checkpoint_sha256, audit.status as audit_status,
       audit.apply_mode as audit_apply_mode,
       audit.parameters as audit_parameters, audit.summary as audit_summary,
       build.freshness_contract_version, build.freshness_contract_sha256,
       build.catalog_generation_id::text, build.catalog_control_revision,
       build.catalog_manifest_sha256, build.audit_active_universe_sha256,
       generation.status as catalog_generation_status,
       control.active_generation_id::text as live_catalog_generation_id,
       control.revision as live_catalog_control_revision
  from chan_c_batches parent
  join chan_c_full_recompute_batches child on child.batch_id=parent.id
  join module_c_eligibility_builds build on build.build_id=child.eligibility_build_id
  left join kline_audit_runs audit on audit.audit_run_id=build.canonical_audit_run_id
  left join kline_scope_catalog_generations generation
    on generation.generation_id=build.catalog_generation_id
  left join kline_scope_catalog_control control on control.control_key='active'
 where ($1::bigint is null or parent.id=$1)
 order by parent.created_at desc, parent.id desc
 limit 1
"""


TASK_PROGRESS_SQL = """
/* module-c-execution:tasks */
select chan_level, status, count(*)::bigint as count,
       coalesce(sum(attempts),0)::bigint as attempts,
       coalesce(sum(bar_count),0)::bigint as bars,
       coalesce(sum(stroke_count),0)::bigint as strokes,
       coalesce(sum(segment_count),0)::bigint as segments,
       coalesce(sum(center_count),0)::bigint as centers,
       coalesce(sum(signal_count),0)::bigint as signals,
       max(updated_at) as latest_update
  from chan_c_full_recompute_tasks
 where batch_id=$1
 group by chan_level,status
 order by chan_level,status
"""


TASK_HEALTH_SQL = """
/* module-c-execution:task-health */
select count(*) filter (
           where status='failed' and $2::integer is not null and attempts < $2
       )::bigint as retryable_failed,
       count(*) filter (
           where status='failed' and $2::integer is not null and attempts >= $2
       )::bigint as exhausted_failed,
       count(*) filter (
           where status='running'
             and (lease_until is null or lease_until <= clock_timestamp())
       )::bigint as expired_leases
  from chan_c_full_recompute_tasks
 where batch_id=$1
"""


FRESHNESS_SQL = """
/* module-c-execution:freshness */
with levels(chan_level,timeframe,expected_closed_watermark) as (
    select * from unnest($2::integer[],$3::text[],$4::timestamptz[])
)
select levels.chan_level, levels.timeframe, levels.expected_closed_watermark,
       min(checkpoint.shard_start) filter (where checkpoint.rows_scanned > 0)
           as actual_min,
       max(checkpoint.shard_end) filter (where checkpoint.rows_scanned > 0)
           as actual_max,
       count(checkpoint.symbol_id) filter (where checkpoint.rows_scanned = 0)::bigint
           as empty_scopes,
       count(checkpoint.symbol_id) filter (
           where levels.expected_closed_watermark is not null
             and (checkpoint.shard_end is null
                  or checkpoint.shard_end < levels.expected_closed_watermark)
       )::bigint as stale_scopes,
       count(checkpoint.symbol_id) filter (
           where levels.expected_closed_watermark is not null
             and checkpoint.shard_end > levels.expected_closed_watermark
       )::bigint as future_scopes,
       count(checkpoint.symbol_id)::bigint as checkpoint_scopes
  from levels
  left join kline_audit_checkpoints checkpoint
    on checkpoint.audit_run_id=$1::uuid
   and checkpoint.timeframe=levels.chan_level
   and checkpoint.status='completed'
 group by levels.chan_level,levels.timeframe,levels.expected_closed_watermark
 order by levels.chan_level
"""


CHECKPOINT_EVIDENCE_SQL = """
/* module-c-execution:checkpoint-evidence */
select symbol_id,timeframe,status,shard_start,shard_end,rows_scanned,metadata
  from kline_audit_checkpoints
 where audit_run_id=$1::uuid
 order by symbol_id,timeframe,shard_start,shard_end
"""


LIVE_UNIVERSE_SQL = """
/* module-c-execution:live-universe */
select id as symbol_id,code,exchange
  from symbols
 where is_active=true and market='A_SHARE'
 order by id
"""


ACTIVE_CATALOG_SQL = """
/* module-c-execution:active-catalog */
select symbol_id,timeframe,state,bounds_complete,min_ts,max_ts,updated_at
  from kline_scope_catalog
 where generation_id=$1::uuid
   and symbol_id=any($2::integer[])
   and timeframe=any($3::integer[])
 order by symbol_id,timeframe
"""


def _dict(row: Any | None) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        import json

        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return []
    return list(value)


def _aware_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return result if result.tzinfo is not None and result.utcoffset() is not None else None


def _sha(value: Any) -> str | None:
    return str(value) if value is not None and SHA256_RE.fullmatch(str(value)) else None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _manifest_sha256(records: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for record in records:
        digest.update(
            json.dumps(
                _json_value(record),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _uuid_text(value: Any) -> str | None:
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return None


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _validated_freshness_contract(value: Any) -> tuple[dict[str, Any], str] | None:
    payload = _json_object(value)
    if set(payload) != {
        "contract_version", "as_of", "trading_calendar", "expected_closed_watermarks"
    } or payload.get("contract_version") != FRESHNESS_CONTRACT_VERSION:
        return None
    calendar = payload.get("trading_calendar")
    if not isinstance(calendar, Mapping) or set(calendar) != {"id", "sha256"}:
        return None
    calendar_id = calendar.get("id")
    calendar_sha = calendar.get("sha256")
    watermarks = payload.get("expected_closed_watermarks")
    if (
        not isinstance(calendar_id, str)
        or not calendar_id.strip()
        or _sha(calendar_sha) is None
        or not isinstance(watermarks, Mapping)
        or set(watermarks) != {name for _code, name in LEVELS}
    ):
        return None
    as_of = _aware_datetime(payload.get("as_of"))
    parsed = {name: _aware_datetime(watermarks.get(name)) for _code, name in LEVELS}
    if as_of is None or any(value is None or value > as_of for value in parsed.values()):
        return None
    normalized = {
        "contract_version": FRESHNESS_CONTRACT_VERSION,
        "as_of": _utc_text(as_of),
        "trading_calendar": {"id": calendar_id.strip(), "sha256": str(calendar_sha)},
        "expected_closed_watermarks": {
            name: _utc_text(parsed[name]) for _code, name in LEVELS
        },
    }
    return normalized, _manifest_sha256([normalized])


def _typed_strict_provenance(batch: Mapping[str, Any]) -> tuple[dict[str, Any] | None, bool]:
    parameters = _json_object(batch.get("build_parameters"))
    columns = {
        "canonical_audit_run_id": _uuid_text(batch.get("canonical_audit_run_id")),
        "audit_evidence_sha256": _sha(batch.get("audit_evidence_sha256")),
        "audit_checkpoint_sha256": _sha(batch.get("audit_checkpoint_sha256")),
        "freshness_contract_version": batch.get("freshness_contract_version"),
        "freshness_contract_sha256": _sha(batch.get("freshness_contract_sha256")),
        "catalog_generation_id": _uuid_text(batch.get("catalog_generation_id")),
        "catalog_control_revision": _int(batch.get("catalog_control_revision")),
        "catalog_manifest_sha256": _sha(batch.get("catalog_manifest_sha256")),
        "audit_active_universe_sha256": _sha(batch.get("audit_active_universe_sha256")),
    }
    parameter_values = {
        "canonical_audit_run_id": _uuid_text(parameters.get("canonical_audit_run_id")),
        "audit_evidence_sha256": _sha(parameters.get("audit_evidence_sha256")),
        "audit_checkpoint_sha256": _sha(parameters.get("audit_checkpoint_sha256")),
        "freshness_contract_version": parameters.get("freshness_contract_version"),
        "freshness_contract_sha256": _sha(parameters.get("freshness_contract_sha256")),
        "catalog_generation_id": _uuid_text(parameters.get("catalog_generation_id")),
        "catalog_control_revision": _int(parameters.get("catalog_control_revision")),
        "catalog_manifest_sha256": _sha(parameters.get("catalog_manifest_sha256")),
        "audit_active_universe_sha256": _sha(
            parameters.get("audit_active_universe_sha256")
        ),
    }
    try:
        expected_canary_universe = selection_active_universe_sha256(
            parameters.get("canary_selection")
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        expected_canary_universe = None
    canary_universe_matches = bool(
        batch.get("batch_kind") == "canary"
        and parameters.get("scope") == "canary"
        and expected_canary_universe is not None
        and _sha(batch.get("build_active_universe_sha256"))
        == expected_canary_universe
    )
    baseline_universe_matches = bool(
        batch.get("batch_kind") != "canary"
        and _sha(batch.get("build_active_universe_sha256"))
        == columns["audit_active_universe_sha256"]
    )
    valid = (
        parameters.get("policy") == "strict-v2"
        and columns == parameter_values
        and all(columns[field] is not None for field in STRICT_PROVENANCE_FIELDS)
        and columns["freshness_contract_version"] == FRESHNESS_CONTRACT_VERSION
        and columns["catalog_control_revision"] is not None
        and columns["catalog_control_revision"] >= 0
        and (canary_universe_matches or baseline_universe_matches)
    )
    return (columns if valid else None), valid


def _nonnegative_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _checkpoint_evidence_matches(
    batch: Mapping[str, Any], rows: list[Mapping[str, Any]], provenance: Mapping[str, Any]
) -> bool:
    parameters = _json_object(batch.get("audit_parameters"))
    summary = _json_object(batch.get("audit_summary"))
    active_count = _nonnegative_int(parameters.get("active_universe_count"))
    unsigned = dict(parameters)
    evidence_sha = _sha(unsigned.pop("evidence_sha256", None))
    if (
        parameters.get("contract_version") != AUDIT_CONTRACT_VERSION
        or parameters.get("apply_mode") is not False
        or parameters.get("timeframes") != [code for code, _name in LEVELS]
        or active_count is None
        or active_count <= 0
        or evidence_sha != provenance["audit_evidence_sha256"]
        or _manifest_sha256([unsigned]) != evidence_sha
        or summary.get("evidence_sha256") != evidence_sha
        or summary.get("evidence_complete") is not True
        or _uuid_text(parameters.get("catalog_generation_id"))
        != provenance["catalog_generation_id"]
        or _int(parameters.get("catalog_control_revision"))
        != provenance["catalog_control_revision"]
        or _sha(parameters.get("catalog_manifest_sha256"))
        != provenance["catalog_manifest_sha256"]
        or _sha(parameters.get("active_universe_sha256"))
        != provenance["audit_active_universe_sha256"]
    ):
        return False

    expected_keys = active_count * len(LEVELS)
    if len(rows) != expected_keys:
        return False
    seen: set[tuple[int, int]] = set()
    symbols_by_timeframe: dict[int, set[int]] = {
        code: set() for code, _name in LEVELS
    }
    manifest: list[dict[str, Any]] = []
    anomaly_aggregates = Counter({field: 0 for field in ANOMALY_FIELDS})
    rows_scanned_total = 0
    dispositions = Counter()
    for raw in rows:
        row = dict(raw)
        symbol_id = _int(row.get("symbol_id"))
        timeframe = _int(row.get("timeframe"))
        key = (symbol_id, timeframe)
        metadata = _json_object(row.get("metadata"))
        rows_scanned = _nonnegative_int(row.get("rows_scanned"))
        if (
            symbol_id is None
            or timeframe not in {code for code, _name in LEVELS}
            or key in seen
            or row.get("status") != "completed"
            or rows_scanned is None
        ):
            return False
        seen.add(key)
        symbols_by_timeframe[timeframe].add(symbol_id)
        anomalies: dict[str, int] = {}
        for field in ANOMALY_FIELDS:
            count = _nonnegative_int(metadata.get(field))
            if count is None:
                return False
            anomalies[field] = count
            anomaly_aggregates[field] += count
        anomaly_total = sum(anomalies.values())
        disposition = metadata.get("disposition")
        if disposition != ("eligible" if anomaly_total == 0 else "unresolved"):
            return False
        dispositions[str(disposition)] += 1
        shard_start = _aware_datetime(row.get("shard_start"))
        shard_end = _aware_datetime(row.get("shard_end"))
        empty = rows_scanned == 0
        if (
            (empty and (shard_start is not None or shard_end is not None))
            or (not empty and (shard_start is None or shard_end is None))
            or (shard_start is not None and shard_end is not None and shard_start > shard_end)
        ):
            return False
        rows_scanned_total += rows_scanned
        manifest.append({
            "symbol_id": symbol_id,
            "timeframe": timeframe,
            "status": "completed",
            "actual_rows": rows_scanned,
            "actual_shard_start": _utc_text(shard_start) if shard_start else None,
            "actual_shard_end": _utc_text(shard_end) if shard_end else None,
            "disposition": disposition,
            "anomaly_total": anomaly_total,
            **anomalies,
        })
    summary_values = {
        field: _nonnegative_int(summary.get(field))
        for field in ("checkpoints", "rows_scanned", "eligible", "unresolved", *ANOMALY_FIELDS, "anomaly_total")
    }
    return bool(
        len(seen) == expected_keys
        and all(len(symbols) == active_count for symbols in symbols_by_timeframe.values())
        and len({frozenset(symbols) for symbols in symbols_by_timeframe.values()}) == 1
        and _manifest_sha256(manifest) == provenance["audit_checkpoint_sha256"]
        and all(value is not None for value in summary_values.values())
        and summary_values["checkpoints"] == expected_keys
        and summary_values["rows_scanned"] == rows_scanned_total
        and summary_values["eligible"] == dispositions["eligible"]
        and summary_values["unresolved"] == dispositions["unresolved"]
        and all(summary_values[field] == anomaly_aggregates[field] for field in ANOMALY_FIELDS)
        and summary_values["anomaly_total"] == sum(anomaly_aggregates.values())
        and summary.get("gate_pass") is (summary_values["anomaly_total"] == 0)
    )


def _catalog_manifest_matches(
    rows: list[Mapping[str, Any]],
    *,
    symbol_ids: list[int],
    expected_sha256: str,
) -> bool:
    expected_keys = {
        (symbol_id, timeframe)
        for symbol_id in symbol_ids
        for timeframe, _name in LEVELS
    }
    observed_keys: set[tuple[int, int]] = set()
    manifest: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        symbol_id = _int(row.get("symbol_id"))
        timeframe = _int(row.get("timeframe"))
        state = row.get("state")
        min_ts = _aware_datetime(row.get("min_ts"))
        max_ts = _aware_datetime(row.get("max_ts"))
        key = (symbol_id, timeframe)
        valid_bounds = row.get("bounds_complete") is True and (
            (state == "empty" and min_ts is None and max_ts is None)
            or (
                state == "present"
                and min_ts is not None
                and max_ts is not None
                and min_ts <= max_ts
            )
        )
        if key not in expected_keys or key in observed_keys or not valid_bounds:
            return False
        observed_keys.add(key)
        manifest.append({
            "symbol_id": symbol_id,
            "timeframe": timeframe,
            "state": state,
            "bounds_complete": True,
            "min_ts": _json_value(min_ts),
            "max_ts": _json_value(max_ts),
            "updated_at": _json_value(row.get("updated_at")),
        })
    return bool(
        observed_keys == expected_keys
        and len(rows) == len(expected_keys)
        and _manifest_sha256(manifest) == expected_sha256
    )


async def get_module_c_execution_status(conn, *, batch_id: int | None) -> dict[str, Any]:
    counts = _dict(await conn.fetchrow(RUNNING_COUNTS_SQL))
    running_batch_ids = [str(value) for value in counts["running_batch_ids"]]
    effective_batch_id = (
        batch_id
        if batch_id is not None
        else (int(running_batch_ids[0]) if running_batch_ids else None)
    )
    batch = _dict(await conn.fetchrow(BATCH_SQL, effective_batch_id))
    result = {
        "observed_at": counts["observed_at"],
        "readonly": True,
        "running_parent_batches": int(counts["running_parent_batches"]),
        "running_child_batches": int(counts["running_child_batches"]),
        "running_tasks": int(counts["running_tasks"]),
        "running_batch_ids": running_batch_ids,
        "batch": None,
    }
    if not batch:
        return result

    durable_max_attempts = _int(batch.get("frozen_max_attempts"))
    task_rows = [
        {
            "chan_level": int(row["chan_level"]),
            "status": str(row["status"]),
            "count": int(row["count"]),
            "attempts": int(row["attempts"]),
            "bars": int(row["bars"]),
            "strokes": int(row["strokes"]),
            "segments": int(row["segments"]),
            "centers": int(row["centers"]),
            "signals": int(row["signals"]),
            "latest_update": row["latest_update"],
        }
        for row in await conn.fetch(TASK_PROGRESS_SQL, int(batch["batch_id"]))
    ]
    task_health = _dict(
        await conn.fetchrow(TASK_HEALTH_SQL, int(batch["batch_id"]), durable_max_attempts)
    )

    build_parameters = _json_object(batch.get("build_parameters"))
    validated_freshness = _validated_freshness_contract(
        build_parameters.get("freshness_contract")
    )
    freshness_contract_matches = bool(
        validated_freshness is not None
        and validated_freshness[1] == _sha(batch.get("freshness_contract_sha256"))
        and build_parameters.get("freshness_contract_version")
        == FRESHNESS_CONTRACT_VERSION
        and _sha(build_parameters.get("freshness_contract_sha256"))
        == validated_freshness[1]
    )
    normalized_freshness = validated_freshness[0] if validated_freshness else {}
    normalized_watermarks = normalized_freshness.get("expected_closed_watermarks", {})
    expected_values = {
        name: _aware_datetime(normalized_watermarks.get(name))
        for _code, name in LEVELS
    }
    freshness_rows = await conn.fetch(
        FRESHNESS_SQL,
        batch.get("canonical_audit_run_id"),
        [code for code, _name in LEVELS],
        [name for _code, name in LEVELS],
        [expected_values[name] for _code, name in LEVELS],
    )
    freshness_watermarks = [
        {
            "timeframe": str(row["timeframe"]),
            "expected": row["expected_closed_watermark"],
            "actual_min": row["actual_min"],
            "actual_max": row["actual_max"],
            "empty_scopes": int(row["empty_scopes"]),
            "stale_scopes": int(row["stale_scopes"]),
            "future_scopes": int(row["future_scopes"]),
        }
        for row in freshness_rows
    ]
    audit_active_universe_count = _nonnegative_int(
        _json_object(batch.get("audit_parameters")).get("active_universe_count")
    )
    exact_five = (
        len(freshness_rows) == len(LEVELS)
        and [int(row["chan_level"]) for row in freshness_rows] == [code for code, _ in LEVELS]
        and audit_active_universe_count is not None
        and audit_active_universe_count > 0
        and all(
            int(row["checkpoint_scopes"]) == audit_active_universe_count
            for row in freshness_rows
        )
    )
    expected_complete = freshness_contract_matches and all(expected_values.values())
    audit_completed = batch.get("audit_status") == "completed" and batch.get("audit_apply_mode") is False
    raw_audit_gate_pass = _json_object(batch.get("audit_summary")).get("gate_pass")
    audit_gate_pass = raw_audit_gate_pass if isinstance(raw_audit_gate_pass, bool) else None
    strict_provenance, strict_parameters_match = _typed_strict_provenance(batch)
    selection = evaluate_selection_evidence(
        build_parameters,
        strict_provenance,
        batch.get("build_active_universe_sha256"),
        applicable=batch.get("batch_kind") == "canary",
    )
    checkpoint_rows = (
        await conn.fetch(CHECKPOINT_EVIDENCE_SQL, batch.get("canonical_audit_run_id"))
        if strict_provenance is not None
        else []
    )
    checkpoint_evidence_matches = bool(
        strict_provenance is not None
        and _checkpoint_evidence_matches(batch, checkpoint_rows, strict_provenance)
    )
    live_universe_rows = (
        await conn.fetch(LIVE_UNIVERSE_SQL) if strict_provenance is not None else []
    )
    live_universe_manifest = [
        {
            "symbol_id": int(row["symbol_id"]),
            "symbol": f"{str(row['code'])}.{str(row['exchange']).upper()}",
        }
        for row in live_universe_rows
    ]
    live_universe_matches = bool(
        strict_provenance is not None
        and live_universe_manifest
        and _manifest_sha256(live_universe_manifest)
        == strict_provenance["audit_active_universe_sha256"]
    )
    live_symbol_ids = [int(row["symbol_id"]) for row in live_universe_rows]
    active_catalog_rows = (
        await conn.fetch(
            ACTIVE_CATALOG_SQL,
            batch.get("catalog_generation_id"),
            live_symbol_ids,
            [code for code, _name in LEVELS],
        )
        if strict_provenance is not None and live_symbol_ids
        else []
    )
    catalog_manifest_matches = bool(
        strict_provenance is not None
        and _catalog_manifest_matches(
            active_catalog_rows,
            symbol_ids=live_symbol_ids,
            expected_sha256=strict_provenance["catalog_manifest_sha256"],
        )
    )
    future_levels = [
        str(row["timeframe"]) for row in freshness_rows if int(row["future_scopes"]) > 0
    ]
    if not (
        audit_completed
        and expected_complete
        and exact_five
        and strict_parameters_match
        and checkpoint_evidence_matches
        and not future_levels
    ):
        freshness_status = "unavailable"
        freshness_reasons = (
            [f"{timeframe}_future_scopes" for timeframe in future_levels]
            or ["freshness_evidence_incomplete"]
        )
    else:
        stale_levels = [
            str(row["timeframe"]) for row in freshness_rows if int(row["stale_scopes"]) > 0
        ]
        freshness_status = "stale" if stale_levels else "current"
        freshness_reasons = [f"{timeframe}_stale_scopes" for timeframe in stale_levels]

    catalog_is_active = (
        batch.get("catalog_generation_id") is not None
        and batch.get("catalog_generation_id") == batch.get("live_catalog_generation_id")
    )
    catalog_revision_matches = (
        _int(batch.get("catalog_control_revision")) is not None
        and _int(batch.get("catalog_control_revision"))
        == _int(batch.get("live_catalog_control_revision"))
    )
    eligibility_manifest_matches = (
        _sha(batch.get("eligibility_manifest_sha256")) is not None
        and batch.get("eligibility_manifest_sha256") == batch.get("build_manifest_sha256")
    )
    config_hash_matches = (
        batch.get("config_hash") is not None
        and batch.get("config_hash") == batch.get("child_config_hash")
        and batch.get("config_hash") == batch.get("build_config_hash")
    )
    execution_identity_matches = bool(
        batch.get("batch_kind") in {"canary", "baseline"}
        and all(
            isinstance(batch.get(field), str) and bool(str(batch.get(field)).strip())
            for field in (
                "config_hash",
                "child_config_hash",
                "run_group_id",
                "child_run_group_id",
                "publication_namespace",
                "child_publication_namespace",
                "profile_id",
                "child_profile_id",
            )
        )
        and batch.get("config_hash") == batch.get("child_config_hash")
        and batch.get("run_group_id") == batch.get("child_run_group_id")
        and batch.get("publication_namespace")
        == batch.get("child_publication_namespace")
        and batch.get("profile_id") == batch.get("child_profile_id")
    )
    expected_frozen_config = {
        "contract": FROZEN_CONTRACT,
        "levels": [name for _code, name in LEVELS],
        "modes": list(FROZEN_MODES),
        "concurrency_per_worker": 1,
        "shard_count": _int(batch.get("shard_count")),
        "eligibility_build_id": batch.get("eligibility_build_id"),
        "max_attempts": durable_max_attempts,
    }
    frozen_config_matches = bool(
        _json_object(batch.get("frozen_effective_config")) == expected_frozen_config
        and batch.get("frozen_contract") == FROZEN_CONTRACT
        and _string_list(batch.get("frozen_levels"))
        == [name for _code, name in LEVELS]
        and _string_list(batch.get("frozen_modes")) == list(FROZEN_MODES)
        and _int(batch.get("frozen_concurrency_per_worker")) == 1
        and _int(batch.get("frozen_shard_count")) == _int(batch.get("shard_count"))
        and _uuid_text(batch.get("frozen_eligibility_build_id"))
        == _uuid_text(batch.get("eligibility_build_id"))
    )
    drift_reasons: list[str] = []
    if not audit_completed:
        drift_reasons.append("canonical_audit_not_completed")
    if audit_gate_pass is not True:
        drift_reasons.append("canonical_audit_gate_failed")
    if batch.get("catalog_generation_status") != "complete":
        drift_reasons.append("catalog_generation_not_complete")
    if not catalog_is_active:
        drift_reasons.append("catalog_generation_not_active")
    if not catalog_revision_matches:
        drift_reasons.append("catalog_control_revision_drift")
    if not eligibility_manifest_matches:
        drift_reasons.append("eligibility_manifest_drift")
    if not config_hash_matches:
        drift_reasons.append("config_hash_drift")
    if not execution_identity_matches:
        drift_reasons.append("execution_identity_drift")
    if not frozen_config_matches:
        drift_reasons.append("frozen_execution_contract_drift")
    if not strict_parameters_match:
        drift_reasons.append("strict_provenance_parameters_drift")
    if not freshness_contract_matches:
        drift_reasons.append("freshness_contract_drift")
    if not checkpoint_evidence_matches:
        drift_reasons.append("canonical_audit_evidence_or_checkpoint_drift")
    if not live_universe_matches:
        drift_reasons.append("active_universe_drift")
    if not catalog_manifest_matches:
        drift_reasons.append("active_catalog_manifest_drift")
    if durable_max_attempts is None or durable_max_attempts < 1:
        drift_reasons.append("frozen_max_attempts_invalid")
    if future_levels:
        drift_reasons.append("authoritative_freshness_future")
    elif freshness_status == "stale":
        drift_reasons.append("authoritative_freshness_stale")
    elif freshness_status == "unavailable":
        drift_reasons.append("freshness_evidence_unavailable")
    drift_reasons.extend(selection["drift_reasons"])

    evidence_complete = bool(
        strict_parameters_match
        and freshness_contract_matches
        and checkpoint_evidence_matches
        and live_universe_matches
        and catalog_manifest_matches
        and frozen_config_matches
        and audit_completed
        and audit_gate_pass is True
        and batch.get("catalog_generation_status") == "complete"
        and catalog_is_active
        and catalog_revision_matches
        and eligibility_manifest_matches
        and config_hash_matches
        and execution_identity_matches
        and freshness_status == "current"
        and selection["status"] in {"pass", "not_applicable"}
        and durable_max_attempts is not None
        and durable_max_attempts > 0
    )

    result["batch"] = {
        "batch_id": str(batch["batch_id"]),
        "batch_key": str(batch["batch_key"]),
        "batch_kind": str(batch["batch_kind"]),
        "parent_status": str(batch["parent_status"]),
        "child_status": str(batch["child_status"]),
        "publication_namespace": str(batch["publication_namespace"]),
        "profile_id": str(batch["profile_id"]),
        "run_group_id": str(batch["run_group_id"]),
        "code_commit": str(batch["code_commit"]),
        "image_digest": str(batch["image_digest"]),
        "vendor_manifest_sha256": str(batch["vendor_manifest_sha256"]),
        "config_hash": str(batch["config_hash"]),
        "created_at": batch["created_at"],
        "started_at": batch["started_at"],
        "finished_at": batch["finished_at"],
        "updated_at": batch["updated_at"],
        "execution": {
            "shard_count": int(batch["shard_count"]),
            "active_symbols": int(batch["active_symbols"]),
            "disposition_rows": int(batch["disposition_rows"]),
            "latest_task_update": batch["latest_task_update"],
            "retryable_failed": (
                int(task_health["retryable_failed"]) if durable_max_attempts else None
            ),
            "exhausted_failed": (
                int(task_health["exhausted_failed"]) if durable_max_attempts else None
            ),
            "expired_leases": int(task_health["expired_leases"]),
            "tasks": task_rows,
        },
        "frozen_config": {
            "contract": batch.get("frozen_contract"),
            "levels": _string_list(batch.get("frozen_levels")),
            "modes": _string_list(batch.get("frozen_modes")),
            "concurrency_per_worker": _int(batch.get("frozen_concurrency_per_worker")),
            "shard_count": _int(batch.get("frozen_shard_count")),
            "max_attempts": durable_max_attempts,
            "eligibility_build_id": batch.get("frozen_eligibility_build_id"),
        },
        "freshness": {
            "as_of": _aware_datetime(normalized_freshness.get("as_of")),
            "status": freshness_status,
            "reasons": freshness_reasons,
            "expected_closed_watermarks": [
                {"timeframe": name, "expected": expected_values[name]}
                for _code, name in LEVELS
            ],
            "actual_checkpoint_watermarks": freshness_watermarks,
        },
        "provenance": {
            "policy": batch.get("policy"),
            "eligibility_build_id": batch.get("eligibility_build_id"),
            "manifest_version": batch.get("manifest_version"),
            "eligibility_manifest_sha256": batch.get("eligibility_manifest_sha256"),
            "build_manifest_sha256": batch.get("build_manifest_sha256"),
            "canonical_audit_run_id": batch.get("canonical_audit_run_id"),
            "audit_evidence_sha256": batch.get("audit_evidence_sha256"),
            "audit_checkpoint_sha256": batch.get("audit_checkpoint_sha256"),
            "audit_status": batch.get("audit_status"),
            "audit_apply_mode": batch.get("audit_apply_mode"),
            "audit_gate_pass": audit_gate_pass,
            "freshness_contract_version": batch.get("freshness_contract_version"),
            "freshness_contract_sha256": batch.get("freshness_contract_sha256"),
            "catalog_generation_id": batch.get("catalog_generation_id"),
            "catalog_control_revision": _int(batch.get("catalog_control_revision")),
            "catalog_manifest_sha256": batch.get("catalog_manifest_sha256"),
            "audit_active_universe_sha256": batch.get("audit_active_universe_sha256"),
            "catalog_generation_status": batch.get("catalog_generation_status"),
            "catalog_is_active": catalog_is_active,
            "live_catalog_control_revision": _int(batch.get("live_catalog_control_revision")),
            "catalog_revision_matches": catalog_revision_matches,
            "eligibility_manifest_matches": eligibility_manifest_matches,
            "config_hash_matches": config_hash_matches,
            "execution_identity_matches": execution_identity_matches,
            "frozen_config_matches": frozen_config_matches,
            "live_universe_matches": live_universe_matches,
            "catalog_manifest_matches": catalog_manifest_matches,
            "selection": selection,
            "evidence_complete": evidence_complete,
            "drift_reasons": drift_reasons,
        },
    }
    return result
