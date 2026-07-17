from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Mapping


LEVELS = ((5, "5f"), (30, "30f"), (1440, "1d"), (10080, "1w"), (43200, "1m"))
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


RUNNING_COUNTS_SQL = """
/* module-c-execution:running-counts */
select clock_timestamp() as observed_at,
       (select count(*)::bigint
          from chan_c_batches parent
          join chan_c_full_recompute_batches child on child.batch_id=parent.id
         where parent.status='running')
           as running_parent_batches,
       (select count(*)::bigint from chan_c_full_recompute_batches where status='running')
           as running_child_batches,
       (select count(*)::bigint from chan_c_full_recompute_tasks where status='running')
           as running_tasks
"""


BATCH_SQL = """
/* module-c-execution:batch */
select parent.id as batch_id, parent.batch_key, parent.batch_kind,
       parent.status as parent_status, child.status as child_status,
       parent.publication_namespace, parent.profile_id, parent.run_group_id,
       parent.code_commit, parent.image_digest, parent.vendor_manifest_sha256,
       parent.config_hash, parent.created_at, child.started_at, child.finished_at,
       child.updated_at, child.shard_count, child.active_symbols,
       child.disposition_rows,
       (select max(task.updated_at) from chan_c_full_recompute_tasks task
         where task.batch_id=parent.id) as latest_task_update,
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
       build.parameters#>>'{freshness_contract,as_of}' as freshness_as_of,
       build.parameters#>>'{freshness_contract,expected_closed_watermarks,5f}'
           as freshness_expected_5f,
       build.parameters#>>'{freshness_contract,expected_closed_watermarks,30f}'
           as freshness_expected_30f,
       build.parameters#>>'{freshness_contract,expected_closed_watermarks,1d}'
           as freshness_expected_1d,
       build.parameters#>>'{freshness_contract,expected_closed_watermarks,1w}'
           as freshness_expected_1w,
       build.parameters#>>'{freshness_contract,expected_closed_watermarks,1m}'
           as freshness_expected_1m,
       build.build_id::text as eligibility_build_id,
       build.manifest_version, parent.eligible_manifest_sha256,
       build.manifest_hash as build_manifest_sha256,
       build.config_hash as build_config_hash,
       build.canonical_audit_run_id::text, build.audit_evidence_sha256,
       build.audit_checkpoint_sha256, audit.status as audit_status,
       audit.apply_mode as audit_apply_mode,
       audit.parameters->>'active_universe_count' as audit_active_universe_count,
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
           where status='running' and lease_until <= clock_timestamp()
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
       count(checkpoint.symbol_id)::bigint as checkpoint_scopes
  from levels
  left join kline_audit_checkpoints checkpoint
    on checkpoint.audit_run_id=$1::uuid
   and checkpoint.timeframe=levels.chan_level
   and checkpoint.status='completed'
 group by levels.chan_level,levels.timeframe,levels.expected_closed_watermark
 order by levels.chan_level
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


async def get_module_c_execution_status(conn, *, batch_id: int | None) -> dict[str, Any]:
    counts = _dict(await conn.fetchrow(RUNNING_COUNTS_SQL))
    batch = _dict(await conn.fetchrow(BATCH_SQL, batch_id))
    result = {
        "observed_at": counts["observed_at"],
        "readonly": True,
        "running_parent_batches": int(counts["running_parent_batches"]),
        "running_child_batches": int(counts["running_child_batches"]),
        "running_tasks": int(counts["running_tasks"]),
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

    expected_values = {
        name: _aware_datetime(batch.get(f"freshness_expected_{name}"))
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
        }
        for row in freshness_rows
    ]
    audit_active_universe_count = _int(batch.get("audit_active_universe_count"))
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
    expected_complete = all(expected_values.values())
    audit_completed = batch.get("audit_status") == "completed" and batch.get("audit_apply_mode") is False
    if not (audit_completed and expected_complete and exact_five):
        freshness_status = "unavailable"
        freshness_reasons = ["freshness_evidence_incomplete"]
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
        and batch.get("config_hash") == batch.get("build_config_hash")
    )
    drift_reasons: list[str] = []
    if not audit_completed:
        drift_reasons.append("canonical_audit_not_completed")
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
    if durable_max_attempts is None or durable_max_attempts < 1:
        drift_reasons.append("frozen_max_attempts_invalid")
    if freshness_status == "stale":
        drift_reasons.append("authoritative_freshness_stale")
    elif freshness_status == "unavailable":
        drift_reasons.append("freshness_evidence_unavailable")

    typed_evidence = (
        batch.get("policy") == "strict-v2"
        and batch.get("canonical_audit_run_id") is not None
        and _sha(batch.get("audit_evidence_sha256")) is not None
        and _sha(batch.get("audit_checkpoint_sha256")) is not None
        and batch.get("freshness_contract_version") == "module-c-authoritative-freshness-v1"
        and _sha(batch.get("freshness_contract_sha256")) is not None
        and batch.get("catalog_generation_id") is not None
        and _int(batch.get("catalog_control_revision")) is not None
        and _sha(batch.get("catalog_manifest_sha256")) is not None
        and _sha(batch.get("audit_active_universe_sha256")) is not None
    )
    evidence_complete = bool(
        typed_evidence
        and audit_completed
        and batch.get("catalog_generation_status") == "complete"
        and catalog_is_active
        and catalog_revision_matches
        and eligibility_manifest_matches
        and config_hash_matches
        and freshness_status == "current"
        and durable_max_attempts is not None
        and durable_max_attempts > 0
    )

    result["batch"] = {
        "batch_id": int(batch["batch_id"]),
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
            "as_of": _aware_datetime(batch.get("freshness_as_of")),
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
            "evidence_complete": evidence_complete,
            "drift_reasons": drift_reasons,
        },
    }
    return result
