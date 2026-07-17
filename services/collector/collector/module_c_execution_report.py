"""Read-only evidence report for one fenced Module C recompute batch."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import asyncpg

from collector.lifecycle_reconciliation import build_reconciliation
from collector.module_c_eligibility import (
    FRESHNESS_CONTRACT_VERSION,
    _SHA256_RE,
    _load_strict_inputs,
    parse_freshness_contract,
)


LEVEL_NAMES = {5: "5f", 30: "30f", 1440: "1d", 10080: "1w", 43200: "1m"}
MODES = ("confirmed", "predictive")

BATCH_SQL = """
/* report:batch */
select batch.batch_id, batch.eligibility_build_id::text, batch.run_group_id,
       batch.config_hash, batch.publication_namespace, batch.profile_id,
       batch.shard_count, batch.status, batch.active_symbols,
       batch.disposition_rows, batch.created_at, batch.started_at,
       batch.finished_at, batch.updated_at,
       evidence.batch_key, evidence.batch_kind, evidence.code_commit,
       evidence.image_digest, evidence.vendor_manifest_sha256,
       evidence.eligible_manifest_uri, evidence.eligible_manifest_sha256,
       evidence.input_watermark, evidence.audit_references,
       eligibility.manifest_version, eligibility.active_universe_hash,
       eligibility.manifest_hash, eligibility.parameters as eligibility_parameters,
       eligibility.summary as eligibility_summary,
       eligibility.canonical_audit_run_id::text,
       eligibility.audit_evidence_sha256,
       eligibility.audit_checkpoint_sha256,
       eligibility.freshness_contract_version,
       eligibility.freshness_contract_sha256,
       eligibility.catalog_generation_id::text,
       eligibility.catalog_control_revision,
       eligibility.catalog_manifest_sha256,
       eligibility.audit_active_universe_sha256
from chan_c_full_recompute_batches batch
join chan_c_batches evidence on evidence.id = batch.batch_id
join module_c_eligibility_builds eligibility
  on eligibility.build_id = batch.eligibility_build_id
where batch.batch_id = $1
"""

TASK_PROGRESS_SQL = """
/* report:task-progress */
select chan_level, status, count(*)::bigint as count,
       coalesce(sum(attempts), 0)::bigint as attempts,
       coalesce(sum(bar_count), 0)::bigint as bars,
       coalesce(sum(stroke_count), 0)::bigint as strokes,
       coalesce(sum(segment_count), 0)::bigint as segments,
       coalesce(sum(center_count), 0)::bigint as centers,
       coalesce(sum(signal_count), 0)::bigint as signals,
       max(updated_at) as latest_update
from chan_c_full_recompute_tasks
where batch_id = $1
group by chan_level, status
order by chan_level, status
"""

HEAD_COVERAGE_SQL = """
/* report:head-coverage */
with expected as (
    select task.symbol_id, task.symbol, task.chan_level, task.status as task_status,
           task.run_id as task_run_id, task.target_bar_until,
           task.bar_count as task_bar_count, mode.mode
    from chan_c_full_recompute_tasks task
    cross join (values ('confirmed'::varchar), ('predictive'::varchar)) mode(mode)
    where task.batch_id = $1 and task.eligible
), coverage as (
    select expected.*,
           head.run_id as head_run_id,
           head.batch_id as head_batch_id,
           head_run.batch_id as head_run_batch_id,
           head_run.status as head_run_status,
           task_run.status as task_run_status,
           (
               task_run.status = 'success'
               and task_run.symbol_id = head_run.symbol_id
               and task_run.chan_level = head_run.chan_level
               and task_run.mode = head_run.mode
               and task_run.input_signature = head_run.input_signature
               and task_run.config_hash = head_run.config_hash
               and task_run.bar_from is not distinct from head_run.bar_from
               and task_run.bar_until = head_run.bar_until
               and task_run.bar_until = expected.target_bar_until
               and task_run.bar_count is not distinct from head_run.bar_count
               and task_run.bar_count is not distinct from expected.task_bar_count
               and task_run.base_timeframe = head_run.base_timeframe
           ) as input_identity_equivalent,
           history.id as history_id,
           outbox.id as outbox_id,
           outbox.status as outbox_status
    from expected
    left join scheme2_chan_c_published_heads head
      on head.symbol_id = expected.symbol_id
     and head.chan_level = expected.chan_level
     and head.mode = expected.mode
     and head.base_timeframe = expected.chan_level
     and head.status = 'published'
    left join chan_c_runs head_run on head_run.id = head.run_id
    left join chan_c_runs task_run
      on task_run.id = expected.task_run_id and task_run.batch_id = $1
    left join chan_c_head_history history
      on history.symbol_id = expected.symbol_id
     and history.chan_level = expected.chan_level
     and history.mode = expected.mode
     and history.base_timeframe = head.base_timeframe
     and history.new_run_id = head.run_id
    left join chan_c_head_outbox outbox on outbox.head_history_id = history.id
), assessed as (
    select coverage.*,
           (
               head_run_status = 'success'
               and (
                   (head_batch_id = $1 and head_run_batch_id = $1)
                   or (task_status = 'completed' and input_identity_equivalent)
               )
           ) as covered,
           (
               head_run_status = 'success'
               and head_batch_id = $1
               and head_run_batch_id = $1
           ) as direct_batch,
           (
               head_run_status = 'success'
               and head_batch_id is distinct from $1
               and task_status = 'completed'
               and input_identity_equivalent
           ) as equivalent_noop
    from coverage
)
select chan_level, mode, count(*)::bigint as expected,
       count(*) filter (where covered)::bigint as published,
       count(*) filter (where not coalesce(covered, false))::bigint as missing,
       count(*) filter (where direct_batch)::bigint as direct_batch,
       count(*) filter (where equivalent_noop)::bigint as equivalent_noop,
       count(*) filter (where history_id is null)::bigint as missing_history,
       count(*) filter (where outbox_id is null)::bigint as missing_outbox,
       count(*) filter (where outbox_status is distinct from 'completed')::bigint
           as outbox_incomplete
from assessed
group by chan_level, mode
order by chan_level, mode
"""

OUTBOX_SQL = """
/* report:outbox */
select outbox.status, count(*)::bigint as count
from chan_c_head_outbox outbox
join chan_c_head_history history on history.id = outbox.head_history_id
join chan_c_runs run on run.id = history.new_run_id
where run.batch_id = $1
group by outbox.status
order by outbox.status
"""

FAILURES_SQL = """
/* report:failures */
select symbol, chan_level, status, attempts, worker_id, last_error, updated_at
from chan_c_full_recompute_tasks
where batch_id = $1 and (status = 'failed' or last_error is not null)
order by updated_at desc, symbol, chan_level
limit 100
"""

CANONICAL_SQL = """
/* report:canonical */
select audit_run_id::text, started_at, completed_at, status, apply_mode,
       parameters, summary, failure
from kline_audit_runs
where audit_run_id = $1::uuid
"""

OFFICIAL_SQL = """
/* report:official */
with expected as (
    select task.symbol_id, task.chan_level, mode.mode
    from chan_c_full_recompute_tasks task
    cross join (values ('confirmed'::varchar), ('predictive'::varchar)) mode(mode)
    where task.batch_id = $1 and task.eligible
)
select
  (select count(*)::bigint from expected) as official_expected_heads,
  (select count(*)::bigint from expected
    where exists (
      select 1 from chan_c_head_history history
      where history.symbol_id = expected.symbol_id
        and history.chan_level = expected.chan_level
        and history.mode = expected.mode
        and history.publication_profile = 'historical_replay'
    )) as historical_replay_heads,
  (select count(*)::bigint from expected
    where not exists (
      select 1 from chan_c_head_history history
      where history.symbol_id = expected.symbol_id
        and history.chan_level = expected.chan_level
        and history.mode = expected.mode
        and history.publication_profile = 'historical_replay'
    )) as official_missing_heads,
  (select count(*)::bigint
     from chan_c_head_history history
     join chan_structure_lifecycle_events event on event.head_history_id = history.id
    where history.publication_profile = 'baseline'
      and event.event_type in ('first_seen', 'confirmed')
      and exists (
        select 1 from expected
        where expected.symbol_id = history.symbol_id
          and expected.chan_level = history.chan_level
          and expected.mode = history.mode
      )) as baseline_claimed_historical_events,
  (select count(*)::bigint
     from chan_c_head_history history
     join chan_structure_lifecycle_events event on event.head_history_id = history.id
    where history.publication_profile = 'historical_replay'
      and event.event_type = 'first_seen'
      and event.effective_time < event.point_time
      and exists (
        select 1 from expected
        where expected.symbol_id = history.symbol_id
          and expected.chan_level = history.chan_level
          and expected.mode = history.mode
      )) as future_leak_events
"""

DB_RESOURCE_SQL = """
/* report:db-resource */
select current_database() as database_name,
       pg_database_size(current_database())::bigint as database_size_bytes,
       pg_current_wal_lsn()::text as current_wal_lsn,
       count(*) filter (where state = 'active')::bigint as active_queries
from pg_stat_activity
"""


def _dict(row: Any | None) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        decoded = json.loads(value)
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone().isoformat()
    return value


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=_json_value) + "\n"


def _jsonl(rows: Sequence[Mapping[str, Any]]) -> str:
    return "".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True, default=_json_value) + "\n" for row in rows)


def _failed_provenance(
    failure_code: str,
    validation_error: str,
    *,
    policy: str = "strict-v2",
    frozen: Mapping[str, Any] | None = None,
    observed: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "readonly": True,
        "kline_rows_scanned": False,
        "policy": policy,
        "decision": "FAIL",
        "failure_code": failure_code,
        "validation_error": validation_error,
        "frozen": dict(frozen or {}),
        "observed": dict(observed or {}),
    }


async def build_strict_v2_provenance(
    conn: Any,
    batch: Mapping[str, Any],
) -> dict[str, Any]:
    parameters = _json_object(batch.get("eligibility_parameters"))
    if parameters.get("policy") != "strict-v2":
        return _failed_provenance(
            "unavailable",
            "Eligibility build does not declare the strict-v2 policy",
            policy=str(parameters.get("policy") or ""),
        )
    try:
        frozen = {
            "canonical_audit_run_id": str(
                uuid.UUID(str(batch["canonical_audit_run_id"]))
            ),
            "audit_evidence_sha256": str(batch["audit_evidence_sha256"]),
            "audit_checkpoint_sha256": str(batch["audit_checkpoint_sha256"]),
            "freshness_contract_version": str(batch["freshness_contract_version"]),
            "freshness_contract_sha256": str(batch["freshness_contract_sha256"]),
            "catalog_generation_id": str(
                uuid.UUID(str(batch["catalog_generation_id"]))
            ),
            "catalog_control_revision": int(batch["catalog_control_revision"]),
            "catalog_manifest_sha256": str(batch["catalog_manifest_sha256"]),
            "audit_active_universe_sha256": str(
                batch["audit_active_universe_sha256"]
            ),
        }
        parameter_provenance = {
            "canonical_audit_run_id": str(
                uuid.UUID(str(parameters["canonical_audit_run_id"]))
            ),
            "audit_evidence_sha256": str(parameters["audit_evidence_sha256"]),
            "audit_checkpoint_sha256": str(parameters["audit_checkpoint_sha256"]),
            "freshness_contract_version": str(
                parameters["freshness_contract_version"]
            ),
            "freshness_contract_sha256": str(
                parameters["freshness_contract_sha256"]
            ),
            "catalog_generation_id": str(
                uuid.UUID(str(parameters["catalog_generation_id"]))
            ),
            "catalog_control_revision": int(parameters["catalog_control_revision"]),
            "catalog_manifest_sha256": str(parameters["catalog_manifest_sha256"]),
            "audit_active_universe_sha256": str(
                parameters["audit_active_universe_sha256"]
            ),
        }
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        return _failed_provenance(
            "unavailable",
            f"Strict-v2 frozen provenance is incomplete: {error}",
        )
    sha_fields = (
        "audit_evidence_sha256",
        "audit_checkpoint_sha256",
        "freshness_contract_sha256",
        "catalog_manifest_sha256",
        "audit_active_universe_sha256",
    )
    if (
        any(not _SHA256_RE.fullmatch(frozen[field]) for field in sha_fields)
        or frozen["freshness_contract_version"] != FRESHNESS_CONTRACT_VERSION
        or isinstance(batch["catalog_control_revision"], bool)
        or isinstance(parameters["catalog_control_revision"], bool)
        or frozen["catalog_control_revision"] < 0
    ):
        return _failed_provenance(
            "unavailable",
            "Strict-v2 frozen provenance has invalid field values",
            frozen=frozen,
        )
    try:
        freshness = parse_freshness_contract(parameters["freshness_contract"])
    except (KeyError, TypeError, ValueError) as error:
        return _failed_provenance(
            "unavailable",
            f"Strict-v2 freshness contract is unavailable: {error}",
            frozen=frozen,
        )
    if (
        parameter_provenance != frozen
        or freshness.contract_version != frozen["freshness_contract_version"]
        or freshness.sha256 != frozen["freshness_contract_sha256"]
        or str(batch.get("active_universe_hash"))
        != frozen["audit_active_universe_sha256"]
    ):
        return _failed_provenance(
            "drift",
            "Strict-v2 eligibility columns, parameters, or freshness contract drifted",
            frozen=frozen,
            observed=parameter_provenance,
        )
    try:
        strict = await _load_strict_inputs(
            conn,
            frozen["canonical_audit_run_id"],
            freshness,
            for_share=False,
        )
    except RuntimeError as error:
        return _failed_provenance(
            "drift",
            str(error),
            frozen=frozen,
        )
    observed = {
        "canonical_audit_run_id": frozen["canonical_audit_run_id"],
        "audit_evidence_sha256": strict.audit_evidence_sha256,
        "audit_checkpoint_sha256": strict.audit_checkpoint_sha256,
        "freshness_contract_version": freshness.contract_version,
        "freshness_contract_sha256": freshness.sha256,
        "catalog_generation_id": str(strict.catalog_generation_id),
        "catalog_control_revision": strict.catalog_control_revision,
        "catalog_manifest_sha256": strict.catalog_manifest_sha256,
        "audit_active_universe_sha256": strict.audit_active_universe_sha256,
    }
    if observed != frozen:
        return _failed_provenance(
            "drift",
            "Live strict-v2 inputs no longer match the frozen eligibility provenance",
            frozen=frozen,
            observed=observed,
        )
    return {
        "readonly": True,
        "kline_rows_scanned": False,
        "policy": "strict-v2",
        "decision": "PASS",
        "failure_code": None,
        "validation_error": None,
        "frozen": frozen,
        "observed": observed,
    }


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _local_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


async def build_report(
    conn: Any,
    batch_id: int,
    *,
    resource_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    generated_at = datetime.now().astimezone().isoformat()
    batch = _dict(await conn.fetchrow(BATCH_SQL, batch_id))
    if not batch:
        raise ValueError(f"Module C recompute batch does not exist: {batch_id}")
    strict_v2_provenance = await build_strict_v2_provenance(conn, batch)

    task_rows = [_dict(row) for row in await conn.fetch(TASK_PROGRESS_SQL, batch_id)]
    head_rows = [_dict(row) for row in await conn.fetch(HEAD_COVERAGE_SQL, batch_id)]
    outbox_rows = [_dict(row) for row in await conn.fetch(OUTBOX_SQL, batch_id)]
    failure_rows = [_dict(row) for row in await conn.fetch(FAILURES_SQL, batch_id)]
    pinned_audit_run_id = strict_v2_provenance["frozen"].get(
        "canonical_audit_run_id"
    )
    canonical = _dict(await conn.fetchrow(CANONICAL_SQL, pinned_audit_run_id))
    official = _dict(await conn.fetchrow(OFFICIAL_SQL, batch_id))
    db_resource = _dict(await conn.fetchrow(DB_RESOURCE_SQL))
    lifecycle = await build_reconciliation(conn)

    statuses: dict[str, int] = {}
    levels: dict[str, dict[str, Any]] = {}
    for row in task_rows:
        status = str(row["status"])
        count = int(row["count"])
        statuses[status] = statuses.get(status, 0) + count
        level = LEVEL_NAMES.get(int(row["chan_level"]), str(row["chan_level"]))
        levels.setdefault(level, {})[status] = count
    expected_heads = sum(int(row["expected"]) for row in head_rows)
    published_heads = sum(int(row["published"]) for row in head_rows)
    missing_heads = sum(int(row["missing"]) for row in head_rows)
    missing_history = sum(int(row["missing_history"]) for row in head_rows)
    missing_outbox = sum(int(row["missing_outbox"]) for row in head_rows)
    incomplete_outbox = sum(int(row["outbox_incomplete"]) for row in head_rows)
    outbox_statuses = {str(row["status"]): int(row["count"]) for row in outbox_rows}
    outbox_blocking = sum(outbox_statuses.get(status, 0) for status in ("pending", "processing", "failed", "dead_letter"))

    summary = {
        "batch_id": batch_id,
        "batch_status": batch["status"],
        "tasks": {
            "expected": int(batch["disposition_rows"]),
            "observed": sum(statuses.values()),
            "statuses": statuses,
            "by_level": levels,
        },
        "heads": {
            "expected": expected_heads,
            "published": published_heads,
            "missing": missing_heads,
            "missing_history": missing_history,
            "missing_outbox": missing_outbox,
            "outbox_incomplete": incomplete_outbox,
        },
        "outbox": {"statuses": outbox_statuses, "blocking": outbox_blocking},
        "lifecycle": {
            "decision": lifecycle["decision"],
            "blockers": lifecycle["blockers"],
        },
        "official": {key: int(value or 0) for key, value in official.items()},
        "provenance": {
            "decision": strict_v2_provenance["decision"],
            "failure_code": strict_v2_provenance["failure_code"],
        },
    }

    blockers: list[dict[str, Any]] = []

    def block(code: str, evidence: Any, minimum_fix: str) -> None:
        blockers.append({"code": code, "evidence": evidence, "minimum_fix": minimum_fix})

    unfinished = statuses.get("pending", 0) + statuses.get("running", 0)
    if batch["status"] != "completed" or unfinished:
        block("recompute_incomplete", {"batch_status": batch["status"], "unfinished": unfinished}, "Resume fenced tasks from the existing batch until none are pending or running.")
    if statuses.get("failed", 0):
        block("recompute_failed", {"failed": statuses["failed"]}, "Repair only listed failures and retry them in the same batch.")
    if sum(statuses.values()) != int(batch["disposition_rows"]):
        block("task_manifest_not_fully_accounted", summary["tasks"], "Restore the frozen task manifest before declaring the batch complete.")
    if missing_heads or missing_history or missing_outbox:
        block("published_head_coverage_incomplete", summary["heads"], "Publish complete eligible heads and their transactional history/outbox evidence.")
    if outbox_blocking or incomplete_outbox:
        block("outbox_not_drained", summary["outbox"], "Resume the durable observer until every batch outbox item is completed.")
    if lifecycle["decision"] != "PASS":
        block("lifecycle_reconciliation_failed", lifecycle["blockers"], "Repair or rebuild only the disposable projection, then rerun reconciliation.")
    if int(official.get("official_missing_heads") or 0):
        block("official_historical_coverage_missing", official, "Complete cutoff historical replay before exposing an official backtest dataset.")
    if int(official.get("baseline_claimed_historical_events") or 0):
        block("baseline_claims_historical_first_seen", official, "Keep baseline as baseline_observed and remove it from official historical eligibility.")
    if int(official.get("future_leak_events") or 0):
        block("official_future_leak", official, "Correct replay event ordering and regenerate only affected official evidence.")
    if strict_v2_provenance["decision"] != "PASS":
        failure_code = str(strict_v2_provenance["failure_code"])
        block(
            f"strict_v2_provenance_{failure_code}",
            strict_v2_provenance,
            (
                "Restore complete frozen strict-v2 provenance before reporting this batch."
                if failure_code == "unavailable"
                else "Resolve the pinned audit, universe, catalog, or freshness drift before continuing."
            ),
        )

    canonical_summary = _json_object(canonical.get("summary"))
    canonical_ready = (
        canonical.get("status") == "completed"
        and canonical.get("apply_mode") is False
        and canonical.get("audit_run_id") == pinned_audit_run_id
    )
    if not canonical_ready:
        block("canonical_gate_unavailable", canonical, "Complete the read-only canonical gate and retain its audit evidence.")

    manifest = {
        "generated_at": generated_at,
        "readonly": True,
        "batch": batch,
        "report_code_commit": _local_commit(),
        "strict_v2_provenance": strict_v2_provenance,
    }
    canonical_report = {
        "generated_at": generated_at,
        "readonly": True,
        "audit": canonical,
        "gate_available": canonical_ready,
        "gate_pass": bool(canonical_summary.get("gate_pass", False)),
    }
    coverage = {
        "generated_at": generated_at,
        "batch_id": batch_id,
        "expected_modes": list(MODES),
        "summary": summary["heads"],
        "by_level_mode": head_rows,
    }
    resource_row = {
        "observed_at": generated_at,
        "batch_id": batch_id,
        **db_resource,
        **dict(resource_metrics or {}),
    }
    decision = {
        "generated_at": generated_at,
        "batch_id": batch_id,
        "decision": "GO" if not blockers else "NO_GO",
        "next_phase": "historical_replay_and_official_backtest",
        "blockers": blockers,
    }
    return {
        "run_manifest": manifest,
        "canonical_gate": canonical_report,
        "task_progress": task_rows,
        "recompute_summary": summary,
        "published_head_coverage": coverage,
        "resource_metrics": [resource_row],
        "failure_samples": failure_rows,
        "strict_v2_provenance": strict_v2_provenance,
        "next_phase_decision": decision,
    }


def _markdown(title: str, value: Mapping[str, Any]) -> str:
    return f"# {title}\n\n```json\n{json.dumps(dict(value), ensure_ascii=False, indent=2, sort_keys=True, default=_json_value)}\n```\n"


def write_artifacts(output_dir: Path, report: Mapping[str, Any]) -> None:
    files = {
        "run_manifest.json": _json_text(report["run_manifest"]),
        "kline_canonical_gate.json": _json_text(report["canonical_gate"]),
        "kline_canonical_gate.md": _markdown("K-line Canonical Gate", report["canonical_gate"]),
        "recompute_progress.jsonl": _jsonl(report["task_progress"]),
        "recompute_summary.json": _json_text(report["recompute_summary"]),
        "recompute_summary.md": _markdown("Module C Recompute Summary", report["recompute_summary"]),
        "published_head_coverage.json": _json_text(report["published_head_coverage"]),
        "resource_metrics.jsonl": _jsonl(report["resource_metrics"]),
        "failure_samples.jsonl": _jsonl(report["failure_samples"]),
        "strict_v2_provenance.json": _json_text(report["strict_v2_provenance"]),
        "next_phase_decision.json": _json_text(report["next_phase_decision"]),
    }
    for name, content in files.items():
        _atomic_write(output_dir / name, content)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate read-only Module C execution evidence")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"), required=os.getenv("DATABASE_URL") is None)
    parser.add_argument("--batch-id", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cpu-percent", type=float)
    parser.add_argument("--memory-rss-bytes", type=int)
    parser.add_argument("--disk-free-bytes", type=int)
    return parser.parse_args(argv)


async def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    resources = {
        key: value
        for key, value in {
            "cpu_percent": args.cpu_percent,
            "memory_rss_bytes": args.memory_rss_bytes,
            "disk_free_bytes": args.disk_free_bytes,
        }.items()
        if value is not None
    }
    conn = await asyncpg.connect(args.database_url)
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            report = await build_report(conn, args.batch_id, resource_metrics=resources)
    finally:
        await conn.close()
    write_artifacts(args.output_dir, report)
    print(json.dumps(report["next_phase_decision"], ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
