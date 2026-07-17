from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Mapping

import asyncpg

from app.engine.time_utils import utc_time


class HistoricalLifecycleScopeError(RuntimeError):
    """The requested replay batch is not an immutable official source."""


def validate_stats(stats: Mapping[str, int]) -> None:
    if int(stats.get("total_tasks", 0)) < 1:
        raise HistoricalLifecycleScopeError("Historical lifecycle source batch is empty")
    relationship_blockers = (
        "pending_tasks",
        "running_tasks",
        "failed_tasks",
        "invalid_task_states",
        "missing_run_ids",
        "invalid_task_contracts",
        "invalid_runs",
        "missing_heads",
        "unexpected_heads",
        "mismatched_heads",
        "missing_history",
        "invalid_history",
        "unexpected_history",
        "missing_outbox",
        "blocking_outbox",
        "invalid_outbox_payload",
        "invalid_lifecycle_events",
    )
    for field in relationship_blockers:
        if int(stats.get(field, 0)):
            raise HistoricalLifecycleScopeError(
                f"Historical lifecycle relationship audit failed: {field}"
            )
    for field in ("non_scope_count", "invalid_clock_count", "future_effective_count"):
        if int(stats.get(field, 0)):
            raise HistoricalLifecycleScopeError(f"Historical lifecycle {field} must be zero")
    if int(stats.get("candidate_count", 0)) != int(stats.get("scoped_count", 0)):
        raise HistoricalLifecycleScopeError("Historical lifecycle candidate/scoped counts differ")


def _digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, Mapping):
            return parsed
    raise HistoricalLifecycleScopeError("Replay contract must be a JSON object")


@dataclass(frozen=True)
class HistoricalLifecycleScope:
    replay_batch_id: int
    source_batch_id: int
    contract_version: str
    contract_hash: str
    run_group_id: str
    publication_namespace: str
    profile_id: str
    config_hash: str
    eligible_universe_snapshot_id: str
    canonical_gate_snapshot_id: str
    contract_cutoff: str
    scope_hash: str

    def manifest(self) -> dict[str, Any]:
        return asdict(self)


def build_scope(
    row: Mapping[str, Any], *, expected_contract_hash: str,
) -> HistoricalLifecycleScope:
    expected = str(expected_contract_hash)
    if len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
        raise HistoricalLifecycleScopeError("Expected contract hash must be lowercase SHA-256")
    if str(row.get("parent_status")) != "sealed" or str(row.get("child_status")) != "sealed":
        raise HistoricalLifecycleScopeError("Historical replay parent and child must both be sealed")
    if str(row.get("source_batch_status")) != "sealed":
        raise HistoricalLifecycleScopeError("Historical replay source batch must be sealed")
    if str(row.get("batch_kind")) != "historical_replay":
        raise HistoricalLifecycleScopeError("Parent batch is not historical_replay")
    if row.get("parent_sealed_at") is None or row.get("source_batch_sealed_at") is None:
        raise HistoricalLifecycleScopeError("Parent and source batches must have sealed timestamps")
    actual = str(row.get("contract_hash"))
    if actual != expected:
        raise HistoricalLifecycleScopeError("Replay batch does not match expected contract hash")
    contract = _mapping(row.get("contract"))
    if _digest(contract) != actual:
        raise HistoricalLifecycleScopeError("Stored replay contract digest is invalid")
    contract_cutoff = utc_time(contract.get("cutoff_time")).isoformat()
    expected_contract_fields = {
        "source_batch_id": int(row["source_batch_id"]),
        "contract_version": str(row["contract_version"]),
        "config_hash": str(row["config_hash"]),
        "run_group": "historical_replay",
        "cutoff_policy": str(row["cutoff_policy"]),
        "eligible_universe_snapshot_id": str(row["eligible_universe_snapshot_id"]),
        "canonical_gate_snapshot_id": str(row["canonical_gate_snapshot_id"]),
    }
    if any(contract.get(key) != value for key, value in expected_contract_fields.items()):
        raise HistoricalLifecycleScopeError("Stored replay contract lineage is inconsistent")
    effective_config = _mapping(row.get("effective_config"))
    audit_references = row.get("audit_references")
    if isinstance(audit_references, str):
        audit_references = json.loads(audit_references)
    source_audit = any(
        isinstance(item, Mapping)
        and item.get("type") == "source_batch"
        and int(item.get("batch_id", 0)) == int(row["source_batch_id"])
        for item in (audit_references or [])
    )
    if (
        str(row.get("publication_namespace")) != "historical-replay"
        or str(row.get("profile_id")) != "module-c-historical-replay-v1"
        or effective_config.get("replay_contract_version") != str(row["contract_version"])
        or int(effective_config.get("source_batch_id", 0)) != int(row["source_batch_id"])
        or not source_audit
    ):
        raise HistoricalLifecycleScopeError("Historical replay parent lineage is inconsistent")
    payload = {
        "replay_batch_id": int(row["replay_batch_id"]),
        "source_batch_id": int(row["source_batch_id"]),
        "contract_version": str(row["contract_version"]),
        "contract_hash": actual,
        "run_group_id": str(row["run_group_id"]),
        "publication_namespace": str(row["publication_namespace"]),
        "profile_id": str(row["profile_id"]),
        "config_hash": str(row["config_hash"]),
        "eligible_universe_snapshot_id": str(row["eligible_universe_snapshot_id"]),
        "canonical_gate_snapshot_id": str(row["canonical_gate_snapshot_id"]),
        "contract_cutoff": contract_cutoff,
    }
    return HistoricalLifecycleScope(**payload, scope_hash=_digest(payload))


BATCH_SCOPE_SQL = """
select parent.id as replay_batch_id,
       child.source_batch_id,
       parent.status as parent_status,
       parent.sealed_at as parent_sealed_at,
       parent.batch_kind,
       parent.publication_namespace,
       parent.profile_id,
       parent.run_group_id,
       parent.config_hash,
       parent.effective_config,
       parent.audit_references,
       child.status as child_status,
       child.contract_version,
       child.contract_hash,
       child.contract,
       child.eligible_universe_snapshot_id,
       child.canonical_gate_snapshot_id,
       child.cutoff_policy,
       source.status as source_batch_status,
       source.sealed_at as source_batch_sealed_at
  from chan_c_historical_replay_batches child
  join chan_c_batches parent on parent.id = child.batch_id
  join chan_c_batches source on source.id = child.source_batch_id
 where child.batch_id = $1
   and child.contract_hash = $2
   and parent.batch_kind = 'historical_replay'
   and parent.status = 'sealed'
   and parent.sealed_at is not null
   and child.status = 'sealed'
   and source.status = 'sealed'
   and source.sealed_at is not null
"""


RELATIONSHIP_STATS_SQL = """
with tasks as materialized (
    select id, batch_id, symbol_id, chan_level, mode, cutoff_time,
           contract_version, replay_identity, eligible, status, run_id
      from chan_c_historical_replay_tasks
     where batch_id = $1
), expected_heads as materialized (
    select task.id as task_id, task.batch_id, task.symbol_id,
           task.chan_level, trim(mode_name) as mode, task.cutoff_time,
           task.contract_version, task.replay_identity, task.run_id
      from tasks task
      cross join lateral unnest(string_to_array(task.mode, ',')) as modes(mode_name)
     where task.status = 'completed'
), actual_heads as materialized (
    select head.batch_id, head.task_id, head.symbol_id, head.chan_level,
           head.mode, head.cutoff_time, head.contract_version,
           head.replay_identity, head.run_id, head.config_hash
      from chan_c_historical_replay_heads head
     where head.batch_id = $1
), ordered_heads as materialized (
    select actual.*,
           lag(actual.run_id) over (
               partition by actual.symbol_id, actual.chan_level, actual.mode
               order by actual.cutoff_time
           ) as expected_old_run_id,
           lag(actual.cutoff_time) over (
               partition by actual.symbol_id, actual.chan_level, actual.mode
               order by actual.cutoff_time
           ) as expected_old_cutoff_time
      from actual_heads actual
), history_evidence as materialized (
    select actual.*, history.id as history_id,
           history.base_timeframe as history_base_timeframe,
           history.config_hash as history_config_hash,
           history.publication_profile, history.run_group_id,
           history.old_run_id as history_old_run_id,
           history.old_base_to_bar_end as history_old_cutoff_time,
           history.new_base_to_bar_end, history.snapshot_version,
           history.source, history.published_at as history_published_at,
           history.provenance as history_provenance,
           outbox.id as outbox_id, outbox.status as outbox_status,
           outbox.processed_at, outbox.lease_until, outbox.payload
      from ordered_heads actual
      left join chan_c_head_history history
        on history.symbol_id = actual.symbol_id
       and history.chan_level = actual.chan_level
       and history.mode = actual.mode
       and history.new_run_id = actual.run_id
      left join chan_c_head_outbox outbox on outbox.head_history_id = history.id
)
select count(*)::bigint as total_tasks,
       count(*) filter (where eligible and status = 'completed')::bigint
           as completed_tasks,
       count(*) filter (where not eligible and status = 'excluded')::bigint
           as excluded_tasks,
       count(*) filter (where status = 'pending')::bigint as pending_tasks,
       count(*) filter (where status = 'running')::bigint as running_tasks,
       count(*) filter (where status = 'failed')::bigint as failed_tasks,
       count(*) filter (
           where (eligible and status <> 'completed')
              or (not eligible and status <> 'excluded')
       )::bigint as invalid_task_states,
       count(*) filter (where status = 'completed' and run_id is null)::bigint
           as missing_run_ids,
       count(*) filter (where contract_version is distinct from $6)::bigint
           as invalid_task_contracts,
       (
           select count(*)
             from tasks task
             left join chan_c_runs run
               on run.id = task.run_id
              and run.batch_id = task.batch_id
              and run.symbol_id = task.symbol_id
              and run.chan_level = task.chan_level
              and run.base_timeframe = task.chan_level
              and run.status = 'success'
              and run.bar_until = task.cutoff_time
              and run.cutoff_bar_end = task.cutoff_time
              and run.config_hash = $2
              and run.run_kind = 'historical_replay'
              and run.run_group_id = $3
              and run.publication_namespace = $4
              and run.profile_id = $5
              and run.run_identity = task.replay_identity
              and run.provenance->>'source' = 'historical_replay'
              and run.provenance->>'profile' = $5
            where task.status = 'completed' and run.id is null
       )::bigint as invalid_runs,
       (
           select count(*)
             from expected_heads expected
             left join actual_heads actual
               on actual.task_id = expected.task_id and actual.mode = expected.mode
            where actual.task_id is null
       )::bigint as missing_heads,
       (
           select count(*)
             from actual_heads actual
             left join expected_heads expected
               on expected.task_id = actual.task_id and expected.mode = actual.mode
            where expected.task_id is null
       )::bigint as unexpected_heads,
       (
           select count(*)
             from expected_heads expected
             join actual_heads actual
               on actual.task_id = expected.task_id and actual.mode = expected.mode
            where actual.batch_id <> expected.batch_id
               or actual.symbol_id <> expected.symbol_id
               or actual.chan_level <> expected.chan_level
               or actual.cutoff_time <> expected.cutoff_time
               or actual.contract_version <> expected.contract_version
               or actual.replay_identity <> expected.replay_identity
               or actual.run_id is distinct from expected.run_id
               or actual.config_hash <> $2
       )::bigint as mismatched_heads,
       (
           select count(*) from history_evidence where history_id is null
       )::bigint as missing_history,
       (
           select count(*)
             from history_evidence
            where history_id is not null
              and (history_base_timeframe <> chan_level
               or history_config_hash <> $2
               or publication_profile <> 'historical_replay'
               or run_group_id <> $3
               or history_old_run_id is distinct from expected_old_run_id
               or history_old_cutoff_time is distinct from expected_old_cutoff_time
               or new_base_to_bar_end <> cutoff_time
               or coalesce(snapshot_version, '') = ''
               or source <> 'historical_replay'
               or history_provenance->>'publication_profile'
                  is distinct from 'historical_replay'
               or history_provenance->>'run_group_id' is distinct from $3)
       )::bigint as invalid_history,
       (
           select count(*)
             from chan_c_head_history history
             join chan_c_runs run on run.id = history.new_run_id
             left join actual_heads actual
               on actual.symbol_id = history.symbol_id
              and actual.chan_level = history.chan_level
              and actual.mode = history.mode
              and actual.run_id = history.new_run_id
            where run.batch_id = $1
              and history.publication_profile = 'historical_replay'
              and actual.run_id is null
       )::bigint as unexpected_history,
       (
           select count(*) from history_evidence
            where history_id is not null and outbox_id is null
       )::bigint as missing_outbox,
       (
           select count(*) from history_evidence
            where outbox_id is not null
              and (outbox_status <> 'completed' or processed_at is null
                   or lease_until is not null)
       )::bigint as blocking_outbox,
       (
           select count(*) from history_evidence
            where outbox_id is not null
              and ((payload->>'id')::bigint is distinct from history_id
               or (payload->>'symbol_id')::integer is distinct from symbol_id
               or (payload->>'chan_level')::integer is distinct from chan_level
               or payload->>'mode' is distinct from mode
               or (payload->>'base_timeframe')::integer
                  is distinct from history_base_timeframe
               or payload->>'config_hash' is distinct from history_config_hash
               or payload->>'publication_profile' is distinct from publication_profile
               or payload->>'run_group_id' is distinct from run_group_id
               or (payload->>'old_run_id')::bigint is distinct from history_old_run_id
               or (payload->>'new_run_id')::bigint is distinct from run_id
               or (payload->>'old_base_to_bar_end')::timestamptz
                  is distinct from history_old_cutoff_time
               or (payload->>'new_base_to_bar_end')::timestamptz
                  is distinct from new_base_to_bar_end
               or (payload->>'published_at')::timestamptz
                  is distinct from history_published_at
               or payload->>'snapshot_version' is distinct from snapshot_version)
       )::bigint as invalid_outbox_payload,
       (
           select count(*)
             from history_evidence evidence
             join chan_structure_lifecycle_events event
               on event.head_history_id = evidence.history_id
             join chan_structure_identity identity
               on identity.fingerprint = event.fingerprint
            where event.run_id is distinct from evidence.run_id
               or event.effective_time <> evidence.cutoff_time
               or event.observed_time is distinct from evidence.history_published_at
               or event.effective_time > event.observed_time
               or event.point_time > event.effective_time
               or event.point_time <> identity.point_time
               or identity.symbol_id <> evidence.symbol_id
               or identity.chan_level <> evidence.chan_level
               or identity.config_hash <> $2
               or event.provenance->>'publication_profile'
                  is distinct from 'historical_replay'
               or event.provenance->>'run_group_id' is distinct from $3
               or (event.provenance->>'new_run_id')::bigint
                  is distinct from evidence.run_id
               or event.provenance->>'mode' is distinct from evidence.mode
               or (event.provenance->>'chan_level')::integer
                  is distinct from evidence.chan_level
               or (event.provenance->>'symbol_id')::integer
                  is distinct from evidence.symbol_id
       )::bigint as invalid_lifecycle_events
  from tasks
"""


STATS_SQL = """
with exact_heads as materialized (
    select head.batch_id, head.task_id, head.symbol_id, head.chan_level,
           head.mode, head.cutoff_time, head.run_id, head.config_hash,
           parent.run_group_id, parent.publication_namespace, parent.profile_id
      from chan_c_historical_replay_heads head
      join chan_c_historical_replay_tasks task
        on task.id = head.task_id
       and task.batch_id = head.batch_id
       and task.run_id = head.run_id
       and task.symbol_id = head.symbol_id
       and task.chan_level = head.chan_level
       and task.cutoff_time = head.cutoff_time
       and task.contract_version = head.contract_version
       and task.replay_identity = head.replay_identity
      join chan_c_historical_replay_batches child on child.batch_id = head.batch_id
      join chan_c_batches parent on parent.id = child.batch_id
      join chan_c_runs replay_run
        on replay_run.id = head.run_id
       and replay_run.batch_id = head.batch_id
       and replay_run.symbol_id = head.symbol_id
       and replay_run.chan_level = head.chan_level
       and replay_run.cutoff_bar_end = head.cutoff_time
       and replay_run.config_hash = head.config_hash
       and replay_run.run_group_id = parent.run_group_id
       and replay_run.status = 'success'
       and replay_run.run_kind = 'historical_replay'
     where head.batch_id = $1
       and task.eligible
       and task.status = 'completed'
       and child.status = 'sealed'
       and parent.status = 'sealed'
       and parent.batch_kind = 'historical_replay'
       and head.config_hash = parent.config_hash
), audited_events as not materialized (
    select event.effective_time, event.observed_time,
           (
               head.run_id is not null
               and event.run_id = head.run_id
               and history.symbol_id = head.symbol_id
               and history.chan_level = head.chan_level
               and history.mode = head.mode
               and history.new_base_to_bar_end = head.cutoff_time
               and history.config_hash = head.config_hash
               and history.run_group_id = head.run_group_id
               and history.source = 'historical_replay'
               and history.published_at = event.observed_time
               and history.provenance->>'publication_profile' = 'historical_replay'
               and history.provenance->>'run_group_id' = head.run_group_id
               and run.status = 'success'
               and run.run_kind = 'historical_replay'
               and run.run_group_id = head.run_group_id
               and run.publication_namespace = head.publication_namespace
               and run.profile_id = head.profile_id
               and run.config_hash = head.config_hash
               and identity.symbol_id = head.symbol_id
               and identity.chan_level = head.chan_level
               and identity.config_hash = head.config_hash
               and outbox.status = 'completed'
               and outbox.processed_at is not null
               and outbox.lease_until is null
               and event.effective_time = head.cutoff_time
               and event.provenance->>'publication_profile' = 'historical_replay'
               and event.provenance->>'run_group_id' = head.run_group_id
               and (event.provenance->>'new_run_id')::bigint = head.run_id
               and event.provenance->>'mode' = head.mode
               and (event.provenance->>'symbol_id')::integer = head.symbol_id
               and (event.provenance->>'chan_level')::integer = head.chan_level
           ) as is_scoped
      from chan_structure_lifecycle_events event
      join chan_c_head_history history on history.id = event.head_history_id
      join chan_c_runs run on run.id = history.new_run_id
      join chan_structure_identity identity on identity.fingerprint = event.fingerprint
      left join exact_heads head
        on head.run_id = history.new_run_id
       and head.symbol_id = history.symbol_id
       and head.chan_level = history.chan_level
       and head.mode = history.mode
      left join chan_c_head_outbox outbox on outbox.head_history_id = history.id
     where run.batch_id = $1
       and history.publication_profile = 'historical_replay'
)
select count(*)::bigint as candidate_count,
       count(*) filter (where event.is_scoped)::bigint as scoped_count,
       count(*) filter (
           where event.is_scoped and event.effective_time <= $2::timestamptz
       )::bigint as row_count,
       count(*) filter (
           where event.is_scoped and event.effective_time > $2::timestamptz
       )::bigint as future_effective_count,
       count(*) filter (
           where event.is_scoped and event.effective_time > event.observed_time
       )::bigint as invalid_clock_count,
       count(*) filter (where not event.is_scoped)::bigint as non_scope_count,
       count(*) filter (
           where event.is_scoped
             and event.effective_time <= $2::timestamptz
             and event.observed_time > $2::timestamptz
       )::bigint as observed_after_cutoff_count
  from audited_events event
"""


EVENTS_SQL = """
select event.id, event.fingerprint, event.event_type, event.effective_time,
       event.observed_time, event.point_time, event.previous_mode,
       event.current_mode, event.run_id, event.provenance,
       history.symbol_id, history.chan_level, history.mode as head_mode,
       history.publication_profile, history.snapshot_version, history.published_at,
       identity.structure_type, identity.side_or_direction, identity.bsp_type,
       identity.price_x1000
  from chan_structure_lifecycle_events event
  join chan_c_head_history history on history.id = event.head_history_id
  join chan_c_runs run on run.id = history.new_run_id
  join chan_structure_identity identity on identity.fingerprint = event.fingerprint
 where run.batch_id = $1
   and history.publication_profile = 'historical_replay'
   and event.effective_time <= $2::timestamptz
 order by event.effective_time, event.observed_time, event.id
"""


GATE_COUNTS_SQL = """
with gate_events as materialized (
    select event.id, event.fingerprint, event.event_type, event.effective_time,
           event.current_mode, history.symbol_id, history.chan_level,
           identity.structure_type, identity.side_or_direction, identity.bsp_type,
           identity.point_time, identity.price_x1000
      from chan_structure_lifecycle_events event
      join chan_c_head_history history on history.id = event.head_history_id
      join chan_c_runs run on run.id = history.new_run_id
      join chan_structure_identity identity on identity.fingerprint = event.fingerprint
     where run.batch_id = $1
       and history.publication_profile = 'historical_replay'
       and event.effective_time <= $2::timestamptz
), source as materialized (
    select full_batch.eligibility_build_id
      from chan_c_full_recompute_batches full_batch
      join module_c_eligibility_builds build
        on build.build_id = full_batch.eligibility_build_id
     where full_batch.batch_id = $3
       and full_batch.eligibility_build_id::text = $4
       and build.manifest_hash = $5
       and build.config_hash = $6
), high_eligible as materialized (
    select eligibility.symbol_id
      from module_c_eligibility eligibility, source
     where eligibility.build_id = source.eligibility_build_id
       and eligibility.timeframe in (1440, 10080, 43200)
       and eligibility.eligible
     group by eligibility.symbol_id
    having count(distinct eligibility.timeframe) = 3
), intraday_eligible as materialized (
    select eligibility.symbol_id
      from module_c_eligibility eligibility, source
     where eligibility.build_id = source.eligibility_build_id
       and eligibility.timeframe in (5, 30)
       and eligibility.eligible
     group by eligibility.symbol_id
    having count(distinct eligibility.timeframe) = 2
), signals as materialized (
    select event.symbol_id, event.chan_level, event.bsp_type,
           event.side_or_direction, event.point_time,
           event.price_x1000, event.effective_time, event.current_mode
      from gate_events event
      join intraday_eligible eligible on eligible.symbol_id = event.symbol_id
     where event.event_type = 'first_seen'
       and event.structure_type = 'signal'
), strict_daily as materialized (
    select distinct daily.symbol_id, daily.effective_time
      from signals daily
     where daily.chan_level = 1440
       and daily.current_mode = 'predictive'
       and daily.side_or_direction = 'buy'
       and daily.bsp_type in ('2', '2s')
       and exists (
           select 1 from signals daily_b1
            where daily_b1.symbol_id = daily.symbol_id
              and daily_b1.chan_level = 1440
              and daily_b1.current_mode = 'predictive'
              and daily_b1.side_or_direction = 'buy'
              and daily_b1.bsp_type = '1'
              and daily_b1.point_time < daily.point_time
              and daily_b1.price_x1000 < daily.price_x1000
              and daily_b1.effective_time <= daily.effective_time
       )
       and exists (
           select 1 from signals weekly_b2
            where weekly_b2.symbol_id = daily.symbol_id
              and weekly_b2.chan_level = 10080
              and weekly_b2.current_mode = 'predictive'
              and weekly_b2.side_or_direction = 'buy'
              and weekly_b2.bsp_type = '2'
              and weekly_b2.effective_time <= daily.effective_time
              and exists (
                  select 1 from signals weekly_b1
                   where weekly_b1.symbol_id = weekly_b2.symbol_id
                     and weekly_b1.chan_level = 10080
                     and weekly_b1.current_mode = 'predictive'
                     and weekly_b1.side_or_direction = 'buy'
                     and weekly_b1.bsp_type = '1'
                     and weekly_b1.point_time < weekly_b2.point_time
                     and weekly_b1.price_x1000 < weekly_b2.price_x1000
                     and weekly_b1.effective_time <= weekly_b2.effective_time
              )
       )
)
select (select count(*) from source)::bigint as source_scope_rows,
       (select count(*) from high_eligible)::bigint as source_high_level_eligible,
       (select count(distinct event.symbol_id)
          from gate_events event
          join high_eligible eligible on eligible.symbol_id = event.symbol_id
         where event.chan_level in (1440, 10080, 43200))::bigint
           as official_high_level_visible,
       (select count(*) from intraday_eligible)::bigint as intraday_eligible,
       (select count(distinct signal.symbol_id) from signals signal
         where signal.chan_level = 10080 and signal.current_mode = 'predictive'
           and signal.side_or_direction = 'buy' and signal.bsp_type = '1')::bigint
           as predictive_weekly_b1,
       (select count(distinct signal.symbol_id) from signals signal
         where signal.chan_level = 10080 and signal.current_mode = 'predictive'
           and signal.side_or_direction = 'buy' and signal.bsp_type = '2')::bigint
           as predictive_weekly_b2,
       (select count(*) from strict_daily)::bigint as strict_daily_episodes,
       (select coalesce(jsonb_agg(level_row order by level_row.chan_level), '[]'::jsonb)
          from (
              select event.chan_level, count(*)::bigint as event_count,
                     0::bigint as invalid_time_count
                from gate_events event
               group by event.chan_level
          ) level_row) as official_events_by_level
"""


GATE_FAILURES_SQL = """
with source as materialized (
    select full_batch.eligibility_build_id
      from chan_c_full_recompute_batches full_batch
      join module_c_eligibility_builds build
        on build.build_id = full_batch.eligibility_build_id
     where full_batch.batch_id = $1
       and full_batch.eligibility_build_id::text = $2
       and build.manifest_hash = $3
       and build.config_hash = $4
), eligible as (
    select eligibility.symbol
      from module_c_eligibility eligibility, source
     where eligibility.build_id = source.eligibility_build_id
       and eligibility.timeframe in (5, 30)
       and eligibility.eligible
     group by eligibility.symbol
    having count(distinct eligibility.timeframe) = 2
)
select symbol from eligible order by symbol limit 20
"""


@dataclass
class HistoricalLifecycleSnapshot:
    connection: Any
    scope: HistoricalLifecycleScope
    stats: dict[str, int]
    replay_batch_id: int
    effective_cutoff: datetime

    def events(self, *, prefetch: int = 1000) -> Any:
        return self.connection.cursor(
            EVENTS_SQL,
            self.replay_batch_id,
            self.effective_cutoff,
            prefetch=prefetch,
        )

    async def gate_inputs(self) -> tuple[dict[str, int], list[dict[str, Any]], list[str]]:
        counts_row = await self.connection.fetchrow(
            GATE_COUNTS_SQL,
            self.replay_batch_id,
            self.effective_cutoff,
            self.scope.source_batch_id,
            self.scope.eligible_universe_snapshot_id,
            self.scope.canonical_gate_snapshot_id,
            self.scope.config_hash,
        )
        failures = await self.connection.fetch(
            GATE_FAILURES_SQL,
            self.scope.source_batch_id,
            self.scope.eligible_universe_snapshot_id,
            self.scope.canonical_gate_snapshot_id,
            self.scope.config_hash,
        )
        raw_counts = dict(counts_row or {})
        raw_levels = raw_counts.pop("official_events_by_level", [])
        if int(raw_counts.pop("source_scope_rows", 0)) != 1:
            raise HistoricalLifecycleScopeError(
                "Pinned source eligibility build or manifest is unavailable"
            )
        if isinstance(raw_levels, str):
            raw_levels = json.loads(raw_levels)
        levels = [dict(row) for row in raw_levels]
        counts = {key: int(value or 0) for key, value in raw_counts.items()}
        counts.update(
            {
                "official_30f_confirmations": 0,
                "official_5f_confirmations": 0,
                "official_candidates": 0,
            }
        )
        return counts, levels, [str(row["symbol"]) for row in failures]


class HistoricalLifecycleRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    @asynccontextmanager
    async def open_snapshot(
        self,
        *,
        replay_batch_id: int,
        expected_contract_hash: str,
        effective_cutoff: datetime,
    ) -> AsyncIterator[HistoricalLifecycleSnapshot]:
        if replay_batch_id < 1:
            raise HistoricalLifecycleScopeError("Replay batch id must be positive")
        cutoff = utc_time(effective_cutoff)
        async with self.pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                row = await conn.fetchrow(
                    BATCH_SCOPE_SQL,
                    replay_batch_id,
                    expected_contract_hash,
                )
                if row is None:
                    raise HistoricalLifecycleScopeError(
                        "Replay batch is missing, unsealed, or has the wrong contract"
                    )
                scope = build_scope(row, expected_contract_hash=expected_contract_hash)
                if scope.contract_cutoff != cutoff.isoformat():
                    raise HistoricalLifecycleScopeError(
                        "Effective cutoff must equal the sealed replay contract cutoff"
                    )
                relationship_row = await conn.fetchrow(
                    RELATIONSHIP_STATS_SQL,
                    replay_batch_id,
                    scope.config_hash,
                    scope.run_group_id,
                    scope.publication_namespace,
                    scope.profile_id,
                    scope.contract_version,
                )
                stats_row = await conn.fetchrow(STATS_SQL, replay_batch_id, cutoff)
                stats = {
                    key: int(value or 0)
                    for row in (relationship_row, stats_row)
                    for key, value in dict(row or {}).items()
                }
                validate_stats(stats)
                yield HistoricalLifecycleSnapshot(
                    connection=conn,
                    scope=scope,
                    stats=stats,
                    replay_batch_id=replay_batch_id,
                    effective_cutoff=cutoff,
                )
