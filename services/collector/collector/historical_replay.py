from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from collector.lifecycle_reconciliation import build_reconciliation


CN_TZ = ZoneInfo("Asia/Shanghai")
REPLAY_CONTRACT_VERSION = "historical-replay-v1"
REPLAY_RUN_GROUP = "historical_replay"
REPLAY_PROVENANCE = "historical_replay"
NATIVE_LEVELS = ("5f", "30f", "1d", "1w", "1m")
EXECUTABLE_PARENT_BATCH_STATUSES = frozenset({"planned", "running"})
EXECUTABLE_REPLAY_BATCH_STATUSES = frozenset({"planned", "running"})
SEALABLE_PARENT_BATCH_STATUSES = frozenset({"planned", "running", "sealed"})
SEALABLE_REPLAY_BATCH_STATUSES = frozenset({"planned", "running", "completed", "sealed"})


class InactiveReplayBatchError(RuntimeError):
    """The durable replay batch is no longer allowed to execute."""


class ReplayBatchNotFinalizableError(RuntimeError):
    """The durable replay evidence does not permit a success seal."""


async def lock_executable_replay_batch(
    conn: Any,
    *,
    batch_id: int,
    contract: ReplayContract | None = None,
    allowed_child_statuses: frozenset[str] = EXECUTABLE_REPLAY_BATCH_STATUSES,
) -> tuple[Any, Any]:
    """Lock parent then child so terminal transitions fence all replay writers."""
    parent = await conn.fetchrow(
        """
        select id, status, batch_kind, publication_namespace, profile_id,
               run_group_id, config_hash, effective_config, audit_references
          from chan_c_batches
         where id = $1
         for share
        """,
        batch_id,
    )
    if parent is None:
        raise InactiveReplayBatchError(f"Unknown parent batch: {batch_id}")
    if str(parent["batch_kind"]) != "historical_replay":
        raise InactiveReplayBatchError(f"Parent batch {batch_id} is not a historical replay batch")
    if str(parent["status"]) not in EXECUTABLE_PARENT_BATCH_STATUSES:
        raise InactiveReplayBatchError(
            f"Parent batch {batch_id} is not executable: {parent['status']}"
        )
    child = await conn.fetchrow(
        """
        select batch_id, status, source_batch_id, contract_version, contract_hash,
               contract, eligible_universe_snapshot_id, canonical_gate_snapshot_id,
               cutoff_policy
          from chan_c_historical_replay_batches
         where batch_id = $1
         for share
        """,
        batch_id,
    )
    if child is None:
        raise InactiveReplayBatchError(f"Unknown replay child batch: {batch_id}")
    if str(child["status"]) not in allowed_child_statuses:
        raise InactiveReplayBatchError(
            f"Replay batch {batch_id} is not executable: {child['status']}"
        )
    payload = child["contract"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, Mapping) or _digest(payload) != str(child["contract_hash"]):
        raise InactiveReplayBatchError(f"Replay batch {batch_id} contract digest mismatch")
    if contract is not None:
        expected = {
            "source_batch_id": contract.source_batch_id,
            "contract_version": contract.contract_version,
            "contract_hash": contract.digest(),
            "eligible_universe_snapshot_id": contract.eligible_universe_snapshot_id,
            "canonical_gate_snapshot_id": contract.canonical_gate_snapshot_id,
            "cutoff_policy": contract.cutoff_policy,
        }
        actual = {key: child[key] for key in expected}
        if actual != expected or dict(payload) != contract.payload():
            raise InactiveReplayBatchError(f"Replay batch {batch_id} contract mismatch")
        effective_config = parent["effective_config"]
        audit_references = parent["audit_references"]
        if isinstance(effective_config, str):
            effective_config = json.loads(effective_config)
        if isinstance(audit_references, str):
            audit_references = json.loads(audit_references)
        source_audit = any(
            isinstance(item, Mapping)
            and item.get("type") == "source_batch"
            and int(item.get("batch_id", 0)) == contract.source_batch_id
            for item in (audit_references or [])
        )
        if (
            str(parent["publication_namespace"]) != "historical-replay"
            or str(parent["profile_id"]) != "module-c-historical-replay-v1"
            or str(parent["config_hash"]) != contract.config_hash
            or not isinstance(effective_config, Mapping)
            or effective_config.get("replay_contract_version") != contract.contract_version
            or int(effective_config.get("source_batch_id", 0)) != contract.source_batch_id
            or not source_audit
        ):
            raise InactiveReplayBatchError(f"Replay parent batch {batch_id} lineage mismatch")
    return parent, child


def utc_datetime(value: datetime | str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Naive datetime is forbidden by the historical replay contract")
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class ReplayContract:
    config_hash: str
    source_batch_id: int
    eligible_universe_snapshot_id: str
    canonical_gate_snapshot_id: str
    cutoff_time: datetime
    cutoff_policy: str = "native_closed_bars_strategy_forward_windows_v1"
    contract_version: str = REPLAY_CONTRACT_VERSION
    run_group: str = REPLAY_RUN_GROUP
    provenance: str = REPLAY_PROVENANCE
    timezone: str = "UTC"

    def __post_init__(self) -> None:
        object.__setattr__(self, "cutoff_time", utc_datetime(self.cutoff_time))
        if self.source_batch_id < 1:
            raise ValueError("source_batch_id must identify a sealed source batch")
        for name in ("config_hash", "eligible_universe_snapshot_id", "canonical_gate_snapshot_id"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        if self.run_group != REPLAY_RUN_GROUP or self.provenance != REPLAY_PROVENANCE:
            raise ValueError("Historical replay provenance cannot be relabeled")
        if self.timezone != "UTC":
            raise ValueError("Historical replay contract timezone must be UTC")

    def payload(self) -> dict[str, Any]:
        result = asdict(self)
        result["cutoff_time"] = self.cutoff_time.isoformat()
        return result

    def digest(self) -> str:
        return _digest(self.payload())


def stable_replay_identity(
    contract: ReplayContract,
    *,
    symbol: str,
    level: str,
    mode: str,
    cutoff_time: datetime | str,
) -> str:
    if level not in NATIVE_LEVELS:
        raise ValueError(f"Unsupported native replay level: {level}")
    cutoff = utc_datetime(cutoff_time)
    return _digest(
        {
            "contract_hash": contract.digest(),
            "contract_version": contract.contract_version,
            "symbol": symbol.upper(),
            "level": level,
            "mode": mode,
            "cutoff_time": cutoff.isoformat(),
        }
    )


def build_initial_cutoff_grid(
    bars: Iterable[Mapping[str, Any]],
    *,
    as_of_time: datetime | str,
) -> list[dict[str, str]]:
    """Build the causal weekly/daily grid before strategy windows expand intraday levels."""
    as_of = utc_datetime(as_of_time)
    result: dict[tuple[str, datetime], dict[str, str]] = {}
    for bar in bars:
        level = str(bar.get("level") or bar.get("timeframe") or "")
        if level not in {"1d", "1w", "1m"} or not bool(bar.get("complete", bar.get("is_complete", False))):
            continue
        cutoff = utc_datetime(bar["ts"])
        if cutoff > as_of or not _closed_period(level, cutoff=cutoff, as_of=as_of):
            continue
        result[(level, cutoff)] = {"level": level, "cutoff_time": cutoff.isoformat()}
    return sorted(result.values(), key=lambda row: (row["cutoff_time"], NATIVE_LEVELS.index(row["level"])))


def build_intraday_cutoff_grid(
    bars: Iterable[Mapping[str, Any]],
    *,
    windows: Iterable[Mapping[str, Any]],
) -> list[dict[str, str]]:
    """Expand only pre-declared forward strategy windows using completed native bars."""
    normalized_windows = [
        (str(item["window_id"]), utc_datetime(item["start_time"]), utc_datetime(item["end_time"]))
        for item in windows
    ]
    result: dict[tuple[str, str, datetime], dict[str, str]] = {}
    for bar in bars:
        level = str(bar.get("level") or bar.get("timeframe") or "")
        if level not in {"5f", "30f"} or not bool(bar.get("complete", bar.get("is_complete", False))):
            continue
        cutoff = utc_datetime(bar["ts"])
        for window_id, start, end in normalized_windows:
            if start <= cutoff <= end:
                result[(window_id, level, cutoff)] = {
                    "window_id": window_id,
                    "level": level,
                    "cutoff_time": cutoff.isoformat(),
                }
    return sorted(result.values(), key=lambda row: (row["window_id"], row["cutoff_time"], row["level"]))


def visible_bars_at_cutoff(
    bars: Iterable[Mapping[str, Any]], *, cutoff_time: datetime | str
) -> list[Mapping[str, Any]]:
    cutoff = utc_datetime(cutoff_time)
    return [
        bar
        for bar in bars
        if bool(bar.get("complete", bar.get("is_complete", False))) and utc_datetime(bar["ts"]) <= cutoff
    ]


async def ensure_replay_batch(*, kline_writer: Any, batch_id: int, contract: ReplayContract) -> None:
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        async with conn.transaction():
            parent = await conn.fetchrow(
                """
                select status, batch_kind, publication_namespace, profile_id,
                       config_hash, effective_config, audit_references
                  from chan_c_batches
                 where id = $1
                 for update
                """,
                batch_id,
            )
            if parent is None:
                raise InactiveReplayBatchError(f"Unknown parent batch: {batch_id}")
            parent_status = str(parent["status"])
            if str(parent["batch_kind"]) != "historical_replay":
                raise InactiveReplayBatchError(
                    f"Parent batch {batch_id} is not a historical replay batch"
                )
            if parent_status not in EXECUTABLE_PARENT_BATCH_STATUSES:
                raise InactiveReplayBatchError(
                    f"Parent batch {batch_id} is not executable: {parent_status}"
                )
            effective_config = parent["effective_config"]
            audit_references = parent["audit_references"]
            if isinstance(effective_config, str):
                effective_config = json.loads(effective_config)
            if isinstance(audit_references, str):
                audit_references = json.loads(audit_references)
            source_audit = any(
                isinstance(item, Mapping)
                and item.get("type") == "source_batch"
                and int(item.get("batch_id", 0)) == contract.source_batch_id
                for item in (audit_references or [])
            )
            if (
                str(parent["publication_namespace"]) != "historical-replay"
                or str(parent["profile_id"]) != "module-c-historical-replay-v1"
                or str(parent["config_hash"]) != contract.config_hash
                or not isinstance(effective_config, Mapping)
                or effective_config.get("replay_contract_version") != contract.contract_version
                or int(effective_config.get("source_batch_id", 0)) != contract.source_batch_id
                or not source_audit
            ):
                raise InactiveReplayBatchError(f"Replay parent batch {batch_id} lineage mismatch")

            batch = await conn.fetchrow(
                """
                select source_batch_id, contract_version, contract_hash,
                       eligible_universe_snapshot_id, canonical_gate_snapshot_id,
                       cutoff_policy, status, contract
                  from chan_c_historical_replay_batches
                 where batch_id = $1
                 for update
                """,
                batch_id,
            )
            if batch is not None and str(batch["status"]) not in EXECUTABLE_REPLAY_BATCH_STATUSES:
                raise InactiveReplayBatchError(
                    f"Replay batch {batch_id} is not executable: {batch['status']}"
                )

            await conn.execute(
                """
                insert into chan_c_historical_replay_batches (
                    batch_id, source_batch_id, contract_version, contract_hash, contract,
                    eligible_universe_snapshot_id, canonical_gate_snapshot_id, cutoff_policy
                ) values ($1, $2, $3, $4, $5::jsonb, $6, $7, $8)
                on conflict (batch_id) do nothing
                """,
                batch_id,
                contract.source_batch_id,
                contract.contract_version,
                contract.digest(),
                json.dumps(contract.payload(), sort_keys=True),
                contract.eligible_universe_snapshot_id,
                contract.canonical_gate_snapshot_id,
                contract.cutoff_policy,
            )
            if batch is None:
                batch = await conn.fetchrow(
                    """
                    select source_batch_id, contract_version, contract_hash,
                           eligible_universe_snapshot_id, canonical_gate_snapshot_id,
                           cutoff_policy, status, contract
                      from chan_c_historical_replay_batches
                     where batch_id = $1
                     for update
                    """,
                    batch_id,
                )
            expected = {
                "source_batch_id": contract.source_batch_id,
                "contract_version": contract.contract_version,
                "contract_hash": contract.digest(),
                "eligible_universe_snapshot_id": contract.eligible_universe_snapshot_id,
                "canonical_gate_snapshot_id": contract.canonical_gate_snapshot_id,
                "cutoff_policy": contract.cutoff_policy,
            }
            actual = {key: batch[key] for key in expected} if batch else None
            if actual != expected:
                raise RuntimeError(f"Replay batch {batch_id} contract mismatch: {actual!r}")
            payload = batch["contract"] if batch else None
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, Mapping) or dict(payload) != contract.payload():
                raise RuntimeError(f"Replay batch {batch_id} contract payload mismatch")


async def claim_replay_task(
    *,
    kline_writer: Any,
    batch_id: int,
    worker_id: str,
    lease_seconds: int = 900,
    max_attempts: int = 3,
    shard_index: int | None = None,
    shard_count: int | None = None,
) -> Mapping[str, Any] | None:
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        async with conn.transaction():
            await lock_executable_replay_batch(conn, batch_id=batch_id)
            row = await conn.fetchrow(
                """
            with candidate as (
                select task.id
                  from chan_c_historical_replay_tasks task
                 where task.batch_id = $1 and task.eligible and task.attempts < $4
                   and ($5::integer is null or mod(task.symbol_id, $5) = $6)
                   and (task.status in ('pending', 'failed')
                        or (task.status = 'running' and task.lease_until <= now()))
                 order by task.symbol_id, task.chan_level, task.cutoff_time, task.id
                 for update of task skip locked
                 limit 1
            )
            update chan_c_historical_replay_tasks task
               set status = 'running', worker_id = $2,
                   claim_token = md5(task.id::text || ':' || (task.lease_version + 1)::text || ':' ||
                                     clock_timestamp()::text || ':' || random()::text),
                   lease_version = task.lease_version + 1,
                   lease_until = now() + ($3::integer * interval '1 second'),
                   lease_heartbeat_at = now(), attempts = task.attempts + 1,
                   started_at = coalesce(task.started_at, now()), updated_at = now()
              from candidate
             where task.id = candidate.id
            returning task.*
            """,
                batch_id,
                worker_id,
                lease_seconds,
                max_attempts,
                shard_count,
                shard_index,
            )
            if row is not None:
                await conn.execute(
                    """
                update chan_c_historical_replay_batches
                   set status = 'running', started_at = coalesce(started_at, now()), updated_at = now()
                 where batch_id = $1 and status = 'planned'
                """,
                    batch_id,
                )
            return row


async def heartbeat_replay_task(
    *, kline_writer: Any, task: Mapping[str, Any], lease_seconds: int = 900
) -> bool:
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        async with conn.transaction():
            await lock_executable_replay_batch(conn, batch_id=int(task["batch_id"]))
            result = await conn.execute(
                """
            update chan_c_historical_replay_tasks task
               set lease_until = now() + ($4::integer * interval '1 second'),
                   lease_heartbeat_at = now(), updated_at = now()
             where task.id = $1 and task.status = 'running' and task.claim_token = $2
               and task.lease_version = $3 and task.lease_until > now()
            """,
                task["id"],
                task["claim_token"],
                task["lease_version"],
                lease_seconds,
            )
            return result.endswith(" 1")


async def fail_replay_task(
    *, kline_writer: Any, task: Mapping[str, Any], error: BaseException
) -> bool:
    assert kline_writer._pool is not None
    failure = {
        "error_type": type(error).__name__,
        "message": str(error)[:2000],
        "failed_at": datetime.now(UTC).isoformat(),
    }
    async with kline_writer._pool.acquire() as conn:
        async with conn.transaction():
            await lock_executable_replay_batch(conn, batch_id=int(task["batch_id"]))
            result = await conn.execute(
                """
            update chan_c_historical_replay_tasks task
               set status = 'failed', last_error = $4, failure = $5::jsonb,
                   worker_id = null, claim_token = null, lease_until = null,
                   lease_heartbeat_at = null, finished_at = now(), updated_at = now()
             where task.id = $1 and task.status = 'running'
               and task.claim_token = $2 and task.lease_version = $3
            """,
                task["id"],
                task["claim_token"],
                task["lease_version"],
                failure["message"],
                json.dumps(failure, sort_keys=True),
            )
            return result.endswith(" 1")


async def finalize_replay_batch(
    conn: Any,
    *,
    batch_id: int,
    sealed_by: str,
    expected_contract_hash: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Seal a successful replay batch using only durable control evidence.

    This operation never rewrites tasks or replay data. A sealed parent with a
    still-running child is repaired only when every task and replay head already
    proves successful completion.
    """
    actor = str(sealed_by).strip()
    if not actor:
        raise ValueError("sealed_by is required")
    expected_hash = str(expected_contract_hash or "")
    if len(expected_hash) != 64 or any(char not in "0123456789abcdef" for char in expected_hash):
        raise ValueError("expected_contract_hash must be a 64-character lowercase hex digest")
    lock = "for share" if dry_run else "for update"
    async with conn.transaction():
        parent = await conn.fetchrow(
            f"""
            select id, status, batch_kind, publication_namespace, profile_id,
                   run_group_id, config_hash, effective_config, audit_references
              from chan_c_batches
             where id = $1
             {lock}
            """,
            batch_id,
        )
        child = await conn.fetchrow(
            f"""
            select batch_id, status, source_batch_id, contract_version,
                   contract_hash, contract, eligible_universe_snapshot_id,
                   canonical_gate_snapshot_id, cutoff_policy
              from chan_c_historical_replay_batches
             where batch_id = $1
             {lock}
            """,
            batch_id,
        )
        blockers: list[str] = []
        if parent is None:
            blockers.append("batch_status:missing_parent")
        elif str(parent["batch_kind"]) != "historical_replay":
            blockers.append("batch_status:not_historical_replay")
        elif str(parent["status"]) not in SEALABLE_PARENT_BATCH_STATUSES:
            blockers.append(f"batch_status:parent_{parent['status']}")
        if child is None:
            blockers.append("batch_status:missing_child")
        elif str(child["status"]) not in SEALABLE_REPLAY_BATCH_STATUSES:
            blockers.append(f"batch_status:child_{child['status']}")
        contract_payload: Mapping[str, Any] = {}
        if child is not None:
            raw_contract = child["contract"]
            if isinstance(raw_contract, str):
                raw_contract = json.loads(raw_contract)
            if isinstance(raw_contract, Mapping):
                contract_payload = raw_contract
            if str(child["contract_hash"]) != expected_hash:
                blockers.append("contract_hash:mismatch")
            if not contract_payload or _digest(contract_payload) != str(child["contract_hash"]):
                blockers.append("contract_hash:invalid_stored_digest")
            if str(child["contract_version"]) != str(contract_payload.get("contract_version", "")):
                blockers.append("contract_version:mismatch")
        if parent is not None and child is not None:
            effective_config = parent["effective_config"]
            audit_references = parent["audit_references"]
            if isinstance(effective_config, str):
                effective_config = json.loads(effective_config)
            if isinstance(audit_references, str):
                audit_references = json.loads(audit_references)
            source_batch_id = int(contract_payload.get("source_batch_id", 0) or 0)
            source_audit = any(
                isinstance(item, Mapping)
                and item.get("type") == "source_batch"
                and int(item.get("batch_id", 0)) == source_batch_id
                for item in (audit_references or [])
            )
            if (
                str(parent["publication_namespace"]) != "historical-replay"
                or str(parent["profile_id"]) != "module-c-historical-replay-v1"
                or str(parent["config_hash"]) != str(contract_payload.get("config_hash", ""))
                or str(child["source_batch_id"]) != str(contract_payload.get("source_batch_id", ""))
                or str(child["eligible_universe_snapshot_id"])
                != str(contract_payload.get("eligible_universe_snapshot_id", ""))
                or str(child["canonical_gate_snapshot_id"])
                != str(contract_payload.get("canonical_gate_snapshot_id", ""))
                or str(child["cutoff_policy"])
                != str(contract_payload.get("cutoff_policy", ""))
                or not isinstance(effective_config, Mapping)
                or effective_config.get("replay_contract_version") != contract_payload.get("contract_version")
                or int(effective_config.get("source_batch_id", 0)) != source_batch_id
                or not source_audit
            ):
                blockers.append("batch_lineage:mismatch")

        contract_config_hash = str(contract_payload.get("config_hash", ""))
        contract_version = str(contract_payload.get("contract_version", ""))
        run_group_id = str(parent["run_group_id"]) if parent is not None else ""
        publication_namespace = (
            str(parent["publication_namespace"]) if parent is not None else ""
        )
        profile_id = str(parent["profile_id"]) if parent is not None else ""

        counts_row = await conn.fetchrow(
            """
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
                       outbox.processed_at, outbox.payload
                  from ordered_heads actual
                  left join chan_c_head_history history
                    on history.symbol_id = actual.symbol_id
                   and history.chan_level = actual.chan_level
                   and history.mode = actual.mode
                   and history.new_run_id = actual.run_id
                  left join chan_c_head_outbox outbox
                    on outbox.head_history_id = history.id
            )
            select count(*) as total_tasks,
                   count(*) filter (where eligible and status = 'completed')
                       as completed_tasks,
                   count(*) filter (where not eligible and status = 'excluded')
                       as excluded_tasks,
                   count(*) filter (where status = 'pending') as pending_tasks,
                   count(*) filter (where status = 'running') as running_tasks,
                   count(*) filter (where status = 'failed') as failed_tasks,
                   count(*) filter (
                       where (eligible and status <> 'completed')
                          or (not eligible and status <> 'excluded')
                   ) as invalid_task_states,
                   count(*) filter (where status = 'completed' and run_id is null)
                       as missing_run_ids,
                   count(*) filter (
                       where status = 'completed' and contract_version <> $6
                   ) as invalid_task_contracts,
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
                   ) as invalid_runs,
                   (
                       select count(*)
                         from expected_heads expected
                         left join actual_heads actual
                           on actual.task_id = expected.task_id and actual.mode = expected.mode
                        where actual.task_id is null
                   ) as missing_heads,
                   (
                       select count(*)
                         from actual_heads actual
                         left join expected_heads expected
                           on expected.task_id = actual.task_id and expected.mode = actual.mode
                        where expected.task_id is null
                   ) as unexpected_heads,
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
                   ) as mismatched_heads,
                   (
                       select count(*) from history_evidence where history_id is null
                   ) as missing_history,
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
                           or history_provenance->>'publication_profile' <> 'historical_replay'
                           or history_provenance->>'run_group_id' <> $3)
                   ) as invalid_history,
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
                   ) as unexpected_history,
                   (
                       select count(*) from history_evidence
                        where history_id is not null and outbox_id is null
                   ) as missing_outbox,
                   (
                       select count(*) from history_evidence
                        where outbox_id is not null
                          and (outbox_status <> 'completed' or processed_at is null)
                   ) as blocking_outbox,
                   (
                       select count(*) from history_evidence
                        where outbox_id is not null
                          and ((payload->>'id')::bigint is distinct from history_id
                           or (payload->>'symbol_id')::integer is distinct from symbol_id
                           or (payload->>'chan_level')::integer is distinct from chan_level
                           or payload->>'mode' is distinct from mode
                           or (payload->>'base_timeframe')::integer is distinct from history_base_timeframe
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
                   ) as invalid_outbox_payload,
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
                           or event.point_time <> identity.point_time
                           or identity.symbol_id <> evidence.symbol_id
                           or identity.chan_level <> evidence.chan_level
                           or identity.config_hash <> $2
                           or event.provenance->>'publication_profile' <> 'historical_replay'
                           or event.provenance->>'run_group_id' <> $3
                           or (event.provenance->>'new_run_id')::bigint is distinct from evidence.run_id
                           or event.provenance->>'mode' is distinct from evidence.mode
                           or (event.provenance->>'chan_level')::integer is distinct from evidence.chan_level
                           or (event.provenance->>'symbol_id')::integer is distinct from evidence.symbol_id
                   ) as invalid_lifecycle_events
              from tasks
            """,
            batch_id,
            contract_config_hash,
            run_group_id,
            publication_namespace,
            profile_id,
            contract_version,
        )
        counts = {key: int(value or 0) for key, value in dict(counts_row or {}).items()}
        if counts.get("total_tasks", 0) == 0:
            blockers.append("empty_batch")
        for key in (
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
        ):
            if counts.get(key, 0):
                blockers.append(key)

        reconciliation = await build_reconciliation(conn)
        if reconciliation.get("decision") != "PASS":
            blockers.extend(
                f"lifecycle_reconciliation:{item}"
                for item in reconciliation.get("blockers", [])
            )

        parent_status = str(parent["status"]) if parent is not None else "missing"
        child_status = str(child["status"]) if child is not None else "missing"
        result = {
            "batch_id": batch_id,
            "dry_run": dry_run,
            "ready": not blockers,
            "blockers": blockers,
            "counts": counts,
            "lifecycle_reconciliation": {
                "decision": reconciliation.get("decision"),
                "blockers": list(reconciliation.get("blockers", [])),
                "outbox_blocking_count": int(
                    reconciliation.get("outbox", {}).get("blocking_count", 0)
                ),
                "projection_mismatch_count": int(
                    reconciliation.get("projection_replay", {}).get("mismatch_count", 0)
                ),
                "published_head_history_missing_count": int(
                    reconciliation.get("published_head_history", {}).get("missing_count", 0)
                ),
            },
            "parent_status_before": parent_status,
            "child_status_before": child_status,
            "parent_status_after": parent_status if dry_run else (
                "sealed" if not blockers else parent_status
            ),
            "child_status_after": child_status if dry_run else (
                "sealed" if not blockers else child_status
            ),
            "would_parent_status": "sealed" if not blockers else parent_status,
            "would_child_status": "sealed" if not blockers else child_status,
            "repaired_parent_already_sealed": (
                not dry_run and not blockers
                and parent_status == "sealed" and child_status != "sealed"
            ),
            "would_repair_parent_already_sealed": (
                not blockers and parent_status == "sealed" and child_status != "sealed"
            ),
        }
        if blockers:
            if dry_run:
                return result
            raise ReplayBatchNotFinalizableError(
                f"Replay batch {batch_id} is not finalizable: {','.join(blockers)}"
            )
        if dry_run:
            return result
        if child_status != "sealed":
            updated = await conn.execute(
                """
                update chan_c_historical_replay_batches
                   set status = 'sealed', finished_at = coalesce(finished_at, now()),
                       updated_at = now()
                 where batch_id = $1 and status = $2
                """,
                batch_id,
                child_status,
            )
            if not updated.endswith(" 1"):
                raise RuntimeError(f"Replay child batch {batch_id} status changed during finalization")
        if parent_status != "sealed":
            updated = await conn.execute(
                """
                update chan_c_batches
                   set status = 'sealed', sealed_at = now(), sealed_by = $2
                 where id = $1 and status = $3
                """,
                batch_id,
                actor,
                parent_status,
            )
            if not updated.endswith(" 1"):
                raise RuntimeError(f"Replay parent batch {batch_id} status changed during finalization")
        return result


def _closed_period(level: str, *, cutoff: datetime, as_of: datetime) -> bool:
    cutoff_local = cutoff.astimezone(CN_TZ)
    as_of_local = as_of.astimezone(CN_TZ)
    if level == "1d":
        return cutoff_local.date() < as_of_local.date() or (
            cutoff_local.date() == as_of_local.date() and as_of_local.time() >= cutoff_local.time()
        )
    if level == "1w":
        return (cutoff_local.isocalendar().year, cutoff_local.isocalendar().week) < (
            as_of_local.isocalendar().year,
            as_of_local.isocalendar().week,
        )
    if level == "1m":
        return (cutoff_local.year, cutoff_local.month) < (as_of_local.year, as_of_local.month)
    raise ValueError(level)


def _digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
