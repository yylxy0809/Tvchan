from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


CN_TZ = ZoneInfo("Asia/Shanghai")
REPLAY_CONTRACT_VERSION = "historical-replay-v1"
REPLAY_RUN_GROUP = "historical_replay"
REPLAY_PROVENANCE = "historical_replay"
NATIVE_LEVELS = ("5f", "30f", "1d", "1w", "1m")


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
            row = await conn.fetchrow(
                """
                select source_batch_id, contract_version, contract_hash,
                       eligible_universe_snapshot_id, canonical_gate_snapshot_id, cutoff_policy
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
            actual = {key: row[key] for key in expected} if row else None
            if actual != expected:
                raise RuntimeError(f"Replay batch {batch_id} contract mismatch: {actual!r}")


async def claim_replay_task(
    *,
    kline_writer: Any,
    batch_id: int,
    worker_id: str,
    lease_seconds: int = 900,
    max_attempts: int = 3,
) -> Mapping[str, Any] | None:
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            with candidate as (
                select id
                  from chan_c_historical_replay_tasks
                 where batch_id = $1 and eligible and attempts < $4
                   and (status in ('pending', 'failed')
                        or (status = 'running' and lease_until <= now()))
                 order by cutoff_time, symbol_id, chan_level, id
                 for update skip locked
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
        )
        if row is not None:
            await conn.execute(
                """
                update chan_c_historical_replay_batches
                   set status = 'running', started_at = coalesce(started_at, now()), updated_at = now()
                 where batch_id = $1 and status in ('planned', 'running')
                """,
                batch_id,
            )
        return row


async def heartbeat_replay_task(
    *, kline_writer: Any, task: Mapping[str, Any], lease_seconds: int = 900
) -> bool:
    assert kline_writer._pool is not None
    async with kline_writer._pool.acquire() as conn:
        result = await conn.execute(
            """
            update chan_c_historical_replay_tasks
               set lease_until = now() + ($4::integer * interval '1 second'),
                   lease_heartbeat_at = now(), updated_at = now()
             where id = $1 and status = 'running' and claim_token = $2
               and lease_version = $3 and lease_until > now()
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
        result = await conn.execute(
            """
            update chan_c_historical_replay_tasks
               set status = 'failed', last_error = $4, failure = $5::jsonb,
                   worker_id = null, claim_token = null, lease_until = null,
                   lease_heartbeat_at = null, finished_at = now(), updated_at = now()
             where id = $1 and status = 'running' and claim_token = $2 and lease_version = $3
            """,
            task["id"],
            task["claim_token"],
            task["lease_version"],
            failure["message"],
            json.dumps(failure, sort_keys=True),
        )
        return result.endswith(" 1")


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
