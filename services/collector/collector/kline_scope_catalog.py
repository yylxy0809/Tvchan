"""Generation-fenced K-line scope catalog bootstrap and finalization.

The bootstrap only reads ``klines`` and writes the small catalog tables.  It
is intentionally one-shot and opt-in; callers choose a bounded batch and may
resume all remaining incomplete rows after interruption.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Sequence
from typing import Any
from uuid import UUID

import asyncpg


TIMEFRAME_CODES = (5, 15, 30, 60, 1440, 10080, 43200)
TIMEFRAME_NAMES = {
    "5f": 5, "15f": 15, "30f": 30, "1h": 60,
    "1d": 1440, "1w": 10080, "1m": 43200,
}

UNKNOWN_SCOPES_SQL = """
select symbol_id, timeframe, state, updated_at
  from kline_scope_catalog
 where generation_id = $1
   and not bounds_complete
 order by symbol_id, timeframe
 limit $2
"""

CONTROL_SHARE_SQL = """
select control_key
  from kline_scope_catalog_control
 where control_key = 'active'
 for share
"""

GENERATION_SHARE_SQL = """
select generation_id, status
  from kline_scope_catalog_generations
 where generation_id = $1
 for share
"""

RECORD_PRESENT_SCOPE_SQL = """
update kline_scope_catalog catalog
   set state = 'present',
       bounds_complete = case when catalog.state = 'unknown' then false
                              else catalog.bounds_complete end,
       min_ts = case when catalog.min_ts is null then target.min_ts
                     else least(catalog.min_ts, target.min_ts) end,
       max_ts = case when catalog.max_ts is null then target.max_ts
                     else greatest(catalog.max_ts, target.max_ts) end,
       updated_at = clock_timestamp()
  from kline_scope_catalog_generations generation,
       unnest($1::integer[], $2::integer[], $3::timestamptz[], $4::timestamptz[])
           as target(symbol_id, timeframe, min_ts, max_ts)
 where catalog.generation_id = generation.generation_id
   and catalog.symbol_id = target.symbol_id
   and catalog.timeframe = target.timeframe
   and (
       generation.status = 'building'
       or catalog.generation_id = (
           select active_generation_id
             from kline_scope_catalog_control
            where control_key = 'active'
       )
   )
   and generation.status in ('building', 'complete')
   and (
       catalog.state <> 'present'
       or catalog.min_ts is null
       or catalog.max_ts is null
       or target.min_ts < catalog.min_ts
       or target.max_ts > catalog.max_ts
   )
"""

CATALOG_TARGETS_SQL = """
select catalog.generation_id, catalog.symbol_id, catalog.timeframe, catalog.updated_at
  from kline_scope_catalog catalog
  join kline_scope_catalog_generations generation
    on generation.generation_id = catalog.generation_id
  join unnest($1::integer[], $2::integer[]) as target(symbol_id, timeframe)
    on target.symbol_id = catalog.symbol_id and target.timeframe = catalog.timeframe
 where generation.status = 'building'
    or (
        generation.status = 'complete'
        and catalog.generation_id = (
            select active_generation_id
              from kline_scope_catalog_control
             where control_key = 'active'
        )
    )
"""

SCAN_SCOPES_SQL = """
select target.symbol_id, target.timeframe,
       count(kline.ts)::bigint as bar_count,
       min(kline.ts) as min_ts,
       max(kline.ts) as max_ts
  from unnest($1::integer[], $2::integer[]) as target(symbol_id, timeframe)
  left join klines kline
    on kline.symbol_id = target.symbol_id and kline.timeframe = target.timeframe
 group by target.symbol_id, target.timeframe
"""

CAS_EXACT_SCOPES_SQL = """
update kline_scope_catalog catalog
   set state = target.state,
       bounds_complete = true,
       min_ts = target.min_ts,
       max_ts = target.max_ts,
       updated_at = clock_timestamp()
  from unnest(
       $1::uuid[], $2::integer[], $3::integer[], $4::varchar[],
       $5::timestamptz[], $6::timestamptz[], $7::timestamptz[]
  ) as target(generation_id, symbol_id, timeframe, state, min_ts, max_ts, selected_version)
 where catalog.generation_id = target.generation_id
   and catalog.symbol_id = target.symbol_id
   and catalog.timeframe = target.timeframe
   and catalog.updated_at = target.selected_version
"""

BOOTSTRAP_CAS_EXACT_SCOPES_SQL = """
update kline_scope_catalog catalog
   set state = target.state,
       bounds_complete = true,
       min_ts = target.min_ts,
       max_ts = target.max_ts,
       updated_at = clock_timestamp()
  from unnest(
       $1::uuid[], $2::integer[], $3::integer[], $4::varchar[],
       $5::timestamptz[], $6::timestamptz[], $7::varchar[], $8::timestamptz[]
  ) as target(
       generation_id, symbol_id, timeframe, state, min_ts, max_ts,
       selected_state, selected_version
  )
 where catalog.generation_id = target.generation_id
   and catalog.symbol_id = target.symbol_id
   and catalog.timeframe = target.timeframe
   and catalog.state = target.selected_state
   and not catalog.bounds_complete
   and catalog.updated_at = target.selected_version
"""

INVALIDATE_SCOPES_SQL = """
update kline_scope_catalog catalog
   set state = 'unknown',
       bounds_complete = false,
       min_ts = null,
       max_ts = null,
       updated_at = clock_timestamp()
  from kline_scope_catalog_generations generation,
       unnest($1::integer[], $2::integer[]) as target(symbol_id, timeframe)
 where catalog.generation_id = generation.generation_id
   and catalog.symbol_id = target.symbol_id
   and catalog.timeframe = target.timeframe
   and (
       generation.status = 'building'
       or catalog.generation_id = (
           select active_generation_id
             from kline_scope_catalog_control
            where control_key = 'active'
       )
   )
   and generation.status in ('building', 'complete')
"""

GENERATION_SUMMARY_SQL = """
select count(*)::bigint as scope_count,
       count(*) filter (where state = 'unknown')::bigint as unknown_count,
       count(*) filter (
           where not bounds_complete
              or state = 'unknown'
              or (state = 'present' and (min_ts is null or max_ts is null or min_ts > max_ts))
              or (state = 'empty' and (min_ts is not null or max_ts is not null))
       )::bigint as incomplete_count
  from kline_scope_catalog
 where generation_id = $1
"""


class InvalidScopeGeneration(RuntimeError):
    pass


class IncompleteScopeGeneration(RuntimeError):
    pass


def _updated_count(command_tag: str) -> int:
    try:
        return int(command_tag.rsplit(" ", 1)[-1])
    except (ValueError, IndexError) as exc:
        raise RuntimeError(f"unexpected database command tag: {command_tag!r}") from exc


def parse_timeframes(raw: str) -> tuple[int, ...]:
    names = [value.strip().lower() for value in raw.split(",") if value.strip()]
    invalid = [value for value in names if value not in TIMEFRAME_NAMES]
    if invalid or not names:
        raise ValueError(f"unsupported timeframes: {','.join(invalid or ['none'])}")
    return tuple(dict.fromkeys(TIMEFRAME_NAMES[value] for value in names))


async def _lock_catalog_control_shared(conn: Any) -> None:
    if await conn.fetchrow(CONTROL_SHARE_SQL) is None:
        raise InvalidScopeGeneration("scope catalog active control row is missing")


async def _lock_building_generation_shared(conn: Any, generation_id: UUID) -> None:
    generation = await conn.fetchrow(GENERATION_SHARE_SQL, generation_id)
    if generation is None:
        raise InvalidScopeGeneration(f"scope catalog generation not found: {generation_id}")
    status = str(generation["status"])
    if status != "building":
        raise InvalidScopeGeneration(
            f"scope catalog generation {generation_id} is {status}, not building"
        )


async def create_generation(
    conn: Any,
    *,
    generation_id: UUID,
    timeframes: Sequence[int] = TIMEFRAME_CODES,
) -> dict[str, Any]:
    """Atomically freeze the active-symbol scope manifest as ``unknown``."""
    normalized = tuple(dict.fromkeys(int(value) for value in timeframes))
    if not normalized or any(value not in TIMEFRAME_CODES for value in normalized):
        raise ValueError("timeframes must be non-empty supported K-line codes")
    async with conn.transaction():
        control = await conn.fetchrow(
            """select active_generation_id, revision
                 from kline_scope_catalog_control
                where control_key = 'active'
                for update"""
        )
        if control is None:
            raise InvalidScopeGeneration("scope catalog active control row is missing")
        building = await conn.fetchrow(
            """select generation_id
                 from kline_scope_catalog_generations
                where status = 'building'
                limit 1
                for update"""
        )
        if building is not None:
            raise InvalidScopeGeneration(
                "scope catalog building generation already exists: "
                f"{building['generation_id']}"
            )
        symbol_ids = list(await conn.fetchval(
            "select coalesce(array_agg(id order by id), '{}'::integer[]) from symbols where is_active"
        ))
        expected_scope_count = len(symbol_ids) * len(normalized)
        if expected_scope_count <= 0:
            raise InvalidScopeGeneration("generation scope manifest must not be empty")
        await conn.execute(
            """insert into kline_scope_catalog_generations(
                   generation_id, status, expected_scope_count, symbol_ids, timeframes,
                   base_active_generation_id, base_control_revision
               ) values ($1, 'building', $2, $3, $4, $5, $6)""",
            generation_id, expected_scope_count, symbol_ids, list(normalized),
            control["active_generation_id"], int(control["revision"]),
        )
        await conn.execute(
            """insert into kline_scope_catalog(generation_id, symbol_id, timeframe)
               select $1, symbol.id, timeframe.code
                 from unnest($2::integer[]) as symbol_id(id)
                 join symbols symbol on symbol.id = symbol_id.id
                 cross join unnest($3::integer[]) as timeframe(code)""",
            generation_id, symbol_ids, list(normalized),
        )
    return {
        "generation_id": str(generation_id),
        "status": "building",
        "expected_scope_count": expected_scope_count,
    }


async def bootstrap_generation_batch(
    conn: Any,
    *,
    generation_id: UUID,
    batch_size: int = 25,
) -> dict[str, int]:
    """Scan and CAS a bounded batch; already-updated scopes always win."""
    if not 1 <= batch_size <= 100:
        raise ValueError("batch_size must be between 1 and 100")
    async with conn.transaction():
        await _lock_catalog_control_shared(conn)
        await _lock_building_generation_shared(conn, generation_id)
        scopes = await conn.fetch(UNKNOWN_SCOPES_SQL, generation_id, batch_size)
        invalid = [
            str(scope["state"])
            for scope in scopes
            if str(scope["state"]) not in {"unknown", "present"}
        ]
        if invalid:
            raise RuntimeError(f"incomplete scope has invalid state: {invalid[0]}")
        if not scopes:
            return {"selected": 0, "updated": 0, "cas_skipped": 0}
        targeted_scopes = tuple(
            (int(scope["symbol_id"]), int(scope["timeframe"])) for scope in scopes
        )
        scanned = await conn.fetch(
            SCAN_SCOPES_SQL,
            [symbol_id for symbol_id, _timeframe in targeted_scopes],
            [timeframe for _symbol_id, timeframe in targeted_scopes],
        )
        exact: dict[tuple[int, int], tuple[str, Any, Any]] = {}
        for row in scanned:
            bar_count = int(row["bar_count"])
            exact[(int(row["symbol_id"]), int(row["timeframe"]))] = (
                "present" if bar_count else "empty",
                row["min_ts"] if bar_count else None,
                row["max_ts"] if bar_count else None,
            )
        expected_scopes = set(targeted_scopes)
        if len(scanned) != len(targeted_scopes) or set(exact) != expected_scopes:
            raise RuntimeError(
                "bulk scope scan returned incomplete or duplicate results: "
                f"expected={len(targeted_scopes)}, rows={len(scanned)}, unique={len(exact)}"
            )
        targets = [
            {
                "generation_id": generation_id,
                "symbol_id": int(scope["symbol_id"]),
                "timeframe": int(scope["timeframe"]),
                "state": str(scope["state"]),
                "updated_at": scope["updated_at"],
            }
            for scope in scopes
        ]
        updated = await _record_bootstrap_exact_scopes(
            conn, targets=targets, exact=exact,
        )
        cas_skipped = len(scopes) - updated
    return {"selected": len(scopes), "updated": updated, "cas_skipped": cas_skipped}


async def record_present_scope(
    conn: Any,
    *,
    symbol_id: int,
    timeframe: int,
    timestamp: Any,
) -> int:
    """Writer hook; call in the K-line transaction and treat zero as catalog fallback.

    A zero result means the scope is absent from every building/active
    generation (for example a newly listed symbol).  It must never be
    interpreted as an empty K-line scope.
    """
    return await record_present_scopes(
        conn, scopes=[(symbol_id, timeframe, timestamp, timestamp)],
    )


async def record_present_scopes(
    conn: Any,
    *,
    scopes: Sequence[tuple[int, int, Any, Any]],
) -> int:
    """Bulk monotonic writer hook; caller keeps it in the K-line transaction."""
    merged: dict[tuple[int, int], tuple[Any, Any]] = {}
    for symbol_id, timeframe, min_ts, max_ts in scopes:
        key = (int(symbol_id), int(timeframe))
        if key[1] not in TIMEFRAME_CODES:
            raise ValueError(f"unsupported timeframe code: {key[1]}")
        if min_ts is None or max_ts is None or min_ts > max_ts:
            raise ValueError("present scopes require ordered non-null min_ts/max_ts")
        previous = merged.get(key)
        merged[key] = (
            min(min_ts, previous[0]) if previous else min_ts,
            max(max_ts, previous[1]) if previous else max_ts,
        )
    if not merged:
        return 0
    await _lock_catalog_control_shared(conn)
    symbol_ids = [key[0] for key in merged]
    timeframes = [key[1] for key in merged]
    min_values = [bounds[0] for bounds in merged.values()]
    max_values = [bounds[1] for bounds in merged.values()]
    return _updated_count(await conn.execute(
        RECORD_PRESENT_SCOPE_SQL, symbol_ids, timeframes, min_values, max_values,
    ))


async def _catalog_targets(
    conn: Any,
    scopes: Sequence[tuple[int, int]],
) -> tuple[tuple[tuple[int, int], ...], list[dict[str, Any]]]:
    normalized = tuple(dict.fromkeys((int(symbol_id), int(timeframe)) for symbol_id, timeframe in scopes))
    invalid = [timeframe for _symbol_id, timeframe in normalized if timeframe not in TIMEFRAME_CODES]
    if invalid:
        raise ValueError(f"unsupported timeframe code: {invalid[0]}")
    if not normalized:
        return normalized, []
    await _lock_catalog_control_shared(conn)
    rows = await conn.fetch(
        CATALOG_TARGETS_SQL,
        [symbol_id for symbol_id, _timeframe in normalized],
        [timeframe for _symbol_id, timeframe in normalized],
    )
    return normalized, [dict(row) for row in rows]


async def _record_exact_scopes(
    conn: Any,
    *,
    targets: Sequence[dict[str, Any]],
    exact: dict[tuple[int, int], tuple[str, Any, Any]],
) -> int:
    if not targets:
        return 0
    return _updated_count(await conn.execute(
        CAS_EXACT_SCOPES_SQL,
        [row["generation_id"] for row in targets],
        [int(row["symbol_id"]) for row in targets],
        [int(row["timeframe"]) for row in targets],
        [exact[(int(row["symbol_id"]), int(row["timeframe"]))][0] for row in targets],
        [exact[(int(row["symbol_id"]), int(row["timeframe"]))][1] for row in targets],
        [exact[(int(row["symbol_id"]), int(row["timeframe"]))][2] for row in targets],
        [row["updated_at"] for row in targets],
    ))


async def _record_bootstrap_exact_scopes(
    conn: Any,
    *,
    targets: Sequence[dict[str, Any]],
    exact: dict[tuple[int, int], tuple[str, Any, Any]],
) -> int:
    if not targets:
        return 0
    return _updated_count(await conn.execute(
        BOOTSTRAP_CAS_EXACT_SCOPES_SQL,
        [row["generation_id"] for row in targets],
        [int(row["symbol_id"]) for row in targets],
        [int(row["timeframe"]) for row in targets],
        [exact[(int(row["symbol_id"]), int(row["timeframe"]))][0] for row in targets],
        [exact[(int(row["symbol_id"]), int(row["timeframe"]))][1] for row in targets],
        [exact[(int(row["symbol_id"]), int(row["timeframe"]))][2] for row in targets],
        [str(row["state"]) for row in targets],
        [row["updated_at"] for row in targets],
    ))


async def record_empty_scopes(
    conn: Any,
    *,
    scopes: Sequence[tuple[int, int]],
) -> int:
    """Record proven emptiness with version-CAS in the caller's K-line transaction."""
    normalized, targets = await _catalog_targets(conn, scopes)
    exact = {scope: ("empty", None, None) for scope in normalized}
    updated = await _record_exact_scopes(conn, targets=targets, exact=exact)
    if updated != len(targets):
        await invalidate_scopes(conn, scopes=normalized)
    return updated


async def refresh_scopes_exact(
    conn: Any,
    *,
    scopes: Sequence[tuple[int, int]],
) -> dict[str, int]:
    """Refresh exact bounds with version-CAS in the caller's K-line transaction."""
    normalized, targets = await _catalog_targets(conn, scopes)
    if not normalized or not targets:
        return {"catalog_rows": 0, "updated": 0, "cas_skipped": 0}
    targeted_scopes = tuple(dict.fromkeys(
        (int(row["symbol_id"]), int(row["timeframe"])) for row in targets
    ))
    scanned = await conn.fetch(
        SCAN_SCOPES_SQL,
        [symbol_id for symbol_id, _timeframe in targeted_scopes],
        [timeframe for _symbol_id, timeframe in targeted_scopes],
    )
    exact: dict[tuple[int, int], tuple[str, Any, Any]] = {}
    for row in scanned:
        count = int(row["bar_count"])
        exact[(int(row["symbol_id"]), int(row["timeframe"]))] = (
            "present" if count else "empty",
            row["min_ts"] if count else None,
            row["max_ts"] if count else None,
        )
    updated = await _record_exact_scopes(conn, targets=targets, exact=exact)
    cas_skipped = len(targets) - updated
    if cas_skipped:
        # A concurrent K-line writer changed at least one catalog version after
        # the exact scan. Fail closed instead of retaining stale complete bounds.
        await invalidate_scopes(conn, scopes=targeted_scopes)
    return {
        "catalog_rows": len(targets),
        "updated": updated,
        "cas_skipped": cas_skipped,
    }


async def invalidate_scopes(
    conn: Any,
    *,
    scopes: Sequence[tuple[int, int]],
) -> int:
    """Invalidate existing building/active memberships in the caller transaction.

    The helper never creates membership.  A delete/rewrite path calls it in
    the same transaction as K-line mutation so active readers fall back until
    a generation is rescanned and finalized.
    """
    normalized = tuple(dict.fromkeys((int(symbol_id), int(timeframe)) for symbol_id, timeframe in scopes))
    if not normalized:
        return 0
    invalid = [timeframe for _symbol_id, timeframe in normalized if timeframe not in TIMEFRAME_CODES]
    if invalid:
        raise ValueError(f"unsupported timeframe code: {invalid[0]}")
    symbol_ids = [symbol_id for symbol_id, _timeframe in normalized]
    timeframes = [timeframe for _symbol_id, timeframe in normalized]
    await _lock_catalog_control_shared(conn)
    return _updated_count(await conn.execute(INVALIDATE_SCOPES_SQL, symbol_ids, timeframes))


async def generation_report(conn: Any, *, generation_id: UUID) -> dict[str, Any]:
    generation = await conn.fetchrow(
        """select generation_id, status, expected_scope_count,
                  base_active_generation_id, base_control_revision, created_at,
                  completed_at, failed_at, failure
             from kline_scope_catalog_generations
            where generation_id = $1""",
        generation_id,
    )
    if generation is None:
        raise InvalidScopeGeneration(f"scope catalog generation not found: {generation_id}")
    summary = await conn.fetchrow(GENERATION_SUMMARY_SQL, generation_id)
    return {**dict(generation), **dict(summary)}


async def management_snapshot(conn: Any) -> dict[str, Any]:
    """Report active and resumable building generations without changing state."""
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        control = await conn.fetchrow(
            """select active_generation_id, revision
                 from kline_scope_catalog_control
                where control_key = 'active'"""
        )
        if control is None:
            raise InvalidScopeGeneration("scope catalog active control row is missing")
        building_row = await conn.fetchrow(
            """select generation_id
                 from kline_scope_catalog_generations
                where status = 'building'
                order by created_at
                limit 1"""
        )
        building = None
        if building_row is not None:
            building = await generation_report(
                conn, generation_id=building_row["generation_id"],
            )

        active_generation_id = control["active_generation_id"]
        if active_generation_id is None:
            snapshot: dict[str, Any] = {"active_generation_id": None}
        else:
            snapshot = await generation_report(conn, generation_id=active_generation_id)
        snapshot["control_revision"] = int(control["revision"])
        snapshot["building_generation"] = building
    return snapshot


async def finalize_generation(conn: Any, *, generation_id: UUID) -> dict[str, Any]:
    """Validate and atomically activate one complete building generation."""
    async with conn.transaction():
        control = await conn.fetchrow(
            """select active_generation_id, revision
                 from kline_scope_catalog_control
                where control_key = 'active'
                for update"""
        )
        if control is None:
            raise InvalidScopeGeneration("scope catalog active control row is missing")
        generation = await conn.fetchrow(
            """select generation_id, status, expected_scope_count,
                      base_active_generation_id, base_control_revision
                 from kline_scope_catalog_generations
                where generation_id = $1
                for update""",
            generation_id,
        )
        if generation is None:
            raise InvalidScopeGeneration(f"scope catalog generation not found: {generation_id}")
        status = str(generation["status"])
        if status not in ("building", "complete"):
            raise InvalidScopeGeneration(
                f"scope catalog generation {generation_id} is {status}, not building"
            )
        previous_generation_id = control["active_generation_id"]
        control_revision = int(control["revision"])
        if status == "building":
            base_control_revision = generation["base_control_revision"]
            base_active_generation_id = generation["base_active_generation_id"]
            if base_control_revision is None:
                raise InvalidScopeGeneration(
                    f"scope catalog generation {generation_id} has no base control revision"
                )
            if previous_generation_id != base_active_generation_id:
                raise InvalidScopeGeneration(
                    f"scope catalog generation {generation_id} base active generation changed: "
                    f"expected={base_active_generation_id}, current={previous_generation_id}"
                )
            if control_revision != int(base_control_revision):
                raise InvalidScopeGeneration(
                    f"scope catalog generation {generation_id} control revision changed: "
                    f"expected={base_control_revision}, current={control_revision}"
                )
        summary = await conn.fetchrow(GENERATION_SUMMARY_SQL, generation_id)
        expected = int(generation["expected_scope_count"])
        scope_count = int(summary["scope_count"])
        unknown_count = int(summary["unknown_count"])
        incomplete_count = int(summary["incomplete_count"])
        if scope_count != expected or unknown_count or incomplete_count:
            raise IncompleteScopeGeneration(
                f"scope catalog generation incomplete: expected={expected}, scopes={scope_count}, "
                f"unknown={unknown_count}, incomplete={incomplete_count}"
            )
        if status == "complete":
            if previous_generation_id != generation_id:
                raise InvalidScopeGeneration(
                    f"complete generation {generation_id} is not the active generation"
                )
            return {
                "generation_id": str(generation_id),
                "status": "complete",
                "expected_scope_count": expected,
                "scope_count": scope_count,
                "previous_generation_id": str(generation_id),
                "idempotent": True,
            }
        await conn.execute(
            """update kline_scope_catalog_generations
                  set status = 'complete', completed_at = clock_timestamp()
                where generation_id = $1 and status = 'building'""",
            generation_id,
        )
        control_changed = _updated_count(await conn.execute(
            """update kline_scope_catalog_control
                  set active_generation_id = $1,
                      revision = revision + 1,
                      updated_at = clock_timestamp()
                where control_key = 'active'
                  and revision = $2
                  and active_generation_id is not distinct from $3""",
            generation_id, int(generation["base_control_revision"]),
            generation["base_active_generation_id"],
        ))
        if control_changed != 1:
            raise InvalidScopeGeneration(
                f"scope catalog generation {generation_id} lost its control revision fence"
            )
        if previous_generation_id is not None and previous_generation_id != generation_id:
            await conn.execute(
                """update kline_scope_catalog_generations
                      set status = 'superseded'
                    where generation_id = $1 and status = 'complete'""",
                previous_generation_id,
            )
    return {
        "generation_id": str(generation_id),
        "status": "complete",
        "expected_scope_count": expected,
        "scope_count": scope_count,
        "previous_generation_id": (
            str(previous_generation_id) if previous_generation_id is not None else None
        ),
        "idempotent": False,
    }


async def fail_generation(conn: Any, *, generation_id: UUID, failure: str) -> bool:
    if not failure.strip():
        raise ValueError("failure reason must not be empty")
    changed = _updated_count(await conn.execute(
        """update kline_scope_catalog_generations
              set status = 'failed', failed_at = clock_timestamp(), failure = $2
            where generation_id = $1 and status = 'building'""",
        generation_id, failure.strip(),
    ))
    return changed == 1


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage the generation-fenced K-line scope catalog")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--generation-id")
    parser.add_argument("--timeframes", default=",".join(TIMEFRAME_NAMES))
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--failure")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--create-generation", action="store_true")
    actions.add_argument("--bootstrap", action="store_true")
    actions.add_argument("--finalize", action="store_true")
    actions.add_argument("--fail", action="store_true")
    args = parser.parse_args(argv)
    if not str(args.database_url or "").strip():
        parser.error("DATABASE_URL or --database-url is required")
    if any((args.create_generation, args.bootstrap, args.finalize, args.fail)) and not args.generation_id:
        parser.error("the selected action requires --generation-id")
    if args.generation_id:
        try:
            args.generation_id = UUID(args.generation_id)
        except ValueError:
            parser.error("--generation-id must be a UUID")
    if not 1 <= args.batch_size <= 100:
        parser.error("--batch-size must be between 1 and 100")
    try:
        args.timeframes = parse_timeframes(args.timeframes)
    except ValueError as exc:
        parser.error(str(exc))
    if args.fail and not str(args.failure or "").strip():
        parser.error("--fail requires --failure")
    return args


async def run(args: argparse.Namespace) -> dict[str, Any]:
    conn = await asyncpg.connect(args.database_url)
    try:
        if args.create_generation:
            return await create_generation(
                conn, generation_id=args.generation_id, timeframes=args.timeframes,
            )
        if args.bootstrap:
            return await bootstrap_generation_batch(
                conn, generation_id=args.generation_id, batch_size=args.batch_size,
            )
        if args.finalize:
            return await finalize_generation(conn, generation_id=args.generation_id)
        if args.fail:
            changed = await fail_generation(
                conn, generation_id=args.generation_id, failure=args.failure,
            )
            return {"generation_id": str(args.generation_id), "failed": changed}
        if args.generation_id is None:
            report = await management_snapshot(conn)
        else:
            report = await generation_report(conn, generation_id=args.generation_id)
        return json.loads(json.dumps(report, default=str))
    finally:
        await conn.close()


async def main(argv: Sequence[str] | None = None) -> int:
    payload = await run(parse_args(argv))
    print(json.dumps(payload, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
