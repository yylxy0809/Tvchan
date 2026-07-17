from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

from collector.lifecycle import LifecycleState, structure_fingerprint, transition_event, utc_instant

LIFECYCLE_ADVISORY_LOCK_SQL = "select pg_advisory_xact_lock(hashtext('chan-lifecycle-v1'))"
TRY_LIFECYCLE_SESSION_LOCK_SQL = "select pg_try_advisory_lock(hashtext('chan-lifecycle-v1'))"
UNLOCK_LIFECYCLE_SESSION_SQL = "select pg_advisory_unlock(hashtext('chan-lifecycle-v1'))"

CLAIM_OUTBOX_SQL = """
with due as (
    select id
    from chan_c_head_outbox
    where status in ('pending', 'processing', 'failed')
    order by id
    limit $1
    for update skip locked
), claimable as (
    select outbox.id
    from chan_c_head_outbox outbox
    join due on due.id = outbox.id
    where (outbox.status in ('pending', 'failed')
           and coalesce(outbox.next_attempt_at, '-infinity'::timestamptz) <= clock_timestamp())
       or (outbox.status = 'processing' and outbox.lease_until <= clock_timestamp())
)
update chan_c_head_outbox outbox
set status = 'processing',
    lease_version = outbox.lease_version + 1,
    lease_token = md5(outbox.id::text || ':' || (outbox.lease_version + 1)::text || ':' || clock_timestamp()::text),
    lease_until = clock_timestamp() + ($2::int * interval '1 second'),
    next_attempt_at = null,
    attempts = outbox.attempts + 1,
    updated_at = clock_timestamp()
from claimable
where outbox.id = claimable.id
returning outbox.*
"""

FAIL_OUTBOX_SQL = """
update chan_c_head_outbox
set status = case when attempts >= $4 then 'dead_letter' else 'failed' end,
    lease_token = null,
    lease_until = null,
    next_attempt_at = case
        when attempts >= $4 then null
        else clock_timestamp() + ($5::int * interval '1 second')
    end,
    last_error = left($6, 2000),
    failed_at = clock_timestamp(),
    dead_lettered_at = case when attempts >= $4 then clock_timestamp() else null end,
    updated_at = clock_timestamp()
where id = $1 and status = 'processing' and lease_token = $2 and lease_version = $3
returning id, status
"""

ACK_OUTBOX_SQL = """
update chan_c_head_outbox
set status = 'completed', lease_until = null, processed_at = clock_timestamp(),
    updated_at = clock_timestamp()
where id = $1 and status = 'processing' and lease_token = $2 and lease_version = $3
returning id
"""

UPSERT_WATERMARK_SQL = """
insert into chan_lifecycle_observer_watermarks(observer_name, last_outbox_id)
select $1, coalesce(
    (select min(id) - 1 from chan_c_head_outbox where status <> 'completed'),
    (select max(id) from chan_c_head_outbox),
    0
)
on conflict (observer_name) do update
set last_outbox_id = excluded.last_outbox_id,
    updated_at = clock_timestamp()
"""

RENEW_OUTBOX_SQL = """
update chan_c_head_outbox
set lease_until = clock_timestamp() + ($4::int * interval '1 second'),
    updated_at = clock_timestamp()
where id = $1 and status = 'processing' and lease_token = $2 and lease_version = $3
returning id
"""

LOAD_RUN_STRUCTURES_SQL = """
select 'stroke' structure_type, start_ts point_time, end_ts end_time,
       case direction when 1 then 'up' when -1 then 'down' else direction::text end side_or_direction,
       null::text bsp_type, null::integer price_x1000,
       start_price_x1000, end_price_x1000, null::integer low_x1000, null::integer high_x1000
from chan_c_strokes where run_id = $1 and mode = $2
union all
select 'segment', start_ts, end_ts,
       case direction when 1 then 'up' when -1 then 'down' else direction::text end,
       null, null, start_price_x1000, end_price_x1000, null, null
from chan_c_segments where run_id = $1 and mode = $2
union all
select 'center', start_ts, end_ts, null, null, null, null, null, low_x1000, high_x1000
from chan_c_centers where run_id = $1 and mode = $2
union all
select 'signal', ts, null,
       coalesce(extra->>'side', case when lower(signal_type) like '%buy' or signal_type like '%买' then 'buy'
                                    when lower(signal_type) like '%sell' or signal_type like '%卖' then 'sell' end),
       coalesce(extra->>'bsp_type', signal_type), price_x1000, null, null, null, null
from chan_c_signals where run_id = $1 and mode = $2
order by point_time, structure_type
"""

REBUILD_CURRENT_PROJECTION_SQL = """
truncate chan_structure_lifecycle_current;
insert into chan_structure_lifecycle_current(
    fingerprint, point_time, first_seen_time, confirm_time, disappear_time,
    current_status, current_mode, first_seen_run_id, confirmed_run_id,
    last_seen_run_id, provenance, updated_at
)
select distinct on (e.fingerprint)
    e.fingerprint,
    e.point_time,
    min(e.effective_time) filter (where e.event_type = 'first_seen')
        over (partition by e.fingerprint),
    min(e.effective_time) filter (where e.event_type = 'confirmed')
        over (partition by e.fingerprint),
    case when e.event_type = 'disappeared' then e.effective_time end,
    case e.event_type when 'disappeared' then 'disappeared'
                      when 'baseline_observed' then 'baseline_observed'
                      else 'visible' end,
    e.current_mode,
    (array_agg(e.run_id) filter (where e.event_type = 'first_seen')
        over (partition by e.fingerprint order by e.effective_time, e.id))[1],
    (array_agg(e.run_id) filter (where e.event_type = 'confirmed')
        over (partition by e.fingerprint order by e.effective_time, e.id))[1],
    e.run_id,
    e.provenance,
    clock_timestamp()
from chan_structure_lifecycle_events e
join chan_c_head_history h on h.id = e.head_history_id
where h.publication_profile <> 'historical_replay'
order by e.fingerprint, e.effective_time desc, e.id desc
"""


@dataclass(frozen=True)
class Observation:
    symbol_id: int
    chan_level: int
    structure_type: str
    point_time: datetime
    mode: str | None
    side_or_direction: str | None = None
    bsp_type: str | None = None
    end_time: datetime | None = None
    price_x1000: int | None = None
    start_price_x1000: int | None = None
    end_price_x1000: int | None = None
    low_x1000: int | None = None
    high_x1000: int | None = None
    config_hash: str = ""
    identity_version: int = 1

    @property
    def fingerprint(self) -> str:
        return structure_fingerprint(
            symbol_id=self.symbol_id,
            chan_level=self.chan_level,
            structure_type=self.structure_type,
            side_or_direction=self.side_or_direction,
            bsp_type=self.bsp_type,
            point_time=self.point_time,
            end_time=self.end_time,
            price_x1000=self.price_x1000,
            start_price_x1000=self.start_price_x1000,
            end_price_x1000=self.end_price_x1000,
            low_x1000=self.low_x1000,
            high_x1000=self.high_x1000,
            config_hash=self.config_hash,
            identity_version=self.identity_version,
        )


@dataclass(frozen=True)
class PlannedEvent:
    observation: Observation
    event_type: str
    previous_mode: str | None


class LostLifecycleLease(RuntimeError):
    pass


def plan_events(
    *,
    profile: str,
    previous: Iterable[Observation],
    current: Iterable[Observation],
    states: dict[str, LifecycleState] | None = None,
) -> list[PlannedEvent]:
    """Plan deterministic events. A baseline never fabricates prior visibility."""
    old = {item.fingerprint: item for item in previous}
    new = {item.fingerprint: item for item in current}
    planned: list[PlannedEvent] = []
    for fingerprint in sorted(new):
        item = new[fingerprint]
        prior = old.get(fingerprint)
        state = (states or {}).get(fingerprint)
        if state is None and prior:
            state = LifecycleState("visible", prior.mode)
        event_type = transition_event(profile=profile, previous=state, current_mode=item.mode)
        if event_type:
            planned.append(PlannedEvent(item, event_type, state.mode if state else None))
    if profile != "baseline":
        for fingerprint in sorted(old.keys() - new.keys()):
            item = old[fingerprint]
            planned.append(PlannedEvent(item, "disappeared", item.mode))
    return planned


class LifecycleObserver:
    def __init__(self, *, observer_name: str = "chan-lifecycle-v1") -> None:
        self.observer_name = observer_name

    async def claim(self, conn: Any, *, limit: int = 1, lease_seconds: int = 60) -> list[dict[str, Any]]:
        rows = await conn.fetch(CLAIM_OUTBOX_SQL, max(1, limit), max(1, lease_seconds))
        return [dict(row) for row in rows]

    async def process_next(
        self,
        conn: Any,
        *,
        lease_seconds: int = 300,
        max_attempts: int = 5,
        retry_delay_seconds: int = 30,
    ) -> int:
        """Serialize planning and persistence so lifecycle events follow outbox order."""
        if not await conn.fetchval(TRY_LIFECYCLE_SESSION_LOCK_SQL):
            return 0
        try:
            claimed = await self.claim(conn, limit=1, lease_seconds=lease_seconds)
            if not claimed:
                return 0
            try:
                await self.process_claimed(conn, claimed[0])
            except Exception as exc:
                if not await self.fail(
                    conn,
                    claimed[0],
                    error=str(exc),
                    max_attempts=max_attempts,
                    retry_delay_seconds=retry_delay_seconds,
                ):
                    raise LostLifecycleLease(
                        f"Lost lifecycle outbox lease while recording failure: {claimed[0]['id']}"
                    ) from exc
            return 1
        finally:
            await conn.fetchval(UNLOCK_LIFECYCLE_SESSION_SQL)

    async def acknowledge(self, conn: Any, claimed: dict[str, Any]) -> bool:
        row = await conn.fetchrow(
            ACK_OUTBOX_SQL, claimed["id"], claimed["lease_token"], claimed["lease_version"]
        )
        if row is None:
            return False
        await self.pulse(conn)
        return True

    async def pulse(self, conn: Any) -> None:
        """Refresh liveness without advancing past the first unfinished outbox row."""
        await conn.execute(UPSERT_WATERMARK_SQL, self.observer_name)

    async def renew(self, conn: Any, claimed: dict[str, Any], *, lease_seconds: int = 60) -> bool:
        row = await conn.fetchrow(
            RENEW_OUTBOX_SQL,
            claimed["id"], claimed["lease_token"], claimed["lease_version"],
            max(1, lease_seconds),
        )
        return row is not None

    async def fail(
        self,
        conn: Any,
        claimed: dict[str, Any],
        *,
        error: str,
        max_attempts: int = 5,
        retry_delay_seconds: int = 30,
    ) -> bool:
        row = await conn.fetchrow(
            FAIL_OUTBOX_SQL,
            claimed["id"],
            claimed["lease_token"],
            claimed["lease_version"],
            max(1, max_attempts),
            max(1, retry_delay_seconds),
            error,
        )
        return row is not None

    async def load_observations(
        self, conn: Any, *, run_id: int | None, symbol_id: int,
        chan_level: int, mode: str, config_hash: str,
    ) -> list[Observation]:
        if run_id is None:
            return []
        mode_code = {"confirmed": 1, "predictive": 2}[mode]
        rows = await conn.fetch(LOAD_RUN_STRUCTURES_SQL, run_id, mode_code)
        return [
            Observation(
                symbol_id=symbol_id, chan_level=chan_level,
                structure_type=row["structure_type"], point_time=row["point_time"],
                end_time=row["end_time"], mode=mode,
                side_or_direction=row["side_or_direction"], bsp_type=row["bsp_type"],
                price_x1000=row["price_x1000"],
                start_price_x1000=row["start_price_x1000"],
                end_price_x1000=row["end_price_x1000"],
                low_x1000=row["low_x1000"], high_x1000=row["high_x1000"],
                config_hash=config_hash,
            )
            for row in rows
        ]

    async def process_claimed(self, conn: Any, claimed: dict[str, Any]) -> int:
        if not await self.renew(conn, claimed):
            raise LostLifecycleLease(f"Lost lifecycle outbox lease: {claimed['id']}")
        history = await conn.fetchrow(
            "select * from chan_c_head_history where id = $1", claimed["head_history_id"]
        )
        if history is None:
            raise RuntimeError(f"Missing head history: {claimed['head_history_id']}")
        common = dict(
            symbol_id=int(history["symbol_id"]), chan_level=int(history["chan_level"]),
            mode=str(history["mode"]),
        )
        old_config_hash = await conn.fetchval(
            "select config_hash from chan_c_runs where id = $1", history["old_run_id"]
        ) if history["old_run_id"] is not None else None
        new_config_hash = await conn.fetchval(
            "select config_hash from chan_c_runs where id = $1", history["new_run_id"]
        )
        previous = await self.load_observations(
            conn, run_id=history["old_run_id"], config_hash=str(old_config_hash or ""), **common
        )
        current = await self.load_observations(
            conn, run_id=history["new_run_id"], config_hash=str(new_config_hash or ""), **common
        )
        fingerprints = sorted({item.fingerprint for item in previous + current})
        profile = str(history["publication_profile"])
        state_rows = await conn.fetch(
            "select fingerprint, current_status, current_mode from chan_structure_lifecycle_current where fingerprint = any($1::varchar[])",
            fingerprints,
        ) if fingerprints and profile != "historical_replay" else []
        states = {
            str(row["fingerprint"]): LifecycleState(str(row["current_status"]), row["current_mode"])
            for row in state_rows
        }
        events = plan_events(
            profile=profile, previous=previous,
            current=current, states=states,
        )
        visible_fingerprints: set[str] = set()
        visible_heads = await conn.fetch(
            """
            select h.run_id, h.mode, r.config_hash
            from scheme2_chan_c_published_heads h
            join chan_c_runs r on r.id = h.run_id
            where h.symbol_id = $1 and h.chan_level = $2
              and h.status = 'published' and h.run_id is not null
            """,
            history["symbol_id"], history["chan_level"],
        )
        for head in visible_heads:
            observations = await self.load_observations(
                conn, run_id=head["run_id"], symbol_id=int(history["symbol_id"]),
                chan_level=int(history["chan_level"]), mode=str(head["mode"]),
                config_hash=str(head["config_hash"]),
            )
            visible_fingerprints.update(item.fingerprint for item in observations)
        events = [
            event for event in events
            if event.event_type != "disappeared" or event.observation.fingerprint not in visible_fingerprints
        ]
        effective_time = history["published_at"]
        if profile == "historical_replay":
            effective_time = await conn.fetchval(
                """
                select cutoff_time
                  from chan_c_historical_replay_heads
                 where run_id = $1 and mode = $2
                 order by cutoff_time
                 limit 1
                """,
                history["new_run_id"], history["mode"],
            )
            if effective_time is None:
                raise RuntimeError(
                    f"Missing historical replay cutoff for run_id={history['new_run_id']}"
                )
        await self.persist_and_acknowledge(
            conn, claimed=claimed, events=events,
            effective_time=effective_time, observed_time=history["published_at"],
            run_id=int(history["new_run_id"]),
            current_mode=str(history["mode"]),
        )
        return len(events)

    async def rebuild_current_projection(self, conn: Any) -> None:
        """Recreate the disposable read model solely from append-only events."""
        async with conn.transaction():
            await conn.execute(LIFECYCLE_ADVISORY_LOCK_SQL)
            await conn.execute(REBUILD_CURRENT_PROJECTION_SQL)

    async def persist_and_acknowledge(
        self,
        conn: Any,
        *,
        claimed: dict[str, Any],
        events: Iterable[PlannedEvent],
        effective_time: datetime,
        run_id: int | None,
        current_mode: str | None,
        observed_time: datetime | None = None,
    ) -> None:
        """Commit events, projection, fenced ACK and watermark atomically."""
        async with conn.transaction():
            await conn.execute(LIFECYCLE_ADVISORY_LOCK_SQL)
            await self.append_events(
                conn,
                claimed=claimed,
                events=events,
                effective_time=effective_time,
                observed_time=observed_time or effective_time,
                run_id=run_id,
                current_mode=current_mode,
            )
            if not await self.acknowledge(conn, claimed):
                raise LostLifecycleLease(f"Lost lifecycle outbox lease: {claimed['id']}")

    async def append_events(
        self,
        conn: Any,
        *,
        claimed: dict[str, Any],
        events: Iterable[PlannedEvent],
        effective_time: datetime,
        observed_time: datetime,
        run_id: int | None,
        current_mode: str | None,
    ) -> None:
        effective_time = utc_instant(effective_time)
        payload = claimed.get("payload") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        head_history_id = claimed["head_history_id"]
        for event in events:
            item = event.observation
            event_current_mode = None if event.event_type == "disappeared" else item.mode
            await conn.execute(
                """
                insert into chan_structure_identity(
                    fingerprint, identity_version, symbol_id, chan_level, structure_type,
                    side_or_direction, bsp_type, point_time, end_time, price_x1000,
                    start_price_x1000, end_price_x1000, low_x1000, high_x1000,
                    config_hash, payload
                ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb)
                on conflict (fingerprint) do nothing
                """,
                item.fingerprint, item.identity_version, item.symbol_id, item.chan_level,
                item.structure_type, item.side_or_direction, item.bsp_type,
                utc_instant(item.point_time), utc_instant(item.end_time) if item.end_time else None,
                item.price_x1000, item.start_price_x1000, item.end_price_x1000,
                item.low_x1000, item.high_x1000, item.config_hash,
                json.dumps({}),
            )
            await conn.execute(
                """
                insert into chan_structure_lifecycle_events(
                    fingerprint, head_history_id, event_type, effective_time, point_time,
                    previous_mode, current_mode, run_id, provenance, observed_time
                ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
                on conflict (fingerprint, event_type, head_history_id) do nothing
                """,
                item.fingerprint, head_history_id, event.event_type, effective_time,
                utc_instant(item.point_time), event.previous_mode, event_current_mode,
                run_id, json.dumps(payload, sort_keys=True, default=str),
                utc_instant(observed_time),
            )
            if payload.get("publication_profile") == "historical_replay":
                continue
            await conn.execute(
                """
                insert into chan_structure_lifecycle_current(
                    fingerprint, point_time, first_seen_time, confirm_time,
                    disappear_time, current_status, current_mode,
                    first_seen_run_id, confirmed_run_id, last_seen_run_id,
                    provenance, updated_at
                ) values (
                    $1, $2,
                    case when $3::text = 'first_seen' then $4::timestamptz else null end,
                    case when $3::text = 'confirmed' then $4::timestamptz else null end,
                    case when $3::text = 'disappeared' then $4::timestamptz else null end,
                    case $3::text when 'baseline_observed' then 'baseline_observed'
                            when 'disappeared' then 'disappeared' else 'visible' end,
                    $5::varchar,
                    case when $3::text = 'first_seen' then $6::bigint else null end,
                    case when $3::text = 'confirmed' then $6::bigint else null end,
                    $6::bigint, $7::jsonb, clock_timestamp()
                )
                on conflict (fingerprint) do update
                set point_time = excluded.point_time,
                    first_seen_time = coalesce(chan_structure_lifecycle_current.first_seen_time, excluded.first_seen_time),
                    confirm_time = coalesce(chan_structure_lifecycle_current.confirm_time, excluded.confirm_time),
                    disappear_time = excluded.disappear_time,
                    current_status = excluded.current_status,
                    current_mode = excluded.current_mode,
                    first_seen_run_id = coalesce(chan_structure_lifecycle_current.first_seen_run_id, excluded.first_seen_run_id),
                    confirmed_run_id = coalesce(chan_structure_lifecycle_current.confirmed_run_id, excluded.confirmed_run_id),
                    last_seen_run_id = excluded.last_seen_run_id,
                    provenance = excluded.provenance,
                    updated_at = clock_timestamp()
                """,
                item.fingerprint, utc_instant(item.point_time), event.event_type,
                effective_time, event_current_mode, run_id,
                json.dumps(payload, sort_keys=True, default=str),
            )
