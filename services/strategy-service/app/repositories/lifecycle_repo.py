from __future__ import annotations

from datetime import datetime
from typing import Any

import asyncpg

from app.engine.time_utils import utc_time


EVENTS_SQL = """
select e.id, e.fingerprint, e.event_type, e.effective_time, e.observed_time, e.point_time,
       e.previous_mode, e.current_mode, e.run_id, e.provenance,
       h.symbol_id, h.chan_level, h.mode as head_mode,
       h.publication_profile, h.snapshot_version, h.published_at,
       i.structure_type, i.side_or_direction, i.bsp_type
from chan_structure_lifecycle_events e
join chan_c_head_history h on h.id = e.head_history_id
join chan_structure_identity i on i.fingerprint = e.fingerprint
where e.effective_time <= $1::timestamptz
  and e.observed_time <= $1::timestamptz
order by e.effective_time, e.observed_time, e.id
"""

CURRENT_SQL = """
select c.fingerprint, c.point_time, c.first_seen_time, c.confirm_time,
       c.disappear_time, c.current_status, c.current_mode,
       c.first_seen_run_id, c.confirmed_run_id, c.last_seen_run_id,
       c.provenance, i.symbol_id, i.chan_level, i.structure_type,
       i.side_or_direction, i.bsp_type
from chan_structure_lifecycle_current c
join chan_structure_identity i on i.fingerprint = c.fingerprint
where c.updated_at <= $1::timestamptz
order by i.symbol_id, i.chan_level, c.point_time, c.fingerprint
"""


class LifecycleRepository:
    """Read lifecycle truth only; this repository never falls back to chan_c_runs."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def events_as_of(self, as_of_time: datetime) -> list[dict[str, Any]]:
        """Return events effective and observed no later than ``as_of_time``."""
        as_of = utc_time(as_of_time)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(EVENTS_SQL, as_of)
        return [dict(row) for row in rows]

    async def current_as_of(self, as_of_time: datetime) -> list[dict[str, Any]]:
        as_of = utc_time(as_of_time)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(CURRENT_SQL, as_of)
        return [dict(row) for row in rows]

    async def snapshot_as_of(self, as_of_time: datetime) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Return one observation-safe lifecycle snapshot at ``as_of_time``."""
        as_of = utc_time(as_of_time)
        async with self.pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                events = await conn.fetch(EVENTS_SQL, as_of)
                current = await conn.fetch(CURRENT_SQL, as_of)
        return [dict(row) for row in events], [dict(row) for row in current]
