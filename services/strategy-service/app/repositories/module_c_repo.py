from __future__ import annotations

from bisect import bisect_right
import json
from datetime import datetime
from dataclasses import dataclass
from typing import Any

import asyncpg

from app.domain.enums import LEVEL_TO_DB
from app.domain.models import ChanCenter, ChanSignal, ChanStroke, PublishedHead, SymbolInfo
from app.engine.time_utils import utc_time


LEGACY_SHARED_MODE = 0
MODE_TO_DB = {"confirmed": 1, "predictive": 2}
DB_TO_MODE = {value: key for key, value in MODE_TO_DB.items()}


@dataclass(slots=True)
class HistoricalRunLookup:
    selected: PublishedHead | None
    nearest_before: PublishedHead | None
    nearest_after: PublishedHead | None
    run_count: int


@dataclass(frozen=True, slots=True)
class HistoricalHeadFilter:
    symbol_id: int
    level: str
    mode: str
    run_kind: str | None
    run_group_id: str | None
    allow_legacy_mode_fallback: bool


class ModuleCRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool
        self._historical_heads: dict[HistoricalHeadFilter, list[PublishedHead]] = {}
        self._historical_head_untils: dict[HistoricalHeadFilter, list[datetime]] = {}
        self._signals_cache: dict[tuple[int, int], list[ChanSignal]] = {}
        self._strokes_cache: dict[tuple[int, int], list[ChanStroke]] = {}
        self._segments_cache: dict[tuple[int, int], list[ChanStroke]] = {}
        self._centers_cache: dict[tuple[int, int], list[ChanCenter]] = {}
        self._run_ids_by_symbol: dict[int, set[int]] = {}

    async def list_active_symbols(
        self,
        *,
        limit: int | None = None,
        symbols: list[str] | None = None,
    ) -> list[SymbolInfo]:
        async with self.pool.acquire() as conn:
            if symbols:
                rows = await conn.fetch(
                    """
                    select id, code, exchange, name
                    from symbols
                    where is_active = true
                      and (code || '.' || exchange) = any($1::text[])
                    order by code
                    """,
                    symbols,
                )
            elif limit:
                rows = await conn.fetch(
                    """
                    select id, code, exchange, name
                    from symbols
                    where is_active = true
                    order by code
                    limit $1
                    """,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    select id, code, exchange, name
                    from symbols
                    where is_active = true
                    order by code
                    """
                )
        return [
            SymbolInfo(
                symbol_id=row["id"],
                symbol=f"{row['code']}.{row['exchange']}",
                code=row["code"],
                exchange=row["exchange"],
                name=row["name"],
            )
            for row in rows
        ]

    async def fetch_historical_run_signal_rows(
        self,
        *,
        symbols: list[str],
        levels: tuple[str, ...] = ("30f", "5f"),
        run_groups: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        """Fetch audit rows in one set query; this never reads published heads."""
        if not symbols or not run_groups:
            return []
        level_ids = [LEVEL_TO_DB[level] for level in levels]
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select r.id as run_id, r.status, r.run_kind, r.run_group_id,
                       coalesce(r.cutoff_bar_end, r.bar_until) as cutoff_bar_end,
                       case r.chan_level when 5 then '5f' when 15 then '15f' when 30 then '30f'
                           when 1440 then '1d' when 10080 then '1w' when 43200 then '1m' end as chan_level,
                       case r.mode when 1 then 'confirmed' when 2 then 'predictive' else 'legacy' end as mode,
                       r.symbol_id, (sym.code || '.' || sym.exchange) as symbol,
                       coalesce(s.extra->>'side', '') as side, coalesce(s.extra->>'bsp_type', '') as bsp_type,
                       coalesce(s.base_ts, s.ts) as signal_point_time, s.price_x1000, s.is_confirmed
                from chan_c_runs r
                join symbols sym on sym.id = r.symbol_id
                join chan_c_signals s on s.run_id = r.id
                where (sym.code || '.' || sym.exchange) = any($1::text[])
                  and r.chan_level = any($2::int[])
                  and r.run_group_id = any($3::text[])
                order by coalesce(r.cutoff_bar_end, r.bar_until), r.id, s.id
                """,
                symbols,
                level_ids,
                list(run_groups),
            )
        return [dict(row) for row in rows]

    async def fetch_intraday_klines_for_episodes(self, episodes: list[dict[str, Any]], *, conn: asyncpg.Connection | None = None) -> list[dict[str, Any]]:
        """One set-based read of completed native 5F/30F bars for all episode windows."""
        if not episodes:
            return []
        symbols = sorted({str(row["symbol"]) for row in episodes})
        start = min(utc_time(row["daily_setup_first_seen_time"]) for row in episodes)
        end = max(utc_time(row["trigger_window_end"]) for row in episodes)
        if conn is not None:
            rows = await conn.fetch("""
                select (s.code || '.' || s.exchange) as symbol, k.timeframe, k.ts, k.is_complete
                from klines k join symbols s on s.id = k.symbol_id
                where (s.code || '.' || s.exchange) = any($1::text[])
                  and k.timeframe = any(array[5,30]) and k.is_complete = true
                  and k.ts >= $2::timestamptz and k.ts <= $3::timestamptz
                order by symbol, k.timeframe, k.ts
            """, symbols, start, end)
        else:
            async with self.pool.acquire() as acquired:
                rows = await acquired.fetch("""
                    select (s.code || '.' || s.exchange) as symbol, k.timeframe, k.ts, k.is_complete
                    from klines k join symbols s on s.id = k.symbol_id
                    where (s.code || '.' || s.exchange) = any($1::text[])
                      and k.timeframe = any(array[5,30]) and k.is_complete = true
                      and k.ts >= $2::timestamptz and k.ts <= $3::timestamptz
                    order by symbol, k.timeframe, k.ts
                """, symbols, start, end)
        return [{**dict(row), "ts": utc_time(row["ts"])} for row in rows]

    async def fetch_historical_runs_with_signals(self, *, symbols: list[str], levels: tuple[str, ...], run_groups: tuple[str, ...], conn: asyncpg.Connection | None = None, start: datetime | None = None, end: datetime | None = None, batch_size: int = 5000, cursor: tuple[datetime, int] | None = None) -> list[dict[str, Any]]:
        """Returns runs with empty signal collections too, which is required for disappearance detection."""
        if not symbols or not levels or not run_groups:
            return []
        if start is not None:
            start = utc_time(start)
        if end is not None:
            end = utc_time(end)
        if cursor is not None:
            cursor = (utc_time(cursor[0]), int(cursor[1]))
        level_ids = [LEVEL_TO_DB[level] for level in levels]
        query = """
                select r.id as run_id, (sym.code || '.' || sym.exchange) as symbol,
                       case r.chan_level when 5 then '5f' when 30 then '30f' when 1440 then '1d' when 10080 then '1w' end as level,
                       case r.mode when 1 then 'confirmed' when 2 then 'predictive' else 'legacy' end as mode,
                       coalesce(r.cutoff_bar_end, r.bar_until) as cutoff_bar_end, r.run_group_id,
                       coalesce(jsonb_agg(jsonb_build_object('side', s.extra->>'side', 'bsp_type', s.extra->>'bsp_type', 'signal_point_time', coalesce(s.base_ts,s.ts), 'price_x1000', s.price_x1000, 'is_confirmed', s.is_confirmed, 'parent_30f_identity', coalesce(s.extra->>'parent_30f_identity', s.extra->>'parent_signal_identity')) order by s.id) filter (where s.id is not null), '[]'::jsonb) as signals
                from chan_c_runs r join symbols sym on sym.id=r.symbol_id
                left join chan_c_signals s on s.run_id=r.id and s.mode=r.mode
                where (sym.code || '.' || sym.exchange)=any($1::text[]) and r.chan_level=any($2::int[])
                  and r.run_group_id=any($3::text[]) and r.status='success' and r.run_kind='historical_backfill'
                  and ($4::timestamptz is null or coalesce(r.cutoff_bar_end, r.bar_until) >= $4)
                  and ($5::timestamptz is null or coalesce(r.cutoff_bar_end, r.bar_until) <= $5)
                  and ($6::timestamptz is null or (coalesce(r.cutoff_bar_end, r.bar_until), r.id) > ($6, $7))
                group by r.id, sym.code, sym.exchange order by cutoff_bar_end, r.id
                limit $8
            """
        if conn is not None:
            rows = await conn.fetch(query, symbols, level_ids, list(run_groups), start, end, cursor[0] if cursor else None, cursor[1] if cursor else 0, batch_size)
        else:
            async with self.pool.acquire() as acquired:
                rows = await acquired.fetch(query, symbols, level_ids, list(run_groups), start, end, cursor[0] if cursor else None, cursor[1] if cursor else 0, batch_size)
        result = []
        for row in rows:
            item = dict(row)
            if isinstance(item["signals"], str):
                item["signals"] = json.loads(item["signals"])
            result.append(item)
        return result

    async def fetch_historical_runs_with_signals_paged(self, *, symbols: list[str], levels: tuple[str, ...], run_groups: tuple[str, ...], conn: asyncpg.Connection, start: datetime, end: datetime, batch_size: int = 2000) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Keyset-page direct intraday snapshots without silently truncating at LIMIT."""
        if batch_size < 1 or batch_size > 2000:
            raise ValueError("batch_size must be between 1 and 2000")
        start, end = utc_time(start), utc_time(end)
        rows, seen, cursor, pages, max_batch = [], set(), None, 0, 0
        while True:
            page = await self.fetch_historical_runs_with_signals(symbols=symbols, levels=levels, run_groups=run_groups, conn=conn, start=start, end=end, batch_size=batch_size, cursor=cursor)
            if not page:
                break
            pages += 1
            max_batch = max(max_batch, len(page))
            next_cursor = (utc_time(page[-1]["cutoff_bar_end"]), int(page[-1]["run_id"]))
            if cursor is not None and next_cursor <= cursor:
                raise RuntimeError("Non-advancing keyset cursor")
            for row in page:
                if row["run_id"] not in seen:
                    seen.add(row["run_id"])
                    rows.append(row)
            cursor = next_cursor
            if len(page) < batch_size:
                break
        rows.sort(key=lambda row: (utc_time(row["cutoff_bar_end"]), int(row["run_id"])))
        return rows, {"pages": pages, "runs": len(rows), "max_batch": max_batch, "batch_size": batch_size}

    async def fetch_historical_structure_runs(self, *, symbols: list[str], levels: tuple[str, ...], run_groups: tuple[str, ...], conn: asyncpg.Connection | None = None, start: datetime | None = None, end: datetime | None = None, batch_size: int = 5000, cursor: tuple[datetime, int] | None = None) -> list[dict[str, Any]]:
        """Fetch snapshot signal/stroke/center structure in set queries, including empty snapshots."""
        runs = await self.fetch_historical_runs_with_signals(symbols=symbols, levels=levels, run_groups=run_groups, conn=conn, start=start, end=end, batch_size=batch_size, cursor=cursor)
        if not runs:
            return []
        run_ids = [row["run_id"] for row in runs]
        if conn is not None:
            stroke_rows, center_rows = await conn.fetch("""
                select run_id, jsonb_agg(jsonb_build_object('direction', direction, 'start_ts', start_ts, 'end_ts', end_ts, 'start_price_x1000', start_price_x1000, 'end_price_x1000', end_price_x1000, 'is_confirmed', is_confirmed) order by seq) as items
                from chan_c_strokes where run_id=any($1::bigint[]) group by run_id
            """, run_ids), await conn.fetch("""
                select run_id, jsonb_agg(jsonb_build_object('start_ts', start_ts, 'end_ts', end_ts, 'low_x1000', low_x1000, 'high_x1000', high_x1000, 'is_confirmed', is_confirmed) order by seq) as items
                from chan_c_centers where run_id=any($1::bigint[]) group by run_id
            """, run_ids)
        else:
            async with self.pool.acquire() as acquired:
                stroke_rows, center_rows = await acquired.fetch("""
                    select run_id, jsonb_agg(jsonb_build_object('direction', direction, 'start_ts', start_ts, 'end_ts', end_ts, 'start_price_x1000', start_price_x1000, 'end_price_x1000', end_price_x1000, 'is_confirmed', is_confirmed) order by seq) as items
                    from chan_c_strokes where run_id=any($1::bigint[]) group by run_id
                """, run_ids), await acquired.fetch("""
                    select run_id, jsonb_agg(jsonb_build_object('start_ts', start_ts, 'end_ts', end_ts, 'low_x1000', low_x1000, 'high_x1000', high_x1000, 'is_confirmed', is_confirmed) order by seq) as items
                    from chan_c_centers where run_id=any($1::bigint[]) group by run_id
                """, run_ids)
        def normalize(rows):
            return {row["run_id"]: json.loads(row["items"]) if isinstance(row["items"], str) else row["items"] for row in rows}
        strokes, centers = normalize(stroke_rows), normalize(center_rows)
        for run in runs:
            run["strokes"] = strokes.get(run["run_id"], [])
            run["centers"] = centers.get(run["run_id"], [])
        return runs

    async def fetch_historical_structure_runs_paged(self, *, symbols: list[str], levels: tuple[str, ...], run_groups: tuple[str, ...], conn: asyncpg.Connection, start: datetime, end: datetime, batch_size: int = 2000) -> tuple[list[dict[str, Any]], dict[str, int]]:
        if batch_size < 1 or batch_size > 2000:
            raise ValueError("batch_size must be between 1 and 2000")
        all_runs, seen = [], set()
        cursor: tuple[datetime, int] | None = None
        pages, max_batch = 0, 0
        while True:
            page = await self.fetch_historical_structure_runs(symbols=symbols, levels=levels, run_groups=run_groups, conn=conn, start=start, end=end, batch_size=batch_size, cursor=cursor)
            if not page:
                break
            pages += 1; max_batch = max(max_batch, len(page))
            next_cursor = (page[-1]["cutoff_bar_end"], int(page[-1]["run_id"]))
            if cursor is not None and next_cursor <= cursor:
                raise RuntimeError("Non-advancing keyset cursor")
            for row in page:
                if row["run_id"] not in seen:
                    seen.add(row["run_id"]); all_runs.append(row)
            cursor = next_cursor
            if len(page) < batch_size:
                break
        all_runs.sort(key=lambda row: (row["cutoff_bar_end"], row["run_id"]))
        return all_runs, {"pages": pages, "runs": len(all_runs), "max_batch": max_batch, "batch_size": batch_size}

    async def fetch_complete_klines(self, *, symbols: list[str], levels: tuple[str, ...], start: datetime | None = None, end: datetime | None = None, conn: asyncpg.Connection | None = None) -> list[dict[str, Any]]:
        if start is None or end is None:
            raise ValueError("fetch_complete_klines requires explicit start and end")
        start, end = utc_time(start), utc_time(end)
        if not symbols or not levels:
            return []
        level_ids = [LEVEL_TO_DB[level] for level in levels]
        if conn is not None:
            symbol_rows = await conn.fetch("select id, (code || '.' || exchange) as symbol from symbols where (code || '.' || exchange)=any($1::text[])", symbols)
            ids_to_symbols = {row["id"]: row["symbol"] for row in symbol_rows}
            if not ids_to_symbols:
                return []
            # This predicates the existing (symbol_id, timeframe, ts) index directly.
            rows = await conn.fetch("""
                select symbol_id, timeframe, ts, open_x1000, high_x1000, low_x1000, close_x1000, volume, is_complete
                from klines
                where symbol_id=any($1::int[]) and timeframe=any($2::int[]) and is_complete=true
                  and ts >= $3::timestamptz and ts <= $4::timestamptz
                order by symbol_id, timeframe, ts
            """, list(ids_to_symbols), level_ids, start, end)
        else:
            async with self.pool.acquire() as acquired:
                symbol_rows = await acquired.fetch("select id, (code || '.' || exchange) as symbol from symbols where (code || '.' || exchange)=any($1::text[])", symbols)
                ids_to_symbols = {row["id"]: row["symbol"] for row in symbol_rows}
                if not ids_to_symbols:
                    return []
                rows = await acquired.fetch("""
                    select symbol_id, timeframe, ts, open_x1000, high_x1000, low_x1000, close_x1000, volume, is_complete
                    from klines
                    where symbol_id=any($1::int[]) and timeframe=any($2::int[]) and is_complete=true
                      and ts >= $3::timestamptz and ts <= $4::timestamptz
                    order by symbol_id, timeframe, ts
                """, list(ids_to_symbols), level_ids, start, end)
        return [{**dict(row), "symbol": ids_to_symbols[row["symbol_id"]], "ts": utc_time(row["ts"])} for row in rows]

    async def filter_symbols_with_weekly_context(
        self,
        symbols: list[SymbolInfo],
        *,
        weekly_b2_types: list[str],
        end_time: datetime,
    ) -> list[SymbolInfo]:
        if not symbols:
            return []
        symbol_ids = [symbol.symbol_id for symbol in symbols]
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                with selected as (
                    select unnest($1::bigint[]) as symbol_id
                ),
                weekly as (
                    select distinct r.symbol_id, s.extra->>'bsp_type' as bsp_type
                    from selected chosen
                    join chan_c_runs r
                      on r.symbol_id = chosen.symbol_id
                    join chan_c_signals s
                      on s.run_id = r.id
                    where r.chan_level = $2
                      and r.status = 'success'
                      and r.bar_until <= $3
                      and s.mode = 2
                      and coalesce(s.extra->>'side', '') = 'buy'
                )
                select symbol_id
                from weekly
                group by symbol_id
                having bool_or(bsp_type = '1')
                   and bool_or(bsp_type = any($4::text[]))
                """,
                symbol_ids,
                LEVEL_TO_DB["1w"],
                end_time,
                weekly_b2_types,
            )
        allowed_ids = {int(row["symbol_id"]) for row in rows}
        return [symbol for symbol in symbols if symbol.symbol_id in allowed_ids]

    async def prime_symbol_cache(
        self,
        symbol_id: int,
        *,
        levels: tuple[str, ...] = ("5f", "30f", "1d", "1w", "1m"),
        modes: tuple[str, ...] = ("predictive", "confirmed"),
    ) -> None:
        for level in levels:
            for mode in modes:
                await self._load_historical_heads(symbol_id, level, mode)

    def release_symbol_cache(self, symbol_id: int) -> None:
        head_keys = [key for key in self._historical_heads if key.symbol_id == symbol_id]
        for key in head_keys:
            self._historical_heads.pop(key, None)
            self._historical_head_untils.pop(key, None)
        run_ids = self._run_ids_by_symbol.pop(symbol_id, set())
        for run_id in run_ids:
            for cache in (self._signals_cache, self._strokes_cache, self._segments_cache, self._centers_cache):
                cache.pop((run_id, 1), None)
                cache.pop((run_id, 2), None)

    async def get_current_head(self, symbol_id: int, level: str, mode: str = "predictive") -> PublishedHead | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select id, run_id, snapshot_version, base_from_bar_end, base_to_bar_end, published_at
                from scheme2_chan_c_published_heads
                where symbol_id = $1
                  and chan_level = $2
                  and mode = $3
                  and base_timeframe = $2
                  and status = 'published'
                limit 1
                """,
                symbol_id,
                LEVEL_TO_DB[level],
                mode,
            )
        if row is None or row["run_id"] is None:
            return None
        return PublishedHead(
            run_id=int(row["run_id"]),
            snapshot_version=str(row["snapshot_version"] or ""),
            bar_from=row["base_from_bar_end"],
            bar_until=row["base_to_bar_end"],
            published_at=row["published_at"],
        )

    async def get_historical_head(
        self,
        symbol_id: int,
        level: str,
        as_of_time: datetime,
        *,
        mode: str = "predictive",
        run_kind: str | None = None,
        run_group_id: str | None = None,
        allow_legacy_mode_fallback: bool = True,
    ) -> PublishedHead | None:
        lookup = await self.get_historical_run_lookup(
            symbol_id,
            level,
            mode,
            as_of_time,
            run_kind=run_kind,
            run_group_id=run_group_id,
            allow_legacy_mode_fallback=allow_legacy_mode_fallback,
        )
        return lookup.selected

    async def get_historical_run_lookup(
        self,
        symbol_id: int,
        level: str,
        mode: str,
        as_of_time: datetime,
        *,
        run_kind: str | None = None,
        run_group_id: str | None = None,
        allow_legacy_mode_fallback: bool = True,
    ) -> HistoricalRunLookup:
        key = HistoricalHeadFilter(
            symbol_id=symbol_id,
            level=level,
            mode=mode,
            run_kind=run_kind,
            run_group_id=run_group_id,
            allow_legacy_mode_fallback=allow_legacy_mode_fallback,
        )
        heads = await self._load_historical_heads(
            symbol_id,
            level,
            mode,
            run_kind=run_kind,
            run_group_id=run_group_id,
            allow_legacy_mode_fallback=allow_legacy_mode_fallback,
        )
        if not heads:
            return HistoricalRunLookup(selected=None, nearest_before=None, nearest_after=None, run_count=0)
        untils = self._historical_head_untils[key]
        index = bisect_right(untils, as_of_time) - 1
        selected = heads[index] if index >= 0 else None
        nearest_before = selected
        nearest_after = heads[index + 1] if index + 1 < len(heads) else None
        return HistoricalRunLookup(
            selected=selected,
            nearest_before=nearest_before,
            nearest_after=nearest_after,
            run_count=len(heads),
        )

    async def get_head(
        self,
        symbol_id: int,
        level: str,
        *,
        mode: str = "predictive",
        as_of_time: datetime | None = None,
        run_kind: str | None = None,
        run_group_id: str | None = None,
        allow_legacy_mode_fallback: bool = True,
    ) -> PublishedHead | None:
        if as_of_time is None:
            return await self.get_current_head(symbol_id, level, mode=mode)
        lookup = await self.get_historical_run_lookup(
            symbol_id,
            level,
            mode,
            as_of_time,
            run_kind=run_kind,
            run_group_id=run_group_id,
            allow_legacy_mode_fallback=allow_legacy_mode_fallback,
        )
        return lookup.selected

    async def list_historical_heads(
        self,
        symbol_id: int,
        level: str,
        *,
        mode: str = "predictive",
        end_time: datetime | None = None,
        run_kind: str | None = None,
        run_group_id: str | None = None,
        allow_legacy_mode_fallback: bool = True,
    ) -> list[PublishedHead]:
        heads = list(
            await self._load_historical_heads(
                symbol_id,
                level,
                mode,
                run_kind=run_kind,
                run_group_id=run_group_id,
                allow_legacy_mode_fallback=allow_legacy_mode_fallback,
            )
        )
        if end_time is None:
            return heads
        return [head for head in heads if head.bar_until <= end_time]

    async def get_signals(
        self,
        symbol_id: int,
        level: str,
        *,
        mode: str = "predictive",
        as_of_time: datetime | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        run_kind: str | None = None,
        run_group_id: str | None = None,
        allow_legacy_mode_fallback: bool = True,
    ) -> list[ChanSignal]:
        head = await self.get_head(
            symbol_id,
            level,
            mode=mode,
            as_of_time=as_of_time,
            run_kind=run_kind,
            run_group_id=run_group_id,
            allow_legacy_mode_fallback=allow_legacy_mode_fallback,
        )
        if head is None:
            return []
        mode_id = 2 if mode == "predictive" else 1
        cache_key = (head.run_id, mode_id)
        signals = self._signals_cache.get(cache_key)
        if signals is None:
            async with self.pool.acquire() as conn:
                rows = await self._fetch_mode_rows(
                    conn,
                    """
                    select id, ts, coalesce(base_ts, ts) as base_ts, base_seq, price_x1000, signal_type,
                           is_confirmed, extra
                    from chan_c_signals
                    where run_id = $1
                      and mode = $2
                    order by coalesce(base_ts, ts), id
                    """,
                    head.run_id,
                    mode,
                )
            signals = [self._signal_from_row(level, mode, head, row) for row in rows]
            self._signals_cache[cache_key] = signals
            self._track_run_id(symbol_id, head.run_id)
        return [
            signal for signal in signals
            if (start is None or signal.base_time >= start)
            and (end is None or signal.base_time <= end)
        ]

    async def get_strokes(
        self,
        symbol_id: int,
        level: str,
        *,
        mode: str = "predictive",
        as_of_time: datetime | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        run_kind: str | None = None,
        run_group_id: str | None = None,
        allow_legacy_mode_fallback: bool = True,
    ) -> list[ChanStroke]:
        head = await self.get_head(
            symbol_id,
            level,
            mode=mode,
            as_of_time=as_of_time,
            run_kind=run_kind,
            run_group_id=run_group_id,
            allow_legacy_mode_fallback=allow_legacy_mode_fallback,
        )
        if head is None:
            return []
        mode_id = 2 if mode == "predictive" else 1
        cache_key = (head.run_id, mode_id)
        strokes = self._strokes_cache.get(cache_key)
        if strokes is None:
            async with self.pool.acquire() as conn:
                rows = await self._fetch_mode_rows(
                    conn,
                    """
                    select seq, direction, start_ts, end_ts,
                           coalesce(begin_base_ts, start_ts) as begin_base_ts,
                           coalesce(end_base_ts, end_ts) as end_base_ts,
                           start_price_x1000, end_price_x1000, is_confirmed
                    from chan_c_strokes
                    where run_id = $1
                      and mode = $2
                    order by coalesce(begin_base_ts, start_ts), seq
                    """,
                    head.run_id,
                    mode,
                )
            strokes = [
                ChanStroke(
                    seq=int(row["seq"]),
                    level=level,
                    mode=mode,
                    direction="up" if int(row["direction"]) > 0 else "down",
                    start_time=row["start_ts"],
                    end_time=row["end_ts"],
                    start_price=row["start_price_x1000"] / 1000,
                    end_price=row["end_price_x1000"] / 1000,
                    begin_base_time=row["begin_base_ts"],
                    end_base_time=row["end_base_ts"],
                    confirmed=bool(row["is_confirmed"]),
                )
                for row in rows
            ]
            self._strokes_cache[cache_key] = strokes
            self._track_run_id(symbol_id, head.run_id)
        return [
            stroke for stroke in strokes
            if (start is None or stroke.end_base_time >= start)
            and (end is None or stroke.begin_base_time <= end)
        ]

    async def get_segments(
        self,
        symbol_id: int,
        level: str,
        *,
        mode: str = "predictive",
        as_of_time: datetime | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        run_kind: str | None = None,
        run_group_id: str | None = None,
        allow_legacy_mode_fallback: bool = True,
    ) -> list[ChanStroke]:
        head = await self.get_head(
            symbol_id,
            level,
            mode=mode,
            as_of_time=as_of_time,
            run_kind=run_kind,
            run_group_id=run_group_id,
            allow_legacy_mode_fallback=allow_legacy_mode_fallback,
        )
        if head is None:
            return []
        mode_id = 2 if mode == "predictive" else 1
        cache_key = (head.run_id, mode_id)
        segments = self._segments_cache.get(cache_key)
        if segments is None:
            async with self.pool.acquire() as conn:
                rows = await self._fetch_mode_rows(
                    conn,
                    """
                    select seq, direction, start_ts, end_ts,
                           coalesce(begin_base_ts, start_ts) as begin_base_ts,
                           coalesce(end_base_ts, end_ts) as end_base_ts,
                           start_price_x1000, end_price_x1000, is_confirmed
                    from chan_c_segments
                    where run_id = $1
                      and mode = $2
                    order by coalesce(begin_base_ts, start_ts), seq
                    """,
                    head.run_id,
                    mode,
                )
            segments = [
                ChanStroke(
                    seq=int(row["seq"]),
                    level=level,
                    mode=mode,
                    direction="up" if int(row["direction"]) > 0 else "down",
                    start_time=row["start_ts"],
                    end_time=row["end_ts"],
                    start_price=row["start_price_x1000"] / 1000,
                    end_price=row["end_price_x1000"] / 1000,
                    begin_base_time=row["begin_base_ts"],
                    end_base_time=row["end_base_ts"],
                    confirmed=bool(row["is_confirmed"]),
                )
                for row in rows
            ]
            self._segments_cache[cache_key] = segments
            self._track_run_id(symbol_id, head.run_id)
        return [
            segment for segment in segments
            if (start is None or segment.end_base_time >= start)
            and (end is None or segment.begin_base_time <= end)
        ]

    async def get_centers(
        self,
        symbol_id: int,
        level: str,
        *,
        mode: str = "predictive",
        as_of_time: datetime | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        run_kind: str | None = None,
        run_group_id: str | None = None,
        allow_legacy_mode_fallback: bool = True,
    ) -> list[ChanCenter]:
        head = await self.get_head(
            symbol_id,
            level,
            mode=mode,
            as_of_time=as_of_time,
            run_kind=run_kind,
            run_group_id=run_group_id,
            allow_legacy_mode_fallback=allow_legacy_mode_fallback,
        )
        if head is None:
            return []
        mode_id = 2 if mode == "predictive" else 1
        cache_key = (head.run_id, mode_id)
        centers = self._centers_cache.get(cache_key)
        if centers is None:
            async with self.pool.acquire() as conn:
                rows = await self._fetch_mode_rows(
                    conn,
                    """
                    select seq, start_ts, end_ts,
                           coalesce(begin_base_ts, start_ts) as begin_base_ts,
                           coalesce(end_base_ts, end_ts) as end_base_ts,
                           low_x1000, high_x1000, is_confirmed
                    from chan_c_centers
                    where run_id = $1
                      and mode = $2
                    order by coalesce(begin_base_ts, start_ts), seq
                    """,
                    head.run_id,
                    mode,
                )
            centers = [
                ChanCenter(
                    seq=int(row["seq"]),
                    level=level,
                    mode=mode,
                    start_time=row["start_ts"],
                    end_time=row["end_ts"],
                    low=row["low_x1000"] / 1000,
                    high=row["high_x1000"] / 1000,
                    confirmed=bool(row["is_confirmed"]),
                    begin_base_time=row["begin_base_ts"],
                    end_base_time=row["end_base_ts"],
                )
                for row in rows
            ]
            self._centers_cache[cache_key] = centers
            self._track_run_id(symbol_id, head.run_id)
        return [
            center for center in centers
            if (start is None or (center.end_base_time or center.end_time) >= start)
            and (end is None or (center.begin_base_time or center.start_time) <= end)
        ]

    async def _load_historical_heads(
        self,
        symbol_id: int,
        level: str,
        mode: str,
        *,
        run_kind: str | None = None,
        run_group_id: str | None = None,
        allow_legacy_mode_fallback: bool = True,
    ) -> list[PublishedHead]:
        key = HistoricalHeadFilter(
            symbol_id=symbol_id,
            level=level,
            mode=mode,
            run_kind=run_kind,
            run_group_id=run_group_id,
            allow_legacy_mode_fallback=allow_legacy_mode_fallback,
        )
        cached = self._historical_heads.get(key)
        if cached is not None:
            return cached
        mode_id = MODE_TO_DB[mode]
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select id, snapshot_version, bar_from, bar_until, computed_at
                from chan_c_runs
                where symbol_id = $1
                  and chan_level = $2
                  and mode = $3
                  and status = 'success'
                  and ($4::text is null or coalesce(run_kind, 'published') = $4)
                  and ($5::text is null or run_group_id = $5)
                order by bar_until, computed_at, id
                """,
                symbol_id,
                LEVEL_TO_DB[level],
                mode_id,
                run_kind,
                run_group_id,
            )
            if not rows and mode_id != LEGACY_SHARED_MODE and allow_legacy_mode_fallback:
                rows = await conn.fetch(
                    """
                    select id, snapshot_version, bar_from, bar_until, computed_at
                    from chan_c_runs
                    where symbol_id = $1
                      and chan_level = $2
                      and mode = $3
                      and status = 'success'
                      and ($4::text is null or coalesce(run_kind, 'published') = $4)
                      and ($5::text is null or run_group_id = $5)
                    order by bar_until, computed_at, id
                    """,
                    symbol_id,
                    LEVEL_TO_DB[level],
                    LEGACY_SHARED_MODE,
                    run_kind,
                    run_group_id,
                )
        heads = [
            PublishedHead(
                run_id=int(row["id"]),
                snapshot_version=str(row["snapshot_version"] or ""),
                bar_from=row["bar_from"],
                bar_until=row["bar_until"],
                published_at=row["computed_at"],
            )
            for row in rows
        ]
        self._historical_heads[key] = heads
        self._historical_head_untils[key] = [head.bar_until for head in heads]
        return heads

    def _track_run_id(self, symbol_id: int, run_id: int) -> None:
        self._run_ids_by_symbol.setdefault(symbol_id, set()).add(run_id)

    async def _fetch_mode_rows(
        self,
        conn: asyncpg.Connection,
        query: str,
        run_id: int,
        mode: str,
    ) -> list[asyncpg.Record]:
        mode_id = MODE_TO_DB[mode]
        rows = await conn.fetch(query, run_id, mode_id)
        if rows or mode_id == LEGACY_SHARED_MODE:
            return rows
        return await conn.fetch(query, run_id, LEGACY_SHARED_MODE)

    def _signal_from_row(self, level: str, mode: str, head: PublishedHead, row: asyncpg.Record) -> ChanSignal:
        extra = row["extra"]
        if isinstance(extra, str):
            extra = json.loads(extra)
        extra = extra if isinstance(extra, dict) else {}
        return ChanSignal(
            signal_id=int(row["id"]),
            level=level,
            mode=mode,
            point_time=row["base_ts"],
            base_time=row["base_ts"],
            base_seq=row["base_seq"],
            price=row["price_x1000"] / 1000,
            signal_type=str(row["signal_type"]),
            side=extra.get("side"),
            bsp_type=extra.get("bsp_type"),
            confirmed=bool(row["is_confirmed"]),
            features=extra.get("features") or {},
            run_id=head.run_id,
            snapshot_version=head.snapshot_version,
        )
