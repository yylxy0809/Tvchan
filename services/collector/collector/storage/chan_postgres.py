from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from collector.storage.chan_state import refresh_symbol_chan_states
from collector.storage.postgres import price_to_x1000, timeframe_to_db_code


MODE_CODES = {
    "confirmed": 1,
    "predictive": 2,
}

DIRECTION_CODES = {
    "up": 1,
    "down": -1,
}


def mode_to_code(value: str) -> int:
    return MODE_CODES.get(value, 0)


def direction_to_code(value: str) -> int:
    return DIRECTION_CODES.get(value.lower(), 0)


def epoch_to_datetime(value: int | float) -> datetime:
    return datetime.fromtimestamp(value, tz=UTC)


class PostgresChanWriter:
    def __init__(
        self,
        database_url: str,
        *,
        pool_min_size: int | None = None,
        pool_max_size: int | None = None,
    ) -> None:
        self.database_url = database_url
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self._pool = None

    async def __aenter__(self) -> "PostgresChanWriter":
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "asyncpg is required for chan persistence. Install collector requirements."
            ) from exc
        kwargs = {}
        if self.pool_min_size is not None:
            kwargs["min_size"] = self.pool_min_size
        if self.pool_max_size is not None:
            kwargs["max_size"] = self.pool_max_size
        self._pool = await asyncpg.create_pool(self.database_url, **kwargs)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def replace_analysis(
        self,
        *,
        symbol: str,
        level: str,
        modes: list[str],
        bar_from: datetime,
        bar_until: datetime,
        bar_count: int,
        response: dict[str, Any],
    ) -> dict[str, int]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            symbol_id = await conn.fetchval(
                """
                select id
                from symbols
                where code = split_part($1, '.', 1)
                  and exchange = split_part($1, '.', 2)
                """,
                symbol,
            )
            if symbol_id is None:
                raise RuntimeError(f"Unknown symbol in database: {symbol}")

            level_code = timeframe_to_db_code(level)
            mode_codes = [mode_to_code(mode) for mode in modes]
            snapshot_version = str(response.get("snapshot_version") or "")
            base_timeframe_code = timeframe_to_db_code("5f")
            run_id = await conn.fetchval(
                """
                insert into chan_runs (
                    symbol_id,
                    chan_level,
                    mode,
                    input_signature,
                    config_hash,
                    bar_from,
                    bar_until,
                    bar_count,
                    status,
                    snapshot_version,
                    computed_at
                )
                values ($1, $2, 0, $3, $4, $5, $6, $7, 'running', $8, now())
                returning id
                """,
                symbol_id,
                level_code,
                f"{symbol}:{level}:{int(bar_until.timestamp())}",
                "module-b:chan.py-default",
                bar_from,
                bar_until,
                bar_count,
                snapshot_version,
            )

            try:
                await self._upsert_published_heads(
                    conn,
                    symbol_id=symbol_id,
                    level_code=level_code,
                    modes=modes,
                    base_timeframe_code=base_timeframe_code,
                    bar_from=bar_from,
                    bar_until=bar_until,
                    bar_count=bar_count,
                    snapshot_version=snapshot_version,
                    run_id=run_id,
                    status="staged",
                    last_error=None,
                )
                async with conn.transaction():
                    for table in (
                        "chan_strokes",
                        "chan_segments",
                        "chan_centers",
                        "chan_signals",
                    ):
                        await conn.execute(
                            f"""
                            delete from {table}
                            where symbol_id = $1
                              and chan_level = $2
                              and mode = any($3::smallint[])
                            """,
                            symbol_id,
                            level_code,
                            mode_codes,
                        )

                    stroke_count = await self._insert_stroke_like(
                        conn,
                        "chan_strokes",
                        symbol_id,
                        level_code,
                        run_id,
                        response.get("strokes", []),
                    )
                    segment_count = await self._insert_stroke_like(
                        conn,
                        "chan_segments",
                        symbol_id,
                        level_code,
                        run_id,
                        response.get("segments", []),
                    )
                    center_count = await self._insert_centers(
                        conn,
                        symbol_id,
                        level_code,
                        run_id,
                        response.get("centers", []),
                    )
                    signal_count = await self._insert_signals(
                        conn,
                        symbol_id,
                        level_code,
                        run_id,
                        response.get("signals", []),
                    )
                await conn.execute(
                    """
                    update chan_runs
                    set status = 'success',
                        finished_at = now()
                    where id = $1
                    """,
                    run_id,
                )
                await self._upsert_published_heads(
                    conn,
                    symbol_id=symbol_id,
                    level_code=level_code,
                    modes=modes,
                    base_timeframe_code=base_timeframe_code,
                    bar_from=bar_from,
                    bar_until=bar_until,
                    bar_count=bar_count,
                    snapshot_version=snapshot_version,
                    run_id=run_id,
                    status="published",
                    last_error=None,
                )
                await refresh_symbol_chan_states(
                    conn,
                    symbol_id=symbol_id,
                    snapshot_version=snapshot_version or None,
                )
            except Exception as exc:
                await conn.execute(
                    """
                    update chan_runs
                    set status = 'failed',
                        finished_at = now(),
                        error_message = $2
                    where id = $1
                    """,
                    run_id,
                    str(exc)[:2000],
                )
                await self._upsert_published_heads(
                    conn,
                    symbol_id=symbol_id,
                    level_code=level_code,
                    modes=modes,
                    base_timeframe_code=base_timeframe_code,
                    bar_from=bar_from,
                    bar_until=bar_until,
                    bar_count=bar_count,
                    snapshot_version=snapshot_version,
                    run_id=run_id,
                    status="failed",
                    last_error=str(exc)[:2000],
                )
                raise

        return {
            "strokes": stroke_count,
            "segments": segment_count,
            "centers": center_count,
            "signals": signal_count,
        }

    async def _upsert_published_heads(
        self,
        conn,
        *,
        symbol_id: int,
        level_code: int,
        modes: list[str],
        base_timeframe_code: int,
        bar_from: datetime,
        bar_until: datetime,
        bar_count: int,
        snapshot_version: str,
        run_id: int,
        status: str,
        last_error: str | None,
    ) -> None:
        rows = [
            (
                symbol_id,
                level_code,
                mode,
                base_timeframe_code,
                bar_from,
                bar_until,
                bar_count,
                snapshot_version,
                status,
                run_id,
                last_error,
            )
            for mode in modes
        ]
        if not rows:
            return
        await conn.executemany(
            """
            insert into scheme2_chan_published_heads (
                symbol_id,
                chan_level,
                mode,
                base_timeframe,
                base_from_bar_end,
                base_to_bar_end,
                bar_count,
                snapshot_version,
                status,
                run_id,
                published_at,
                updated_at,
                last_error
            )
            values (
                $1, $2, $3::varchar, $4, $5, $6, $7, $8::varchar, $9::varchar, $10,
                case when $9::text = 'published' then now() else null end,
                now(),
                $11
            )
            on conflict (symbol_id, chan_level, mode, base_timeframe)
            do update
            set base_from_bar_end = excluded.base_from_bar_end,
                base_to_bar_end = excluded.base_to_bar_end,
                bar_count = excluded.bar_count,
                snapshot_version = excluded.snapshot_version,
                status = excluded.status,
                run_id = excluded.run_id,
                published_at = case
                    when excluded.status = 'published' then now()
                    else scheme2_chan_published_heads.published_at
                end,
                updated_at = now(),
                last_error = excluded.last_error
            """,
            rows,
        )

    async def _insert_stroke_like(
        self,
        conn,
        table: str,
        symbol_id: int,
        level_code: int,
        run_id: int,
        items: list[dict[str, Any]],
    ) -> int:
        rows = [
            (
                symbol_id,
                level_code,
                mode_to_code(item.get("mode", "")),
                run_id,
                index,
                epoch_to_datetime(item["start"]["time"]),
                epoch_to_datetime(item["end"]["time"]),
                price_to_x1000(item["start"]["price"]),
                price_to_x1000(item["end"]["price"]),
                direction_to_code(item.get("direction", "")),
                bool(item.get("confirmed")),
                epoch_to_datetime(item.get("begin_base_ts") or item["start"]["time"]),
                epoch_to_datetime(item.get("end_base_ts") or item["end"]["time"]),
                item.get("begin_base_seq"),
                item.get("end_base_seq"),
                json.dumps(
                    {
                        "id": item.get("id"),
                        "side": item.get("side"),
                        "bsp_type": item.get("bsp_type"),
                        "features": item.get("features") or {},
                    },
                    ensure_ascii=False,
                ),
            )
            for index, item in enumerate(items)
        ]
        if not rows:
            return 0
        await conn.executemany(
            f"""
            insert into {table} (
                symbol_id,
                chan_level,
                mode,
                run_id,
                seq,
                start_ts,
                end_ts,
                start_price_x1000,
                end_price_x1000,
                direction,
                is_confirmed,
                begin_base_ts,
                end_base_ts,
                begin_base_seq,
                end_base_seq,
                extra
            )
            values (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16::jsonb
            )
            """,
            rows,
        )
        return len(rows)

    async def _insert_centers(
        self,
        conn,
        symbol_id: int,
        level_code: int,
        run_id: int,
        items: list[dict[str, Any]],
    ) -> int:
        rows = [
            (
                symbol_id,
                level_code,
                mode_to_code(item.get("mode", "")),
                run_id,
                index,
                epoch_to_datetime(item["start_time"]),
                epoch_to_datetime(item["end_time"]),
                price_to_x1000(item["low"]),
                price_to_x1000(item["high"]),
                bool(item.get("confirmed")),
                epoch_to_datetime(item.get("begin_base_ts") or item["start_time"]),
                epoch_to_datetime(item.get("end_base_ts") or item["end_time"]),
                item.get("begin_base_seq"),
                item.get("end_base_seq"),
                json.dumps({"id": item.get("id")}, ensure_ascii=False),
            )
            for index, item in enumerate(items)
        ]
        if not rows:
            return 0
        await conn.executemany(
            """
            insert into chan_centers (
                symbol_id,
                chan_level,
                mode,
                run_id,
                seq,
                start_ts,
                end_ts,
                low_x1000,
                high_x1000,
                is_confirmed,
                begin_base_ts,
                end_base_ts,
                begin_base_seq,
                end_base_seq,
                extra
            )
            values (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15::jsonb
            )
            """,
            rows,
        )
        return len(rows)

    async def _insert_signals(
        self,
        conn,
        symbol_id: int,
        level_code: int,
        run_id: int,
        items: list[dict[str, Any]],
    ) -> int:
        rows = [
            (
                symbol_id,
                level_code,
                mode_to_code(item.get("mode", "")),
                run_id,
                epoch_to_datetime(item["time"]),
                price_to_x1000(item["price"]),
                item.get("signal_type", ""),
                bool(item.get("confirmed")),
                epoch_to_datetime(item.get("base_ts") or item["time"]),
                item.get("base_seq"),
                json.dumps(
                    {
                        "id": item.get("id"),
                        "side": item.get("side"),
                        "bsp_type": item.get("bsp_type"),
                        "features": item.get("features") or {},
                    },
                    ensure_ascii=False,
                ),
            )
            for item in items
        ]
        if not rows:
            return 0
        await conn.executemany(
            """
            insert into chan_signals (
                symbol_id,
                chan_level,
                mode,
                run_id,
                ts,
                price_x1000,
                signal_type,
                is_confirmed,
                base_ts,
                base_seq,
                extra
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
            """,
            rows,
        )
        return len(rows)
