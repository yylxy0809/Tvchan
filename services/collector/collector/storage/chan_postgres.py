from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from collector.storage.postgres import price_to_x1000, timeframe_to_db_code


MODE_CODES = {
    "confirmed": 1,
    "predictive": 2,
}
CODE_TO_MODE = {value: key for key, value in MODE_CODES.items()}

DIRECTION_CODES = {
    "up": 1,
    "down": -1,
}
CODE_TO_DIRECTION = {value: key for key, value in DIRECTION_CODES.items()}

MODULE_C_CHAN_TABLES = {
    "runs": "chan_c_runs",
    "strokes": "chan_c_strokes",
    "segments": "chan_c_segments",
    "centers": "chan_c_centers",
    "signals": "chan_c_signals",
    "published_heads": "scheme2_chan_c_published_heads",
    "recompute_watermarks": "scheme2_chan_c_recompute_watermarks",
}


def mode_to_code(value: str) -> int:
    return MODE_CODES.get(value, 0)


def direction_to_code(value: str) -> int:
    return DIRECTION_CODES.get(value.lower(), 0)


def epoch_to_datetime(value: int | float) -> datetime:
    return datetime.fromtimestamp(value, tz=UTC)


def _filter_tail_response(
    response: dict[str, Any],
    *,
    level: str,
    modes: list[str],
    anchor_bar_end: datetime,
) -> dict[str, list[dict[str, Any]]]:
    anchor_epoch = int(anchor_bar_end.timestamp())
    mode_set = set(modes)

    def item_matches(item: dict[str, Any]) -> bool:
        item_level = str(item.get("level") or level)
        item_mode = str(item.get("mode") or "confirmed")
        return item_level == level and item_mode in mode_set

    return {
        "strokes": [
            item
            for item in response.get("strokes", [])
            if isinstance(item, dict)
            and item_matches(item)
            and _line_end_epoch(item) > anchor_epoch
        ],
        "segments": [
            item
            for item in response.get("segments", [])
            if isinstance(item, dict)
            and item_matches(item)
            and _line_end_epoch(item) > anchor_epoch
        ],
        "centers": [
            item
            for item in response.get("centers", [])
            if isinstance(item, dict)
            and item_matches(item)
            and _range_end_epoch(item) > anchor_epoch
        ],
        "signals": [
            item
            for item in response.get("signals", [])
            if isinstance(item, dict)
            and item_matches(item)
            and _point_epoch(item) > anchor_epoch
        ],
    }


def _order_lines(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            _line_begin_epoch(item),
            _line_end_epoch(item),
            int(item.get("seq") or 0),
        ),
    )


def _order_ranges(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            _range_begin_epoch(item),
            _range_end_epoch(item),
            int(item.get("seq") or 0),
        ),
    )


def _order_points(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items, key=lambda item: (_point_epoch(item), int(item.get("seq") or 0))
    )


def _line_begin_epoch(item: dict[str, Any]) -> int:
    start = item.get("start")
    if not isinstance(start, dict):
        start = {}
    return int(
        item.get("begin_base_ts") or start.get("base_ts") or start.get("time") or 0
    )


def _line_end_epoch(item: dict[str, Any]) -> int:
    end = item.get("end")
    if not isinstance(end, dict):
        end = {}
    return int(item.get("end_base_ts") or end.get("base_ts") or end.get("time") or 0)


def _range_begin_epoch(item: dict[str, Any]) -> int:
    return int(item.get("begin_base_ts") or item.get("start_time") or 0)


def _range_end_epoch(item: dict[str, Any]) -> int:
    return int(item.get("end_base_ts") or item.get("end_time") or 0)


def _point_epoch(item: dict[str, Any]) -> int:
    return int(item.get("base_ts") or item.get("time") or 0)


def _read_extra(extra: Any) -> dict[str, Any]:
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except json.JSONDecodeError:
            return {}
    return extra if isinstance(extra, dict) else {}


class StaleChanHeadError(RuntimeError):
    pass


class PostgresChanWriter:
    def __init__(
        self,
        database_url: str,
        *,
        pool_min_size: int | None = None,
        pool_max_size: int | None = None,
        tables: dict[str, str] | None = None,
        run_config_hash: str = "module-c:default",
        tail_config_hash: str = "module-c:stream-v1",
        native_base_timeframe: bool = False,
        publication_profile: str = "online",
        run_group_id: str | None = None,
        publication_source: str = "collector",
        run_kind: str = "online",
        worker_id: str | None = None,
        claim_token: str | None = None,
    ) -> None:
        self.database_url = database_url
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.tables = {**MODULE_C_CHAN_TABLES, **(tables or {})}
        self.run_config_hash = run_config_hash
        self.tail_config_hash = tail_config_hash
        self.native_base_timeframe = native_base_timeframe
        if publication_profile not in {"baseline", "online", "historical_replay"}:
            raise ValueError(f"Unsupported publication profile: {publication_profile}")
        self.publication_profile = publication_profile
        self.run_group_id = run_group_id
        self.publication_source = publication_source
        self.run_kind = run_kind
        self.worker_id = worker_id
        self.claim_token = claim_token
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
            snapshot_version = str(response.get("snapshot_version") or "")
            base_timeframe_code = timeframe_to_db_code(
                level if self.native_base_timeframe else "5f"
            )
            runs_table = self.tables["runs"]
            run_id = await conn.fetchval(
                f"""
                insert into {runs_table} (
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
                    computed_at,
                    run_kind,
                    run_group_id,
                    cutoff_bar_end
                )
                values ($1, $2, 0, $3, $4, $5, $6, $7, 'running', $8, now(), $9, $10, $6)
                returning id
                """,
                symbol_id,
                level_code,
                f"{symbol}:{level}:{int(bar_until.timestamp())}",
                self.run_config_hash,
                bar_from,
                bar_until,
                bar_count,
                snapshot_version,
                self.run_kind,
                self.run_group_id,
            )

            try:
                async with conn.transaction():
                    stroke_count = await self._insert_stroke_like(
                        conn,
                        self.tables["strokes"],
                        symbol_id,
                        level_code,
                        run_id,
                        response.get("strokes", []),
                    )
                    segment_count = await self._insert_stroke_like(
                        conn,
                        self.tables["segments"],
                        symbol_id,
                        level_code,
                        run_id,
                        response.get("segments", []),
                    )
                    center_count = await self._insert_centers(
                        conn,
                        self.tables["centers"],
                        symbol_id,
                        level_code,
                        run_id,
                        response.get("centers", []),
                    )
                    signal_count = await self._insert_signals(
                        conn,
                        self.tables["signals"],
                        symbol_id,
                        level_code,
                        run_id,
                        response.get("signals", []),
                    )
                    await conn.execute(
                        f"""
                        update {runs_table}
                        set status = 'success', finished_at = now()
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
                    await self._upsert_recompute_watermarks(
                        conn,
                        symbol_id=symbol_id,
                        level_code=level_code,
                        modes=modes,
                        base_timeframe_code=base_timeframe_code,
                        last_computed_bar_end=bar_until,
                        dirty_from_bar_end=None,
                        last_error=None,
                    )
            except Exception as exc:
                await conn.execute(
                    f"""
                    update {runs_table}
                    set status = 'failed',
                        finished_at = now(),
                        error_message = $2
                    where id = $1
                    """,
                    run_id,
                    str(exc)[:2000],
                )
                await self._upsert_recompute_watermarks(
                    conn,
                    symbol_id=symbol_id,
                    level_code=level_code,
                    modes=modes,
                    base_timeframe_code=base_timeframe_code,
                    last_computed_bar_end=None,
                    dirty_from_bar_end=bar_from,
                    last_error=str(exc)[:2000],
                )
                raise

        return {
            "strokes": stroke_count,
            "segments": segment_count,
            "centers": center_count,
            "signals": signal_count,
        }

    async def replace_incremental_analysis(
        self,
        *,
        symbol: str,
        level: str,
        modes: list[str],
        anchor_bar_end: datetime,
        bar_until: datetime,
        response: dict[str, Any],
        expected_head_run_id: int | None = None,
        expected_head_base_to_bar_end: datetime | None = None,
        publication_claim_token: str | None = None,
    ) -> dict[str, Any]:
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
            base_timeframe_code = timeframe_to_db_code(
                level if self.native_base_timeframe else "5f"
            )
            mode_codes = [mode_to_code(mode) for mode in modes]
            runs_table = self.tables["runs"]
            published_heads_table = self.tables["published_heads"]
            head = await conn.fetchrow(
                f"""
                select run_id, base_from_bar_end, base_to_bar_end, snapshot_version
                from {published_heads_table}
                where symbol_id = $1
                  and chan_level = $2
                  and base_timeframe = $3
                  and mode = $4
                  and status = 'published'
                  and run_id is not null
                  and base_from_bar_end is not null
                order by published_at desc nulls last, updated_at desc, id desc
                limit 1
                """,
                symbol_id,
                level_code,
                base_timeframe_code,
                modes[0] if modes else "confirmed",
            )
            if head is None:
                raise RuntimeError(
                    f"No published Chan head for incremental publish: {symbol} {level}"
                )
            if expected_head_run_id is not None and int(head["run_id"]) != int(
                expected_head_run_id
            ):
                raise StaleChanHeadError(
                    f"Stale Chan head for {symbol} {level}: expected run {expected_head_run_id}, got {head['run_id']}"
                )
            if (
                expected_head_base_to_bar_end is not None
                and head["base_to_bar_end"] != expected_head_base_to_bar_end
            ):
                raise StaleChanHeadError(
                    f"Stale Chan head endpoint for {symbol} {level}: expected {expected_head_base_to_bar_end}, "
                    f"got {head['base_to_bar_end']}"
                )
            if (
                head["base_to_bar_end"] is not None
                and bar_until <= head["base_to_bar_end"]
            ):
                raise StaleChanHeadError(
                    f"Refusing to publish non-advancing Chan tail for {symbol} {level}: {bar_until}"
                )

            bar_from = head["base_from_bar_end"]
            bar_count = await conn.fetchval(
                """
                select count(*)::int
                from klines
                where symbol_id = $1
                  and timeframe = $2
                  and ts >= $3
                  and ts <= $4
                """,
                symbol_id,
                base_timeframe_code,
                bar_from,
                bar_until,
            )
            snapshot_version = str(response.get("snapshot_version") or "").strip()
            if not snapshot_version:
                snapshot_version = (
                    f"{head['snapshot_version']}:tail:{int(anchor_bar_end.timestamp())}:"
                    f"{int(bar_until.timestamp())}"
                )
            combined_response = await self._build_incremental_response(
                conn,
                previous_run_id=int(head["run_id"]),
                mode_codes=mode_codes,
                level=level,
                anchor_bar_end=anchor_bar_end,
                response={**response, "snapshot_version": snapshot_version},
                modes=modes,
            )

            run_id = await conn.fetchval(
                f"""
                insert into {runs_table} (
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
                    computed_at,
                    run_kind,
                    run_group_id,
                    cutoff_bar_end
                )
                values ($1, $2, 0, $3, $4, $5, $6, $7, 'running', $8, now(), $9, $10, $6)
                returning id
                """,
                symbol_id,
                level_code,
                f"{symbol}:{level}:tail:{int(anchor_bar_end.timestamp())}:{int(bar_until.timestamp())}",
                self.tail_config_hash,
                bar_from,
                bar_until,
                int(bar_count or 0),
                snapshot_version,
                self.run_kind,
                self.run_group_id,
            )

            try:
                async with conn.transaction():
                    stroke_count = await self._insert_stroke_like(
                        conn,
                        self.tables["strokes"],
                        symbol_id,
                        level_code,
                        run_id,
                        combined_response.get("strokes", []),
                    )
                    segment_count = await self._insert_stroke_like(
                        conn,
                        self.tables["segments"],
                        symbol_id,
                        level_code,
                        run_id,
                        combined_response.get("segments", []),
                    )
                    center_count = await self._insert_centers(
                        conn,
                        self.tables["centers"],
                        symbol_id,
                        level_code,
                        run_id,
                        combined_response.get("centers", []),
                    )
                    signal_count = await self._insert_signals(
                        conn,
                        self.tables["signals"],
                        symbol_id,
                        level_code,
                        run_id,
                        combined_response.get("signals", []),
                    )
                    await conn.execute(
                        f"""
                        update {runs_table}
                        set status = 'success',
                            finished_at = now()
                        where id = $1
                        """,
                        run_id,
                    )
                    committed_head = await self._publish_heads_cas(
                        conn,
                        symbol_id=symbol_id,
                        level_code=level_code,
                        modes=modes,
                        base_timeframe_code=base_timeframe_code,
                        bar_from=bar_from,
                        bar_until=bar_until,
                        bar_count=int(bar_count or 0),
                        snapshot_version=snapshot_version,
                        run_id=run_id,
                        expected_run_id=expected_head_run_id or int(head["run_id"]),
                        old_base_to_bar_end=head["base_to_bar_end"],
                        publication_claim_token=publication_claim_token,
                    )
                    await self._upsert_recompute_watermarks(
                        conn,
                        symbol_id=symbol_id,
                        level_code=level_code,
                        modes=modes,
                        base_timeframe_code=base_timeframe_code,
                        last_computed_bar_end=bar_until,
                        dirty_from_bar_end=None,
                        last_error=None,
                    )
            except Exception as exc:
                await conn.execute(
                    f"""
                    update {runs_table}
                    set status = 'failed',
                        finished_at = now(),
                        error_message = $2
                    where id = $1
                    """,
                    run_id,
                    str(exc)[:2000],
                )
                await self._upsert_recompute_watermarks(
                    conn,
                    symbol_id=symbol_id,
                    level_code=level_code,
                    modes=modes,
                    base_timeframe_code=base_timeframe_code,
                    last_computed_bar_end=None,
                    dirty_from_bar_end=anchor_bar_end,
                    last_error=str(exc)[:2000],
                )
                raise

        return {
            "strokes": stroke_count,
            "segments": segment_count,
            "centers": center_count,
            "signals": signal_count,
            **committed_head,
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
    ) -> dict[str, Any]:
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
                self.run_config_hash,
                status,
                run_id,
                last_error,
            )
            for mode in modes
        ]
        if not rows:
            return
        table = self.tables["published_heads"]
        previous = {}
        for mode in modes:
            previous[mode] = await conn.fetchrow(
                f"""
                select run_id, base_to_bar_end
                from {table}
                where symbol_id = $1 and chan_level = $2
                  and mode = $3::varchar and base_timeframe = $4
                for update
                """,
                symbol_id, level_code, mode, base_timeframe_code,
            )
        upsert_sql = f"""
            insert into {table} (
                symbol_id,
                chan_level,
                mode,
                base_timeframe,
                base_from_bar_end,
                base_to_bar_end,
                bar_count,
                snapshot_version,
                config_hash,
                status,
                run_id,
                published_at,
                updated_at,
                last_error
            )
            values (
                $1, $2, $3::varchar, $4, $5, $6, $7, $8::varchar, $9::varchar, $10::varchar, $11,
                case when $10::text = 'published' then now() else null end,
                now(),
                $12
            )
            on conflict (symbol_id, chan_level, mode, base_timeframe)
            do update
            set base_from_bar_end = excluded.base_from_bar_end,
                base_to_bar_end = excluded.base_to_bar_end,
                bar_count = excluded.bar_count,
                snapshot_version = excluded.snapshot_version,
                config_hash = excluded.config_hash,
                status = excluded.status,
                run_id = excluded.run_id,
                published_at = case
                    when excluded.status = 'published' then now()
                    else {table}.published_at
                end,
                updated_at = now(),
                last_error = excluded.last_error
            where {table}.status <> 'published'
               or {table}.base_to_bar_end is null
               or {table}.base_to_bar_end < excluded.base_to_bar_end
               or (
                    {table}.base_to_bar_end = excluded.base_to_bar_end
                    and {table}.config_hash is distinct from excluded.config_hash
               )
            """
        for mode, row in zip(modes, rows, strict=True):
            result = await conn.execute(upsert_sql, *row)
            if result.endswith(" 0"):
                continue
            old = previous[mode]
            await self._append_head_history_outbox(
                conn,
                symbol_id=symbol_id,
                level_code=level_code,
                mode=mode,
                base_timeframe_code=base_timeframe_code,
                old_run_id=int(old["run_id"]) if old and old["run_id"] is not None else None,
                new_run_id=run_id,
                old_base_to_bar_end=old["base_to_bar_end"] if old else None,
                new_base_to_bar_end=bar_until,
                snapshot_version=snapshot_version,
            )

    async def _publish_heads_cas(
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
        expected_run_id: int,
        old_base_to_bar_end: datetime | None = None,
        publication_claim_token: str | None = None,
    ) -> None:
        table = self.tables["published_heads"]
        for mode in modes:
            result = await conn.execute(
                f"""
                update {table}
                set base_from_bar_end = $5,
                    base_to_bar_end = $6,
                    bar_count = $7,
                    snapshot_version = $8::varchar,
                    config_hash = $11::varchar,
                    status = 'published',
                    run_id = $9,
                    published_at = now(),
                    updated_at = now(),
                    last_error = null
                where symbol_id = $1
                  and chan_level = $2
                  and mode = $3::varchar
                  and base_timeframe = $4
                  and status = 'published'
                  and run_id = $10
                  and (
                      base_to_bar_end is null
                      or base_to_bar_end < $6
                  )
                """,
                symbol_id,
                level_code,
                mode,
                base_timeframe_code,
                bar_from,
                bar_until,
                bar_count,
                snapshot_version,
                run_id,
                expected_run_id,
                self.tail_config_hash,
            )
            if not result.endswith(" 1"):
                raise StaleChanHeadError(
                    f"Chan head CAS failed for symbol_id={symbol_id} level={level_code} mode={mode}"
                )
            await self._append_head_history_outbox(
                conn,
                symbol_id=symbol_id,
                level_code=level_code,
                mode=mode,
                base_timeframe_code=base_timeframe_code,
                old_run_id=expected_run_id,
                new_run_id=run_id,
                old_base_to_bar_end=old_base_to_bar_end,
                new_base_to_bar_end=bar_until,
                snapshot_version=snapshot_version,
                claim_token=publication_claim_token,
                config_hash=self.tail_config_hash,
            )
        return {
            "run_id": int(run_id),
            "snapshot_version": snapshot_version,
        }

    async def _append_head_history_outbox(
        self,
        conn,
        *,
        symbol_id: int,
        level_code: int,
        mode: str,
        base_timeframe_code: int,
        old_run_id: int | None,
        new_run_id: int,
        old_base_to_bar_end: datetime | None,
        new_base_to_bar_end: datetime,
        snapshot_version: str,
        claim_token: str | None = None,
        config_hash: str | None = None,
    ) -> None:
        await conn.execute(
            """
            with inserted_history as (
                insert into chan_c_head_history (
                    symbol_id, chan_level, mode, base_timeframe, config_hash,
                    publication_profile, run_group_id, old_run_id, new_run_id,
                    old_base_to_bar_end, new_base_to_bar_end, snapshot_version,
                    worker_id, claim_token, source, provenance
                ) values (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    $10, $11, $12, $13, $14, $15,
                    jsonb_build_object('publication_profile', $6::text, 'run_group_id', $7::text)
                )
                on conflict (symbol_id, chan_level, mode, base_timeframe, new_run_id)
                do nothing
                returning id, symbol_id, chan_level, mode, base_timeframe,
                          publication_profile, run_group_id, old_run_id, new_run_id,
                          old_base_to_bar_end, new_base_to_bar_end, snapshot_version,
                          config_hash, published_at
            )
            insert into chan_c_head_outbox (head_history_id, payload)
            select id, to_jsonb(inserted_history) from inserted_history
            on conflict (head_history_id) do nothing
            """,
            symbol_id, level_code, mode, base_timeframe_code,
            config_hash or self.run_config_hash, self.publication_profile, self.run_group_id,
            old_run_id, new_run_id, old_base_to_bar_end, new_base_to_bar_end,
            snapshot_version, self.worker_id, claim_token or self.claim_token, self.publication_source,
        )

    async def _upsert_recompute_watermarks(
        self,
        conn,
        *,
        symbol_id: int,
        level_code: int,
        modes: list[str],
        base_timeframe_code: int,
        last_computed_bar_end: datetime | None,
        dirty_from_bar_end: datetime | None,
        last_error: str | None,
    ) -> None:
        rows = [
            (
                symbol_id,
                level_code,
                mode,
                base_timeframe_code,
                dirty_from_bar_end,
                last_computed_bar_end,
                last_error,
            )
            for mode in modes
        ]
        if not rows:
            return
        table = self.tables["recompute_watermarks"]
        await conn.executemany(
            f"""
            insert into {table} (
                symbol_id,
                chan_level,
                mode,
                base_timeframe,
                dirty_from_bar_end,
                last_computed_bar_end,
                last_error,
                updated_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, now())
            on conflict (symbol_id, chan_level, mode, base_timeframe)
            do update
            set dirty_from_bar_end = excluded.dirty_from_bar_end,
                last_computed_bar_end = coalesce(
                    excluded.last_computed_bar_end,
                    {table}.last_computed_bar_end
                ),
                last_error = excluded.last_error,
                updated_at = now()
            """,
            rows,
        )

    async def _build_incremental_response(
        self,
        conn,
        *,
        previous_run_id: int,
        mode_codes: list[int],
        level: str,
        anchor_bar_end: datetime,
        response: dict[str, Any],
        modes: list[str],
    ) -> dict[str, Any]:
        prefix_strokes = await self._fetch_existing_stroke_like(
            conn,
            self.tables["strokes"],
            previous_run_id,
            mode_codes,
            anchor_bar_end,
        )
        prefix_segments = await self._fetch_existing_stroke_like(
            conn,
            self.tables["segments"],
            previous_run_id,
            mode_codes,
            anchor_bar_end,
        )
        prefix_centers = await self._fetch_existing_centers(
            conn,
            previous_run_id,
            mode_codes,
            anchor_bar_end,
        )
        prefix_signals = await self._fetch_existing_signals(
            conn,
            previous_run_id,
            mode_codes,
            anchor_bar_end,
        )
        tail = _filter_tail_response(
            response, level=level, modes=modes, anchor_bar_end=anchor_bar_end
        )
        return {
            **response,
            "timeframe": level,
            "strokes": _order_lines([*prefix_strokes, *tail["strokes"]]),
            "segments": _order_lines([*prefix_segments, *tail["segments"]]),
            "centers": _order_ranges([*prefix_centers, *tail["centers"]]),
            "signals": _order_points([*prefix_signals, *tail["signals"]]),
        }

    async def _fetch_existing_stroke_like(
        self,
        conn,
        table: str,
        run_id: int,
        mode_codes: list[int],
        anchor_bar_end: datetime,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            f"""
            select
                id,
                mode,
                seq,
                start_ts,
                end_ts,
                start_price_x1000,
                end_price_x1000,
                direction,
                is_confirmed,
                coalesce(begin_base_ts, start_ts) as begin_base_ts,
                coalesce(end_base_ts, end_ts) as end_base_ts,
                begin_base_seq,
                end_base_seq,
                extra
            from {table}
            where run_id = $1
              and mode = any($2::smallint[])
              and coalesce(end_base_ts, end_ts) <= $3
            order by seq, coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts)
            """,
            run_id,
            mode_codes,
            anchor_bar_end,
        )
        return [
            {
                "id": _read_extra(row["extra"]).get("id") or f"{table}:{row['id']}",
                "seq": row["seq"],
                "mode": CODE_TO_MODE.get(row["mode"], "confirmed"),
                "start": {
                    "time": int(row["begin_base_ts"].timestamp()),
                    "price": row["start_price_x1000"] / 1000,
                    "base_ts": int(row["begin_base_ts"].timestamp()),
                    "base_seq": row["begin_base_seq"],
                },
                "end": {
                    "time": int(row["end_base_ts"].timestamp()),
                    "price": row["end_price_x1000"] / 1000,
                    "base_ts": int(row["end_base_ts"].timestamp()),
                    "base_seq": row["end_base_seq"],
                },
                "begin_base_ts": int(row["begin_base_ts"].timestamp()),
                "end_base_ts": int(row["end_base_ts"].timestamp()),
                "begin_base_seq": row["begin_base_seq"],
                "end_base_seq": row["end_base_seq"],
                "direction": CODE_TO_DIRECTION.get(row["direction"], "up"),
                "confirmed": bool(row["is_confirmed"]),
            }
            for row in rows
        ]

    async def _fetch_existing_centers(
        self,
        conn,
        run_id: int,
        mode_codes: list[int],
        anchor_bar_end: datetime,
    ) -> list[dict[str, Any]]:
        table = self.tables["centers"]
        rows = await conn.fetch(
            f"""
            select
                id,
                mode,
                seq,
                low_x1000,
                high_x1000,
                is_confirmed,
                coalesce(begin_base_ts, start_ts) as begin_base_ts,
                coalesce(end_base_ts, end_ts) as end_base_ts,
                begin_base_seq,
                end_base_seq,
                extra
            from {table}
            where run_id = $1
              and mode = any($2::smallint[])
              and coalesce(end_base_ts, end_ts) <= $3
            order by seq, coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts)
            """,
            run_id,
            mode_codes,
            anchor_bar_end,
        )
        return [
            {
                "id": _read_extra(row["extra"]).get("id") or f"center:{row['id']}",
                "seq": row["seq"],
                "mode": CODE_TO_MODE.get(row["mode"], "confirmed"),
                "start_time": int(row["begin_base_ts"].timestamp()),
                "end_time": int(row["end_base_ts"].timestamp()),
                "begin_base_ts": int(row["begin_base_ts"].timestamp()),
                "end_base_ts": int(row["end_base_ts"].timestamp()),
                "begin_base_seq": row["begin_base_seq"],
                "end_base_seq": row["end_base_seq"],
                "low": row["low_x1000"] / 1000,
                "high": row["high_x1000"] / 1000,
                "confirmed": bool(row["is_confirmed"]),
            }
            for row in rows
        ]

    async def _fetch_existing_signals(
        self,
        conn,
        run_id: int,
        mode_codes: list[int],
        anchor_bar_end: datetime,
    ) -> list[dict[str, Any]]:
        table = self.tables["signals"]
        rows = await conn.fetch(
            f"""
            select
                id,
                mode,
                ts,
                price_x1000,
                signal_type,
                is_confirmed,
                coalesce(base_ts, ts) as base_ts,
                base_seq,
                extra
            from {table}
            where run_id = $1
              and mode = any($2::smallint[])
              and coalesce(base_ts, ts) <= $3
            order by coalesce(base_ts, ts), id
            """,
            run_id,
            mode_codes,
            anchor_bar_end,
        )
        return [
            {
                "id": _read_extra(row["extra"]).get("id") or f"signal:{row['id']}",
                "seq": row["id"],
                "mode": CODE_TO_MODE.get(row["mode"], "confirmed"),
                "time": int(row["base_ts"].timestamp()),
                "base_ts": int(row["base_ts"].timestamp()),
                "base_seq": row["base_seq"],
                "price": row["price_x1000"] / 1000,
                "signal_type": row["signal_type"],
                "side": _read_extra(row["extra"]).get("side"),
                "bsp_type": _read_extra(row["extra"]).get("bsp_type"),
                "features": _read_extra(row["extra"]).get("features") or {},
                "confirmed": bool(row["is_confirmed"]),
            }
            for row in rows
        ]

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
        await _copy_or_executemany(
            conn,
            table,
            (
                "symbol_id",
                "chan_level",
                "mode",
                "run_id",
                "seq",
                "start_ts",
                "end_ts",
                "start_price_x1000",
                "end_price_x1000",
                "direction",
                "is_confirmed",
                "begin_base_ts",
                "end_base_ts",
                "begin_base_seq",
                "end_base_seq",
                "extra",
            ),
            rows,
            f"""
            insert into {table} (
                symbol_id, chan_level, mode, run_id, seq, start_ts, end_ts,
                start_price_x1000, end_price_x1000, direction, is_confirmed,
                begin_base_ts, end_base_ts, begin_base_seq, end_base_seq, extra
            )
            values (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16::jsonb
            )
            """,
        )
        return len(rows)

    async def _insert_centers(
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
        await _copy_or_executemany(
            conn,
            table,
            (
                "symbol_id",
                "chan_level",
                "mode",
                "run_id",
                "seq",
                "start_ts",
                "end_ts",
                "low_x1000",
                "high_x1000",
                "is_confirmed",
                "begin_base_ts",
                "end_base_ts",
                "begin_base_seq",
                "end_base_seq",
                "extra",
            ),
            rows,
            f"""
            insert into {table} (
                symbol_id, chan_level, mode, run_id, seq, start_ts, end_ts,
                low_x1000, high_x1000, is_confirmed, begin_base_ts, end_base_ts,
                begin_base_seq, end_base_seq, extra
            )
            values (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15::jsonb
            )
            """,
        )
        return len(rows)

    async def _insert_signals(
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
        await _copy_or_executemany(
            conn,
            table,
            (
                "symbol_id",
                "chan_level",
                "mode",
                "run_id",
                "ts",
                "price_x1000",
                "signal_type",
                "is_confirmed",
                "base_ts",
                "base_seq",
                "extra",
            ),
            rows,
            f"""
            insert into {table} (
                symbol_id, chan_level, mode, run_id, ts, price_x1000,
                signal_type, is_confirmed, base_ts, base_seq, extra
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
            """,
        )
        return len(rows)


async def _copy_or_executemany(
    conn, table: str, columns: tuple[str, ...], rows: list[tuple], fallback_sql: str
) -> None:
    copy_records = getattr(conn, "copy_records_to_table", None)
    if copy_records is not None:
        await copy_records(table, records=rows, columns=list(columns))
        return
    await conn.executemany(fallback_sql, rows)
