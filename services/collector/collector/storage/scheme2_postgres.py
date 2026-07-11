from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from collector.storage.postgres import amount_to_x100, price_to_x1000, source_priority_case
from trading_protocol import Bar, SymbolInfo, canonical_kline_timestamp

PARQUET_5F_SOURCE = "parquet_5f"
PARQUET_5F_SOURCE_CODE = 4
PARQUET_5F_TIMEFRAME = 5


@dataclass(frozen=True)
class Scheme2SourceMember:
    root_path: str
    source_profile: str
    zip_path: str
    member_path: str
    member_crc32: int | None
    member_size_bytes: int | None
    timeframe: int = PARQUET_5F_TIMEFRAME


class PostgresScheme2MemberCheckpointStore:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._pool = None

    async def __aenter__(self) -> "PostgresScheme2MemberCheckpointStore":
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError("asyncpg is required for Scheme 2 parquet checkpoints.") from exc
        self._pool = await asyncpg.create_pool(self.database_url)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def ensure_member_checkpoints(
        self,
        members: Iterable[Scheme2SourceMember],
        *,
        reset: bool = False,
    ) -> int:
        assert self._pool is not None
        rows = [
            (
                member.root_path,
                member.source_profile,
                member.zip_path,
                member.member_path,
                member.member_crc32,
                member.member_size_bytes,
                member.timeframe,
            )
            for member in members
        ]
        if not rows:
            return 0
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                insert into scheme2_source_member_checkpoints (
                    root_path,
                    source_profile,
                    zip_path,
                    member_path,
                    member_crc32,
                    member_size_bytes,
                    timeframe,
                    status
                )
                values ($1, $2, $3, $4, $5, $6, $7, 'pending')
                on conflict (
                    root_path,
                    source_profile,
                    zip_path,
                    member_path,
                    (coalesce(member_crc32, -1)),
                    (coalesce(member_size_bytes, -1))
                ) do update
                set status = case
                        when $8::boolean then 'pending'
                        else scheme2_source_member_checkpoints.status
                    end,
                    imported_rows = case
                        when $8::boolean then 0
                        else scheme2_source_member_checkpoints.imported_rows
                    end,
                    error_message = case
                        when $8::boolean then null
                        else scheme2_source_member_checkpoints.error_message
                    end,
                    started_at = case
                        when $8::boolean then null
                        else scheme2_source_member_checkpoints.started_at
                    end,
                    completed_at = case
                        when $8::boolean then null
                        else scheme2_source_member_checkpoints.completed_at
                    end,
                    updated_at = now()
                """,
                [(*row, reset) for row in rows],
            )
        return len(rows)

    async def reset_running(self) -> int:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                update scheme2_source_member_checkpoints
                set status = 'pending',
                    error_message = null,
                    updated_at = now()
                where source_profile = $1
                  and status = 'running'
                """,
                PARQUET_5F_SOURCE,
            )
        return int(result.split()[-1])

    async def claim_member_checkpoints(self, *, limit: int) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                with next_members as (
                    select id
                    from scheme2_source_member_checkpoints
                    where source_profile = $1
                      and timeframe = $2
                      and status in ('pending', 'failed')
                    order by updated_at, id
                    limit $3
                    for update skip locked
                )
                update scheme2_source_member_checkpoints checkpoint
                set status = 'running',
                    error_message = null,
                    started_at = coalesce(checkpoint.started_at, now()),
                    completed_at = null,
                    updated_at = now()
                from next_members
                where checkpoint.id = next_members.id
                returning
                    checkpoint.id,
                    checkpoint.root_path,
                    checkpoint.source_profile,
                    checkpoint.zip_path,
                    checkpoint.member_path,
                    checkpoint.member_crc32,
                    checkpoint.member_size_bytes,
                    checkpoint.timeframe
                """,
                PARQUET_5F_SOURCE,
                PARQUET_5F_TIMEFRAME,
                max(1, limit),
            )
        return [dict(row) for row in rows]

    async def record_member_success(self, *, checkpoint_id: int, imported_rows: int) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update scheme2_source_member_checkpoints
                set status = 'success',
                    imported_rows = $2,
                    error_message = null,
                    completed_at = now(),
                    updated_at = now()
                where id = $1
                """,
                checkpoint_id,
                imported_rows,
            )

    async def record_member_failure(
        self,
        *,
        checkpoint_id: int,
        error: str,
        imported_rows: int = 0,
    ) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                update scheme2_source_member_checkpoints
                set status = 'failed',
                    imported_rows = $2,
                    error_message = $3,
                    completed_at = now(),
                    updated_at = now()
                where id = $1
                """,
                checkpoint_id,
                imported_rows,
                error[:2000],
            )


class PostgresScheme2KlineWriter:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._pool = None

    async def __aenter__(self) -> "PostgresScheme2KlineWriter":
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError("asyncpg is required for Scheme 2 parquet writes.") from exc
        self._pool = await asyncpg.create_pool(self.database_url)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def upsert_5f_bars(
        self,
        *,
        symbols: Iterable[SymbolInfo],
        bars: Iterable[Bar],
    ) -> int:
        assert self._pool is not None
        symbol_rows = _symbol_rows(symbols)
        bar_rows = _bar_rows(bars)
        if not bar_rows:
            return 0
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _create_symbol_stage(conn)
                if symbol_rows:
                    await conn.copy_records_to_table(
                        "_scheme2_symbol_stage",
                        records=symbol_rows,
                        columns=[
                            "code",
                            "exchange",
                            "name",
                            "asset_type",
                            "market",
                            "is_active",
                        ],
                    )
                    await _upsert_symbols_from_stage(conn)

                await _create_kline_stage(conn)
                await conn.copy_records_to_table(
                    "_scheme2_kline_stage",
                    records=bar_rows,
                    columns=[
                        "code",
                        "exchange",
                        "timeframe",
                        "bar_end",
                        "open_x1000",
                        "high_x1000",
                        "low_x1000",
                        "close_x1000",
                        "volume",
                        "amount_x100",
                        "is_complete",
                        "revision",
                        "source",
                    ],
                )
                await _register_source_coverage_from_stage(conn)
                await _upsert_klines_from_stage(conn)
                await _upsert_watermarks_from_stage(conn)
        return len(bar_rows)


def _symbol_rows(symbols: Iterable[SymbolInfo]) -> list[tuple]:
    seen: dict[tuple[str, str], tuple] = {}
    for symbol in symbols:
        seen[(symbol.code, symbol.exchange)] = (
            symbol.code,
            symbol.exchange,
            symbol.name,
            symbol.asset_type,
            symbol.market,
            symbol.is_active,
        )
    return list(seen.values())


def _bar_rows(bars: Iterable[Bar]) -> list[tuple]:
    rows: list[tuple] = []
    for bar in bars:
        code, exchange = _split_symbol(bar.symbol)
        # The parquet source column is named trade_time. Scheme 2 stores it as the
        # canonical 5f bar_end in klines.ts without adding 5 minutes or shifting TZ.
        bar_end = canonical_kline_timestamp("5f", bar.ts)
        rows.append(
            (
                code,
                exchange,
                PARQUET_5F_TIMEFRAME,
                bar_end,
                price_to_x1000(bar.open),
                price_to_x1000(bar.high),
                price_to_x1000(bar.low),
                price_to_x1000(bar.close),
                int(bar.volume),
                amount_to_x100(bar.amount),
                bar.complete,
                bar.revision,
                PARQUET_5F_SOURCE_CODE,
            )
        )
    return rows


def _split_symbol(symbol: str) -> tuple[str, str]:
    code, exchange = symbol.split(".", 1)
    return code, exchange


async def _create_symbol_stage(conn) -> None:
    await conn.execute(
        """
        create temp table if not exists _scheme2_symbol_stage (
            code text not null,
            exchange text not null,
            name text not null,
            asset_type text not null,
            market text not null,
            is_active boolean not null
        ) on commit drop
        """
    )


async def _upsert_symbols_from_stage(conn) -> None:
    await conn.execute(
        """
        insert into symbols (code, exchange, name, asset_type, market, is_active)
        select distinct on (exchange, code)
            code,
            exchange,
            name,
            asset_type,
            market,
            is_active
        from _scheme2_symbol_stage
        order by exchange, code
        on conflict (exchange, code) do update
        set name = excluded.name,
            asset_type = excluded.asset_type,
            market = excluded.market,
            is_active = excluded.is_active,
            updated_at = now()
        """
    )


async def _create_kline_stage(conn) -> None:
    await conn.execute(
        """
        create temp table if not exists _scheme2_kline_stage (
            code text not null,
            exchange text not null,
            timeframe integer not null,
            bar_end timestamptz not null,
            open_x1000 integer not null,
            high_x1000 integer not null,
            low_x1000 integer not null,
            close_x1000 integer not null,
            volume bigint not null,
            amount_x100 bigint,
            is_complete boolean not null,
            revision integer not null,
            source smallint not null
        ) on commit drop
        """
    )


async def _upsert_klines_from_stage(conn) -> None:
    await conn.execute(
        f"""
        with staged_rows as (
            select distinct on (symbols.id, stage.timeframe, stage.bar_end)
                symbols.id as symbol_id,
                stage.timeframe,
                stage.bar_end,
                stage.open_x1000,
                stage.high_x1000,
                stage.low_x1000,
                stage.close_x1000,
                stage.volume,
                stage.amount_x100,
                stage.is_complete,
                stage.revision,
                stage.source
            from _scheme2_kline_stage stage
            join symbols
              on symbols.code = stage.code
             and symbols.exchange = stage.exchange
            order by symbols.id, stage.timeframe, stage.bar_end, stage.revision desc
        )
        insert into klines (
            symbol_id,
            timeframe,
            ts,
            open_x1000,
            high_x1000,
            low_x1000,
            close_x1000,
            volume,
            amount_x100,
            is_complete,
            revision,
            source
        )
        select
            symbol_id,
            timeframe,
            bar_end,
            open_x1000,
            high_x1000,
            low_x1000,
            close_x1000,
            volume,
            amount_x100,
            is_complete,
            revision,
            source
        from staged_rows
        on conflict (symbol_id, timeframe, ts) do update
        set open_x1000 = excluded.open_x1000,
            high_x1000 = excluded.high_x1000,
            low_x1000 = excluded.low_x1000,
            close_x1000 = excluded.close_x1000,
            volume = excluded.volume,
            amount_x100 = excluded.amount_x100,
            is_complete = excluded.is_complete,
            revision = excluded.revision,
            source = excluded.source,
            updated_at = now()
        where ({source_priority_case('excluded.source')}) > ({source_priority_case('klines.source')})
           or (
                ({source_priority_case('excluded.source')}) = ({source_priority_case('klines.source')})
                and (
                    excluded.revision > klines.revision
                    or (excluded.is_complete and not klines.is_complete)
                )
           )
        """
    )


async def _upsert_watermarks_from_stage(conn) -> None:
    await conn.execute(
        """
        insert into scheme2_ingest_watermarks (
            symbol_id,
            timeframe,
            last_bar_end,
            source,
            note
        )
        select
            symbols.id,
            stage.timeframe,
            max(stage.bar_end) as last_bar_end,
            $1,
            'source=4 parquet_5f historical bootstrap'
        from _scheme2_kline_stage stage
        join symbols
          on symbols.code = stage.code
         and symbols.exchange = stage.exchange
        group by symbols.id, stage.timeframe
        on conflict (symbol_id, timeframe) do update
        set last_bar_end = greatest(
                coalesce(scheme2_ingest_watermarks.last_bar_end, excluded.last_bar_end),
                excluded.last_bar_end
            ),
            source = excluded.source,
            note = excluded.note,
            updated_at = now()
        """,
        PARQUET_5F_SOURCE,
    )


async def _register_source_coverage_from_stage(conn) -> None:
    await conn.execute(
        """
        insert into kline_source_coverage (symbol_id, timeframe, source, covered_until)
        select symbols.id, stage.timeframe, 4, max(stage.bar_end)
        from _scheme2_kline_stage stage
        join symbols on symbols.code = stage.code and symbols.exchange = stage.exchange
        group by symbols.id, stage.timeframe
        on conflict (symbol_id, timeframe, source) do update
        set covered_until = greatest(kline_source_coverage.covered_until, excluded.covered_until),
            updated_at = now()
        """
    )
