from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from collector.kline_scope_catalog import record_present_scopes
from collector.storage.postgres import amount_to_x100, price_to_x1000, source_priority_case
from trading_protocol import Bar, SymbolInfo, canonical_kline_timestamp

PARQUET_5F_SOURCE = "parquet_5f"
PARQUET_5F_SOURCE_CODE = 4
PARQUET_5F_TIMEFRAME = 5


class LostScheme2MemberLease(RuntimeError):
    """Raised when a Scheme 2 member writer no longer owns its checkpoint."""


@dataclass(frozen=True)
class Scheme2SourceMember:
    root_path: str
    source_profile: str
    zip_path: str
    member_path: str
    member_crc32: int | None
    member_size_bytes: int | None
    content_sha256: str | None = None
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
                member.content_sha256,
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
                    content_sha256,
                    timeframe,
                    status
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
                on conflict (
                    root_path,
                    source_profile,
                    zip_path,
                    member_path,
                    (coalesce(member_crc32, -1)),
                    (coalesce(member_size_bytes, -1)),
                    (coalesce(content_sha256, ''))
                ) do update
                set status = case
                        when $9::boolean then 'pending'
                        else scheme2_source_member_checkpoints.status
                    end,
                    worker_id = case
                        when $9::boolean then null
                        else scheme2_source_member_checkpoints.worker_id
                    end,
                    claim_token = case
                        when $9::boolean then null
                        else scheme2_source_member_checkpoints.claim_token
                    end,
                    lease_until = case
                        when $9::boolean then null
                        else scheme2_source_member_checkpoints.lease_until
                    end,
                    lease_heartbeat_at = case
                        when $9::boolean then null
                        else scheme2_source_member_checkpoints.lease_heartbeat_at
                    end,
                    attempts = case
                        when $9::boolean then 0
                        else scheme2_source_member_checkpoints.attempts
                    end,
                    imported_rows = case
                        when $9::boolean then 0
                        else scheme2_source_member_checkpoints.imported_rows
                    end,
                    error_message = case
                        when $9::boolean then null
                        else scheme2_source_member_checkpoints.error_message
                    end,
                    started_at = case
                        when $9::boolean then null
                        else scheme2_source_member_checkpoints.started_at
                    end,
                    completed_at = case
                        when $9::boolean then null
                        else scheme2_source_member_checkpoints.completed_at
                    end,
                    updated_at = clock_timestamp()
                where not $9::boolean
                   or scheme2_source_member_checkpoints.status <> 'running'
                   or scheme2_source_member_checkpoints.lease_until <= clock_timestamp()
                """,
                [(*row, reset) for row in rows],
            )
        return len(rows)

    async def reset_running(self) -> int:
        """Release only expired leases; a live owner is never operator-reset."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                update scheme2_source_member_checkpoints
                set status = case
                        when attempts + 1 >= max_attempts then 'dead_letter'
                        else 'pending'
                    end,
                    attempts = attempts + 1,
                    worker_id = null,
                    claim_token = null,
                    lease_until = null,
                    lease_heartbeat_at = null,
                    error_message = case
                        when attempts + 1 >= max_attempts
                            then coalesce(error_message, 'maximum attempts exhausted')
                        else null
                    end,
                    completed_at = case
                        when attempts + 1 >= max_attempts then clock_timestamp()
                        else completed_at
                    end,
                    updated_at = clock_timestamp()
                where source_profile = $1
                  and status = 'running'
                  and lease_until <= clock_timestamp()
                """,
                PARQUET_5F_SOURCE,
            )
        return int(result.split()[-1])

    async def claim_member_checkpoints(
        self,
        *,
        limit: int,
        worker_id: str,
        lease_seconds: int,
        max_attempts: int,
    ) -> list[dict[str, Any]]:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    with exhausted_members as (
                        select id
                          from scheme2_source_member_checkpoints
                         where source_profile = $1
                           and timeframe = $2
                           and (
                               (status in ('pending', 'failed')
                                and attempts >= least(max_attempts, $3))
                               or
                               (status = 'running'
                                and lease_until <= clock_timestamp()
                                and attempts + 1 >= least(max_attempts, $3))
                           )
                         order by updated_at, id
                         limit $4
                         for update skip locked
                    )
                    update scheme2_source_member_checkpoints checkpoint
                       set status = 'dead_letter',
                           worker_id = null,
                           claim_token = null,
                           lease_until = null,
                           lease_heartbeat_at = null,
                           attempts = case
                               when status = 'running' then attempts + 1
                               else attempts
                           end,
                           max_attempts = $3,
                           error_message = coalesce(
                               error_message,
                               'maximum attempts exhausted'
                           ),
                           completed_at = clock_timestamp(),
                           updated_at = clock_timestamp()
                      from exhausted_members
                     where checkpoint.id = exhausted_members.id
                    """,
                    PARQUET_5F_SOURCE,
                    PARQUET_5F_TIMEFRAME,
                    max(1, max_attempts),
                    max(1, limit),
                )
                rows = await conn.fetch(
                    """
                    with next_members as (
                    select id
                    from scheme2_source_member_checkpoints
                    where source_profile = $1
                      and timeframe = $2
                      and (
                          (status in ('pending', 'failed')
                           and attempts < least(max_attempts, $6))
                          or
                          (status = 'running'
                           and lease_until <= clock_timestamp()
                           and attempts + 1 < least(max_attempts, $6))
                      )
                    order by updated_at, id
                    limit $3
                    for update skip locked
                )
                    update scheme2_source_member_checkpoints checkpoint
                    set status = 'running',
                    worker_id = $4,
                    lease_version = checkpoint.lease_version + 1,
                    claim_token = md5(
                        checkpoint.id::text || ':' ||
                        (checkpoint.lease_version + 1)::text || ':' ||
                        clock_timestamp()::text || ':' || random()::text
                    ),
                    lease_until = clock_timestamp() + ($5::integer * interval '1 second'),
                    lease_heartbeat_at = clock_timestamp(),
                    attempts = case
                        when checkpoint.status = 'running' then checkpoint.attempts + 1
                        else checkpoint.attempts
                    end,
                    max_attempts = $6,
                    error_message = null,
                    started_at = coalesce(checkpoint.started_at, clock_timestamp()),
                    completed_at = null,
                    updated_at = clock_timestamp()
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
                    checkpoint.content_sha256,
                    checkpoint.timeframe,
                    checkpoint.imported_rows,
                    checkpoint.worker_id,
                    checkpoint.claim_token,
                    checkpoint.lease_version,
                    checkpoint.lease_until,
                    checkpoint.attempts,
                    checkpoint.max_attempts
                    """,
                    PARQUET_5F_SOURCE,
                    PARQUET_5F_TIMEFRAME,
                    max(1, limit),
                    worker_id,
                    max(1, lease_seconds),
                    max(1, max_attempts),
                )
        return [dict(row) for row in rows]

    async def heartbeat(
        self,
        *,
        checkpoint_id: int,
        claim_token: str,
        lease_version: int,
        lease_seconds: int,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                update scheme2_source_member_checkpoints
                   set lease_until = clock_timestamp() + ($4::integer * interval '1 second'),
                       lease_heartbeat_at = clock_timestamp(),
                       updated_at = clock_timestamp()
                 where id = $1
                   and status = 'running'
                   and claim_token = $2
                   and lease_version = $3
                   and lease_until > clock_timestamp()
                returning id
                """,
                checkpoint_id,
                claim_token,
                lease_version,
                max(1, lease_seconds),
            )
        return row is not None

    async def record_member_success(
        self,
        *,
        checkpoint_id: int,
        claim_token: str,
        lease_version: int,
        expected_imported_rows: int,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                update scheme2_source_member_checkpoints
                set status = 'success',
                    error_message = null,
                    worker_id = null,
                    claim_token = null,
                    lease_until = null,
                    lease_heartbeat_at = null,
                    completed_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                where id = $1
                  and status = 'running'
                  and claim_token = $2
                  and lease_version = $3
                  and lease_until > clock_timestamp()
                  and imported_rows = $4
                returning id
                """,
                checkpoint_id,
                claim_token,
                lease_version,
                expected_imported_rows,
            )
        return row is not None

    async def yield_member(
        self,
        *,
        checkpoint_id: int,
        claim_token: str,
        lease_version: int,
        expected_imported_rows: int,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                update scheme2_source_member_checkpoints
                   set status = 'pending',
                       worker_id = null,
                       claim_token = null,
                       lease_until = null,
                       lease_heartbeat_at = null,
                       updated_at = clock_timestamp()
                 where id = $1
                   and status = 'running'
                   and claim_token = $2
                   and lease_version = $3
                   and lease_until > clock_timestamp()
                   and imported_rows = $4
                returning id
                """,
                checkpoint_id,
                claim_token,
                lease_version,
                expected_imported_rows,
            )
        return row is not None

    async def record_member_failure(
        self,
        *,
        checkpoint_id: int,
        claim_token: str,
        lease_version: int,
        error: str,
        expected_imported_rows: int,
    ) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                update scheme2_source_member_checkpoints
                set status = case
                        when attempts + 1 >= max_attempts then 'dead_letter'
                        else 'failed'
                    end,
                    attempts = attempts + 1,
                    worker_id = null,
                    claim_token = null,
                    lease_until = null,
                    lease_heartbeat_at = null,
                    error_message = $5,
                    completed_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                where id = $1
                  and status = 'running'
                  and claim_token = $2
                  and lease_version = $3
                  and lease_until > clock_timestamp()
                  and imported_rows = $4
                returning id
                """,
                checkpoint_id,
                claim_token,
                lease_version,
                expected_imported_rows,
                error[:2000],
            )
        return row is not None


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
                await self._write_5f_batch(conn, symbol_rows=symbol_rows, bar_rows=bar_rows)
        return len(bar_rows)

    async def commit_member_batch(
        self,
        *,
        task: Mapping[str, object],
        expected_imported_rows: int,
        symbols: Iterable[SymbolInfo],
        bars: Iterable[Bar],
        lease_seconds: int,
    ) -> int:
        """Write one member batch and advance its checkpoint in one fenced transaction."""
        assert self._pool is not None
        if expected_imported_rows < 0:
            raise ValueError("Scheme 2 imported row progress cannot be negative")
        symbol_rows = _symbol_rows(symbols)
        bar_rows = _bar_rows(bars)
        checkpoint_id = int(task["id"])
        claim_token = str(task["claim_token"])
        lease_version = int(task["lease_version"])

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                owned = await conn.fetchrow(
                    """
                    select id,
                           root_path,
                           source_profile,
                           zip_path,
                           member_path,
                           member_crc32,
                           member_size_bytes,
                           content_sha256,
                           timeframe
                      from scheme2_source_member_checkpoints
                     where id = $1
                       and status = 'running'
                       and claim_token = $2
                       and lease_version = $3
                       and lease_until > clock_timestamp()
                       and imported_rows = $4
                     for update
                    """,
                    checkpoint_id,
                    claim_token,
                    lease_version,
                    expected_imported_rows,
                )
                if owned is None:
                    raise LostScheme2MemberLease(
                        f"Scheme 2 member lease lost before batch write: {checkpoint_id}"
                    )
                for field in (
                    "root_path",
                    "source_profile",
                    "zip_path",
                    "member_path",
                    "member_crc32",
                    "member_size_bytes",
                    "content_sha256",
                    "timeframe",
                ):
                    if task.get(field) != owned[field]:
                        raise ValueError(
                            f"Scheme 2 member checkpoint identity mismatch: {checkpoint_id}"
                        )
                if str(owned["source_profile"]) != PARQUET_5F_SOURCE:
                    raise ValueError(
                        f"Scheme 2 member source profile is not authoritative: {checkpoint_id}"
                    )
                if int(owned["timeframe"]) != PARQUET_5F_TIMEFRAME:
                    raise ValueError(
                        f"Scheme 2 member timeframe is not authoritative: {checkpoint_id}"
                    )

                if bar_rows:
                    await self._write_5f_batch(
                        conn,
                        symbol_rows=symbol_rows,
                        bar_rows=bar_rows,
                    )
                updated = await conn.fetchrow(
                    """
                    update scheme2_source_member_checkpoints
                       set imported_rows = $5,
                           attempts = 0,
                           lease_until = clock_timestamp()
                               + ($6::integer * interval '1 second'),
                           lease_heartbeat_at = clock_timestamp(),
                           updated_at = clock_timestamp()
                     where id = $1
                       and status = 'running'
                       and claim_token = $2
                       and lease_version = $3
                       and imported_rows = $4
                    returning id
                    """,
                    checkpoint_id,
                    claim_token,
                    lease_version,
                    expected_imported_rows,
                    expected_imported_rows + len(bar_rows),
                    max(1, lease_seconds),
                )
                if updated is None:
                    raise LostScheme2MemberLease(
                        f"Scheme 2 member lease lost during batch write: {checkpoint_id}"
                    )
        return len(bar_rows)

    async def _write_5f_batch(
        self,
        conn,
        *,
        symbol_rows: list[tuple],
        bar_rows: list[tuple],
    ) -> None:
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
        scope_rows = await conn.fetch(
            """select symbols.id as symbol_id,
                      stage.timeframe,
                      min(stage.bar_end) as min_ts,
                      max(stage.bar_end) as max_ts
                 from _scheme2_kline_stage stage
                 join symbols
                   on symbols.code = stage.code
                  and symbols.exchange = stage.exchange
                group by symbols.id, stage.timeframe
                order by symbols.id, stage.timeframe"""
        )
        await record_present_scopes(
            conn,
            scopes=[
                (
                    int(row["symbol_id"]),
                    int(row["timeframe"]),
                    row["min_ts"],
                    row["max_ts"],
                )
                for row in scope_rows
            ],
        )
        await _upsert_watermarks_from_stage(conn)


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
    return [seen[key] for key in sorted(seen, key=lambda item: (item[1], item[0]))]


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
    rows.sort(key=lambda row: (row[1], row[0], row[2], row[3], row[11]))
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
        order by symbols.id, stage.timeframe
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
        order by symbols.id, stage.timeframe
        on conflict (symbol_id, timeframe, source) do update
        set covered_until = greatest(kline_source_coverage.covered_until, excluded.covered_until),
            updated_at = now()
        """
    )
