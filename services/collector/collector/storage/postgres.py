from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from zoneinfo import ZoneInfo

from trading_protocol import (
    Bar,
    SymbolInfo,
    canonical_kline_timestamp,
    code_to_source as contract_code_to_source,
    kline_logical_key,
    normalize_timeframe,
    source_priority,
    source_priority_with_coverage,
    source_priority_sql,
    source_priority_with_coverage_sql,
    source_to_code as contract_source_to_code,
)
from trading_protocol.timeframes import TIMEFRAMES


def timeframe_to_db_code(timeframe: str) -> int:
    return TIMEFRAMES[normalize_timeframe(timeframe)].minutes


def price_to_x1000(value: float) -> int:
    return int(round(value * 1000))


def amount_to_x100(value: float | None) -> int | None:
    if value is None:
        return None
    return int(round(value * 100))


def source_to_code(value: str) -> int:
    return contract_source_to_code(value)


def code_to_source(value: int) -> str:
    return contract_code_to_source(value)


def source_priority_case(column: str, *, timestamp: str | None = None, coverage_end: str | None = None) -> str:
    if timestamp is not None and coverage_end is not None:
        return source_priority_with_coverage_sql(column, timestamp, coverage_end)
    return source_priority_sql(column)


def bar_to_db_values(bar: Bar) -> tuple:
    timeframe = normalize_timeframe(bar.timeframe)
    local = bar.ts.astimezone(ZoneInfo("Asia/Shanghai"))
    ts = canonical_kline_timestamp(
        timeframe,
        bar.ts,
        date_only=timeframe in {"1d", "1w", "1m"} and (local.hour, local.minute) == (0, 0),
    )
    return (
        bar.symbol,
        timeframe_to_db_code(timeframe),
        ts,
        price_to_x1000(bar.open),
        price_to_x1000(bar.high),
        price_to_x1000(bar.low),
        price_to_x1000(bar.close),
        bar.volume,
        amount_to_x100(bar.amount),
        bar.complete,
        bar.revision,
        source_to_code(bar.source),
    )


class PostgresKlineWriter:
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

    async def __aenter__(self) -> "PostgresKlineWriter":
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "asyncpg is required for --write-db. Install collector requirements."
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

    async def upsert_symbols(self, symbols: Iterable[SymbolInfo]) -> int:
        assert self._pool is not None
        rows = [
            (item.code, item.exchange, item.name, item.asset_type, item.market, item.is_active)
            for item in symbols
        ]
        if not rows:
            return 0
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                insert into symbols (code, exchange, name, asset_type, market, is_active)
                values ($1, $2, $3, $4, $5, $6)
                on conflict (exchange, code) do update
                set name = case
                        when excluded.name in (excluded.code, excluded.code || '.' || excluded.exchange)
                            then symbols.name
                        else excluded.name
                    end,
                    asset_type = excluded.asset_type,
                    market = excluded.market,
                    is_active = excluded.is_active,
                    updated_at = now()
                """,
                rows,
            )
        return len(rows)

    async def upsert_bars(self, bars: Iterable[Bar]) -> int:
        assert self._pool is not None
        rows = list(bar_to_db_values(bar) for bar in bars)
        if not rows:
            return 0
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await self._upsert_bars_rows(conn, rows)
        return len(rows)

    async def upsert_bars_for_market_claim(
        self,
        *,
        task_id: int,
        claim_token: str,
        bars: Iterable[Bar],
    ) -> tuple[int, bool]:
        assert self._pool is not None
        rows = list(bar_to_db_values(bar) for bar in bars)
        if not rows:
            return 0, True
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                valid = await conn.fetchval(
                    """
                    select true
                    from scheme2_market_fetch_tasks
                    where id = $1
                      and claim_token = $2
                      and status = 'running'
                      and lease_until > now()
                    for update
                    """,
                    task_id,
                    claim_token,
                )
                if not valid:
                    return 0, False
                await self._upsert_bars_rows(conn, rows)
        return len(rows), True

    async def _upsert_bars_rows(self, conn, rows: list[tuple]) -> None:
        await self._register_source_coverage(conn, rows)
        coverage_end = """(select max(coverage.covered_until) from kline_source_coverage coverage
            where coverage.symbol_id = klines.symbol_id
              and coverage.timeframe = klines.timeframe
              and coverage.source in (4, 9))"""
        await conn.executemany(
            f"""
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
                s.id,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11,
                $12
            from symbols s
            where s.code = split_part($1, '.', 1)
              and s.exchange = split_part($1, '.', 2)
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
            where ({source_priority_case('excluded.source', timestamp='excluded.ts', coverage_end=coverage_end)}) > ({source_priority_case('klines.source', timestamp='klines.ts', coverage_end=coverage_end)})
               or (
                    ({source_priority_case('excluded.source', timestamp='excluded.ts', coverage_end=coverage_end)}) = ({source_priority_case('klines.source', timestamp='klines.ts', coverage_end=coverage_end)})
                    and (
                        excluded.revision > klines.revision
                        or (excluded.is_complete and not klines.is_complete)
                    )
               )
            """,
            rows,
        )
    async def _register_source_coverage(self, conn, rows: list[tuple]) -> None:
        coverage_rows = [
            (symbol, timeframe_code, source_code, ts)
            for symbol, timeframe_code, ts, *_values, source_code in rows
            if source_code in {4, 9}
        ]
        if not coverage_rows:
            return
        await conn.executemany(
            """
            insert into kline_source_coverage (symbol_id, timeframe, source, covered_until)
            select s.id, $2, $3, $4
            from symbols s
            where (s.code || '.' || s.exchange) = $1
            on conflict (symbol_id, timeframe, source) do update
            set covered_until = greatest(kline_source_coverage.covered_until, excluded.covered_until),
                updated_at = now()
            """,
            coverage_rows,
        )

    async def delete_bars(self, symbols: Iterable[str], timeframes: Iterable[str]) -> int:
        assert self._pool is not None
        symbol_list = list(symbols)
        timeframe_codes = [timeframe_to_db_code(item) for item in timeframes]
        if not symbol_list or not timeframe_codes:
            return 0
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                delete from klines k
                using symbols s
                where s.id = k.symbol_id
                  and (s.code || '.' || s.exchange) = any($1::text[])
                  and k.timeframe = any($2::int[])
                """,
                symbol_list,
                timeframe_codes,
            )
        return int(result.split()[-1])

    async def get_bars(self, symbol: str, timeframe: str) -> list[Bar]:
        assert self._pool is not None
        timeframe_code = timeframe_to_db_code(timeframe)
        async with self._pool.acquire() as conn:
            symbol_id = await self._resolve_symbol_id(conn, symbol)
            if symbol_id is None:
                return []
            rows = await self._fetch_bar_rows(
                conn,
                symbol_id=symbol_id,
                timeframe_code=timeframe_code,
                after_ts=None,
                limit=None,
            )
        return _rows_to_canonical_bars(symbol, timeframe, rows)

    async def get_bars_chunk(
        self,
        symbol: str,
        timeframe: str,
        *,
        after_ts: datetime | None = None,
        limit: int = 5000,
    ) -> list[Bar]:
        assert self._pool is not None
        timeframe_code = timeframe_to_db_code(timeframe)
        async with self._pool.acquire() as conn:
            symbol_id = await self._resolve_symbol_id(conn, symbol)
            if symbol_id is None:
                return []
            rows = await self._fetch_bar_rows(
                conn,
                symbol_id=symbol_id,
                timeframe_code=timeframe_code,
                after_ts=after_ts,
                limit=limit,
            )
        return _rows_to_canonical_bars(symbol, timeframe, rows)[:limit]

    async def _resolve_symbol_id(self, conn, symbol: str) -> int | None:
        return await conn.fetchval(
            """
            select id
            from symbols
            where code = split_part($1, '.', 1)
              and exchange = split_part($1, '.', 2)
            """,
            symbol,
        )

    async def _fetch_bar_rows(
        self,
        conn,
        *,
        symbol_id: int,
        timeframe_code: int,
        after_ts,
        limit: int | None,
    ):
        rows = await self._fetch_bar_rows_for_sources(
            conn,
            symbol_id=symbol_id,
            timeframe_code=timeframe_code,
            after_ts=after_ts,
            limit=limit,
            sources=[2, 3, 4, 5, 6, 7, 8, 9],
        )
        if rows:
            return rows
        return await self._fetch_bar_rows_for_sources(
            conn,
            symbol_id=symbol_id,
            timeframe_code=timeframe_code,
            after_ts=after_ts,
            limit=limit,
            sources=[1],
        )

    async def _fetch_bar_rows_for_sources(
        self,
        conn,
        *,
        symbol_id: int,
        timeframe_code: int,
        after_ts,
        limit: int | None,
        sources: list[int],
    ):
        is_period_timeframe = timeframe_code in {10080, 43200}
        period_cte = """
            ), daily_period_ends as (
                select case when $2 = 10080 then date_trunc('week', daily.ts at time zone 'Asia/Shanghai')
                            else date_trunc('month', daily.ts at time zone 'Asia/Shanghai') end as period_key,
                       max((date_trunc('day', daily.ts at time zone 'Asia/Shanghai') + interval '15 hours') at time zone 'Asia/Shanghai') as final_ts
                from klines daily
                where daily.symbol_id = $1 and daily.timeframe = 1440 and daily.source = any($3::smallint[])
                  and ($4::timestamptz is null or daily.ts > $4 - interval '7 days')
                group by 1
        """ if is_period_timeframe else ""
        period_join = "left join daily_period_ends period_ends on period_ends.period_key = canonical.period_key" if is_period_timeframe else ""
        final_ts = "period_ends.final_ts" if is_period_timeframe else "null::timestamptz"
        return await conn.fetch(
            f"""
            with canonical as (
                select
                    case when k.timeframe in (1440, 10080, 43200)
                        then (date_trunc('day', k.ts at time zone 'Asia/Shanghai') + interval '15 hours') at time zone 'Asia/Shanghai'
                    else k.ts end as ts,
                    case when k.timeframe = 10080 then date_trunc('week', k.ts at time zone 'Asia/Shanghai')
                         when k.timeframe = 43200 then date_trunc('month', k.ts at time zone 'Asia/Shanghai')
                         else case when k.timeframe = 1440 then date_trunc('day', k.ts at time zone 'Asia/Shanghai') else k.ts at time zone 'Asia/Shanghai' end end as period_key,
                    k.open_x1000,
                    k.high_x1000,
                    k.low_x1000,
                    k.close_x1000,
                    k.volume,
                    k.amount_x100,
                    k.is_complete,
                    k.revision,
                    k.source,
                    k.updated_at
                from klines k
                where k.symbol_id = $1
                  and k.timeframe = $2
                  and k.source = any($3::smallint[])
            {period_cte}), covered as (
                select canonical.*, max(coverage.covered_until) as parquet_coverage_end, {final_ts} as final_ts
                from canonical
                left join kline_source_coverage coverage
                  on coverage.symbol_id = $1
                 and coverage.timeframe = $2
                 and coverage.source in (4, 9)
                {period_join}
                group by canonical.ts, canonical.open_x1000, canonical.high_x1000,
                     canonical.low_x1000, canonical.close_x1000, canonical.volume,
                     canonical.amount_x100, canonical.is_complete, canonical.revision,
                         canonical.source, canonical.updated_at, canonical.period_key, {final_ts}
            ), ranked as (
                select *, row_number() over (
                    partition by period_key
                    order by ({source_priority_case('source', timestamp='ts', coverage_end='parquet_coverage_end')}) desc,
                             is_complete desc, revision desc, updated_at desc
                ) as rn
                from covered
                where ($4::timestamptz is null or ts > $4)
                  and ($2 <> 10080 or period_key < date_trunc('week', now() at time zone 'Asia/Shanghai'))
                  and ($2 <> 43200 or period_key < date_trunc('month', now() at time zone 'Asia/Shanghai'))
            )
            select
                coalesce(final_ts, ts) as ts,
                k.open_x1000,
                k.high_x1000,
                k.low_x1000,
                k.close_x1000,
                k.volume,
                k.amount_x100,
                k.is_complete,
                k.revision,
                source,
                updated_at
            from ranked k
            where rn = 1
            order by ts asc
            limit coalesce($5::int, 2147483647)
            """,
            symbol_id,
            timeframe_code,
            sources,
            after_ts,
            limit,
        )


def _rows_to_canonical_bars(
    symbol: str,
    timeframe: str,
    rows,
    *,
    parquet_coverage_end: datetime | None = None,
) -> list[Bar]:
    normalized = normalize_timeframe(timeframe)
    winners: dict[tuple[str, object], dict] = {}
    period_final_ts: dict[tuple[str, object], object] = {}
    fallback_updated_at = datetime.min.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    materialized_rows = [dict(row) for row in rows]
    if parquet_coverage_end is None:
        parquet_coverage_end = max(
            (row["ts"] for row in materialized_rows if int(row.get("source", 0)) in {4, 9}),
            default=None,
        )
    for row in materialized_rows:
        try:
            _unused, logical_ts = kline_logical_key(normalized, row["ts"])
            local = row["ts"].astimezone(ZoneInfo("Asia/Shanghai"))
            canonical_ts = canonical_kline_timestamp(
                normalized,
                row["ts"],
                date_only=normalized in {"1d", "1w", "1m"} and (local.hour, local.minute) == (0, 0),
            )
        except ValueError:
            continue
        row["ts"] = canonical_ts
        key = (normalized, logical_ts)
        period_final_ts[key] = max(canonical_ts, period_final_ts.get(key, canonical_ts))
        source = int(row.get("source", 0))
        order = (
            source_priority_with_coverage(source, canonical_ts, parquet_coverage_end),
            bool(row.get("is_complete", False)),
            int(row.get("revision", 0)),
            row.get("updated_at") or fallback_updated_at,
        )
        existing = winners.get(key)
        if existing is None or order > (
            source_priority_with_coverage(int(existing.get("source", 0)), existing["ts"], parquet_coverage_end),
            bool(existing.get("is_complete", False)),
            int(existing.get("revision", 0)),
            existing.get("updated_at") or fallback_updated_at,
        ):
            winners[key] = row
    for key, winner in winners.items():
        winner["ts"] = period_final_ts[key]
    return [
        Bar(
            symbol=symbol,
            timeframe=normalized,
            ts=row["ts"],
            open=row["open_x1000"] / 1000,
            high=row["high_x1000"] / 1000,
            low=row["low_x1000"] / 1000,
            close=row["close_x1000"] / 1000,
            volume=row["volume"],
            amount=None if row["amount_x100"] is None else row["amount_x100"] / 100,
            complete=row["is_complete"],
            revision=row["revision"],
            source=code_to_source(row["source"]),
        )
        for row in sorted(winners.values(), key=lambda item: item["ts"])
    ]
