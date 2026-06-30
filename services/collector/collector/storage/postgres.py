from __future__ import annotations

from collections.abc import Iterable

from trading_protocol import Bar, SymbolInfo, normalize_timeframe
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
    return {
        "seed": 1,
        "pytdx": 2,
        "tdx_csv": 3,
        "parquet_5f": 4,
    }.get(value, 0)


def code_to_source(value: int) -> str:
    return {
        1: "seed",
        2: "pytdx",
        3: "tdx_csv",
        4: "parquet_5f",
    }.get(value, "database")


def bar_to_db_values(bar: Bar) -> tuple:
    return (
        bar.symbol,
        timeframe_to_db_code(bar.timeframe),
        bar.ts,
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
                set name = excluded.name,
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
            await conn.executemany(
                """
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
                """,
                rows,
            )
        return len(rows)

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
        return [
            Bar(
                symbol=symbol,
                timeframe=normalize_timeframe(timeframe),
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
            for row in rows
        ]

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
        return [
            Bar(
                symbol=symbol,
                timeframe=normalize_timeframe(timeframe),
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
            for row in rows
        ]

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
            sources=[2, 3, 4],
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
        return await conn.fetch(
            """
            select
                k.ts,
                k.open_x1000,
                k.high_x1000,
                k.low_x1000,
                k.close_x1000,
                k.volume,
                k.amount_x100,
                k.is_complete,
                k.revision,
                k.source
            from klines k
            where k.symbol_id = $1
              and k.timeframe = $2
              and k.source = any($3::smallint[])
              and ($4::timestamptz is null or k.ts > $4)
            order by k.ts asc
            limit coalesce($5::int, 2147483647)
            """,
            symbol_id,
            timeframe_code,
            sources,
            after_ts,
            limit,
        )
