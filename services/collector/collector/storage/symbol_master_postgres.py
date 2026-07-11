from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from trading_protocol import SymbolInfo, normalize_timeframe
from trading_protocol.timeframes import TIMEFRAMES

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class SymbolMasterRefreshResult:
    stage_count: int
    active_before: int
    would_activate: int
    would_deactivate: int
    activated_or_inserted: int
    deactivated: int
    dry_run: bool


@dataclass(frozen=True)
class SymbolBarAuditResult:
    target_date: date
    timeframe: str
    active_before: int
    would_deactivate: int
    deactivated: int
    dry_run: bool


class PostgresSymbolMasterStore:
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

    async def __aenter__(self) -> "PostgresSymbolMasterStore":
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError("asyncpg is required for symbol master refresh.") from exc
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

    async def refresh_provider_symbols(
        self,
        symbols: Iterable[SymbolInfo],
        *,
        deactivate_missing: bool = True,
        dry_run: bool = False,
    ) -> SymbolMasterRefreshResult:
        assert self._pool is not None
        rows = [
            (item.code, item.exchange, item.name or item.symbol, item.asset_type, item.market)
            for item in symbols
        ]
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _load_stage(conn, rows)
                active_before = await _active_a_share_count(conn)
                would_activate = await conn.fetchval(
                    """
                    select count(*)::int
                    from _symbol_master_stage stage
                    left join symbols s
                      on s.exchange = stage.exchange
                     and s.code = stage.code
                    where s.id is null
                       or s.is_active = false
                    """
                )
                would_deactivate = 0
                if deactivate_missing:
                    would_deactivate = await conn.fetchval(
                        """
                        select count(*)::int
                        from symbols s
                        where s.asset_type = 'stock'
                          and s.market = 'A_SHARE'
                          and s.is_active = true
                          and not exists (
                              select 1
                              from _symbol_master_stage stage
                              where stage.exchange = s.exchange
                                and stage.code = s.code
                          )
                        """
                    )
                activated_or_inserted = 0
                deactivated = 0
                if not dry_run:
                    upsert_result = await conn.execute(
                        """
                        insert into symbols (code, exchange, name, asset_type, market, is_active)
                        select code, exchange, name, asset_type, market, true
                        from _symbol_master_stage
                        on conflict (exchange, code) do update
                        set name = case
                                when excluded.name in (
                                    excluded.code,
                                    excluded.code || '.' || excluded.exchange
                                )
                                    then symbols.name
                                else excluded.name
                            end,
                            asset_type = excluded.asset_type,
                            market = excluded.market,
                            is_active = true,
                            updated_at = now()
                        """
                    )
                    activated_or_inserted = _affected_rows(upsert_result)
                    if deactivate_missing:
                        deactivate_result = await conn.execute(
                            """
                            update symbols s
                            set is_active = false,
                                updated_at = now()
                            where s.asset_type = 'stock'
                              and s.market = 'A_SHARE'
                              and s.is_active = true
                              and not exists (
                                  select 1
                                  from _symbol_master_stage stage
                                  where stage.exchange = s.exchange
                                    and stage.code = s.code
                              )
                            """
                        )
                        deactivated = _affected_rows(deactivate_result)
        return SymbolMasterRefreshResult(
            stage_count=len(rows),
            active_before=active_before,
            would_activate=int(would_activate or 0),
            would_deactivate=int(would_deactivate or 0),
            activated_or_inserted=activated_or_inserted,
            deactivated=deactivated,
            dry_run=dry_run,
        )

    async def deactivate_symbols_without_bars_on_date(
        self,
        *,
        target_date: date,
        timeframe: str = "5f",
        dry_run: bool = False,
    ) -> SymbolBarAuditResult:
        assert self._pool is not None
        normalized_timeframe = normalize_timeframe(timeframe)
        timeframe_code = TIMEFRAMES[normalized_timeframe].minutes
        day_start = datetime.combine(target_date, time.min, tzinfo=SHANGHAI_TZ)
        day_end = day_start + timedelta(days=1)
        async with self._pool.acquire() as conn:
            active_before = await _active_a_share_count(conn)
            would_deactivate = await conn.fetchval(
                """
                select count(*)::int
                from symbols s
                where s.asset_type = 'stock'
                  and s.market = 'A_SHARE'
                  and s.is_active = true
                  and not exists (
                      select 1
                      from klines k
                      where k.symbol_id = s.id
                        and k.timeframe = $1
                        and k.ts >= $2
                        and k.ts < $3
                  )
                """,
                timeframe_code,
                day_start,
                day_end,
            )
            deactivated = 0
            if not dry_run:
                result = await conn.execute(
                    """
                    update symbols s
                    set is_active = false,
                        updated_at = now()
                    where s.asset_type = 'stock'
                      and s.market = 'A_SHARE'
                      and s.is_active = true
                      and not exists (
                          select 1
                          from klines k
                          where k.symbol_id = s.id
                            and k.timeframe = $1
                            and k.ts >= $2
                            and k.ts < $3
                      )
                    """,
                    timeframe_code,
                    day_start,
                    day_end,
                )
                deactivated = _affected_rows(result)
        return SymbolBarAuditResult(
            target_date=target_date,
            timeframe=normalized_timeframe,
            active_before=active_before,
            would_deactivate=int(would_deactivate or 0),
            deactivated=deactivated,
            dry_run=dry_run,
        )


async def _load_stage(conn, rows: list[tuple[str, str, str, str, str]]) -> None:
    await conn.execute(
        """
        create temporary table _symbol_master_stage (
            code varchar(16) not null,
            exchange varchar(8) not null,
            name varchar(64) not null,
            asset_type varchar(16) not null,
            market varchar(16) not null,
            primary key (exchange, code)
        ) on commit drop
        """
    )
    if rows:
        await conn.executemany(
            """
            insert into _symbol_master_stage (code, exchange, name, asset_type, market)
            values ($1, $2, $3, $4, $5)
            on conflict (exchange, code) do update
            set name = excluded.name,
                asset_type = excluded.asset_type,
                market = excluded.market
            """,
            rows,
        )


async def _active_a_share_count(conn) -> int:
    return int(
        await conn.fetchval(
            """
            select count(*)::int
            from symbols
            where asset_type = 'stock'
              and market = 'A_SHARE'
              and is_active = true
            """
        )
        or 0
    )


def _affected_rows(result: str) -> int:
    try:
        return int(result.rsplit(" ", 1)[1])
    except (IndexError, ValueError):
        return 0
