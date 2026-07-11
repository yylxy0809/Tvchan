from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from typing import Any

import asyncpg

from app.domain.models import ScanResult, Trade


class StrategyRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def ensure_default_definition(
        self,
        *,
        strategy_code: str,
        version: str,
        strategy_name: str,
        description: str,
        rule_spec_json: dict[str, Any],
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                insert into strategy_definitions (
                    strategy_code, version, strategy_name, description, rule_spec_json, enabled
                )
                values ($1, $2, $3, $4, $5::jsonb, true)
                on conflict (strategy_code, version)
                do update
                set strategy_name = excluded.strategy_name,
                    description = excluded.description,
                    rule_spec_json = excluded.rule_spec_json,
                    updated_at = now()
                """,
                strategy_code,
                version,
                strategy_name,
                description,
                json.dumps(rule_spec_json, ensure_ascii=False),
            )

    async def insert_scan_result(
        self,
        result: ScanResult,
        *,
        strategy_code: str,
        strategy_version: str,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                insert into strategy_contexts (
                    symbol_id, strategy_code, strategy_version, context_type, status,
                    start_time, weekly_b1_signal_id, weekly_b2_signal_id,
                    weekly_b1_price_x1000, weekly_b2_price_x1000,
                    source_run_id, source_snapshot_version, features_json, reason_json
                )
                values (
                    $1, $2, $3, 'WEEKLY_B2_CONTEXT', 'active', $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb
                )
                """,
                result.symbol.symbol_id,
                strategy_code,
                strategy_version,
                result.weekly_context.weekly_b2.point_time,
                result.weekly_context.weekly_b1.signal_id if result.weekly_context.weekly_b1 else None,
                result.weekly_context.weekly_b2.signal_id,
                int(round(result.weekly_context.weekly_b1.price * 1000)) if result.weekly_context.weekly_b1 else None,
                int(round(result.weekly_context.weekly_b2.price * 1000)),
                result.weekly_context.weekly_b2.run_id,
                result.weekly_context.weekly_b2.snapshot_version,
                json.dumps(
                    {
                        "dif": result.weekly_context.dif,
                        "dea": result.weekly_context.dea,
                        "context_mode": result.weekly_context.context_mode,
                        "weekly_bsp_type": result.weekly_context.weekly_bsp_type,
                        "anchor_time": result.weekly_context.anchor_time.isoformat(),
                        "anchor_source": result.weekly_context.anchor_source,
                        "prior_weekly_b1_found": result.weekly_context.prior_weekly_b1_found,
                        "same_bar_with_b1": result.weekly_context.same_bar_with_b1,
                        "same_price_with_b1": result.weekly_context.same_price_with_b1,
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "latest_close": result.weekly_context.latest_close,
                        "stop_reference_price": result.weekly_context.stop_reference_price,
                        "stop_reference_source": result.weekly_context.stop_reference_source,
                    },
                    ensure_ascii=False,
                ),
            )
            await conn.execute(
                """
                insert into strategy_signal_events (
                    symbol_id, strategy_code, strategy_version,
                    event_type, status, source_namespace, source_level,
                    point_time, first_seen_time, confirm_time, price_x1000,
                    source_run_id, source_snapshot_version, confidence_score, strength_score,
                    features_json, reason_json
                )
                values (
                    $1, $2, $3, $4, 'active', 'c', $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15::jsonb
                )
                """,
                result.symbol.symbol_id,
                strategy_code,
                strategy_version,
                result.status.value.upper(),
                "30f" if result.entry.has_30f_b1 else "1d",
                result.daily_setup.daily_b1.point_time,
                result.as_of_time,
                result.daily_setup.daily_b2.point_time if result.daily_setup.daily_b2 and result.daily_setup.daily_b2.confirmed else None,
                int(round(result.daily_setup.daily_b1.price * 1000)),
                result.daily_setup.daily_b1.run_id,
                result.daily_setup.daily_b1.snapshot_version,
                result.entry.confidence_score,
                result.daily_setup.strength_score,
                json.dumps(result.daily_setup.features, ensure_ascii=False),
                json.dumps(result.entry.reasons, ensure_ascii=False),
            )

    async def create_backtest_run(
        self,
        *,
        strategy_code: str,
        strategy_version: str,
        run_name: str,
        run_mode: str,
        start_time: datetime,
        end_time: datetime,
        rule_spec_json: dict[str, Any],
        data_source_json: dict[str, Any],
        notes: str | None = None,
    ) -> int:
        async with self.pool.acquire() as conn:
            run_id = await conn.fetchval(
                """
                insert into strategy_backtest_runs (
                    strategy_code, strategy_version, run_name, run_mode,
                    start_time, end_time, rule_spec_json, data_source_json, notes
                )
                values ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9)
                returning id
                """,
                strategy_code,
                strategy_version,
                run_name,
                run_mode,
                start_time,
                end_time,
                json.dumps(rule_spec_json, ensure_ascii=False),
                json.dumps(data_source_json, ensure_ascii=False),
                notes,
            )
        return int(run_id)

    async def insert_trades(
        self,
        backtest_run_id: int,
        strategy_code: str,
        strategy_version: str,
        trades: list[Trade],
    ) -> None:
        if not trades:
            return
        rows = []
        for trade in trades:
            rows.append(
                (
                    backtest_run_id,
                    trade.symbol.symbol_id,
                    strategy_code,
                    strategy_version,
                    trade.entry_time,
                    int(round(trade.entry_price * 1000)),
                    trade.entry_level,
                    trade.entry_reason,
                    trade.entry_confidence,
                    trade.exit_time,
                    None if trade.exit_price is None else int(round(trade.exit_price * 1000)),
                    trade.exit_reason,
                    int(round(trade.daily_b1_price * 1000)),
                    int(round(trade.stop_price * 1000)),
                    trade.return_pct,
                    trade.max_favorable_pct,
                    trade.max_adverse_pct,
                    trade.holding_bars,
                    trade.holding_days,
                    json.dumps(trade.features, ensure_ascii=False),
                    json.dumps(asdict(trade), ensure_ascii=False, default=str),
                )
            )
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                insert into strategy_backtest_trades (
                    backtest_run_id, symbol_id, strategy_code, strategy_version,
                    entry_time, entry_price_x1000, entry_level, entry_reason, entry_confidence_score,
                    exit_time, exit_price_x1000, exit_reason,
                    daily_b1_price_x1000, stop_price_x1000,
                    return_pct, max_favorable_pct, max_adverse_pct, holding_bars, holding_days,
                    features_json, event_trace_json
                )
                values (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                    $20::jsonb, $21::jsonb
                )
                """,
                rows,
            )

    async def finalize_backtest_run(self, run_id: int, metrics: dict[str, Any], total_symbols: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                update strategy_backtest_runs
                set total_symbols = $2,
                    total_trades = $3,
                    win_rate = $4,
                    avg_return = $5,
                    profit_factor = $6,
                    max_drawdown = $7,
                    avg_holding_bars = $8
                where id = $1
                """,
                run_id,
                total_symbols,
                metrics.get("total_trades", 0),
                metrics.get("win_rate"),
                metrics.get("avg_return"),
                metrics.get("profit_factor"),
                metrics.get("max_drawdown"),
                metrics.get("avg_holding_bars"),
            )
