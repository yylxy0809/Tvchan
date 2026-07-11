from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from statistics import median

import asyncpg


BSP_TYPES = ("1", "2", "2s", "3a", "3b")


async def build_weekly_signal_distribution(pool: asyncpg.Pool, *, as_of_time: datetime) -> dict:
    async with pool.acquire() as conn:
        symbol_rows = await conn.fetch(
            """
            select id, code, exchange, name
            from symbols
            where is_active = true
            order by id
            """
        )
        meta = {
            int(row["id"]): {
                "symbol": f"{row['code']}.{row['exchange']}",
                "name": row["name"],
            }
            for row in symbol_rows
        }
        run_rows = await conn.fetch(
            """
            select distinct on (r.symbol_id)
                r.symbol_id, r.id as run_id, r.bar_until
            from chan_c_runs r
            join symbols s on s.id = r.symbol_id
            where s.is_active = true
              and r.chan_level = 10080
              and r.status = 'success'
              and r.bar_until <= $1
            order by r.symbol_id, r.bar_until desc, r.computed_at desc
            """,
            as_of_time,
        )
        run_by_symbol = {int(row["symbol_id"]): int(row["run_id"]) for row in run_rows}
        signal_rows = await conn.fetch(
            """
            with latest_runs as (
                select distinct on (r.symbol_id)
                    r.symbol_id, r.id as run_id
                from chan_c_runs r
                join symbols s on s.id = r.symbol_id
                where s.is_active = true
                  and r.chan_level = 10080
                  and r.status = 'success'
                  and r.bar_until <= $1
                order by r.symbol_id, r.bar_until desc, r.computed_at desc
            )
            select lr.symbol_id,
                   lr.run_id,
                   coalesce(cs.base_ts, cs.ts) as point_time,
                   cs.price_x1000,
                   cs.signal_type,
                   cs.extra
            from latest_runs lr
            join chan_c_signals cs on cs.run_id = lr.run_id
            where cs.mode = 2
              and cs.chan_level = 10080
            order by lr.symbol_id, coalesce(cs.base_ts, cs.ts), cs.id
            """,
            as_of_time,
        )

        b2_symbol_ids = set()
        grouped_signals: dict[int, list[dict]] = defaultdict(list)
        for row in signal_rows:
            extra = row["extra"]
            if isinstance(extra, str):
                extra = json.loads(extra)
            extra = extra if isinstance(extra, dict) else {}
            signal = {
                "point_time": row["point_time"],
                "price": row["price_x1000"] / 1000,
                "signal_type": str(row["signal_type"]),
                "side": extra.get("side"),
                "bsp_type": extra.get("bsp_type"),
            }
            grouped_signals[int(row["symbol_id"])].append(signal)
            if signal["side"] == "buy" and signal["bsp_type"] == "2":
                b2_symbol_ids.add(int(row["symbol_id"]))

        weekly_bars_rows = await conn.fetch(
            """
            select symbol_id, ts, close_x1000
            from klines
            where symbol_id = any($1::integer[])
              and timeframe = 10080
              and source = any(array[2,3,4,5,6,7,8,9]::smallint[])
              and ts <= $2
            order by symbol_id, ts
            """,
            list(b2_symbol_ids) or [0],
            as_of_time,
        )

    closes_by_symbol: dict[int, list[tuple[datetime, float]]] = defaultdict(list)
    for row in weekly_bars_rows:
        closes_by_symbol[int(row["symbol_id"])].append((row["ts"], row["close_x1000"] / 1000))

    symbols_by_type = {bsp_type: set() for bsp_type in BSP_TYPES}
    latest_buy_type_distribution = Counter()
    weekly_b1_no_b2 = []
    weekly_b2_no_prior_b1 = []
    weekly_b2_break_b1 = []
    weekly_b2_dif_le_zero = []

    for symbol_id, signals in grouped_signals.items():
        buy_signals = [item for item in signals if item["side"] == "buy" and item["bsp_type"] in BSP_TYPES]
        if not buy_signals:
            continue
        for bsp_type in BSP_TYPES:
            if any(item["bsp_type"] == bsp_type for item in buy_signals):
                symbols_by_type[bsp_type].add(symbol_id)
        latest_buy_type_distribution[buy_signals[-1]["bsp_type"]] += 1

        b1_candidates = [item for item in buy_signals if item["bsp_type"] == "1"]
        b2_candidates = [item for item in buy_signals if item["bsp_type"] == "2"]
        if b1_candidates and not b2_candidates:
            weekly_b1_no_b2.append(symbol_id)
        if not b2_candidates:
            continue
        latest_b2 = b2_candidates[-1]
        prior_b1 = [item for item in b1_candidates if item["point_time"] < latest_b2["point_time"]]
        if not prior_b1:
            weekly_b2_no_prior_b1.append(symbol_id)
            continue
        latest_b1 = prior_b1[-1]
        if latest_b2["price"] <= latest_b1["price"]:
            weekly_b2_break_b1.append(symbol_id)

        macd_row = _macd_at(closes_by_symbol.get(symbol_id, []), latest_b2["point_time"])
        if macd_row is not None and macd_row["dif"] <= 0:
            weekly_b2_dif_le_zero.append(symbol_id)

    report = {
        "as_of_time": as_of_time.isoformat(),
        "active_symbols_total": len(meta),
        "weekly_run_coverage": {
            "symbols_with_latest_weekly_run": len(run_by_symbol),
            "ratio": round(len(run_by_symbol) / (len(meta) or 1), 6),
        },
        "weekly_buy_point_distribution": {
            bsp_type: {
                "symbol_count": len(symbols_by_type[bsp_type]),
                "ratio": round(len(symbols_by_type[bsp_type]) / (len(meta) or 1), 6),
                "sample_symbols": _sample_symbols(symbols_by_type[bsp_type], meta),
            }
            for bsp_type in BSP_TYPES
        },
        "latest_weekly_buy_type_distribution": dict(latest_buy_type_distribution),
        "weekly_b1_without_weekly_b2": _count_and_samples(weekly_b1_no_b2, meta),
        "weekly_b2_without_prior_weekly_b1": _count_and_samples(weekly_b2_no_prior_b1, meta),
        "weekly_b2_break_weekly_b1": _count_and_samples(weekly_b2_break_b1, meta),
        "weekly_b2_dif_le_zero": _count_and_samples(weekly_b2_dif_le_zero, meta),
        "signal_rows_total": len(signal_rows),
        "run_rows_total": len(run_rows),
        "weekly_bar_rows_for_b2_symbols": len(weekly_bars_rows),
    }
    return report


def render_weekly_signal_distribution_markdown(report: dict) -> str:
    lines = [
        "# Weekly Signal Distribution",
        "",
        f"- As of: `{report['as_of_time']}`",
        f"- Active symbols: `{report['active_symbols_total']}`",
        f"- Weekly run coverage: `{report['weekly_run_coverage']['symbols_with_latest_weekly_run']}`",
        "",
        "## Weekly Buy Point Distribution",
        "",
    ]
    for bsp_type, payload in report["weekly_buy_point_distribution"].items():
        lines.append(
            f"- `B{bsp_type}`: symbols=`{payload['symbol_count']}` ratio=`{payload['ratio']}` samples=`{payload['sample_symbols']}`"
        )
    lines.extend(
        [
            "",
            "## Latest Weekly Buy Type Distribution",
            "",
            f"- `{json.dumps(report['latest_weekly_buy_type_distribution'], ensure_ascii=False)}`",
            "",
            "## Diagnostics",
            "",
            f"- Weekly B1 without weekly B2: `{json.dumps(report['weekly_b1_without_weekly_b2'], ensure_ascii=False)}`",
            f"- Weekly B2 without prior weekly B1: `{json.dumps(report['weekly_b2_without_prior_weekly_b1'], ensure_ascii=False)}`",
            f"- Weekly B2 break weekly B1: `{json.dumps(report['weekly_b2_break_weekly_b1'], ensure_ascii=False)}`",
            f"- Weekly B2 DIF<=0: `{json.dumps(report['weekly_b2_dif_le_zero'], ensure_ascii=False)}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _count_and_samples(symbol_ids: list[int], meta: dict[int, dict[str, str]]) -> dict:
    unique_ids = sorted(set(symbol_ids))
    return {
        "count": len(unique_ids),
        "ratio": round(len(unique_ids) / (len(meta) or 1), 6),
        "sample_symbols": _sample_symbols(set(unique_ids), meta),
    }


def _sample_symbols(symbol_ids: set[int], meta: dict[int, dict[str, str]], *, limit: int = 20) -> list[str]:
    return [meta[symbol_id]["symbol"] for symbol_id in sorted(symbol_ids)[:limit]]


def _macd_at(points: list[tuple[datetime, float]], point_time: datetime) -> dict | None:
    if not points:
        return None
    difs, deas = _compute_macd(points)
    result = None
    for idx, (ts, _) in enumerate(points):
        if ts <= point_time:
            result = {"ts": ts, "dif": difs[idx], "dea": deas[idx]}
        else:
            break
    return result


def _compute_macd(points: list[tuple[datetime, float]]) -> tuple[list[float], list[float]]:
    closes = [close for _, close in points]
    ema_fast = _ema(closes, 12)
    ema_slow = _ema(closes, 26)
    difs = [fast - slow for fast, slow in zip(ema_fast, ema_slow)]
    deas = _ema(difs, 9)
    return difs, deas


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    result: list[float] = []
    current = values[0]
    for value in values:
        current = alpha * value + (1 - alpha) * current
        result.append(current)
    return result
