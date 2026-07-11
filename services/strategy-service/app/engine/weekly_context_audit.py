from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from app.domain.enums import LEVEL_TO_DB


def _same_price(left: float, right: float) -> bool:
    return abs(left - right) <= 0.0005


async def build_weekly_context_audit(pool: asyncpg.Pool, *, as_of_time: datetime) -> dict[str, Any]:
    async with pool.acquire() as conn:
        symbol_rows = await conn.fetch(
            """
            with latest_weekly_run as (
                select distinct on (r.symbol_id)
                    r.id as run_id,
                    r.symbol_id,
                    r.bar_until
                from chan_c_runs r
                join symbols s on s.id = r.symbol_id
                where s.is_active = true
                  and r.chan_level = $1
                  and r.status = 'success'
                  and r.bar_until <= $2
                order by r.symbol_id, r.bar_until desc, r.id desc
            )
            select s.id as symbol_id, s.code, s.exchange, s.name, lwr.run_id, lwr.bar_until
            from symbols s
            left join latest_weekly_run lwr on lwr.symbol_id = s.id
            where s.is_active = true
            order by s.code
            """,
            LEVEL_TO_DB["1w"],
            as_of_time,
        )
        run_ids = [int(row["run_id"]) for row in symbol_rows if row["run_id"] is not None]
        signal_rows = []
        if run_ids:
            signal_rows = await conn.fetch(
                """
                select run_id, id, ts, coalesce(base_ts, ts) as base_ts, base_seq, price_x1000, signal_type, is_confirmed, extra
                from chan_c_signals
                where run_id = any($1::bigint[])
                  and mode = 2
                order by run_id, coalesce(base_ts, ts), id
                """,
                run_ids,
            )

    symbol_meta: dict[int, dict[str, Any]] = {}
    run_to_symbol: dict[int, int] = {}
    active_symbols = 0
    for row in symbol_rows:
        symbol_id = int(row["symbol_id"])
        active_symbols += 1
        symbol = f"{row['code']}.{row['exchange']}"
        symbol_meta[symbol_id] = {
            "symbol_id": symbol_id,
            "symbol": symbol,
            "name": row["name"],
            "run_id": int(row["run_id"]) if row["run_id"] is not None else None,
            "bar_until": row["bar_until"].isoformat() if row["bar_until"] is not None else None,
        }
        if row["run_id"] is not None:
            run_to_symbol[int(row["run_id"])] = symbol_id

    timelines: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in signal_rows:
        run_id = int(row["run_id"])
        symbol_id = run_to_symbol.get(run_id)
        if symbol_id is None:
            continue
        extra = row["extra"]
        if isinstance(extra, str):
            extra = json.loads(extra)
        extra = extra if isinstance(extra, dict) else {}
        timelines[symbol_id].append(
            {
                "signal_id": int(row["id"]),
                "run_id": run_id,
                "point_time": row["base_ts"],
                "base_ts": row["base_ts"],
                "base_seq": int(row["base_seq"]) if row["base_seq"] is not None else None,
                "price": row["price_x1000"] / 1000,
                "signal_type": str(row["signal_type"]),
                "side": extra.get("side"),
                "bsp_type": extra.get("bsp_type"),
                "confirmed": bool(row["is_confirmed"]),
            }
        )

    categories = {
        "B2_with_prior_B1": [],
        "B2_without_prior_B1": [],
        "B2_same_bar_with_B1": [],
        "B2s_with_prior_B1": [],
        "B2s_without_prior_B1": [],
        "B2s_same_bar_with_B1": [],
    }
    symbol_sets = {
        "symbols_with_weekly_B1_any": set(),
        "symbols_with_weekly_B2_any": set(),
        "symbols_with_weekly_B2s_any": set(),
    }
    b2_gap_counter: Counter[str] = Counter()
    b2s_gap_counter: Counter[str] = Counter()
    sample_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []

    for symbol_id, meta in symbol_meta.items():
        timeline = timelines.get(symbol_id, [])
        buy_signals = [signal for signal in timeline if signal["side"] == "buy"]
        b1_signals = [signal for signal in buy_signals if signal["bsp_type"] == "1"]
        b2_signals = [signal for signal in buy_signals if signal["bsp_type"] == "2"]
        b2s_signals = [signal for signal in buy_signals if signal["bsp_type"] == "2s"]
        if b1_signals:
            symbol_sets["symbols_with_weekly_B1_any"].add(symbol_id)
        if b2_signals:
            symbol_sets["symbols_with_weekly_B2_any"].add(symbol_id)
        if b2s_signals:
            symbol_sets["symbols_with_weekly_B2s_any"].add(symbol_id)

        for signal in b2_signals + b2s_signals:
            prior_b1 = [item for item in b1_signals if item["point_time"] < signal["point_time"]]
            same_bar_b1 = [item for item in b1_signals if item["point_time"] == signal["point_time"]]
            latest_prior_b1 = prior_b1[-1] if prior_b1 else None
            latest_same_bar_b1 = same_bar_b1[-1] if same_bar_b1 else None
            same_bar = latest_same_bar_b1 is not None
            same_price = same_bar and _same_price(latest_same_bar_b1["price"], signal["price"])
            gap_bars = None
            if latest_prior_b1 is not None and latest_prior_b1["base_seq"] is not None and signal["base_seq"] is not None:
                gap_bars = max(0, int(signal["base_seq"]) - int(latest_prior_b1["base_seq"]))

            signal_key = "B2s" if signal["bsp_type"] == "2s" else "B2"
            if latest_prior_b1 is not None:
                categories[f"{signal_key}_with_prior_B1"].append(symbol_id)
                if gap_bars is not None:
                    (b2s_gap_counter if signal_key == "B2s" else b2_gap_counter)[str(gap_bars)] += 1
            else:
                categories[f"{signal_key}_without_prior_B1"].append(symbol_id)
            if same_bar:
                categories[f"{signal_key}_same_bar_with_B1"].append(symbol_id)

            event_row = {
                "symbol": meta["symbol"],
                "name": meta["name"],
                "run_id": meta["run_id"],
                "bsp_type": signal["bsp_type"],
                "point_time": signal["point_time"].isoformat(),
                "price": signal["price"],
                "has_prior_b1": latest_prior_b1 is not None,
                "prior_b1_time": latest_prior_b1["point_time"].isoformat() if latest_prior_b1 else None,
                "prior_b1_price": latest_prior_b1["price"] if latest_prior_b1 else None,
                "same_bar_with_b1": same_bar,
                "same_price_with_b1": same_price,
                "same_bar_b1_time": latest_same_bar_b1["point_time"].isoformat() if latest_same_bar_b1 else None,
                "same_bar_b1_price": latest_same_bar_b1["price"] if latest_same_bar_b1 else None,
                "gap_bars": gap_bars,
            }
            event_rows.append(event_row)

        seen_categories: set[str] = set()
        for signal in b2_signals + b2s_signals:
            prior_b1 = [item for item in b1_signals if item["point_time"] < signal["point_time"]]
            same_bar_b1 = [item for item in b1_signals if item["point_time"] == signal["point_time"]]
            signal_key = "B2s" if signal["bsp_type"] == "2s" else "B2"
            category = f"{signal_key}_{'with_prior_B1' if prior_b1 else 'without_prior_B1'}"
            if category not in seen_categories and len([row for row in sample_rows if row["category"] == category]) < 20:
                sample_rows.append(
                    {
                        "category": category,
                        "symbol": meta["symbol"],
                        "name": meta["name"],
                        "run_id": meta["run_id"],
                        "signal": {
                            "bsp_type": signal["bsp_type"],
                            "point_time": signal["point_time"].isoformat(),
                            "price": signal["price"],
                        },
                        "same_bar_with_b1": bool(same_bar_b1),
                        "same_price_with_b1": bool(
                            same_bar_b1 and _same_price(same_bar_b1[-1]["price"], signal["price"])
                        ),
                        "timeline": [_serialize_signal(item) for item in timeline[-20:]],
                    }
                )
                seen_categories.add(category)
            same_bar_category = f"{signal_key}_same_bar_with_B1"
            if same_bar_b1 and same_bar_category not in seen_categories and len([row for row in sample_rows if row["category"] == same_bar_category]) < 20:
                sample_rows.append(
                    {
                        "category": same_bar_category,
                        "symbol": meta["symbol"],
                        "name": meta["name"],
                        "run_id": meta["run_id"],
                        "signal": {
                            "bsp_type": signal["bsp_type"],
                            "point_time": signal["point_time"].isoformat(),
                            "price": signal["price"],
                        },
                        "same_bar_with_b1": True,
                        "same_price_with_b1": _same_price(same_bar_b1[-1]["price"], signal["price"]),
                        "timeline": [_serialize_signal(item) for item in timeline[-20:]],
                    }
                )
                seen_categories.add(same_bar_category)

    report = {
        "as_of_time": as_of_time.isoformat(),
        "active_symbols": active_symbols,
        "symbols_with_weekly_B1_any": len(symbol_sets["symbols_with_weekly_B1_any"]),
        "symbols_with_weekly_B2_any": len(symbol_sets["symbols_with_weekly_B2_any"]),
        "symbols_with_weekly_B2s_any": len(symbol_sets["symbols_with_weekly_B2s_any"]),
        "B2_with_prior_B1": len(categories["B2_with_prior_B1"]),
        "B2_without_prior_B1": len(categories["B2_without_prior_B1"]),
        "B2_same_bar_with_B1": len(categories["B2_same_bar_with_B1"]),
        "B2s_with_prior_B1": len(categories["B2s_with_prior_B1"]),
        "B2s_without_prior_B1": len(categories["B2s_without_prior_B1"]),
        "B2s_same_bar_with_B1": len(categories["B2s_same_bar_with_B1"]),
        "B2_after_B1_gap_bars_distribution": dict(sorted(b2_gap_counter.items(), key=lambda item: int(item[0]))),
        "B2s_after_B1_gap_bars_distribution": dict(sorted(b2s_gap_counter.items(), key=lambda item: int(item[0]))),
        "sample_symbols": {
            key: _sample_symbol_list(value, symbol_meta)
            for key, value in categories.items()
        },
    }
    return {
        "report": report,
        "events": event_rows,
        "samples": sample_rows,
    }


def write_weekly_context_audit(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    report = payload["report"]
    events = payload["events"]
    samples = payload["samples"]
    (output_dir / "weekly_context_audit.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "weekly_context_audit.md").write_text(
        render_weekly_context_audit_markdown(report),
        encoding="utf-8",
    )
    with (output_dir / "weekly_context_events.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "name",
                "run_id",
                "bsp_type",
                "point_time",
                "price",
                "has_prior_b1",
                "prior_b1_time",
                "prior_b1_price",
                "same_bar_with_b1",
                "same_price_with_b1",
                "same_bar_b1_time",
                "same_bar_b1_price",
                "gap_bars",
            ],
        )
        writer.writeheader()
        writer.writerows(events)
    with (output_dir / "weekly_context_samples.jsonl").open("w", encoding="utf-8") as handle:
        for sample in samples:
            handle.write(json.dumps(sample, ensure_ascii=False) + "\n")


def render_weekly_context_audit_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Weekly Context Audit",
        "",
        f"- As of: `{report['as_of_time']}`",
        f"- Active symbols: `{report['active_symbols']}`",
        f"- symbols_with_weekly_B1_any: `{report['symbols_with_weekly_B1_any']}`",
        f"- symbols_with_weekly_B2_any: `{report['symbols_with_weekly_B2_any']}`",
        f"- symbols_with_weekly_B2s_any: `{report['symbols_with_weekly_B2s_any']}`",
        "",
        "## Event Counts",
        "",
        f"- B2_with_prior_B1: `{report['B2_with_prior_B1']}`",
        f"- B2_without_prior_B1: `{report['B2_without_prior_B1']}`",
        f"- B2_same_bar_with_B1: `{report['B2_same_bar_with_B1']}`",
        f"- B2s_with_prior_B1: `{report['B2s_with_prior_B1']}`",
        f"- B2s_without_prior_B1: `{report['B2s_without_prior_B1']}`",
        f"- B2s_same_bar_with_B1: `{report['B2s_same_bar_with_B1']}`",
        "",
        "## Gap Distribution",
        "",
        f"- B2_after_B1_gap_bars_distribution: `{json.dumps(report['B2_after_B1_gap_bars_distribution'], ensure_ascii=False)}`",
        f"- B2s_after_B1_gap_bars_distribution: `{json.dumps(report['B2s_after_B1_gap_bars_distribution'], ensure_ascii=False)}`",
        "",
        "## Sample Symbols",
        "",
    ]
    for key, value in report["sample_symbols"].items():
        lines.append(f"- `{key}`: `{value}`")
    return "\n".join(lines) + "\n"


def _serialize_signal(signal: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_id": signal["signal_id"],
        "run_id": signal["run_id"],
        "point_time": signal["point_time"].isoformat(),
        "base_ts": signal["base_ts"].isoformat(),
        "base_seq": signal["base_seq"],
        "price": signal["price"],
        "signal_type": signal["signal_type"],
        "side": signal["side"],
        "bsp_type": signal["bsp_type"],
        "confirmed": signal["confirmed"],
    }


def _sample_symbol_list(symbol_ids: list[int], symbol_meta: dict[int, dict[str, Any]], *, limit: int = 20) -> list[str]:
    return [symbol_meta[symbol_id]["symbol"] for symbol_id in symbol_ids[:limit] if symbol_id in symbol_meta]
