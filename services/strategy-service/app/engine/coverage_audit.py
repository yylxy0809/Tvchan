from __future__ import annotations

import csv
from datetime import date, datetime
from time import perf_counter

import asyncpg

from app.config.strategy_params import StrategyParams
from app.domain.enums import LEVEL_TO_DB, MarketCapPolicy
from app.repositories.module_c_repo import DB_TO_MODE


LEVELS = ("5f", "30f", "1d", "1w", "1m")
MODES = ("predictive", "confirmed")


async def build_coverage_report(pool: asyncpg.Pool, *, as_of_time: datetime, params: StrategyParams | None = None) -> dict:
    params = params or StrategyParams.default()
    started_at = perf_counter()
    timings: dict[str, float] = {}
    async with pool.acquire() as conn:
        t0 = perf_counter()
        symbol_rows = await conn.fetch(
            """
            select id, code, exchange, name
            from symbols
            where is_active = true
            order by id
            """
        )
        timings["active_symbols"] = perf_counter() - t0

        t0 = perf_counter()
        head_rows = await conn.fetch(
            """
            select h.symbol_id, h.chan_level, h.mode, h.base_to_bar_end
            from scheme2_chan_c_published_heads h
            join symbols s on s.id = h.symbol_id
            where s.is_active = true
              and h.status = 'published'
              and h.base_timeframe = h.chan_level
              and h.mode in ('confirmed', 'predictive')
            """
        )
        timings["published_heads"] = perf_counter() - t0

        t0 = perf_counter()
        run_rows = await conn.fetch(
            """
            select distinct on (r.symbol_id, r.chan_level, r.mode)
                r.symbol_id, r.chan_level, r.mode, r.bar_until
            from chan_c_runs r
            join symbols s on s.id = r.symbol_id
            where s.is_active = true
              and r.status = 'success'
              and r.bar_until <= $1
            order by r.symbol_id, r.chan_level, r.mode, r.bar_until desc, r.computed_at desc
            """,
            as_of_time,
        )
        timings["successful_runs"] = perf_counter() - t0

        t0 = perf_counter()
        watermark_rows = await conn.fetch(
            """
            select wm.symbol_id, wm.timeframe, wm.last_bar_end
            from scheme2_ingest_watermarks wm
            join symbols s on s.id = wm.symbol_id
            where s.is_active = true
              and wm.timeframe = any($1::integer[])
            """,
            [LEVEL_TO_DB[level] for level in LEVELS],
        )
        timings["kline_watermarks"] = perf_counter() - t0

        t0 = perf_counter()
        cap_rows = await conn.fetch(
            """
            select s.id as symbol_id, sf.market_cap_x100, sf.as_of_date
            from symbols s
            left join symbol_fundamentals sf on sf.symbol_id = s.id
            where s.is_active = true
            """
        )
        timings["market_cap"] = perf_counter() - t0

    symbol_meta = {
        int(row["id"]): {
            "symbol": f"{row['code']}.{row['exchange']}",
            "code": row["code"],
            "exchange": row["exchange"],
            "name": row["name"],
        }
        for row in symbol_rows
    }
    active_ids = set(symbol_meta)
    head_buckets = _init_mode_buckets()
    run_buckets = _init_mode_buckets()
    kline_buckets = _init_level_buckets()
    with_cap: set[int] = set()
    above_min: set[int] = set()
    cap_dates: list[date] = []

    for row in head_rows:
        level = _level_name(int(row["chan_level"]))
        mode = str(row["mode"])
        bucket = head_buckets[level][mode]
        bucket["ids"].add(int(row["symbol_id"]))
        if row["base_to_bar_end"] is not None:
            bucket["times"].append(row["base_to_bar_end"])

    for row in run_rows:
        level = _level_name(int(row["chan_level"]))
        mode = DB_TO_MODE.get(int(row["mode"]))
        if mode is None:
            continue
        bucket = run_buckets[level][mode]
        bucket["ids"].add(int(row["symbol_id"]))
        if row["bar_until"] is not None:
            bucket["times"].append(row["bar_until"])

    for row in watermark_rows:
        level = _level_name(int(row["timeframe"]))
        bucket = kline_buckets[level]
        bucket["ids"].add(int(row["symbol_id"]))
        if row["last_bar_end"] is not None:
            bucket["times"].append(row["last_bar_end"])

    min_x100 = params.market_cap_min * 100
    for row in cap_rows:
        symbol_id = int(row["symbol_id"])
        cap_value = row["market_cap_x100"]
        if cap_value is not None:
            with_cap.add(symbol_id)
            if int(cap_value) >= min_x100:
                above_min.add(symbol_id)
        if row["as_of_date"] is not None:
            cap_dates.append(row["as_of_date"])

    report: dict[str, object] = {
        "as_of_time": as_of_time.isoformat(),
        "market_cap_policy": params.market_cap_policy.value,
        "market_cap_min": params.market_cap_min,
        "market_cap_rule_enabled": params.market_cap_min > 0,
        "market_cap_missing_allowed": params.allow_missing_market_cap,
        "active_symbols_total": len(active_ids),
        "published_heads": _summarize_mode_buckets(head_buckets, active_ids, symbol_meta),
        "successful_runs_as_of": _summarize_mode_buckets(run_buckets, active_ids, symbol_meta),
        "kline_coverage_as_of": _summarize_kline_buckets(kline_buckets, active_ids, symbol_meta),
        "market_cap_coverage": _summarize_market_cap(
            cap_rows=cap_rows,
            active_ids=active_ids,
            symbol_meta=symbol_meta,
            market_cap_min=params.market_cap_min,
            with_cap=with_cap,
            above_min=above_min,
            latest_dates=cap_dates,
        ),
    }
    market_cap_coverage = report["market_cap_coverage"]
    report["market_cap_data_coverage_ratio"] = market_cap_coverage["active_with_market_cap_ratio"]
    report["market_cap_hard_filter_effective"] = bool(
        params.market_cap_policy == MarketCapPolicy.REQUIRE
        and market_cap_coverage["active_with_market_cap_ratio"] > 0
    )
    report["scan_eligibility"] = _scan_eligibility(
        active_ids=active_ids,
        symbol_meta=symbol_meta,
        head_buckets=head_buckets,
        run_buckets=run_buckets,
        kline_buckets=kline_buckets,
        with_cap=with_cap,
        above_min=above_min,
    )
    report["coverage_perf"] = {
        "elapsed_seconds": round(perf_counter() - started_at, 3),
        "stage_seconds": {key: round(value, 3) for key, value in timings.items()},
        "row_counts": {
            "active_symbols": len(symbol_rows),
            "published_heads": len(head_rows),
            "successful_runs": len(run_rows),
            "kline_watermarks": len(watermark_rows),
            "market_caps": len(cap_rows),
        },
        "kline_coverage_source": "scheme2_ingest_watermarks",
    }
    return report


def write_coverage_summary_csv(report: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["category", "level", "mode", "count", "ratio", "max_ts"],
        )
        writer.writeheader()
        for category in ("published_heads", "successful_runs_as_of"):
            for level, payload in report[category].items():
                for mode, item in payload.items():
                    writer.writerow(
                        {
                            "category": category,
                            "level": level,
                            "mode": mode,
                            "count": item["count"],
                            "ratio": item["ratio"],
                            "max_ts": item["freshness"]["max_ts"],
                        }
                    )
        for level, item in report["kline_coverage_as_of"].items():
            writer.writerow(
                {
                    "category": "kline_coverage_as_of",
                    "level": level,
                    "mode": "canonical",
                    "count": item["count"],
                    "ratio": item["ratio"],
                    "max_ts": item["freshness"]["max_ts"],
                }
            )


def _init_mode_buckets() -> dict[str, dict[str, dict[str, list | set[int]]]]:
    return {
        level: {mode: {"ids": set(), "times": []} for mode in MODES}
        for level in LEVELS
    }


def _init_level_buckets() -> dict[str, dict[str, list | set[int]]]:
    return {level: {"ids": set(), "times": []} for level in LEVELS}


def _level_name(db_level: int) -> str:
    for key, value in LEVEL_TO_DB.items():
        if value == int(db_level):
            return key
    raise KeyError(db_level)


def _freshness_summary(values: list[datetime]) -> dict[str, str | int | None]:
    if not values:
        return {"covered_symbols": 0, "min_ts": None, "p50_ts": None, "max_ts": None}
    ordered = sorted(values)
    p50_value = ordered[(len(ordered) - 1) // 2]
    return {
        "covered_symbols": len(ordered),
        "min_ts": ordered[0].isoformat(),
        "p50_ts": p50_value.isoformat(),
        "max_ts": ordered[-1].isoformat(),
    }


def _sample_symbols(ids: set[int], symbol_meta: dict[int, dict[str, str]], *, limit: int = 20) -> list[str]:
    return [symbol_meta[symbol_id]["symbol"] for symbol_id in sorted(ids)[:limit]]


def _summarize_mode_buckets(
    buckets: dict[str, dict[str, dict[str, list | set[int]]]],
    active_ids: set[int],
    symbol_meta: dict[int, dict[str, str]],
) -> dict:
    total = len(active_ids) or 1
    result = {}
    for level in LEVELS:
        result[level] = {}
        for mode in MODES:
            ids = buckets[level][mode]["ids"]
            result[level][mode] = {
                "count": len(ids),
                "ratio": round(len(ids) / total, 6),
                "freshness": _freshness_summary(buckets[level][mode]["times"]),
                "missing_samples": _sample_symbols(active_ids - ids, symbol_meta),
            }
    return result


def _summarize_kline_buckets(
    buckets: dict[str, dict[str, list | set[int]]],
    active_ids: set[int],
    symbol_meta: dict[int, dict[str, str]],
) -> dict:
    total = len(active_ids) or 1
    result = {}
    for level in LEVELS:
        ids = buckets[level]["ids"]
        result[level] = {
            "count": len(ids),
            "ratio": round(len(ids) / total, 6),
            "freshness": _freshness_summary(buckets[level]["times"]),
            "missing_samples": _sample_symbols(active_ids - ids, symbol_meta),
            "source": "scheme2_ingest_watermarks",
        }
    return result


def _summarize_market_cap(
    *,
    cap_rows,
    active_ids: set[int],
    symbol_meta: dict[int, dict[str, str]],
    market_cap_min: int,
    with_cap: set[int],
    above_min: set[int],
    latest_dates: list[date],
) -> dict:
    total = len(active_ids) or 1
    return {
        "symbol_fundamentals_total": len(cap_rows),
        "active_with_market_cap": len(with_cap),
        "active_with_market_cap_ratio": round(len(with_cap) / total, 6),
        "active_above_market_cap_min": len(above_min),
        "active_above_market_cap_min_ratio": round(len(above_min) / total, 6),
        "active_missing_market_cap": len(active_ids - with_cap),
        "market_cap_min": market_cap_min,
        "missing_samples": _sample_symbols(active_ids - with_cap, symbol_meta),
        "below_min_samples": _sample_symbols(with_cap - above_min, symbol_meta),
        "latest_as_of_date": max(latest_dates).isoformat() if latest_dates else None,
    }


def _intersect_level_ids(source: dict, mode: str | None = None) -> set[int]:
    ids: set[int] | None = None
    for level in LEVELS:
        current = source[level]["ids"] if mode is None else source[level][mode]["ids"]
        ids = set(current) if ids is None else ids & set(current)
    return ids or set()


def _scan_eligibility(
    *,
    active_ids: set[int],
    symbol_meta: dict[int, dict[str, str]],
    head_buckets: dict[str, dict[str, dict[str, list | set[int]]]],
    run_buckets: dict[str, dict[str, dict[str, list | set[int]]]],
    kline_buckets: dict[str, dict[str, list | set[int]]],
    with_cap: set[int],
    above_min: set[int],
) -> dict:
    head_complete = _intersect_level_ids(head_buckets, "predictive")
    run_complete = _intersect_level_ids(run_buckets, "predictive")
    kline_complete = _intersect_level_ids(kline_buckets)
    base_eligible = active_ids & head_complete & run_complete & kline_complete
    warn_allowed = base_eligible - (with_cap - above_min)
    require_allowed = warn_allowed & above_min
    return {
        "eligible_symbols_require": len(require_allowed),
        "eligible_symbols_warn_allow_missing": len(warn_allowed),
        "eligible_symbols_ignore": len(base_eligible),
        "eligible_require_samples": _sample_symbols(require_allowed, symbol_meta),
        "eligible_warn_allow_missing_samples": _sample_symbols(warn_allowed, symbol_meta),
        "eligible_ignore_samples": _sample_symbols(base_eligible, symbol_meta),
        "excluded_counts": {
            "missing_published_head_any": len(active_ids - head_complete),
            "missing_success_run_any": len(active_ids - run_complete),
            "missing_kline_any": len(active_ids - kline_complete),
            "missing_market_cap": len(active_ids - with_cap),
            "below_market_cap_min": len(with_cap - above_min),
        },
        "excluded_samples": {
            "missing_published_head_any": _sample_symbols(active_ids - head_complete, symbol_meta),
            "missing_success_run_any": _sample_symbols(active_ids - run_complete, symbol_meta),
            "missing_kline_any": _sample_symbols(active_ids - kline_complete, symbol_meta),
            "missing_market_cap": _sample_symbols(active_ids - with_cap, symbol_meta),
            "below_market_cap_min": _sample_symbols(with_cap - above_min, symbol_meta),
        },
    }


def render_coverage_report_markdown(report: dict) -> str:
    lines = [
        "# Coverage Audit",
        "",
        f"- As of: `{report['as_of_time']}`",
        f"- Active symbols: `{report['active_symbols_total']}`",
        f"- Market cap policy: `{report['market_cap_policy']}`",
        f"- Market cap min: `{report['market_cap_min']}`",
        f"- Market cap rule enabled: `{report['market_cap_rule_enabled']}`",
        f"- Market cap missing allowed: `{report['market_cap_missing_allowed']}`",
        f"- Market cap coverage ratio: `{report['market_cap_data_coverage_ratio']}`",
        f"- Market cap hard filter effective: `{report['market_cap_hard_filter_effective']}`",
        f"- Elapsed seconds: `{report['coverage_perf']['elapsed_seconds']}`",
        "",
    ]
    if report["market_cap_rule_enabled"] and not report["market_cap_hard_filter_effective"]:
        lines.extend(
            [
                "WARNING: market cap data coverage is "
                f"{report['market_cap_data_coverage_ratio']}%; market_cap_min was not effectively enforceable under "
                f"{report['market_cap_policy']}.",
                "",
            ]
        )
    lines.extend(["## Published Heads", ""])
    for level, payload in report["published_heads"].items():
        for mode, item in payload.items():
            lines.append(
                f"- `{level}` `{mode}`: count=`{item['count']}` ratio=`{item['ratio']}` latest=`{item['freshness']['max_ts']}`"
            )
    lines.extend(["", "## Successful Runs As Of", ""])
    for level, payload in report["successful_runs_as_of"].items():
        for mode, item in payload.items():
            lines.append(
                f"- `{level}` `{mode}`: count=`{item['count']}` ratio=`{item['ratio']}` latest=`{item['freshness']['max_ts']}`"
            )
    lines.extend(["", "## Kline Coverage As Of", ""])
    for level, item in report["kline_coverage_as_of"].items():
        lines.append(
            f"- `{level}`: count=`{item['count']}` ratio=`{item['ratio']}` latest=`{item['freshness']['max_ts']}` source=`{item['source']}`"
        )
    lines.extend(["", "## Market Cap Coverage", ""])
    for key, value in report["market_cap_coverage"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Scan Eligibility", ""])
    lines.append(f"- `eligible_symbols_require`: `{report['scan_eligibility']['eligible_symbols_require']}`")
    lines.append(
        f"- `eligible_symbols_warn_allow_missing`: `{report['scan_eligibility']['eligible_symbols_warn_allow_missing']}`"
    )
    lines.append(f"- `eligible_symbols_ignore`: `{report['scan_eligibility']['eligible_symbols_ignore']}`")
    for key, value in report["scan_eligibility"]["excluded_counts"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Performance", ""])
    for key, value in report["coverage_perf"]["stage_seconds"].items():
        lines.append(f"- `{key}`: `{value}`")
    return "\n".join(lines) + "\n"
