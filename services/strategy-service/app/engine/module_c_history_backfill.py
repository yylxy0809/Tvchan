from __future__ import annotations

import csv
import hashlib
import json
import sys
import asyncio
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any, Callable
from zoneinfo import ZoneInfo

import asyncpg
from trading_protocol import MODULE_C_CONFIG_HASH

from app.domain.enums import DB_TO_LEVEL, LEVEL_TO_DB
from app.domain.models import SymbolInfo
from app.repositories.kline_repo import KlineBar, KlineRepository
from app.repositories.module_c_repo import MODE_TO_DB, ModuleCRepository


SH_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_PROFILE = "research_daily_close"
SUPPORTED_PROFILES = {"research_daily_close", "strategy_30f"}
RUN_KIND_HISTORICAL_BACKFILL = "historical_backfill"
DEFAULT_LEVELS = ("5f", "30f", "1d", "1w", "1m")


@dataclass(slots=True)
class BackfillSnapshot:
    level: str
    cutoff_time: datetime


@dataclass(slots=True)
class BackfillRunResult:
    symbol: str
    level: str
    cutoff_time: datetime
    bar_count: int
    run_id: int
    snapshot_version: str
    strokes: int
    segments: int
    centers: int
    signals: int
    elapsed_seconds: float


@dataclass(slots=True)
class BackfillFailure:
    symbol: str
    level: str
    cutoff_time: datetime | None
    error: str


@dataclass(slots=True)
class BackfillPerfSample:
    symbol: str
    level: str
    cutoff_time: datetime
    bar_count: int
    schedule_build_seconds: float
    resume_check_seconds: float
    overlay_build_seconds: float
    db_insert_seconds: float
    total_snapshot_seconds: float


def ensure_module_c_paths() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    collector_root = repo_root / "services" / "collector"
    collector_root_text = str(collector_root)
    if collector_root_text not in sys.path:
        sys.path.insert(0, collector_root_text)


def load_overlay_builder() -> Callable[[dict[str, Any]], dict[str, Any]]:
    ensure_module_c_paths()
    from collector.module_c_adapter import build_overlay

    return build_overlay


def build_snapshot_schedule(
    *,
    profile: str,
    bars_by_level: dict[str, list[KlineBar]],
    backtest_start: datetime,
    end_time: datetime,
    levels: tuple[str, ...] = DEFAULT_LEVELS,
) -> dict[str, list[datetime]]:
    if profile not in SUPPORTED_PROFILES:
        raise ValueError(f"unsupported backfill profile: {profile}")
    schedule: dict[str, list[datetime]] = {}
    for level in levels:
        bars = [bar for bar in bars_by_level.get(level, []) if backtest_start <= bar.ts <= end_time]
        if level in {"1d", "1w", "1m"}:
            schedule[level] = [bar.ts for bar in bars]
            continue
        if profile == "research_daily_close":
            schedule[level] = _latest_bar_per_local_day(bars)
        elif profile == "strategy_30f":
            if level == "5f":
                source = [bar for bar in bars_by_level.get("30f", []) if backtest_start <= bar.ts <= end_time]
                schedule[level] = [bar.ts for bar in source]
            else:
                schedule[level] = [bar.ts for bar in bars]
    return schedule


def build_backfill_dry_run(
    *,
    symbols: list[SymbolInfo],
    bars_by_symbol: dict[str, dict[str, list[KlineBar]]],
    profile: str,
    warmup_start: datetime,
    backtest_start: datetime,
    end_time: datetime,
    levels: tuple[str, ...],
    mode: str,
) -> dict[str, Any]:
    snapshots_by_level = {level: 0 for level in levels}
    snapshots_by_symbol: list[dict[str, Any]] = []
    for symbol in symbols:
        bars_by_level = bars_by_symbol.get(symbol.symbol, {})
        schedule = build_snapshot_schedule(
            profile=profile,
            bars_by_level=bars_by_level,
            backtest_start=backtest_start,
            end_time=end_time,
            levels=levels,
        )
        symbol_counts = {}
        for level, cutoffs in schedule.items():
            snapshots_by_level[level] += len(cutoffs)
            symbol_counts[level] = len(cutoffs)
        snapshots_by_symbol.append({"symbol": symbol.symbol, "snapshots_by_level": symbol_counts})
    estimated_total_runs = sum(snapshots_by_level.values())
    return {
        "profile": profile,
        "mode": mode,
        "warmup_start": warmup_start.isoformat(),
        "backtest_start": backtest_start.isoformat(),
        "end_time": end_time.isoformat(),
        "estimated_symbols": len(symbols),
        "estimated_levels": list(levels),
        "estimated_snapshots_by_level": snapshots_by_level,
        "estimated_total_runs": estimated_total_runs,
        "estimated_kline_reads": len(symbols) * len(levels),
        "estimated_db_writes": estimated_total_runs * 5,
        "estimated_runtime_seconds_or_unknown": None,
        "symbol_samples": snapshots_by_symbol[:20],
    }


def render_backfill_plan_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Module C Historical Backfill Plan",
        "",
        f"- Profile: `{payload['profile']}`",
        f"- Mode: `{payload['mode']}`",
        f"- Warmup start: `{payload['warmup_start']}`",
        f"- Backtest start: `{payload['backtest_start']}`",
        f"- End time: `{payload['end_time']}`",
        f"- Estimated symbols: `{payload['estimated_symbols']}`",
        f"- Estimated total runs: `{payload['estimated_total_runs']}`",
        f"- Estimated K-line reads: `{payload['estimated_kline_reads']}`",
        f"- Estimated DB writes: `{payload['estimated_db_writes']}`",
        "",
        "## Snapshots By Level",
        "",
        "| Level | Snapshot Count |",
        "| --- | ---: |",
    ]
    for level, count in payload["estimated_snapshots_by_level"].items():
        lines.append(f"| {level} | {count} |")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Historical backfill only writes `chan_c_runs` and child detail tables.",
            "- Current `scheme2_chan_c_published_heads` are not updated by this workflow.",
            "- Replay consumes historical `chan_c_runs` by `bar_until <= as_of_time`.",
        ]
    )
    return "\n".join(lines) + "\n"


async def preload_symbol_bars(
    *,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    levels: tuple[str, ...],
    warmup_start: datetime,
    end_time: datetime,
) -> dict[str, dict[str, list[KlineBar]]]:
    bars_by_symbol: dict[str, dict[str, list[KlineBar]]] = {}
    for symbol in symbols:
        await kline_repo.prime_symbol_cache(
            symbol.symbol_id,
            start_time=warmup_start,
            end_time=end_time,
            timeframes=levels,
        )
        bars_by_symbol[symbol.symbol] = {
            level: await kline_repo.get_klines(symbol.symbol_id, level, start=warmup_start, end=end_time)
            for level in levels
        }
        kline_repo.release_symbol_cache(symbol.symbol_id)
    return bars_by_symbol


async def run_historical_backfill(
    *,
    pool: asyncpg.Pool,
    symbols: list[SymbolInfo],
    bars_by_symbol: dict[str, dict[str, list[KlineBar]]],
    profile: str,
    warmup_start: datetime,
    backtest_start: datetime,
    end_time: datetime,
    levels: tuple[str, ...],
    mode: str,
    max_workers: int,
    resume: bool,
    run_group_id: str | None = None,
    optimization_mode: str = "optimized",
) -> dict[str, Any]:
    overlay_builder = load_overlay_builder()
    writer = HistoricalBackfillWriter(pool)
    semaphore = asyncio.Semaphore(max(1, max_workers))
    started_at = perf_counter()
    results: list[BackfillRunResult] = []
    failures: list[BackfillFailure] = []
    symbol_elapsed: list[float] = []
    skipped_existing_runs = 0
    perf_samples: list[BackfillPerfSample] = []

    async def run_symbol(symbol: SymbolInfo) -> None:
        nonlocal skipped_existing_runs
        async with semaphore:
            symbol_started = perf_counter()
            try:
                schedule_started = perf_counter()
                schedule = build_snapshot_schedule(
                    profile=profile,
                    bars_by_level=bars_by_symbol[symbol.symbol],
                    backtest_start=backtest_start,
                    end_time=end_time,
                    levels=levels,
                )
                schedule_build_seconds = perf_counter() - schedule_started
                existing_cutoffs = {}
                if resume:
                    existing_cutoffs = await writer.prefetch_existing_cutoffs(
                        symbol_id=symbol.symbol_id,
                        levels=levels,
                        mode=mode,
                        run_group_id=run_group_id or profile,
                    )
                for level in levels:
                    series = bars_by_symbol[symbol.symbol].get(level, [])
                    cutoff_windows = build_cutoff_windows(
                        series,
                        schedule.get(level, []),
                        use_cursor=optimization_mode == "optimized",
                    )
                    for cutoff_time, window in cutoff_windows:
                        try:
                            if not window:
                                continue
                            resume_started = perf_counter()
                            if resume and cutoff_time in existing_cutoffs.get(level, set()):
                                skipped_existing_runs += 1
                                continue
                            resume_check_seconds = perf_counter() - resume_started
                            overlay_started = perf_counter()
                            payload = build_backfill_overlay_request(
                                symbol=symbol.symbol,
                                level=level,
                                mode=mode,
                                bars=window,
                            )
                            response = overlay_builder(payload)
                            overlay_build_seconds = perf_counter() - overlay_started
                            assert_no_future_leakage(window, cutoff_time)
                            insert_started = perf_counter()
                            run_id, counts = await writer.insert_historical_run(
                                symbol_id=symbol.symbol_id,
                                symbol=symbol.symbol,
                                level=level,
                                mode=mode,
                                profile=run_group_id or profile,
                                warmup_start=warmup_start,
                                cutoff_time=cutoff_time,
                                bars=window,
                                response=response,
                            )
                            db_insert_seconds = perf_counter() - insert_started
                            results.append(
                                BackfillRunResult(
                                    symbol=symbol.symbol,
                                    level=level,
                                    cutoff_time=cutoff_time,
                                    bar_count=len(window),
                                    run_id=run_id,
                                    snapshot_version=str(response.get("snapshot_version") or ""),
                                    strokes=counts["strokes"],
                                    segments=counts["segments"],
                                    centers=counts["centers"],
                                    signals=counts["signals"],
                                    elapsed_seconds=round(resume_check_seconds + overlay_build_seconds + db_insert_seconds, 6),
                                )
                            )
                            perf_samples.append(
                                BackfillPerfSample(
                                    symbol=symbol.symbol,
                                    level=level,
                                    cutoff_time=cutoff_time,
                                    bar_count=len(window),
                                    schedule_build_seconds=round(schedule_build_seconds, 6),
                                    resume_check_seconds=round(resume_check_seconds, 6),
                                    overlay_build_seconds=round(overlay_build_seconds, 6),
                                    db_insert_seconds=round(db_insert_seconds, 6),
                                    total_snapshot_seconds=round(schedule_build_seconds + resume_check_seconds + overlay_build_seconds + db_insert_seconds, 6),
                                )
                            )
                        except Exception as exc:  # pragma: no cover - retained for jsonl outputs
                            failures.append(
                                BackfillFailure(
                                    symbol=symbol.symbol,
                                    level=level,
                                    cutoff_time=cutoff_time,
                                    error=str(exc),
                                )
                            )
            finally:
                symbol_elapsed.append(perf_counter() - symbol_started)

    await asyncio.gather(*(run_symbol(symbol) for symbol in symbols))
    total_elapsed = perf_counter() - started_at
    return {
        "profile": profile,
        "mode": mode,
        "warmup_start": warmup_start.isoformat(),
        "backtest_start": backtest_start.isoformat(),
        "end_time": end_time.isoformat(),
        "symbols": len(symbols),
        "levels": list(levels),
        "written_runs": len(results),
        "failed_runs": len(failures),
        "skipped_existing_runs": skipped_existing_runs,
        "written_by_level": _count_by_level(results),
        "signals_by_level": _sum_by_level(results, "signals"),
        "elapsed_seconds": round(total_elapsed, 3),
        "symbol_elapsed_seconds_p50": round(median(symbol_elapsed), 3) if symbol_elapsed else 0.0,
        "symbol_elapsed_seconds_p95": _percentile(symbol_elapsed, 0.95),
        "perf_profile": build_perf_profile(perf_samples),
        "results": results,
        "failures": failures,
    }


class HistoricalBackfillWriter:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def run_exists(
        self,
        *,
        symbol_id: int,
        level: str,
        mode: str,
        cutoff_time: datetime,
        run_group_id: str,
    ) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchval(
                """
                select 1
                from chan_c_runs
                where symbol_id = $1
                  and chan_level = $2
                  and mode = $3
                  and run_kind = $4
                  and run_group_id = $5
                  and bar_until = $6
                  and config_hash = $7
                  and status = 'success'
                limit 1
                """,
                symbol_id,
                LEVEL_TO_DB[level],
                MODE_TO_DB[mode],
                RUN_KIND_HISTORICAL_BACKFILL,
                run_group_id,
                cutoff_time,
                MODULE_C_CONFIG_HASH,
            )
        return row is not None

    async def prefetch_existing_cutoffs(
        self,
        *,
        symbol_id: int,
        levels: tuple[str, ...],
        mode: str,
        run_group_id: str,
    ) -> dict[str, set[datetime]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select chan_level, bar_until
                from chan_c_runs
                where symbol_id = $1
                  and chan_level = any($2::integer[])
                  and mode = $3
                  and run_kind = $4
                  and run_group_id = $5
                  and config_hash = $6
                  and status = 'success'
                """,
                symbol_id,
                [LEVEL_TO_DB[level] for level in levels],
                MODE_TO_DB[mode],
                RUN_KIND_HISTORICAL_BACKFILL,
                run_group_id,
                MODULE_C_CONFIG_HASH,
            )
        payload: dict[str, set[datetime]] = {level: set() for level in levels}
        for row in rows:
            payload[DB_TO_LEVEL[int(row["chan_level"])]].add(row["bar_until"])
        return payload

    async def insert_historical_run(
        self,
        *,
        symbol_id: int,
        symbol: str,
        level: str,
        mode: str,
        profile: str,
        warmup_start: datetime,
        cutoff_time: datetime,
        bars: list[KlineBar],
        response: dict[str, Any],
    ) -> tuple[int, dict[str, int]]:
        level_code = LEVEL_TO_DB[level]
        mode_code = MODE_TO_DB[mode]
        input_signature = build_input_signature(
            profile=profile,
            symbol=symbol,
            level=level,
            mode=mode,
            cutoff_time=cutoff_time,
            bar_count=len(bars),
            snapshot_version=str(response.get("snapshot_version") or ""),
        )
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                run_id = await conn.fetchval(
                    """
                    insert into chan_c_runs (
                        symbol_id,
                        chan_level,
                        mode,
                        input_signature,
                        config_hash,
                        bar_from,
                        bar_until,
                        bar_count,
                        status,
                        finished_at,
                        snapshot_version,
                        computed_at,
                        run_kind,
                        run_group_id,
                        cutoff_bar_end
                    )
                    values (
                        $1, $2, $3, $4, $5, $6, $7, $8,
                        'success', now(), $9, now(), $10, $11, $12
                    )
                    returning id
                    """,
                    symbol_id,
                    level_code,
                    mode_code,
                    input_signature,
                    MODULE_C_CONFIG_HASH,
                    warmup_start,
                    cutoff_time,
                    len(bars),
                    str(response.get("snapshot_version") or ""),
                    RUN_KIND_HISTORICAL_BACKFILL,
                    profile,
                    cutoff_time,
                )
                counts = {
                    "strokes": await self._insert_lines(conn, "chan_c_strokes", symbol_id, level_code, run_id, response.get("strokes", []), mode_code),
                    "segments": await self._insert_lines(conn, "chan_c_segments", symbol_id, level_code, run_id, response.get("segments", []), mode_code),
                    "centers": await self._insert_centers(conn, symbol_id, level_code, run_id, response.get("centers", []), mode_code),
                    "signals": await self._insert_signals(conn, symbol_id, level_code, run_id, response.get("signals", []), mode_code),
                }
        return int(run_id), counts

    async def _insert_lines(
        self,
        conn: asyncpg.Connection,
        table: str,
        symbol_id: int,
        level_code: int,
        run_id: int,
        items: list[dict[str, Any]],
        mode_code: int,
    ) -> int:
        rows = []
        for item in items:
            if mode_to_name(item.get("mode")) != code_to_mode_name(mode_code):
                continue
            start = item.get("start") or {}
            end = item.get("end") or {}
            rows.append(
                (
                    symbol_id,
                    level_code,
                    mode_code,
                    run_id,
                    int(item.get("seq") or 0),
                    epoch_to_datetime(int(start.get("time") or 0)),
                    epoch_to_datetime(int(end.get("time") or 0)),
                    price_x1000(float(start.get("price") or 0.0)),
                    price_x1000(float(end.get("price") or 0.0)),
                    1 if str(item.get("direction") or "").lower() == "up" else -1,
                    bool(item.get("confirmed")),
                    0,
                    epoch_to_datetime(int(item.get("begin_base_ts") or start.get("base_ts") or start.get("time") or 0)),
                    epoch_to_datetime(int(item.get("end_base_ts") or end.get("base_ts") or end.get("time") or 0)),
                    item.get("begin_base_seq"),
                    item.get("end_base_seq"),
                    json.dumps(item.get("extra") or {}),
                )
            )
        if not rows:
            return 0
        await conn.executemany(
            f"""
            insert into {table} (
                symbol_id, chan_level, mode, run_id, seq,
                start_ts, end_ts, start_price_x1000, end_price_x1000, direction,
                is_confirmed, revision, begin_base_ts, end_base_ts, begin_base_seq, end_base_seq, extra
            )
            values (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17::jsonb
            )
            """,
            rows,
        )
        return len(rows)

    async def _insert_centers(
        self,
        conn: asyncpg.Connection,
        symbol_id: int,
        level_code: int,
        run_id: int,
        items: list[dict[str, Any]],
        mode_code: int,
    ) -> int:
        rows = []
        for item in items:
            if mode_to_name(item.get("mode")) != code_to_mode_name(mode_code):
                continue
            rows.append(
                (
                    symbol_id,
                    level_code,
                    mode_code,
                    run_id,
                    int(item.get("seq") or 0),
                    epoch_to_datetime(int(item.get("start_time") or item.get("begin_base_ts") or 0)),
                    epoch_to_datetime(int(item.get("end_time") or item.get("end_base_ts") or 0)),
                    price_x1000(float(item.get("low") or 0.0)),
                    price_x1000(float(item.get("high") or 0.0)),
                    bool(item.get("confirmed")),
                    0,
                    epoch_to_datetime(int(item.get("begin_base_ts") or item.get("start_time") or 0)),
                    epoch_to_datetime(int(item.get("end_base_ts") or item.get("end_time") or 0)),
                    item.get("begin_base_seq"),
                    item.get("end_base_seq"),
                    json.dumps(item.get("extra") or {}),
                )
            )
        if not rows:
            return 0
        await conn.executemany(
            """
            insert into chan_c_centers (
                symbol_id, chan_level, mode, run_id, seq,
                start_ts, end_ts, low_x1000, high_x1000,
                is_confirmed, revision, begin_base_ts, end_base_ts,
                begin_base_seq, end_base_seq, extra
            )
            values (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15, $16::jsonb
            )
            """,
            rows,
        )
        return len(rows)

    async def _insert_signals(
        self,
        conn: asyncpg.Connection,
        symbol_id: int,
        level_code: int,
        run_id: int,
        items: list[dict[str, Any]],
        mode_code: int,
    ) -> int:
        rows = []
        for item in items:
            if mode_to_name(item.get("mode")) != code_to_mode_name(mode_code):
                continue
            rows.append(
                (
                    symbol_id,
                    level_code,
                    mode_code,
                    run_id,
                    epoch_to_datetime(int(item.get("time") or 0)),
                    price_x1000(float(item.get("price") or 0.0)),
                    str(item.get("kind") or item.get("signal_type") or "bsp"),
                    bool(item.get("confirmed")),
                    0,
                    epoch_to_datetime(int(item.get("base_ts") or item.get("time") or 0)),
                    item.get("base_seq"),
                    json.dumps(
                        {
                            "side": item.get("side"),
                            "bsp_type": item.get("bsp_type"),
                            "label": item.get("label"),
                            "features": item.get("features") or {},
                            **(item.get("extra") or {}),
                        }
                    ),
                )
            )
        if not rows:
            return 0
        await conn.executemany(
            """
            insert into chan_c_signals (
                symbol_id, chan_level, mode, run_id, ts,
                price_x1000, signal_type, is_confirmed, revision,
                base_ts, base_seq, extra
            )
            values (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12::jsonb
            )
            """,
            rows,
        )
        return len(rows)


def build_backfill_overlay_request(
    *,
    symbol: str,
    level: str,
    mode: str,
    bars: list[KlineBar],
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "timeframe": level,
        "chan_levels": [level],
        "modes": [mode],
        "bars_by_level": {
            level: [
                {
                    "time": int(bar.ts.timestamp()),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": int(bar.volume or 0),
                }
                for bar in bars
            ]
        },
    }


def slice_bars_for_cutoff(bars: list[KlineBar], cutoff_time: datetime) -> list[KlineBar]:
    if not bars:
        return []
    times = [bar.ts for bar in bars]
    right = bisect_right(times, cutoff_time)
    return bars[:right]


def build_cutoff_windows(
    bars: list[KlineBar],
    cutoffs: list[datetime],
    *,
    use_cursor: bool,
) -> list[tuple[datetime, list[KlineBar]]]:
    if not bars or not cutoffs:
        return []
    if not use_cursor:
        return [(cutoff_time, slice_bars_for_cutoff(bars, cutoff_time)) for cutoff_time in cutoffs]

    windows: list[tuple[datetime, list[KlineBar]]] = []
    right = 0
    total = len(bars)
    for cutoff_time in cutoffs:
        while right < total and bars[right].ts <= cutoff_time:
            right += 1
        windows.append((cutoff_time, bars[:right]))
    return windows


def assert_no_future_leakage(bars: list[KlineBar], cutoff_time: datetime) -> None:
    if not bars:
        return
    if bars[-1].ts > cutoff_time:
        raise ValueError(f"future bar leakage detected: last={bars[-1].ts.isoformat()} cutoff={cutoff_time.isoformat()}")


def build_perf_profile(samples: list[BackfillPerfSample]) -> dict[str, Any]:
    if not samples:
        return {
            "sample_count": 0,
            "per_symbol": [],
            "per_level": [],
            "aggregate": {},
        }

    def summarize(values: list[float]) -> dict[str, float]:
        ordered = sorted(values)
        return {
            "p50": round(float(median(ordered)), 6),
            "p95": _percentile_float_values(ordered, 0.95),
            "sum": round(float(sum(ordered)), 6),
        }

    by_symbol: dict[str, list[BackfillPerfSample]] = defaultdict(list)
    by_level: dict[str, list[BackfillPerfSample]] = defaultdict(list)
    for sample in samples:
        by_symbol[sample.symbol].append(sample)
        by_level[sample.level].append(sample)

    fields = (
        "schedule_build_seconds",
        "resume_check_seconds",
        "overlay_build_seconds",
        "db_insert_seconds",
        "total_snapshot_seconds",
    )

    per_symbol = []
    for symbol, rows in sorted(by_symbol.items()):
        payload = {"symbol": symbol, "sample_count": len(rows)}
        for field in fields:
            payload[field] = summarize([float(getattr(row, field)) for row in rows])
        per_symbol.append(payload)

    per_level = []
    for level, rows in sorted(by_level.items()):
        payload = {"level": level, "sample_count": len(rows)}
        for field in fields:
            payload[field] = summarize([float(getattr(row, field)) for row in rows])
        per_level.append(payload)

    aggregate = {"sample_count": len(samples)}
    for field in fields:
        aggregate[field] = summarize([float(getattr(row, field)) for row in samples])

    return {
        "sample_count": len(samples),
        "per_symbol": per_symbol,
        "per_level": per_level,
        "aggregate": aggregate,
    }


def build_input_signature(
    *,
    profile: str,
    symbol: str,
    level: str,
    mode: str,
    cutoff_time: datetime,
    bar_count: int,
    snapshot_version: str,
) -> str:
    raw = f"{profile}|{symbol}|{level}|{mode}|{cutoff_time.isoformat()}|{bar_count}|{snapshot_version}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:64]


def render_backfill_summary_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Module C Backfill Summary",
        "",
        f"- Profile: `{payload['profile']}`",
        f"- Mode: `{payload['mode']}`",
        f"- Symbols: `{payload['symbols']}`",
        f"- Written runs: `{payload['written_runs']}`",
        f"- Skipped existing runs: `{payload.get('skipped_existing_runs', 0)}`",
        f"- Failed runs: `{payload['failed_runs']}`",
        f"- Elapsed seconds: `{payload['elapsed_seconds']}`",
        f"- Symbol elapsed p50: `{payload['symbol_elapsed_seconds_p50']}`",
        f"- Symbol elapsed p95: `{payload['symbol_elapsed_seconds_p95']}`",
        "",
        "## Written By Level",
        "",
        "| Level | Runs | Signals |",
        "| --- | ---: | ---: |",
    ]
    for level in payload["levels"]:
        lines.append(
            f"| {level} | {payload['written_by_level'].get(level, 0)} | {payload['signals_by_level'].get(level, 0)} |"
        )
    if payload["failed_runs"]:
        lines.extend(["", "## Failures", ""])
        for failure in payload["failures"][:20]:
            lines.append(
                f"- `{failure.symbol}` `{failure.level}` `{failure.cutoff_time.isoformat() if failure.cutoff_time else 'n/a'}`: {failure.error}"
            )
    return "\n".join(lines) + "\n"


def render_backtest_report_markdown(payload: dict[str, Any], metrics: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Backtest Report After Backfill",
            "",
            f"- Strategy: `{payload['strategy_code']}`",
            f"- Replayed symbols: `{payload['replayed_symbols']}`",
            f"- Total replay steps: `{payload['total_replay_steps']}`",
            f"- Future leakage detected: `{payload['future_leakage_detected']}`",
            f"- Trades: `{metrics.get('trade_count', 0)}`",
            f"- Win rate: `{metrics.get('win_rate', 0)}`",
            f"- Avg return: `{metrics.get('avg_return', 0)}`",
            f"- Total return: `{metrics.get('total_return', 0)}`",
            "",
        ]
    ) + "\n"


def write_failed_symbols_jsonl(path: Path, failures: list[BackfillFailure]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for failure in failures:
            handle.write(
                json.dumps(
                    {
                        "symbol": failure.symbol,
                        "level": failure.level,
                        "cutoff_time": failure.cutoff_time.isoformat() if failure.cutoff_time else None,
                        "error": failure.error,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def write_runs_manifest_csv(path: Path, results: list[BackfillRunResult]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "level",
                "cutoff_time",
                "bar_count",
                "run_id",
                "snapshot_version",
                "strokes",
                "segments",
                "centers",
                "signals",
                "elapsed_seconds",
            ],
        )
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "symbol": result.symbol,
                    "level": result.level,
                    "cutoff_time": result.cutoff_time.isoformat(),
                    "bar_count": result.bar_count,
                    "run_id": result.run_id,
                    "snapshot_version": result.snapshot_version,
                    "strokes": result.strokes,
                    "segments": result.segments,
                    "centers": result.centers,
                    "signals": result.signals,
                    "elapsed_seconds": result.elapsed_seconds,
                }
            )


def render_phase_1_6_summary_markdown(
    *,
    dry_run: dict[str, Any],
    backfill_summary: dict[str, Any] | None,
    coverage: dict[str, Any] | None,
    effective_window: dict[str, Any] | None,
    replay_audit: dict[str, Any] | None,
) -> str:
    lines = [
        "# Phase 1.6 Summary",
        "",
        "## Backfill Planning",
        "",
        f"- Profile: `{dry_run['profile']}`",
        f"- Estimated total runs: `{dry_run['estimated_total_runs']}`",
        "",
        "## Schema Notes",
        "",
        "- No new migration was required because `chan_c_runs` already contains `run_kind`, `run_group_id`, and `cutoff_bar_end`.",
        "- Historical backfill writes `run_kind=historical_backfill` and keeps current published heads untouched.",
    ]
    if backfill_summary is not None:
        lines.extend(
            [
                "",
                "## Backfill Execution",
                "",
                f"- Written runs: `{backfill_summary['written_runs']}`",
                f"- Skipped existing runs: `{backfill_summary.get('skipped_existing_runs', 0)}`",
                f"- Failed runs: `{backfill_summary['failed_runs']}`",
                f"- Elapsed seconds: `{backfill_summary['elapsed_seconds']}`",
            ]
        )
    if coverage is not None:
        lines.extend(
            [
                "",
                "## Coverage After Backfill",
                "",
                f"- Active symbols audited: `{coverage['active_symbols_total']}`",
                f"- All levels have any run: `{coverage['summary']['all_levels_has_any_run_count']}`",
                f"- All levels cover window: `{coverage['summary']['all_levels_cover_window_count']}`",
            ]
        )
    if effective_window is not None:
        lines.extend(
            [
                "",
                "## Effective Backtest Window",
                "",
                f"- Strict global effective start: `{effective_window.get('strict_global_effective_start')}`",
                f"- Strict global effective end: `{effective_window.get('strict_global_effective_end')}`",
                f"- Window valid: `{effective_window.get('strict_global_window_valid')}`",
            ]
        )
    if replay_audit is not None:
        lines.extend(
            [
                "",
                "## Replay After Backfill",
                "",
                f"- Replayed symbols: `{replay_audit['replayed_symbols']}`",
                f"- Total replay steps: `{replay_audit['total_replay_steps']}`",
                f"- Future leakage detected: `{replay_audit['future_leakage_detected']}`",
            ]
        )
    return "\n".join(lines) + "\n"


def _latest_bar_per_local_day(bars: list[KlineBar]) -> list[datetime]:
    by_day: dict[datetime.date, datetime] = {}
    for bar in bars:
        local_day = bar.ts.astimezone(SH_TZ).date()
        current = by_day.get(local_day)
        if current is None or bar.ts > current:
            by_day[local_day] = bar.ts
    return [by_day[day] for day in sorted(by_day)]


def _count_by_level(results: list[BackfillRunResult]) -> dict[str, int]:
    payload: dict[str, int] = {}
    for result in results:
        payload[result.level] = payload.get(result.level, 0) + 1
    return payload


def _sum_by_level(results: list[BackfillRunResult], field: str) -> dict[str, int]:
    payload: dict[str, int] = {}
    for result in results:
        payload[result.level] = payload.get(result.level, 0) + int(getattr(result, field))
    return payload


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(len(ordered) * quantile) - 1))
    return round(ordered[index], 3)


def _percentile_float_values(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    index = min(len(values) - 1, max(0, int(len(values) * quantile) - 1))
    return round(float(values[index]), 6)


def price_x1000(value: float) -> int:
    return int(round(value * 1000))


def epoch_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value, tz=UTC)


def mode_to_name(value: Any) -> str:
    if value is None:
        return "confirmed"
    return str(value)


def code_to_mode_name(mode_code: int) -> str:
    return "predictive" if mode_code == MODE_TO_DB["predictive"] else "confirmed"
