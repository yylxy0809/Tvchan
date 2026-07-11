from __future__ import annotations

from collections import defaultdict
from typing import Any

from app.analyzers.strength_evaluator import evaluate_daily_first_up_strength
from app.domain.models import ChanCenter, ChanStroke
from app.repositories.kline_repo import KlineBar, compute_macd
from app.engine.time_utils import utc_time


REQUIRED_LEVELS = {"1w", "1d", "30f", "5f"}


def build_observable_research_universe(runs: list[dict[str, Any]], *, klines: list[dict[str, Any]] | None = None, market_cap_by_symbol: dict[str, float | None] | None = None) -> dict[str, Any]:
    levels: dict[str, set[str]] = defaultdict(set)
    for run in runs:
        if run.get("mode") in {"predictive", "confirmed", "legacy"} and run.get("cutoff_bar_end"):
            utc_time(run["cutoff_bar_end"])
            levels[str(run.get("symbol"))].add(str(run.get("level")))
    kline_levels: dict[str, set[str]] = defaultdict(set)
    for row in klines or []:
        if row.get("is_complete"):
            utc_time(row["ts"])
            kline_levels[str(row.get("symbol"))].add({5: "5f", 30: "30f", 1440: "1d", 10080: "1w"}.get(row.get("timeframe"), ""))
    symbols = sorted(symbol for symbol, present in levels.items() if REQUIRED_LEVELS <= present and (not klines or REQUIRED_LEVELS <= kline_levels[symbol]))
    caps = market_cap_by_symbol or {}
    without_cap = sorted(symbol for symbol in symbols if caps.get(symbol) is None)
    official = sorted(symbol for symbol in symbols if caps.get(symbol) is not None)
    return {"observable_symbols": symbols, "symbol_count": len(symbols), "observable_symbol_count": len(symbols), "official_eligible_symbol_count": len(official), "diagnostic_symbol_count": len(without_cap), "diagnostic_universe_without_market_cap": without_cap, "market_cap_status": {symbol: "available" if caps.get(symbol) is not None else "missing_diagnostic_only" for symbol in symbols}, "required_levels": sorted(REQUIRED_LEVELS), "levels_by_symbol": {symbol: sorted(value) for symbol, value in levels.items()}, "kline_levels_by_symbol": {symbol: sorted(value) for symbol, value in kline_levels.items()}, "historical_run_verified": True, "kline_location_verified": bool(klines), "intraday_coverage_auditable": bool(klines)}


def _time(value: Any):
    return utc_time(value)


def _bars(rows: list[dict[str, Any]], symbol: str, timeframe: int) -> list[KlineBar]:
    return [KlineBar(ts=_time(row["ts"]), open=row["open_x1000"] / 1000, high=row["high_x1000"] / 1000, low=row["low_x1000"] / 1000, close=row["close_x1000"] / 1000, volume=int(row["volume"] or 0)) for row in rows if row["symbol"] == symbol and row["timeframe"] == timeframe]


def _stroke(row: dict[str, Any], level: str) -> ChanStroke:
    return ChanStroke(seq=0, level=level, mode="predictive", direction="up" if row["direction"] > 0 else "down", start_time=_time(row["start_ts"]), end_time=_time(row["end_ts"]), start_price=row["start_price_x1000"] / 1000, end_price=row["end_price_x1000"] / 1000, begin_base_time=_time(row["start_ts"]), end_base_time=_time(row["end_ts"]), confirmed=bool(row["is_confirmed"]))


def _center(row: dict[str, Any], level: str) -> ChanCenter:
    return ChanCenter(seq=0, level=level, mode="predictive", start_time=_time(row["start_ts"]), end_time=_time(row["end_ts"]), low=row["low_x1000"] / 1000, high=row["high_x1000"] / 1000, confirmed=bool(row["is_confirmed"]))


def build_reconstructed_episodes(*, universe: dict[str, Any], lifecycle: list[dict[str, Any]], structure_runs: list[dict[str, Any]], klines: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Create independent contexts/setups only after linking lifecycle identities to full snapshots."""
    eligible = set(universe["observable_symbols"])
    buy = [row for row in lifecycle if row["symbol"] in eligible and row["mode"] == "predictive" and row["side"] == "buy" and row["first_seen_time"]]
    by_level: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in buy: by_level[(row["symbol"], row["level"])].append(row)
    for rows in by_level.values(): rows.sort(key=lambda row: _time(row["first_seen_time"]))
    runs = {row["run_id"]: row for row in structure_runs}
    weekly, daily = [], []
    for (symbol, level), rows in by_level.items():
        if level != "1w": continue
        bars = _bars(klines, symbol, 10080)
        for b2 in (row for row in rows if row["bsp_type"] in {"2", "2s"}):
            prior = [row for row in rows if row["bsp_type"] == "1" and _time(row["first_seen_time"]) <= _time(b2["first_seen_time"])]
            if not prior: continue
            b1 = prior[-1]
            macd = compute_macd([bar for bar in bars if bar.ts <= _time(b2["point_time"])])
            macd_row = macd[-1] if macd else None
            weekly.append({"episode_id": f"weekly|{b1['identity']}|{b2['identity']}", "symbol": symbol, "weekly_b1_identity": b1["identity"], "weekly_b2_identity": b2["identity"], "weekly_b1": b1, "weekly_b2": b2, "weekly_context_first_seen_time": b2["first_seen_time"], "weekly_price_relation_valid": b2["price_x1000"] > b1["price_x1000"], "weekly_dif": macd_row["dif"] if macd_row else None, "weekly_dif_status": "reconstructed" if macd_row else "unreconstructable"})
    weekly_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in weekly: weekly_by_symbol[row["symbol"]].append(row)
    for (symbol, level), rows in by_level.items():
        if level != "1d": continue
        bars = _bars(klines, symbol, 1440)
        for b2 in (row for row in rows if row["bsp_type"] in {"2", "2s"}):
            b1s = [row for row in rows if row["bsp_type"] == "1" and _time(row["first_seen_time"]) <= _time(b2["first_seen_time"])]
            contexts = [row for row in weekly_by_symbol[symbol] if _time(row["weekly_context_first_seen_time"]) <= _time(b2["first_seen_time"])]
            if not b1s or not contexts: continue
            b1, context = b1s[-1], contexts[-1]
            snapshot = runs.get(b2["first_seen_run_id"], {})
            strokes = [_stroke(row, "1d") for row in snapshot.get("strokes", [])]
            previous_down = [row for row in strokes if row.direction == "down" and row.end_time <= _time(b1["point_time"])]
            first_up = [row for row in strokes if row.direction == "up" and row.start_time >= _time(b1["point_time"])]
            centers = [_center(row, "1d") for row in snapshot.get("centers", [])]
            if previous_down and first_up and bars:
                strength = evaluate_daily_first_up_strength(previous_down_stroke=previous_down[-1], first_up_stroke=first_up[0], daily_bars=[bar for bar in bars if bar.ts <= _time(b2["point_time"])], daily_center_low=None, daily_center_high=None, sub_segments=[], sub_centers=centers)
                strength_status = "reconstructed"
            else:
                strength, strength_status = None, "unreconstructable_missing_snapshot_structure_or_klines"
            daily.append({"episode_id": f"entry|{context['episode_id']}|{b1['identity']}|{b2['identity']}", "symbol": symbol, "weekly_context_episode_id": context["episode_id"], "daily_b1_identity": b1["identity"], "daily_b2_identity": b2["identity"], "daily_b1": b1, "daily_b2": b2, "daily_setup_first_seen_time": b2["first_seen_time"], "daily_first_up_strength": strength and strength["strength_score"], "daily_first_up_strength_status": strength_status, "daily_first_up": strength, "observation_count": 1})
    return weekly, daily
