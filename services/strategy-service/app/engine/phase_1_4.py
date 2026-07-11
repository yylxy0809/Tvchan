from __future__ import annotations

import asyncio
import csv
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any

from app.backtest.metrics import compute_metrics
from app.config.strategy_params import (
    PHASE_1_4_EXPLICIT_B1_THEN_B2_STRATEGY_CODE,
    PHASE_1_4_TRUST_CHAN_SIGNAL_STRATEGY_CODE,
    PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE,
    StrategyParams,
)
from app.domain.enums import BacktestMode, ScanStatus
from app.domain.models import ScanDiagnosis, SymbolInfo, Trade
from app.engine.diagnostic_reporting import GATE_ORDER, render_gate_waterfall_markdown, top_failure_gates
from app.engine.strategy_diagnoser import StrategyDiagnoser
from app.engine.strategy_runner import StrategyRunner
from app.repositories.kline_repo import KlineBar, KlineRepository
from app.repositories.module_c_repo import ModuleCRepository


@dataclass(slots=True)
class HistoricalBacktestResult:
    trades: list[Trade]
    replay_audit: dict[str, Any]
    gate_waterfall: dict[str, Any]
    trade_analysis: dict[str, Any]
    metrics: dict[str, Any]


def phase_1_4_strategy_codes() -> list[str]:
    return [
        PHASE_1_4_EXPLICIT_B1_THEN_B2_STRATEGY_CODE,
        PHASE_1_4_TRUST_CHAN_SIGNAL_STRATEGY_CODE,
        PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE,
    ]


async def build_weekly_context_compare(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    as_of_time: datetime,
    concurrency: int,
) -> dict[str, Any]:
    diagnoser = StrategyDiagnoser(module_c_repo, kline_repo)
    rows: list[dict[str, Any]] = []

    async def diagnose_batch(batch_symbols: list[SymbolInfo], params: StrategyParams) -> list[ScanDiagnosis]:
        return await _gather_bounded(
            [lambda symbol=symbol: diagnoser.diagnose_symbol(symbol, as_of_time=as_of_time, params=params) for symbol in batch_symbols],
            concurrency=concurrency,
        )

    for strategy_code in phase_1_4_strategy_codes():
        params = StrategyParams.from_strategy_code(strategy_code)
        diagnoses: list[ScanDiagnosis] = []
        for index in range(0, len(symbols), concurrency):
            diagnoses.extend(await diagnose_batch(symbols[index : index + concurrency], params))

        weekly_context_count = sum(1 for diagnosis in diagnoses if diagnosis.weekly_context is not None)
        daily_setup_count = sum(1 for diagnosis in diagnoses if diagnosis.daily_setup is not None)
        entry_watch_count = sum(
            1
            for diagnosis in diagnoses
            if diagnosis.result is not None and diagnosis.result.status in {ScanStatus.WATCH, ScanStatus.TRIGGER}
        )
        trigger_count = sum(
            1
            for diagnosis in diagnoses
            if diagnosis.result is not None and diagnosis.result.status == ScanStatus.TRIGGER
        )
        trigger_30f_count = sum(
            1
            for diagnosis in diagnoses
            if diagnosis.result is not None
            and diagnosis.result.status == ScanStatus.TRIGGER
            and diagnosis.entry is not None
            and diagnosis.entry.entry_level == "30f"
        )
        rows.append(
            {
                "strategy_code": strategy_code,
                "weekly_context_mode": params.weekly_context_mode_normalized,
                "weekly_b2_types": params.weekly_b2_types,
                "weekly_context_count": weekly_context_count,
                "daily_setup_count": daily_setup_count,
                "entry_watch_count": entry_watch_count,
                "trigger_count": trigger_count,
                "trigger_30f_count": trigger_30f_count,
                "top_failure_gates": top_failure_gates(diagnoses, limit=5),
            }
        )

    return {
        "as_of_time": as_of_time.isoformat(),
        "symbol_count": len(symbols),
        "rows": rows,
    }


async def run_historical_backtest(
    *,
    module_c_repo: ModuleCRepository,
    kline_repo: KlineRepository,
    symbols: list[SymbolInfo],
    params: StrategyParams,
    start_time: datetime,
    end_time: datetime,
    concurrency: int,
    mode: BacktestMode = BacktestMode.EVENT_REPLAY,
) -> HistoricalBacktestResult:
    runner = StrategyRunner(module_c_repo, kline_repo)
    diagnoser = StrategyDiagnoser(module_c_repo, kline_repo)
    replay_started = perf_counter()
    gate_counts = _blank_gate_counts()
    stage_counts = Counter(
        {
            "historical_symbol_count": len(symbols),
            "replayed_symbols": 0,
            "total_replay_steps": 0,
            "total_symbol_time_points": 0,
            "weekly_context_found": 0,
            "daily_setup_found": 0,
            "entry_watch_found": 0,
            "entry_trigger_found": 0,
            "trade_opened": 0,
        }
    )
    leak_free = True
    leak_violations: list[dict[str, Any]] = []
    trades: list[Trade] = []
    success_samples: list[dict[str, Any]] = []
    failure_samples: list[dict[str, Any]] = []
    per_symbol_elapsed: list[float] = []

    async def backtest_one(symbol: SymbolInfo) -> dict[str, Any]:
        return await _backtest_symbol(
            symbol=symbol,
            params=params,
            start_time=start_time,
            end_time=end_time,
            mode=mode,
            runner=runner,
            diagnoser=diagnoser,
            kline_repo=kline_repo,
            module_c_repo=module_c_repo,
        )

    for index in range(0, len(symbols), concurrency):
        batch = symbols[index : index + concurrency]
        batch_results = await _gather_bounded(
            [lambda symbol=symbol: backtest_one(symbol) for symbol in batch],
            concurrency=concurrency,
        )
        for result in batch_results:
            stage_counts["replayed_symbols"] += result["replayed_symbols"]
            stage_counts["total_replay_steps"] += result["total_replay_steps"]
            stage_counts["total_symbol_time_points"] += result["total_symbol_time_points"]
            stage_counts["weekly_context_found"] += result["weekly_context_found"]
            stage_counts["daily_setup_found"] += result["daily_setup_found"]
            stage_counts["entry_watch_found"] += result["entry_watch_found"]
            stage_counts["entry_trigger_found"] += result["entry_trigger_found"]
            stage_counts["trade_opened"] += result["trade_opened"]
            per_symbol_elapsed.append(result["elapsed_seconds"])
            trades.extend(result["trades"])
            _merge_gate_counts(gate_counts, result["gate_counts"])
            if not result["leak_free"]:
                leak_free = False
                leak_violations.extend(result["leak_violations"])
            _append_limited(success_samples, result["success_samples"], limit=5)
            _append_limited(failure_samples, result["failure_samples"], limit=5)

    metrics = compute_metrics(trades)
    elapsed_seconds = round(perf_counter() - replay_started, 3)
    symbol_elapsed_total = round(sum(per_symbol_elapsed), 3)
    replay_audit = {
        "strategy_code": params.strategy_code,
        "weekly_context_mode": params.weekly_context_mode_normalized,
        "first_seen_time_source": params.first_seen_time_source,
        "replayed_symbols": int(stage_counts["replayed_symbols"]),
        "total_replay_steps": int(stage_counts["total_replay_steps"]),
        "total_symbol_time_points": int(stage_counts["total_symbol_time_points"]),
        "future_leakage_detected": not leak_free,
        "future_leakage_violation_count": len(leak_violations),
        "future_leakage_samples": leak_violations[:20],
        "symbol_backtest_elapsed_seconds_total": symbol_elapsed_total,
        "backtest_elapsed_seconds": elapsed_seconds,
        "symbol_backtest_elapsed_seconds_p50": round(median(per_symbol_elapsed), 3) if per_symbol_elapsed else 0.0,
        "symbol_backtest_elapsed_seconds_p95": _percentile_float(per_symbol_elapsed, 0.95),
    }
    total_steps = int(stage_counts["total_replay_steps"]) or 1
    gate_waterfall = {
        "rows": [_gate_row(gate, gate_counts[gate], total_steps=total_steps) for gate in GATE_ORDER],
        "historical_funnel": _historical_funnel_rows(stage_counts),
    }
    trade_analysis = {
        "weekly_context_mode_distribution": dict(Counter(_trade_feature(trade, "weekly_context_mode") for trade in trades)),
        "confidence_distribution": dict(Counter(_confidence_bucket(trade.entry_confidence) for trade in trades)),
        "entry_level_distribution": dict(Counter(trade.entry_level for trade in trades)),
        "exit_reason_distribution": dict(Counter((trade.exit_reason or "UNKNOWN") for trade in trades)),
        "success_samples": success_samples,
        "failure_samples": failure_samples,
    }
    return HistoricalBacktestResult(
        trades=trades,
        replay_audit=replay_audit,
        gate_waterfall=gate_waterfall,
        trade_analysis=trade_analysis,
        metrics=metrics,
    )


def write_phase_1_4_outputs(
    output_dir: Path,
    *,
    weekly_context_compare: dict[str, Any],
    historical_backtest: HistoricalBacktestResult,
    start_time: datetime,
    end_time: datetime,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "weekly_context_compare.json").write_text(
        json.dumps(weekly_context_compare, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "weekly_context_compare.md").write_text(
        _render_weekly_context_compare_markdown(weekly_context_compare),
        encoding="utf-8",
    )
    (output_dir / "replay_audit.json").write_text(
        json.dumps(historical_backtest.replay_audit, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "replay_audit.md").write_text(
        _render_replay_audit_markdown(historical_backtest.replay_audit),
        encoding="utf-8",
    )
    (output_dir / "gate_waterfall.json").write_text(
        json.dumps(historical_backtest.gate_waterfall, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "gate_waterfall.md").write_text(
        _render_historical_gate_waterfall_markdown(historical_backtest.gate_waterfall),
        encoding="utf-8",
    )
    _write_phase_1_4_trades_csv(output_dir / "trades.csv", historical_backtest.trades)
    (output_dir / "metrics.json").write_text(
        json.dumps(
            {
                "weekly_context_compare": weekly_context_compare,
                "replay_audit": historical_backtest.replay_audit,
                "metrics": historical_backtest.metrics,
                "trade_analysis": historical_backtest.trade_analysis,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (output_dir / "backtest_report.md").write_text(
        _render_backtest_report_markdown(
            historical_backtest.metrics,
            historical_backtest.replay_audit,
            historical_backtest.trade_analysis,
            start_time=start_time,
            end_time=end_time,
        ),
        encoding="utf-8",
    )
    (output_dir / "trade_analysis.md").write_text(
        _render_trade_analysis_markdown(historical_backtest.trade_analysis),
        encoding="utf-8",
    )
    (output_dir / "phase_1_4_summary.md").write_text(
        _render_phase_1_4_summary_markdown(
            weekly_context_compare=weekly_context_compare,
            replay_audit=historical_backtest.replay_audit,
            metrics=historical_backtest.metrics,
        ),
        encoding="utf-8",
    )


async def _backtest_symbol(
    *,
    symbol: SymbolInfo,
    params: StrategyParams,
    start_time: datetime,
    end_time: datetime,
    mode: BacktestMode,
    runner: StrategyRunner,
    diagnoser: StrategyDiagnoser,
    kline_repo: KlineRepository,
    module_c_repo: ModuleCRepository,
) -> dict[str, Any]:
    started = perf_counter()
    await kline_repo.prime_symbol_cache(symbol.symbol_id, start_time=start_time, end_time=end_time)
    await module_c_repo.prime_symbol_cache(symbol.symbol_id)
    gate_counts = _blank_gate_counts()
    trades: list[Trade] = []
    success_samples: list[dict[str, Any]] = []
    failure_samples: list[dict[str, Any]] = []
    leak_violations: list[dict[str, Any]] = []
    replayed_symbols = 0
    total_replay_steps = 0
    total_symbol_time_points = 0
    weekly_context_found = 0
    daily_setup_found = 0
    entry_watch_found = 0
    entry_trigger_found = 0
    trade_opened = 0
    leak_free = True
    position: Trade | None = None
    try:
        bars_30f = await kline_repo.get_klines(symbol.symbol_id, "30f", start=start_time, end=end_time)
        if bars_30f:
            replayed_symbols = 1
        total_symbol_time_points = len(bars_30f)

        for index, bar in enumerate(bars_30f):
            as_of_time = bar.ts
            diagnosis = await diagnoser.diagnose_symbol(symbol, as_of_time=as_of_time, params=params)
            total_replay_steps += 1
            _accumulate_gate_counts(gate_counts, diagnosis)
            leak_free, leak_violations = _collect_leak_check(
                diagnosis=diagnosis,
                as_of_time=as_of_time,
                leak_free=leak_free,
                leak_violations=leak_violations,
            )
            if diagnosis.weekly_context is not None:
                weekly_context_found += 1
            if diagnosis.daily_setup is not None:
                daily_setup_found += 1
            if diagnosis.result is not None and diagnosis.result.status in {ScanStatus.WATCH, ScanStatus.TRIGGER}:
                entry_watch_found += 1
            if diagnosis.result is not None and diagnosis.result.status == ScanStatus.TRIGGER:
                entry_trigger_found += 1
            if diagnosis.failed_gate is not None and len(failure_samples) < 5:
                failure_samples.append(
                    {
                        "symbol": symbol.symbol,
                        "name": symbol.name,
                        "as_of_time": as_of_time.isoformat(),
                        "failed_gate": diagnosis.failed_gate,
                        "failed_reason": diagnosis.failed_reason,
                        "weekly_context_mode": diagnosis.weekly_context.context_mode if diagnosis.weekly_context else None,
                    }
                )

            if position is None:
                result = diagnosis.result
                if result is None:
                    continue
                if mode == BacktestMode.EXPLORATORY_STATIC:
                    should_open = result.status in {ScanStatus.WATCH, ScanStatus.TRIGGER}
                else:
                    should_open = result.status == ScanStatus.TRIGGER
                if not should_open:
                    continue
                next_bar = bars_30f[index + 1] if index + 1 < len(bars_30f) else None
                if next_bar is None:
                    continue
                position = Trade(
                    symbol=symbol,
                    entry_time=next_bar.ts,
                    entry_price=next_bar.open,
                    entry_reason=result.status.value,
                    entry_confidence=result.entry.confidence_score,
                    entry_level=result.entry.entry_level or "30f",
                    daily_b1_price=result.daily_setup.daily_b1.price,
                    stop_price=result.daily_setup.daily_b1.price,
                    features=_trade_features_from_result(result),
                )
                trade_opened += 1
                continue

            position.holding_bars += 1
            position.holding_days = max(position.holding_days, (bar.ts.date() - position.entry_time.date()).days)
            mfe = (bar.high - position.entry_price) / position.entry_price
            mae = (bar.low - position.entry_price) / position.entry_price
            position.max_favorable_pct = max(position.max_favorable_pct, mfe)
            position.max_adverse_pct = min(position.max_adverse_pct, mae)
            result = await runner.evaluate_symbol(symbol, as_of_time=as_of_time, params=params)
            if result is None:
                continue
            exit_decision = await runner.diagnoser.exit_evaluator.evaluate(position, as_of_time, result.daily_setup)
            if not exit_decision.should_exit:
                continue
            execution = await kline_repo.get_next_open(
                symbol.symbol_id,
                exit_decision.execution_timeframe or "30f",
                exit_decision.signal_time or as_of_time,
            )
            if execution is None:
                position.exit_time = bar.ts
                position.exit_price = bar.close
            else:
                position.exit_time = execution[0]
                position.exit_price = execution[1]
            position.exit_reason = exit_decision.reason
            trades.append(position)
            if position.return_pct is not None and position.return_pct > 0 and len(success_samples) < 5:
                success_samples.append(_trade_case(position))
            elif position.return_pct is not None and len(failure_samples) < 10:
                failure_samples.append(_trade_case(position))
            position = None

        if position is not None and bars_30f:
            last_bar: KlineBar = bars_30f[-1]
            position.exit_time = last_bar.ts
            position.exit_price = last_bar.close
            position.exit_reason = "FORCED_END"
            trades.append(position)
            if position.return_pct is not None and position.return_pct > 0 and len(success_samples) < 5:
                success_samples.append(_trade_case(position))
            elif position.return_pct is not None and len(failure_samples) < 10:
                failure_samples.append(_trade_case(position))
        return {
            "replayed_symbols": replayed_symbols,
            "total_replay_steps": total_replay_steps,
            "total_symbol_time_points": total_symbol_time_points,
            "weekly_context_found": weekly_context_found,
            "daily_setup_found": daily_setup_found,
            "entry_watch_found": entry_watch_found,
            "entry_trigger_found": entry_trigger_found,
            "trade_opened": trade_opened,
            "trades": trades,
            "gate_counts": gate_counts,
            "leak_free": leak_free,
            "leak_violations": leak_violations,
            "success_samples": success_samples,
            "failure_samples": failure_samples,
            "elapsed_seconds": perf_counter() - started,
        }
    finally:
        kline_repo.release_symbol_cache(symbol.symbol_id)
        module_c_repo.release_symbol_cache(symbol.symbol_id)


def _trade_features_from_result(result) -> dict[str, Any]:
    return {
        "weekly_context_mode": result.weekly_context.context_mode,
        "weekly_context_score": result.weekly_context.context_score,
        "weekly_bsp_type": result.weekly_context.weekly_bsp_type,
        "weekly_b1_time": result.weekly_context.weekly_b1.point_time.isoformat() if result.weekly_context.weekly_b1 else None,
        "weekly_b2_time": result.weekly_context.weekly_b2.point_time.isoformat(),
        "daily_b1_time": result.daily_setup.daily_b1.point_time.isoformat(),
        "daily_b2_time": result.daily_setup.daily_b2.point_time.isoformat() if result.daily_setup.daily_b2 else None,
        "thirty_f_b1_time": result.entry.thirty_b1.point_time.isoformat() if result.entry.thirty_b1 else None,
        "five_f_b2_time": result.entry.five_b2_confirm.point_time.isoformat() if result.entry.five_b2_confirm else None,
        "confidence_score": result.entry.confidence_score,
        "entry_level": result.entry.entry_level,
        "stop_reference_source": result.weekly_context.stop_reference_source,
        "stop_reference_price": result.weekly_context.stop_reference_price,
    }


def _trade_case(trade: Trade) -> dict[str, Any]:
    return {
        "symbol": trade.symbol.symbol,
        "name": trade.symbol.name,
        "entry_time": trade.entry_time.isoformat(),
        "entry_price": trade.entry_price,
        "weekly_context_mode": _trade_feature(trade, "weekly_context_mode"),
        "weekly_context_score": _trade_feature(trade, "weekly_context_score"),
        "weekly_b2_time": _trade_feature(trade, "weekly_b2_time"),
        "daily_b1_time": _trade_feature(trade, "daily_b1_time"),
        "daily_b2_time": _trade_feature(trade, "daily_b2_time"),
        "thirty_f_b1_time": _trade_feature(trade, "thirty_f_b1_time"),
        "five_f_b2_time": _trade_feature(trade, "five_f_b2_time"),
        "confidence_score": trade.entry_confidence,
        "entry_level": trade.entry_level,
        "exit_time": trade.exit_time.isoformat() if trade.exit_time else None,
        "exit_price": trade.exit_price,
        "exit_reason": trade.exit_reason,
        "return_pct": trade.return_pct,
    }


def _blank_gate_counts() -> dict[str, Counter[str]]:
    return {gate: Counter({"reached": 0, "passed": 0, "failed": 0}) for gate in GATE_ORDER}


def _accumulate_gate_counts(target: dict[str, Counter[str]], diagnosis: ScanDiagnosis) -> None:
    for gate in diagnosis.gates:
        bucket = target.setdefault(gate.name, Counter({"reached": 0, "passed": 0, "failed": 0}))
        bucket["reached"] += 1
        bucket["passed"] += int(gate.passed)
        bucket["failed"] += int(not gate.passed)


def _merge_gate_counts(target: dict[str, Counter[str]], source: dict[str, Counter[str]]) -> None:
    for gate_name, bucket in source.items():
        target_bucket = target.setdefault(gate_name, Counter({"reached": 0, "passed": 0, "failed": 0}))
        target_bucket.update(bucket)


def _gate_row(gate_name: str, bucket: Counter[str], *, total_steps: int) -> dict[str, Any]:
    reached = int(bucket.get("reached", 0))
    passed = int(bucket.get("passed", 0))
    failed = int(bucket.get("failed", 0))
    return {
        "gate": gate_name,
        "reached": reached,
        "passed": passed,
        "failed": failed,
        "pass_rate_from_reached": round(passed / reached, 6) if reached else 0.0,
        "pass_rate_from_total": round(passed / total_steps, 6),
    }


def _historical_funnel_rows(stage_counts: Counter[str]) -> list[dict[str, Any]]:
    total = int(stage_counts["total_replay_steps"]) or 1
    rows = []
    for stage in (
        "weekly_context_found",
        "daily_setup_found",
        "entry_watch_found",
        "entry_trigger_found",
        "trade_opened",
    ):
        passed = int(stage_counts[stage])
        rows.append(
            {
                "stage": stage,
                "passed": passed,
                "pass_rate_from_total_steps": round(passed / total, 6),
            }
        )
    return rows


def _collect_leak_check(
    *,
    diagnosis: ScanDiagnosis,
    as_of_time: datetime,
    leak_free: bool,
    leak_violations: list[dict[str, Any]],
) -> tuple[bool, list[dict[str, Any]]]:
    for level, head in diagnosis.heads.items():
        if head is None or head.bar_until <= as_of_time:
            continue
        leak_free = False
        if len(leak_violations) < 20:
            leak_violations.append(
                {
                    "symbol": diagnosis.symbol.symbol,
                    "level": level,
                    "as_of_time": as_of_time.isoformat(),
                    "head_bar_until": head.bar_until.isoformat(),
                    "run_id": head.run_id,
                }
            )
    return leak_free, leak_violations


async def _gather_bounded(tasks: list, *, concurrency: int):
    results = []
    for index in range(0, len(tasks), concurrency):
        results.extend(await asyncio.gather(*(task() for task in tasks[index : index + concurrency])))
    return results


def _append_limited(target: list[dict[str, Any]], rows: list[dict[str, Any]], *, limit: int) -> None:
    for row in rows:
        if len(target) >= limit:
            return
        target.append(row)


def _trade_feature(trade: Trade, key: str) -> Any:
    return trade.features.get(key)


def _confidence_bucket(value: float) -> str:
    if value >= 100.0:
        return "100"
    if value >= 70.0:
        return "70"
    if value >= 40.0:
        return "40"
    return "<40"


def _percentile_float(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * ratio))
    return round(float(ordered[index]), 3)


def _render_weekly_context_compare_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Weekly Context Compare",
        "",
        f"- As of: `{payload['as_of_time']}`",
        f"- Symbols: `{payload['symbol_count']}`",
        "",
        "| Mode | Weekly B2 Types | Weekly Contexts | Daily Setups | Entry Watch | Trigger | Trigger 30F |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload["rows"]:
        lines.append(
            f"| `{row['weekly_context_mode']}` | `{','.join(row['weekly_b2_types'])}` | "
            f"{row['weekly_context_count']} | {row['daily_setup_count']} | {row['entry_watch_count']} | "
            f"{row['trigger_count']} | {row['trigger_30f_count']} |"
        )
    lines.append("")
    for row in payload["rows"]:
        lines.append(f"## {row['weekly_context_mode']}")
        lines.append("")
        lines.append(f"- Top failure gates: `{json.dumps(row['top_failure_gates'], ensure_ascii=False)}`")
        lines.append("")
    return "\n".join(lines) + "\n"


def _render_replay_audit_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Replay Audit",
        "",
        f"- Strategy: `{payload['strategy_code']}`",
        f"- Weekly context mode: `{payload['weekly_context_mode']}`",
        f"- first_seen_time_source: `{payload['first_seen_time_source']}`",
        f"- replayed_symbols: `{payload['replayed_symbols']}`",
        f"- total_replay_steps: `{payload['total_replay_steps']}`",
        f"- total_symbol_time_points: `{payload['total_symbol_time_points']}`",
        f"- future_leakage_detected: `{payload['future_leakage_detected']}`",
        f"- future_leakage_violation_count: `{payload['future_leakage_violation_count']}`",
        f"- symbol_backtest_elapsed_seconds_total: `{payload['symbol_backtest_elapsed_seconds_total']}`",
        f"- backtest_elapsed_seconds: `{payload['backtest_elapsed_seconds']}`",
        f"- symbol_backtest_elapsed_seconds_p50: `{payload['symbol_backtest_elapsed_seconds_p50']}`",
        f"- symbol_backtest_elapsed_seconds_p95: `{payload['symbol_backtest_elapsed_seconds_p95']}`",
    ]
    if payload["future_leakage_samples"]:
        lines.extend(
            [
                "",
                "## Future Leakage Samples",
                "",
                f"`{json.dumps(payload['future_leakage_samples'], ensure_ascii=False, indent=2)}`",
            ]
        )
    return "\n".join(lines) + "\n"


def _render_historical_gate_waterfall_markdown(payload: dict[str, Any]) -> str:
    lines = [render_gate_waterfall_markdown(payload["rows"]).rstrip(), "", "## Historical Funnel", "", "| Stage | Passed | Pass/Total Steps |", "| --- | ---: | ---: |"]
    for row in payload["historical_funnel"]:
        lines.append(f"| `{row['stage']}` | {row['passed']} | {row['pass_rate_from_total_steps']:.4f} |")
    return "\n".join(lines) + "\n"


def _render_backtest_report_markdown(
    metrics: dict[str, Any],
    replay_audit: dict[str, Any],
    trade_analysis: dict[str, Any],
    *,
    start_time: datetime,
    end_time: datetime,
) -> str:
    lines = [
        "# Backtest Report",
        "",
        f"- Start: `{start_time.isoformat()}`",
        f"- End: `{end_time.isoformat()}`",
        f"- Weekly context mode: `{replay_audit['weekly_context_mode']}`",
        f"- Replayed symbols: `{replay_audit['replayed_symbols']}`",
        f"- Total replay steps: `{replay_audit['total_replay_steps']}`",
        "",
        "## Metrics",
        "",
        f"- Total trades: `{metrics['total_trades']}`",
        f"- Win rate: `{metrics['win_rate']:.4f}`",
        f"- Avg return: `{metrics['avg_return']:.4f}`",
        f"- Median return: `{metrics.get('median_return', 0.0):.4f}`",
        f"- Profit factor: `{metrics['profit_factor']}`",
        f"- Max drawdown: `{metrics['max_drawdown']:.4f}`",
        f"- Avg holding bars: `{metrics['avg_holding_bars']:.2f}`",
        f"- Avg win: `{metrics.get('avg_win', 0.0):.4f}`",
        f"- Avg loss: `{metrics.get('avg_loss', 0.0):.4f}`",
        "",
        "## Distributions",
        "",
        f"- Weekly context mode: `{json.dumps(trade_analysis['weekly_context_mode_distribution'], ensure_ascii=False)}`",
        f"- Confidence: `{json.dumps(trade_analysis['confidence_distribution'], ensure_ascii=False)}`",
        f"- Entry level: `{json.dumps(trade_analysis['entry_level_distribution'], ensure_ascii=False)}`",
        f"- Exit reason: `{json.dumps(trade_analysis['exit_reason_distribution'], ensure_ascii=False)}`",
    ]
    return "\n".join(lines) + "\n"


def _render_trade_analysis_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Trade Analysis",
        "",
        "## Success Samples",
        "",
    ]
    for sample in payload["success_samples"]:
        lines.append(f"- `{sample['symbol']}` entry=`{sample['entry_time']}` exit=`{sample['exit_time']}` return=`{sample['return_pct']}` reason=`{sample['exit_reason']}`")
    if not payload["success_samples"]:
        lines.append("- none")
    lines.extend(["", "## Failure Samples", ""])
    for sample in payload["failure_samples"]:
        if "failed_gate" in sample:
            lines.append(
                f"- `{sample['symbol']}` as_of=`{sample['as_of_time']}` failed_gate=`{sample['failed_gate']}` reason=`{sample['failed_reason']}`"
            )
        else:
            lines.append(f"- `{sample['symbol']}` entry=`{sample['entry_time']}` exit=`{sample['exit_time']}` return=`{sample['return_pct']}` reason=`{sample['exit_reason']}`")
    if not payload["failure_samples"]:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def _render_phase_1_4_summary_markdown(
    *,
    weekly_context_compare: dict[str, Any],
    replay_audit: dict[str, Any],
    metrics: dict[str, Any],
) -> str:
    best_compare = max(weekly_context_compare["rows"], key=lambda item: item["trigger_count"], default=None)
    lines = [
        "# Phase 1.4 Summary",
        "",
        f"- Weekly context compare modes: `{len(weekly_context_compare['rows'])}`",
        f"- Replay mode: `{replay_audit['weekly_context_mode']}`",
        f"- Replayed symbols: `{replay_audit['replayed_symbols']}`",
        f"- Total replay steps: `{replay_audit['total_replay_steps']}`",
        f"- Future leakage detected: `{replay_audit['future_leakage_detected']}`",
        f"- Total trades: `{metrics['total_trades']}`",
        f"- Win rate: `{metrics['win_rate']:.4f}`",
        f"- Max drawdown: `{metrics['max_drawdown']:.4f}`",
    ]
    if best_compare is not None:
        lines.append(
            f"- Highest trigger mode: `{best_compare['weekly_context_mode']}` trigger_count=`{best_compare['trigger_count']}` trigger_30f_count=`{best_compare['trigger_30f_count']}`"
        )
    return "\n".join(lines) + "\n"


def _write_phase_1_4_trades_csv(path: Path, trades: list[Trade]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "name",
                "entry_time",
                "entry_price",
                "weekly_context_mode",
                "weekly_context_score",
                "weekly_b2_time",
                "weekly_b1_time",
                "daily_b1_time",
                "daily_b2_time",
                "30f_b1_time",
                "5f_b2_time",
                "confidence_score",
                "entry_level",
                "exit_time",
                "exit_price",
                "exit_reason",
                "return_pct",
                "holding_bars",
                "holding_days",
                "max_favorable_pct",
                "max_adverse_pct",
            ],
        )
        writer.writeheader()
        for trade in trades:
            writer.writerow(
                {
                    "symbol": trade.symbol.symbol,
                    "name": trade.symbol.name,
                    "entry_time": trade.entry_time.isoformat(),
                    "entry_price": trade.entry_price,
                    "weekly_context_mode": _trade_feature(trade, "weekly_context_mode"),
                    "weekly_context_score": _trade_feature(trade, "weekly_context_score"),
                    "weekly_b2_time": _trade_feature(trade, "weekly_b2_time"),
                    "weekly_b1_time": _trade_feature(trade, "weekly_b1_time"),
                    "daily_b1_time": _trade_feature(trade, "daily_b1_time"),
                    "daily_b2_time": _trade_feature(trade, "daily_b2_time"),
                    "30f_b1_time": _trade_feature(trade, "thirty_f_b1_time"),
                    "5f_b2_time": _trade_feature(trade, "five_f_b2_time"),
                    "confidence_score": trade.entry_confidence,
                    "entry_level": trade.entry_level,
                    "exit_time": trade.exit_time.isoformat() if trade.exit_time else "",
                    "exit_price": trade.exit_price if trade.exit_price is not None else "",
                    "exit_reason": trade.exit_reason or "",
                    "return_pct": trade.return_pct if trade.return_pct is not None else "",
                    "holding_bars": trade.holding_bars,
                    "holding_days": trade.holding_days,
                    "max_favorable_pct": trade.max_favorable_pct,
                    "max_adverse_pct": trade.max_adverse_pct,
                }
            )
