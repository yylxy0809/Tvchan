from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

from app.domain.models import Trade


def write_report(output_dir: Path, trades: list[Trade], metrics: dict, metadata: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_trades(output_dir / "trades.csv", trades)
    _write_equity_curve(output_dir / "equity_curve.csv", trades)
    _write_monthly_returns(output_dir / "monthly_returns.csv", trades)
    (output_dir / "exit_reason_distribution.json").write_text(
        json.dumps(metrics.get("exit_reason_distribution", {}), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "entry_level_distribution.json").write_text(
        json.dumps(metrics.get("entry_level_distribution", {}), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "confidence_distribution.json").write_text(
        json.dumps(metrics.get("confidence_distribution", {}), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "backtest_perf.json").write_text(
        json.dumps(metadata.get("backtest_perf", {}), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "metrics.json").write_text(
        json.dumps({"metrics": metrics, "metadata": metadata}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "report.md").write_text(_render_markdown(metrics, metadata), encoding="utf-8")


def _write_trades(path: Path, trades: list[Trade]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "entry_time",
                "entry_price",
                "exit_time",
                "exit_price",
                "exit_reason",
                "return_pct",
                "entry_confidence",
                "entry_level",
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
                    "entry_time": trade.entry_time.isoformat(),
                    "entry_price": trade.entry_price,
                    "exit_time": trade.exit_time.isoformat() if trade.exit_time else "",
                    "exit_price": trade.exit_price if trade.exit_price is not None else "",
                    "exit_reason": trade.exit_reason or "",
                    "return_pct": trade.return_pct if trade.return_pct is not None else "",
                    "entry_confidence": trade.entry_confidence,
                    "entry_level": trade.entry_level,
                    "holding_bars": trade.holding_bars,
                    "holding_days": trade.holding_days,
                    "max_favorable_pct": trade.max_favorable_pct,
                    "max_adverse_pct": trade.max_adverse_pct,
                }
            )


def _write_equity_curve(path: Path, trades: list[Trade]) -> None:
    closed = sorted((trade for trade in trades if trade.return_pct is not None), key=lambda item: item.exit_time or item.entry_time)
    equity = 1.0
    peak = 1.0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["step", "symbol", "exit_time", "return_pct", "equity", "peak", "drawdown"],
        )
        writer.writeheader()
        for step, trade in enumerate(closed, start=1):
            value = trade.return_pct or 0.0
            equity *= 1 + value
            peak = max(peak, equity)
            drawdown = (equity - peak) / peak if peak else 0.0
            writer.writerow(
                {
                    "step": step,
                    "symbol": trade.symbol.symbol,
                    "exit_time": trade.exit_time.isoformat() if trade.exit_time else "",
                    "return_pct": value,
                    "equity": equity,
                    "peak": peak,
                    "drawdown": drawdown,
                }
            )


def _write_monthly_returns(path: Path, trades: list[Trade]) -> None:
    month_buckets: dict[str, list[float]] = defaultdict(list)
    for trade in trades:
        if trade.return_pct is None or trade.exit_time is None:
            continue
        month_buckets[trade.exit_time.strftime("%Y-%m")].append(trade.return_pct)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["month", "trade_count", "avg_return", "compounded_return"],
        )
        writer.writeheader()
        for month in sorted(month_buckets):
            returns = month_buckets[month]
            compounded = 1.0
            for value in returns:
                compounded *= 1 + value
            writer.writerow(
                {
                    "month": month,
                    "trade_count": len(returns),
                    "avg_return": sum(returns) / len(returns),
                    "compounded_return": compounded - 1,
                }
            )


def _render_markdown(metrics: dict, metadata: dict) -> str:
    lines = [
        "# Strategy Backtest Report",
        "",
        f"- Strategy: `{metadata['strategy_code']}`",
        f"- Version: `{metadata.get('strategy_version', 'v1')}`",
        f"- Mode: `{metadata['run_mode']}`",
        f"- Symbols: `{metadata['total_symbols']}`",
        f"- Eligible symbols: `{metadata.get('eligible_symbols', 0)}`",
        f"- Market cap policy: `{metadata.get('market_cap_policy', '')}`",
        f"- Market cap rule enabled: `{metadata.get('market_cap_rule_enabled', False)}`",
        f"- Market cap filter applied: `{metadata.get('market_cap_filter_applied', False)}`",
        f"- Market cap hard filter effective: `{metadata.get('market_cap_hard_filter_effective', False)}`",
        f"- Market cap missing allowed: `{metadata.get('market_cap_missing_allowed', False)}`",
        f"- Market cap coverage ratio: `{metadata.get('market_cap_data_coverage_ratio', 0.0)}`",
        f"- first_seen_time_source: `{metadata.get('first_seen_time_source', '')}`",
        "",
    ]
    if metadata.get("market_cap_rule_enabled") and not metadata.get("market_cap_hard_filter_effective"):
        lines.extend(
            [
                "WARNING: market cap data coverage is "
                f"{metadata.get('market_cap_data_coverage_ratio', 0.0)}%; market_cap_min was not effectively enforceable under "
                f"{metadata.get('market_cap_policy', '')}.",
                "",
            ]
        )
    lines.extend(
        [
        "## Metrics",
        "",
        f"- Total trades: `{metrics['total_trades']}`",
        f"- Win rate: `{metrics['win_rate']:.4f}`",
        f"- Avg return: `{metrics['avg_return']:.4f}`",
        f"- Median return: `{metrics.get('median_return', 0.0):.4f}`",
        f"- Profit factor: `{metrics['profit_factor']}`",
        f"- Max drawdown: `{metrics['max_drawdown']:.4f}`",
        f"- Avg holding bars: `{metrics['avg_holding_bars']:.2f}`",
        f"- Median holding bars: `{metrics.get('median_holding_bars', 0.0):.2f}`",
        f"- Max holding bars: `{metrics.get('max_holding_bars', 0)}`",
        f"- Avg win: `{metrics.get('avg_win', 0.0):.4f}`",
        f"- Avg loss: `{metrics.get('avg_loss', 0.0):.4f}`",
        f"- Payoff ratio: `{metrics.get('payoff_ratio', 0.0):.4f}`",
        f"- Max consecutive losses: `{metrics.get('max_consecutive_losses', 0)}`",
        "",
        "## Distributions",
        "",
        f"- Exit reasons: `{metadata_or_json(metrics.get('exit_reason_distribution', {}))}`",
        f"- Confidence: `{metadata_or_json(metrics.get('confidence_distribution', {}))}`",
        f"- Entry levels: `{metadata_or_json(metrics.get('entry_level_distribution', {}))}`",
        "",
        "## Coverage Summary",
        "",
        f"- Data coverage summary: `{metadata_or_json(metadata.get('data_coverage_summary', {}))}`",
        f"- Top failure gates: `{metadata_or_json(metadata.get('top_failure_gates', []))}`",
        f"- Backtest perf: `{metadata_or_json(metadata.get('backtest_perf', {}))}`",
    ]
    )
    return "\n".join(lines) + "\n"


def metadata_or_json(value) -> str:
    return json.dumps(value, ensure_ascii=False)
