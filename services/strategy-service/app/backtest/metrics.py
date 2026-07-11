from __future__ import annotations

from collections import Counter
from statistics import median

from app.domain.models import Trade


def compute_metrics(trades: list[Trade]) -> dict:
    closed = [trade for trade in trades if trade.return_pct is not None]
    if not closed:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "median_return": 0.0,
            "profit_factor": 0.0,
            "max_drawdown": 0.0,
            "avg_holding_bars": 0.0,
            "median_holding_bars": 0.0,
            "max_holding_bars": 0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "payoff_ratio": 0.0,
            "max_consecutive_losses": 0,
            "exit_reason_distribution": {},
            "confidence_distribution": {},
            "entry_level_distribution": {},
        }
    returns = [trade.return_pct or 0.0 for trade in closed]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value <= 0]
    profit_factor = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else float("inf")
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in returns:
        equity *= 1 + value
        peak = max(peak, equity)
        drawdown = (equity - peak) / peak
        max_drawdown = min(max_drawdown, drawdown)
    consecutive_losses = 0
    max_consecutive_losses = 0
    for value in returns:
        if value <= 0:
            consecutive_losses += 1
            max_consecutive_losses = max(max_consecutive_losses, consecutive_losses)
        else:
            consecutive_losses = 0
    confidence_distribution = Counter(_confidence_bucket(trade.entry_confidence) for trade in closed)
    entry_level_distribution = Counter(trade.entry_level for trade in closed)
    exit_reason_distribution = Counter((trade.exit_reason or "UNKNOWN") for trade in closed)
    return {
        "total_trades": len(closed),
        "win_rate": len(wins) / len(closed),
        "avg_return": sum(returns) / len(closed),
        "median_return": median(returns),
        "profit_factor": profit_factor,
        "max_drawdown": abs(max_drawdown),
        "avg_holding_bars": sum(trade.holding_bars for trade in closed) / len(closed),
        "median_holding_bars": median(trade.holding_bars for trade in closed),
        "max_holding_bars": max(trade.holding_bars for trade in closed),
        "avg_win": sum(wins) / len(wins) if wins else 0.0,
        "avg_loss": sum(losses) / len(losses) if losses else 0.0,
        "payoff_ratio": (sum(wins) / len(wins)) / abs(sum(losses) / len(losses)) if wins and losses and sum(losses) != 0 else 0.0,
        "max_consecutive_losses": max_consecutive_losses,
        "exit_reason_distribution": dict(exit_reason_distribution),
        "confidence_distribution": dict(confidence_distribution),
        "entry_level_distribution": dict(entry_level_distribution),
    }


def _confidence_bucket(value: float) -> str:
    if value >= 100.0:
        return "100"
    if value >= 70.0:
        return "70"
    if value >= 40.0:
        return "40"
    return "<40"
