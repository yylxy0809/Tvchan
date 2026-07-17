from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.cli import run_backtest
from app.config.strategy_params import (
    DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_STRATEGY_CODE,
    SANITY_LOOSE_STRATEGY_CODE,
    STRATEGY_CODE,
    STRICT_EXPLICIT_B1_STRATEGY_CODE,
)


DIAGNOSTIC_STRATEGIES = {
    STRICT_EXPLICIT_B1_STRATEGY_CODE,
    DIAG_TRUST_B2_STRATEGY_CODE,
    DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
    DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
    SANITY_LOOSE_STRATEGY_CODE,
}


def test_execution_guard_allows_only_explicit_diagnostic_strategies() -> None:
    for strategy_code in DIAGNOSTIC_STRATEGIES:
        params = run_backtest.require_diagnostic_backtest_strategy(strategy_code)
        assert params.strategy_code == strategy_code


@pytest.mark.parametrize("strategy_code", [STRATEGY_CODE, "unknown-strategy"])
def test_execution_guard_rejects_official_and_unknown_strategies(strategy_code: str) -> None:
    with pytest.raises(ValueError, match="diagnostic|official"):
        run_backtest.require_diagnostic_backtest_strategy(strategy_code)


@pytest.mark.parametrize("strategy_code", [STRATEGY_CODE, "unknown-strategy"])
def test_blocked_strategy_fails_before_database_pool_creation(monkeypatch, strategy_code: str) -> None:
    pool_created = False

    async def create_pool(**_kwargs):
        nonlocal pool_created
        pool_created = True
        raise AssertionError("database pool must not be created for blocked execution")

    monkeypatch.setattr(run_backtest, "create_pool", create_pool)

    with pytest.raises(ValueError, match="diagnostic|official"):
        asyncio.run(
            run_backtest._run(
                SimpleNamespace(
                    strategy=strategy_code,
                    concurrency=1,
                )
            )
        )

    assert pool_created is False
