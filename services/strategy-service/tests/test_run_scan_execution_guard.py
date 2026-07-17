from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.cli import run_scan
from app.config.strategy_execution import (
    DIAGNOSTIC_STRATEGY_CODES,
    require_diagnostic_strategy,
)
from app.config.strategy_params import (
    DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_STRATEGY_CODE,
    SANITY_LOOSE_STRATEGY_CODE,
    STRATEGY_CODE,
    STRICT_EXPLICIT_B1_STRATEGY_CODE,
)


EXPECTED_DIAGNOSTIC_STRATEGIES = {
    STRICT_EXPLICIT_B1_STRATEGY_CODE,
    DIAG_TRUST_B2_STRATEGY_CODE,
    DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
    DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
    SANITY_LOOSE_STRATEGY_CODE,
}


def test_execution_guard_allows_only_explicit_diagnostic_strategies() -> None:
    assert DIAGNOSTIC_STRATEGY_CODES == EXPECTED_DIAGNOSTIC_STRATEGIES
    for strategy_code in EXPECTED_DIAGNOSTIC_STRATEGIES:
        params = require_diagnostic_strategy(strategy_code)
        assert params.strategy_code == strategy_code


@pytest.mark.parametrize("strategy_code", [STRATEGY_CODE, "unknown-strategy"])
def test_execution_guard_rejects_official_and_unknown_strategies(strategy_code: str) -> None:
    with pytest.raises(ValueError, match="diagnostic|official"):
        require_diagnostic_strategy(strategy_code)


@pytest.mark.parametrize(
    ("diagnose", "trace"),
    [
        (False, False),
        (True, False),
        (False, True),
    ],
)
@pytest.mark.parametrize("strategy_code", [STRATEGY_CODE, "unknown-strategy"])
def test_blocked_scan_fails_before_database_or_repository_side_effects(
    monkeypatch,
    strategy_code: str,
    diagnose: bool,
    trace: bool,
) -> None:
    pool_created = False
    repository_created = False

    async def create_pool(**_kwargs):
        nonlocal pool_created
        pool_created = True
        raise AssertionError("database pool must not be created for blocked execution")

    def create_repository(_pool):
        nonlocal repository_created
        repository_created = True
        raise AssertionError("repository must not be created for blocked execution")

    monkeypatch.setattr(run_scan, "create_pool", create_pool)
    monkeypatch.setattr(run_scan, "StrategyRepository", create_repository)

    with pytest.raises(ValueError, match="diagnostic|official"):
        asyncio.run(
            run_scan._run(
                SimpleNamespace(
                    strategy=strategy_code,
                    diagnose=diagnose,
                    trace=trace,
                )
            )
        )

    assert pool_created is False
    assert repository_created is False
