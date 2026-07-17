from __future__ import annotations

from app.config.strategy_params import (
    DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
    DIAG_TRUST_B2_STRATEGY_CODE,
    SANITY_LOOSE_STRATEGY_CODE,
    STRATEGY_CODE,
    STRICT_EXPLICIT_B1_STRATEGY_CODE,
    StrategyParams,
)


DIAGNOSTIC_STRATEGY_CODES = frozenset(
    {
        STRICT_EXPLICIT_B1_STRATEGY_CODE,
        DIAG_TRUST_B2_STRATEGY_CODE,
        DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
        DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
        SANITY_LOOSE_STRATEGY_CODE,
    }
)


def require_diagnostic_strategy(strategy_code: str) -> StrategyParams:
    if strategy_code == STRATEGY_CODE:
        raise ValueError(
            "official strategy execution is unavailable in generic strategy runners; "
            "the current official decision remains NO_GO"
        )
    if strategy_code not in DIAGNOSTIC_STRATEGY_CODES:
        raise ValueError(f"unsupported diagnostic strategy: {strategy_code}")
    return StrategyParams.from_strategy_code(strategy_code)
