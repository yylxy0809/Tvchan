from __future__ import annotations


def audit_formal_universe_readiness(facts: dict) -> dict:
    required = ("historical_universe_available", "historical_market_cap_available", "listing_status_available", "adjustment_basis_available", "tradability_available", "cost_model_available")
    blockers = [key.replace("_available", "") for key in required if not facts.get(key, False)]
    return {"formal_backtest_allowed": not blockers, "blockers": blockers, "facts": facts}
