from app.engine.formal_universe_readiness_audit import audit_formal_universe_readiness


def test_missing_historical_universe_blocks_formal_backtest():
    result = audit_formal_universe_readiness({"historical_universe_available": False})
    assert result["formal_backtest_allowed"] is False
    assert "historical_universe" in result["blockers"]
