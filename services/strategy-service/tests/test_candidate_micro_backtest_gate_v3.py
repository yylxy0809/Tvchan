from app.engine.candidate_micro_backtest_gate_v3 import candidate_micro_backtest_gate_v3


def test_candidate_backtest_requires_independent_trigger_and_all_safety_gates():
    blocked = candidate_micro_backtest_gate_v3(independent_entry_episode_count=0, future_leakage_detected=False, all_trigger_traces_complete=True, fresh_30f_required=True, official_candidate_isolation_passed=True, execution_bar_available=True)
    assert blocked["allowed"] is False
    assert "no_independent_entry_episode" in blocked["block_reasons"]
