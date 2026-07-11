from __future__ import annotations


def candidate_micro_backtest_gate_v3(*, independent_entry_episode_count: int, future_leakage_detected: bool, all_trigger_traces_complete: bool, fresh_30f_required: bool, official_candidate_isolation_passed: bool, execution_bar_available: bool) -> dict:
    checks = {"no_independent_entry_episode": independent_entry_episode_count > 0, "future_leakage_detected": not future_leakage_detected, "trigger_traces_incomplete": all_trigger_traces_complete, "fresh_30f_not_required": fresh_30f_required, "official_candidate_isolation_failed": official_candidate_isolation_passed, "execution_bar_unavailable": execution_bar_available}
    reasons = [reason for reason, passed in checks.items() if not passed]
    return {"allowed": not reasons, "block_reasons": reasons, "research_only": True, "official_strategy_evaluation": False, "sample_size_insufficient": independent_entry_episode_count < 30}
