from __future__ import annotations


def decide_next_phase(*, exact_missing_cutoff_count: int, official_trigger_count: int, candidate_trigger_count: int, daily_episode_count: int, symbol_count: int, semantic_blocker: bool, backfill_admission_valid: bool = False, resource_estimate_valid: bool = False) -> str:
    if semantic_blocker:
        return "F_DATA_OR_SEMANTIC_BLOCKED"
    if exact_missing_cutoff_count and backfill_admission_valid and resource_estimate_valid:
        return "A_COVERAGE_GAP_BACKFILL_READY"
    if official_trigger_count:
        return "B_OFFICIAL_TRIGGER_SAMPLE_READY"
    if candidate_trigger_count:
        return "C_CANDIDATE_ONLY_REQUIRES_USER_DECISION"
    if daily_episode_count < 30 or symbol_count < 10:
        return "E_SAMPLE_UNIVERSE_TOO_SMALL"
    if semantic_blocker:
        return "F_DATA_OR_SEMANTIC_BLOCKED"
    return "D_STRATEGY_TOO_RESTRICTIVE_REQUIRES_USER_DECISION"
