from app.engine.phase_1_21_decision import decide_next_phase


def test_small_universe_selects_e():
    assert decide_next_phase(exact_missing_cutoff_count=0, official_trigger_count=0, candidate_trigger_count=0, daily_episode_count=2, symbol_count=1, semantic_blocker=False) == "E_SAMPLE_UNIVERSE_TOO_SMALL"


def test_semantic_blocker_has_priority_even_when_sample_is_small():
    assert decide_next_phase(exact_missing_cutoff_count=0, official_trigger_count=0, candidate_trigger_count=0, daily_episode_count=0, symbol_count=0, semantic_blocker=True) == "F_DATA_OR_SEMANTIC_BLOCKED"


def test_small_universe_without_blocker_remains_e():
    assert decide_next_phase(exact_missing_cutoff_count=0, official_trigger_count=0, candidate_trigger_count=0, daily_episode_count=0, symbol_count=0, semantic_blocker=False) == "E_SAMPLE_UNIVERSE_TOO_SMALL"
