from app.engine.entry_state_machine_v4 import evaluate_entry_state_v4


def test_confidence_and_visibility_gate_entry():
    assert evaluate_entry_state_v4(as_of_time="2025-01-03T00:00:00+00:00", bottom_visible=True, five_f_visible=True)["confidence"] == 30
    assert evaluate_entry_state_v4(as_of_time="2025-01-03T00:00:00+00:00", thirty_f_first_seen="2025-01-02T00:00:00+00:00", bottom_visible=True)["entry_eligible"] is True
    assert evaluate_entry_state_v4(as_of_time="2025-01-03T00:00:00+00:00", thirty_f_first_seen="2025-01-04T00:00:00+00:00", bottom_visible=True)["entry_eligible"] is False


def test_five_f_cannot_precede_parent_thirty_f():
    result = evaluate_entry_state_v4(as_of_time="2025-01-03T00:00:00+00:00", thirty_f_first_seen="2025-01-02T00:00:00+00:00", five_f_first_seen="2025-01-01T00:00:00+00:00")
    assert result["five_f_counted"] is False


def test_window_and_bound_five_f_gate_scoring():
    late = evaluate_entry_state_v4(as_of_time="2025-01-04T00:00:00+00:00", trigger_window_end="2025-01-03T00:00:00+00:00", thirty_f_first_seen="2025-01-04T00:00:00+00:00", bottom_visible=True)
    assert late["entry_triggered"] is False
    bound = evaluate_entry_state_v4(as_of_time="2025-01-03T00:00:00+00:00", thirty_f_first_seen="2025-01-02T00:00:00+00:00", bottom_visible=True, five_f_first_seen="2025-01-03T00:00:00+00:00", five_f_parent_valid=True)
    assert bound["confidence"] == 100


def test_invalid_five_f_parent_cannot_add_confidence_or_trigger():
    blocked = evaluate_entry_state_v4(as_of_time="2025-01-03T00:00:00+00:00", thirty_f_first_seen="2025-01-02T00:00:00+00:00", bottom_visible=True, five_f_first_seen="2025-01-03T00:00:00+00:00", five_f_parent_valid=False)
    assert blocked["confidence"] == 70
    assert blocked["entry_triggered"] is True
    no_bottom = evaluate_entry_state_v4(as_of_time="2025-01-03T00:00:00+00:00", thirty_f_first_seen="2025-01-02T00:00:00+00:00", bottom_visible=False, five_f_first_seen="2025-01-03T00:00:00+00:00", five_f_parent_valid=False)
    assert no_bottom["confidence"] == 40
    assert no_bottom["entry_triggered"] is False


def test_offset_normalization_beats_lexical_timestamp_order_and_rejects_naive_time():
    result = evaluate_entry_state_v4(as_of_time="2025-01-01T01:00:00+00:00", trigger_window_end="2025-01-01T01:00:00+00:00", thirty_f_first_seen="2025-01-01T08:30:00+08:00", bottom_visible=True)
    assert result["entry_triggered"] is True
    import pytest
    with pytest.raises(ValueError, match="Naive"):
        evaluate_entry_state_v4(as_of_time="2025-01-01T01:00:00", thirty_f_first_seen="2025-01-01T00:00:00+00:00")
