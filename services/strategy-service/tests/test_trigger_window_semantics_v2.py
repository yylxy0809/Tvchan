from app.engine.trigger_window_semantics_v2 import bind_five_f_parent, evaluate_policy_matrix, select_valid_five_f_confirmation


def test_official_window_blocks_late_signal_and_five_f_is_bound():
    rows = evaluate_policy_matrix(as_of_time="2025-01-03T00:00:00+00:00", trigger_window_end="2025-01-02T00:00:00+00:00", thirty_f_first_seen="2025-01-03T00:00:00+00:00", thirty_f_confirm_time=None, bottom_visible=True, five_f_first_seen="2025-01-03T00:00:00+00:00", five_f_confirm_time=None)
    assert rows[0]["entry_triggered"] is False


def test_trading_session_and_candidate_use_real_supplied_times():
    rows = evaluate_policy_matrix(as_of_time="2025-01-04T00:00:00+00:00", trigger_window_end="2025-01-03T00:00:00+00:00", trading_session_window_end="2025-01-02T00:00:00+00:00", thirty_f_first_seen=None, thirty_f_confirm_time=None, one_p_first_seen="2025-01-02T00:00:00+00:00", bottom_visible=True, five_f_first_seen="2025-01-02T00:00:00+00:00", five_f_confirm_time=None, has_1p=True)
    candidate = next(row for row in rows if row["policy"] == "candidate_1p_research_only")
    session = next(row for row in rows if row["policy"] == "diagnostic_trading_session_window_first_seen")
    assert candidate["thirty_f_time_used"] == "2025-01-02T00:00:00+00:00"
    assert session["trigger_window_end_used"] == "2025-01-02T00:00:00+00:00"


def test_five_f_binding_requires_window_and_explicit_structure_parent_evidence():
    result = bind_five_f_parent(b1={"identity": "b1", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T01:00:00+00:00"}, five={"symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T02:00:00+00:00", "parent_30f_identity": None}, trigger_window_end="2025-01-01T03:00:00+00:00", as_of_time="2025-01-01T02:30:00+00:00")
    assert result["valid"] is False
    assert result["reason"] == "parent_evidence_unavailable"


def test_direct_parent_identity_requires_valid_five_f_buy_b1_and_price_before_scoring():
    b1 = {"identity": "thirty-b1", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T01:00:00+00:00"}
    five = {"symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T03:00:00+00:00", "price_x1000": 110, "parent_30f_identity": "thirty-b1"}
    parent = {"identity": "five-b1", "symbol": "x", "mode": "predictive", "side": "buy", "bsp_type": "1", "first_seen_time": "2025-01-01T02:00:00+00:00", "price_x1000": 100, "first_seen_run_id": 7}
    binding = bind_five_f_parent(b1=b1, five=five, trigger_window_end="2025-01-01T04:00:00+00:00", as_of_time="2025-01-01T04:00:00+00:00", five_f_b1_candidates=[parent])
    assert binding["valid"] is True
    assert binding["evidence_method"] == "direct_30f_binding_plus_validated_5f_b1"
    assert binding["five_f_b1_run_id"] == 7
    rows = evaluate_policy_matrix(as_of_time="2025-01-01T03:00:00+00:00", trigger_window_end="2025-01-01T03:00:00+00:00", thirty_f_first_seen=b1["first_seen_time"], thirty_f_confirm_time=None, bottom_visible=True, five_f_first_seen=five["first_seen_time"], five_f_confirm_time=None, five_f_parent_valid=binding["valid"])
    assert rows[0]["confidence"] == 100


def test_direct_parent_cannot_bypass_five_f_b1_price_side_or_identity_contract():
    b1 = {"identity": "thirty-b1", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T01:00:00+00:00"}
    base = {"symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T03:00:00+00:00", "price_x1000": 110, "parent_30f_identity": "thirty-b1"}
    valid_parent = {"identity": "five-b1", "symbol": "x", "mode": "predictive", "side": "buy", "bsp_type": "1", "first_seen_time": "2025-01-01T02:00:00+00:00", "price_x1000": 100}
    missing = bind_five_f_parent(b1=b1, five=base, trigger_window_end="2025-01-01T04:00:00+00:00", as_of_time="2025-01-01T04:00:00+00:00")
    sell_parent = bind_five_f_parent(b1=b1, five=base, trigger_window_end="2025-01-01T04:00:00+00:00", as_of_time="2025-01-01T04:00:00+00:00", five_f_b1_candidates=[{**valid_parent, "side": "sell"}])
    below = bind_five_f_parent(b1=b1, five={**base, "price_x1000": 90}, trigger_window_end="2025-01-01T04:00:00+00:00", as_of_time="2025-01-01T04:00:00+00:00", five_f_b1_candidates=[valid_parent])
    sell_b2 = bind_five_f_parent(b1=b1, five={**base, "side": "sell"}, trigger_window_end="2025-01-01T04:00:00+00:00", as_of_time="2025-01-01T04:00:00+00:00", five_f_b1_candidates=[valid_parent])
    wrong_identity = bind_five_f_parent(b1=b1, five={**base, "parent_30f_identity": "other-30f"}, trigger_window_end="2025-01-01T04:00:00+00:00", as_of_time="2025-01-01T04:00:00+00:00", five_f_b1_candidates=[valid_parent])
    assert missing["valid"] is False
    assert sell_parent["valid"] is False
    assert below["valid"] is False
    assert sell_b2["valid"] is False
    assert wrong_identity["reason"] == "direct_parent_identity_mismatch"


def test_derived_five_f_b1_parent_uses_nearest_eligible_pre_b2_signal():
    b1 = {"identity": "thirty", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T01:00:00+00:00"}
    five = {"identity": "five-b2", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T04:00:00+00:00", "price_x1000": 110}
    candidates = [{"identity": "early", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T02:00:00+00:00", "price_x1000": 100, "first_seen_run_id": 1}, {"identity": "nearest", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T03:00:00+00:00", "price_x1000": 105, "first_seen_run_id": 2}, {"identity": "future", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T05:00:00+00:00", "price_x1000": 90, "first_seen_run_id": 3}]
    result = bind_five_f_parent(b1=b1, five=five, trigger_window_end="2025-01-01T06:00:00+00:00", as_of_time="2025-01-01T06:00:00+00:00", five_f_b1_candidates=candidates)
    assert result["valid"] is True
    assert result["evidence_method"] == "derived_5f_b1"
    assert result["five_f_b1_identity"] == "nearest"


def test_derived_parent_rejects_broken_price_or_temporal_order():
    b1 = {"identity": "thirty", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T02:00:00+00:00"}
    five = {"identity": "five-b2", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T04:00:00+00:00", "price_x1000": 90}
    broken_price = [{"identity": "five-b1", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T03:00:00+00:00", "price_x1000": 100}]
    after_b2 = [{"identity": "future", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T05:00:00+00:00", "price_x1000": 80}]
    before_thirty = [{"identity": "past", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T01:00:00+00:00", "price_x1000": 80}]
    for candidates in (broken_price, after_b2, before_thirty):
        result = bind_five_f_parent(b1=b1, five=five, trigger_window_end="2025-01-01T06:00:00+00:00", as_of_time="2025-01-01T06:00:00+00:00", five_f_b1_candidates=candidates)
        assert result["valid"] is False
        assert result["reason"] == "parent_evidence_unavailable"


def test_derived_valid_parent_is_the_only_derived_path_that_counts_five_f_score():
    b1 = {"identity": "thirty", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T01:00:00+00:00"}
    five = {"identity": "five-b2", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T03:00:00+00:00", "price_x1000": 110}
    valid = bind_five_f_parent(b1=b1, five=five, trigger_window_end="2025-01-01T04:00:00+00:00", as_of_time="2025-01-01T04:00:00+00:00", five_f_b1_candidates=[{"identity": "five-b1", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T02:00:00+00:00", "price_x1000": 100}])
    rows = evaluate_policy_matrix(as_of_time="2025-01-01T04:00:00+00:00", trigger_window_end="2025-01-01T04:00:00+00:00", thirty_f_first_seen=b1["first_seen_time"], thirty_f_confirm_time=None, bottom_visible=True, five_f_first_seen=five["first_seen_time"], five_f_confirm_time=None, five_f_parent_valid=valid["valid"])
    assert rows[0]["confidence"] == 100


def test_sell_side_five_signals_never_form_official_parent_evidence():
    b1 = {"identity": "thirty", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T01:00:00+00:00"}
    sell_b2 = {"identity": "sell-b2", "symbol": "x", "mode": "predictive", "side": "sell", "bsp_type": "2", "first_seen_time": "2025-01-01T03:00:00+00:00", "price_x1000": 110}
    sell_b1 = {"identity": "sell-b1", "symbol": "x", "mode": "predictive", "side": "sell", "bsp_type": "1", "first_seen_time": "2025-01-01T02:00:00+00:00", "price_x1000": 100}
    selected, binding = select_valid_five_f_confirmation(b1=b1, five_candidates=[sell_b2], five_f_b1_candidates=[sell_b1], trigger_window_end="2025-01-01T04:00:00+00:00", as_of_time="2025-01-01T04:00:00+00:00")
    assert selected is None
    assert binding["valid"] is False


def test_later_valid_five_f_b2_is_selected_after_earlier_invalid_candidate():
    b1 = {"identity": "thirty", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T01:00:00+00:00"}
    five_b1 = {"identity": "five-b1", "symbol": "x", "mode": "predictive", "side": "buy", "bsp_type": "1", "first_seen_time": "2025-01-01T02:00:00+00:00", "price_x1000": 100}
    early_invalid = {"identity": "early", "symbol": "x", "mode": "predictive", "side": "buy", "bsp_type": "2", "first_seen_time": "2025-01-01T03:00:00+00:00", "price_x1000": 90}
    later_valid = {"identity": "later", "symbol": "x", "mode": "predictive", "side": "buy", "bsp_type": "2s", "first_seen_time": "2025-01-01T04:00:00+00:00", "price_x1000": 110}
    selected, binding = select_valid_five_f_confirmation(b1=b1, five_candidates=[later_valid, early_invalid], five_f_b1_candidates=[five_b1], trigger_window_end="2025-01-01T05:00:00+00:00", as_of_time="2025-01-01T05:00:00+00:00")
    assert selected["identity"] == "later"
    assert binding["valid"] is True


def test_all_five_f_b2_candidates_invalid_remains_unbound():
    b1 = {"identity": "thirty", "symbol": "x", "mode": "predictive", "side": "buy", "first_seen_time": "2025-01-01T01:00:00+00:00"}
    five_b1 = {"identity": "five-b1", "symbol": "x", "mode": "predictive", "side": "buy", "bsp_type": "1", "first_seen_time": "2025-01-01T02:00:00+00:00", "price_x1000": 100}
    invalid = {"identity": "bad", "symbol": "x", "mode": "predictive", "side": "buy", "bsp_type": "2", "first_seen_time": "2025-01-01T03:00:00+00:00", "price_x1000": 90}
    selected, binding = select_valid_five_f_confirmation(b1=b1, five_candidates=[invalid], five_f_b1_candidates=[five_b1], trigger_window_end="2025-01-01T04:00:00+00:00", as_of_time="2025-01-01T04:00:00+00:00")
    assert selected is None
    assert binding["reason"] == "parent_evidence_unavailable"


def test_policy_matrix_end_to_end_does_not_score_invalid_parent_confirmation():
    rows = evaluate_policy_matrix(as_of_time="2025-01-03T00:00:00+00:00", trigger_window_end="2025-01-03T00:00:00+00:00", thirty_f_first_seen="2025-01-02T00:00:00+00:00", thirty_f_confirm_time=None, bottom_visible=True, five_f_first_seen="2025-01-03T00:00:00+00:00", five_f_confirm_time=None, five_f_parent_valid=False)
    official = rows[0]
    assert official["confidence"] == 70
    assert official["five_f_counted"] is False
