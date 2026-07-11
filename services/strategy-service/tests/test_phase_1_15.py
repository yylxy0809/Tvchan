from app.engine.phase_1_15 import (
    _render_gate_waterfall_md,
    build_thirty_f_price_deep_dive,
    build_sample_lineage_audit,
    classify_bottom_fractal_equivalence,
    classify_five_f_root_cause,
    recommend_thirty_f_price_policy,
)


def test_sample_lineage_audit_reports_two_symbol_limit_without_breaking_lineage():
    phase_1_12_rows = [
        {"sample_id": "000001.SZ|2025-09-18T07:00:00+00:00", "symbol": "000001.SZ"},
        {"sample_id": "000651.SZ|2025-09-18T07:00:00+00:00", "symbol": "000651.SZ"},
    ]
    phase_1_13_rows = [
        {"sample_id": "000001.SZ|2025-09-18T07:00:00+00:00", "symbol": "000001.SZ"},
    ]
    phase_1_14_rows = [
        {"sample_id": "000001.SZ|2025-09-18T07:00:00+00:00", "symbol": "000001.SZ"},
    ]

    payload = build_sample_lineage_audit(
        phase_1_12_candidate_rows=phase_1_12_rows,
        phase_1_13_candidate_rows=phase_1_13_rows,
        phase_1_14_candidate_rows=phase_1_14_rows,
        phase_1_13_thirty_f_rows=phase_1_13_rows,
        requested_symbols=["000001.SZ", "000651.SZ"],
    )

    assert payload["lineage_consistent"] is True
    assert payload["phase_1_14_actual_symbol_count"] == 1
    assert payload["candidate_symbols"] == {"000001.SZ": 1, "000651.SZ": 1}
    assert payload["entry_confidence_symbols"] == {"000001.SZ": 1}
    assert payload["phase_1_14_two_symbol_limitation_affects_generalization"] is True


def test_sample_lineage_audit_marks_mismatch_when_downstream_has_unknown_sample():
    phase_1_12_rows = [{"sample_id": "000001.SZ|2025-09-18T07:00:00+00:00", "symbol": "000001.SZ"}]
    phase_1_13_rows = [{"sample_id": "000651.SZ|2025-09-18T07:00:00+00:00", "symbol": "000651.SZ"}]

    payload = build_sample_lineage_audit(
        phase_1_12_candidate_rows=phase_1_12_rows,
        phase_1_13_candidate_rows=phase_1_13_rows,
        phase_1_14_candidate_rows=[],
        phase_1_13_thirty_f_rows=[],
        requested_symbols=["000001.SZ", "000651.SZ"],
    )

    assert payload["lineage_consistent"] is False
    assert payload["sample_lineage_mismatch_count"] == 1


def test_recommend_thirty_f_price_policy_keeps_strict_when_window_not_valid():
    payload = recommend_thirty_f_price_policy(
        {
            "window_valid": False,
            "strict_price_valid": False,
            "signal_price_only_valid": True,
            "bar_low_high_overlap_valid": True,
            "no_break_daily_b1_valid": True,
        }
    )

    assert payload["recommended_policy"] == "strict_existing"
    assert payload["decision"] == "keep_strict"


def test_recommend_thirty_f_price_policy_promotes_candidate_variant_only_for_window_valid_nine_case():
    payload = recommend_thirty_f_price_policy(
        {
            "window_valid": True,
            "strict_price_valid": False,
            "strict_invalid_reason": "thirty_f_signal_already_invalidated",
            "signal_price_only_valid": True,
            "bar_low_high_overlap_valid": True,
            "no_break_daily_b1_valid": True,
        }
    )

    assert payload["recommended_policy"] == "signal_price_only"
    assert payload["decision"] == "promote_candidate_variant"


def test_classify_bottom_fractal_equivalence_requires_more_than_raw_presence_for_proven():
    payload = classify_bottom_fractal_equivalence(
        {
            "sample_count": 72,
            "bottom_fractal_confirmed_count": 18,
            "point_time_matches_daily_signal_count": 18,
            "stroke_turn_match_count": 18,
            "module_c_direct_fractal_match_count": 0,
            "future_leakage_detected": False,
        }
    )

    assert payload["module_c_fractal_equivalence"] == "partially_supported"
    assert payload["recommend_bottom_fractal_ledger_as_candidate_confirmation"] is True


def test_classify_five_f_root_cause_identifies_snapshot_sparsity():
    category = classify_five_f_root_cause(
        {
            "has_5f_run_covering_window": True,
            "latest_5f_head_bar_until_before_as_of": True,
            "run_group_id": "research_daily_close",
            "visible_5f_buy_count": 0,
            "visible_5f_b2_count": 0,
            "future_5f_b2_count": 0,
            "selected_run_filtered": False,
            "has_5f_structure_turn": False,
        }
    )

    assert category == "research_daily_close_snapshot_too_sparse"


def test_thirty_f_price_deep_dive_preserves_window_valid_for_policy_recommendation():
    payload = build_thirty_f_price_deep_dive(
        daily_rows=[
            {
                "symbol": "000001.SZ",
                "name": "平安银行",
                "as_of_time": "2025-09-18T07:00:00+00:00",
                "candidate_audit": {"selected_buy_signal_any": {"price": 11.38}},
            }
        ],
        price_rows=[
            {
                "sample_id": "000001.SZ|2025-09-18T07:00:00+00:00",
                "symbol": "000001.SZ",
                "as_of_time": "2025-09-18T07:00:00+00:00",
                "window_valid": True,
                "price_valid": False,
                "price_invalid_reason": "thirty_f_signal_already_invalidated",
            }
        ],
        price_policy_rows=[
            {
                "sample_id": "000001.SZ|2025-09-18T07:00:00+00:00",
                "symbol": "000001.SZ",
                "as_of_time": "2025-09-18T07:00:00+00:00",
                "window_valid": True,
                "thirty_f_price_policy_signal_price_only": True,
                "thirty_f_price_policy_bar_low_high_overlap": True,
                "thirty_f_price_policy_no_break_daily_b1": True,
            }
        ],
        bottom_rows=[],
    )

    row = payload["rows"][0]
    assert row["window_valid"] is True
    assert row["recommended_policy"] == "signal_price_only"
    assert row["decision"] == "promote_candidate_variant"


def test_render_gate_waterfall_reads_phase_1_14_block_reason_counts_contract():
    markdown = _render_gate_waterfall_md(
        {
            "targeted_payload": {
                "summary": {
                    "block_reason_counts": {
                        "bottom_fractal_not_first_seen_yet": 3,
                        "no_5f_confirmation": 5,
                    }
                }
            }
        }
    )

    assert "bottom_fractal_not_first_seen_yet" in markdown
    assert "no_5f_confirmation" in markdown
