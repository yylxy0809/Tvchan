from pathlib import Path

from app.engine.phase_1_16 import (
    build_candidate_samples_master,
    build_entry_trigger_v5_audit,
    build_entry_trigger_v5_compare,
    build_thirty_f_window_price_policy_v2_audit,
    load_phase_1_16_artifacts,
)


def _phase_1_15_output_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "outputs" / "phase-1-15-entry-chain-microdiagnostic"


def test_phase_1_16_candidate_samples_master_reconciles_expected_counts_from_existing_outputs():
    artifacts = load_phase_1_16_artifacts(phase_1_15_output_dir=_phase_1_15_output_dir())
    payload = build_candidate_samples_master(artifacts)

    assert payload["summary"]["candidate_samples_master_count"] == 171
    assert payload["summary"]["visible_30f_b1_or_1p_count"] == 72
    assert payload["summary"]["window_valid_count"] == 9
    assert payload["summary"]["v4_confidence_70_count"] == 11


def test_phase_1_16_window_price_policy_v2_audit_keeps_official_policy_and_recommends_candidate_variant():
    artifacts = load_phase_1_16_artifacts(phase_1_15_output_dir=_phase_1_15_output_dir())
    payload = build_thirty_f_window_price_policy_v2_audit(artifacts)

    assert payload["summary"]["candidate_samples_audited"] == 171
    assert payload["summary"]["window_valid_price_invalid_count"] == 9
    assert payload["decision"]["official_policy"] == "strict_existing"
    assert payload["decision"]["candidate_policy"] == "signal_price_only"


def test_phase_1_16_entry_trigger_v5_audit_covers_all_confidence_70_rows_without_future_leakage():
    artifacts = load_phase_1_16_artifacts(phase_1_15_output_dir=_phase_1_15_output_dir())
    payload = build_entry_trigger_v5_audit(artifacts)

    assert payload["summary"]["v4_confidence_70_input_count"] == 11
    assert payload["summary"]["v5_audited_count"] == 11
    assert payload["summary"]["future_leakage_detected"] is False
    assert payload["summary"]["final_block_reason_counts"]["trigger_window_expired"] == 11


def test_phase_1_16_compare_preserves_zero_entry_trigger_result_for_all_candidate_policies():
    artifacts = load_phase_1_16_artifacts(phase_1_15_output_dir=_phase_1_15_output_dir())
    payload = build_entry_trigger_v5_compare(artifacts)

    assert payload["summary"]["all_entry_trigger_count_zero"] is True
    assert payload["summary"]["scenario_count"] == 5
    assert all(row["entry_trigger_count"] == 0 for row in payload["rows"])
