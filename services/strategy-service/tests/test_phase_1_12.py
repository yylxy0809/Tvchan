from __future__ import annotations

from app.engine.phase_1_12 import build_policy_decision, build_replay_compare


def test_build_policy_decision_prefers_candidate_mode_when_candidate_exists() -> None:
    daily_dataset = {
        "summary": {
            "strict_daily_setup_count": 0,
            "candidate_daily_setup_count": 171,
            "observation_daily_setup_count": 252,
            "observation_non_b2_b2s_ratio": 0.3214,
        }
    }
    thirty_f_audit = {
        "summary": {
            "visible_30f_b1_samples": 18,
            "recommend_30f_event_ledger_design_next": True,
        }
    }
    entry_diagnosis = {"summary": {"entry_trigger_count": 0}}

    decision = build_policy_decision(
        daily_dataset=daily_dataset,
        thirty_f_audit=thirty_f_audit,
        entry_diagnosis=entry_diagnosis,
    )

    assert decision["decision"] == "Decision B"
    assert decision["keep_strict_daily_b1_as_official_baseline"] is True
    assert decision["recommend_candidate_daily_b2_b2s_setup"] is True
    assert decision["recommend_strategy_30f_smoke_next"] is False
    assert decision["recommend_30f_event_ledger_design_next"] is True


def test_build_replay_compare_maps_phase_1_11_reference_and_counts() -> None:
    phase_1_11_compare_rows = [
        {
            "daily_signal_source": "event_ledger",
            "daily_setup_mode": "true_trust_daily_b2_or_b2s",
            "daily_setup_count": 171,
            "entry_watch_count": 171,
        }
    ]
    daily_dataset = {
        "rows": [
            {
                "symbol": "000001.SZ",
                "as_of_time": "2025-08-15T07:00:00+00:00",
                "strict_accept": False,
                "candidate_b2_b2s_accept": True,
                "observation_accept": True,
                "scored_audit": {"daily_setup_accepted_by_mode": True},
            }
        ]
    }
    thirty_f_audit = {
        "rows": [
            {
                "symbol": "000001.SZ",
                "as_of_time": "2025-08-15T07:00:00+00:00",
                "visible_30f_b1_or_1p_count": 1,
            }
        ]
    }
    entry_diagnosis = {
        "rows": [
            {
                "symbol": "000001.SZ",
                "as_of_time": "2025-08-15T07:00:00+00:00",
                "daily_bottom_fractal_found": True,
                "five_f_b2_confirm_found": False,
                "confidence_score": 70.0,
                "entry_triggered": True,
            }
        ]
    }

    payload = build_replay_compare(
        phase_1_11_compare_rows=phase_1_11_compare_rows,
        daily_dataset=daily_dataset,
        thirty_f_audit=thirty_f_audit,
        entry_diagnosis=entry_diagnosis,
    )

    candidate_row = next(row for row in payload["rows"] if row["daily_setup_mode"] == "event_ledger_daily_b2_or_b2s_setup_v1")
    assert candidate_row["daily_setup_count"] == 1
    assert candidate_row["thirty_f_b1_count"] == 1
    assert candidate_row["confidence_70_count"] == 1
    assert candidate_row["entry_trigger_count"] == 1
    assert candidate_row["phase_1_11_reference"]["daily_setup_count"] == 171
