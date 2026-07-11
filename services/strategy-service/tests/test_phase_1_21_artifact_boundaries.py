import json
from pathlib import Path

from app.engine.phase_1_21 import DEFAULT_OUTPUT_DIR, _partition_observable_symbols


def _json(path: str):
    return json.loads((DEFAULT_OUTPUT_DIR / path).read_text(encoding="utf-8"))


def _jsonl(path: str):
    return [json.loads(line) for line in (DEFAULT_OUTPUT_DIR / path).read_text(encoding="utf-8").splitlines() if line]


def test_current_official_and_diagnostic_artifacts_are_physically_disjoint():
    universe = _json("observable_research_universe.json")
    official = _json("official_gate_waterfall.json")
    diagnostic = _json("diagnostic_gate_waterfall.json")
    assert universe["official_eligible_count"] == 0
    assert universe["diagnostic_only_count"] == 13
    assert official["rows"] == []
    assert not ({row["symbol"] for row in official["rows"]} & {row["symbol"] for row in diagnostic["rows"]})
    assert _jsonl("official_observable_symbols.jsonl") == []
    diagnostic_symbols = _jsonl("diagnostic_observable_symbols.jsonl")
    assert len(diagnostic_symbols) == 13
    assert {row["symbol"] for row in diagnostic_symbols}.isdisjoint({row["symbol"] for row in _jsonl("official_observable_symbols.jsonl")})


def test_diagnostic_manifest_is_non_executable_and_decision_is_not_official_backfill():
    decision = _json("next_phase_decision.json")
    manifest = (DEFAULT_OUTPUT_DIR / "micro_backfill_v4_manifest.csv").read_text(encoding="utf-8")
    assert decision["next_phase_decision"] == "E_SAMPLE_UNIVERSE_TOO_SMALL"
    assert decision["execute_backfill"] is False
    assert "execute" in manifest and "universe_class" in manifest and "diagnostic_only" in manifest


def test_policy_rows_and_counters_keep_contract_label_separate_from_universe_result():
    semantics = _json("trigger_window_semantics_v2.json")
    official_rows = _jsonl("official_policy_rows.jsonl")
    diagnostic_rows = _jsonl("diagnostic_policy_rows.jsonl")
    decision = _json("next_phase_decision.json")
    matrix = _json("policy_counterfactual_matrix.json")
    assert official_rows == []
    assert diagnostic_rows and {row["universe_class"] for row in diagnostic_rows} == {"diagnostic_only"}
    assert "official" not in diagnostic_rows[0]
    assert sum(row["policy_contract_official"] for row in semantics["rows"]) == 16
    assert decision["official_trigger_count"] == 0
    assert decision["official_candidate_trigger_count"] == 0
    assert decision["diagnostic_candidate_trigger_count"] == 1
    assert matrix["diagnostic_candidate_trigger_count"] == decision["diagnostic_candidate_trigger_count"]


def test_decision_inputs_explicitly_separate_official_and_observable_counts():
    decision = _json("next_phase_decision.json")
    inputs = decision["decision_inputs"]
    assert decision["official_eligible_symbol_count"] == inputs["official_eligible_symbol_count"] == 0
    assert decision["observable_symbol_count"] == inputs["observable_symbol_count"] == 13
    assert decision["diagnostic_symbol_count"] == inputs["diagnostic_symbol_count"] == 13
    assert "official_eligible_symbol_count" in decision["decision_rationale"]


def test_observable_universe_classes_never_overlap_or_include_unobservable_fallbacks():
    all_official = {"observable_symbols": ["a"], "market_cap_status": {"a": "available"}}
    assert _partition_observable_symbols(all_official) == (["a"], [])
    empty = {"observable_symbols": [], "market_cap_status": {}}
    assert _partition_observable_symbols(empty) == ([], [])


def test_coverage_trace_matches_only_its_expected_cutoffs_and_duplicate_provenance():
    trace = (DEFAULT_OUTPUT_DIR / "traces" / "coverage_complete.md").read_text(encoding="utf-8")
    values = {}
    for line in trace.splitlines():
        if line.startswith("- ") and ": `" in line:
            key, value = line[2:].split(": `", 1)
            values[key] = json.loads(value[:-1])
    mapping = values["cutoff_to_actual_runs"]
    assert values["covered_cutoff_count"] == values["actual_cutoff_count"] == len(mapping)
    assert set(mapping).issubset(set(values["expected_cutoffs"]))
    assert all(rows and all("run_id" in row and "run_group_id" in row for row in rows) for rows in mapping.values())
    assert values["universe_class"] == "diagnostic_only"


def test_official_and_diagnostic_nearest_trigger_traces_are_physically_separated():
    official = (DEFAULT_OUTPUT_DIR / "traces" / "closest_official_trigger.md").read_text(encoding="utf-8")
    diagnostic = (DEFAULT_OUTPUT_DIR / "traces" / "closest_diagnostic_trigger.md").read_text(encoding="utf-8")
    assert "available: `false`" in official and "no_official_candidate" in official
    assert "trace_type: `\"diagnostic_nearest_trigger\"`" in diagnostic
    assert "universe_class: `\"diagnostic_only\"`" in diagnostic


def test_coverage_trace_is_available_with_coverage_specific_evidence():
    trace = (DEFAULT_OUTPUT_DIR / "traces" / "coverage_complete.md").read_text(encoding="utf-8")
    assert "available: `true`" in trace
    assert "expected_cutoff_count" in trace and "run_ids" in trace


def test_coverage_gap_trace_is_available_and_lists_missing_cutoffs():
    trace = (DEFAULT_OUTPUT_DIR / "traces" / "coverage_gap.md").read_text(encoding="utf-8")
    checklist = (DEFAULT_OUTPUT_DIR / "phase_1_21_task_checklist_report.md").read_text(encoding="utf-8")
    assert "available: `true`" in trace
    assert "trace_type: `\"coverage_gap\"`" in trace
    assert "missing_cutoffs" in trace and "coverage_classification" in trace
    assert '"coverage_gap": true' in checklist


def test_available_non_b1_traces_are_not_rejected_for_nullable_lifecycle_fields():
    for name in ("official_blocked_candidate", "weekly_daily_gate_failure", "closest_diagnostic_trigger"):
        trace = (DEFAULT_OUTPUT_DIR / "traces" / f"{name}.md").read_text(encoding="utf-8")
        assert "available: `true`" in trace
        assert "kline_grid" in trace and "gate_reason" in trace
        assert "kline_grid: `[]`" not in trace


def test_checklist_marks_required_missing_b1_samples_as_partial_not_complete():
    checklist = (DEFAULT_OUTPUT_DIR / "phase_1_21_task_checklist_report.md").read_text(encoding="utf-8")
    assert 'trace_task_status: `"partial_required_samples_unavailable"`' in checklist
    assert "b1_disappeared" in checklist and "b1_confirmed" in checklist
