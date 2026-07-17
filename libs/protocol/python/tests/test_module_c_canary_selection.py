from __future__ import annotations

import copy

import pytest

from trading_protocol.module_c_canary_selection import (
    ACTIVITY_BASIS,
    BARS_PER_COMPLETE_5F_SESSION,
    BOARD_ORDER,
    BOARD_QUOTAS,
    BOUNDARY_COUNTS,
    CONTRACT_VERSION,
    canonical_selection_sha256,
    classify_board,
    evaluate_selection_evidence,
    normalize_selection_source,
    selection_active_universe_sha256,
    selection_policy,
    validate_selection_manifest,
)


PROVENANCE = {
    "canonical_audit_run_id": "11111111-1111-1111-1111-111111111111",
    "audit_evidence_sha256": "1" * 64,
    "audit_checkpoint_sha256": "2" * 64,
    "freshness_contract_version": "module-c-authoritative-freshness-v1",
    "freshness_contract_sha256": "3" * 64,
    "catalog_generation_id": "22222222-2222-2222-2222-222222222222",
    "catalog_control_revision": 7,
    "catalog_manifest_sha256": "4" * 64,
    "audit_active_universe_sha256": "5" * 64,
}


def _manifest() -> dict:
    source = {
        "eligibility_build_id": "33333333-3333-3333-3333-333333333333",
        "eligibility_manifest_sha256": "6" * 64,
        **PROVENANCE,
    }
    symbols = []
    symbol_id = 1
    for board, prefix, exchange in (
        ("main_board", "600", "SH"),
        ("chinext", "300", "SZ"),
        ("star", "688", "SH"),
        ("bj", "920", "BJ"),
    ):
        for offset, boundary in enumerate(("lower", "lower", "middle", "upper", "upper")):
            five_minute_rows = 4_800 + offset
            daily_rows = 100
            symbols.append({
                "symbol_id": symbol_id,
                "symbol": f"{prefix}{offset:03d}.{exchange}",
                "board": board,
                "activity_boundary": boundary,
                "traits": [board, f"{boundary}_activity_boundary"],
                "eligible_timeframes": ["5f", "30f", "1d", "1w", "1m"],
                "evidence": {
                    "basis": ACTIVITY_BASIS,
                    "canonical_audit_run_id": PROVENANCE["canonical_audit_run_id"],
                    "five_minute_rows": five_minute_rows,
                    "daily_rows": daily_rows,
                    "activity_ratio_numerator": five_minute_rows,
                    "activity_ratio_denominator": daily_rows * 49,
                },
            })
            symbol_id += 1
    unsigned = {
        "contract_version": CONTRACT_VERSION,
        "source": source,
        "policy": selection_policy(),
        "symbols": symbols,
    }
    return {**unsigned, "selection_sha256": canonical_selection_sha256(unsigned)}


def _parameters(manifest: dict) -> dict:
    return {
        "scope": "canary",
        "source_build_id": manifest["source"]["eligibility_build_id"],
        "selection_contract_version": CONTRACT_VERSION,
        "selection_manifest_sha256": manifest["selection_sha256"],
        "selection_traits": sorted({
            trait for entry in manifest["symbols"] for trait in entry["traits"]
        }),
        "canary_selection": manifest,
    }


def test_contract_golden_values_and_board_mapping() -> None:
    assert BARS_PER_COMPLETE_5F_SESSION == 49
    assert BOARD_ORDER == ("main_board", "chinext", "star", "bj")
    assert BOARD_QUOTAS == {board: 5 for board in BOARD_ORDER}
    assert BOUNDARY_COUNTS == {"lower": 2, "middle": 1, "upper": 2}
    assert classify_board("600000.SH") == "main_board"
    assert classify_board("301001.SZ") == "chinext"
    assert classify_board("689001.SH") == "star"
    assert classify_board("920047.BJ") == "bj"
    assert classify_board("123456.SH") is None


def test_manifest_round_trip_hash_source_and_subset_universe() -> None:
    manifest = _manifest()
    assert validate_selection_manifest(manifest) == manifest
    assert normalize_selection_source(manifest["source"]) == manifest["source"]
    assert manifest["selection_sha256"] == canonical_selection_sha256({
        key: value for key, value in manifest.items() if key != "selection_sha256"
    })
    assert selection_active_universe_sha256(manifest) == "aeac40e34887d04d25760dc6a713bbeeeec1a1698365bd78a1f5c33dbfc68ea8"


@pytest.mark.parametrize("mutation", [
    lambda value: value["source"].update({"extra": True}),
    lambda value: value["policy"].update({"bars_per_complete_5f_session": 48}),
    lambda value: value["symbols"][0].update({"board": "bj"}),
    lambda value: value["symbols"][0]["evidence"].update({"activity_ratio_denominator": 4_800}),
    lambda value: value["symbols"].__setitem__(slice(0, 2), list(reversed(value["symbols"][:2]))),
])
def test_manifest_rejects_schema_policy_ratio_board_and_order_drift(mutation) -> None:
    manifest = _manifest()
    mutation(manifest)
    unsigned = {key: value for key, value in manifest.items() if key != "selection_sha256"}
    manifest["selection_sha256"] = canonical_selection_sha256(unsigned)
    with pytest.raises(ValueError):
        validate_selection_manifest(manifest)


def test_evaluator_pass_missing_baseline_and_never_raises() -> None:
    manifest = _manifest()
    parameters = _parameters(manifest)
    subset_sha = selection_active_universe_sha256(manifest)
    passed = evaluate_selection_evidence(parameters, PROVENANCE, subset_sha)
    assert set(passed) == {
        "status", "contract_version", "manifest_sha256", "source_build_id",
        "activity_basis", "board_counts", "boundary_counts", "contract_matches",
        "hash_matches", "source_matches", "quotas_match", "active_universe_matches",
        "drift_reasons",
    }
    assert passed["status"] == "pass"
    assert all(passed[field] is True for field in (
        "contract_matches", "hash_matches", "source_matches", "quotas_match",
        "active_universe_matches",
    ))

    missing = evaluate_selection_evidence({}, PROVENANCE, subset_sha)
    assert missing["status"] == "unavailable"
    assert all(missing[field] is None for field in (
        "contract_matches", "hash_matches", "source_matches", "quotas_match",
        "active_universe_matches",
    ))
    baseline = evaluate_selection_evidence({}, {}, None, applicable=False)
    assert baseline["status"] == "not_applicable"
    assert baseline["board_counts"] == baseline["boundary_counts"] == {}
    assert evaluate_selection_evidence(
        {"canary_selection": {"symbols": [{"board": []}]}}, [], [],
    )["status"] == "failed"
    assert evaluate_selection_evidence(
        {"canary_selection": {"symbols": [], "unserializable": object()}}, {}, None,
    )["status"] == "failed"
    assert evaluate_selection_evidence(
        {
            "canary_selection": {
                "symbols": [{"board": "main_board", "activity_boundary": []}]
            }
        },
        {},
        None,
    )["status"] == "failed"


@pytest.mark.parametrize(("mutation", "active_sha", "reason", "match_field"), [
    (lambda params: params.update({"selection_contract_version": "v1"}), None,
     "canary_selection_contract_drift", "contract_matches"),
    (lambda params: params.update({"selection_manifest_sha256": "f" * 64}), None,
     "canary_selection_hash_drift", "hash_matches"),
    (lambda params: params.update({"source_build_id": "44444444-4444-4444-4444-444444444444"}), None,
     "canary_selection_source_drift", "source_matches"),
    (lambda params: params["canary_selection"]["symbols"][0].update({"board": "bj"}), None,
     "canary_selection_quota_drift", "quotas_match"),
    (lambda params: None, "f" * 64,
     "canary_selection_active_universe_drift", "active_universe_matches"),
])
def test_evaluator_is_fail_visible_for_each_binding(mutation, active_sha, reason, match_field) -> None:
    manifest = _manifest()
    parameters = _parameters(copy.deepcopy(manifest))
    mutation(parameters)
    actual_sha = active_sha or selection_active_universe_sha256(manifest)
    evidence = evaluate_selection_evidence(parameters, PROVENANCE, actual_sha)
    assert evidence["status"] == "failed"
    assert evidence[match_field] is False
    assert reason in evidence["drift_reasons"]
