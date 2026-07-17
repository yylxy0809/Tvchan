from __future__ import annotations

import hashlib
import json
from typing import Any


STRATEGY_CODE = "weekly_daily_b2_resonance_v1"
SOURCE_CONTRACT = "sealed_historical_replay_event_ledger_v1"


def _sha256_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _valid_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _valid_bound_scope(scope: Any, scope_hash: Any) -> bool:
    if not isinstance(scope, dict) or scope.get("scope_hash") != scope_hash:
        return False
    required_text = (
        "run_group_id",
        "config_hash",
        "eligible_universe_snapshot_id",
        "canonical_gate_snapshot_id",
        "contract_cutoff",
    )
    if any(not isinstance(scope.get(field), str) or not scope[field] for field in required_text):
        return False
    if (
        not isinstance(scope.get("replay_batch_id"), int)
        or scope["replay_batch_id"] < 1
        or not isinstance(scope.get("source_batch_id"), int)
        or scope["source_batch_id"] < 1
        or scope.get("contract_version") != "historical-replay-v1"
        or not _valid_sha256(scope.get("contract_hash"))
        or scope.get("publication_namespace") != "historical-replay"
        or scope.get("profile_id") != "module-c-historical-replay-v1"
    ):
        return False
    payload = {key: value for key, value in scope.items() if key != "scope_hash"}
    return _sha256_payload(payload) == scope_hash


def build_official_historical_gate(snapshot: dict[str, Any]) -> dict[str, Any]:
    counts = snapshot["counts"]
    stages = [
        {"gate": "source_high_level_eligible", "count": int(counts["source_high_level_eligible"])},
        {"gate": "official_high_level_visible", "count": int(counts["official_high_level_visible"])},
        {"gate": "intraday_5f_30f_eligible", "count": int(counts["intraday_eligible"])},
        {"gate": "predictive_weekly_b1_visible", "count": int(counts["predictive_weekly_b1"])},
        {"gate": "predictive_weekly_b2_visible", "count": int(counts["predictive_weekly_b2"])},
        {"gate": "strict_daily_setup_episode", "count": int(counts["strict_daily_episodes"])},
        {"gate": "official_30f_confirmation", "count": int(counts["official_30f_confirmations"])},
        {"gate": "official_5f_parent_bound_confirmation", "count": int(counts["official_5f_confirmations"])},
        {"gate": "official_event_replay_candidate", "count": int(counts["official_candidates"])},
    ]
    monotonic = all(current["count"] <= previous["count"] for previous, current in zip(stages, stages[1:]))
    blockers = []
    scope_hash = snapshot.get("scope_hash")
    content_hash = snapshot.get("official_jsonl_sha256")
    scope = snapshot.get("scope")
    if (
        not _valid_sha256(scope_hash)
        or not _valid_sha256(content_hash)
        or not _valid_bound_scope(scope, scope_hash)
    ):
        blockers.append("unbound_historical_lifecycle_input")
    # The current waterfall is an upstream visibility audit only. It does not
    # replay disappeared/reappeared state or produce parent-bound 30f/5f
    # confirmation traces, so it must never authorize an official backtest.
    blockers.append("official_event_replay_not_implemented")
    if not monotonic:
        blockers.append("non_monotonic_gate_waterfall")
    if not counts["predictive_weekly_b2"]:
        blockers.append("official_predictive_weekly_b2_unavailable")
    if not counts["strict_daily_episodes"]:
        blockers.append("strict_daily_setup_episode_unavailable")
    if int(counts["official_candidates"]) < 3:
        blockers.append("insufficient_complete_official_traces")
    decision = "GO" if not blockers else "NO_GO"
    digest_input = {
        "as_of_time": snapshot["as_of_time"],
        "scope_hash": scope_hash,
        "official_jsonl_sha256": content_hash,
        "official_events_by_level": snapshot.get("official_events_by_level", []),
        "strategy_code": STRATEGY_CODE,
        "source_contract": SOURCE_CONTRACT,
        "stages": stages,
        "blockers": blockers,
    }
    return {
        **snapshot,
        "strategy_code": STRATEGY_CODE,
        "source_contract": SOURCE_CONTRACT,
        "gate_stages": stages,
        "gate_counts_monotonic": monotonic,
        "blockers": blockers,
        "decision": decision,
        "input_hash": hashlib.sha256(
            json.dumps(digest_input, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
