from __future__ import annotations

from typing import Any

from app.engine.entry_state_machine_v4 import evaluate_entry_state_v4
from app.engine.time_utils import utc_time


def bind_five_f_parent(*, b1: dict[str, Any] | None, five: dict[str, Any] | None, trigger_window_end: str, as_of_time: str, five_f_b1_candidates: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    if not b1 or not five:
        return {"valid": False, "reason": "missing_b1_or_5f", "parent_identity": None, "evidence_method": "none"}
    limit = min(utc_time(trigger_window_end), utc_time(as_of_time))
    if b1.get("side") != "buy" or five.get("side") != "buy":
        return {"valid": False, "reason": "non_buy_signal_not_eligible", "parent_identity": b1.get("identity"), "evidence_method": "none"}
    if five.get("symbol") != b1.get("symbol") or five.get("mode") != b1.get("mode"):
        return {"valid": False, "reason": "symbol_or_mode_mismatch", "parent_identity": b1.get("identity"), "evidence_method": "none"}
    if not (utc_time(b1["first_seen_time"]) <= utc_time(five["first_seen_time"]) <= limit):
        return {"valid": False, "reason": "outside_parent_window", "parent_identity": b1.get("identity"), "evidence_method": "none"}
    direct_parent = five.get("parent_30f_identity")
    if direct_parent is not None and direct_parent != b1.get("identity"):
        return {"valid": False, "reason": "direct_parent_identity_mismatch", "parent_identity": b1.get("identity"), "evidence_method": "none", "five_f_b1_identity": None, "five_f_b1_run_id": None}
    candidates = [row for row in five_f_b1_candidates or [] if row.get("side") == "buy" and row.get("bsp_type", "1") == "1" and row.get("symbol") == b1.get("symbol") and row.get("mode") == b1.get("mode") and utc_time(b1["first_seen_time"]) < utc_time(row["first_seen_time"]) < utc_time(five["first_seen_time"]) <= limit and row.get("price_x1000") is not None and five.get("price_x1000") is not None and five["price_x1000"] >= row["price_x1000"]]
    if candidates:
        parent = max(candidates, key=lambda row: row["first_seen_time"])
        direct = direct_parent == b1.get("identity")
        return {"valid": True, "reason": "direct_30f_binding_plus_validated_5f_b1" if direct else "derived_5f_b1_price_structure", "parent_identity": b1.get("identity"), "evidence_method": "direct_30f_binding_plus_validated_5f_b1" if direct else "derived_5f_b1", "five_f_b1_identity": parent.get("identity"), "five_f_b1_run_id": parent.get("first_seen_run_id"), "five_f_b1_price_x1000": parent.get("price_x1000")}
    return {"valid": False, "reason": "parent_evidence_unavailable", "parent_identity": b1.get("identity"), "evidence_method": "none", "five_f_b1_identity": None, "five_f_b1_run_id": None}


def select_valid_five_f_confirmation(*, b1: dict[str, Any] | None, five_candidates: list[dict[str, Any]], five_f_b1_candidates: list[dict[str, Any]], trigger_window_end: str, as_of_time: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Select the first time-ordered buy B2/B2S that satisfies the full §11.2 parent contract."""
    last = {"valid": False, "reason": "no_buy_5f_b2_b2s_in_window", "parent_identity": b1 and b1.get("identity"), "evidence_method": "none"}
    for five in sorted((row for row in five_candidates if row.get("side") == "buy" and row.get("bsp_type") in {"2", "2s"}), key=lambda row: utc_time(row["first_seen_time"])):
        binding = bind_five_f_parent(b1=b1, five=five, trigger_window_end=trigger_window_end, as_of_time=as_of_time, five_f_b1_candidates=five_f_b1_candidates)
        if binding["valid"]:
            return five, binding
        last = binding
    return None, last


def evaluate_policy_matrix(*, as_of_time: str, trigger_window_end: str, thirty_f_first_seen: str | None, thirty_f_confirm_time: str | None, bottom_visible: bool, five_f_first_seen: str | None, five_f_confirm_time: str | None, five_f_parent_valid: bool = False, one_p_first_seen: str | None = None, trading_session_window_end: str | None = None, has_1p: bool = False) -> list[dict[str, Any]]:
    session_end = trading_session_window_end or trigger_window_end
    policies = [("official_calendar_window_first_seen", thirty_f_first_seen, five_f_first_seen, trigger_window_end), ("official_calendar_window_confirm_time", thirty_f_confirm_time, five_f_confirm_time, trigger_window_end), ("diagnostic_trading_session_window_first_seen", thirty_f_first_seen, five_f_first_seen, session_end), ("candidate_1p_research_only", one_p_first_seen if has_1p else None, five_f_first_seen, trigger_window_end)]
    rows = []
    for name, first_seen, five_seen, window_end in policies:
        state = evaluate_entry_state_v4(as_of_time=as_of_time, trigger_window_end=window_end, thirty_f_first_seen=first_seen, bottom_visible=bottom_visible, five_f_first_seen=five_seen, five_f_parent_valid=five_f_parent_valid, policy=name)
        rows.append({**state, "policy_contract_official": name == "official_calendar_window_first_seen", "research_only": name != "official_calendar_window_first_seen", "thirty_f_time_used": first_seen, "trigger_window_end_used": window_end})
    return rows
