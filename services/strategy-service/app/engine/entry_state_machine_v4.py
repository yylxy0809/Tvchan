from __future__ import annotations

from typing import Any

from app.contracts.weekly_daily_b2_contract import can_trigger, score
from app.engine.time_utils import utc_time


def _visible(value: str | None, as_of: str) -> bool:
    return bool(value and utc_time(value) <= utc_time(as_of))


def evaluate_entry_state_v4(*, as_of_time: str, thirty_f_first_seen: str | None = None, bottom_visible: bool = False, five_f_first_seen: str | None = None, five_f_visible: bool = False, five_f_parent_valid: bool = False, trigger_window_end: str | None = None, policy: str = "official") -> dict[str, Any]:
    fresh_thirty_f = _visible(thirty_f_first_seen, as_of_time) and (not trigger_window_end or utc_time(thirty_f_first_seen) <= utc_time(trigger_window_end))
    five_f_visible_now = five_f_visible or _visible(five_f_first_seen, as_of_time)
    five_f_counted = five_f_parent_valid and five_f_visible_now and fresh_thirty_f and (not five_f_first_seen or utc_time(five_f_first_seen) >= utc_time(thirty_f_first_seen))
    # The 5F score is valid only after it is bound to a visible fresh 30F B1.
    confidence = score(thirty_f=fresh_thirty_f, bottom=bottom_visible, five_f=five_f_counted)
    eligible = can_trigger(score=confidence, fresh_thirty_f=fresh_thirty_f)
    state = "ENTRY_ELIGIBLE" if eligible else ("BLOCKED_STALE" if thirty_f_first_seen and not fresh_thirty_f else "WAIT_FRESH_30F")
    return {"policy": policy, "state": state, "confidence": confidence, "fresh_thirty_f": fresh_thirty_f, "five_f_counted": five_f_counted, "entry_eligible": eligible, "entry_triggered": eligible, "future_leakage_detected": bool(thirty_f_first_seen and not _visible(thirty_f_first_seen, as_of_time))}
