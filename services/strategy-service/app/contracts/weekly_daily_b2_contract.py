from __future__ import annotations

import json
from pathlib import Path


_PATH = Path(__file__).with_name("weekly_daily_b2_contract_v1.json")


def load_contract() -> dict:
    return json.loads(_PATH.read_text(encoding="utf-8"))


def official_contract() -> dict:
    return load_contract()["official"]


def candidate_contract() -> dict:
    return load_contract()["candidate"]


def score(*, thirty_f: bool, bottom: bool, five_f: bool) -> int:
    weights = official_contract()["confidence"]
    return sum((weights["30F_B1"] if thirty_f else 0, weights["DAILY_BOTTOM_FRACTAL"] if bottom else 0, weights["5F_B2_CONFIRM_30F_B1"] if five_f else 0))


def can_trigger(*, score: int | float, fresh_thirty_f: bool) -> bool:
    entry = official_contract()["entry"]
    return bool(fresh_thirty_f and score >= entry["confidence_threshold"])
