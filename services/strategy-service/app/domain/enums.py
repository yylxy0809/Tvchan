from __future__ import annotations

from enum import StrEnum


class ChanLevel(StrEnum):
    F5 = "5f"
    F30 = "30f"
    D1 = "1d"
    W1 = "1w"
    M1 = "1m"


LEVEL_TO_DB = {
    ChanLevel.F5.value: 5,
    ChanLevel.F30.value: 30,
    ChanLevel.D1.value: 1440,
    ChanLevel.W1.value: 10080,
    ChanLevel.M1.value: 43200,
}

DB_TO_LEVEL = {value: key for key, value in LEVEL_TO_DB.items()}


class BacktestMode(StrEnum):
    EXPLORATORY_STATIC = "exploratory_static"
    EVENT_REPLAY = "event_replay"


class ScanStatus(StrEnum):
    CANDIDATE = "candidate"
    WATCH = "watch"
    TRIGGER = "trigger"
    NONE = "none"


class ExitReason(StrEnum):
    DAILY_B1_BROKEN = "DAILY_B1_BROKEN"
    F30_S1 = "30F_S1"
    DAILY_TOP_FRACTAL = "DAILY_TOP_FRACTAL"
    WEEKLY_TOP_FRACTAL = "WEEKLY_TOP_FRACTAL"


class MarketCapPolicy(StrEnum):
    REQUIRE = "require"
    WARN_ALLOW_MISSING = "warn_allow_missing"
    IGNORE = "ignore"
