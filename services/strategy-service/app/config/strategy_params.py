from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.domain.enums import MarketCapPolicy


STRATEGY_CODE = "weekly_daily_b2_resonance_v1"
STRATEGY_VERSION = "v1"
SANITY_LOOSE_STRATEGY_CODE = "weekly_daily_b2_resonance_sanity_loose_v1"
SANITY_LOOSE_VERSION = "v1"
STRICT_EXPLICIT_B1_STRATEGY_CODE = "strict_explicit_b1"
DIAG_TRUST_B2_STRATEGY_CODE = "diag_trust_b2"
DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE = "diag_trust_b2_or_b2s"
DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE = "diag_same_bar_b1_b2s"
PHASE_1_4_EXPLICIT_B1_THEN_B2_STRATEGY_CODE = "phase_1_4_explicit_b1_then_b2"
PHASE_1_4_TRUST_CHAN_SIGNAL_STRATEGY_CODE = "phase_1_4_trust_chan_signal"
PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE = "phase_1_4_trust_chan_signal_with_b1_score"

DEFAULT_RULE_SPEC: dict[str, Any] = {
    "strategy_code": STRATEGY_CODE,
    "data_source": {
        "namespace": "c",
        "levels": ["5f", "30f", "1d", "1w", "1m"],
        "first_seen_time_source": "chan_c_runs_event_replay_approx",
        "daily_signal_source": "selected_run",
    },
    "universe": {
        "market": "A_SHARE",
        "is_active": True,
        "market_cap_min": 10_000_000_000,
        "market_cap_policy": MarketCapPolicy.WARN_ALLOW_MISSING.value,
    },
    "weekly": {
        "require_b2": True,
        "b2_types": ["2"],
        "context_mode": "explicit_prior_b1",
        "require_macd_dif_gt_zero": True,
        "failure_price_mode": "close_below_w_b1",
    },
    "daily": {
        "allow_b2": True,
        "allow_b2s": True,
        "b2_low_must_above_b1": True,
        "first_up_strength_threshold": 70.0,
        "daily_b2_confirm_mode": "first_seen",
        "allow_center_not_entered": False,
        "setup_mode": "strict_daily_b1_after_weekly_context",
        "daily_b1_lookback_trading_days": 60,
        "daily_b1_lookforward_trading_days": 20,
    },
    "strength_score": {
        "structure_weight": 40.0,
        "location_weight": 30.0,
        "momentum_weight": 30.0,
        "efficiency_ratio_break_1": 1.2,
        "efficiency_ratio_break_2": 1.0,
        "efficiency_ratio_break_3": 0.8,
    },
    "entry": {
        "confidence_threshold": 70.0,
        "require_30f_b1": True,
        "execution_price": "next_30f_open",
        "entry_priority": ["30F_B1"],
    },
    "confidence": {
        "30F_B1": 40.0,
        "DAILY_BOTTOM_FRACTAL": 30.0,
        "5F_B2_CONFIRM_30F_B1": 30.0,
    },
    "exit": {
        "failure": {
            "type": "daily_close_below_d_b1",
            "execution_price": "next_daily_open",
        },
        "profit": [
            "30F_S1",
            "DAILY_TOP_FRACTAL",
            "WEEKLY_TOP_FRACTAL",
        ],
    },
    "backtest": {
        "use_time": "first_seen_time",
        "allow_point_time_trade": False,
    },
}

SANITY_LOOSE_RULE_SPEC: dict[str, Any] = {
    **json.loads(json.dumps(DEFAULT_RULE_SPEC)),
    "strategy_code": SANITY_LOOSE_STRATEGY_CODE,
    "universe": {
        **DEFAULT_RULE_SPEC["universe"],
        "market_cap_policy": MarketCapPolicy.IGNORE.value,
    },
    "weekly": {
        **DEFAULT_RULE_SPEC["weekly"],
        "b2_types": ["2", "2s"],
        "context_mode": "trust_weekly_b2_or_b2s_signal",
        "require_macd_dif_gt_zero": False,
    },
    "daily": {
        **DEFAULT_RULE_SPEC["daily"],
        "first_up_strength_threshold": 40.0,
        "allow_center_not_entered": True,
    },
    "entry": {
        **DEFAULT_RULE_SPEC["entry"],
        "confidence_threshold": 40.0,
        "require_30f_b1": False,
        "entry_priority": ["30F_B1", "5F_B2"],
    },
}

STRICT_EXPLICIT_B1_RULE_SPEC: dict[str, Any] = {
    **json.loads(json.dumps(DEFAULT_RULE_SPEC)),
    "strategy_code": STRICT_EXPLICIT_B1_STRATEGY_CODE,
}

DIAG_TRUST_B2_RULE_SPEC: dict[str, Any] = {
    **json.loads(json.dumps(DEFAULT_RULE_SPEC)),
    "strategy_code": DIAG_TRUST_B2_STRATEGY_CODE,
    "weekly": {
        **DEFAULT_RULE_SPEC["weekly"],
        "b2_types": ["2"],
        "context_mode": "trust_weekly_b2_signal",
    },
}

DIAG_TRUST_B2_OR_B2S_RULE_SPEC: dict[str, Any] = {
    **json.loads(json.dumps(DEFAULT_RULE_SPEC)),
    "strategy_code": DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
    "weekly": {
        **DEFAULT_RULE_SPEC["weekly"],
        "b2_types": ["2", "2s"],
        "context_mode": "trust_weekly_b2_or_b2s_signal",
    },
}

DIAG_SAME_BAR_B1_B2S_RULE_SPEC: dict[str, Any] = {
    **json.loads(json.dumps(DEFAULT_RULE_SPEC)),
    "strategy_code": DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
    "weekly": {
        **DEFAULT_RULE_SPEC["weekly"],
        "b2_types": ["2s"],
        "context_mode": "same_bar_b1_b2s_as_candidate",
    },
}

PHASE_1_4_EXPLICIT_B1_THEN_B2_RULE_SPEC: dict[str, Any] = {
    **json.loads(json.dumps(DEFAULT_RULE_SPEC)),
    "strategy_code": PHASE_1_4_EXPLICIT_B1_THEN_B2_STRATEGY_CODE,
    "weekly": {
        **DEFAULT_RULE_SPEC["weekly"],
        "b2_types": ["2"],
        "context_mode": "explicit_b1_then_b2",
    },
}

PHASE_1_4_TRUST_CHAN_SIGNAL_RULE_SPEC: dict[str, Any] = {
    **json.loads(json.dumps(DEFAULT_RULE_SPEC)),
    "strategy_code": PHASE_1_4_TRUST_CHAN_SIGNAL_STRATEGY_CODE,
    "weekly": {
        **DEFAULT_RULE_SPEC["weekly"],
        "b2_types": ["2"],
        "context_mode": "trust_chan_signal",
    },
}

PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_RULE_SPEC: dict[str, Any] = {
    **json.loads(json.dumps(DEFAULT_RULE_SPEC)),
    "strategy_code": PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE,
    "weekly": {
        **DEFAULT_RULE_SPEC["weekly"],
        "b2_types": ["2", "2s"],
        "context_mode": "trust_chan_signal_with_b1_score",
    },
}


@dataclass(slots=True)
class StrategyParams:
    raw: dict[str, Any]

    @classmethod
    def default(cls) -> "StrategyParams":
        return cls.from_strategy_code(STRATEGY_CODE)

    @classmethod
    def from_strategy_code(cls, strategy_code: str) -> "StrategyParams":
        if strategy_code == STRICT_EXPLICIT_B1_STRATEGY_CODE:
            return cls(json.loads(json.dumps(STRICT_EXPLICIT_B1_RULE_SPEC)))
        if strategy_code == DIAG_TRUST_B2_STRATEGY_CODE:
            return cls(json.loads(json.dumps(DIAG_TRUST_B2_RULE_SPEC)))
        if strategy_code == DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE:
            return cls(json.loads(json.dumps(DIAG_TRUST_B2_OR_B2S_RULE_SPEC)))
        if strategy_code == DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE:
            return cls(json.loads(json.dumps(DIAG_SAME_BAR_B1_B2S_RULE_SPEC)))
        if strategy_code == PHASE_1_4_EXPLICIT_B1_THEN_B2_STRATEGY_CODE:
            return cls(json.loads(json.dumps(PHASE_1_4_EXPLICIT_B1_THEN_B2_RULE_SPEC)))
        if strategy_code == PHASE_1_4_TRUST_CHAN_SIGNAL_STRATEGY_CODE:
            return cls(json.loads(json.dumps(PHASE_1_4_TRUST_CHAN_SIGNAL_RULE_SPEC)))
        if strategy_code == PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_STRATEGY_CODE:
            return cls(json.loads(json.dumps(PHASE_1_4_TRUST_CHAN_SIGNAL_WITH_B1_SCORE_RULE_SPEC)))
        if strategy_code == SANITY_LOOSE_STRATEGY_CODE:
            return cls(json.loads(json.dumps(SANITY_LOOSE_RULE_SPEC)))
        return cls(json.loads(json.dumps(DEFAULT_RULE_SPEC)))

    def with_overrides(
        self,
        *,
        market_cap_policy: str | None = None,
        daily_setup_mode: str | None = None,
        daily_b1_lookback_trading_days: int | None = None,
        daily_b1_lookforward_trading_days: int | None = None,
        daily_signal_source: str | None = None,
    ) -> "StrategyParams":
        raw = json.loads(json.dumps(self.raw))
        if market_cap_policy is not None:
            raw["universe"]["market_cap_policy"] = market_cap_policy
        if daily_setup_mode is not None:
            raw["daily"]["setup_mode"] = daily_setup_mode
        if daily_b1_lookback_trading_days is not None:
            raw["daily"]["daily_b1_lookback_trading_days"] = int(daily_b1_lookback_trading_days)
        if daily_b1_lookforward_trading_days is not None:
            raw["daily"]["daily_b1_lookforward_trading_days"] = int(daily_b1_lookforward_trading_days)
        if daily_signal_source is not None:
            raw["data_source"]["daily_signal_source"] = str(daily_signal_source)
        return StrategyParams(raw)

    @property
    def strategy_code(self) -> str:
        return str(self.raw.get("strategy_code") or STRATEGY_CODE)

    @property
    def strategy_version(self) -> str:
        if self.strategy_code in {
            SANITY_LOOSE_STRATEGY_CODE,
            STRICT_EXPLICIT_B1_STRATEGY_CODE,
            DIAG_TRUST_B2_STRATEGY_CODE,
            DIAG_TRUST_B2_OR_B2S_STRATEGY_CODE,
            DIAG_SAME_BAR_B1_B2S_STRATEGY_CODE,
        }:
            return SANITY_LOOSE_VERSION
        return STRATEGY_VERSION

    @property
    def market_cap_min(self) -> int:
        return int(self.raw["universe"].get("market_cap_min", 0))

    @property
    def market_cap_policy(self) -> MarketCapPolicy:
        return MarketCapPolicy(self.raw["universe"].get("market_cap_policy", MarketCapPolicy.WARN_ALLOW_MISSING.value))

    @property
    def allow_missing_market_cap(self) -> bool:
        return self.market_cap_policy != MarketCapPolicy.REQUIRE

    @property
    def require_weekly_macd_dif_gt_zero(self) -> bool:
        return bool(self.raw["weekly"].get("require_macd_dif_gt_zero", True))

    @property
    def weekly_b2_types(self) -> list[str]:
        value = self.raw["weekly"].get("b2_types", ["2"])
        return [str(item) for item in value]

    @property
    def weekly_context_mode(self) -> str:
        return str(self.raw["weekly"].get("context_mode", "explicit_prior_b1"))

    @property
    def weekly_context_mode_normalized(self) -> str:
        mode = self.weekly_context_mode
        if mode == "explicit_prior_b1":
            return "explicit_b1_then_b2"
        if mode in {"trust_weekly_b2_signal", "trust_weekly_b2_or_b2s_signal"}:
            return "trust_chan_signal"
        return mode

    @property
    def strength_threshold(self) -> float:
        return float(self.raw["daily"].get("first_up_strength_threshold", 70.0))

    @property
    def daily_b2_confirm_mode(self) -> str:
        return str(self.raw["daily"].get("daily_b2_confirm_mode", "first_seen"))

    @property
    def allow_center_not_entered(self) -> bool:
        return bool(self.raw["daily"].get("allow_center_not_entered", False))

    @property
    def daily_setup_mode(self) -> str:
        return str(self.raw["daily"].get("setup_mode", "strict_daily_b1_after_weekly_context"))

    @property
    def daily_b1_lookback_trading_days(self) -> int:
        return int(self.raw["daily"].get("daily_b1_lookback_trading_days", 60))

    @property
    def daily_b1_lookforward_trading_days(self) -> int:
        return int(self.raw["daily"].get("daily_b1_lookforward_trading_days", 20))

    @property
    def is_official_daily_setup_mode(self) -> bool:
        return self.daily_setup_mode == "strict_daily_b1_after_weekly_context"

    @property
    def is_diagnostic_only_daily_setup_mode(self) -> bool:
        return self.daily_setup_mode in {
            "daily_b1_near_weekly_context",
            "trust_daily_b2_or_b2s_signal",
            "true_trust_daily_b2_or_b2s",
            "daily_b2_or_b2s_with_b1_score",
            "daily_buy_signal_any_observation",
        }

    @property
    def require_30f_b1(self) -> bool:
        return bool(self.raw["entry"].get("require_30f_b1", True))

    @property
    def entry_confidence_threshold(self) -> float:
        return float(self.raw["entry"].get("confidence_threshold", 70.0))

    @property
    def entry_priority(self) -> list[str]:
        value = self.raw["entry"].get("entry_priority", ["30F_B1"])
        return [str(item) for item in value]

    @property
    def first_seen_time_source(self) -> str:
        return str(self.raw["data_source"].get("first_seen_time_source", "chan_c_runs_event_replay_approx"))

    @property
    def daily_signal_source(self) -> str:
        return str(self.raw["data_source"].get("daily_signal_source", "selected_run"))

    def confidence_weight(self, key: str) -> float:
        return float(self.raw["confidence"].get(key, 0.0))
