from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app.analyzers.center_query_service import fallback_segment_overlap, find_last_relevant_daily_center
from app.analyzers.exit_signal_evaluator import ExitSignalEvaluator
from app.analyzers.fractal_detector import latest_bottom_fractal_time
from app.analyzers.strength_evaluator import evaluate_daily_first_up_strength
from app.domain.enums import MarketCapPolicy, ScanStatus
from app.domain.models import (
    DailySetup,
    EntryEvaluation,
    GateOutcome,
    ScanDiagnosis,
    ScanResult,
    SymbolInfo,
    Trade,
    WeeklyContext,
)
from app.repositories.kline_repo import KlineRepository, compute_macd
from app.repositories.module_c_repo import ModuleCRepository


@dataclass(slots=True)
class _WeeklyContextSelection:
    weekly_signal: object
    weekly_b1: object | None
    context_score: float
    anchor_time: datetime
    anchor_source: str
    stop_reference_price: float
    stop_reference_source: str
    prior_weekly_b1_found: bool
    same_bar_with_b1: bool
    same_price_with_b1: bool
    bypass_prior_b1_gate: bool
    bypass_price_break_gate: bool


@dataclass(slots=True)
class _DailySetupSelection:
    daily_b1: object
    daily_b2: object | None
    daily_b2s: object | None
    context_start_time: datetime
    setup_mode: str
    signal_source: str
    relation_to_weekly_signal: str
    distance_trading_days: int | None
    score: float
    is_official_strategy: bool


@dataclass(slots=True)
class _DailySetupSemanticsAudit:
    mode: str
    context_start_time: datetime
    window_start_time: datetime
    window_end_time: datetime
    daily_signal_any_found: bool
    daily_b1_found: bool
    daily_b2_or_b2s_found: bool
    daily_prior_b1_for_b2_found: bool
    daily_b2_or_b2s_self_contained_accepted: bool
    daily_setup_accepted_by_mode: bool
    selected_daily_b1: object | None
    selected_daily_b2_or_b2s: object | None
    selected_buy_signal_any: object | None
    selected_signal_source: str | None
    selected_signal_kind: str | None
    selected_signal_score: float


class StrategyDiagnoser:
    def __init__(self, module_c_repo: ModuleCRepository, kline_repo: KlineRepository) -> None:
        self.module_c_repo = module_c_repo
        self.kline_repo = kline_repo
        self.exit_evaluator = ExitSignalEvaluator(module_c_repo, kline_repo)

    async def diagnose_symbol(
        self,
        symbol: SymbolInfo,
        *,
        as_of_time: datetime,
        params,
    ) -> ScanDiagnosis:
        gates: list[GateOutcome] = []
        market_cap = await self.kline_repo.get_latest_market_cap(symbol.symbol_id)
        levels = ("5f", "30f", "1d", "1w", "1m")
        historical_lookups = {
            level: await self.module_c_repo.get_historical_run_lookup(
                symbol.symbol_id,
                level,
                "predictive",
                as_of_time,
            )
            for level in levels
        }
        heads = {level: lookup.selected for level, lookup in historical_lookups.items()}
        weekly_signals = []
        daily_signals = []
        diagnosis = ScanDiagnosis(
            symbol=symbol,
            as_of_time=as_of_time,
            strategy_code=params.strategy_code,
            market_cap=market_cap,
            heads=heads,
            weekly_signals=weekly_signals,
            daily_signals=daily_signals,
            gates=gates,
        )

        self._add_gate(gates, "active_symbol", True, "symbol selected from active universe")

        market_cap_ok = True
        market_reason = None
        if params.market_cap_policy == MarketCapPolicy.REQUIRE:
            market_cap_ok = market_cap is not None and market_cap >= params.market_cap_min
            if market_cap is None:
                market_reason = "market cap missing"
            elif market_cap < params.market_cap_min:
                market_reason = f"market cap {market_cap:.0f} below min {params.market_cap_min}"
        elif params.market_cap_policy == MarketCapPolicy.WARN_ALLOW_MISSING:
            market_cap_ok = market_cap is None or market_cap >= params.market_cap_min
            if market_cap is not None and market_cap < params.market_cap_min:
                market_reason = f"market cap {market_cap:.0f} below min {params.market_cap_min}"
                market_cap_ok = False
            elif market_cap is None:
                market_reason = "market cap missing but allowed"
        self._add_gate(
            gates,
            "market_cap_ok",
            market_cap_ok,
            market_reason,
            {"market_cap": market_cap, "market_cap_policy": params.market_cap_policy.value},
        )
        if not market_cap_ok:
            return diagnosis

        available_levels = [level for level, head in heads.items() if head is not None]
        missing_levels = [level for level in levels if heads[level] is None]
        level_details = {
            level: self._historical_lookup_features(historical_lookups[level])
            for level in levels
        }
        for level in levels:
            gate_name = f"module_c_{level}_run_available"
            self._add_gate(
                gates,
                gate_name,
                heads[level] is not None,
                f"missing module C {level} historical run",
                {
                    "available": heads[level] is not None,
                    "available_levels": available_levels,
                    "missing_levels": missing_levels,
                    **level_details[level],
                },
            )
        all_heads_available = not missing_levels
        self._add_gate(
            gates,
            "module_c_all_runs_available",
            all_heads_available,
            f"missing module C historical runs: {', '.join(missing_levels)}",
            {
                "available_levels": available_levels,
                "missing_levels": missing_levels,
                "level_details": level_details,
            },
        )
        if not all_heads_available:
            return diagnosis

        weekly_signals[:] = await self.module_c_repo.get_signals(symbol.symbol_id, "1w", mode="predictive", as_of_time=as_of_time)
        buy_weekly = [signal for signal in weekly_signals if signal.side == "buy"]
        weekly_b1_candidates = [signal for signal in buy_weekly if signal.bsp_type == "1"]
        weekly_b2_candidates = [signal for signal in buy_weekly if signal.bsp_type in set(params.weekly_b2_types)]
        normalized_context_mode = params.weekly_context_mode_normalized
        weekly_b1_required = normalized_context_mode == "explicit_b1_then_b2"
        self._add_gate(
            gates,
            "weekly_b1_found",
            bool(weekly_b1_candidates) or not weekly_b1_required,
            "no weekly B1 found" if weekly_b1_required else None,
            {
                "weekly_context_mode": normalized_context_mode,
                "weekly_b1_count": len(weekly_b1_candidates),
                "required": weekly_b1_required,
            },
        )
        if not weekly_b1_candidates and weekly_b1_required:
            return diagnosis
        weekly_b2_label = "/".join(params.weekly_b2_types)
        self._add_gate(gates, "weekly_b2_found", bool(weekly_b2_candidates), f"no weekly {weekly_b2_label} found")
        if not weekly_b2_candidates:
            return diagnosis

        weekly_selection = self._select_weekly_context(
            weekly_b1_candidates=weekly_b1_candidates,
            weekly_b2_candidates=weekly_b2_candidates,
            context_mode=params.weekly_context_mode,
        )
        prior_b1_gate_ok = weekly_selection is not None and (
            weekly_selection.prior_weekly_b1_found or weekly_selection.bypass_prior_b1_gate
        )
        self._add_gate(
            gates,
            "weekly_b2_after_weekly_b1",
            prior_b1_gate_ok,
            "weekly B2 has no prior weekly B1",
            {
                "weekly_context_mode": normalized_context_mode,
                "prior_weekly_b1_found": bool(weekly_selection and weekly_selection.prior_weekly_b1_found),
                "same_bar_with_b1": bool(weekly_selection and weekly_selection.same_bar_with_b1),
                "same_price_with_b1": bool(weekly_selection and weekly_selection.same_price_with_b1),
                "bypass_prior_b1_gate": bool(weekly_selection and weekly_selection.bypass_prior_b1_gate),
            },
        )
        if not prior_b1_gate_ok or weekly_selection is None:
            return diagnosis
        weekly_b2 = weekly_selection.weekly_signal
        weekly_b1 = weekly_selection.weekly_b1

        weekly_b2_not_break = (
            weekly_selection.bypass_price_break_gate
            or weekly_b1 is None
            or weekly_b2.price > weekly_b1.price
        )
        self._add_gate(
            gates,
            "weekly_b2_not_break_weekly_b1",
            weekly_b2_not_break,
            None if weekly_b2_not_break else "weekly B2 price <= weekly B1 price",
            {
                "weekly_context_mode": normalized_context_mode,
                "weekly_b1_price": weekly_b1.price if weekly_b1 else None,
                "weekly_b2_price": weekly_b2.price,
                "bypass_price_break_gate": weekly_selection.bypass_price_break_gate,
            },
        )
        if not weekly_b2_not_break:
            return diagnosis

        weekly_bars = await self.kline_repo.get_klines(symbol.symbol_id, "1w", end=as_of_time)
        weekly_macd = compute_macd(weekly_bars)
        macd_row = next((item for item in reversed(weekly_macd) if item["ts"] <= weekly_b2.point_time), None)
        dif = float(macd_row["dif"]) if macd_row else 0.0
        dea = float(macd_row["dea"]) if macd_row else 0.0
        weekly_macd_ok = (dif > 0) or (not params.require_weekly_macd_dif_gt_zero)
        self._add_gate(
            gates,
            "weekly_macd_dif_gt_zero",
            weekly_macd_ok,
            None if weekly_macd_ok else f"weekly DIF <= 0 ({dif:.4f}) at weekly B2",
            {"dif": dif, "dea": dea, "check_enabled": params.require_weekly_macd_dif_gt_zero},
        )
        if not weekly_macd_ok:
            return diagnosis

        latest_close = weekly_bars[-1].close if weekly_bars else 0.0
        diagnosis.weekly_context = WeeklyContext(
            weekly_b1=weekly_b1,
            weekly_b2=weekly_b2,
            weekly_bsp_type=str(weekly_b2.bsp_type or ""),
            context_mode=normalized_context_mode,
            context_score=weekly_selection.context_score,
            anchor_time=weekly_selection.anchor_time,
            anchor_source=weekly_selection.anchor_source,
            stop_reference_price=weekly_selection.stop_reference_price,
            stop_reference_source=weekly_selection.stop_reference_source,
            prior_weekly_b1_found=weekly_selection.prior_weekly_b1_found,
            same_bar_with_b1=weekly_selection.same_bar_with_b1,
            same_price_with_b1=weekly_selection.same_price_with_b1,
            dif=dif,
            dea=dea,
            latest_close=latest_close,
            is_active=True,
        )

        daily_signals[:] = await self.module_c_repo.get_signals(symbol.symbol_id, "1d", mode="predictive", as_of_time=as_of_time)
        daily_bars_all = await self.kline_repo.get_klines(symbol.symbol_id, "1d", end=as_of_time)
        daily_selection = self._select_daily_setup(
            daily_signals=daily_signals,
            weekly_context=diagnosis.weekly_context,
            as_of_time=as_of_time,
            params=params,
            daily_bars=daily_bars_all,
        )
        self._add_gate(
            gates,
            "daily_b1_found_in_weekly_context",
            daily_selection is not None,
            self._daily_setup_gate_reason(params.daily_setup_mode),
            {
                "daily_setup_mode": params.daily_setup_mode,
                "is_official_strategy": params.is_official_daily_setup_mode,
            },
        )
        if daily_selection is None:
            return diagnosis
        daily_b1 = daily_selection.daily_b1

        strokes_1d = await self.module_c_repo.get_strokes(symbol.symbol_id, "1d", mode="predictive", as_of_time=as_of_time)
        prev_down = self._latest_stroke(strokes_1d, direction="down", before=daily_b1.point_time)
        self._add_gate(gates, "daily_previous_down_found", prev_down is not None, "no previous down stroke before daily B1")
        if prev_down is None:
            return diagnosis
        first_up = self._first_stroke(strokes_1d, direction="up", after=daily_b1.point_time)
        self._add_gate(gates, "daily_first_up_found", first_up is not None, "no first up stroke after daily B1")
        if first_up is None:
            return diagnosis

        daily_b2_candidates = [
            signal
            for signal in daily_signals
            if signal.side == "buy"
            and signal.bsp_type == "2"
            and signal.point_time > daily_b1.point_time
            and signal.price > daily_b1.price
        ]
        daily_b2s_candidates = [
            signal
            for signal in daily_signals
            if signal.side == "buy"
            and signal.bsp_type == "2s"
            and signal.point_time > daily_b1.point_time
            and signal.price > daily_b1.price
        ]
        if daily_selection.daily_b2 is not None and daily_selection.daily_b2 not in daily_b2_candidates:
            daily_b2_candidates.append(daily_selection.daily_b2)
        if daily_selection.daily_b2s is not None and daily_selection.daily_b2s not in daily_b2s_candidates:
            daily_b2s_candidates.append(daily_selection.daily_b2s)
        has_daily_b2_area = bool(daily_b2_candidates or daily_b2s_candidates)
        self._add_gate(
            gates,
            "daily_b2_or_2s_area_valid",
            has_daily_b2_area,
            "no valid daily B2/B2S area above daily B1",
        )

        centers_1d = await self.module_c_repo.get_centers(symbol.symbol_id, "1d", mode="predictive", as_of_time=as_of_time)
        segments_1d = await self.module_c_repo.get_segments(symbol.symbol_id, "1d", mode="predictive", as_of_time=as_of_time)
        center_type, center_low, center_high = find_last_relevant_daily_center(
            centers_1d,
            daily_b1_time=daily_b1.point_time,
            weekly_context_start_time=daily_selection.context_start_time,
        )
        if center_type is None:
            overlap_low, overlap_high = fallback_segment_overlap(segments_1d, daily_b1_time=daily_b1.point_time)
            if overlap_low is not None and overlap_high is not None:
                center_type = "SEGMENT_OVERLAP"
                center_low = overlap_low
                center_high = overlap_high
        has_center_or_overlap = center_type is not None
        self._add_gate(
            gates,
            "nearest_daily_center_or_overlap_found",
            has_center_or_overlap,
            "no daily center or segment overlap found",
            {"center_type": center_type, "center_low": center_low, "center_high": center_high},
        )
        if not has_center_or_overlap:
            return diagnosis

        segments_30f = await self.module_c_repo.get_segments(symbol.symbol_id, "30f", mode="predictive", as_of_time=as_of_time)
        centers_30f = await self.module_c_repo.get_centers(symbol.symbol_id, "30f", mode="predictive", as_of_time=as_of_time)
        daily_bars = [bar for bar in daily_bars_all if prev_down.start_time <= bar.ts <= as_of_time]
        strength = evaluate_daily_first_up_strength(
            previous_down_stroke=prev_down,
            first_up_stroke=first_up,
            daily_bars=daily_bars,
            daily_center_low=center_low,
            daily_center_high=center_high,
            sub_segments=segments_30f,
            sub_centers=centers_30f,
        )
        strength_ok = strength["strength_score"] >= params.strength_threshold
        self._add_gate(
            gates,
            "daily_strength_score_ok",
            strength_ok,
            None if strength_ok else f"strength score {strength['strength_score']:.2f} below threshold {params.strength_threshold:.2f}",
            strength,
        )
        if not strength_ok:
            diagnosis.daily_setup = DailySetup(
                daily_b1=daily_b1,
                daily_b2=daily_b2_candidates[-1] if daily_b2_candidates else None,
                daily_b2s=daily_b2s_candidates[-1] if daily_b2s_candidates else None,
                previous_down_stroke=prev_down,
                first_up_stroke=first_up,
                center_low=center_low,
                center_high=center_high,
                center_type=center_type,
                structure_score=strength["structure_score"],
                location_score=strength["location_score"],
                momentum_score=strength["momentum_score"],
                strength_score=strength["strength_score"],
                features={
                    **strength,
                    "daily_setup_mode": daily_selection.setup_mode,
                    "daily_setup_score": daily_selection.score,
                    "daily_b1_relation_to_weekly_signal": daily_selection.relation_to_weekly_signal,
                    "daily_b1_distance_trading_days": daily_selection.distance_trading_days,
                    "daily_signal_source": daily_selection.signal_source,
                    "is_official_strategy": daily_selection.is_official_strategy,
                },
            )
            return diagnosis

        center_entered = (
            params.allow_center_not_entered
            or strength["location_state"] in {"BREAK_ABOVE_CENTER", "ENTER_CENTER"}
        )
        self._add_gate(
            gates,
            "daily_first_up_enter_or_exceed_center",
            center_entered,
            None if center_entered else "first up stroke did not enter or exceed nearest center/overlap",
            {"location_state": strength["location_state"]},
        )
        if not center_entered:
            return diagnosis

        diagnosis.daily_setup = DailySetup(
            daily_b1=daily_b1,
            daily_b2=daily_b2_candidates[-1] if daily_b2_candidates else None,
            daily_b2s=daily_b2s_candidates[-1] if daily_b2s_candidates else None,
            previous_down_stroke=prev_down,
            first_up_stroke=first_up,
            center_low=center_low,
            center_high=center_high,
            center_type=center_type,
            structure_score=strength["structure_score"],
            location_score=strength["location_score"],
            momentum_score=strength["momentum_score"],
            strength_score=strength["strength_score"],
            features={
                **strength,
                "daily_setup_mode": daily_selection.setup_mode,
                "daily_setup_score": daily_selection.score,
                "daily_b1_relation_to_weekly_signal": daily_selection.relation_to_weekly_signal,
                "daily_b1_distance_trading_days": daily_selection.distance_trading_days,
                "daily_signal_source": daily_selection.signal_source,
                "is_official_strategy": daily_selection.is_official_strategy,
            },
        )
        self._add_gate(gates, "entry_watch_active", True, None, {"strength_score": strength["strength_score"]})

        daily_floor_time = diagnosis.daily_setup.daily_b2.point_time if diagnosis.daily_setup.daily_b2 else diagnosis.daily_setup.daily_b1.point_time
        signals_30f = await self.module_c_repo.get_signals(
            symbol.symbol_id,
            "30f",
            mode="predictive",
            as_of_time=as_of_time,
            start=daily_floor_time,
        )
        thirty_b1 = next(
            (signal for signal in reversed(signals_30f) if signal.side == "buy" and signal.bsp_type == "1"),
            None,
        )
        self._add_gate(gates, "thirty_f_b1_found", thirty_b1 is not None, "no 30F B1 found after daily setup")

        daily_all_bars = await self.kline_repo.get_klines(symbol.symbol_id, "1d", end=as_of_time)
        daily_bottom_time = latest_bottom_fractal_time(daily_all_bars, after=daily_floor_time)
        self._add_gate(
            gates,
            "daily_bottom_fractal_found",
            daily_bottom_time is not None,
            "no daily bottom fractal found after daily setup",
        )

        five_b2_confirm = None
        if thirty_b1 is not None:
            signals_5f = await self.module_c_repo.get_signals(
                symbol.symbol_id,
                "5f",
                mode="predictive",
                as_of_time=as_of_time,
                start=thirty_b1.point_time,
            )
            five_b2_confirm = next(
                (
                    signal for signal in reversed(signals_5f)
                    if signal.side == "buy" and signal.bsp_type in {"2", "2s"}
                ),
                None,
            )
        self._add_gate(
            gates,
            "five_f_b2_confirm_found",
            five_b2_confirm is not None,
            "no 5F B2/B2S confirmation found after 30F B1",
        )

        confidence_score = 0.0
        if thirty_b1 is not None:
            confidence_score += params.confidence_weight("30F_B1")
        if daily_bottom_time is not None:
            confidence_score += params.confidence_weight("DAILY_BOTTOM_FRACTAL")
        if five_b2_confirm is not None:
            confidence_score += params.confidence_weight("5F_B2_CONFIRM_30F_B1")
        entry_level = "30f" if thirty_b1 is not None else ("5f" if (five_b2_confirm is not None or daily_bottom_time is not None) else None)
        diagnosis.entry = EntryEvaluation(
            confidence_score=confidence_score,
            has_30f_b1=thirty_b1 is not None,
            thirty_b1=thirty_b1,
            five_b2_confirm=five_b2_confirm,
            daily_bottom_time=daily_bottom_time,
            entry_level=entry_level,
            reasons={
                "thirty_b1_time": thirty_b1.point_time.isoformat() if thirty_b1 else None,
                "daily_bottom_time": daily_bottom_time.isoformat() if daily_bottom_time else None,
                "five_b2_confirm_time": five_b2_confirm.point_time.isoformat() if five_b2_confirm else None,
            },
        )
        self._add_gate(gates, "entry_confidence_40", confidence_score >= 40.0, None, {"confidence_score": confidence_score})
        self._add_gate(gates, "entry_confidence_70", confidence_score >= 70.0, None, {"confidence_score": confidence_score})
        self._add_gate(gates, "entry_confidence_100", confidence_score >= 100.0, None, {"confidence_score": confidence_score})

        triggered = confidence_score >= params.entry_confidence_threshold and (
            not params.require_30f_b1 or thirty_b1 is not None
        )
        self._add_gate(
            gates,
            "entry_triggered",
            triggered,
            None if triggered else f"confidence {confidence_score:.2f} below trigger threshold {params.entry_confidence_threshold:.2f}",
            {"entry_level": entry_level},
        )

        status = ScanStatus.CANDIDATE
        if confidence_score > 0:
            status = ScanStatus.WATCH
        if triggered:
            status = ScanStatus.TRIGGER

        diagnosis.result = ScanResult(
            status=status,
            symbol=symbol,
            as_of_time=as_of_time,
            weekly_context=diagnosis.weekly_context,
            daily_setup=diagnosis.daily_setup,
            entry=diagnosis.entry,
            failed_gate=diagnosis.failed_gate,
        )

        exit_found = False
        exit_reason = "scan snapshot does not look ahead"
        if triggered and diagnosis.daily_setup is not None:
            pseudo_trade = Trade(
                symbol=symbol,
                entry_time=as_of_time,
                entry_price=daily_all_bars[-1].close if daily_all_bars else diagnosis.daily_setup.daily_b1.price,
                entry_reason=status.value,
                entry_confidence=confidence_score,
                entry_level=entry_level or "30f",
                daily_b1_price=diagnosis.daily_setup.daily_b1.price,
                stop_price=diagnosis.daily_setup.daily_b1.price,
                features={},
            )
            exit_decision = await self.exit_evaluator.evaluate(pseudo_trade, as_of_time, diagnosis.daily_setup)
            exit_found = exit_decision.should_exit
            exit_reason = exit_decision.reason or exit_reason
        self._add_gate(gates, "exit_found", exit_found, exit_reason if not exit_found else None)
        return diagnosis

    @staticmethod
    def _add_gate(gates: list[GateOutcome], name: str, passed: bool, reason: str | None, features: dict | None = None) -> None:
        gates.append(GateOutcome(name=name, passed=passed, reason=None if passed else reason, features=features or {}))

    @staticmethod
    def _historical_lookup_features(lookup) -> dict:
        return {
            "run_count": lookup.run_count,
            "selected_run_id": lookup.selected.run_id if lookup.selected else None,
            "selected_bar_until": lookup.selected.bar_until if lookup.selected else None,
            "selected_computed_at": lookup.selected.published_at if lookup.selected else None,
            "nearest_before_run_id": lookup.nearest_before.run_id if lookup.nearest_before else None,
            "nearest_before_bar_until": lookup.nearest_before.bar_until if lookup.nearest_before else None,
            "nearest_before_computed_at": lookup.nearest_before.published_at if lookup.nearest_before else None,
            "nearest_after_run_id": lookup.nearest_after.run_id if lookup.nearest_after else None,
            "nearest_after_bar_until": lookup.nearest_after.bar_until if lookup.nearest_after else None,
            "nearest_after_computed_at": lookup.nearest_after.published_at if lookup.nearest_after else None,
        }

    @staticmethod
    def _latest_stroke(strokes, *, direction: str, before):
        candidates = [stroke for stroke in strokes if stroke.direction == direction and stroke.end_time <= before]
        return candidates[-1] if candidates else None

    @staticmethod
    def _first_stroke(strokes, *, direction: str, after):
        candidates = [stroke for stroke in strokes if stroke.direction == direction and stroke.start_time >= after]
        return candidates[0] if candidates else None

    @staticmethod
    def _select_weekly_context(*, weekly_b1_candidates, weekly_b2_candidates, context_mode: str) -> _WeeklyContextSelection | None:
        if not weekly_b2_candidates:
            return None

        normalized_context_mode = StrategyDiagnoser._normalize_context_mode(context_mode)

        if normalized_context_mode == "same_bar_b1_b2s_as_candidate":
            for weekly_signal in reversed(weekly_b2_candidates):
                same_bar_b1 = StrategyDiagnoser._latest_same_bar_b1(weekly_b1_candidates, weekly_signal)
                if same_bar_b1 is None or not StrategyDiagnoser._same_price(same_bar_b1.price, weekly_signal.price):
                    continue
                return _WeeklyContextSelection(
                    weekly_signal=weekly_signal,
                    weekly_b1=same_bar_b1,
                    context_score=100.0,
                    anchor_time=weekly_signal.point_time,
                    anchor_source="same_bar_b1_b2s",
                    stop_reference_price=same_bar_b1.price,
                    stop_reference_source="same_bar_weekly_b1_price",
                    prior_weekly_b1_found=False,
                    same_bar_with_b1=True,
                    same_price_with_b1=True,
                    bypass_prior_b1_gate=True,
                    bypass_price_break_gate=True,
                )
            return None

        weekly_signal = weekly_b2_candidates[-1]
        prior_b1 = StrategyDiagnoser._latest_prior_b1(weekly_b1_candidates, weekly_signal.point_time)
        same_bar_b1 = StrategyDiagnoser._latest_same_bar_b1(weekly_b1_candidates, weekly_signal)
        same_bar_same_price = same_bar_b1 is not None and StrategyDiagnoser._same_price(same_bar_b1.price, weekly_signal.price)

        if normalized_context_mode == "explicit_b1_then_b2":
            if prior_b1 is None:
                return None
            return _WeeklyContextSelection(
                weekly_signal=weekly_signal,
                weekly_b1=prior_b1,
                context_score=100.0,
                anchor_time=prior_b1.point_time,
                anchor_source="prior_weekly_b1",
                stop_reference_price=prior_b1.price,
                stop_reference_source="weekly_b1_price",
                prior_weekly_b1_found=True,
                same_bar_with_b1=same_bar_b1 is not None,
                same_price_with_b1=same_bar_same_price,
                bypass_prior_b1_gate=False,
                bypass_price_break_gate=False,
            )

        if normalized_context_mode == "trust_chan_signal_with_b1_score":
            context_score = 60.0
            if same_bar_b1 is not None:
                context_score += 10.0
            if prior_b1 is not None:
                context_score += 30.0
            return _WeeklyContextSelection(
                weekly_signal=weekly_signal,
                weekly_b1=prior_b1 if prior_b1 is not None else (same_bar_b1 if same_bar_same_price else None),
                context_score=context_score,
                anchor_time=prior_b1.point_time if prior_b1 is not None else weekly_signal.point_time,
                anchor_source="prior_weekly_b1" if prior_b1 is not None else "weekly_signal_point_time",
                stop_reference_price=prior_b1.price if prior_b1 is not None else weekly_signal.price,
                stop_reference_source="weekly_b1_price" if prior_b1 is not None else f"weekly_{weekly_signal.bsp_type}_price",
                prior_weekly_b1_found=prior_b1 is not None,
                same_bar_with_b1=same_bar_b1 is not None,
                same_price_with_b1=same_bar_same_price,
                bypass_prior_b1_gate=True,
                bypass_price_break_gate=True,
            )

        if prior_b1 is not None:
            return _WeeklyContextSelection(
                weekly_signal=weekly_signal,
                weekly_b1=prior_b1,
                context_score=100.0,
                anchor_time=prior_b1.point_time,
                anchor_source="prior_weekly_b1",
                stop_reference_price=prior_b1.price,
                stop_reference_source="weekly_b1_price",
                prior_weekly_b1_found=True,
                same_bar_with_b1=same_bar_b1 is not None,
                same_price_with_b1=same_bar_same_price,
                bypass_prior_b1_gate=True,
                bypass_price_break_gate=False,
            )

        return _WeeklyContextSelection(
            weekly_signal=weekly_signal,
            weekly_b1=same_bar_b1 if same_bar_same_price else None,
            context_score=80.0 if same_bar_same_price else 60.0,
            anchor_time=weekly_signal.point_time,
            anchor_source="weekly_signal_point_time",
            stop_reference_price=weekly_signal.price,
            stop_reference_source=f"weekly_{weekly_signal.bsp_type}_price",
            prior_weekly_b1_found=False,
            same_bar_with_b1=same_bar_b1 is not None,
            same_price_with_b1=same_bar_same_price,
            bypass_prior_b1_gate=True,
            bypass_price_break_gate=True,
        )

    @staticmethod
    def _latest_prior_b1(weekly_b1_candidates, point_time):
        previous_b1 = [signal for signal in weekly_b1_candidates if signal.point_time < point_time]
        return previous_b1[-1] if previous_b1 else None

    @staticmethod
    def _latest_same_bar_b1(weekly_b1_candidates, weekly_signal):
        same_bar = [signal for signal in weekly_b1_candidates if signal.point_time == weekly_signal.point_time]
        return same_bar[-1] if same_bar else None

    @staticmethod
    def _same_price(left: float, right: float) -> bool:
        return abs(left - right) <= 0.0005

    @staticmethod
    def _normalize_context_mode(context_mode: str) -> str:
        if context_mode == "explicit_prior_b1":
            return "explicit_b1_then_b2"
        if context_mode in {"trust_weekly_b2_signal", "trust_weekly_b2_or_b2s_signal"}:
            return "trust_chan_signal"
        return context_mode

    @staticmethod
    def _daily_setup_gate_reason(daily_setup_mode: str) -> str:
        if daily_setup_mode == "daily_b1_near_weekly_context":
            return "no daily B1 found near weekly context window"
        if daily_setup_mode == "trust_daily_b2_or_b2s_signal":
            return "no trusted daily B2/B2s found in weekly context"
        if daily_setup_mode == "true_trust_daily_b2_or_b2s":
            return "no self-contained daily B2/B2s found in weekly context"
        if daily_setup_mode == "daily_b2_or_b2s_with_b1_score":
            return "no daily B2/B2s found for scored setup"
        if daily_setup_mode == "daily_buy_signal_any_observation":
            return "no daily buy signal found in weekly context"
        return "no daily B1 found after weekly context anchor"

    @classmethod
    def audit_daily_setup_semantics(
        cls,
        *,
        daily_signals,
        weekly_context: WeeklyContext,
        as_of_time: datetime,
        params,
        daily_bars,
    ) -> _DailySetupSemanticsAudit:
        buy_signals = [signal for signal in daily_signals if signal.side == "buy" and signal.point_time <= as_of_time]
        mode = params.daily_setup_mode
        context_start = weekly_context.anchor_time
        window_start = context_start
        window_end = as_of_time

        if mode == "daily_b1_near_weekly_context":
            window_start, window_end = cls._daily_window_bounds(
                daily_bars=daily_bars,
                anchor_time=weekly_context.weekly_b2.point_time,
                as_of_time=as_of_time,
                lookback_days=params.daily_b1_lookback_trading_days,
                lookforward_days=params.daily_b1_lookforward_trading_days,
            )

        signals_in_window = [signal for signal in buy_signals if window_start <= signal.point_time <= window_end]
        b1_in_window = [signal for signal in signals_in_window if signal.bsp_type == "1"]
        b2_or_b2s_in_window = [signal for signal in signals_in_window if signal.bsp_type in {"2", "2s"}]
        selected_b2_or_b2s = b2_or_b2s_in_window[-1] if b2_or_b2s_in_window else None
        prior_b1_for_b2 = (
            cls._latest_signal_before([signal for signal in buy_signals if signal.bsp_type == "1"], selected_b2_or_b2s.point_time)
            if selected_b2_or_b2s is not None
            else None
        )
        selected_buy_any = signals_in_window[-1] if signals_in_window else None
        selection = cls._select_daily_setup(
            daily_signals=daily_signals,
            weekly_context=weekly_context,
            as_of_time=as_of_time,
            params=params,
            daily_bars=daily_bars,
        )

        accepted_by_mode = selection is not None
        selected_signal_source = selection.signal_source if selection is not None else None
        selected_signal_kind = (
            selection.daily_b2.bsp_type if selection and selection.daily_b2 is not None
            else selection.daily_b2s.bsp_type if selection and selection.daily_b2s is not None
            else selection.daily_b1.bsp_type if selection is not None
            else None
        )
        selected_score = selection.score if selection is not None else 0.0

        if mode == "true_trust_daily_b2_or_b2s":
            accepted_by_mode = selected_b2_or_b2s is not None
            selected_signal_source = "self_contained_daily_b2_or_b2s" if accepted_by_mode else None
            selected_signal_kind = selected_b2_or_b2s.bsp_type if selected_b2_or_b2s is not None else None
            selected_score = 80.0 if accepted_by_mode else 0.0
        elif mode == "daily_b2_or_b2s_with_b1_score":
            accepted_by_mode = selected_b2_or_b2s is not None
            selected_signal_source = "daily_b2_or_b2s_with_b1_score" if accepted_by_mode else None
            selected_signal_kind = selected_b2_or_b2s.bsp_type if selected_b2_or_b2s is not None else None
            selected_score = 80.0 if accepted_by_mode else 0.0
            if accepted_by_mode and prior_b1_for_b2 is not None:
                selected_score += 10.0
                if selected_b2_or_b2s.price > prior_b1_for_b2.price:
                    selected_score += 10.0
        elif mode == "daily_buy_signal_any_observation":
            accepted_by_mode = selected_buy_any is not None
            selected_signal_source = "daily_buy_signal_any" if accepted_by_mode else None
            selected_signal_kind = selected_buy_any.bsp_type if selected_buy_any is not None else None
            selected_score = 30.0 if accepted_by_mode else 0.0

        return _DailySetupSemanticsAudit(
            mode=mode,
            context_start_time=context_start,
            window_start_time=window_start,
            window_end_time=window_end,
            daily_signal_any_found=bool(signals_in_window),
            daily_b1_found=bool(b1_in_window),
            daily_b2_or_b2s_found=bool(b2_or_b2s_in_window),
            daily_prior_b1_for_b2_found=prior_b1_for_b2 is not None,
            daily_b2_or_b2s_self_contained_accepted=selected_b2_or_b2s is not None,
            daily_setup_accepted_by_mode=accepted_by_mode,
            selected_daily_b1=selection.daily_b1 if selection is not None else prior_b1_for_b2,
            selected_daily_b2_or_b2s=selected_b2_or_b2s,
            selected_buy_signal_any=selected_buy_any,
            selected_signal_source=selected_signal_source,
            selected_signal_kind=selected_signal_kind,
            selected_signal_score=selected_score,
        )

    @classmethod
    def _select_daily_setup(
        cls,
        *,
        daily_signals,
        weekly_context: WeeklyContext,
        as_of_time: datetime,
        params,
        daily_bars,
    ) -> _DailySetupSelection | None:
        buy_signals = [signal for signal in daily_signals if signal.side == "buy" and signal.point_time <= as_of_time]
        b1_all = [signal for signal in buy_signals if signal.bsp_type == "1"]
        b2_all = [signal for signal in buy_signals if signal.bsp_type == "2"]
        b2s_all = [signal for signal in buy_signals if signal.bsp_type == "2s"]
        mode = params.daily_setup_mode

        if mode == "daily_b1_near_weekly_context":
            window = cls._daily_window_bounds(
                daily_bars=daily_bars,
                anchor_time=weekly_context.weekly_b2.point_time,
                as_of_time=as_of_time,
                lookback_days=params.daily_b1_lookback_trading_days,
                lookforward_days=params.daily_b1_lookforward_trading_days,
            )
            candidates = [signal for signal in b1_all if window[0] <= signal.point_time <= window[1]]
            if not candidates:
                return None
            daily_b1 = cls._select_nearest_daily_b1(candidates, weekly_context.weekly_b2.point_time, daily_bars)
            return _DailySetupSelection(
                daily_b1=daily_b1,
                daily_b2=cls._latest_signal_after_price(b2_all, daily_b1),
                daily_b2s=cls._latest_signal_after_price(b2s_all, daily_b1),
                context_start_time=min(weekly_context.anchor_time, daily_b1.point_time),
                setup_mode=mode,
                signal_source="explicit_b1",
                relation_to_weekly_signal=cls._relation_to_time(daily_b1.point_time, weekly_context.weekly_b2.point_time),
                distance_trading_days=cls._trading_day_distance(daily_bars, daily_b1.point_time, weekly_context.weekly_b2.point_time),
                score=60.0,
                is_official_strategy=False,
            )

        if mode == "trust_daily_b2_or_b2s_signal":
            trusted_candidates = [
                signal
                for signal in (b2_all + b2s_all)
                if signal.point_time >= weekly_context.anchor_time and signal.point_time <= as_of_time
            ]
            if not trusted_candidates:
                return None
            trusted_candidates.sort(key=lambda signal: (signal.point_time, signal.price))
            trusted = trusted_candidates[-1]
            prior_b1 = cls._latest_signal_before(b1_all, trusted.point_time)
            if prior_b1 is None:
                return None
            return _DailySetupSelection(
                daily_b1=prior_b1,
                daily_b2=trusted if trusted.bsp_type == "2" else None,
                daily_b2s=trusted if trusted.bsp_type == "2s" else None,
                context_start_time=min(weekly_context.anchor_time, prior_b1.point_time),
                setup_mode=mode,
                signal_source="trusted_b2" if trusted.bsp_type == "2" else "trusted_2s",
                relation_to_weekly_signal=cls._relation_to_time(prior_b1.point_time, weekly_context.weekly_b2.point_time),
                distance_trading_days=cls._trading_day_distance(daily_bars, prior_b1.point_time, weekly_context.weekly_b2.point_time),
                score=80.0,
                is_official_strategy=False,
            )

        candidates = [signal for signal in b1_all if signal.point_time >= weekly_context.anchor_time and signal.point_time <= as_of_time]
        if not candidates:
            return None
        daily_b1 = candidates[-1]
        return _DailySetupSelection(
            daily_b1=daily_b1,
            daily_b2=cls._latest_signal_after_price(b2_all, daily_b1),
            daily_b2s=cls._latest_signal_after_price(b2s_all, daily_b1),
            context_start_time=weekly_context.anchor_time,
            setup_mode=mode,
            signal_source="explicit_b1",
            relation_to_weekly_signal=cls._relation_to_time(daily_b1.point_time, weekly_context.weekly_b2.point_time),
            distance_trading_days=cls._trading_day_distance(daily_bars, daily_b1.point_time, weekly_context.weekly_b2.point_time),
            score=100.0,
            is_official_strategy=True,
        )

    @staticmethod
    def _latest_signal_after_price(signals, daily_b1):
        candidates = [
            signal
            for signal in signals
            if signal.point_time > daily_b1.point_time and signal.price > daily_b1.price
        ]
        return candidates[-1] if candidates else None

    @staticmethod
    def _latest_signal_before(signals, point_time):
        candidates = [signal for signal in signals if signal.point_time <= point_time]
        return candidates[-1] if candidates else None

    @staticmethod
    def _daily_window_bounds(*, daily_bars, anchor_time: datetime, as_of_time: datetime, lookback_days: int, lookforward_days: int) -> tuple[datetime, datetime]:
        if not daily_bars:
            return anchor_time, as_of_time
        times = [bar.ts for bar in daily_bars if bar.ts <= as_of_time]
        if not times:
            return anchor_time, as_of_time
        anchor_index = 0
        for index, ts in enumerate(times):
            if ts <= anchor_time:
                anchor_index = index
            else:
                break
        left = max(0, anchor_index - max(0, lookback_days))
        right = min(len(times) - 1, anchor_index + max(0, lookforward_days))
        return times[left], times[right]

    @staticmethod
    def _select_nearest_daily_b1(candidates, weekly_signal_time: datetime, daily_bars):
        def sort_key(signal):
            distance = StrategyDiagnoser._trading_day_distance(daily_bars, signal.point_time, weekly_signal_time)
            relation_rank = {"same_day": 0, "before": 1, "after": 2}.get(
                StrategyDiagnoser._relation_to_time(signal.point_time, weekly_signal_time),
                3,
            )
            return (abs(distance if distance is not None else 10**9), relation_rank, -int(signal.point_time.timestamp()))

        return sorted(candidates, key=sort_key)[0]

    @staticmethod
    def _relation_to_time(left: datetime, right: datetime) -> str:
        if left.date() == right.date():
            return "same_day"
        return "before" if left < right else "after"

    @staticmethod
    def _trading_day_distance(daily_bars, left: datetime, right: datetime) -> int | None:
        if not daily_bars:
            return None
        ordered = [bar.ts for bar in daily_bars]
        try:
            left_index = max(index for index, ts in enumerate(ordered) if ts <= left)
            right_index = max(index for index, ts in enumerate(ordered) if ts <= right)
        except ValueError:
            return None
        return left_index - right_index
