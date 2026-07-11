from __future__ import annotations

from datetime import datetime, timezone

from app.domain.models import (
    ChanSignal,
    EntryEvaluation,
    GateOutcome,
    PublishedHead,
    ScanDiagnosis,
    SymbolInfo,
    WeeklyContext,
)
from app.engine.diagnostic_reporting import GATE_ORDER, render_trace_markdown


def test_render_trace_markdown_handles_missing_prior_weekly_b1():
    symbol = SymbolInfo(1, "000001.SZ", "000001", "SZ", "PingAn")
    weekly_signal = ChanSignal(
        signal_id=2,
        level="1w",
        mode="predictive",
        point_time=datetime(2026, 7, 3, tzinfo=timezone.utc),
        base_time=datetime(2026, 7, 3, tzinfo=timezone.utc),
        base_seq=100,
        price=12.3,
        signal_type="bsp",
        side="buy",
        bsp_type="2s",
        confirmed=False,
        features={},
        run_id=99,
        snapshot_version="s99",
    )
    diagnosis = ScanDiagnosis(
        symbol=symbol,
        as_of_time=datetime(2026, 7, 7, tzinfo=timezone.utc),
        strategy_code="diag_trust_b2_or_b2s",
        market_cap=None,
        heads={
            "5f": PublishedHead(1, "s1", None, datetime(2026, 7, 7, tzinfo=timezone.utc), None),
            "30f": None,
            "1d": None,
            "1w": None,
            "1m": None,
        },
        weekly_signals=[weekly_signal],
        daily_signals=[],
        gates=[GateOutcome(name="weekly_b2_after_weekly_b1", passed=True, features={"bypass_prior_b1_gate": True})],
        weekly_context=WeeklyContext(
            weekly_b1=None,
            weekly_b2=weekly_signal,
            weekly_bsp_type="2s",
            context_mode="trust_weekly_b2_or_b2s_signal",
            context_score=60.0,
            anchor_time=weekly_signal.point_time,
            anchor_source="weekly_signal_point_time",
            stop_reference_price=12.3,
            stop_reference_source="weekly_2s_price",
            prior_weekly_b1_found=False,
            same_bar_with_b1=False,
            same_price_with_b1=False,
            dif=0.1,
            dea=0.05,
            latest_close=12.5,
            is_active=True,
        ),
        entry=EntryEvaluation(
            confidence_score=40.0,
            has_30f_b1=False,
            thirty_b1=None,
            five_b2_confirm=None,
            daily_bottom_time=None,
            entry_level=None,
            reasons={},
        ),
    )

    rendered = render_trace_markdown(diagnosis)

    assert "Weekly context mode" in rendered
    assert "trust_weekly_b2_or_b2s_signal" in rendered
    assert "Stop reference" in rendered
    assert "Weekly B1: `none`" in rendered


def test_gate_order_uses_level_specific_module_c_gates():
    assert "module_c_heads_available" not in GATE_ORDER
    assert GATE_ORDER[2:8] == [
        "module_c_5f_run_available",
        "module_c_30f_run_available",
        "module_c_1d_run_available",
        "module_c_1w_run_available",
        "module_c_1m_run_available",
        "module_c_all_runs_available",
    ]
