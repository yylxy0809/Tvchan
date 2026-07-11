from __future__ import annotations

import pytest

from collector.symbol_master import ensure_symbol_count_safe, prepare_symbol_master_symbols
from trading_protocol import SymbolInfo


def test_prepare_symbol_master_symbols_deduplicates_and_filters_non_a_share() -> None:
    symbols = prepare_symbol_master_symbols(
        [
            SymbolInfo("000001.SZ", "000001", "SZ", "Ping An"),
            SymbolInfo("000001.SZ", "000001", "SZ", "Ping An A"),
            SymbolInfo("900901.SH", "900901", "SH", "B share", market="B_SHARE"),
            SymbolInfo("510300.SH", "510300", "SH", "ETF", asset_type="fund"),
            SymbolInfo("", "", "SZ", ""),
        ]
    )

    assert len(symbols) == 1
    assert symbols[0].symbol == "000001.SZ"
    assert symbols[0].name == "Ping An A"
    assert symbols[0].is_active is True


def test_ensure_symbol_count_safe_rejects_seed_sized_lists() -> None:
    symbols = [SymbolInfo("000001.SZ", "000001", "SZ", "Ping An")]

    with pytest.raises(RuntimeError, match="Refusing to refresh symbol master"):
        ensure_symbol_count_safe(symbols, min_symbol_count=4000)


def test_ensure_symbol_count_safe_accepts_provider_sized_lists() -> None:
    symbols = [
        SymbolInfo(f"{index:06d}.SZ", f"{index:06d}", "SZ", f"Stock {index}")
        for index in range(4000)
    ]

    ensure_symbol_count_safe(symbols, min_symbol_count=4000)
