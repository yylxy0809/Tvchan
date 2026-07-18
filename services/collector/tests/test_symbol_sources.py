from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import collector.symbol_sources as symbol_sources
from collector.symbol_sources import (
    SourceDiscovery,
    clean_symbol_name,
    discover_consensus_symbols,
    discover_symbol_source,
    infer_exchange,
    parse_bse_jsonp,
    prepare_exchanges,
    _confirm_tencent_batch,
)
from collector.providers.http_kline import (
    baidu_row_to_bar,
    parse_local_datetime,
    tencent_row_to_bar,
    tencent_symbol,
    tencent_volume_to_shares,
)
from collector.providers.mootdx_provider import parse_mootdx_datetime
from collector.providers.pytdx_provider import (
    PytdxLunchReopenRowError,
    _tdx_bar_to_bar,
    _tdx_rows_to_bars,
)
from trading_protocol import SymbolInfo


def test_prepare_exchanges_defaults_to_sh_sz() -> None:
    assert prepare_exchanges(None) == {"SH", "SZ"}
    assert prepare_exchanges("SH,SZ,BJ") == {"SH", "SZ", "BJ"}


def test_infer_exchange_supports_bj_without_enabling_it_by_default() -> None:
    assert infer_exchange("600000") == "SH"
    assert infer_exchange("000001") == "SZ"
    assert infer_exchange("430047") == "BJ"
    assert infer_exchange("920000") == "BJ"


def test_exchange_response_helpers_parse_names_and_bse_jsonp() -> None:
    assert clean_symbol_name("<a><u>平安银行</u></a>") == "平安银行"

    payload = parse_bse_jsonp('callback([{"totalPages":1,"content":[{"xxzqdm":"920000","xxzqjc":"安徽凤凰"}]}])')
    assert payload["totalPages"] == 1
    assert payload["content"][0]["xxzqdm"] == "920000"


def test_tencent_symbol_supports_bj_prefix() -> None:
    assert tencent_symbol("600000.SH") == "sh600000"
    assert tencent_symbol("000001.SZ") == "sz000001"
    assert tencent_symbol("920000.BJ") == "bj920000"


def test_tencent_12_digit_minute_timestamp_parses_as_bar_end() -> None:
    parsed = parse_local_datetime("202607031455", "5f", timestamp_is_bar_end=True)

    assert parsed.hour == 14
    assert parsed.minute == 55
    assert parsed.second == 0


def test_date_only_provider_rows_normalize_to_daily_close() -> None:
    tencent = tencent_row_to_bar("000001.SZ", "1d", ["2026-07-10", "10", "11", "12", "9", "1"])
    baidu = baidu_row_to_bar("000001.SZ", "1d", ["2026-07-10", "10", "11", "9", "12", "1"])
    mootdx = parse_mootdx_datetime({"date": "2026-07-10"}, "1w")
    pytdx = _tdx_bar_to_bar(
        "000001.SZ",
        "1m",
        {"datetime": "2026-07-10", "open": 10, "high": 11, "low": 9, "close": 10, "vol": 1},
    )

    assert (tencent.ts.hour, tencent.ts.minute) == (15, 0)
    assert (baidu.ts.hour, baidu.ts.minute) == (15, 0)
    assert (mootdx.hour, mootdx.minute) == (15, 0)
    assert (pytdx.ts.hour, pytdx.ts.minute) == (15, 0)


def test_datetime_date_only_higher_timeframe_rows_normalize_to_close() -> None:
    local_midnight = datetime(2026, 7, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
    mootdx = parse_mootdx_datetime({"datetime": local_midnight}, "1w")
    pytdx = _tdx_bar_to_bar(
        "000001.SZ",
        "1m",
        {"datetime": local_midnight, "open": 10, "high": 11, "low": 9, "close": 10, "vol": 1},
    )

    assert (mootdx.hour, mootdx.minute) == (15, 0)
    assert (pytdx.ts.hour, pytdx.ts.minute) == (15, 0)


def test_datetime_intraday_provider_rows_preserve_bar_end_and_reject_off_session() -> None:
    valid = _tdx_bar_to_bar(
        "000001.SZ",
        "5f",
        {"datetime": datetime(2026, 7, 10, 9, 35), "open": 10, "high": 11, "low": 9, "close": 10, "vol": 1},
    )

    assert (valid.ts.hour, valid.ts.minute) == (9, 35)
    with pytest.raises(ValueError, match="session bar-end"):
        parse_mootdx_datetime({"datetime": datetime(2026, 7, 10, 12, 0)}, "30f")


def test_provider_intraday_opening_snapshot_is_preserved_and_0931_is_rejected() -> None:
    opening_snapshot = _tdx_bar_to_bar(
        "000001.SZ",
        "5f",
        {"datetime": datetime(2026, 7, 10, 9, 30), "open": 10, "high": 11, "low": 9, "close": 10, "vol": 1},
    )

    assert (opening_snapshot.ts.hour, opening_snapshot.ts.minute) == (9, 30)
    with pytest.raises(ValueError, match="session bar-end"):
        parse_mootdx_datetime({"datetime": datetime(2026, 7, 10, 9, 31)}, "30f")


@pytest.mark.parametrize(
    ("timeframe", "afternoon_time"),
    [("5f", "13:05"), ("15f", "13:15"), ("30f", "13:30"), ("1h", "14:00")],
)
def test_pytdx_discards_exact_1300_lunch_reopen_duplicate(
    timeframe: str, afternoon_time: str
) -> None:
    rows = _tdx_rows_to_bars(
        "000001.SZ",
        timeframe,
        [
            _tdx_row("2026-07-10 11:30", amount=1234.5),
            _tdx_row("2026-07-10 13:00", amount=1234.5),
            _tdx_row(f"2026-07-10 {afternoon_time}", close=10.6),
        ],
    )

    assert [(bar.ts.hour, bar.ts.minute) for bar in rows] == [
        (11, 30),
        tuple(int(value) for value in afternoon_time.split(":")),
    ]


@pytest.mark.parametrize("timeframe", ["5f", "15f", "30f", "1h"])
def test_pytdx_rejects_1300_without_same_day_1130_comparator(timeframe: str) -> None:
    with pytest.raises(PytdxLunchReopenRowError, match="missing_1130_comparator"):
        _tdx_rows_to_bars("000001.SZ", timeframe, [_tdx_row("2026-07-10 13:00")])


@pytest.mark.parametrize("field", ["close", "vol"])
def test_pytdx_rejects_1300_when_comparable_values_differ(field: str) -> None:
    morning = _tdx_row("2026-07-10 11:30", amount=1234.5)
    lunch = _tdx_row("2026-07-10 13:00", amount=1234.5)
    lunch[field] = float(lunch[field]) + 1

    with pytest.raises(PytdxLunchReopenRowError, match="mismatched_ohlcv"):
        _tdx_rows_to_bars("000001.SZ", "15f", [morning, lunch])


@pytest.mark.parametrize(
    ("morning_amount", "lunch_amount"),
    [(None, 1234.5), (1234.5, None), (1234.5, 1235.5)],
)
def test_pytdx_rejects_1300_when_amount_equality_is_unproven(
    morning_amount: float | None, lunch_amount: float | None
) -> None:
    morning = _tdx_row("2026-07-10 11:30", amount=morning_amount)
    lunch = _tdx_row("2026-07-10 13:00", amount=lunch_amount)

    with pytest.raises(PytdxLunchReopenRowError, match="amount_mismatch"):
        _tdx_rows_to_bars("000001.SZ", "15f", [morning, lunch])


def test_pytdx_discards_1300_when_both_amounts_are_null() -> None:
    rows = _tdx_rows_to_bars(
        "000001.SZ",
        "15f",
        [_tdx_row("2026-07-10 11:30"), _tdx_row("2026-07-10 13:00")],
    )

    assert [(bar.ts.hour, bar.ts.minute) for bar in rows] == [(11, 30)]


def test_pytdx_page_uses_overlap_only_as_lunch_comparator() -> None:
    rows = _tdx_rows_to_bars(
        "000001.SZ",
        "5f",
        [_tdx_row("2026-07-10 13:00", amount=1234.5)],
        comparator_items=[_tdx_row("2026-07-10 11:30", amount=1234.5)],
    )

    assert rows == []


def _tdx_row(
    value: str,
    *,
    close: float = 10.5,
    amount: float | None = None,
) -> dict:
    return {
        "datetime": value,
        "open": 10.0,
        "high": 11.0,
        "low": 9.5,
        "close": close,
        "vol": 1000.0,
        "amount": amount,
    }


def test_tencent_volume_is_normalized_to_share_units() -> None:
    assert tencent_volume_to_shares("600176.SH", "37295.00") == 3_729_500
    assert tencent_volume_to_shares("000001.SZ", "15283.00") == 1_528_300
    assert tencent_volume_to_shares("300733.SZ", "1391.00") == 139_100
    assert tencent_volume_to_shares("688538.SH", "5383592.00") == 5_383_592


def test_discover_consensus_symbols_requires_minimum_confirmations(monkeypatch) -> None:
    async def fake_discover(source, **_kwargs):
        if source == "cninfo":
            return SourceDiscovery(
                "cninfo",
                [
                    SymbolInfo("000001.SZ", "000001", "SZ", "Ping An"),
                    SymbolInfo("000002.SZ", "000002", "SZ", "Vanke"),
                ],
            )
        return SourceDiscovery("pytdx", [SymbolInfo("000001.SZ", "000001", "SZ", "Ping An")])

    async def fake_confirm(candidates, **_kwargs):
        return SourceDiscovery("tencent", [item for item in candidates if item.code == "000002"])

    monkeypatch.setattr(symbol_sources, "discover_symbol_source", fake_discover)
    monkeypatch.setattr(symbol_sources, "confirm_symbols_with_tencent", fake_confirm)

    result = asyncio.run(
        discover_consensus_symbols(
            sources=["cninfo", "pytdx", "tencent"],
            exchanges={"SH", "SZ"},
            min_confirmations=2,
            timeout=1,
            tdx_host=None,
            tdx_port=7709,
            tdx_timeout=1,
            tdx_retries=1,
        )
    )

    assert [item.symbol for item in result.symbols] == ["000001.SZ", "000002.SZ"]
    assert result.source_counts == {"cninfo": 2, "pytdx": 1, "tencent": 1}
    assert result.confirmation_counts == {"2": 2}


def test_szse_source_falls_back_to_cninfo_sz_only(monkeypatch) -> None:
    async def fail_szse(**_kwargs):
        raise RuntimeError("szse blocked")

    async def fake_cninfo(**kwargs):
        rows = [
            SymbolInfo("000001.SZ", "000001", "SZ", "Ping An"),
            SymbolInfo("600000.SH", "600000", "SH", "SPD Bank"),
        ]
        return [item for item in rows if item.exchange in kwargs["exchanges"]]

    monkeypatch.setattr(symbol_sources, "list_szse_symbols", fail_szse)
    monkeypatch.setattr(symbol_sources, "list_cninfo_symbols", fake_cninfo)

    result = asyncio.run(
        discover_symbol_source(
            "szse",
            exchanges={"SH", "SZ"},
            timeout=1,
            tdx_host=None,
            tdx_port=7709,
            tdx_timeout=1,
            tdx_retries=1,
        )
    )

    assert result.source == "szse"
    assert [item.symbol for item in result.symbols] == ["000001.SZ"]
    assert "used cninfo fallback" in (result.error or "")


def test_tencent_confirmation_filters_delisted_and_unlisted_flags() -> None:
    def payload(market: str, name: str, code: str, status: str) -> str:
        fields = [""] * 88
        fields[0] = market
        fields[1] = name
        fields[2] = code
        fields[3] = "10.0"
        fields[30] = "20260703100000"
        fields[40] = status
        return "~".join(fields)

    class FakeResponse:
        text = (
            f'v_sz000001="{payload("51", "Ping An", "000001", "")}";'
            f'v_sz000003="{payload("51", "PT Jintian", "000003", "D")}";'
            f'v_sh688688="{payload("1", "Invalid", "688688", "U")}";'
        )

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        async def get(self, _url):
            return FakeResponse()

    confirmed = asyncio.run(
        _confirm_tencent_batch(
            FakeClient(),
            [
                SymbolInfo("000001.SZ", "000001", "SZ", "Ping An"),
                SymbolInfo("000003.SZ", "000003", "SZ", "PT Jintian"),
                SymbolInfo("688688.SH", "688688", "SH", "Invalid"),
            ],
        )
    )

    assert [item.symbol for item in confirmed] == ["000001.SZ"]
