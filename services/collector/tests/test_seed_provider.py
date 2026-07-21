from __future__ import annotations

import asyncio
import zipfile
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from collector.providers.seed import SeedProvider
from collector.providers.pytdx_provider import (
    TDX_SERVERS,
    TIMEFRAME_TO_TDX_CATEGORY,
    PytdxProvider,
    _is_a_share_code,
    _split_tdx_symbol,
    _tdx_bar_to_bar,
)
from collector.market_fill import (
    bar_to_chan_payload,
    normalize_symbol,
    parse_timeframes,
    select_symbols,
)
from collector.tdx_csv_import import (
    SymbolBudget,
    discover_archives,
    infer_exchange,
    infer_identity,
    parse_csv_entry,
    parse_csv_row,
    parse_entry_metadata,
    process_task as process_tdx_csv_task,
    resolve_metadata_from_row,
)
from collector.history_backfill import (
    DB_TO_TIMEFRAME as HISTORY_DB_TO_TIMEFRAME,
    get_provider_page,
    load_symbols_file,
    parse_stop_at,
    process_task as process_history_task,
    process_tasks_concurrently as process_history_tasks_concurrently,
)
from collector.storage.chan_postgres import (
    direction_to_code,
    epoch_to_datetime,
    mode_to_code,
)
from collector.storage.postgres import (
    bar_to_db_values,
    source_to_code,
    timeframe_to_db_code,
)
from trading_protocol import Bar


def test_seed_provider_lists_symbols() -> None:
    asyncio.run(_assert_seed_provider_lists_symbols())


async def _assert_seed_provider_lists_symbols() -> None:
    provider = SeedProvider()
    symbols = await provider.list_symbols()
    assert any(item.symbol == "000001.SZ" for item in symbols)


def test_seed_provider_returns_valid_bars() -> None:
    asyncio.run(_assert_seed_provider_returns_valid_bars())


async def _assert_seed_provider_returns_valid_bars() -> None:
    provider = SeedProvider()
    bars = await provider.get_bars("000001.SZ", "5f", limit=10)
    assert bars
    for bar in bars:
        assert bar.low <= bar.open <= bar.high
        assert bar.low <= bar.close <= bar.high


def test_timeframe_to_db_code_keeps_month_as_month() -> None:
    assert timeframe_to_db_code("1m") == 43200
    assert timeframe_to_db_code("M") == 43200


def test_bar_to_db_values_scales_prices() -> None:
    provider = SeedProvider()
    bars = asyncio.run(provider.get_bars("000001.SZ", "5f", limit=1))
    values = bar_to_db_values(bars[0])
    assert values[1] == 5
    assert values[3] == int(round(bars[0].open * 1000))


def test_source_codes_distinguish_seed_and_pytdx() -> None:
    assert source_to_code("seed") == 1
    assert source_to_code("pytdx") == 2
    assert source_to_code("tdx_csv") == 3


def test_pytdx_symbol_market_mapping() -> None:
    assert _split_tdx_symbol("000001.SZ") == (0, "000001")
    assert _split_tdx_symbol("600519.SH") == (1, "600519")
    assert _split_tdx_symbol("600000") == (1, "600000")
    assert _split_tdx_symbol("510300.SH") == (1, "510300")
    assert _split_tdx_symbol("510300") == (1, "510300")
    assert _split_tdx_symbol("159915.SZ") == (0, "159915")
    assert _split_tdx_symbol("159915") == (0, "159915")


@pytest.mark.parametrize(
    "symbol", ["920047.BJ", "920047", "430047", "00700.HK", "000001.UNKNOWN"]
)
def test_pytdx_symbol_market_mapping_rejects_unsupported_exchange(symbol: str) -> None:
    with pytest.raises(ValueError, match="SH/SZ"):
        _split_tdx_symbol(symbol)


def test_pytdx_timeframe_mapping_keeps_monthly_1m() -> None:
    assert TIMEFRAME_TO_TDX_CATEGORY["5f"] == "KLINE_TYPE_5MIN"
    assert TIMEFRAME_TO_TDX_CATEGORY["1m"] == "KLINE_TYPE_MONTHLY"


def test_pytdx_default_servers_include_local_tdx_site() -> None:
    assert ("124.70.199.56", 7709) in TDX_SERVERS


def test_pytdx_timeout_is_configurable() -> None:
    provider = PytdxProvider(host="124.70.199.56", timeout=12, retries=4)
    assert provider.timeout == 12
    assert provider.retries == 4


def test_pytdx_marks_the_open_intraday_period_incomplete() -> None:
    item = {
        "datetime": "2026-07-21 09:35",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "vol": 100,
    }

    open_bar = _tdx_bar_to_bar(
        "000001.SZ",
        "5f",
        item,
        now=datetime(2026, 7, 21, 9, 33, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    closed_bar = _tdx_bar_to_bar(
        "000001.SZ",
        "5f",
        item,
        now=datetime(2026, 7, 21, 9, 36, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert (open_bar.complete, open_bar.revision) == (False, 1)
    assert (closed_bar.complete, closed_bar.revision) == (True, 0)


def test_pytdx_close_boundary_requires_one_second_grace() -> None:
    item = {
        "datetime": "2026-07-21 09:35",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "vol": 100,
    }

    boundary = _tdx_bar_to_bar(
        "000001.SZ", "5f", item,
        now=datetime(2026, 7, 21, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    after_grace = _tdx_bar_to_bar(
        "000001.SZ", "5f", item,
        now=datetime(2026, 7, 21, 9, 35, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert boundary.complete is False
    assert after_grace.complete is True


def test_pytdx_weekly_period_closes_only_after_the_week() -> None:
    item = {
        "datetime": "2026-07-20",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "vol": 100,
    }

    current_week = _tdx_bar_to_bar(
        "000001.SZ",
        "1w",
        item,
        now=datetime(2026, 7, 24, 15, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    next_week = _tdx_bar_to_bar(
        "000001.SZ",
        "1w",
        item,
        now=datetime(2026, 7, 27, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert current_week.complete is False
    assert next_week.complete is True


@pytest.mark.parametrize("comparator_offset", [9, 11])
def test_pytdx_raw_page_uses_adjacent_overlap_and_reports_raw_count(comparator_offset: int) -> None:
    def row(value: str) -> dict:
        return {
            "datetime": value,
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "vol": 1000.0,
            "amount": 1234.5,
        }

    class FakeApi:
        def __init__(self) -> None:
            self.calls = []

        def get_security_bars(self, _category, market, code, offset, limit):
            self.calls.append((market, code, offset, limit))
            if offset == 10:
                return [row("2026-07-10 13:00")]
            if offset == comparator_offset:
                return [row("2026-07-10 11:30")]
            return [row("2026-07-10 13:05")]

    api = FakeApi()
    provider = PytdxProvider()
    provider._get_connected_api = lambda _factory: api

    bars, raw_count = provider._get_bars_page_with_raw_count_sync(
        "000001.SZ", "5f", 10, 1
    )

    assert bars == []
    assert raw_count == 1
    assert api.calls == [
        (0, "000001", 10, 1),
        (0, "000001", 9, 1),
        (0, "000001", 11, 1),
    ]


def test_pytdx_a_share_code_filter() -> None:
    assert _is_a_share_code("SH", "600000")
    assert _is_a_share_code("SH", "688001")
    assert _is_a_share_code("SZ", "000001")
    assert _is_a_share_code("SZ", "300750")
    assert not _is_a_share_code("SH", "900901")
    assert not _is_a_share_code("SZ", "200001")


def test_tdx_csv_entry_metadata_parses_stock_file() -> None:
    metadata = parse_entry_metadata("202108/000001_骞冲畨閾惰_1_姝灒缃?www.waizaowang.com).csv")
    assert metadata is not None
    assert metadata.code == "000001"
    assert metadata.symbol == "000001.SZ"
    assert metadata.name == "骞冲畨閾惰"
    assert metadata.category == "1"
    assert infer_exchange("600519") == "SH"
    assert infer_exchange("300750") == "SZ"


def test_tdx_csv_identity_disambiguates_index_and_stock_code() -> None:
    index_exchange, index_asset_type = infer_identity("000001", "\u4e0a\u8bc1\u6307\u6570")
    stock_exchange, stock_asset_type = infer_identity("000001", "\u5e73\u5b89\u94f6\u884c")

    assert (index_exchange, index_asset_type) == ("SH", "index")
    assert (stock_exchange, stock_asset_type) == ("SZ", "stock")


def test_tdx_csv_row_metadata_overrides_filename_guess_for_index() -> None:
    metadata = parse_entry_metadata("202108/000001_000001_1_test.csv")
    assert metadata is not None

    resolved = resolve_metadata_from_row(
        metadata,
        [
            "000001",
            "\u4e0a\u8bc1\u6307\u6570",
            "5.0",
            "0.0",
            "2021-08-06 09:35:00",
            "3021.21",
            "3027.71",
            "3045.58",
            "3011.45",
            "31116.0",
            "5.46935E7",
            "0.02",
        ],
    )

    assert resolved.symbol == "000001.SH"
    assert resolved.asset_type == "index"


def test_tdx_csv_row_parses_bar_with_share_volume() -> None:
    row = [
        "000001",
        "骞冲畨閾惰",
        "5.0",
        "0.0",
        "2021-08-06 09:35:00",
        "17.55",
        "17.59",
        "17.70",
        "17.49",
        "31116.0",
        "5.46935E7",
        "0.02",
    ]
    bar = parse_csv_row(row, "000001.SZ", "5f")
    assert bar is not None
    assert bar.symbol == "000001.SZ"
    assert bar.timeframe == "5f"
    assert bar.open == 17.55
    assert bar.close == 17.59
    assert bar.volume == 3_111_600
    assert bar.amount == 54_693_500
    assert bar.source == "tdx_csv"


def test_tdx_csv_row_skips_unwanted_fq_series() -> None:
    row = [
        "000001",
        "骞冲畨閾惰",
        "5.0",
        "2.0",
        "2021-08-06 09:35:00",
        "3021.21",
        "3027.71",
        "3045.58",
        "3011.45",
        "31116.0",
        "5.46935E7",
        "0.02",
    ]
    assert parse_csv_row(row, "000001.SZ", "5f", fq_values={"0"}) is None


def test_tdx_csv_entry_filters_fq_and_keeps_stock_identity(tmp_path) -> None:
    entry_name = "202108/000001_\u5e73\u5b89\u94f6\u884c_1_test.csv"
    zip_path = tmp_path / "202108.zip"
    content = "\n".join(
        [
            "code,name,ktype,fq,tdate,open,close,high,low,cjl,cje,hsl",
            "\u4ee3\u7801,\u540d\u79f0,K\u7ebf\u7c7b\u578b,\u590d\u6743,\u65f6\u95f4,\u5f00,\u6536,\u9ad8,\u4f4e,\u6210\u4ea4\u91cf,\u6210\u4ea4\u989d,\u6362\u624b\u7387",
            "000001,\u5e73\u5b89\u94f6\u884c,5.0,2.0,2021-08-06 09:35:00,17.55,17.59,17.70,17.49,31116.0,5.46935E7,0.02",
            "000001,\u5e73\u5b89\u94f6\u884c,5.0,0.0,2021-08-06 09:40:00,17.59,17.60,17.72,17.51,12000.0,2.10000E7,0.01",
        ]
    )
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(entry_name, content)

    with zipfile.ZipFile(zip_path) as archive:
        entry = archive.infolist()[0]
        metadata = parse_entry_metadata(entry.filename)
        assert metadata is not None
        parsed = parse_csv_entry(
            archive,
            entry,
            metadata,
            "5f",
            asset_types={"stock"},
            fq_values={"0"},
        )

    assert parsed is not None
    assert parsed.symbol.symbol == "000001.SZ"
    assert parsed.symbol.asset_type == "stock"
    assert len(parsed.bars) == 1
    assert parsed.bars[0].ts.minute == 40


def test_tdx_csv_entry_can_import_same_code_index_separately(tmp_path) -> None:
    entry_name = "202108/000001_\u4e0a\u8bc1\u6307\u6570_10_test.csv"
    zip_path = tmp_path / "202108.zip"
    content = "\n".join(
        [
            "code,name,ktype,fq,tdate,open,close,high,low,cjl,cje,hsl",
            "000001,\u4e0a\u8bc1\u6307\u6570,5.0,0.0,2021-08-06 09:35:00,3021.21,3027.71,3045.58,3011.45,31116.0,5.46935E7,0.02",
        ]
    )
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(entry_name, content)

    with zipfile.ZipFile(zip_path) as archive:
        entry = archive.infolist()[0]
        metadata = parse_entry_metadata(entry.filename)
        assert metadata is not None
        parsed = parse_csv_entry(
            archive,
            entry,
            metadata,
            "5f",
            asset_types={"index"},
            fq_values={"0"},
        )

    assert parsed is not None
    assert parsed.symbol.symbol == "000001.SH"
    assert parsed.symbol.asset_type == "index"
    assert parsed.symbol.market == "A_SHARE_INDEX"


def test_tdx_csv_entry_imports_header_without_fq_column(tmp_path) -> None:
    entry_name = "202108/000001_\u5e73\u5b89\u94f6\u884c_1_test.csv"
    zip_path = tmp_path / "202108.zip"
    content = "\n".join(
        [
            "code,name,ktype,tdate,open,close,high,low,cjl,cje,hsl",
            "000001,\u5e73\u5b89\u94f6\u884c,5.0,2021-08-06 09:35:00,17.55,17.59,17.70,17.49,31116.0,5.46935E7,0.02",
        ]
    )
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(entry_name, content)

    with zipfile.ZipFile(zip_path) as archive:
        entry = archive.infolist()[0]
        metadata = parse_entry_metadata(entry.filename)
        assert metadata is not None
        parsed = parse_csv_entry(
            archive,
            entry,
            metadata,
            "5f",
            asset_types={"stock"},
            fq_values={"0"},
        )

    assert parsed is not None
    assert parsed.symbol.symbol == "000001.SZ"
    assert len(parsed.bars) == 1
    assert parsed.bars[0].open == 17.55
    assert parsed.bars[0].close == 17.59
    assert parsed.bars[0].high == 17.70
    assert parsed.bars[0].low == 17.49


def test_tdx_csv_entry_imports_minimal_one_minute_header(tmp_path) -> None:
    entry_name = "202108/000001_\u5e73\u5b89\u94f6\u884c_1_test.csv"
    zip_path = tmp_path / "202108.zip"
    content = "\n".join(
        [
            "code,tdate,open,close,high,low,cjl,cje,cjjj",
            "000001,2021-08-06 09:31:00,17.55,17.59,17.70,17.49,31116.0,5.46935E7,17.58",
        ]
    )
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(entry_name, content)

    with zipfile.ZipFile(zip_path) as archive:
        entry = archive.infolist()[0]
        metadata = parse_entry_metadata(entry.filename)
        assert metadata is not None
        parsed = parse_csv_entry(
            archive,
            entry,
            metadata,
            "1f",
            asset_types={"stock"},
            fq_values={"0"},
        )

    assert parsed is not None
    assert parsed.symbol.symbol == "000001.SZ"
    assert parsed.symbol.name == "\u5e73\u5b89\u94f6\u884c"
    assert len(parsed.bars) == 1
    assert parsed.bars[0].timeframe == "1f"
    assert parsed.bars[0].volume == 3_111_600


def test_tdx_csv_entry_rejects_unsupported_header(tmp_path) -> None:
    entry_name = "202108/000001_\u5e73\u5b89\u94f6\u884c_1_test.csv"
    zip_path = tmp_path / "202108.zip"
    content = "\n".join(
        [
            "code,name,tdate,open,close",
            "000001,\u5e73\u5b89\u94f6\u884c,2021-08-06 09:35:00,17.55,17.59",
        ]
    )
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(entry_name, content)

    with zipfile.ZipFile(zip_path) as archive:
        entry = archive.infolist()[0]
        metadata = parse_entry_metadata(entry.filename)
        assert metadata is not None
        with pytest.raises(ValueError, match="Unsupported TDX CSV header"):
            parse_csv_entry(
                archive,
                entry,
                metadata,
                "5f",
                asset_types={"stock"},
                fq_values={"0"},
            )


def test_tdx_csv_discover_archives(tmp_path) -> None:
    folder = tmp_path / "五分钟K线数据"
    folder.mkdir()
    archive = folder / "202108.zip"
    archive.write_bytes(b"not-a-real-zip")
    discovered = discover_archives(tmp_path, ["5f"])
    assert len(discovered) == 1
    assert discovered[0].zip_path.endswith("202108.zip")
    assert discovered[0].timeframe == "5f"


def test_tdx_csv_process_task_honors_symbol_limit_without_name_error(tmp_path) -> None:
    zip_path = tmp_path / "202108.zip"
    header = "code,name,ktype,fq,tdate,open,close,high,low,cjl,cje,hsl"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "202108/000001_\u5e73\u5b89\u94f6\u884c_1_test.csv",
            "\n".join(
                [
                    header,
                    "000001,\u5e73\u5b89\u94f6\u884c,5.0,0.0,2021-08-06 09:35:00,17.55,17.59,17.70,17.49,31116.0,5.46935E7,0.02",
                ]
            ),
        )
        archive.writestr(
            "202108/000002_\u4e07\u79d1A_1_test.csv",
            "\n".join(
                [
                    header,
                    "000002,\u4e07\u79d1A,5.0,0.0,2021-08-06 09:35:00,20.01,20.02,20.10,19.99,100.0,2.0E5,0.01",
                ]
            ),
        )

    class FakeWriter:
        def __init__(self) -> None:
            self.symbols = []
            self.bars = []

        async def upsert_symbols(self, symbols):
            self.symbols.extend(symbols)

        async def upsert_bars(self, bars):
            self.bars.extend(bars)
            return len(bars)

    class FakeTaskStore:
        def __init__(self) -> None:
            self.progress = []
            self.paused = []
            self.successes = []

        async def record_progress(self, **kwargs):
            self.progress.append(kwargs)

        async def record_success(self, **kwargs):
            self.successes.append(kwargs)

        async def record_paused(self, **kwargs):
            self.paused.append(kwargs)

        async def record_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    writer = FakeWriter()
    task_store = FakeTaskStore()
    result = asyncio.run(
        process_tdx_csv_task(
            writer=writer,
            task_store=task_store,
            task={
                "id": 1,
                "zip_path": str(zip_path),
                "timeframe": 5,
                "last_entry_index": -1,
            },
            symbols_filter=set(),
            symbol_limit=1,
            categories={"1"},
            asset_types={"stock"},
            fq_values={"0"},
            entry_batch_size=1,
            bar_batch_size=100,
            max_entries_per_task=0,
            symbol_budget=SymbolBudget(symbol_limit=1),
        )
    )

    assert result == {"bars": 1}
    assert [item.symbol for item in writer.symbols] == ["000001.SZ"]
    assert [item.symbol for item in writer.bars] == ["000001.SZ"]
    assert task_store.successes == []
    assert task_store.paused[0]["entries_total"] == 2


def test_chan_storage_codes() -> None:
    assert mode_to_code("confirmed") == 1
    assert mode_to_code("predictive") == 2
    assert direction_to_code("up") == 1
    assert direction_to_code("down") == -1
    assert epoch_to_datetime(0).tzinfo is not None


def test_market_fill_normalizes_symbols_and_timeframes() -> None:
    assert normalize_symbol("600519") == "600519.SH"
    assert normalize_symbol("000001") == "000001.SZ"
    assert parse_timeframes("5,D,M") == ["5f", "1d", "1m"]


def test_market_fill_selects_requested_symbols() -> None:
    provider = SeedProvider()
    selected = asyncio.run(select_symbols(provider, "000001,600519.SH", 10))
    assert [item.symbol for item in selected] == ["000001.SZ", "600519.SH"]


def test_market_fill_bar_to_chan_payload() -> None:
    provider = SeedProvider()
    bar = asyncio.run(provider.get_bars("000001.SZ", "5f", limit=1))[0]
    payload = bar_to_chan_payload(bar)
    assert payload["time"] == int(bar.ts.timestamp())
    assert payload["open"] == bar.open
    assert payload["volume"] == bar.volume


def test_history_backfill_db_timeframe_mapping() -> None:
    assert HISTORY_DB_TO_TIMEFRAME[5] == "5f"
    assert HISTORY_DB_TO_TIMEFRAME[1440] == "1d"
    assert HISTORY_DB_TO_TIMEFRAME[43200] == "1m"


def test_history_backfill_loads_explicit_symbols_file(tmp_path) -> None:
    path = tmp_path / "symbols.txt"
    path.write_text("# scoped tail\n000001.SZ\n600000.SH\n", encoding="utf-8")

    symbols = load_symbols_file(path)

    assert [item.symbol for item in symbols] == ["000001.SZ", "600000.SH"]

    path.write_text("000001.SZ\n000001.SZ\n", encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate symbol"):
        load_symbols_file(path)
    path.write_text("000001.sz\n", encoding="utf-8")
    with pytest.raises(ValueError, match="canonical SH/SZ"):
        load_symbols_file(path)


def test_history_backfill_parses_exact_stop_at_for_every_timeframe() -> None:
    cutoffs = parse_stop_at(
        "5f=2026-07-17T07:00:00Z,30f=2026-07-17T07:00:00+00:00",
        ["5f", "30f"],
    )

    assert cutoffs == {
        "5f": datetime(2026, 7, 17, 7, 0, tzinfo=UTC),
        "30f": datetime(2026, 7, 17, 7, 0, tzinfo=UTC),
    }
    with pytest.raises(ValueError, match="missing timeframes"):
        parse_stop_at("5f=2026-07-17T07:00:00Z", ["5f", "30f"])
    with pytest.raises(ValueError, match="Shanghai 15:00"):
        parse_stop_at(
            "5f=2026-07-17T07:01:00Z", ["5f"], canonical_tail_labels=True
        )


def test_history_backfill_provider_page_prefers_native_paging() -> None:
    class FakeProvider:
        def __init__(self) -> None:
            self.calls = []

        async def get_bars_page(self, symbol, timeframe, *, offset, limit):
            self.calls.append((symbol, timeframe, offset, limit))
            return [_fake_bar(symbol, timeframe, offset)]

    provider = FakeProvider()
    bars = asyncio.run(
        get_provider_page(
            provider,
            symbol="000001.SZ",
            timeframe="5f",
            offset=7,
            limit=3,
        )
    )
    assert provider.calls == [("000001.SZ", "5f", 7, 3)]
    assert bars[0].close == 7


def test_history_backfill_process_task_advances_offsets_until_exhausted() -> None:
    class FakeProvider:
        async def get_bars_page(self, symbol, timeframe, *, offset, limit):
            if offset == 0:
                return [_fake_bar(symbol, timeframe, index) for index in range(3)]
            if offset == 3:
                return [_fake_bar(symbol, timeframe, 3)]
            return []

    class FakeKlineWriter:
        def __init__(self) -> None:
            self.written = []
            self.records = []

        async def commit_history_backfill_page(self, **kwargs):
            bars = kwargs["bars"]
            self.written.extend(bars)
            self.records.append(kwargs)
            return len(bars)

    class FakeTaskStore:
        def __init__(self) -> None:
            self.yields = []

        async def heartbeat(self, **_kwargs):
            return True

        async def yield_task(self, **kwargs):
            self.yields.append(kwargs)
            return True

        async def record_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    writer = FakeKlineWriter()
    task_store = FakeTaskStore()
    result = asyncio.run(
        process_history_task(
            provider=FakeProvider(),
            kline_writer=writer,
            task_store=task_store,
            task={
                "id": 1,
                "symbol": "000001.SZ",
                "timeframe": 5,
                "page_size": 3,
                "next_offset": 0,
                "claim_token": "claim-a",
                "lease_version": 1,
            },
            max_pages_per_task=0,
            sleep=0,
            lease_seconds=300,
        )
    )
    assert result == {"pages": 2, "bars": 4}
    assert len(writer.written) == 4
    assert [item["next_offset"] for item in writer.records] == [3, 4]
    assert [item["exhausted"] for item in writer.records] == [False, True]
    assert task_store.yields == []


def test_history_backfill_advances_by_raw_rows_when_pytdx_filters_a_duplicate() -> None:
    class FakeProvider:
        def __init__(self) -> None:
            self.offsets = []

        async def get_bars_page_with_raw_count(self, symbol, timeframe, *, offset, limit):
            self.offsets.append(offset)
            if offset == 0:
                return ([_fake_bar(symbol, timeframe, 0), _fake_bar(symbol, timeframe, 1)], 3)
            if offset == 3:
                return ([_fake_bar(symbol, timeframe, 3)], 1)
            return ([], 0)

    class FakeKlineWriter:
        def __init__(self) -> None:
            self.records = []

        async def commit_history_backfill_page(self, **kwargs):
            self.records.append(kwargs)
            return len(kwargs["bars"])

    class FakeTaskStore:
        async def heartbeat(self, **_kwargs):
            return True

        async def yield_task(self, **_kwargs):
            raise AssertionError("exhausted task must not yield")

        async def record_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    provider = FakeProvider()
    writer = FakeKlineWriter()
    result = asyncio.run(
        process_history_task(
            provider=provider,
            kline_writer=writer,
            task_store=FakeTaskStore(),
            task={
                "id": 10,
                "symbol": "000001.SZ",
                "timeframe": 5,
                "page_size": 3,
                "next_offset": 0,
                "claim_token": "claim-raw-offset",
                "lease_version": 1,
            },
            max_pages_per_task=0,
            sleep=0,
            lease_seconds=300,
        )
    )

    assert result == {"pages": 2, "bars": 3}
    assert provider.offsets == [0, 3]
    assert [item["next_offset"] for item in writer.records] == [3, 4]
    assert [item["exhausted"] for item in writer.records] == [False, True]


def test_history_backfill_commits_progress_for_fully_filtered_raw_page() -> None:
    class FakeProvider:
        def __init__(self) -> None:
            self.offsets = []

        async def get_bars_page_with_raw_count(self, symbol, timeframe, *, offset, limit):
            self.offsets.append(offset)
            if offset == 0:
                return ([], 2)
            if offset == 2:
                return ([_fake_bar(symbol, timeframe, 2)], 1)
            return ([], 0)

    class FakeKlineWriter:
        def __init__(self) -> None:
            self.records = []

        async def commit_history_backfill_page(self, **kwargs):
            self.records.append(kwargs)
            return len(kwargs["bars"])

    class FakeTaskStore:
        async def heartbeat(self, **_kwargs):
            return True

        async def yield_task(self, **_kwargs):
            raise AssertionError("exhausted task must not yield")

        async def record_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    provider = FakeProvider()
    writer = FakeKlineWriter()
    result = asyncio.run(
        process_history_task(
            provider=provider,
            kline_writer=writer,
            task_store=FakeTaskStore(),
            task={
                "id": 11,
                "symbol": "000001.SZ",
                "timeframe": 5,
                "page_size": 2,
                "next_offset": 0,
                "claim_token": "claim-empty-page",
                "lease_version": 1,
            },
            max_pages_per_task=0,
            sleep=0,
            lease_seconds=300,
        )
    )

    assert result == {"pages": 2, "bars": 1}
    assert provider.offsets == [0, 2]
    assert [item["next_offset"] for item in writer.records] == [2, 3]
    assert writer.records[0]["bars"] == []
    assert [item["exhausted"] for item in writer.records] == [False, True]


def test_history_backfill_processes_tasks_with_bounded_concurrency() -> None:
    class Monitor:
        active = 0
        max_active = 0
        created = 0

    class FakeProvider:
        def __init__(self) -> None:
            Monitor.created += 1

        async def get_bars_page(self, symbol, timeframe, *, offset, limit):
            Monitor.active += 1
            Monitor.max_active = max(Monitor.max_active, Monitor.active)
            await asyncio.sleep(0.01)
            Monitor.active -= 1
            return [_fake_bar(symbol, timeframe, offset)]

    class FakeKlineWriter:
        async def commit_history_backfill_page(self, **kwargs):
            bars = kwargs["bars"]
            return len(bars)

    class FakeTaskStore:
        def __init__(self) -> None:
            self.yields = []

        async def heartbeat(self, **_kwargs):
            return True

        async def yield_task(self, **kwargs):
            self.yields.append(kwargs)
            return True

        async def record_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    task_store = FakeTaskStore()
    tasks = [
        {
            "id": index + 1,
            "symbol": f"00000{index + 1}.SZ",
            "timeframe": 5,
            "page_size": 2,
            "next_offset": 0,
            "claim_token": f"claim-{index}",
            "lease_version": 1,
        }
        for index in range(3)
    ]
    result = asyncio.run(
        process_history_tasks_concurrently(
            provider_factory=FakeProvider,
            kline_writer=FakeKlineWriter(),
            task_store=task_store,
            tasks=tasks,
            concurrency=2,
            max_pages_per_task=1,
            sleep=0,
            lease_seconds=300,
        )
    )
    assert result == {"pages": 3, "bars": 3}
    assert Monitor.created == 3
    assert Monitor.max_active == 2
    assert task_store.yields == []


def test_history_backfill_keeps_ownership_until_an_explicit_yield() -> None:
    class FakeProvider:
        async def get_bars_page(self, symbol, timeframe, *, offset, limit):
            return [_fake_bar(symbol, timeframe, offset)]

    class FakeKlineWriter:
        def __init__(self) -> None:
            self.statuses = []

        async def commit_history_backfill_page(self, **kwargs):
            self.statuses.append((kwargs["expected_offset"], kwargs["exhausted"]))
            return len(kwargs["bars"])

    class FakeTaskStore:
        def __init__(self) -> None:
            self.yields = []

        async def heartbeat(self, **_kwargs):
            return True

        async def yield_task(self, **kwargs):
            self.yields.append(kwargs)
            return True

        async def record_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    writer = FakeKlineWriter()
    store = FakeTaskStore()
    result = asyncio.run(
        process_history_task(
            provider=FakeProvider(),
            kline_writer=writer,
            task_store=store,
            task={
                "id": 9,
                "symbol": "000009.SZ",
                "timeframe": 5,
                "page_size": 1,
                "next_offset": 0,
                "claim_token": "claim-yield",
                "lease_version": 4,
            },
            max_pages_per_task=2,
            sleep=0,
            lease_seconds=300,
        )
    )
    assert result == {"pages": 2, "bars": 2}
    assert writer.statuses == [(0, False), (1, False)]
    assert store.yields == [
        {"task_id": 9, "claim_token": "claim-yield", "lease_version": 4}
    ]


def test_history_backfill_stop_at_writes_only_tail_and_completes() -> None:
    cutoff = datetime(2026, 7, 17, 7, 0, tzinfo=UTC)

    class FakeProvider:
        async def get_bars_page(self, symbol, timeframe, *, offset, limit):
            return [
                _fake_bar_at(symbol, timeframe, cutoff.replace(hour=6)),
                _fake_bar_at(symbol, timeframe, cutoff.replace(hour=8)),
            ]

    class FakeKlineWriter:
        def __init__(self) -> None:
            self.record = None

        async def commit_history_backfill_page(self, **kwargs):
            self.record = kwargs
            return len(kwargs["bars"])

    class FakeTaskStore:
        async def heartbeat(self, **_kwargs):
            return True

        async def yield_task(self, **_kwargs):
            raise AssertionError("stop-at task must complete, not yield")

        async def record_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    writer = FakeKlineWriter()
    result = asyncio.run(
        process_history_task(
            provider=FakeProvider(),
            kline_writer=writer,
            task_store=FakeTaskStore(),
            task={
                "id": 12, "symbol": "000001.SZ", "timeframe": 5,
                "page_size": 2, "next_offset": 0,
                "claim_token": "claim-stop", "lease_version": 1,
            },
            max_pages_per_task=1,
            sleep=0,
            lease_seconds=300,
            stop_at=cutoff,
        )
    )

    assert result == {"pages": 1, "bars": 1}
    assert [bar.ts for bar in writer.record["bars"]] == [cutoff.replace(hour=8)]
    assert writer.record["exhausted"] is True


def test_history_backfill_stop_at_yields_until_page_reaches_boundary() -> None:
    cutoff = datetime(2026, 7, 17, 7, 0, tzinfo=UTC)

    class FakeProvider:
        async def get_bars_page(self, symbol, timeframe, *, offset, limit):
            return [_fake_bar_at(symbol, timeframe, cutoff.replace(hour=8))]

    class FakeKlineWriter:
        def __init__(self) -> None:
            self.records = []

        async def commit_history_backfill_page(self, **kwargs):
            self.records.append(kwargs)
            return len(kwargs["bars"])

    class FakeTaskStore:
        def __init__(self) -> None:
            self.yields = []

        async def heartbeat(self, **_kwargs):
            return True

        async def yield_task(self, **kwargs):
            self.yields.append(kwargs)
            return True

        async def record_failure(self, **kwargs):
            raise AssertionError(f"unexpected failure: {kwargs}")

    writer = FakeKlineWriter()
    store = FakeTaskStore()
    asyncio.run(
        process_history_task(
            provider=FakeProvider(), kline_writer=writer, task_store=store,
            task={
                "id": 13, "symbol": "000001.SZ", "timeframe": 5,
                "page_size": 1, "next_offset": 0,
                "claim_token": "claim-no-stop", "lease_version": 1,
            },
            max_pages_per_task=1, sleep=0, lease_seconds=300, stop_at=cutoff,
        )
    )

    assert [bar.ts for bar in writer.records[0]["bars"]] == [cutoff.replace(hour=8)]
    assert writer.records[0]["exhausted"] is False
    assert store.yields == [
        {"task_id": 13, "claim_token": "claim-no-stop", "lease_version": 1}
    ]


def test_scoped_history_backfill_empty_first_page_fails_before_kline_write() -> None:
    cutoff = datetime(2026, 7, 10, 7, tzinfo=UTC)
    target = datetime(2026, 7, 17, 7, tzinfo=UTC)

    class FakeProvider:
        async def get_bars_page_with_raw_count(self, *_args, **_kwargs):
            return [], 0

    class FakeWriter:
        async def commit_history_backfill_page(self, **_kwargs):
            raise AssertionError("unproven scoped source must not write K-lines")

    class FakeStore:
        def __init__(self):
            self.failures = []

        async def heartbeat(self, **_kwargs):
            return True

        async def record_failure(self, **kwargs):
            self.failures.append(kwargs)
            return True

    store = FakeStore()
    result = asyncio.run(process_history_task(
        provider=FakeProvider(), kline_writer=FakeWriter(), task_store=store,
        task={
            "id": 14, "symbol": "000001.SZ", "timeframe": 5,
            "page_size": 260, "next_offset": 0, "claim_token": "empty",
            "lease_version": 1, "run_id": "run-a", "stop_at": cutoff,
            "expected_through": target, "provider_newest_ts": None,
        },
        max_pages_per_task=1, sleep=0, lease_seconds=300,
        stop_at=cutoff, expected_through=target, run_id="run-a",
    ))
    assert result == {"pages": 0, "bars": 0, "failed": 1, "lease_lost": 0}
    assert "expected-through" in store.failures[0]["error"]


def test_scoped_history_backfill_provider_exhaustion_before_stop_fails() -> None:
    cutoff = datetime(2026, 7, 10, 7, tzinfo=UTC)
    target = datetime(2026, 7, 17, 7, tzinfo=UTC)

    class FakeProvider:
        async def get_bars_page_with_raw_count(self, symbol, timeframe, **_kwargs):
            return [_fake_bar_at(symbol, timeframe, target)], 1

    class FakeWriter:
        async def commit_history_backfill_page(self, **_kwargs):
            raise AssertionError("incomplete scoped history must not commit completion")

    class FakeStore:
        def __init__(self):
            self.failures = []

        async def heartbeat(self, **_kwargs):
            return True

        async def record_failure(self, **kwargs):
            self.failures.append(kwargs)
            return True

    store = FakeStore()
    result = asyncio.run(process_history_task(
        provider=FakeProvider(), kline_writer=FakeWriter(), task_store=store,
        task={
            "id": 15, "symbol": "000001.SZ", "timeframe": 5,
            "page_size": 260, "next_offset": 0, "claim_token": "short",
            "lease_version": 1, "run_id": "run-b", "stop_at": cutoff,
            "expected_through": target, "provider_newest_ts": None,
        },
        max_pages_per_task=1, sleep=0, lease_seconds=300,
        stop_at=cutoff, expected_through=target, run_id="run-b",
    ))
    assert result["failed"] == 1
    assert "before stop-at" in store.failures[0]["error"]


def _fake_bar(symbol: str, timeframe: str, index: int) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe=timeframe,
        ts=datetime.fromtimestamp(index + 1, tz=UTC),
        open=float(index),
        high=float(index) + 0.5,
        low=float(index) - 0.5,
        close=float(index),
        volume=100 + index,
        source="pytdx",
    )


def _fake_bar_at(symbol: str, timeframe: str, timestamp: datetime) -> Bar:
    return Bar(
        symbol=symbol, timeframe=timeframe, ts=timestamp,
        open=1.0, high=1.1, low=0.9, close=1.0,
        volume=100, source="pytdx",
    )

