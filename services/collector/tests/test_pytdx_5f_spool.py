from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path

from collector import pytdx_5f_spool as spool
from trading_protocol import Bar, SymbolInfo


def _bar(ts: str, close: float = 10.0) -> Bar:
    return Bar(
        symbol="605123.SH",
        timeframe="5f",
        ts=datetime.fromisoformat(ts).replace(tzinfo=spool.SHANGHAI_TZ),
        open=close,
        high=close + 0.1,
        low=close - 0.1,
        close=close,
        volume=100,
        amount=1000.0,
        source="pytdx",
    )


def test_bars_to_parquet_rows_uses_trade_time_bar_end_without_timezone() -> None:
    rows = spool.bars_to_parquet_rows([_bar("2026-04-20T09:35:00", close=99.1)])

    assert rows == [
        {
            "code": "605123",
            "trade_time": datetime(2026, 4, 20, 9, 35),
            "open": 99.1,
            "high": 99.19999999999999,
            "low": 99.0,
            "close": 99.1,
            "vol": 100,
            "amount": 1000.0,
        }
    ]


def test_output_and_checkpoint_paths_are_symbol_scoped(tmp_path: Path) -> None:
    assert spool.symbol_output_file(tmp_path, "605123.SH") == tmp_path / "symbols" / "SH" / "605123.parquet"
    assert spool.checkpoint_file(tmp_path, "605123.SH") == tmp_path / "checkpoints" / "SH" / "605123.json"


def test_spool_symbol_filters_window_and_writes_checkpoint(tmp_path: Path) -> None:
    class FakeProvider:
        async def get_bars_page(self, symbol: str, timeframe: str, *, offset: int, limit: int):
            assert symbol == "605123.SH"
            assert timeframe == "5f"
            if offset == 0:
                return [
                    _bar("2026-04-20T09:35:00", 99.1),
                    _bar("2026-04-20T09:40:00", 99.2),
                ]
            return [_bar("2026-04-17T15:00:00", 98.0)]

    result = asyncio.run(
        spool.spool_symbol(
            provider=FakeProvider(),
            symbol=SymbolInfo(symbol="605123.SH", code="605123", exchange="SH", name="威龙股份"),
            timeframe="5f",
            start_dt=spool.parse_boundary("2026-04-18", end_of_day=False),
            end_dt=spool.parse_boundary("2026-04-30", end_of_day=True),
            output_root=tmp_path,
            page_size=2,
            max_pages_per_symbol=0,
            sleep=0,
            reset=False,
        )
    )

    assert result.bars == 2
    assert result.pages == 2
    assert result.oldest_ts == datetime(2026, 4, 20, 9, 35, tzinfo=spool.SHANGHAI_TZ)
    assert result.newest_ts == datetime(2026, 4, 20, 9, 40, tzinfo=spool.SHANGHAI_TZ)
    assert result.output_path is not None
    assert result.output_path.exists()

    checkpoint = json.loads((tmp_path / "checkpoints" / "SH" / "605123.json").read_text(encoding="utf-8"))
    assert checkpoint["status"] == "success"
    assert checkpoint["bars"] == 2
