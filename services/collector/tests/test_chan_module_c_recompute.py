from __future__ import annotations

import asyncio
import inspect
from datetime import UTC, datetime
from pathlib import Path
import sys
from types import SimpleNamespace

from collector.chan_module_c_recompute import (
    MODULE_C_CONFIG_HASH,
    aggregate_from_5f,
    claim_recompute_task,
    filter_completed_symbols,
    is_module_c_complete,
    parse_args,
    process_claimed_task,
)
import collector.chan_module_c_recompute as recompute
from collector.module_c_adapter import DEFAULT_CHAN_CONFIG
from trading_protocol import Bar


def _bar(value: int, *, hour: int = 1, minute: int, price: float) -> Bar:
    return Bar(
        symbol="000001.SZ",
        timeframe="5f",
        ts=datetime(2026, 7, 3, hour, minute, tzinfo=UTC),
        open=price,
        high=price + 0.2,
        low=price - 0.2,
        close=price + 0.1,
        volume=value,
        amount=float(value * 10),
        complete=True,
        revision=1,
        source="pytdx",
    )


def test_module_c_prepare_aggregates_30f_to_market_session_grid() -> None:
    bars = [
        _bar(100, minute=35, price=10.0),
        _bar(200, minute=40, price=10.1),
        _bar(300, minute=45, price=10.2),
        _bar(400, minute=50, price=10.3),
        _bar(500, minute=55, price=10.4),
        _bar(600, hour=2, minute=0, price=10.5),
    ]

    result = aggregate_from_5f(symbol="000001.SZ", source_bars=bars, target_timeframe="30f")

    assert len(result) == 1
    assert result[0].ts == datetime(2026, 7, 3, 2, 0, tzinfo=UTC)
    assert result[0].timeframe == "30f"
    assert result[0].open == 10.0
    assert result[0].close == 10.6
    assert result[0].volume == 2100
    assert result[0].source == "derived_5f"


def test_module_c_config_hash_identifies_no_sub_peak_semantics() -> None:
    assert MODULE_C_CONFIG_HASH == "module-c:native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false"
    assert "bi-allow-sub-peak-false" in MODULE_C_CONFIG_HASH
    assert MODULE_C_CONFIG_HASH != "module-c:native-5lvl-v3-bi-strict-false"


def test_module_c_cli_parses_defaults(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["chan_module_c_recompute"])
    args = parse_args()
    assert args.chan_levels == "5f,30f,1d,1w,1m"
    assert args.symbol_limit == 10


def test_collector_owned_adapter_disables_sub_peak_strokes() -> None:
    assert DEFAULT_CHAN_CONFIG["bi_allow_sub_peak"] is False
    assert DEFAULT_CHAN_CONFIG["bs_type"] == "1,2,3a,3b"
    assert "bsp3_type" not in DEFAULT_CHAN_CONFIG


def test_module_c_prepare_aggregates_1d_to_close_time() -> None:
    bars = [
        _bar(100, minute=35, price=10.0),
        _bar(200, minute=40, price=10.1),
    ]

    result = aggregate_from_5f(symbol="000001.SZ", source_bars=bars, target_timeframe="1d")

    assert len(result) == 1
    assert result[0].ts == datetime(2026, 7, 3, 7, 0, tzinfo=UTC)
    assert result[0].timeframe == "1d"
    assert result[0].open == 10.0
    assert result[0].close == 10.2


class _FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _FakePool:
    def __init__(self, rows):
        self.conn = _FakeConn(rows)

    def acquire(self):
        return _FakeAcquire(self.conn)


class _FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def fetch(self, query, *args):
        self.calls.append((query, args))
        if callable(self.rows):
            return self.rows(query, args)
        return self.rows

    async def fetchrow(self, query, *args):
        self.calls.append((query, args))
        rows = self.rows(query, args) if callable(self.rows) else self.rows
        return rows[0] if rows else None

    async def execute(self, query, *args):
        self.calls.append((query, args))
        return "UPDATE 1"


def test_module_c_filter_completed_symbols_keeps_only_incomplete_symbols() -> None:
    pool = _FakePool(_complete_rows())
    kline_writer = SimpleNamespace(_pool=pool)

    result = asyncio.run(
        filter_completed_symbols(
            kline_writer=kline_writer,
            symbols=["000001.SZ", "000002.SZ"],
            levels=["5f", "30f"],
            modes=["confirmed", "predictive"],
        )
    )

    assert result == ["000002.SZ"]
    _query, args = pool.conn.calls[0]
    assert args[0] == ["000001.SZ", "000002.SZ"]
    assert args[1] == [5, 30]
    assert args[2] == ["confirmed", "predictive"]
    assert len(args) == 3


def _complete_rows(*, config_hash: str = MODULE_C_CONFIG_HASH, symbol: str = "000001.SZ") -> list[dict[str, object]]:
    return [
        {
            "symbol": symbol,
            "chan_level": level,
            "base_timeframe": level,
            "mode": mode,
            "head_status": "published",
            "run_status": "success",
            "config_hash": config_hash,
        }
        for level in (5, 30)
        for mode in ("confirmed", "predictive")
    ]


def test_module_c_completeness_requires_every_current_native_successful_head() -> None:
    rows = _complete_rows()
    assert is_module_c_complete(rows, levels=[5, 30], modes=["confirmed", "predictive"])

    invalid_cases = [
        _complete_rows(config_hash="module-c:old-config"),
        _complete_rows()[1:],
        [{**row, "run_status": "failed"} if row["mode"] == "confirmed" else row for row in _complete_rows()],
        [{**row, "mode": "wrong"} if row["mode"] == "confirmed" else row for row in _complete_rows()],
        [{**row, "chan_level": 15, "base_timeframe": 15} if row["chan_level"] == 5 else row for row in _complete_rows()],
        [{**row, "base_timeframe": 5} if row["chan_level"] == 30 else row for row in _complete_rows()],
    ]
    for rows in invalid_cases:
        assert not is_module_c_complete(rows, levels=[5, 30], modes=["confirmed", "predictive"])


def test_module_c_filter_completed_symbols_recomputes_old_config_heads() -> None:
    kline_writer = SimpleNamespace(_pool=_FakePool(_complete_rows(config_hash="module-c:old-config")))

    result = asyncio.run(
        filter_completed_symbols(
            kline_writer=kline_writer,
            symbols=["000001.SZ"],
            levels=["5f", "30f"],
            modes=["confirmed", "predictive"],
        )
    )

    assert result == ["000001.SZ"]


def test_module_c_filter_completed_symbols_skips_current_config_heads() -> None:
    kline_writer = SimpleNamespace(_pool=_FakePool(_complete_rows()))

    result = asyncio.run(
        filter_completed_symbols(
            kline_writer=kline_writer,
            symbols=["000001.SZ"],
            levels=["5f", "30f"],
            modes=["confirmed", "predictive"],
        )
    )

    assert result == []


def test_full_recompute_claim_is_sharded_leased_and_attempt_bounded() -> None:
    row = {
        "batch_id": 11,
        "symbol_id": 22,
        "chan_level": 30,
        "claim_token": "token",
        "lease_version": 3,
    }
    pool = _FakePool([row])
    kline_writer = SimpleNamespace(_pool=pool)

    result = asyncio.run(
        claim_recompute_task(
            kline_writer=kline_writer,
            batch_id=11,
            worker_id="worker-2",
            shard_index=2,
            shard_count=4,
            lease_seconds=900,
            max_attempts=3,
        )
    )

    assert result == row
    query, args = pool.conn.calls[0]
    assert "for update skip locked" in query.lower()
    assert "lease_version = task.lease_version + 1" in query
    assert args == (11, "worker-2", 900, 2, 4, 3)


def test_batch_init_casts_config_hash_for_asyncpg() -> None:
    sql = inspect.getsource(recompute.ensure_recompute_batch)
    assert "$4::varchar" in sql
    assert "build.config_hash = $4::text" in sql


def test_claimed_task_uses_one_level_and_frozen_cutoff(monkeypatch) -> None:
    cutoff = datetime(2026, 7, 10, 7, tzinfo=UTC)
    bars = [
        Bar(
            symbol="000001.SZ", timeframe="1d",
            ts=datetime(2026, 7, day, 7, tzinfo=UTC),
            open=10, high=11, low=9, close=10.5, volume=100,
            amount=1000, complete=True, revision=1, source="parquet",
        )
        for day in (8, 9, 10, 11)
    ]

    class KlineWriter:
        async def get_bars(self, symbol, level):
            assert (symbol, level) == ("000001.SZ", "1d")
            return bars

    class ChanWriter:
        def __init__(self):
            self.kwargs = None

        async def replace_analysis(self, **kwargs):
            self.kwargs = kwargs
            return {"strokes": 0, "segments": 0, "centers": 0, "signals": 0}

    async def fake_compute(**kwargs):
        assert kwargs["levels"] == ["1d"]
        assert [bar.ts for bar in kwargs["bars_by_level"]["1d"]] == [bar.ts for bar in bars[:3]]
        return {
            "engine": "module-c:chan.py-native-levels",
            "snapshot_version": "frozen",
            "strokes": [], "segments": [], "centers": [], "signals": [], "channels": [],
        }

    async def fake_heartbeat(**_kwargs):
        return True

    monkeypatch.setattr(recompute, "compute_module_c_overlay", fake_compute)
    monkeypatch.setattr(recompute, "heartbeat_recompute_task", fake_heartbeat)
    writer = ChanWriter()
    task = {
        "batch_id": 11, "symbol_id": 22, "symbol": "000001.SZ",
        "chan_level": 1440, "target_bar_until": cutoff,
        "claim_token": "token", "lease_version": 1,
    }

    asyncio.run(
        process_claimed_task(
            kline_writer=KlineWriter(), chan_writer=writer, task=task,
            modes=["confirmed", "predictive"], lease_seconds=30,
            chan_py_path="unused",
        )
    )

    assert writer.kwargs["level"] == "1d"
    assert writer.kwargs["bar_until"] == cutoff
    assert writer.kwargs["bar_count"] == 3
    assert writer.kwargs["full_recompute_task"] is task


def test_migration_035_contains_provenance_and_fenced_batch_tasks() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "db" / "sql" / "035_chan_c_full_recompute_tasks.sql"
    ).read_text(encoding="utf-8")

    assert "add column if not exists batch_id bigint" in migration
    assert "create table if not exists chan_c_full_recompute_batches" in migration
    assert "create table if not exists chan_c_full_recompute_tasks" in migration
    assert "claim_token varchar(64)" in migration
    assert "lease_version bigint" in migration
    assert "target_bar_until timestamptz" in migration
