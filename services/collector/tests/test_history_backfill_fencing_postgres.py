"""Opt-in PostgreSQL acceptance for historical backfill ownership fencing.

The database must be disposable and already contain migrations 001..044.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from collector.storage.backfill_postgres import PostgresBackfillTaskStore
from collector.storage.postgres import LostBackfillLease, PostgresKlineWriter
from trading_protocol import Bar


TEST_DATABASE_URL = os.getenv("HISTORY_BACKFILL_FENCING_TEST_DATABASE_URL", "")


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="set HISTORY_BACKFILL_FENCING_TEST_DATABASE_URL for a disposable migrated PostgreSQL database",
)
def test_stale_backfill_owner_has_no_page_or_terminal_side_effects() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        setup = await asyncpg.connect(TEST_DATABASE_URL)
        code = f"HB{uuid4().hex[:10].upper()}"
        symbol = f"{code}.TS"
        symbol_id = await setup.fetchval(
            """insert into symbols(code, exchange, name, asset_type, market, is_active)
               values($1, 'TS', $1, 'stock', 'test', true) returning id""",
            code,
        )
        try:
            await setup.execute(
                """insert into historical_backfill_tasks(
                       symbol_id, timeframe, provider, page_size, max_attempts
                   ) values($1, 5, 'pytdx', 1, 3)""",
                symbol_id,
            )
            async with PostgresBackfillTaskStore(TEST_DATABASE_URL) as store:
                async with PostgresKlineWriter(TEST_DATABASE_URL) as writer:
                    claimed_a = await store.claim_tasks(
                        provider="pytdx", limit=1, worker_id="worker-a",
                        lease_seconds=30, max_attempts=3,
                    )
                    assert len(claimed_a) == 1
                    task_a = claimed_a[0]
                    before_scope_rejections = await _snapshot(
                        setup, symbol_id, task_a["id"]
                    )

                    unknown_scope_task = dict(task_a)
                    unknown_scope_task["symbol"] = "UNKNOWN.TS"
                    with pytest.raises(ValueError, match="claimed scope mismatch"):
                        await writer.commit_history_backfill_page(
                            task=unknown_scope_task, expected_offset=0, next_offset=1,
                            bars=[], oldest_ts=None, newest_ts=None,
                            exhausted=True, lease_seconds=30,
                        )
                    assert (
                        await _snapshot(setup, symbol_id, task_a["id"])
                        == before_scope_rejections
                    )

                    wrong_symbol = _bar("UNKNOWN.TS", 1)
                    with pytest.raises(ValueError, match="page scope mismatch"):
                        await writer.commit_history_backfill_page(
                            task=task_a, expected_offset=0, next_offset=1,
                            bars=[wrong_symbol], oldest_ts=wrong_symbol.ts,
                            newest_ts=wrong_symbol.ts, exhausted=False,
                            lease_seconds=30,
                        )
                    assert (
                        await _snapshot(setup, symbol_id, task_a["id"])
                        == before_scope_rejections
                    )

                    wrong_timeframe = _bar(symbol, 1, timeframe="15f")
                    with pytest.raises(ValueError, match="page scope mismatch"):
                        await writer.commit_history_backfill_page(
                            task=task_a, expected_offset=0, next_offset=1,
                            bars=[wrong_timeframe], oldest_ts=wrong_timeframe.ts,
                            newest_ts=wrong_timeframe.ts, exhausted=False,
                            lease_seconds=30,
                        )
                    assert (
                        await _snapshot(setup, symbol_id, task_a["id"])
                        == before_scope_rejections
                    )

                    first = _bar(symbol, 1)
                    await writer.commit_history_backfill_page(
                        task=task_a, expected_offset=0, next_offset=1,
                        bars=[first], oldest_ts=first.ts, newest_ts=first.ts,
                        exhausted=False, lease_seconds=30,
                    )
                    row = await setup.fetchrow(
                        "select status, next_offset from historical_backfill_tasks where id=$1",
                        task_a["id"],
                    )
                    assert (row["status"], row["next_offset"]) == ("running", 1)
                    assert await store.reset_running(provider="pytdx") == 0

                    await setup.execute(
                        "update historical_backfill_tasks set lease_until=clock_timestamp()-interval '1 second' where id=$1",
                        task_a["id"],
                    )
                    claimed_b = await store.claim_tasks(
                        provider="pytdx", limit=1, worker_id="worker-b",
                        lease_seconds=30, max_attempts=3,
                    )
                    assert len(claimed_b) == 1
                    task_b = claimed_b[0]
                    assert task_b["lease_version"] > task_a["lease_version"]
                    second = _bar(symbol, 2)
                    await writer.commit_history_backfill_page(
                        task=task_b, expected_offset=1, next_offset=2,
                        bars=[second], oldest_ts=second.ts, newest_ts=second.ts,
                        exhausted=False, lease_seconds=30,
                    )
                    third = _bar(symbol, 3)
                    await writer.commit_history_backfill_page(
                        task=task_b, expected_offset=2, next_offset=3,
                        bars=[third], oldest_ts=third.ts, newest_ts=third.ts,
                        exhausted=False, lease_seconds=30,
                    )
                    before = await _snapshot(setup, symbol_id, task_b["id"])

                    stale = _bar(symbol, 4)
                    with pytest.raises(LostBackfillLease):
                        await writer.commit_history_backfill_page(
                            task=task_a, expected_offset=1, next_offset=2,
                            bars=[stale], oldest_ts=stale.ts, newest_ts=stale.ts,
                            exhausted=True, lease_seconds=30,
                        )
                    assert not await store.record_failure(
                        task_id=task_a["id"], claim_token=task_a["claim_token"],
                        lease_version=task_a["lease_version"], error="late-a",
                    )
                    assert await _snapshot(setup, symbol_id, task_b["id"]) == before

                    final = _bar(symbol, 5)
                    await writer.commit_history_backfill_page(
                        task=task_b, expected_offset=3, next_offset=4,
                        bars=[final], oldest_ts=final.ts, newest_ts=final.ts,
                        exhausted=True, lease_seconds=30,
                    )
                    assert not await store.record_failure(
                        task_id=task_a["id"], claim_token=task_a["claim_token"],
                        lease_version=task_a["lease_version"], error="late-terminal-a",
                    )
                    terminal = await setup.fetchrow(
                        "select status, next_offset, pages_done from historical_backfill_tasks where id=$1",
                        task_b["id"],
                    )
                    assert (terminal["status"], terminal["next_offset"], terminal["pages_done"]) == (
                        "success", 4, 4,
                    )

                    await setup.execute(
                        """insert into historical_backfill_tasks(
                               symbol_id, timeframe, provider, page_size, max_attempts
                           ) values($1, 15, 'pytdx-reset', 1, 3)""",
                        symbol_id,
                    )
                    reset_candidate = (await store.claim_tasks(
                        provider="pytdx-reset", limit=1, worker_id="worker-reset",
                        lease_seconds=30, max_attempts=3,
                    ))[0]
                    await setup.execute(
                        "update historical_backfill_tasks set lease_until=clock_timestamp()-interval '1 second' where id=$1",
                        reset_candidate["id"],
                    )
                    assert await store.reset_running(provider="pytdx-reset") == 1
                    reset_row = await setup.fetchrow(
                        "select status, worker_id, claim_token, lease_until from historical_backfill_tasks where id=$1",
                        reset_candidate["id"],
                    )
                    assert tuple(reset_row.values()) == ("pending", None, None, None)

                    await setup.execute(
                        """insert into historical_backfill_tasks(
                               symbol_id, timeframe, provider, page_size, max_attempts
                           ) values($1, 5, 'pytdx-midtx', 1, 3)""",
                        symbol_id,
                    )
                    midtx = (await store.claim_tasks(
                        provider="pytdx-midtx", limit=1, worker_id="worker-midtx",
                        lease_seconds=30, max_attempts=3,
                    ))[0]
                    before_midtx = await _snapshot(setup, symbol_id, midtx["id"])
                    original_upsert = writer._upsert_bars_rows

                    async def expire_after_writes(conn, rows):
                        await original_upsert(conn, rows)
                        await conn.execute(
                            "update historical_backfill_tasks set lease_until=clock_timestamp()-interval '1 second' where id=$1",
                            midtx["id"],
                        )

                    writer._upsert_bars_rows = expire_after_writes
                    rolled_back_bar = _bar(symbol, 6)
                    try:
                        with pytest.raises(LostBackfillLease):
                            await writer.commit_history_backfill_page(
                                task=midtx, expected_offset=0, next_offset=1,
                                bars=[rolled_back_bar], oldest_ts=rolled_back_bar.ts,
                                newest_ts=rolled_back_bar.ts, exhausted=False,
                                lease_seconds=30,
                            )
                    finally:
                        writer._upsert_bars_rows = original_upsert
                    assert await _snapshot(setup, symbol_id, midtx["id"]) == before_midtx

                    await setup.execute(
                        """insert into historical_backfill_tasks(
                               symbol_id, timeframe, provider, page_size, max_attempts
                           ) values($1, 30, 'pytdx-max-one', 1, 1)""",
                        symbol_id,
                    )
                    one = (await store.claim_tasks(
                        provider="pytdx-max-one", limit=1, worker_id="worker-one",
                        lease_seconds=30, max_attempts=1,
                    ))[0]
                    await setup.execute(
                        "update historical_backfill_tasks set lease_until=clock_timestamp()-interval '1 second' where id=$1",
                        one["id"],
                    )
                    assert await store.claim_tasks(
                        provider="pytdx-max-one", limit=1, worker_id="worker-two",
                        lease_seconds=30, max_attempts=1,
                    ) == []
                    exhausted = await setup.fetchrow(
                        "select status, attempts from historical_backfill_tasks where id=$1",
                        one["id"],
                    )
                    assert (exhausted["status"], exhausted["attempts"]) == (
                        "dead_letter", 1,
                    )
        finally:
            await setup.execute(
                "delete from historical_backfill_tasks where symbol_id=$1", symbol_id
            )
            await setup.execute("delete from klines where symbol_id=$1", symbol_id)
            await setup.execute("delete from kline_source_coverage where symbol_id=$1", symbol_id)
            await setup.execute("delete from symbols where id=$1", symbol_id)
            await setup.close()

    asyncio.run(scenario())


async def _snapshot(conn, symbol_id: int, task_id: int) -> tuple:
    task = await conn.fetchrow(
        """select status, next_offset, pages_done, bars_read, bars_written,
                  worker_id, claim_token, lease_version, attempts, max_attempts
             from historical_backfill_tasks where id=$1""",
        task_id,
    )
    bars = await conn.fetchval("select count(*) from klines where symbol_id=$1", symbol_id)
    catalog = await conn.fetch(
        """select generation_id, state, min_ts, max_ts
             from kline_scope_catalog where symbol_id=$1 order by generation_id""",
        symbol_id,
    )
    return tuple(task.values()), int(bars), tuple(tuple(row.values()) for row in catalog)


def _bar(symbol: str, index: int, *, timeframe: str = "5f") -> Bar:
    minutes = 15 if timeframe == "15f" else 5
    return Bar(
        symbol=symbol,
        timeframe=timeframe,
        ts=datetime(2026, 1, 2, 1, 30, tzinfo=UTC)
        + timedelta(minutes=index * minutes),
        open=10.0 + index,
        high=10.5 + index,
        low=9.5 + index,
        close=10.1 + index,
        volume=100 + index,
        source="pytdx",
    )
