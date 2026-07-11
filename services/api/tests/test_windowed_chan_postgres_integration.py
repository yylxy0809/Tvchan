"""Opt-in, rollback-only PostgreSQL index-plan verification for a dedicated test DB."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest


TEST_DATABASE_URL = os.getenv("CHAN_WINDOW_TEST_DATABASE_URL", "")


def _walk_plan(node: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = [node]
    for child in node.get("Plans", []):
        nodes.extend(_walk_plan(child))
    return nodes


@pytest.mark.skipif(not TEST_DATABASE_URL, reason="set CHAN_WINDOW_TEST_DATABASE_URL for dedicated local/staging verification")
def test_windowed_schema_indexes_and_explain_use_gist() -> None:
    asyncpg = pytest.importorskip("asyncpg")

    async def scenario() -> None:
        conn = await asyncpg.connect(TEST_DATABASE_URL)
        try:
            async with conn.transaction():
                await conn.execute("set local enable_seqscan = off")
                indexes = await conn.fetch(
                    """
                    select indexname from pg_indexes
                    where schemaname = current_schema()
                      and indexname in (
                        'idx_chan_c_strokes_window_range_gist',
                        'idx_chan_c_segments_window_range_gist',
                        'idx_chan_c_centers_window_range_gist',
                        'idx_chan_c_signals_window_lookup'
                      )
                    """
                )
                assert len(indexes) == 4

                symbol_id = await conn.fetchval(
                    """
                    insert into symbols (code, exchange, name)
                    values ('CWPLAN', 'TEST', 'window-plan-test') returning id
                    """
                )
                start = datetime(2026, 1, 1, tzinfo=UTC)
                end = start + timedelta(hours=1)
                run_id = await conn.fetchval(
                    """
                    insert into chan_c_runs (
                        symbol_id, chan_level, mode, input_signature, config_hash,
                        bar_from, bar_until, bar_count, status
                    ) values ($1, 5, 1, 'plan', 'plan', $2, $3, 3, 'success') returning id
                    """,
                    symbol_id, start - timedelta(hours=1), end + timedelta(hours=1),
                )

                for table in ("chan_c_strokes", "chan_c_segments"):
                    await conn.execute(
                        f"""
                        insert into {table} (
                            symbol_id, chan_level, mode, run_id, seq, start_ts, end_ts,
                            start_price_x1000, end_price_x1000, direction, is_confirmed,
                            begin_base_ts, end_base_ts
                        ) values ($1, 5, 1, $2, 1, $3, $4, 10000, 10100, 1, true, $3, $4)
                        """,
                        symbol_id, run_id, start - timedelta(minutes=5), start + timedelta(minutes=5),
                    )
                await conn.execute(
                    """
                    insert into chan_c_centers (
                        symbol_id, chan_level, mode, run_id, seq, start_ts, end_ts,
                        low_x1000, high_x1000, is_confirmed, begin_base_ts, end_base_ts
                    ) values ($1, 5, 1, $2, 1, $3, $4, 10000, 10100, true, $3, $4)
                    """,
                    symbol_id, run_id, start - timedelta(minutes=5), start + timedelta(minutes=5),
                )

                expected_indexes = {
                    "chan_c_strokes": "idx_chan_c_strokes_window_range_gist",
                    "chan_c_segments": "idx_chan_c_segments_window_range_gist",
                    "chan_c_centers": "idx_chan_c_centers_window_range_gist",
                }
                for table, index_name in expected_indexes.items():
                    plan_value = await conn.fetchval(
                        f"""
                        explain (format json, costs off)
                        select id from {table}
                        where run_id = $1 and mode = $2
                          and tstzrange(coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), '[]')
                              && tstzrange($3, $4, '[]')
                        order by coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), seq, id
                        limit $5
                        """,
                        run_id, 1, start, end, 10,
                    )
                    plan_json = json.loads(plan_value) if isinstance(plan_value, str) else plan_value
                    nodes = _walk_plan(plan_json[0]["Plan"])
                    assert any(node.get("Index Name") == index_name for node in nodes)
                    assert not any(
                        node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == table
                        for node in nodes
                    )
                # The transaction exits by rollback below, removing all seed rows.
                raise _RollbackVerification()
        except _RollbackVerification:
            pass
        finally:
            await conn.close()

    asyncio.run(scenario())


class _RollbackVerification(Exception):
    pass
