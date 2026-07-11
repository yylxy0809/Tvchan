from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from app.repositories.chan_screener import (
    ScreenerCondition,
    conditions_from_llm_payload,
    parse_chan_screener_query,
    query_chan_screener,
)


def test_parse_multi_level_structure_and_segment_query() -> None:
    query = (
        "\u65e5\u7ebf\u7ea7\u522b\u8d8b\u52bf\u4e0a\u6da8\u4e2d"
        "\uff0c30f\u7ea7\u522b\u76d8\u6574\u4e0b\u8dcc"
        "\uff0c5f\u7ea7\u522b\u7ebf\u6bb5\u4e0a\u6da8"
    )
    conditions, unsupported = parse_chan_screener_query(query)

    assert unsupported == []
    assert [item.as_response() for item in conditions] == [
        {
            "level": "1d",
            "kind": "structure",
            "direction": "up",
            "value": "trend",
            "raw": "\u65e5\u7ebf\u7ea7\u522b\u8d8b\u52bf\u4e0a\u6da8\u4e2d",
        },
        {
            "level": "30f",
            "kind": "structure",
            "direction": "down",
            "value": "consolidation",
            "raw": "30f\u7ea7\u522b\u76d8\u6574\u4e0b\u8dcc",
        },
        {
            "level": "5f",
            "kind": "segment",
            "direction": "up",
            "value": None,
            "raw": "5f\u7ea7\u522b\u7ebf\u6bb5\u4e0a\u6da8",
        },
    ]


def test_parse_signal_and_stroke_query() -> None:
    query = "30f\u7ea7\u522b\u7c7b2\u4e70\uff0c\u65e5\u7ebf\u5411\u4e0a\u4e00\u7b14\u8fdb\u884c\u4e2d"
    conditions, unsupported = parse_chan_screener_query(query)

    assert unsupported == []
    assert [item.as_response() for item in conditions] == [
        {
            "level": "30f",
            "kind": "signal",
            "direction": None,
            "value": "\u7c7b2\u4e70",
            "raw": "30f\u7ea7\u522b\u7c7b2\u4e70",
        },
        {
            "level": "1d",
            "kind": "stroke",
            "direction": "up",
            "value": None,
            "raw": "\u65e5\u7ebf\u5411\u4e0a\u4e00\u7b14\u8fdb\u884c\u4e2d",
        },
    ]


def test_parse_marks_unsupported_special_state() -> None:
    query = "30f\u7ea7\u522b\u5411\u4e0b\u7ebf\u6bb5\u88ab\u7834\u574f\u6216\u7ec8\u7ed3"
    conditions, unsupported = parse_chan_screener_query(query)

    assert [item.as_response() for item in conditions] == [
        {
            "level": "30f",
            "kind": "segment",
            "direction": "down",
            "value": None,
            "raw": query,
        }
    ]
    assert unsupported == [query]


def test_llm_payload_conditions_share_canonical_parser_shape() -> None:
    payload = {
        "conditions": [
            {
                "level": "1d",
                "kind": "structure",
                "direction": "up",
                "value": "trend",
                "raw": "\u65e5\u7ebf\u8d8b\u52bf\u4e0a\u6da8",
            },
            {
                "level": "30f",
                "kind": "signal",
                "direction": None,
                "value": "\u7c7b2\u4e70",
                "raw": "30f\u7ea7\u522b\u7c7b2\u4e70",
            },
        ],
        "unsupported": ["30f\u7ebf\u6bb5\u7834\u574f"],
    }

    conditions, unsupported = conditions_from_llm_payload(payload)

    assert [item.as_response() for item in conditions] == [
        {
            "level": "1d",
            "kind": "structure",
            "direction": "up",
            "value": "trend",
            "raw": "\u65e5\u7ebf\u8d8b\u52bf\u4e0a\u6da8",
        },
        {
            "level": "30f",
            "kind": "signal",
            "direction": None,
            "value": "\u7c7b2\u4e70",
            "raw": "30f\u7ea7\u522b\u7c7b2\u4e70",
        },
    ]
    assert unsupported == ["30f\u7ebf\u6bb5\u7834\u574f"]


def test_module_c_screener_queries_only_published_module_c_outputs() -> None:
    class Conn:
        def __init__(self) -> None:
            self.queries: list[str] = []

        @asynccontextmanager
        async def transaction(self):
            yield

        async def execute(self, query: str) -> None:
            self.queries.append(query)

        async def fetch(self, query: str, *args):
            self.queries.append(query)
            if "from symbols s" in query:
                return [{"id": 7, "code": "000001", "exchange": "SZ", "name": "Ping An"}]
            if "with ranked_heads" in query:
                return [{
                    "symbol_id": 7,
                    "chan_level": 5,
                    "mode": "confirmed",
                    "structure_state": None,
                    "structure_direction": None,
                    "latest_stroke_direction": 1,
                    "latest_segment_direction": -1,
                    "center_count": 2,
                    "last_signal_type": "2类买",
                    "last_signal_side": "buy",
                    "last_signal_bsp_type": "2",
                    "is_complete": None,
                    "asof_base_ts": datetime(2026, 7, 11, tzinfo=UTC),
                    "source_bar_until": datetime(2026, 7, 11, tzinfo=UTC),
                }]
            if "from klines" in query:
                return []
            raise AssertionError(query)

    class Pool:
        def __init__(self) -> None:
            self.conn = Conn()

        @asynccontextmanager
        async def acquire(self):
            yield self.conn

    pool = Pool()
    response = asyncio.run(query_chan_screener(
        pool,
        query="5f向上一笔",
        limit=10,
        parsed_conditions=[ScreenerCondition(5, "stroke", 1, None, "5f向上一笔")],
    ))

    sql = "\n".join(pool.conn.queries)
    assert "scheme2_chan_c_published_heads" in sql
    assert "chan_c_runs" in sql
    assert "chan_c_strokes" in sql
    retired_tables = (
        "chan_" + "level_state_snapshots",
        "chan_" + "cross_level_states",
        "scheme2_chan_" + "published_heads",
        "chan_" + "strokes",
        "chan_" + "segments",
        "chan_" + "centers",
        "chan_" + "signals",
        "chan_" + "runs",
    )
    assert all(table not in sql for table in retired_tables)
    assert response["items"][0]["states"]["5f"]["structure_state"] is None
    assert response["items"][0]["states"]["5f"]["is_complete"] is None


def test_module_c_marks_derived_structure_state_unsupported() -> None:
    condition = ScreenerCondition(30, "structure", 1, "trend", "30f趋势上涨")

    response = asyncio.run(query_chan_screener(
        None,
        query=condition.raw,
        limit=10,
        parsed_conditions=[condition],
    ))

    assert response["conditions"] == []
    assert response["unsupported"] == [condition.raw]
    assert response["items"] == []
