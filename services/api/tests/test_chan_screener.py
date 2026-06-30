from __future__ import annotations

from app.repositories.chan_screener import conditions_from_llm_payload, parse_chan_screener_query


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
