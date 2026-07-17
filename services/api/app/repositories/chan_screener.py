from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from trading_protocol import MODULE_C_CONFIG_HASH

MARKET_SNAPSHOT_MAX_SYMBOLS = 20
MARKET_SNAPSHOT_TIMEOUT_MS = 800
SUPPORTED_MODULE_C_CONFIG_HASHES = (
    MODULE_C_CONFIG_HASH,
)

LEVEL_LABELS = {
    5: "5f",
    30: "30f",
    1440: "1d",
    10080: "1w",
    43200: "1m",
}

LABEL_TO_LEVEL = {value: key for key, value in LEVEL_LABELS.items()}
LABEL_TO_LEVEL.update(
    {
        "5分钟": 5,
        "30分钟": 30,
        "日线": 1440,
        "周线": 10080,
        "月线": 43200,
    }
)

DISPLAY_LEVELS = ["1m", "1w", "1d", "30f", "5f"]
TREND_LEVELS = ["1d", "30f", "5f"]

LEVEL_PATTERNS = [
    (43200, re.compile(r"(月线|月级别|1m)", re.IGNORECASE)),
    (10080, re.compile(r"(周线|周级别|1w)", re.IGNORECASE)),
    (1440, re.compile(r"(日线|日级别|1d)", re.IGNORECASE)),
    (30, re.compile(r"(30f|30分钟|30分)", re.IGNORECASE)),
    (5, re.compile(r"(5f|5分钟|5分)", re.IGNORECASE)),
]

MODE_TO_DB = {
    "current": "current",
    "confirmed": "confirmed",
    "predictive": "predictive",
}


@dataclass(frozen=True)
class ScreenerCondition:
    level: int
    kind: str
    direction: int | None
    value: str | None
    raw: str

    def as_response(self) -> dict[str, Any]:
        return {
            "level": LEVEL_LABELS.get(self.level, str(self.level)),
            "kind": self.kind,
            "direction": _direction_label(self.direction),
            "value": self.value,
            "raw": self.raw,
        }


def parse_chan_screener_query(query: str) -> tuple[list[ScreenerCondition], list[str]]:
    text = _normalize_query(query)
    conditions: list[ScreenerCondition] = []
    unsupported: list[str] = []
    if not text:
        return conditions, unsupported

    clauses = [item.strip() for item in re.split(r"[，,。；;\n]+", text) if item.strip()]
    for clause in clauses:
        levels = _levels_in_text(clause)
        if not levels:
            continue
        for level in levels:
            conditions.extend(_parse_clause_for_level(clause, level))
        if _has_unsupported_state(clause):
            unsupported.append(clause)
    return _dedupe_conditions(conditions), unsupported


def conditions_from_llm_payload(payload: dict[str, Any]) -> tuple[list[ScreenerCondition], list[str]]:
    raw_conditions = payload.get("conditions")
    unsupported = payload.get("unsupported")
    conditions: list[ScreenerCondition] = []
    if isinstance(raw_conditions, list):
        for item in raw_conditions:
            condition = _condition_from_payload(item)
            if condition is not None:
                conditions.append(condition)
    unsupported_items = [
        str(item)
        for item in (unsupported if isinstance(unsupported, list) else [])
        if isinstance(item, str)
    ]
    return _dedupe_conditions(conditions), unsupported_items


async def query_chan_screener(
    pool,
    *,
    query: str,
    limit: int,
    mode: str = "confirmed",
    parsed_conditions: list[ScreenerCondition] | None = None,
    parsed_unsupported: list[str] | None = None,
    parser: str = "rules",
    parser_error: str | None = None,
) -> dict[str, Any]:
    normalized_mode = MODE_TO_DB.get(mode, "current")
    if parsed_conditions is None:
        conditions, unsupported = parse_chan_screener_query(query)
    else:
        conditions = parsed_conditions
        unsupported = parsed_unsupported or []
    conditions, unsupported = _module_c_conditions(conditions, unsupported)
    if not conditions:
        return {
            "query": query,
            "mode": normalized_mode,
            "parser": parser,
            "parser_error": parser_error,
            "conditions": [],
            "unsupported": unsupported,
            "items": [],
        }

    async with pool.acquire() as conn:
        rows = await _find_matching_symbols(
            conn,
            conditions=conditions,
            mode=normalized_mode,
            limit=limit,
        )
        symbol_ids = [int(row["id"]) for row in rows]
        states = await _fetch_states(conn, symbol_ids=symbol_ids, mode=normalized_mode)
        markets = await _fetch_market_snapshots(conn, symbol_ids=symbol_ids)

    return {
        "query": query,
        "mode": normalized_mode,
        "parser": parser,
        "parser_error": parser_error,
        "conditions": [condition.as_response() for condition in conditions],
        "unsupported": unsupported,
        "items": [
            _build_item(row=row, states=states.get(int(row["id"]), {}), market=markets.get(int(row["id"]), {}))
            for row in rows
        ],
    }


async def _find_matching_symbols(
    conn,
    *,
    conditions: list[ScreenerCondition],
    mode: str,
    limit: int,
):
    params: list[Any] = [mode, list(SUPPORTED_MODULE_C_CONFIG_HASHES)]
    exists_clauses: list[str] = []
    for condition in conditions:
        exists_clauses.append(_condition_sql(condition, params, mode=mode))
    params.append(limit)
    query = f"""
        select s.id, s.code, s.exchange, s.name
        from symbols s
        where s.is_active = true
          and {" and ".join(exists_clauses)}
        order by s.code, s.exchange
        limit ${len(params)}
    """
    return await conn.fetch(query, *params)


def _condition_sql(condition: ScreenerCondition, params: list[Any], *, mode: str) -> str:
    params.append(condition.level)
    level_param = len(params)
    head_clauses = [
        "head.symbol_id = s.id",
        f"head.chan_level = ${level_param}",
        "head.base_timeframe = head.chan_level",
        "head.status = 'published'",
        "head.run_id is not null",
        "run.status = 'success'",
        "run.config_hash = any($2::varchar[])",
        _mode_sql(mode),
    ]
    detail_clause: str
    if condition.kind == "stroke":
        params.append(condition.direction)
        direction_param = len(params)
        detail_clause = f"""
            exists (
                select 1 from chan_c_strokes detail
                where detail.run_id = head.run_id
                  and detail.mode = case head.mode when 'confirmed' then 1 when 'predictive' then 2 end
                  and detail.id = (
                      select latest.id from chan_c_strokes latest
                      where latest.run_id = head.run_id
                        and latest.mode = detail.mode
                      order by coalesce(latest.end_base_ts, latest.end_ts) desc, latest.seq desc, latest.id desc
                      limit 1
                  )
                  and detail.direction = ${direction_param}
            )"""
    elif condition.kind == "segment":
        params.append(condition.direction)
        direction_param = len(params)
        detail_clause = f"""
            exists (
                select 1 from chan_c_segments detail
                where detail.run_id = head.run_id
                  and detail.mode = case head.mode when 'confirmed' then 1 when 'predictive' then 2 end
                  and detail.id = (
                      select latest.id from chan_c_segments latest
                      where latest.run_id = head.run_id
                        and latest.mode = detail.mode
                      order by coalesce(latest.end_base_ts, latest.end_ts) desc, latest.seq desc, latest.id desc
                      limit 1
                  )
                  and detail.direction = ${direction_param}
            )"""
    elif condition.kind == "signal":
        side = _signal_side(condition.value)
        params.append(_signal_bsp_type(condition.value))
        bsp_type_param = len(params)
        params.append(side)
        side_param = len(params)
        detail_clause = f"""
            exists (
                select 1 from chan_c_signals detail
                where detail.run_id = head.run_id
                  and detail.mode = case head.mode when 'confirmed' then 1 when 'predictive' then 2 end
                  and detail.id = (
                      select latest.id from chan_c_signals latest
                      where latest.run_id = head.run_id
                        and latest.mode = detail.mode
                      order by coalesce(latest.base_ts, latest.ts) desc, latest.id desc
                      limit 1
                  )
                  and detail.extra ->> 'bsp_type' = ${bsp_type_param}
                  and detail.extra ->> 'side' = ${side_param}
            )"""
    else:
        raise ValueError(f"Unsupported screener condition kind: {condition.kind}")
    return """exists (
        select 1
        from scheme2_chan_c_published_heads head
        join chan_c_runs run on run.id = head.run_id
        where """ + " and ".join(head_clauses) + " and " + detail_clause + ")"


def _mode_sql(mode: str, *, table_alias: str = "head") -> str:
    if mode != "current":
        return f"{table_alias}.mode = $1"
    return f"""(
        {table_alias}.mode = 'predictive'
        or (
            {table_alias}.mode = 'confirmed'
            and not exists (
                select 1
                from scheme2_chan_c_published_heads preferred
                join chan_c_runs preferred_run on preferred_run.id = preferred.run_id
                where preferred.symbol_id = {table_alias}.symbol_id
                  and preferred.chan_level = {table_alias}.chan_level
                  and preferred.base_timeframe = {table_alias}.base_timeframe
                  and preferred.mode = 'predictive'
                  and preferred.status = 'published'
                  and preferred.run_id is not null
                  and preferred_run.status = 'success'
                  and preferred_run.config_hash = any($2::varchar[])
            )
        )
    )"""


def _module_c_conditions(
    conditions: list[ScreenerCondition],
    unsupported: list[str],
) -> tuple[list[ScreenerCondition], list[str]]:
    supported: list[ScreenerCondition] = []
    unsupported_items = list(unsupported)
    for condition in conditions:
        # Module C publishes raw structures, but not the retired derived structure-state contract.
        if condition.kind == "structure" or (
            condition.kind == "signal" and _signal_bsp_type(condition.value) is None
        ):
            if condition.raw and condition.raw not in unsupported_items:
                unsupported_items.append(condition.raw)
            continue
        supported.append(condition)
    return supported, unsupported_items


async def _fetch_states(conn, *, symbol_ids: list[int], mode: str) -> dict[int, dict[str, dict[str, Any]]]:
    if not symbol_ids:
        return {}
    rows = await conn.fetch(
        f"""
        with ranked_heads as (
            select
                head.symbol_id,
                head.chan_level,
                head.mode,
                head.run_id,
                head.base_to_bar_end,
                row_number() over (
                    partition by head.symbol_id, head.chan_level
                    order by
                        case head.mode when 'predictive' then 0 when 'confirmed' then 1 else 2 end,
                        coalesce(head.published_at, head.updated_at) desc,
                        head.id desc
                ) as rn
            from scheme2_chan_c_published_heads head
            join chan_c_runs run on run.id = head.run_id
            where head.symbol_id = any($1::bigint[])
              and head.base_timeframe = head.chan_level
              and head.status = 'published'
              and head.run_id is not null
              and run.status = 'success'
              and run.config_hash = any($2::varchar[])
              and {_mode_sql(mode, table_alias='head')}
        )
        select
            head.symbol_id,
            head.chan_level,
            head.mode,
            null::varchar as structure_state,
            null::smallint as structure_direction,
            stroke.direction as latest_stroke_direction,
            segment.direction as latest_segment_direction,
            centers.center_count,
            signal.signal_type as last_signal_type,
            signal.extra ->> 'side' as last_signal_side,
            signal.extra ->> 'bsp_type' as last_signal_bsp_type,
            null::boolean as is_complete,
            head.base_to_bar_end as asof_base_ts,
            head.base_to_bar_end as source_bar_until
        from ranked_heads head
        left join lateral (
            select direction
            from chan_c_strokes
            where run_id = head.run_id
              and mode = case head.mode when 'confirmed' then 1 when 'predictive' then 2 end
            order by coalesce(end_base_ts, end_ts) desc, seq desc, id desc
            limit 1
        ) stroke on true
        left join lateral (
            select direction
            from chan_c_segments
            where run_id = head.run_id
              and mode = case head.mode when 'confirmed' then 1 when 'predictive' then 2 end
            order by coalesce(end_base_ts, end_ts) desc, seq desc, id desc
            limit 1
        ) segment on true
        left join lateral (
            select count(*)::integer as center_count
            from chan_c_centers
            where run_id = head.run_id
              and mode = case head.mode when 'confirmed' then 1 when 'predictive' then 2 end
        ) centers on true
        left join lateral (
            select signal_type, extra
            from chan_c_signals
            where run_id = head.run_id
              and mode = case head.mode when 'confirmed' then 1 when 'predictive' then 2 end
            order by coalesce(base_ts, ts) desc, id desc
            limit 1
        ) signal on true
        where head.rn = 1
        order by head.symbol_id, head.chan_level
        """,
        symbol_ids,
        list(SUPPORTED_MODULE_C_CONFIG_HASHES),
    )
    result: dict[int, dict[str, dict[str, Any]]] = {}
    for row in rows:
        symbol_id = int(row["symbol_id"])
        level = LEVEL_LABELS.get(int(row["chan_level"]), str(row["chan_level"]))
        result.setdefault(symbol_id, {})[level] = {
            "level": level,
            "mode": row["mode"],
            "structure_state": row["structure_state"],
            "structure_direction": _direction_label(row["structure_direction"]),
            "latest_stroke_direction": _direction_label(row["latest_stroke_direction"]),
            "latest_segment_direction": _direction_label(row["latest_segment_direction"]),
            "center_count": int(row["center_count"] or 0),
            "last_signal_type": row["last_signal_type"],
            "last_signal_side": row["last_signal_side"],
            "last_signal_bsp_type": row["last_signal_bsp_type"],
            "is_complete": row["is_complete"],
            "asof": _datetime_to_unix(row["asof_base_ts"]),
            "source_bar_until": _datetime_to_unix(row["source_bar_until"]),
        }
    return result


async def _fetch_market_snapshots(conn, *, symbol_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not symbol_ids:
        return {}
    if len(symbol_ids) > MARKET_SNAPSHOT_MAX_SYMBOLS:
        return {}
    try:
        async with conn.transaction():
            await conn.execute(f"set local statement_timeout = {MARKET_SNAPSHOT_TIMEOUT_MS}")
            rows = await conn.fetch(
                """
                with latest as (
                    select distinct on (symbol_id)
                        symbol_id,
                        ts,
                        close_x1000,
                        amount_x100
                    from klines
                    where symbol_id = any($1::bigint[])
                      and timeframe = 5
                      and source = any($2::smallint[])
                    order by symbol_id, ts desc
                )
                select
                    l.symbol_id,
                    l.ts,
                    l.close_x1000,
                    l.amount_x100,
                    prev.close_x1000 as previous_close_x1000
                from latest l
                left join lateral (
                    select k.close_x1000
                    from klines k
                    where k.symbol_id = l.symbol_id
                      and k.timeframe = 5
                      and k.source = any($2::smallint[])
                      and k.ts < (date_trunc('day', l.ts at time zone 'Asia/Shanghai') at time zone 'Asia/Shanghai')
                    order by k.ts desc
                    limit 1
                ) prev on true
                """,
                symbol_ids,
                [2, 3, 4],
            )
    except Exception:
        # Screener conditions are authoritative; market fields are best-effort enrichment.
        return {}
    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        price = _x1000_to_float(row["close_x1000"])
        previous = _x1000_to_float(row["previous_close_x1000"])
        result[int(row["symbol_id"])] = {
            "price": price,
            "change_percent": _change_percent(price, previous),
            "industry": None,
            "fund_net_inflow": None,
            "latest_bar_time": _datetime_to_unix(row["ts"]),
        }
    return result


def _build_item(*, row, states: dict[str, dict[str, Any]], market: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": f"{row['code']}.{row['exchange']}",
        "code": row["code"],
        "exchange": row["exchange"],
        "name": row["name"],
        "states": states,
        "trend_status": {level: _structure_label(states.get(level)) for level in TREND_LEVELS},
        "stroke_states": {level: _direction_state_label(level, states.get(level), "latest_stroke_direction") for level in DISPLAY_LEVELS},
        "segment_states": {level: _direction_state_label(level, states.get(level), "latest_segment_direction") for level in DISPLAY_LEVELS},
        "market": {
            "price": market.get("price"),
            "change_percent": market.get("change_percent"),
            "industry": market.get("industry"),
            "fund_net_inflow": market.get("fund_net_inflow"),
            "latest_bar_time": market.get("latest_bar_time"),
        },
    }


def _parse_clause_for_level(clause: str, level: int) -> list[ScreenerCondition]:
    direction = _direction_in_text(clause)
    conditions: list[ScreenerCondition] = []
    if "趋势" in clause and direction is not None:
        conditions.append(ScreenerCondition(level, "structure", direction, "trend", clause))
    if "盘整" in clause and direction is not None:
        conditions.append(ScreenerCondition(level, "structure", direction, "consolidation", clause))
    if "无中枢" in clause and direction is not None:
        conditions.append(ScreenerCondition(level, "structure", direction, "no_center", clause))
    if "线段" in clause and direction is not None:
        conditions.append(ScreenerCondition(level, "segment", direction, None, clause))
    if ("笔" in clause or "一笔" in clause) and direction is not None:
        conditions.append(ScreenerCondition(level, "stroke", direction, None, clause))
    signal = _signal_in_text(clause)
    if signal is not None:
        conditions.append(ScreenerCondition(level, "signal", None, signal, clause))
    return conditions


def _condition_from_payload(value: Any) -> ScreenerCondition | None:
    if not isinstance(value, dict):
        return None
    level = _payload_level(value.get("level"))
    kind = value.get("kind")
    if level is None or kind not in {"structure", "stroke", "segment", "signal"}:
        return None
    direction = _payload_direction(value.get("direction"))
    raw = str(value.get("raw") or "")
    raw_value = value.get("value")
    condition_value = str(raw_value) if raw_value is not None else None
    if kind == "structure" and condition_value not in {"trend", "consolidation", "no_center"}:
        return None
    if kind in {"stroke", "segment"} and direction is None:
        return None
    if kind == "signal" and not condition_value:
        return None
    return ScreenerCondition(
        level=level,
        kind=kind,
        direction=direction,
        value=condition_value,
        raw=raw,
    )


def _payload_level(value: Any) -> int | None:
    if isinstance(value, int):
        return value if value in LEVEL_LABELS else None
    if not isinstance(value, str):
        return None
    return LABEL_TO_LEVEL.get(value.strip())


def _payload_direction(value: Any) -> int | None:
    if value == "up":
        return 1
    if value == "down":
        return -1
    if value in {1, -1}:
        return int(value)
    return None


def _normalize_query(query: str) -> str:
    return query.strip().replace(" ", "").replace("\u3000", "")


def _levels_in_text(text: str) -> list[int]:
    return [level for level, pattern in LEVEL_PATTERNS if pattern.search(text)]


def _direction_in_text(text: str) -> int | None:
    if re.search(r"(向上|上涨|上行|上升|笔上|线上|上笔|线段上|多头)", text):
        return 1
    if re.search(r"(向下|下跌|下行|下降|笔下|线下|下笔|线段下|空头)", text):
        return -1
    return None


def _signal_in_text(text: str) -> str | None:
    normalized = (
        text.replace("一", "1")
        .replace("二", "2")
        .replace("三", "3")
        .replace("１", "1")
        .replace("２", "2")
        .replace("３", "3")
    )
    match = re.search(r"(类)?([123])\s*([买卖])", normalized)
    if not match:
        return None
    prefix, number, side = match.groups()
    return f"{prefix or ''}{number}{side}"


def _has_unsupported_state(text: str) -> bool:
    return any(keyword in text for keyword in ("破坏", "终结", "背驰", "中枢扩展"))


def _dedupe_conditions(conditions: list[ScreenerCondition]) -> list[ScreenerCondition]:
    seen: set[tuple[int, str, int | None, str | None]] = set()
    result: list[ScreenerCondition] = []
    for condition in conditions:
        key = (condition.level, condition.kind, condition.direction, condition.value)
        if key in seen:
            continue
        seen.add(key)
        result.append(condition)
    return result


def _signal_side(value: str | None) -> str | None:
    if not value:
        return None
    if "买" in value:
        return "buy"
    if "卖" in value:
        return "sell"
    return None


def _signal_bsp_type(value: str | None) -> str | None:
    if not value:
        return None
    match = re.fullmatch(r"(?:类)?([12])[买卖]", value)
    return match.group(1) if match else None


def _direction_label(value: int | None) -> str | None:
    if value == 1:
        return "up"
    if value == -1:
        return "down"
    return None


def _structure_label(state: dict[str, Any] | None) -> str | None:
    if not state:
        return None
    kind = {
        "trend": "趋势",
        "consolidation": "盘整",
        "no_center": "无中枢",
    }.get(state.get("structure_state"))
    direction = _direction_cn(state.get("structure_direction"))
    if not kind:
        return None
    suffix = "进行中" if state.get("is_complete") is False else ""
    return f"{kind}{direction or ''}{suffix}"


def _direction_state_label(level: str, state: dict[str, Any] | None, field: str) -> str | None:
    if not state:
        return None
    direction = _direction_cn(state.get(field))
    if not direction:
        return None
    return f"{_level_cn(level)}{direction}"


def _level_cn(level: str) -> str:
    return {
        "1m": "月线",
        "1w": "周线",
        "1d": "日线",
        "30f": "30f",
        "5f": "5f",
    }.get(level, level)


def _direction_cn(value: Any) -> str | None:
    if value == "up" or value == 1:
        return "上"
    if value == "down" or value == -1:
        return "下"
    return None


def _datetime_to_unix(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.timestamp())


def _x1000_to_float(value: Any) -> float | None:
    return None if value is None else round(float(value) / 1000, 3)


def _change_percent(price: float | None, previous_close: float | None) -> float | None:
    if price is None or previous_close is None or previous_close == 0:
        return None
    return round((price - previous_close) / previous_close * 100, 2)
