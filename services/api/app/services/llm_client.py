from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class LlmParseResult:
    payload: dict[str, Any]
    raw_text: str


SYSTEM_PROMPT = """你是缠论自然语言选股条件解析器。
只输出 JSON，不输出解释。
任务：把用户中文自然语言解析成白名单结构化条件，禁止生成 SQL。

允许的 level: "1m", "1w", "1d", "30f", "5f"。
允许的 kind: "structure", "stroke", "segment", "signal"。
direction 只能是 "up"、"down" 或 null。
structure 的 value 只能是 "trend"、"consolidation"、"no_center"。
stroke/segment 的 value 必须为 null。
signal 的 value 使用原始买卖点文本，如 "3买"、"类2买"、"1卖"。

输出格式：
{
  "conditions": [
    {"level":"1d","kind":"structure","direction":"up","value":"trend","raw":"日线趋势上涨"}
  ],
  "unsupported": []
}

缠论定义：
一段走势只有一个中枢称为盘整；有两个同方向中枢称为趋势。
"""


async def parse_chan_query_with_llm(
    *,
    query: str,
    api_key: str,
    base_url: str,
    model: str,
    timeout_seconds: float,
) -> LlmParseResult:
    endpoint = f"{base_url.rstrip('/')}/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": query},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.post(
            endpoint,
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
    text = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    parsed = _parse_json_object(text)
    return LlmParseResult(payload=parsed, raw_text=text)


def _parse_json_object(value: str) -> dict[str, Any]:
    text = value.strip()
    if text.startswith("```"):
        text = text.strip("`").strip()
        if text.lower().startswith("json"):
            text = text[4:].strip()
    return json.loads(text)
