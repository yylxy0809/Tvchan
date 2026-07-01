from __future__ import annotations

import json
from typing import Any


def runtime_config_row_to_dict(row) -> dict:
    return {
        "key": row["key"],
        "value": _read_json_value(row["value"]),
        "version": row["version"],
        "updated_at": row["updated_at"],
    }


async def get_config(pool, key: str) -> dict | None:
    row = await pool.fetchrow(
        """
        select key, value, version, updated_at
        from runtime_config
        where key = $1
        """,
        key,
    )
    return runtime_config_row_to_dict(row) if row else None


async def upsert_config(pool, *, key: str, value: Any) -> dict:
    row = await pool.fetchrow(
        """
        insert into runtime_config (key, value)
        values ($1, $2::jsonb)
        on conflict (key) do update
        set value = excluded.value,
            version = runtime_config.version + 1,
            updated_at = now()
        returning key, value, version, updated_at
        """,
        key,
        json.dumps(value, separators=(",", ":")),
    )
    return runtime_config_row_to_dict(row)


def _read_json_value(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value
