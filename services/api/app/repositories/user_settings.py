from __future__ import annotations

import json
from typing import Any

USER_SETTING_BUCKETS = ("theme", "watchlist", "layout", "indicatorSettings")
USER_SETTING_BUCKET_SET = frozenset(USER_SETTING_BUCKETS)


def user_setting_row_to_dict(row) -> dict:
    return {
        "bucket": row["bucket"],
        "value": _read_json_value(row["value"]),
        "version": row["version"],
        "updated_at": row["updated_at"],
    }


async def list_settings(pool, owner_token_hash: str) -> list[dict]:
    rows = await pool.fetch(
        """
        select bucket, value, version, updated_at
        from user_settings
        where owner_token_hash = $1
        order by bucket
        """,
        owner_token_hash,
    )
    return [user_setting_row_to_dict(row) for row in rows]


async def upsert_setting(
    pool,
    *,
    owner_token_hash: str,
    bucket: str,
    value: Any,
) -> dict:
    row = await pool.fetchrow(
        """
        insert into user_settings (owner_token_hash, bucket, value)
        values ($1, $2, $3::jsonb)
        on conflict (owner_token_hash, bucket) do update
        set value = excluded.value,
            version = user_settings.version + 1,
            updated_at = now()
        returning bucket, value, version, updated_at
        """,
        owner_token_hash,
        bucket,
        json.dumps(value, separators=(",", ":")),
    )
    return user_setting_row_to_dict(row)


def _read_json_value(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value
