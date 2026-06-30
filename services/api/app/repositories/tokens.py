from __future__ import annotations


def token_row_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "label": row["label"],
        "display_name": row["display_name"],
        "role": row["role"],
        "is_active": row["is_active"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "disabled_at": row["disabled_at"],
        "last_used_at": row["last_used_at"],
    }


async def find_active_token_by_hash(pool, token_hash: str) -> dict | None:
    row = await pool.fetchrow(
        """
        select id, label, display_name, role, is_active, created_at, updated_at,
               disabled_at, last_used_at
        from user_api_tokens
        where token_hash = $1
          and is_active = true
        """,
        token_hash,
    )
    return token_row_to_dict(row) if row else None


async def list_tokens(pool) -> list[dict]:
    rows = await pool.fetch(
        """
        select id, label, display_name, role, is_active, created_at, updated_at,
               disabled_at, last_used_at
        from user_api_tokens
        order by created_at desc, id desc
        """
    )
    return [token_row_to_dict(row) for row in rows]


async def create_token(
    pool,
    *,
    token_hash: str,
    label: str,
    display_name: str | None,
) -> dict:
    row = await pool.fetchrow(
        """
        insert into user_api_tokens (token_hash, label, display_name)
        values ($1, $2, $3)
        returning id, label, display_name, role, is_active, created_at, updated_at,
                  disabled_at, last_used_at
        """,
        token_hash,
        label,
        display_name,
    )
    return token_row_to_dict(row)


async def disable_token(pool, token_id: int) -> dict | None:
    row = await pool.fetchrow(
        """
        update user_api_tokens
        set is_active = false,
            disabled_at = coalesce(disabled_at, now()),
            updated_at = now()
        where id = $1
        returning id, label, display_name, role, is_active, created_at, updated_at,
                  disabled_at, last_used_at
        """,
        token_id,
    )
    return token_row_to_dict(row) if row else None


async def delete_token(pool, token_id: int) -> bool:
    result = await pool.execute(
        """
        delete from user_api_tokens
        where id = $1
        """,
        token_id,
    )
    return result.split()[-1] != "0"


async def touch_token_last_used(pool, token_id: int) -> None:
    await pool.execute(
        """
        update user_api_tokens
        set last_used_at = now(),
            updated_at = now()
        where id = $1
        """,
        token_id,
    )
