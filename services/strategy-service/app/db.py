from __future__ import annotations

import os

import asyncpg


def database_url() -> str:
    value = os.getenv("DATABASE_URL", "").strip()
    if value:
        return value
    user = os.getenv("POSTGRES_USER", "trader")
    password = os.getenv("POSTGRES_PASSWORD", "change-me-before-long-running")
    host = os.getenv("POSTGRES_HOST", os.getenv("POSTGRES_BIND", "127.0.0.1"))
    port = os.getenv("POSTGRES_PORT", "15432")
    database = os.getenv("POSTGRES_DB", "tradingview_local")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


async def create_pool(*, min_size: int = 1, max_size: int = 4) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url(), min_size=min_size, max_size=max_size)
