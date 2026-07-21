from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    app.state.db_pool = None
    if not settings.use_seed_data:
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "asyncpg is required when USE_SEED_DATA=false. "
                "Install services/api/requirements.txt."
            ) from exc
        app.state.db_pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=settings.database_pool_min_size,
            max_size=settings.database_pool_max_size,
        )
    repository = getattr(app.state, "market_sidebar_repository", None)
    set_db_pool = getattr(repository, "set_db_pool", None)
    if set_db_pool is not None:
        set_db_pool(app.state.db_pool)
    try:
        yield
    finally:
        repository = getattr(app.state, "market_sidebar_repository", None)
        if repository is not None:
            await repository.close()
        pool = getattr(app.state, "db_pool", None)
        if pool is not None:
            await pool.close()
