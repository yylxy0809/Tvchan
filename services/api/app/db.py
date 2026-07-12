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
        # Chart reads are coalesced by the frontend managers. One lazy reader
        # avoids compiling the same hypertable query independently on idle
        # connections; the local deployment favours predictable latency and
        # low resource use over parallel chart reads.
        app.state.db_pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=1,
            max_size=1,
        )
    try:
        yield
    finally:
        repository = getattr(app.state, "market_sidebar_repository", None)
        if repository is not None:
            await repository.close()
        pool = getattr(app.state, "db_pool", None)
        if pool is not None:
            await pool.close()
