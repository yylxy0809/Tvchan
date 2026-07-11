from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from app.core.config import Settings, get_settings
from app.models import HealthResponse
from app.repositories.bars import utc_now_iso
from app.repositories.chan_postgres import get_module_c_published_head_coverage_db

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health(
    request: Request, settings: Settings = Depends(get_settings)
) -> HealthResponse:
    db_status = "seed" if settings.use_seed_data else "not_checked"
    source_labels = ["seed"] if settings.use_seed_data else []
    if not settings.use_seed_data:
        pool = getattr(request.app.state, "db_pool", None)
        if pool is None:
            db_status = "not_ready"
        else:
            try:
                async with pool.acquire() as conn:
                    await conn.fetchval("select 1")
                    try:
                        sources = await conn.fetch(
                            """
                            select distinct source
                            from scheme2_ingest_watermarks
                            order by source
                            """
                        )
                    except Exception:
                        sources = []
                db_status = "ok"
                source_labels = [_source_label(row["source"]) for row in sources]
            except Exception:
                db_status = "error"
                source_labels = []
    module_c_status = await _module_c_status(
        getattr(request.app.state, "db_pool", None), settings
    )
    return HealthResponse(
        status="ok",
        db=db_status,
        redis="not_checked",
        collector="seed" if settings.use_seed_data else "not_checked",
        module_c=module_c_status,
        server_time=utc_now_iso(),
        seed_data=settings.use_seed_data,
        data_source=(
            "seed"
            if settings.use_seed_data
            else f"database:{','.join(source_labels) if source_labels else 'empty'}"
        ),
        data_note=(
            "K-lines are deterministic seed samples, not live pytdx market data."
            if settings.use_seed_data
            else "K-lines are read from PostgreSQL/TimescaleDB. Source shows seed, pytdx, or tdx_csv origin."
        ),
    )


def _source_label(value) -> str:
    if isinstance(value, str):
        return value
    return {
        1: "seed",
        2: "pytdx",
        3: "tdx_csv",
        4: "parquet_5f",
        5: "mootdx",
        6: "tencent",
        7: "baidu",
    }.get(value, f"source_{value}")


async def _module_c_status(pool, settings: Settings) -> dict:
    if settings.use_seed_data:
        return {"ready": False, "reason": "seed_data"}
    if pool is None:
        return {"ready": False, "reason": "db_pool_not_ready"}
    try:
        return await get_module_c_published_head_coverage_db(pool)
    except Exception:
        return {"ready": False, "reason": "coverage_query_failed"}
