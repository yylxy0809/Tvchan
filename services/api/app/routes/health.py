from __future__ import annotations

import httpx
from fastapi import APIRouter, Depends, Request

from app.core.config import Settings, get_settings
from app.models import HealthResponse
from app.repositories.bars import utc_now_iso

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
                    has_klines = await conn.fetchval(
                        "select exists(select 1 from klines limit 1)"
                    )
                db_status = "ok"
                source_labels = ["available"] if has_klines else []
            except Exception:
                db_status = "error"
                source_labels = []
    chan_status = await _chan_status(settings)
    return HealthResponse(
        status="ok",
        db=db_status,
        redis="not_checked",
        collector="seed" if settings.use_seed_data else "not_checked",
        chan=chan_status,
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


def _source_label(value: int) -> str:
    return {
        1: "seed",
        2: "pytdx",
        3: "tdx_csv",
        4: "parquet_5f",
    }.get(value, f"source_{value}")


async def _chan_status(settings: Settings) -> str:
    if not settings.chan_service_url:
        return "local-fallback"
    try:
        async with httpx.AsyncClient(
            base_url=settings.chan_service_url.rstrip("/"),
            timeout=2.0,
            trust_env=False,
        ) as client:
            response = await client.get("/health")
            response.raise_for_status()
            body = response.json()
    except Exception:
        return "error"
    engine = body.get("engine", "unknown")
    return f"chan-service:{engine}"
