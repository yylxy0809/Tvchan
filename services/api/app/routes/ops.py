from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request

from app.core.config import Settings, get_settings
from app.core.security import require_admin_token
from app.repositories.chan_postgres import get_module_c_published_head_coverage_db

router = APIRouter(prefix="/admin/ops", tags=["admin-ops"])


@router.get("/status")
async def ops_status(
    request: Request,
    settings: Settings = Depends(get_settings),
    _admin=Depends(require_admin_token),
) -> dict[str, Any]:
    pool = getattr(request.app.state, "db_pool", None)
    try:
        db = await asyncio.wait_for(_db_status(pool), timeout=4.0)
    except TimeoutError:
        db = {"ok": False, "error": "db_status_timeout"}
    try:
        redis = await asyncio.wait_for(_redis_status(settings.redis_url), timeout=2.0)
    except TimeoutError:
        redis = {"ok": False, "error": "redis_status_timeout"}
    if pool is None:
        module_c = {"ready": False, "reason": "db_pool_not_ready"}
    else:
        try:
            module_c = await asyncio.wait_for(get_module_c_published_head_coverage_db(pool), timeout=2.5)
        except TimeoutError:
            module_c = {"ready": False, "reason": "coverage_query_timeout"}
        except Exception:
            module_c = {"ready": False, "reason": "coverage_query_failed"}
    return {
        "status": "ok" if db.get("ok") and redis.get("ok") else "degraded",
        "server_time": datetime.utcnow().isoformat() + "Z",
        "db": db,
        "redis": redis,
        "module_c_published_heads": module_c,
    }


async def _db_status(pool) -> dict[str, Any]:
    if pool is None:
        return {"ok": False, "error": "db_pool_not_ready"}
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("select 1")
            return {
                "ok": True,
                "symbol_count": await _fetchval(conn, "select count(*) from symbols where is_active = true"),
                "ingest_source_counts": await _fetch_rows(
                    conn,
                    """
                    select source, timeframe, count(*) as symbols, max(last_bar_end) as latest_bar_end
                    from scheme2_ingest_watermarks
                    group by source, timeframe
                    order by source, timeframe
                    """,
                ),
                "ingest_watermarks": await _fetch_rows(
                    conn,
                    """
                    select timeframe, count(*) as symbols, max(last_bar_end) as latest_bar_end,
                           min(last_bar_end) as oldest_bar_end
                    from scheme2_ingest_watermarks
                    group by timeframe
                    order by timeframe
                    """,
                ),
                "fetch_queue": await _queue_status(conn, "scheme2_market_fetch_tasks"),
                "module_c_queue": await _queue_status(conn, "scheme2_chan_c_tail_tasks"),
                "module_c_published_heads": await _fetch_rows(
                    conn,
                    """
                    select chan_level, mode, count(*) as heads,
                           min(base_to_bar_end) as oldest_base_to_bar_end,
                           max(base_to_bar_end) as latest_base_to_bar_end,
                           max(published_at) as latest_published_at
                    from scheme2_chan_c_published_heads
                    where status = 'published'
                    group by chan_level, mode
                    order by chan_level, mode
                    """,
                ),
            }
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:500]}


async def _queue_status(conn, table: str) -> dict[str, Any]:
    return {
        "by_status": await _fetch_rows(
            conn,
            f"""
            select status, count(*) as tasks
            from {table}
            group by status
            order by status
            """,
        ),
        "due": await _fetchval(
            conn,
            f"""
            select count(*)
            from {table}
            where next_run_at <= now()
              and coalesce(backoff_until, '-infinity'::timestamptz) <= now()
              and status in ('pending', 'failed', 'success')
            """,
        ),
        "running": await _fetchval(
            conn,
            f"select count(*) from {table} where status = 'running'",
        ),
        "oldest_pending_since": await _fetchval(
            conn,
            f"select min(pending_since) from {table} where status in ('pending', 'failed')",
        ),
        "last_heartbeat_at": await _fetchval(
            conn,
            f"select max(lease_heartbeat_at) from {table}",
        ),
    }


async def _fetch_rows(conn, sql: str) -> list[dict[str, Any]]:
    try:
        rows = await asyncio.wait_for(conn.fetch(sql), timeout=1.5)
    except Exception as exc:
        return [{"error": str(exc)[:300]}]
    return [dict(row) for row in rows]


async def _fetchval(conn, sql: str):
    try:
        return await asyncio.wait_for(conn.fetchval(sql), timeout=1.5)
    except Exception as exc:
        return {"error": str(exc)[:300]}


async def _redis_status(redis_url: str) -> dict[str, Any]:
    try:
        import redis.asyncio as redis
    except ImportError:
        return {"ok": False, "error": "redis package is not installed"}
    try:
        client = redis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        )
        try:
            latency_ms = await _time_ping(client)
        finally:
            await client.aclose()
        return {"ok": True, "latency_ms": latency_ms}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:300]}


async def _time_ping(client) -> int:
    import time

    started = time.perf_counter()
    await client.ping()
    return int((time.perf_counter() - started) * 1000)
