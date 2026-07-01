from __future__ import annotations

from fastapi import FastAPI

from app.routes import (
    admin,
    auth,
    bars,
    chan,
    chart,
    chart_ws,
    health,
    history,
    realtime,
    runtime_config,
    screener,
    symbols,
    user_settings,
)


def register_routes(app: FastAPI) -> None:
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(admin.router, prefix="/api/v1")
    app.include_router(runtime_config.router, prefix="/api/v1")
    app.include_router(runtime_config.admin_router, prefix="/api/v1")
    app.include_router(user_settings.router, prefix="/api/v1")
    app.include_router(symbols.router, prefix="/api/v1")
    app.include_router(bars.router, prefix="/api/v1")
    app.include_router(chan.router, prefix="/api/v1")
    app.include_router(screener.router, prefix="/api/v1")
    app.include_router(chart.router, prefix="/api/v1")
    app.include_router(chart.v2_router, prefix="/api/v2")
    app.include_router(chart.v3_router, prefix="/api/v3")
    app.include_router(history.router, prefix="/api/v1")
    app.include_router(realtime.router)
    app.include_router(chart_ws.router)
