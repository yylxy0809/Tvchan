from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.db import lifespan
from app.routes import admin, auth, bars, chan, chart, chart_ws, health, history, realtime, screener, symbols


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="TradingView A Share Local API",
        version="0.1.0",
        description="Phase 0/1 API for local TradingView A-share development.",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.cors_origins),
        allow_origin_regex=settings.cors_origin_regex,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(admin.router, prefix="/api/v1")
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
    return app


app = create_app()
