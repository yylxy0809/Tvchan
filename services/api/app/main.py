from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.db import lifespan
from app.market_sidebar import router as market_sidebar_router
from app.market_sidebar.repository import RedisSidebarSnapshotRepository
from app.market_sidebar.service import SidebarAggregator
from app.routes.registry import register_routes


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
    register_routes(app)
    repository = RedisSidebarSnapshotRepository(settings.redis_url)
    app.state.market_sidebar_repository = repository
    app.state.market_sidebar_aggregator = SidebarAggregator(repository)
    app.include_router(market_sidebar_router, prefix="/api/v3")
    return app


app = create_app()
