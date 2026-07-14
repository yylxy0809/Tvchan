from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, Request

from app.core.security import TokenPrincipal, require_token
from app.market_sidebar.dto import BootstrapRequest, SidebarBootstrapResponse
from app.market_sidebar.service import SidebarAggregator, SidebarContext


router = APIRouter(tags=["market-sidebar"])


def get_sidebar_aggregator(request: Request) -> SidebarAggregator:
    repository = request.app.state.market_sidebar_repository
    set_db_pool = getattr(repository, "set_db_pool", None)
    if set_db_pool is not None:
        set_db_pool(getattr(request.app.state, "db_pool", None))
    aggregator = request.app.state.market_sidebar_aggregator
    if aggregator.repository is not repository:
        aggregator = SidebarAggregator(repository)
        request.app.state.market_sidebar_aggregator = aggregator
    return aggregator


@router.post("/market/sidebar/bootstrap", response_model=SidebarBootstrapResponse)
async def bootstrap_sidebar(
    body: BootstrapRequest,
    _principal: TokenPrincipal = Depends(require_token),
    aggregator: SidebarAggregator = Depends(get_sidebar_aggregator),
) -> dict:
    snapshot = await aggregator.bootstrap(
        chart_symbol=body.chart_symbol,
        chart_epoch=body.chart_epoch,
        watchlist_id=body.watchlist_id,
        watchlist_revision=body.watchlist_revision,
        watchlist_symbols=body.watchlist_symbols,
    )
    # HTTP bootstrap must never wait for iWencai or the collector.
    context = SidebarContext(
        connection_id="bootstrap",
        subscription_id="bootstrap",
        chart_symbol=body.chart_symbol,
        chart_epoch=body.chart_epoch,
        watchlist_symbols=tuple(body.watchlist_symbols),
        channels=frozenset(),
        watchlist_id=body.watchlist_id,
        watchlist_revision=body.watchlist_revision,
    )
    asyncio.create_task(aggregator.request_refresh(context, "bootstrap"))
    return snapshot
