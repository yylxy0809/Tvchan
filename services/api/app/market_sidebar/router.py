from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from app.core.security import TokenPrincipal, require_token
from app.market_sidebar.dto import BootstrapRequest
from app.market_sidebar.service import SidebarAggregator


router = APIRouter(tags=["market-sidebar"])


def get_sidebar_aggregator(request: Request) -> SidebarAggregator:
    repository = request.app.state.market_sidebar_repository
    aggregator = request.app.state.market_sidebar_aggregator
    if aggregator.repository is not repository:
        aggregator = SidebarAggregator(repository)
        request.app.state.market_sidebar_aggregator = aggregator
    return aggregator


@router.post("/market/sidebar/bootstrap")
async def bootstrap_sidebar(
    body: BootstrapRequest,
    _principal: TokenPrincipal = Depends(require_token),
    aggregator: SidebarAggregator = Depends(get_sidebar_aggregator),
) -> dict:
    return await aggregator.bootstrap(
        chart_symbol=body.chart_symbol,
        chart_epoch=body.chart_epoch,
        watchlist_id=body.watchlist_id,
        watchlist_revision=body.watchlist_revision,
        watchlist_symbols=body.watchlist_symbols,
    )
