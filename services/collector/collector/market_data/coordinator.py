from __future__ import annotations
import asyncio
from dataclasses import replace
from .contracts import MarketDataResult,MarketDataSnapshot,SidebarContext
from .trading_day_cache import CacheKey,MemoryTradingDayCache,WeekdayTradingCalendar,stale_or_unavailable
class MarketDataCoordinator:
    def __init__(self,provider,*,cache=None,calendar=None,publish=None):self._provider,self._cache,self._calendar,self._publish=provider,cache or MemoryTradingDayCache(),calendar or WeekdayTradingCalendar(),publish;self._inflight={};self._quote_inflight={};self.changed=False
    @property
    def cache(self):return self._cache
    async def ensure_snapshot(self,domain,subject):
        if domain=="quote":return (await self.ensure_quotes((subject,)))[subject]
        day=self._calendar.trading_day();key=CacheKey(day,domain,subject);cached=await self._cache.get(key)
        if cached is not None:return cached
        task=self._inflight.get(key)
        if task is None:task=asyncio.create_task(self._fill(key));self._inflight[key]=task
        try:return await asyncio.shield(task)
        finally:
            if task.done() and self._inflight.get(key) is task:self._inflight.pop(key,None)
    async def ensure_quotes(self,symbols):
        day=self._calendar.trading_day();results={};missing=[]
        for symbol in tuple(dict.fromkeys(symbols)):
            cached=await self._cache.get(CacheKey(day,"quote",symbol))
            if cached is None:missing.append(symbol)
            else:results[symbol]=cached
        if missing:
            flight_key=(day,tuple(sorted(missing)))
            existing=self._quote_inflight.get(flight_key)
            if existing is not None:
                await asyncio.shield(existing)
                return await self.ensure_quotes(symbols)
            task=asyncio.create_task(self._fill_quotes(day,missing,results));self._quote_inflight[flight_key]=task
            try:await asyncio.shield(task)
            finally:
                if task.done() and self._quote_inflight.get(flight_key) is task:self._quote_inflight.pop(flight_key,None)
            return results
        for symbol in missing:
            if symbol not in results:results[symbol]=stale_or_unavailable(await self._cache.latest("quote",symbol),day)
        return results
    async def _fill_quotes(self,day,missing,results):
            leader_key=CacheKey(day,"quote-batch","-".join(sorted(missing)));token=await self._cache.acquire(leader_key)
            if token:
                try:
                    fetched=await self._provider.get_quotes(missing)
                    for symbol in missing:
                        result=_for_trading_day(fetched.get(symbol) or MarketDataResult.unavailable(trading_date=day),day);self.changed=(await self._cache.put(CacheKey(day,"quote",symbol),result)) or self.changed;results[symbol]=result
                finally:await self._cache.release(leader_key,token)
            else:
                for symbol in missing:results[symbol]=await self._cache.get(CacheKey(day,"quote",symbol)) or stale_or_unavailable(await self._cache.latest("quote",symbol),day)
            for symbol in missing:
                if symbol not in results:results[symbol]=stale_or_unavailable(await self._cache.latest("quote",symbol),day)
    async def load_context(self,context):
        self.changed=False
        prepare = getattr(self._provider,"prepare_context",None)
        if prepare is not None:await prepare(context)
        symbols=tuple(dict.fromkeys((context.chart_symbol,*context.watchlist_symbols)));quotes=await self.ensure_quotes(symbols)
        profile,valuation,flow,themes,strength,news=await asyncio.gather(*(self.ensure_snapshot(d,s) for d,s in (("profile",context.chart_symbol),("valuation",context.chart_symbol),("capital_flow",context.chart_symbol),("themes",context.chart_symbol),("strength","market"),("news",context.chart_symbol))))
        return MarketDataSnapshot(context,quotes[context.chart_symbol],profile,valuation,themes,{s:quotes[s] for s in context.watchlist_symbols},flow,strength,news)
    async def _fill(self,key):
        token=await self._cache.acquire(key)
        if not token:return await self._cache.get(key) or stale_or_unavailable(await self._cache.latest(key.domain,key.subject),key.trading_date)
        try:
            try:result=await self._fetch(key)
            except Exception:result=MarketDataResult.unavailable(trading_date=key.trading_date)
            result=_for_trading_day(result,key.trading_date)
            changed=await self._cache.put(key,result);self.changed=changed or self.changed
            if changed and self._publish:await self._publish(key.domain,result)
            return result if result.value is not None else stale_or_unavailable(await self._cache.latest(key.domain,key.subject),key.trading_date)
        finally:await self._cache.release(key,token)
    async def _fetch(self,key):
        if key.domain=="profile":return await self._provider.get_profile(key.subject)
        if key.domain=="valuation":return await self._provider.get_valuation(key.subject)
        if key.domain=="capital_flow":return await self._provider.get_capital_flow(key.subject)
        if key.domain=="themes":return await self._provider.get_themes(key.subject)
        if key.domain=="strength":return await self._provider.get_market_strength()
        if key.domain=="news":return await self._provider.get_news(key.subject)
        raise ValueError("unknown domain")

def _for_trading_day(result, trading_day):
    return replace(result, metadata=replace(result.metadata, trading_date=trading_day))
