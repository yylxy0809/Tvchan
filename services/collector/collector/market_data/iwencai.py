from __future__ import annotations
from dataclasses import dataclass,field
from datetime import date,datetime
from typing import Iterable,Protocol,Callable
from urllib.parse import urlsplit
import httpx
from .contracts import CapitalFlow,MarketDataResult,MarketStrength,NewsItem,NewsSource,Profile,ProviderError,Quote,Themes,Valuation
from .iwencai_contract import SchemaError,build_request,number,parse_rows,provider_timestamp,text
from .provider import UnifiedMarketDataProvider

@dataclass(frozen=True,slots=True)
class IwencaiApiKey:
    label:str="default"; key:str=field(default="",repr=False); enabled:bool=True; priority:int=0
    def __post_init__(self):
        if not self.key.strip(): raise ValueError("invalid Iwencai API key")

@dataclass(frozen=True,slots=True)
class IwencaiConfig:
    api_key:str=field(default="",repr=False); timeout_seconds:float=5.0; api_keys:tuple[IwencaiApiKey,...]=()
    def __post_init__(self):
        if not self.enabled_api_keys() or not 0<self.timeout_seconds<=5: raise ValueError("invalid Iwencai configuration")
    def enabled_api_keys(self):
        keys=self.api_keys or (IwencaiApiKey(key=self.api_key),)
        return tuple(sorted((key for key in keys if key.enabled),key=lambda key:key.priority))
class IwencaiTransport(Protocol):
    async def query(self,capability:str,query:str,*,limit:int=50)->tuple[dict[str,object],...]:...
class HttpxIwencaiTransport:
    def __init__(self,*,query_endpoint:str,news_endpoint:str,api_key:str="",api_keys:tuple[IwencaiApiKey,...]=(),timeout_seconds:float,allowed_hosts:tuple[str,...],transport=None):
        for endpoint in (query_endpoint,news_endpoint):
            parsed=urlsplit(endpoint)
            if parsed.scheme!="https" or not parsed.hostname or parsed.hostname.lower() not in allowed_hosts: raise ValueError("Iwencai endpoint host is not allowed")
        self._query_endpoint,self._news_endpoint,self._api_keys,self._timeout,self._transport=query_endpoint,news_endpoint,tuple(sorted((key for key in (api_keys or (IwencaiApiKey(key=api_key),)) if key.enabled),key=lambda key:key.priority)),timeout_seconds,transport;self._next=0;self._cooldowns={}
    async def query(self,capability,query,*,limit=50):
        now=__import__('time').monotonic(); keys=[key for key in self._api_keys if self._cooldowns.get(hash(key.key),0)<=now]
        if not keys: raise RuntimeError("Iwencai API key pool exhausted")
        start=0
        last=None
        for key in keys[start:]+keys[:start]:
            for attempt in range(2):
                try:
                    request=build_request(query,key.key,capability,limit=limit); endpoint=self._news_endpoint if capability=="news-search" else self._query_endpoint
                    async with httpx.AsyncClient(timeout=self._timeout,transport=self._transport,follow_redirects=False) as client:
                        response=await client.request(url=endpoint,**request); response.raise_for_status(); return parse_rows(response.json(),news=capability=="news-search")
                except (TimeoutError,httpx.TimeoutException) as exc:
                    last=exc
                    if attempt==0: continue
                except httpx.HTTPStatusError as exc:
                    last=exc
                    if exc.response.status_code in (401,403,429) or "quota" in exc.response.text.lower(): self._cooldowns[hash(key.key)]=__import__('time').monotonic()+300
                break
        raise RuntimeError("Iwencai API key pool exhausted") from last

class IwencaiSidebarProvider(UnifiedMarketDataProvider):
    def __init__(self,config:IwencaiConfig,transport:IwencaiTransport,*,today:Callable[[],date]|None=None): self._config,self._transport,self._today=config,transport,today or date.today
    async def get_quotes(self,symbols:Iterable[str]):
        symbols=tuple(dict.fromkeys(symbols))
        try:
            rows=await self._transport.query("hithink-market-query",f"{'、'.join(symbols)} 最新价 涨跌额 涨跌幅 成交量 成交额 换手率",limit=len(symbols))
            by={_symbol(r):r for r in rows}
            return {s:self._result(_quote(s,by.get(s))) for s in symbols}
        except Exception as exc:return {s:self._failed(exc) for s in symbols}
    async def get_profile(self,symbol): return await self._one("hithink-business-query",f"{symbol} 股票简称 交易所 所属行业 主营业务",lambda r:Profile(symbol,text(r,"股票简称","name"),text(r,"交易所","exchange"),text(r,"所属行业","industry"),text(r,"主营业务","business_summary")))
    async def get_valuation(self,symbol): return await self._one("hithink-finance-query",f"{symbol} 总市值 市盈率 市净率 市销率",lambda r:Valuation(symbol,number(r,"总市值","market_cap"),number(r,"市盈率","pe_ratio"),number(r,"市净率","pb_ratio"),number(r,"市销率","ps_ratio")))
    async def get_capital_flow(self,symbol): return await self._one("hithink-market-query",f"{symbol} 净流入 主力净流入 大单净流入 中单净流入 小单净流入",lambda r:CapitalFlow(symbol,number(r,"净流入","net_inflow"),number(r,"主力净流入","main_net_inflow"),number(r,"大单净流入","large_net_inflow"),number(r,"中单净流入","medium_net_inflow"),number(r,"小单净流入","small_net_inflow")))
    async def get_themes(self,symbol):
        return await self._one("hithink-industry-query",f"{symbol} 所属行业 所属概念",lambda r:Themes(symbol,text(r,"所属行业","industry"),_tuple_text(r,"所属概念","concepts"),text(r,"主营业务","business_summary")))
    async def get_market_strength(self):
        try:
            sector,index,stocks=await __import__('asyncio').gather(self._transport.query("hithink-sector-selector","今日热点板块 涨跌幅",limit=20),self._transport.query("hithink-zhishu-query","上证指数最新点位 涨跌幅",limit=1),self._transport.query("hithink-astock-selector","今日上涨 下跌 涨停 跌停股票",limit=20))
            first=index[0] if index else {}; stats=stocks[0] if stocks else {}
            return self._result(MarketStrength(themes=tuple(filter(None,(text(r,"板块名称","名称") for r in sector))),up_count=_int(stats,"上涨家数"),down_count=_int(stats,"下跌家数"),limit_up_count=_int(stats,"涨停家数"),limit_down_count=_int(stats,"跌停家数"),index_level=number(first,"最新点位"),index_change_percent=number(first,"涨跌幅")))
        except Exception as exc:return self._failed(exc)
    async def get_news(self,symbol,since=None):
        try:
            rows=await self._transport.query("news-search",f"{symbol} 最新新闻 公告 风险",limit=20); now=datetime.now().astimezone(); items=[]
            for row in rows:
                published=provider_timestamp(row); title=text(row,"title","标题")
                if not title or not published or (since and published<since):continue
                url=text(row,"url") or ""; source=text(row,"source_name","来源") or "同花顺问财"
                items.append(NewsItem(text(row,"id") or f"{symbol}:{published.isoformat()}:{title}",symbol,text(row,"category") or "news",title,text(row,"summary","摘要") or title,published,now,sources=(NewsSource(source,url),) if url else ()))
            return self._result(tuple(items),allow_empty=True)
        except Exception as exc:return self._failed(exc)
    async def _one(self,capability,query,make):
        try:
            rows=await self._transport.query(capability,query,limit=1); return self._result(make(rows[0])) if rows else self._failed(None)
        except Exception as exc:return self._failed(exc)
    def _result(self,value,allow_empty=False): return MarketDataResult.available(value,trading_date=self._today(),provider_ts=None) if allow_empty or _meaningful(value) else self._failed(None)
    def _failed(self,exc): return MarketDataResult.unavailable(error=_error(exc),trading_date=self._today())
def _symbol(r):
    value=text(r,"股票代码","symbol","code") or ""; return value.upper()
def _quote(s,r): return None if r is None else Quote(s,number(r,"最新价","price"),number(r,"涨跌额","change"),number(r,"最新涨跌幅","涨跌幅","change_percent"),number(r,"成交量","volume"),number(r,"成交额","amount"),number(r,"换手率","turnover_rate"))
def _tuple_text(r,*keys):
    value=text(r,*keys); return tuple(x.strip() for x in value.replace("，",",").split(",") if x.strip()) if value else ()
def _int(r,*keys):
    value=number(r,*keys); return int(value) if value is not None else None
def _error(exc):
    if isinstance(exc,(TimeoutError,httpx.TimeoutException)):return ProviderError.TIMEOUT
    if isinstance(exc,SchemaError):return ProviderError.SCHEMA
    if isinstance(exc,httpx.HTTPStatusError):
        response_text=exc.response.text
        quota_exhausted=any(marker in response_text for marker in ("次数已用完","升级权益","quota","rate limit"))
        if exc.response.status_code==429 or quota_exhausted:return ProviderError.RATE_LIMITED
        if exc.response.status_code in (401,403):return ProviderError.AUTHENTICATION
        return ProviderError.UNAVAILABLE
    return ProviderError.UNAVAILABLE

def _meaningful(value):
    if value is None:return False
    fields=getattr(value,"__dataclass_fields__",{})
    if fields:
        return any(getattr(value,name) not in (None,"",(),[]) for name in fields if name!="symbol")
    return value not in (None,"",(),[])
