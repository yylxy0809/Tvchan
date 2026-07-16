import asyncio,json
from datetime import UTC,date,datetime
from collector.market_data import Freshness,MarketDataMetadata,MarketDataResult,NewsItem,NewsSource
from collector.market_data.trading_day_cache import CacheKey,RedisTradingDayCache
class Redis:
    def __init__(self):self.values={};self.expirations={}
    async def get(self,key):return self.values.get(key)
    async def set(self,key,value,**kwargs):self.values[key]=value;self.expirations[key]=kwargs.get("ex");return True
def test_flat_cache_wire_is_api_and_frontend_consumable_with_nested_news():
    redis=Redis();cache=RedisTradingDayCache(redis);ts=datetime(2026,7,10,9,30,tzinfo=UTC);item=NewsItem("n1","000001.SZ","company","标题","摘要",ts,ts,sources=(NewsSource("证券时报","https://example.cn/1"),));result=MarketDataResult.available((item,),trading_date=date(2026,7,10),provider_ts=ts)
    asyncio.run(cache.put(CacheKey(date(2026,7,10),"news","000001.SZ"),result));payload=json.loads(redis.values["sidebar:iwencai:2026-07-10:news:000001.SZ"])
    assert payload["source"]=="iwencai" and payload["freshness"]=="fresh" and payload["trading_date"]=="2026-07-10" and payload["as_of"]==ts.isoformat()
    assert payload["items"][0]["sources"][0]["name"]=="证券时报" and isinstance(payload["snapshot_version"],str)
    assert "value" not in payload

def test_cache_wire_preserves_notte_metadata_source():
    redis=Redis();cache=RedisTradingDayCache(redis);ts=datetime(2026,7,10,9,30,tzinfo=UTC)
    result=MarketDataResult({"symbol":"000001.SZ"},MarketDataMetadata(source="notte",trading_date=date(2026,7,10),provider_ts=ts,freshness=Freshness.FRESH))
    asyncio.run(cache.put(CacheKey(date(2026,7,10),"quote","000001.SZ"),result))
    assert json.loads(redis.values["sidebar:iwencai:2026-07-10:quote:000001.SZ"])["source"]=="notte"

def test_unavailable_cache_entries_expire_quickly_so_upstream_can_retry():
    redis=Redis();cache=RedisTradingDayCache(redis);key=CacheKey(date(2026,7,10),"strength","market")
    asyncio.run(cache.put(key,MarketDataResult.unavailable(trading_date=date(2026,7,10))))
    assert redis.expirations[key.redis_key()]==60

def test_fresh_cache_entries_are_retained_across_the_trading_day():
    redis=Redis();cache=RedisTradingDayCache(redis);key=CacheKey(date(2026,7,10),"quote","000001.SZ");ts=datetime(2026,7,10,9,30,tzinfo=UTC)
    result=MarketDataResult({"symbol":"000001.SZ"},MarketDataMetadata(trading_date=date(2026,7,10),provider_ts=ts,freshness=Freshness.FRESH))
    asyncio.run(cache.put(key,result))
    assert redis.expirations[key.redis_key()]==16*86400
