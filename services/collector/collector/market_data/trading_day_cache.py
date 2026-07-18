from __future__ import annotations
import json, secrets
from dataclasses import asdict,is_dataclass,dataclass
from datetime import UTC,date,datetime,timedelta
from enum import Enum
from typing import Any,Protocol
from zoneinfo import ZoneInfo
from .contracts import Freshness,MarketDataMetadata,MarketDataResult,ProviderError
SHANGHAI=ZoneInfo("Asia/Shanghai"); RETENTION_SECONDS=16*86400; UNAVAILABLE_RETENTION_SECONDS=60; LEASE_SECONDS=120
RELEASE_LEASE_SCRIPT='''
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
'''
class TradingCalendar(Protocol):
    def trading_day(self,now:datetime|None=None)->date:...
    def is_trading_day(self,now:datetime|None=None)->bool:...
class WeekdayTradingCalendar:
    def trading_day(self,now=None):
        current=(now or datetime.now(UTC)).astimezone(SHANGHAI).date()
        while current.weekday()>=5:current-=timedelta(days=1)
        return current
    def is_trading_day(self,now=None):return (now or datetime.now(UTC)).astimezone(SHANGHAI).weekday()<5
@dataclass(frozen=True,slots=True)
class CacheKey:
    trading_date:date; domain:str; subject:str
    def redis_key(self):return f"sidebar:iwencai:{self.trading_date.isoformat()}:{self.domain}:{self.subject}"
    def lease_key(self):return self.redis_key()+":lease"
class MemoryTradingDayCache:
    def __init__(self):self._items={}
    async def get(self,key):return self._items.get(key)
    async def put(self,key,value):self._items[key]=value;return True
    async def latest(self,domain,subject):
        keys=[k for k in self._items if k.domain==domain and k.subject==subject];return self._items[max(keys,key=lambda k:k.trading_date)] if keys else None
    async def acquire(self,key):return "memory"
    async def release(self,key,token):pass
class RedisTradingDayCache:
    def __init__(self,redis):self._redis=redis
    async def get(self,key):return decode_result(await self._redis.get(key.redis_key()))
    async def put(self,key,value):
        payload=json.dumps(wire_payload(value),ensure_ascii=False,separators=(",",":")); previous=await self._redis.get(key.redis_key())
        if isinstance(previous,bytes):previous=previous.decode()
        if previous==payload:return False
        ttl=UNAVAILABLE_RETENTION_SECONDS if value.metadata.freshness is Freshness.UNAVAILABLE else RETENTION_SECONDS
        await self._redis.set(key.redis_key(),payload,ex=ttl);return True
    async def latest(self,domain,subject):
        today=datetime.now(UTC).astimezone(SHANGHAI).date()
        for offset in range(0,17):
            value=await self.get(CacheKey(today-timedelta(days=offset),domain,subject))
            if value is not None and value.metadata.freshness is not Freshness.UNAVAILABLE:return value
        return None
    async def acquire(self,key):
        token=secrets.token_hex(16); ok=await self._redis.set(key.lease_key(),token,nx=True,ex=LEASE_SECONDS);return token if ok else None
    async def release(self,key,token):
        await self._redis.eval(RELEASE_LEASE_SCRIPT,1,key.lease_key(),token)
def wire_payload(result):
    value=_json_value(result.value)
    if isinstance(value,dict):payload=dict(value)
    elif isinstance(value,list):payload={"items":value}
    elif value is None:payload={}
    else:payload={"value":value}
    payload.update(source=result.metadata.source,freshness=result.metadata.freshness.value,as_of=(result.metadata.provider_ts or result.metadata.received_at).isoformat(),trading_date=result.metadata.trading_date.isoformat() if result.metadata.trading_date else None,snapshot_version=result.metadata.snapshot_version or _version(value))
    if result.error:payload["error"]=result.error.value
    return payload
def decode_result(raw):
    if isinstance(raw,bytes):raw=raw.decode()
    if not isinstance(raw,str):return None
    try:p=json.loads(raw); td=date.fromisoformat(p["trading_date"]) if p.get("trading_date") else None; as_of=datetime.fromisoformat(p["as_of"]); error=ProviderError(p["error"]) if p.get("error") else None
    except (ValueError,TypeError,KeyError,json.JSONDecodeError):return None
    metadata_keys={"source","freshness","as_of","trading_date","snapshot_version","error"}; value={k:v for k,v in p.items() if k not in metadata_keys}
    return MarketDataResult(value,MarketDataMetadata(source=p["source"],trading_date=td,provider_ts=as_of,received_at=as_of,freshness=Freshness(p["freshness"]),snapshot_version=p.get("snapshot_version")),error)
def stale_or_unavailable(value,trading_date=None):return value.as_stale() if value is not None and value.value is not None else MarketDataResult.unavailable(trading_date=trading_date)
def _json_value(value):
    if is_dataclass(value):return {k:_json_value(v) for k,v in asdict(value).items()}
    if isinstance(value,(tuple,list)):return [_json_value(v) for v in value]
    if isinstance(value,dict):return {str(k):_json_value(v) for k,v in value.items()}
    if isinstance(value,(datetime,date)):return value.isoformat()
    if isinstance(value,Enum):return value.value
    return value
def _version(value):
    import hashlib
    return hashlib.sha256(json.dumps(_json_value(value),sort_keys=True,ensure_ascii=False,separators=(",",":")).encode()).hexdigest()[:16]
