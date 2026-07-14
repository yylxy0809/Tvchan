from __future__ import annotations
import math, secrets
from collections.abc import Mapping
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit

QUERY_PATH = "/v1/query2data"
NEWS_PATH = "/v1/comprehensive/search"
MAX_ROWS = 500
MAX_TEXT = 2048
CAPABILITIES = frozenset({"hithink-market-query", "hithink-business-query", "hithink-finance-query", "hithink-industry-query", "hithink-sector-selector", "hithink-astock-selector", "hithink-zhishu-query", "news-search"})

class SchemaError(ValueError): pass

def build_endpoint(base_url: str, allowed_hosts: tuple[str, ...], *, news: bool = False) -> str:
    parsed = urlsplit(base_url); host = parsed.hostname.lower() if parsed.hostname else None
    if parsed.scheme != "https" or not host or parsed.username or parsed.password or parsed.port not in (None,443) or parsed.query or parsed.fragment or parsed.path not in ("","/") or host not in allowed_hosts: raise ValueError("Iwencai base URL must be an allowed HTTPS origin")
    return urlunsplit(("https", parsed.netloc, NEWS_PATH if news else QUERY_PATH, "", ""))

def build_request(query: str, api_key: str, capability: str, *, limit: int = 50) -> dict[str,object]:
    if capability not in CAPABILITIES or not query or len(query)>MAX_TEXT or not 0<limit<=MAX_ROWS: raise ValueError("invalid Iwencai request")
    headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json","X-Claw-Call-Type":"normal","X-Claw-Skill-Id":capability,"X-Claw-Skill-Version":"1.0.0","X-Claw-Plugin-Id":"none","X-Claw-Plugin-Version":"none","X-Claw-Trace-Id":secrets.token_hex(32)}
    body={"query":query,"channels":["news"],"app_id":"AIME_SKILL","size":limit} if capability=="news-search" else {"query":query,"page":"1","limit":str(limit),"is_cache":"1","expand_index":"true"}
    return {"method":"POST","headers":headers,"json":body}

def parse_rows(payload: object, *, news: bool = False) -> tuple[dict[str,object],...]:
    if not isinstance(payload,Mapping): raise SchemaError("invalid Iwencai response")
    rows = payload.get("data") if news else payload.get("datas")
    if news and payload.get("status_code") != 0: raise SchemaError("invalid news response")
    if not isinstance(rows,list) or len(rows)>MAX_ROWS: raise SchemaError("invalid Iwencai rows")
    if any(not isinstance(row,Mapping) for row in rows):raise SchemaError("invalid Iwencai row")
    return tuple({str(k):_safe(v) for k,v in row.items() if isinstance(k,str) and len(k)<=100} for row in rows)

def number(row: Mapping[str,object], *keys:str)->float|None:
    value=_field(row,keys)
    if isinstance(value,str): value=value.strip().replace(",","").rstrip("%")
    try: result=float(value)
    except (TypeError,ValueError): return None
    return result if math.isfinite(result) else None
def text(row: Mapping[str,object], *keys:str)->str|None:
    value=_field(row,keys); return value.strip() if isinstance(value,str) and value.strip() and len(value)<=MAX_TEXT else None
def provider_timestamp(row:Mapping[str,object])->datetime|None:
    value=text(row,"as_of","更新时间","时间","publish_time")
    if not value:return None
    try: parsed=datetime.fromisoformat(value.replace("Z","+00:00"))
    except ValueError:return None
    return parsed if parsed.tzinfo else None
def _field(row,keys): return next((row[k] for k in keys if k in row),None)
def _safe(value):
    if value is None or isinstance(value,(bool,int)): return value
    if isinstance(value,float):
        if not math.isfinite(value): raise SchemaError("non-finite number")
        return value
    if isinstance(value,str) and len(value)<=MAX_TEXT and "\x00" not in value:return value
    if isinstance(value,Mapping): return {str(k):_safe(v) for k,v in value.items() if isinstance(k,str)}
    if isinstance(value,list) and len(value)<=100:return [_safe(v) for v in value]
    raise SchemaError("invalid field")

# Backward-compatible names used by construction tests.
build_query_endpoint = build_endpoint
def build_query_request(query,api_key,*,limit=50): return build_request(query,api_key,"hithink-market-query",limit=limit)
