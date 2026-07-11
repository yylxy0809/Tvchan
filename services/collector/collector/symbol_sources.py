from __future__ import annotations

import asyncio
import html
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Iterable

import httpx

from collector.providers.factory import ProviderFactoryConfig, create_single_provider, normalize_provider_name
from trading_protocol import SymbolInfo

CNINFO_STOCK_URL = "http://www.cninfo.com.cn/new/data/szse_stock.json"
SSE_STOCK_URL = "https://query.sse.com.cn/security/stock/getStockListData2.do"
SZSE_STOCK_URL = "https://www.szse.cn/api/report/ShowReport/data"
BSE_LIST_PAGE_URL = "https://www.bse.cn/nq/listedcompany.html"
BSE_STOCK_URL = "https://www.bse.cn/nqxxController/nqxxCnzq.do"
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q="
EASTMONEY_CLIST_URLS = (
    "https://push2.eastmoney.com/api/qt/clist/get",
    "https://82.push2.eastmoney.com/api/qt/clist/get",
)
EASTMONEY_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81"


@dataclass(frozen=True)
class SourceDiscovery:
    source: str
    symbols: list[SymbolInfo]
    error: str | None = None


@dataclass(frozen=True)
class ConsensusDiscovery:
    symbols: list[SymbolInfo]
    source_counts: dict[str, int]
    source_errors: dict[str, str]
    confirmation_counts: dict[str, int]
    candidate_count: int
    min_confirmations: int


async def discover_consensus_symbols(
    *,
    sources: list[str],
    exchanges: set[str],
    min_confirmations: int,
    timeout: float,
    tdx_host: str | None,
    tdx_port: int,
    tdx_timeout: int,
    tdx_retries: int,
) -> ConsensusDiscovery:
    normalized_sources = [normalize_symbol_source_name(item) for item in sources]
    list_source_names = [item for item in normalized_sources if item != "tencent"]
    if not list_source_names:
        raise RuntimeError("At least one list source is required before tencent confirmation")

    discoveries: list[SourceDiscovery] = []
    for source in list_source_names:
        discoveries.append(
            await discover_symbol_source(
                source,
                exchanges=exchanges,
                timeout=timeout,
                tdx_host=tdx_host,
                tdx_port=tdx_port,
                tdx_timeout=tdx_timeout,
                tdx_retries=tdx_retries,
            )
        )

    by_symbol: dict[str, SymbolInfo] = {}
    source_votes: dict[str, set[str]] = defaultdict(set)
    source_counts: dict[str, int] = {}
    source_errors: dict[str, str] = {}
    for discovery in discoveries:
        source_counts[discovery.source] = len(discovery.symbols)
        if discovery.error:
            source_errors[discovery.source] = discovery.error
        for symbol in discovery.symbols:
            if symbol.exchange not in exchanges:
                continue
            key = symbol.symbol.upper()
            by_symbol[key] = prefer_named_symbol(by_symbol.get(key), symbol)
            source_votes[key].add(discovery.source)

    if "tencent" in normalized_sources:
        confirmed = await confirm_symbols_with_tencent(
            by_symbol.values(),
            exchanges=exchanges,
            timeout=timeout,
        )
        source_counts["tencent"] = len(confirmed.symbols)
        if confirmed.error:
            source_errors["tencent"] = confirmed.error
        for symbol in confirmed.symbols:
            key = symbol.symbol.upper()
            by_symbol[key] = prefer_named_symbol(by_symbol.get(key), symbol)
            source_votes[key].add("tencent")

    agreed: list[SymbolInfo] = []
    confirmation_counter: Counter[str] = Counter()
    for key, sources_for_symbol in source_votes.items():
        confirmation_counter[str(len(sources_for_symbol))] += 1
        if len(sources_for_symbol) < min_confirmations:
            continue
        symbol = by_symbol[key]
        agreed.append(
            SymbolInfo(
                symbol=symbol.symbol,
                code=symbol.code,
                exchange=symbol.exchange,
                name=symbol.name or symbol.symbol,
                asset_type="stock",
                market="A_SHARE",
                is_active=True,
            )
        )
    return ConsensusDiscovery(
        symbols=sorted(agreed, key=lambda item: item.symbol),
        source_counts=source_counts,
        source_errors=source_errors,
        confirmation_counts=dict(sorted(confirmation_counter.items())),
        candidate_count=len(by_symbol),
        min_confirmations=min_confirmations,
    )


async def discover_symbol_source(
    source: str,
    *,
    exchanges: set[str],
    timeout: float,
    tdx_host: str | None,
    tdx_port: int,
    tdx_timeout: int,
    tdx_retries: int,
) -> SourceDiscovery:
    try:
        if source == "sse":
            return SourceDiscovery(source, await list_sse_symbols(exchanges=exchanges, timeout=timeout))
        if source == "szse":
            try:
                return SourceDiscovery(source, await list_szse_symbols(exchanges=exchanges, timeout=timeout))
            except Exception as exc:
                fallback_exchanges = exchanges & {"SZ"}
                if not fallback_exchanges:
                    raise
                fallback = await list_cninfo_symbols(exchanges=fallback_exchanges, timeout=timeout)
                return SourceDiscovery(source, fallback, f"szse failed; used cninfo fallback: {exc}")
        if source == "bse":
            return SourceDiscovery(source, await list_bse_symbols(exchanges=exchanges, timeout=timeout))
        if source == "cninfo":
            return SourceDiscovery(source, await list_cninfo_symbols(exchanges=exchanges, timeout=timeout))
        if source == "eastmoney":
            return SourceDiscovery(source, await list_eastmoney_symbols(exchanges=exchanges, timeout=timeout))
        if source in {"pytdx", "mootdx", "seed"}:
            provider = create_single_provider(
                source,
                ProviderFactoryConfig(
                    names=[source],
                    tdx_host=tdx_host,
                    tdx_port=tdx_port,
                    tdx_timeout=tdx_timeout,
                    tdx_retries=tdx_retries,
                    http_timeout=timeout,
                ),
            )
            symbols = await asyncio.wait_for(provider.list_symbols(), timeout=max(timeout, tdx_timeout + 2))
            return SourceDiscovery(source, normalize_symbols(symbols, exchanges=exchanges))
        raise RuntimeError(f"Unsupported symbol source: {source}")
    except Exception as exc:
        return SourceDiscovery(source, [], str(exc)[:500])


async def list_sse_symbols(*, exchanges: set[str], timeout: float) -> list[SymbolInfo]:
    if "SH" not in exchanges:
        return []
    headers = {**http_headers(), "Referer": "https://www.sse.com.cn/"}
    symbols: list[SymbolInfo] = []
    async with httpx.AsyncClient(timeout=max(timeout, 20), follow_redirects=True, trust_env=False) as client:
        page = 1
        page_size = 1000
        total = None
        while total is None or len(symbols) < total:
            payload = await get_json_with_retries(
                client,
                SSE_STOCK_URL,
                headers=headers,
                params={
                    "jsonCallBack": "",
                    "isPagination": "true",
                    "stockCode": "",
                    "csrcCode": "",
                    "areaName": "",
                    "stockType": "10",
                    "pageHelp.cacheSize": "1",
                    "pageHelp.beginPage": str(page),
                    "pageHelp.pageSize": str(page_size),
                    "pageHelp.pageNo": str(page),
                    "pageHelp.endPage": str(page),
                },
                attempts=5,
            )
            total = int((payload.get("pageHelp") or {}).get("total") or 0)
            rows = payload.get("result") or []
            if not rows:
                break
            for item in rows:
                code = str(item.get("SECURITY_CODE_A") or "").strip()
                name = clean_symbol_name(str(item.get("SECURITY_ABBR_A") or item.get("COMPANY_ABBR") or code))
                if is_a_share_code(code, "SH"):
                    symbols.append(make_symbol(code, "SH", name))
            page += 1
            await asyncio.sleep(0.05)
    return dedupe_symbols(symbols)


async def list_szse_symbols(*, exchanges: set[str], timeout: float) -> list[SymbolInfo]:
    if "SZ" not in exchanges:
        return []
    headers = {
        **http_headers(),
        "Referer": "https://www.szse.cn/market/product/stock/list/index.html",
        "Accept": "application/json,text/javascript,*/*;q=0.01",
        "Connection": "close",
        "X-Requested-With": "XMLHttpRequest",
    }
    symbols: list[SymbolInfo] = []
    async with httpx.AsyncClient(timeout=max(timeout, 20), follow_redirects=True, trust_env=False) as client:
        page = 1
        page_count = 1
        while page <= page_count:
            payload = await get_json_with_retries(
                client,
                SZSE_STOCK_URL,
                headers=headers,
                params={
                    "SHOWTYPE": "JSON",
                    "CATALOGID": "1110",
                    "TABKEY": "tab1",
                    "txtDMorJC": "",
                    "PAGENO": str(page),
                    "random": "0.123456",
                },
                attempts=6,
            )
            tab = find_szse_a_share_tab(payload)
            metadata = tab.get("metadata") or {}
            page_count = int(metadata.get("pagecount") or page_count)
            for item in tab.get("data") or []:
                code = str(item.get("agdm") or "").strip()
                name = clean_symbol_name(str(item.get("agjc") or code))
                if is_a_share_code(code, "SZ"):
                    symbols.append(make_symbol(code, "SZ", name))
            page += 1
            await asyncio.sleep(0.08)
    return dedupe_symbols(symbols)


async def list_bse_symbols(*, exchanges: set[str], timeout: float) -> list[SymbolInfo]:
    if "BJ" not in exchanges:
        return []
    headers = {
        **http_headers(),
        "Referer": BSE_LIST_PAGE_URL,
    }
    symbols: list[SymbolInfo] = []
    async with httpx.AsyncClient(timeout=max(timeout, 20), follow_redirects=True, trust_env=False, headers=headers) as client:
        await client.get(BSE_LIST_PAGE_URL)
        page = 0
        page_count = 1
        while page < page_count:
            response = await client.get(
                BSE_STOCK_URL,
                params={
                    "callback": "callback",
                    "page": str(page),
                    "typejb": "T",
                    "xxfcbj[]": "2",
                    "xxzqdm": "",
                    "sortfield": "xxzqdm",
                    "sorttype": "asc",
                },
            )
            response.raise_for_status()
            payload = parse_bse_jsonp(response.content.decode("utf-8", "replace"))
            page_count = int(payload.get("totalPages") or page_count)
            for item in payload.get("content") or []:
                code = str(item.get("xxzqdm") or "").strip()
                name = clean_symbol_name(str(item.get("xxzqjc") or code))
                if is_a_share_code(code, "BJ"):
                    symbols.append(make_symbol(code, "BJ", name))
            page += 1
            await asyncio.sleep(0.01)
    return dedupe_symbols(symbols)


async def list_cninfo_symbols(*, exchanges: set[str], timeout: float) -> list[SymbolInfo]:
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, trust_env=False) as client:
        response = await client.get(CNINFO_STOCK_URL, headers=http_headers())
        response.raise_for_status()
        payload = response.json()
    symbols: list[SymbolInfo] = []
    for item in payload.get("stockList") or []:
        code = str(item.get("code") or "").strip()
        name = str(item.get("zwjc") or code).strip()
        if item.get("category") != "\u0041\u80a1":
            continue
        if "\u9000" in name:
            continue
        exchange = infer_exchange(code)
        if exchange not in exchanges:
            continue
        symbols.append(make_symbol(code, exchange, name))
    return dedupe_symbols(symbols)


async def get_json_with_retries(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    attempts: int = 3,
):
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt == attempts - 1:
                break
            await asyncio.sleep(0.4 * (attempt + 1))
    raise RuntimeError(f"request failed for {url}: {last_error}")


def find_szse_a_share_tab(payload) -> dict:
    for item in payload or []:
        metadata = item.get("metadata") or {}
        if metadata.get("tabkey") == "tab1":
            return item
    raise RuntimeError("szse A-share tab is missing from response")


def parse_bse_jsonp(text: str) -> dict:
    match = re.search(r"callback\((.*)\)\s*$", text, re.S)
    if not match:
        raise RuntimeError("bse response is not callback JSONP")
    payload = json.loads(match.group(1))
    if not payload:
        raise RuntimeError("bse response is empty")
    return payload[0]


def clean_symbol_name(value: str) -> str:
    text = re.sub(r"<[^>]+>", "", value)
    return html.unescape(text).strip()


async def list_eastmoney_symbols(*, exchanges: set[str], timeout: float) -> list[SymbolInfo]:
    last_error: Exception | None = None
    for url in EASTMONEY_CLIST_URLS:
        try:
            return await _list_eastmoney_symbols_once(url=url, exchanges=exchanges, timeout=timeout)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"eastmoney symbol list failed: {last_error}")


async def _list_eastmoney_symbols_once(*, url: str, exchanges: set[str], timeout: float) -> list[SymbolInfo]:
    headers = {**http_headers(), "Referer": "https://quote.eastmoney.com/center/gridlist.html"}
    params = {
        "pn": 1,
        "pz": 5000,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": EASTMONEY_FS,
        "fields": "f12,f13,f14",
    }
    symbols: list[SymbolInfo] = []
    async with httpx.AsyncClient(timeout=timeout, trust_env=False, headers=headers) as client:
        page = 1
        total = None
        while True:
            response = await client.get(url, params={**params, "pn": page})
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data") or {}
            rows = data.get("diff") or []
            total = int(data.get("total") or total or 0)
            for item in rows:
                code = str(item.get("f12") or "").strip()
                name = str(item.get("f14") or code).strip()
                if "\u9000" in name:
                    continue
                exchange = infer_exchange(code)
                if exchange not in exchanges:
                    continue
                symbols.append(make_symbol(code, exchange, name))
            if not rows or len(symbols) >= total or page * int(params["pz"]) >= total:
                break
            page += 1
    return dedupe_symbols(symbols)


async def confirm_symbols_with_tencent(
    candidates: Iterable[SymbolInfo],
    *,
    exchanges: set[str],
    timeout: float,
) -> SourceDiscovery:
    candidate_list = [item for item in dedupe_symbols(candidates) if item.exchange in exchanges]
    confirmed: list[SymbolInfo] = []
    errors: list[str] = []
    async with httpx.AsyncClient(
        timeout=max(timeout, 20),
        trust_env=False,
        headers=http_headers(),
        limits=httpx.Limits(max_connections=1),
    ) as client:
        for index in range(0, len(candidate_list), 60):
            batch = candidate_list[index : index + 60]
            try:
                confirmed.extend(await _confirm_tencent_batch(client, batch))
            except Exception as exc:
                errors.append(str(exc)[:160])
            await asyncio.sleep(0.05)
    return SourceDiscovery("tencent", dedupe_symbols(confirmed), "; ".join(errors[:5]) or None)


async def _confirm_tencent_batch(client: httpx.AsyncClient, batch: list[SymbolInfo]) -> list[SymbolInfo]:
    query = ",".join(tencent_code(item) for item in batch)
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = await client.get(TENCENT_QUOTE_URL + query)
            response.raise_for_status()
            text = response.text
            break
        except Exception as exc:
            last_error = exc
            if attempt == 2:
                raise
            await asyncio.sleep(0.2 * (attempt + 1))
    else:
        raise RuntimeError(f"tencent confirmation failed: {last_error}")

    confirmed: list[SymbolInfo] = []
    for item in batch:
        market_code = tencent_code(item)
        match = re.search(rf"v_{market_code}=\"([^\"]*)\"", text)
        if not match:
            continue
        parts = match.group(1).split("~")
        if len(parts) <= 3:
            continue
        if parts[2] != item.code or parts[0] not in {"1", "51", "62"}:
            continue
        name = parts[1].strip() or item.name
        status_flag = parts[40].strip().upper() if len(parts) > 40 else ""
        if status_flag in {"D", "U"} or name.startswith("\u65e0\u6548"):
            continue
        confirmed.append(make_symbol(item.code, item.exchange, name))
    return confirmed


def normalize_symbols(symbols: Iterable[SymbolInfo], *, exchanges: set[str]) -> list[SymbolInfo]:
    normalized: list[SymbolInfo] = []
    for item in symbols:
        code = item.code.strip()
        exchange = item.exchange.strip().upper()
        if exchange not in exchanges:
            continue
        if not is_a_share_code(code, exchange):
            continue
        normalized.append(make_symbol(code, exchange, item.name or item.symbol))
    return dedupe_symbols(normalized)


def prepare_exchanges(value: str | None) -> set[str]:
    exchanges = {item.strip().upper() for item in (value or "").split(",") if item.strip()}
    return exchanges or {"SH", "SZ"}


def normalize_symbol_source_name(value: str) -> str:
    return normalize_provider_name(value).replace("-", "")


def make_symbol(code: str, exchange: str, name: str) -> SymbolInfo:
    clean_code = code.strip()
    clean_exchange = exchange.strip().upper()
    return SymbolInfo(
        symbol=f"{clean_code}.{clean_exchange}",
        code=clean_code,
        exchange=clean_exchange,
        name=(name or clean_code).strip(),
        asset_type="stock",
        market="A_SHARE",
        is_active=True,
    )


def dedupe_symbols(symbols: Iterable[SymbolInfo]) -> list[SymbolInfo]:
    by_key: dict[str, SymbolInfo] = {}
    for item in symbols:
        key = item.symbol.upper()
        by_key[key] = prefer_named_symbol(by_key.get(key), item)
    return [by_key[key] for key in sorted(by_key)]


def prefer_named_symbol(current: SymbolInfo | None, candidate: SymbolInfo) -> SymbolInfo:
    if current is None:
        return candidate
    if current.name in {current.code, current.symbol} and candidate.name not in {candidate.code, candidate.symbol}:
        return candidate
    return current


def infer_exchange(code: str) -> str | None:
    if code.startswith(("600", "601", "603", "605", "688")):
        return "SH"
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return "SZ"
    if code.startswith(("4", "8", "9")):
        return "BJ"
    return None


def is_a_share_code(code: str, exchange: str) -> bool:
    if len(code) != 6 or not code.isdigit():
        return False
    return infer_exchange(code) == exchange


def tencent_code(symbol: SymbolInfo) -> str:
    return f"{symbol.exchange.lower()}{symbol.code}"


def http_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }
