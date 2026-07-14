import asyncio
from datetime import date
import httpx
from collector.market_data import ProviderError
from collector.market_data.iwencai import HttpxIwencaiTransport,IwencaiApiKey,IwencaiConfig,IwencaiSidebarProvider,_error
class Transport:
    def __init__(self):self.queries=[]
    async def query(self,capability,query,*,limit=50):
        self.queries.append((capability,query));return ({"股票代码":"000001.SZ","最新价":"10.5","涨跌幅":"1.2%"},)
def test_adapter_uses_official_market_capability_and_normalizes_quote():
    async def run():
        transport=Transport();provider=IwencaiSidebarProvider(IwencaiConfig("secret"),transport,today=lambda:date(2026,7,10));result=(await provider.get_quotes(("000001.SZ",)))["000001.SZ"]
        assert result.value.price==10.5 and result.metadata.source=="iwencai"
        assert transport.queries[0][0]=="hithink-market-query" and "000001.SZ" in transport.queries[0][1]
    asyncio.run(run())

def test_quota_exhaustion_is_reported_as_rate_limited_not_authentication():
    request=httpx.Request("POST","https://openapi.iwencai.com/v1/comprehensive/search")
    response=httpx.Response(403,request=request,text="您今天的次数已用完，建议您升级权益")
    assert _error(httpx.HTTPStatusError("quota",request=request,response=response)) is ProviderError.RATE_LIMITED


def test_transport_rotates_to_next_priority_key_after_quota_exhaustion():
    authorizations = []

    def handler(request: httpx.Request) -> httpx.Response:
        authorizations.append(request.headers["Authorization"])
        if request.headers["Authorization"] == "Bearer first-secret":
            return httpx.Response(429, request=request, text="quota exhausted")
        return httpx.Response(200, request=request, json={"datas": []})

    transport = HttpxIwencaiTransport(
        query_endpoint="https://openapi.iwencai.com/v1/query2data",
        news_endpoint="https://openapi.iwencai.com/v1/comprehensive/search",
        api_keys=(
            IwencaiApiKey(label="primary", key="first-secret", priority=1),
            IwencaiApiKey(label="backup", key="second-secret", priority=2),
        ),
        timeout_seconds=1,
        allowed_hosts=("openapi.iwencai.com",),
        transport=httpx.MockTransport(handler),
    )

    rows = asyncio.run(transport.query("hithink-market-query", "000001.SZ latest price"))

    assert rows == ()
    assert authorizations == ["Bearer first-secret", "Bearer second-secret"]


def test_empty_structured_iwencai_results_are_unavailable_not_fresh():
    class EmptyTransport:
        async def query(self, capability, query, *, limit=50):
            return ({"股票代码": "600176.SH", "股票简称": "中国巨石"},)

    provider = IwencaiSidebarProvider(IwencaiConfig("secret"), EmptyTransport(), today=lambda: date(2026, 7, 13))

    async def run():
        themes = await provider.get_themes("600176.SH")
        strength = await provider.get_market_strength()
        assert themes.value is None
        assert themes.metadata.freshness.value == "unavailable"
        assert strength.value is None
        assert strength.metadata.freshness.value == "unavailable"

    asyncio.run(run())
