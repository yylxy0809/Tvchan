import json,re
from pathlib import Path
import pytest
from collector.market_data.iwencai_contract import CAPABILITIES,SchemaError,build_endpoint,build_request,parse_rows
FIXTURES=Path(__file__).parent/"fixtures"/"iwencai"
CASES={"hithink-market-query":"market_query.json","hithink-business-query":"business_query.json","hithink-finance-query":"finance_query.json","hithink-industry-query":"industry_query.json","hithink-sector-selector":"sector_selector.json","hithink-astock-selector":"astock_selector.json","hithink-zhishu-query":"zhishu_query.json","news-search":"news_search.json"}
def test_every_official_capability_has_real_response_shape_and_exact_header():
    assert set(CASES)==CAPABILITIES
    for capability,filename in CASES.items():
        request=build_request("测试查询","secret",capability)
        assert request["headers"]["X-Claw-Skill-Id"]==capability and re.fullmatch(r"[0-9a-f]{64}",request["headers"]["X-Claw-Trace-Id"])
        payload=json.loads((FIXTURES/filename).read_text(encoding="utf-8"));assert parse_rows(payload,news=capability=="news-search")
def test_endpoints_are_official_and_https_only():
    hosts=("openapi.iwencai.com",);assert build_endpoint("https://openapi.iwencai.com",hosts).endswith("/v1/query2data");assert build_endpoint("https://openapi.iwencai.com",hosts,news=True).endswith("/v1/comprehensive/search")
    with pytest.raises(ValueError):build_endpoint("http://openapi.iwencai.com",hosts)
def test_malformed_rows_rejected():
    with pytest.raises(SchemaError):parse_rows({"datas":[float("nan")]})
