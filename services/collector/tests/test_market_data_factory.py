import pytest
from collector.market_data.factory import create_market_data_provider
from collector.market_data.iwencai import HttpxIwencaiTransport, IwencaiSidebarProvider
from collector.market_data.provider import FallbackMarketDataProvider

class Transport(HttpxIwencaiTransport):
    def __init__(self, **kwargs): self.kwargs = kwargs

def env(): return {"IWENCAI_BASE_URL": "https://iwencai.example", "IWENCAI_ALLOWED_HOSTS": "iwencai.example", "IWENCAI_API_KEY": "secret", "IWENCAI_TIMEOUT_SECONDS": "5"}
def test_factory_builds_only_iwencai_sidebar_provider():
    provider = create_market_data_provider(env=env(), http_transport_cls=Transport)
    assert isinstance(provider, IwencaiSidebarProvider)
    assert provider._transport.kwargs["query_endpoint"] == "https://iwencai.example/v1/query2data"
    assert provider._transport.kwargs["news_endpoint"] == "https://iwencai.example/v1/comprehensive/search"
def test_factory_fails_closed_without_iwencai_key():
    configured = env(); configured.pop("IWENCAI_API_KEY")
    with pytest.raises(ValueError, match="IWENCAI_API_KEY"): create_market_data_provider(env=configured, http_transport_cls=Transport)


def test_factory_accepts_priority_key_pool_and_omits_secrets_from_provider_repr():
    configured = env() | {"IWENCAI_API_KEYS": '[{"label":"first","key":"first-secret","priority":1},{"label":"second","key":"second-secret","priority":2}]'}
    configured.pop("IWENCAI_API_KEY")
    provider = create_market_data_provider(env=configured, http_transport_cls=Transport)
    assert provider._config.api_keys[0].label == "first"
    assert "first-secret" not in repr(provider._config)


def test_factory_uses_notte_primary_with_iwencai_fallback_when_configured():
    configured = env() | {
        "NOTTE_API_KEY": "notte-secret",
        "NOTTE_FUNCTION_ID": "function-id",
        "MARKET_DATA_PROVIDER_ORDER": "notte,iwencai",
    }
    provider = create_market_data_provider(
        env=configured,
        http_transport_cls=Transport,
        notte_function=object(),
    )
    assert isinstance(provider, FallbackMarketDataProvider)
    assert provider.primary.__class__.__name__ == "NotteSidebarProvider"
    assert isinstance(provider.fallback, IwencaiSidebarProvider)
