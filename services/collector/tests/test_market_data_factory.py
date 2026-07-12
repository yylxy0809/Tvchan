from __future__ import annotations

import sys
import types

import pytest

from collector.market_data.factory import create_market_data_provider


class FakeNodeTransport:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class FakeHttpTransport:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def _factories(monkeypatch):
    module = types.ModuleType("test_market_data_components")
    module.normalizer = lambda: object()
    module.resolver = lambda: (lambda symbol: ("name", "industry"))
    module.failing_factory = lambda: (_ for _ in ()).throw(ValueError("top-secret"))
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return module.__name__


def _env(module_name):
    return {
        "DATABASE_URL": "postgresql://quote-reader:secret@localhost/market",
        "WESTOCK_NORMALIZER_FACTORY": f"{module_name}:normalizer",
        "WESTOCK_BRIDGE_MODULE": "/verified/provider.mjs",
        "WESTOCK_BRIDGE_MODULE_VERSION": "westock-data@1.0.4",
        "WESTOCK_BRIDGE_SHA256": "a" * 64,
        "WESTOCK_TIMEOUT_SECONDS": "3.5",
        "WESTOCK_MAX_OUTPUT_BYTES": "2048",
        "WESTOCK_BATCH_SIZE": "25",
        "WESTOCK_PROCESS_POOL_SIZE": "1",
        "IWENCAI_BASE_URL": "https://iwencai.example",
        "IWENCAI_ALLOWED_HOSTS": "iwencai.example",
        "IWENCAI_API_KEY": "top-secret",
        "IWENCAI_TIMEOUT_SECONDS": "4",
    }


def test_builds_composite_provider_without_starting_real_transports(monkeypatch):
    module_name = _factories(monkeypatch)

    provider = create_market_data_provider(
        env=_env(module_name),
        node_transport_cls=FakeNodeTransport,
        http_transport_cls=FakeHttpTransport,
    )

    quotes = provider._quotes
    market = provider._market
    news = provider._news
    assert quotes._database_url == "postgresql://quote-reader:secret@localhost/market"
    assert market._transport.kwargs == {
        "timeout_seconds": 3.5,
        "max_output_bytes": 2048,
        "env": {
            "WESTOCK_BRIDGE_MODULE": "/verified/provider.mjs",
            "WESTOCK_BRIDGE_MODULE_VERSION": "westock-data@1.0.4",
            "WESTOCK_BRIDGE_SHA256": "a" * 64,
        },
    }
    assert market._batch_size == 25
    assert market._normalizer is not None
    assert news._config.api_key == "top-secret"
    assert news._config.timeout_seconds == 4
    assert news._transport.kwargs["endpoint"] == "https://iwencai.example/v1/comprehensive/search"
    assert news._transport.kwargs["api_key"] == "top-secret"
    assert news._transport.kwargs["allowed_hosts"] == ("iwencai.example",)
    assert news._resolver is not None


def test_uses_built_in_iwencai_contract_without_external_factories(monkeypatch):
    module_name = _factories(monkeypatch)
    env = _env(module_name)

    provider = create_market_data_provider(
        env=env,
        node_transport_cls=FakeNodeTransport,
        http_transport_cls=FakeHttpTransport,
    )

    transport = provider._news._transport
    assert isinstance(transport, FakeHttpTransport)
    assert transport.kwargs["request_builder"].__module__ == "collector.market_data.iwencai_contract"
    assert transport.kwargs["response_parser"].__module__ == "collector.market_data.iwencai_contract"


def test_uses_the_bundled_westock_normalizer_factory_by_default(monkeypatch):
    module_name = _factories(monkeypatch)
    default_module = types.ModuleType("collector.market_data.westock_normalizer")
    normalizer = object()
    default_module.create_westock_normalizer = lambda: normalizer
    monkeypatch.setitem(sys.modules, default_module.__name__, default_module)
    env = _env(module_name)
    env.pop("WESTOCK_NORMALIZER_FACTORY")

    provider = create_market_data_provider(
        env=env,
        node_transport_cls=FakeNodeTransport,
        http_transport_cls=FakeHttpTransport,
    )

    assert provider._market._normalizer is normalizer


@pytest.mark.parametrize(
    "missing",
    [
        "DATABASE_URL",
        "WESTOCK_BRIDGE_MODULE",
        "WESTOCK_BRIDGE_MODULE_VERSION",
        "WESTOCK_BRIDGE_SHA256",
        "IWENCAI_BASE_URL",
        "IWENCAI_API_KEY",
    ],
)
def test_required_configuration_fails_fast_without_leaking_secret(monkeypatch, missing):
    module_name = _factories(monkeypatch)
    env = _env(module_name)
    secret = env["IWENCAI_API_KEY"]
    env.pop(missing)

    with pytest.raises(ValueError) as exc:
        create_market_data_provider(
            env=env,
            node_transport_cls=FakeNodeTransport,
            http_transport_cls=FakeHttpTransport,
        )

    assert missing in str(exc.value)
    assert secret not in str(exc.value)


def test_rejects_invalid_factory_path_without_leaking_secret(monkeypatch):
    module_name = _factories(monkeypatch)
    env = _env(module_name)
    env["WESTOCK_NORMALIZER_FACTORY"] = "not-a-factory-path"

    with pytest.raises(ValueError, match="WESTOCK_NORMALIZER_FACTORY") as exc:
        create_market_data_provider(
            env=env,
            node_transport_cls=FakeNodeTransport,
            http_transport_cls=FakeHttpTransport,
        )

    assert env["IWENCAI_API_KEY"] not in str(exc.value)


def test_factory_failure_does_not_leak_secret(monkeypatch):
    module_name = _factories(monkeypatch)
    env = _env(module_name)
    env["WESTOCK_NORMALIZER_FACTORY"] = f"{module_name}:failing_factory"

    with pytest.raises(ValueError, match="WESTOCK_NORMALIZER_FACTORY factory failed") as exc:
        create_market_data_provider(
            env=env,
            node_transport_cls=FakeNodeTransport,
            http_transport_cls=FakeHttpTransport,
        )

    assert env["IWENCAI_API_KEY"] not in str(exc.value)
