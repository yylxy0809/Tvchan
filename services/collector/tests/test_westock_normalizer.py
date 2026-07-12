from __future__ import annotations

from pathlib import Path

import pytest

from collector.market_data.contracts import CapitalFlow, MarketStrength, Profile, StrengthLeader, StrengthTheme
from collector.market_data.westock_normalizer import create_westock_normalizer


_FIXTURES = Path(__file__).with_name("fixtures") / "westock"


def _raw(operation: str) -> list[object]:
    return [{"operation": operation, "markdown": (_FIXTURES / f"{operation}.md").read_text(encoding="utf-8")}]


def test_normalizer_parses_fixed_utf8_markdown_fixtures() -> None:
    normalizer = create_westock_normalizer()

    profile = normalizer.normalize_profile("000001.SZ", _raw("profile"))
    capital_flow = normalizer.normalize_capital_flow("000001.SZ", _raw("asfund"))
    strength = normalizer.normalize_market_strength(_raw("board"))

    assert profile == Profile(symbol="000001.SZ", name="平安银行", industry="银行", description="人民币和外币存贷款业务")
    assert capital_flow == CapitalFlow(symbol="000001.SZ", main_net_inflow=-3.0, large_net_inflow=-2.0, medium_net_inflow=5.0, small_net_inflow=8.0)
    assert strength == MarketStrength(
        leaders=("航天环宇(20.01)",),
        themes=("航天装备",),
        leader_details=(StrengthLeader("航天环宇", 20.01),),
        theme_details=(StrengthTheme("航天装备", 10.22, None),),
    )


@pytest.mark.parametrize(
    "replacement",
    [
        "| code | code |\n| --- | --- |\n| sz000001 | 平安银行 |",
        "| code | unexpected |\n| --- | --- |\n| sz000001 | value |",
    ],
)
def test_normalizer_rejects_duplicate_or_unknown_headers(replacement: str) -> None:
    normalizer = create_westock_normalizer()
    with pytest.raises(ValueError, match="header"):
        normalizer.normalize_profile("000001.SZ", [{"operation": "profile", "markdown": replacement}])


def test_normalizer_rejects_non_finite_numeric_fields() -> None:
    normalizer = create_westock_normalizer()
    raw = _raw("asfund")
    raw[0]["markdown"] = raw[0]["markdown"].replace("| -3.00 |", "| NaN |", 1)
    with pytest.raises(ValueError, match="non-finite"):
        normalizer.normalize_capital_flow("000001.SZ", raw)
