from __future__ import annotations

import math
import re

from .contracts import CapitalFlow, MarketStrength, Profile, Quote, StrengthLeader, StrengthTheme


_PROFILE_HEADERS = frozenset({"code", "name", "listedDate", "business", "website", "industry", "sector", "issuePrice", "regCapital", "establishDate", "chairman", "regAddress", "officeAddress", "tel", "email"})
_ASFUND_HEADERS = frozenset({"code", "BlockNetFlow", "BlockTradingInfos", "ClosePrice", "EndDate", "FwdClosePrice", "JumboNetFlow", "LastestTradedPrice", "LhbTradingDetails", "MainInFlow", "MainInflowCircRate", "MainInflowIndustryRank", "MainInflowRank", "MainNetFlow", "MainNetFlow10D", "MainNetFlow20D", "MainNetFlow5D", "MainOutFlow", "MarginTradeInfos", "MidNetFlow", "RetailInFlow", "RetailOutFlow", "SecuCode", "SmallNetFlow"})
_BOARD_HEADERS = frozenset({"name", "changePct", "turnoverRate", "changePct5d", "changePct20d", "leadStock", "mainNetInflow", "mainNetInflow5d", "upDownRatio"})
_LEADER = re.compile(r"^(?P<name>.+)\((?P<change>[+-]?\d+(?:\.\d+)?)\)$")
def create_westock_normalizer() -> "WeStockMarkdownNormalizer":
    return WeStockMarkdownNormalizer()


class WeStockMarkdownNormalizer:
    def normalize_quotes(self, symbols: tuple[str, ...], raw: list[object]) -> dict[str, Quote]:
        raise ValueError("quote is unsupported by westock-data-clawhub@1.0.4")

    def normalize_profile(self, symbol: str, profile_raw: list[object]) -> Profile:
        profile = _single_row(profile_raw, _PROFILE_HEADERS)
        if profile["code"] != _westock_symbol(symbol):
            raise ValueError("profile symbol mismatch")
        return Profile(
            symbol=symbol,
            name=_text(profile.get("name")),
            industry=_text(profile.get("industry")),
            description=_text(profile.get("business")),
        )

    def normalize_capital_flow(self, symbol: str, raw: list[object]) -> CapitalFlow:
        row = _single_row(raw, _ASFUND_HEADERS)
        if row["code"] != _westock_symbol(symbol) or row.get("SecuCode") != _westock_symbol(symbol):
            raise ValueError("capital-flow symbol mismatch")
        return CapitalFlow(symbol=symbol, main_net_inflow=_number(row.get("MainNetFlow")), large_net_inflow=_number(row.get("JumboNetFlow")), medium_net_inflow=_number(row.get("MidNetFlow")), small_net_inflow=_number(row.get("SmallNetFlow")))

    def normalize_market_strength(self, raw: list[object]) -> MarketStrength:
        tables = _parse_tables(_markdown(raw), allowed_headers=_BOARD_HEADERS)
        rows = [row for table in tables for row in table]
        themes = tuple(dict.fromkeys(row["name"] for row in rows if _text(row.get("name"))))
        leaders = tuple(dict.fromkeys(row["leadStock"] for row in rows if _text(row.get("leadStock"))))
        if not themes:
            raise ValueError("board response contained no named rows")
        theme_by_name: dict[str, StrengthTheme] = {}
        for row in rows:
            name = _text(row.get("name"))
            if not name:
                continue
            previous = theme_by_name.get(name)
            theme_by_name[name] = StrengthTheme(
                name=name,
                change_percent=_number(row.get("changePct")) if row.get("changePct") not in (None, "") else previous.change_percent if previous else None,
                main_net_inflow_wan=_number(row.get("mainNetInflow")) if row.get("mainNetInflow") not in (None, "") else previous.main_net_inflow_wan if previous else None,
            )
        return MarketStrength(
            leaders=leaders,
            themes=themes,
            leader_details=tuple(_parse_leader(value) for value in leaders),
            theme_details=tuple(theme_by_name.values()),
        )


def _parse_leader(value: str) -> StrengthLeader:
    match = _LEADER.fullmatch(value)
    if match is None:
        return StrengthLeader(name=value)
    return StrengthLeader(name=match.group("name"), change_percent=_number(match.group("change")))


def _markdown(raw: list[object]) -> str:
    if len(raw) != 1 or not isinstance(raw[0], dict) or set(raw[0]) != {"operation", "markdown"}:
        raise ValueError("invalid westock bridge payload")
    markdown = raw[0]["markdown"]
    if not isinstance(markdown, str) or not markdown:
        raise ValueError("westock CLI returned no UTF-8 markdown")
    return markdown


def _single_row(raw: list[object], headers: frozenset[str]) -> dict[str, str]:
    tables = _parse_tables(_markdown(raw), allowed_headers=headers)
    rows = [row for table in tables for row in table]
    if len(rows) != 1:
        raise ValueError("expected exactly one westock row")
    return rows[0]


def _parse_tables(markdown: str, *, allowed_headers: frozenset[str] | None) -> list[list[dict[str, str]]]:
    lines = [line.strip() for line in markdown.splitlines()]
    tables: list[list[dict[str, str]]] = []
    index = 0
    while index + 1 < len(lines):
        if not lines[index].startswith("|") or not lines[index + 1].startswith("|"):
            index += 1
            continue
        headers = _cells(lines[index])
        if len(headers) != len(set(headers)) or not headers or (allowed_headers is not None and not set(headers) <= allowed_headers):
            raise ValueError("unknown or duplicate westock markdown header")
        if any(not cell or set(cell) - {"-", ":"} for cell in _cells(lines[index + 1])):
            raise ValueError("invalid westock markdown separator")
        index += 2
        rows: list[dict[str, str]] = []
        while index < len(lines) and lines[index].startswith("|"):
            values = _cells(lines[index])
            if len(values) != len(headers):
                raise ValueError("westock markdown row width mismatch")
            rows.append(dict(zip(headers, values, strict=True)))
            index += 1
        tables.append(rows)
    if not tables:
        raise ValueError("westock CLI returned no markdown table")
    return tables


def _cells(line: str) -> list[str]:
    if not line.endswith("|"):
        raise ValueError("invalid westock markdown table")
    return [cell.strip() for cell in line[1:-1].split("|")]


def _number(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except ValueError as exc:
        raise ValueError("westock numeric field is invalid") from exc
    if not math.isfinite(number):
        raise ValueError("westock numeric field is non-finite")
    return number


def _text(value: str | None) -> str | None:
    return value or None


def _westock_symbol(symbol: str) -> str:
    code, exchange = symbol.upper().split(".", 1)
    return f"{exchange.lower()}{code}"
