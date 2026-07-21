from datetime import datetime, timezone

import pytest

from collector.module_c_supplemental_eligibility import build_dispositions, parse_symbols


def test_parse_symbols_requires_small_explicit_deduplicated_scope() -> None:
    assert parse_symbols(" 605003.sh,605003.SH ") == ("605003.SH",)
    with pytest.raises(ValueError, match="1..20"):
        parse_symbols("")
    with pytest.raises(ValueError, match="1..20"):
        parse_symbols(",".join(f"{index:06d}.SZ" for index in range(21)))


def test_supplemental_scope_requires_five_eligible_resolved_dispositions() -> None:
    rows = [
        {
            "symbol_id": 2047,
            "symbol": "605003.SH",
            "timeframe": timeframe,
            "eligible": True,
            "reasons": [],
            "covered_until": datetime(2026, 7, 21, 7, tzinfo=timezone.utc),
            "unresolved_rows": 0,
        }
        for timeframe in (5, 30, 1440, 10080, 43200)
    ]
    dispositions = build_dispositions(rows, ("605003.SH",))
    assert len(dispositions) == 5
    rows[0]["eligible"] = False
    with pytest.raises(RuntimeError, match="five eligible resolved"):
        build_dispositions(rows, ("605003.SH",))
