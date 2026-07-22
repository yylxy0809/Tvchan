from datetime import datetime, timezone

import pytest

from collector.module_c_supplemental_eligibility import (
    build_dispositions,
    validate_catalog_scope_rows,
    parse_symbols,
)
from collector.module_c_eligibility import _write_outputs


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


def test_supplemental_output_contract_requires_explicit_empty_exclusion_summary(tmp_path) -> None:
    metadata = {"excluded_summary": {"excluded_scopes": 0, "reasons": {}}}
    _write_outputs(tmp_path, [], metadata)
    assert (tmp_path / "excluded_summary.json").is_file()


def test_supplemental_catalog_scope_must_match_audit_checkpoint_bounds() -> None:
    catalog = [{
        "symbol_id": 2047,
        "timeframe": timeframe,
        "state": "present",
        "bounds_complete": True,
        "min_ts": datetime(2020, 9, 8, 1, 30, tzinfo=timezone.utc),
        "max_ts": datetime(2026, 7, 21, 7, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 7, 21, 14, tzinfo=timezone.utc),
    } for timeframe in (5, 30, 1440, 10080, 43200)]
    checkpoints = [{
        "symbol_id": 2047,
        "timeframe": row["timeframe"],
        "status": "completed",
        "shard_start": row["min_ts"],
        "shard_end": row["max_ts"],
        "metadata": {"disposition": "eligible"},
    } for row in catalog]

    digest = validate_catalog_scope_rows(catalog, checkpoints, {2047})
    assert len(digest) == 64

    catalog[0]["max_ts"] = datetime(2026, 7, 22, 7, tzinfo=timezone.utc)
    with pytest.raises(RuntimeError, match="audit checkpoint bounds"):
        validate_catalog_scope_rows(catalog, checkpoints, {2047})
