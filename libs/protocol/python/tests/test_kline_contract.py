from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from trading_protocol import (
    canonical_kline_timestamp,
    kline_logical_key,
    source_priority,
    source_priority_with_coverage,
    should_replace_kline,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.mark.parametrize(
    ("timeframe", "value"),
    [
        ("5f", "2026-07-10 09:35"),
        ("15f", "2026-07-10 09:45"),
        ("30f", "2026-07-10 10:00"),
        ("1h", "2026-07-10 10:30"),
        ("1d", "2026-07-10 15:00"),
        ("1w", "2026-07-10 15:00"),
        ("1m", "2026-07-10 15:00"),
    ],
)
def test_canonical_kline_timestamp_preserves_valid_bar_end_labels(timeframe: str, value: str) -> None:
    timestamp = datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=SHANGHAI)

    assert canonical_kline_timestamp(timeframe, timestamp) == timestamp
    assert kline_logical_key(timeframe, timestamp)[0] == timeframe


@pytest.mark.parametrize("timeframe", ["1d", "1w", "1m"])
def test_date_only_higher_timeframe_normalizes_to_close(timeframe: str) -> None:
    supplied_date = datetime(2026, 7, 10, tzinfo=SHANGHAI)

    assert canonical_kline_timestamp(timeframe, supplied_date, date_only=True) == datetime(
        2026, 7, 10, 15, 0, tzinfo=SHANGHAI
    )


def test_intraday_off_session_label_is_rejected_instead_of_rounded() -> None:
    with pytest.raises(ValueError, match="session bar-end"):
        canonical_kline_timestamp("30f", datetime(2026, 7, 10, 12, 0, tzinfo=SHANGHAI))


@pytest.mark.parametrize("timeframe", ["15f", "1h"])
def test_supported_intraday_timeframes_reject_off_grid_labels(timeframe: str) -> None:
    with pytest.raises(ValueError, match="session bar-end"):
        canonical_kline_timestamp(timeframe, datetime(2026, 7, 10, 10, 5, tzinfo=SHANGHAI))


@pytest.mark.parametrize("timeframe", ["5f", "15f", "30f", "1h"])
def test_intraday_opening_snapshot_0930_is_canonical_and_unshifted(timeframe: str) -> None:
    opening_snapshot = datetime(2026, 7, 10, 9, 30, tzinfo=SHANGHAI)

    assert canonical_kline_timestamp(timeframe, opening_snapshot) == opening_snapshot
    assert kline_logical_key(timeframe, opening_snapshot) == (timeframe, opening_snapshot)


@pytest.mark.parametrize("timeframe", ["5f", "15f", "30f", "1h"])
def test_intraday_0931_remains_invalid(timeframe: str) -> None:
    with pytest.raises(ValueError, match="session bar-end"):
        canonical_kline_timestamp(timeframe, datetime(2026, 7, 10, 9, 31, tzinfo=SHANGHAI))


def test_source_priority_is_explicit_not_numeric_source_order() -> None:
    assert source_priority("parquet_native") > source_priority("parquet_5f")
    assert source_priority("pytdx") > source_priority("mootdx")
    assert source_priority("mootdx") > source_priority("tencent") > source_priority("baidu")
    assert source_priority("baidu") > source_priority("derived_5f") > source_priority("seed")


def test_source_priority_uses_parquet_coverage_boundary_for_pytdx() -> None:
    coverage = datetime(2026, 6, 30, 15, tzinfo=SHANGHAI)

    assert source_priority_with_coverage("parquet_native", coverage, coverage) > source_priority_with_coverage("pytdx", coverage, coverage)
    assert source_priority_with_coverage("pytdx", coverage.replace(day=1), coverage) > source_priority_with_coverage("tencent", coverage.replace(day=1), coverage)
    assert source_priority_with_coverage("pytdx", datetime(2026, 7, 1, 15, tzinfo=SHANGHAI), coverage) > source_priority_with_coverage("parquet_native", datetime(2026, 7, 1, 15, tzinfo=SHANGHAI), coverage)


@pytest.mark.parametrize(
    ("timeframe", "first", "second", "different_period"),
    [
        ("1w", "2026-07-06 15:00", "2026-07-10 15:00", "2026-07-13 15:00"),
        ("1m", "2026-07-01 15:00", "2026-07-31 15:00", "2026-08-03 15:00"),
    ],
)
def test_higher_timeframe_logical_key_groups_one_calendar_period(
    timeframe: str, first: str, second: str, different_period: str
) -> None:
    first_ts = datetime.strptime(first, "%Y-%m-%d %H:%M").replace(tzinfo=SHANGHAI)
    second_ts = datetime.strptime(second, "%Y-%m-%d %H:%M").replace(tzinfo=SHANGHAI)
    different_ts = datetime.strptime(different_period, "%Y-%m-%d %H:%M").replace(tzinfo=SHANGHAI)

    assert kline_logical_key(timeframe, first_ts) == kline_logical_key(timeframe, second_ts)
    assert kline_logical_key(timeframe, first_ts) != kline_logical_key(timeframe, different_ts)


def test_replacement_requires_better_source_or_equal_source_improvement() -> None:
    assert should_replace_kline(
        existing_source="tencent",
        existing_revision=0,
        existing_complete=True,
        incoming_source="pytdx",
        incoming_revision=0,
        incoming_complete=True,
    )
    assert not should_replace_kline(
        existing_source="pytdx",
        existing_revision=0,
        existing_complete=True,
        incoming_source="tencent",
        incoming_revision=99,
        incoming_complete=True,
    )
    assert should_replace_kline(
        existing_source="pytdx",
        existing_revision=0,
        existing_complete=False,
        incoming_source="pytdx",
        incoming_revision=0,
        incoming_complete=True,
    )
    assert not should_replace_kline(
        existing_source="pytdx",
        existing_revision=1,
        existing_complete=True,
        incoming_source="pytdx",
        incoming_revision=1,
        incoming_complete=True,
    )
