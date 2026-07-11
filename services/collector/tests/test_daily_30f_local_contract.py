from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from collector.local_parquet_profiler import expected_session_times
from trading_protocol import canonical_kline_timestamp


SHANGHAI = ZoneInfo("Asia/Shanghai")


def test_local_30f_complete_session_is_nine_native_bar_end_labels() -> None:
    assert expected_session_times("30min") == {
        "09:30",
        "10:00",
        "10:30",
        "11:00",
        "11:30",
        "13:30",
        "14:00",
        "14:30",
        "15:00",
    }


def test_daily_source_midnight_is_normalized_to_shanghai_close() -> None:
    source_trade_date = datetime(2024, 4, 3, tzinfo=SHANGHAI)

    assert canonical_kline_timestamp("1d", source_trade_date, date_only=True) == datetime(
        2024, 4, 3, 15, 0, tzinfo=SHANGHAI
    )


@pytest.mark.parametrize("hour, minute", [(9, 31), (12, 0), (13, 0)])
def test_30f_rejects_non_native_session_labels(hour: int, minute: int) -> None:
    with pytest.raises(ValueError, match="session bar-end"):
        canonical_kline_timestamp("30f", datetime(2024, 4, 3, hour, minute, tzinfo=SHANGHAI))
