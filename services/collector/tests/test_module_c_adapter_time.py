from datetime import UTC, datetime
from types import SimpleNamespace

from collector.module_c_adapter import _time


def test_time_prefers_original_native_epoch_over_vendor_timezone_value() -> None:
    native_epoch = int(datetime(2026, 7, 3, 7, 0, tzinfo=UTC).timestamp())
    vendor_time = SimpleNamespace(ts=native_epoch + 8 * 60 * 60, _module_c_base_ts=native_epoch)
    item = SimpleNamespace(time=vendor_time)

    assert _time(item) == native_epoch


def test_time_rebuilds_shanghai_wall_clock_without_process_timezone() -> None:
    vendor_time = SimpleNamespace(
        year=2026, month=7, day=3, hour=15, minute=0, second=0,
        ts=int(datetime(2026, 7, 3, 15, 0, tzinfo=UTC).timestamp()),
    )
    item = SimpleNamespace(time=vendor_time)

    assert _time(item) == int(datetime(2026, 7, 3, 7, 0, tzinfo=UTC).timestamp())
