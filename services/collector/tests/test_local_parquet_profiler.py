from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.local_parquet_profiler import profile_daily_file, profile_file, profile_root


def _write(path, *, bad=False):
    times = [datetime(2026, 1, 5, 9, 30), datetime(2026, 1, 5, 9, 35)]
    pq.write_table(pa.table({
        "ts_code": [path.stem, path.stem], "trade_date": [datetime(2026, 1, 5)] * 2,
        "trade_time": times, "open": [10.0, 10.0], "high": [11.0, 9.0 if bad else 11.0],
        "low": [9.0, 9.0], "close": [10.5, 10.5], "vol": [100.0, -1.0 if bad else 100.0],
        "amount": [1000.0, 1000.0],
    }), path, row_group_size=1)


def _write_root_metadata(root):
    pq.write_table(pa.table({"ts_code": ["000001.SZ", "000002.SZ", "000003.SZ"],
                             "list_status": ["L", "L", "D"]}), root / "stock_basic_data.parquet")
    pq.write_table(pa.table({
        "ts_code": ["000001.SZ"], "trade_date": [datetime(2026, 1, 5)],
        "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
        "vol": [2.0], "amount": [1000.0],
    }), root / "stock_daily.parquet")


def test_profiles_batches_without_loading_whole_file(tmp_path):
    directory = tmp_path / "stock_5min"
    directory.mkdir()
    path = directory / "000001.SZ.parquet"
    _write(path)
    result = profile_file(path, batch_size=1)
    assert result.rows == 2
    assert result.row_groups == 2
    assert result.day_count_histogram == {"2": 1}
    assert result.min_trade_time.endswith("09:30:00")
    assert result.invalid_ohlc_rows == 0


def test_reports_ohlc_and_volume_anomalies(tmp_path):
    path = tmp_path / "000001.SZ.parquet"
    _write(path, bad=True)
    result = profile_file(path, batch_size=1)
    assert result.invalid_ohlc_rows == 1
    assert result.negative_volume_rows == 1
    assert {item["kind"] for item in result.anomaly_examples} == {"invalid_ohlc", "negative_volume"}


def test_root_inventory_and_dry_run_contract(tmp_path):
    directory = tmp_path / "stock_5min"
    directory.mkdir()
    _write(directory / "000001.SZ.parquet")
    _write(directory / "000002.SZ.parquet")
    _write_root_metadata(tmp_path)
    result = profile_root(tmp_path, max_files=1, batch_size=1)
    assert result.discovered_files == 2
    assert result.profiled_files == 1
    assert result.database_writes == 0
    assert result.dry_run is True
    assert result.required_columns_ok is True
    assert result.active_symbols == 2
    assert result.active_symbols_missing_file == 0
    assert result.inactive_symbols_with_file == 0
    assert result.daily_profile["metadata_rows"] == 1
    assert result.volume_unit_evidence["daily_unit_decision"] == "hundred_shares"


def test_session_contract_and_projected_disposition(tmp_path):
    path = tmp_path / "000001.SZ.parquet"
    _write(path, bad=True)
    result = profile_file(path, batch_size=1)
    assert result.opening_snapshot_rows == 1
    assert result.invalid_session_rows == 0
    assert result.incomplete_session_days == 1
    assert result.missing_expected_bars == 47
    assert result.projected_quarantined_rows == 1
    assert result.projected_accepted_rows == 1
    assert result.projected_rejected_rows == 0


def test_static_shards_are_stable_disjoint_and_exhaustive(tmp_path):
    directory = tmp_path / "stock_5min"
    directory.mkdir()
    for code in ("000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"):
        _write(directory / f"{code}.parquet")
    _write_root_metadata(tmp_path)
    reports = [profile_root(tmp_path, shard_index=index, shard_count=3,
                            profile_daily=False) for index in range(3)]
    shards = [{item.expected_symbol for item in report.files} for report in reports]
    assert shards == [{"000001.SZ", "000004.SZ"}, {"000002.SZ", "000005.SZ"}, {"000003.SZ"}]
    assert set().union(*shards) == {f"00000{index}.SZ" for index in range(1, 6)}
    assert not (shards[0] & shards[1] or shards[1] & shards[2] or shards[0] & shards[2])
    assert [(report.shard_index, report.shard_count) for report in reports] == [(0, 3), (1, 3), (2, 3)]


def test_invalid_shard_arguments_are_rejected(tmp_path):
    (tmp_path / "stock_5min").mkdir()
    with pytest.raises(ValueError, match="shard_count"):
        profile_root(tmp_path, shard_count=0)
    with pytest.raises(ValueError, match="shard_index"):
        profile_root(tmp_path, shard_index=1, shard_count=1)


def test_full_daily_scan_reports_amount_volume_and_ohlc_anomalies(tmp_path):
    path = tmp_path / "stock_daily.parquet"
    pq.write_table(pa.table({
        "ts_code": ["000001.SZ", "000001.SZ"],
        "trade_date": [datetime(2026, 1, 5), datetime(2026, 1, 5)],
        "open": [10.0, 10.0], "high": [11.0, 9.0], "low": [9.0, 9.0], "close": [10.5, 10.5],
        "vol": [1.0, -1.0], "amount": [1.0, -2.0],
    }), path, row_group_size=1)
    result = profile_daily_file(path, batch_size=1, max_batches=None)
    assert result["full_scan"] is True
    assert result["anomaly_totals"] == {
        "invalid_ohlc_rows": 1, "negative_volume_rows": 1,
        "negative_amount_rows": 1, "duplicate_key_rows": 1,
    }
