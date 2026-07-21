from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
import re
from types import SimpleNamespace

from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.main import app
from app.core.config import Settings
from app.repositories.chan_postgres import (
    OverlayTooLargeError,
    _OutputBudget,
    _fetch_windowed_stroke_like,
    _select_windowed_module_c_runs,
    _windowed_snapshot_version,
    get_windowed_module_c_overlay_db,
)
from app.routes import chan
from trading_protocol import MODULE_C_CONFIG_HASH


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *_args):
        return None


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


class _Conn:
    def __init__(self):
        self.queries: list[str] = []
        self.args: list[tuple] = []
        self.transactions: list[dict] = []

    def transaction(self, **kwargs):
        self.transactions.append(kwargs)
        return _Acquire(self)

    async def fetchval(self, *_args):
        return 1

    async def execute(self, query, *_args):
        self.queries.append(query)
        self.args.append(_args)
        return "SET"

    async def fetch(self, query, *_args):
        self.queries.append(query)
        self.args.append(_args)
        placeholders = [int(value) for value in re.findall(r"\$(\d+)", query)]
        assert max(placeholders, default=0) == len(_args)
        if "from scheme2_chan_c_published_heads" in query:
            return [{
                "chan_level": 5, "mode": "confirmed", "run_id": 7,
                "snapshot_version": "s-1", "base_from_bar_end": _dt(1),
                "base_to_bar_end": _dt(99), "bar_from": _dt(1),
                "bar_until": _dt(99), "computed_at": _dt(99),
            }]
        if "chan_c_strokes" in query and "with requested as" in query:
            return [
                {**_line(1, 2, "previous"), "requested_level": "5f"},
                {**_line(11, 12, "inside"), "requested_level": "5f"},
                {**_line(21, 22, "following"), "requested_level": "5f"},
            ]
        if "from chan_c_strokes" in query:
            if "limit 1" in query and " < $3" in query:
                return [_line(1, 2, "previous")]
            if "limit 1" in query and " > $3" in query:
                return [_line(21, 22, "following")]
            return [_line(11, 12, "inside")]
        if "chan_c_segments" in query:
            return []
        if "chan_c_centers" in query:
            return [{**_center(9, 14), "requested_level": "5f"}]
        if "chan_c_signals" in query:
            return [{**_signal(12), "requested_level": "5f"}]
        return []


def _dt(value: int) -> datetime:
    return datetime.fromtimestamp(value, UTC)


def _line(start: int, end: int, stable_id: str) -> dict:
    return {
        "id": start, "mode": 1, "seq": start, "start_ts": _dt(start), "end_ts": _dt(end),
        "begin_base_ts": _dt(start), "end_base_ts": _dt(end), "begin_base_seq": None,
        "end_base_seq": None, "start_price_x1000": 10000, "end_price_x1000": 11000,
        "direction": 1, "is_confirmed": True, "extra": {"id": stable_id},
    }


def _center(start: int, end: int) -> dict:
    return {
        "id": 1, "mode": 1, "seq": 1, "start_ts": _dt(start), "end_ts": _dt(end),
        "begin_base_ts": _dt(start), "end_base_ts": _dt(end), "begin_base_seq": None,
        "end_base_seq": None, "low_x1000": 10000, "high_x1000": 11000,
        "is_confirmed": True, "extra": {"id": "center"},
    }


def _signal(at: int) -> dict:
    return {
        "id": 2, "mode": 1, "ts": _dt(at), "base_ts": _dt(at), "base_seq": None,
        "price_x1000": 11000, "signal_type": "1buy", "is_confirmed": True,
        "extra": {"id": "signal"},
    }


def test_frozen_display_level_mapping() -> None:
    assert {key: chan._display_levels_for_chart(key) for key in chan.DISPLAY_LEVELS} == {
        "5f": ["5f", "30f", "1d"], "15f": ["5f", "30f", "1d"],
        "30f": ["30f", "1d"], "1h": ["30f", "1d"], "1d": ["1d", "1w"],
        "1w": ["1w", "1m"], "1m": ["1m"],
    }


def test_windowed_detail_keeps_one_line_on_each_boundary_and_raw_geometry() -> None:
    async def scenario():
        conn = _Conn()
        response = await get_windowed_module_c_overlay_db(
            _Pool(conn), symbol="000001.SZ", chart_timeframe="5f", levels=["5f"],
            modes=["confirmed"], first_ts=_dt(10), last_ts=_dt(20), requested_bar_count=3,
        )
        assert response is not None
        assert [item.id for item in response.strokes] == ["previous", "inside", "following"]
        assert response.strokes[0].begin_base_ts == 1
        assert response.centers[0].begin_base_ts == 9
        assert response.signals[0].base_ts == 12
        assert response.snapshot_version == _windowed_snapshot_version("000001.SZ", {
            ("5f", "confirmed"): {"run_id": 7, "snapshot_version": "s-1"},
        })
        detail_queries = [query for query in conn.queries if "chan_c_" in query and "published_heads" not in query]
        assert len(detail_queries) == 4
        assert len([query for query in detail_queries if "chan_c_strokes" in query]) == 1
        stroke_query = next(query for query in detail_queries if "chan_c_strokes" in query)
        assert "unnest($1::bigint[], $2::integer[], $3::text[]) with ordinality" in stroke_query.lower()
        assert "tstzrange(coalesce(detail.begin_base_ts, detail.start_ts), coalesce(detail.end_base_ts, detail.end_ts), '[]')" in stroke_query
        assert stroke_query.count("limit 1") == 2
        assert "cross join lateral" in stroke_query.lower()
        assert "limit $6" in stroke_query.lower()
        assert conn.queries[0] == "set local statement_timeout = '1000ms'"
        assert conn.transactions == [{"isolation": "repeatable_read", "readonly": True}]
    asyncio.run(scenario())


def test_head_selection_rejects_missing_mode_or_invalid_module_c_run() -> None:
    class RejectedConn:
        async def fetch(self, query, *_args):
            assert "head.base_timeframe = head.chan_level" in query
            assert "run.status = 'success'" in query
            assert "run.config_hash = any($6::varchar[])" in query
            return []

    assert asyncio.run(_select_windowed_module_c_runs(
        RejectedConn(), symbol_id=1, levels=["5f"], modes=["confirmed", "predictive"],
        first_ts=_dt(10), last_ts=_dt(20),
    )) is None
    assert asyncio.run(_select_windowed_module_c_runs(
        RejectedConn(), symbol_id=1, levels=["5f"], modes=["confirmed", "confirmed"],
        first_ts=_dt(10), last_ts=_dt(20),
    )) is None


def test_head_selection_rejects_each_invalid_head_attribute() -> None:
    async def assert_rejected(row: dict) -> None:
        class HeadConn:
            async def fetch(self, query, *_args):
                assert "head.mode = any($3::varchar[])" in query
                assert "head.base_timeframe = head.chan_level" in query
                assert "head.status = 'published'" in query
                assert "run.status = 'success' and run.config_hash = any($6::varchar[])" in query
                _symbol_id, _levels, modes, first, last, config_hashes = _args
                if not (
                    row["mode"] in modes
                    and row["base_timeframe"] == row["chan_level"]
                    and row["head_status"] == "published"
                    and row["run_status"] == "success"
                    and row["config_hash"] in config_hashes
                    and row["base_from_bar_end"] <= first <= last <= row["base_to_bar_end"]
                    and row["bar_from"] <= first <= last <= row["bar_until"]
                ):
                    return []
                return [row]

        assert await _select_windowed_module_c_runs(
            HeadConn(), symbol_id=1, levels=["5f"], modes=["confirmed"],
            first_ts=_dt(10), last_ts=_dt(20),
        ) is None

    valid = {
        "chan_level": 5, "mode": "confirmed", "run_id": 1, "snapshot_version": "v",
        "base_from_bar_end": _dt(1), "base_to_bar_end": _dt(99),
        "bar_from": _dt(1), "bar_until": _dt(99), "base_timeframe": 5,
        "head_status": "published", "run_status": "success",
        "config_hash": MODULE_C_CONFIG_HASH,
    }
    # The query predicates are tested independently because a DB fake must not
    # accidentally accept rows that a real WHERE clause would reject.
    for rejected in (
        {**valid, "mode": "predictive"},
        {**valid, "base_timeframe": 30},
        {**valid, "config_hash": "module-c:chan.py-native-tail-v1"},
        {**valid, "run_status": "failed"},
        {**valid, "head_status": "failed"},
        {**valid, "base_from_bar_end": _dt(11)},
        {**valid, "bar_until": _dt(19)},
    ):
        asyncio.run(assert_rejected(rejected))


def test_windowed_queries_cap_at_remaining_plus_one() -> None:
    async def scenario() -> None:
        conn = _Conn()
        response = await get_windowed_module_c_overlay_db(
            _Pool(conn), symbol="000001.SZ", chart_timeframe="5f", levels=["5f"],
            modes=["confirmed"], first_ts=_dt(10), last_ts=_dt(20), requested_bar_count=3,
        )
        assert response is not None
        main_line_args = next(args for query, args in zip(conn.queries, conn.args) if "chan_c_strokes" in query)
        assert main_line_args[-1] == 2_001
    asyncio.run(scenario())


def test_windowed_detail_batches_all_level_mode_runs_into_four_queries() -> None:
    class BatchConn(_Conn):
        async def fetch(self, query, *args):
            self.queries.append(query)
            self.args.append(args)
            if "from scheme2_chan_c_published_heads" in query:
                rows = []
                for level in (5, 30, 1440):
                    for index, mode in enumerate(("confirmed", "predictive"), start=1):
                        rows.append({
                            "chan_level": level,
                            "mode": mode,
                            "run_id": level * 10 + index,
                            "snapshot_version": f"{level}-{mode}",
                            "base_from_bar_end": _dt(1),
                            "base_to_bar_end": _dt(99),
                            "bar_from": _dt(1),
                            "bar_until": _dt(99),
                            "computed_at": _dt(99),
                        })
                return rows
            return []

    async def scenario() -> None:
        conn = BatchConn()
        response = await get_windowed_module_c_overlay_db(
            _Pool(conn), symbol="000001.SZ", chart_timeframe="5f",
            levels=["5f", "30f", "1d"], modes=["confirmed", "predictive"],
            first_ts=_dt(10), last_ts=_dt(20), requested_bar_count=3,
        )
        assert response is not None
        detail = [
            (query, args)
            for query, args in zip(conn.queries, conn.args)
            if "with requested as" in query
        ]
        assert len(detail) == 4
        for _query, args in detail:
            assert "cross join lateral" in _query.lower()
            assert _query.lower().count("limit $6") >= 2
            assert len(args[0]) == len(args[1]) == len(args[2]) == 6
            assert args[2] == ["5f", "5f", "30f", "30f", "1d", "1d"]

    asyncio.run(scenario())


def test_line_base_scan_fetches_only_remaining_plus_one_before_413() -> None:
    class CapConn:
        def __init__(self):
            self.args: list[tuple] = []

        async def fetch(self, query, *args):
            self.args.append(args)
            if "limit $5" in query:
                return [_line(11, 12, "one"), _line(13, 14, "two")]
            raise AssertionError("boundary queries must not run after the cap is exceeded")

    async def scenario() -> None:
        conn = CapConn()
        try:
            await _fetch_windowed_stroke_like(conn, "chan_c_strokes", 7, 1, _dt(10), _dt(20), _OutputBudget(1))
        except OverlayTooLargeError:
            pass
        else:
            raise AssertionError("expected the shared cap to reject the response")
        assert conn.args == [(7, 1, _dt(10), _dt(20), 2)]

    asyncio.run(scenario())


def test_windowed_sql_placeholder_arity_and_migration_expression_indexes() -> None:
    async def scenario() -> None:
        conn = _Conn()
        response = await get_windowed_module_c_overlay_db(
            _Pool(conn), symbol="000001.SZ", chart_timeframe="5f", levels=["5f"],
            modes=["confirmed"], first_ts=_dt(10), last_ts=_dt(20), requested_bar_count=3,
        )
        assert response is not None

    asyncio.run(scenario())
    migration = (Path(__file__).resolve().parents[3] / "db/sql/025_chan_c_window_indexes.sql").read_text(encoding="utf-8")
    for table in ("strokes", "segments", "centers"):
        assert f"idx_chan_c_{table}_window_range_gist" in migration
        assert "using gist" in migration
        assert "tstzrange(coalesce(begin_base_ts, start_ts), coalesce(end_base_ts, end_ts), '[]')" in migration
    assert "idx_chan_c_signals_window_lookup" in migration
    assert "(coalesce(base_ts, ts))" in migration


def test_authoritative_overlay_requires_a_bounded_valid_window() -> None:
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(db_pool=None)))
    for start, end, status in ((None, _dt(2), 422), (_dt(3), _dt(2), 400)):
        try:
            asyncio.run(chan.build_chan_overlay(
                request=request, symbol="000001.SZ", timeframe="5f", levels="5f,30f,1d",
                modes="confirmed", from_ts=start, to_ts=end, limit=3, settings=Settings(),
                authoritative_window=True,
            ))
        except HTTPException as exc:
            assert exc.status_code == status
        else:
            raise AssertionError("invalid window was accepted")


def test_monthly_overlay_accepts_the_bounded_three_hundred_bar_chart_window() -> None:
    start = datetime(2001, 7, 31, 7, tzinfo=UTC)
    end = datetime(2026, 6, 30, 7, tzinfo=UTC)
    assert chan._validate_window(start, end, "1m", 300) == (start, end)


def test_daily_and_monthly_overlay_allow_trading_calendar_gaps() -> None:
    daily_start = datetime(2025, 4, 16, 7, tzinfo=UTC)
    daily_end = datetime(2026, 7, 6, 7, tzinfo=UTC)
    monthly_start = datetime(1997, 6, 30, 0, tzinfo=UTC)
    monthly_end = datetime(2026, 6, 30, 7, tzinfo=UTC)

    assert chan._validate_window(daily_start, daily_end, "1d", 300) == (daily_start, daily_end)
    assert chan._validate_window(monthly_start, monthly_end, "1m", 300) == (monthly_start, monthly_end)


def test_chart_overlay_route_rejects_extra_levels_and_unknown_modes() -> None:
    client = TestClient(app)
    common = {
        "symbol": "000001.SZ", "timeframe": "5f",
        "from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00Z",
    }
    headers = {"Authorization": "Bearer dev-local-token"}
    assert client.get(
        "/api/v3/chart/overlay", params={**common, "levels": "5f,30f,1d,1w"}, headers=headers,
    ).status_code == 400
    assert client.get(
        "/api/v3/chart/overlay", params={**common, "modes": "unconfirmed"}, headers=headers,
    ).status_code == 400
    assert client.get(
        "/api/v3/chart/overlay", params={**common, "modes": "confirmed,confirmed"}, headers=headers,
    ).status_code == 400


def test_chart_overlay_route_requires_aware_window_and_normalizes_offsets() -> None:
    client = TestClient(app)
    headers = {"Authorization": "Bearer dev-local-token"}
    common = {"symbol": "000001.SZ", "timeframe": "5f"}
    assert client.get(
        "/api/v3/chart/overlay", params={**common, "from": "2026-01-01T00:00:00", "to": "2026-01-02T00:00:00Z"}, headers=headers,
    ).status_code == 422
    assert client.get(
        "/api/v3/chart/overlay", params={**common, "from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00"}, headers=headers,
    ).status_code == 422
    response = client.get(
        "/api/v3/chart/overlay", params={
            **common, "from": "2026-03-08T01:30:00-05:00", "to": "2026-03-08T03:30:00-04:00",
        }, headers=headers,
    )
    assert response.status_code == 200


def test_legacy_bundles_keep_fixed_levels_for_higher_chart_timeframes() -> None:
    client = TestClient(app)
    headers = {"Authorization": "Bearer dev-local-token"}
    for timeframe in ("30f", "1d"):
        v2 = client.get(
            "/api/v2/chart/bundle", params={"symbol": "000001.SZ", "timeframe": timeframe, "limit": 4}, headers=headers,
        )
        assert v2.status_code == 200
        assert v2.json()["chan"]["levels"] == ["5f", "30f", "1d"]

        v3 = client.get(
            "/api/v3/chart/bundle", params={"symbol": "000001.SZ", "timeframe": timeframe, "limit": 4}, headers=headers,
        )
        assert v3.status_code == 200
        body = v3.json()
        assert body["analysis_levels"] == ["5f", "30f", "1d"]
        assert set(body["chan"]["levels"]) == {"5f", "30f", "1d"}
        assert body["source_watermarks"]["canonical_5f_last_seen_end"] is not None

    explicit = client.get(
        "/api/v3/chart/bundle",
        params={"symbol": "000001.SZ", "timeframe": "1d", "levels": "1w", "limit": 4},
        headers=headers,
    )
    assert explicit.status_code == 200
    assert explicit.json()["analysis_levels"] == ["1w"]
