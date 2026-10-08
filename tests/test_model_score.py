"""Scoring: decimal-string outputs, out-of-support skips, snapshots, upserts."""

import re
import time
from decimal import Decimal

import duckdb
import pytest

from pmkt.model import score as score_mod
from pmkt.model.calibrate import write_artifact
from pmkt.model.score import (
    base_price_of,
    horizon_bucket_for,
    load_current_artifact,
    score_once,
    score_rows,
    snapshot_path,
    write_snapshot,
)
from pmkt.state import HotStore

NOW = 2_000_000_000
DAY = 90_000  # > MIN_DURATION_S


def _artifact(version=1, shadow=False):
    """Minimal valid artifact: identity global curve + a distinct sports|6h
    curve so group resolution is observable."""
    base = f"kalshi-flb-v{version}"
    return {
        "version": version,
        "model_version": ("shadow-" + base) if shadow else base,
        "shadow": shadow,
        "trained_at": NOW,
        "train_window": [NOW - 10 * DAY, NOW],
        "n_train": 100, "n_test": 30, "k": 2000.0,
        "horizons": ["1h", "6h", "24h", "72h", "168h"],
        "curves": {
            "*|*": [(0.0, 0.0), (1.0, 1.0)],
            "sports|6h": [(0.0, 0.1), (1.0, 0.9)],
        },
        "fallback_map": {"sports|6h": "sports|6h"},
        "mz": {"psi": 0.0, "alpha": 0.0, "n": 130},
        "metrics": {"gate_pass": True},
    }


def _open_market(mid, *, bid=None, ask=None, last=None, close_ts=NOW + 8 * 3600,
                 open_ts=None, status="open", series="KXSPORT"):
    # default 8h to close → floors into the 6h training bucket (dataset.py
    # trains 6h on TTC ∈ [6h, 24h)); fixtures assert sports|6h behavior.
    return {
        "venue_market_id": mid, "venue_series_id": series, "title": mid,
        "status_norm": status, "open_ts": open_ts or (close_ts - 10 * DAY),
        "close_ts": close_ts, "yes_bid_prob": bid, "yes_ask_prob": ask,
        "last_prob": last,
    }


@pytest.fixture
def store(tmp_path):
    s = HotStore(tmp_path / "db" / "hot.sqlite")
    s.upsert_series("kalshi", [{"venue_series_id": "KXSPORT", "category": "Sports"}], 1)
    yield s
    s.close()


# ---------------------------------------------------------------------------
# horizon mapping + base price
# ---------------------------------------------------------------------------

def test_horizon_bucket_mapping():
    # FLOOR to the bucket whose training window [h, next_h) contains TTC —
    # dataset.py trains each bucket on disjoint TTC ranges (review 2026-07-12).
    assert horizon_bucket_for(60) == "1h"       # below smallest → clamp up
    assert horizon_bucket_for(3_600) == "1h"    # exactly 1h
    assert horizon_bucket_for(3_601) == "1h"    # 1h+1s stays 1h (was 6h: the bug)
    assert horizon_bucket_for(18_000) == "1h"   # 5h out → 1h window [1h,6h)
    assert horizon_bucket_for(21_599) == "1h"   # just under 6h
    assert horizon_bucket_for(21_600) == "6h"   # exactly 6h
    assert horizon_bucket_for(86_400) == "24h"  # exactly 24h
    assert horizon_bucket_for(90_000) == "24h"  # 25h out → 24h (was 72h→skipped)
    assert horizon_bucket_for(259_200) == "72h"
    assert horizon_bucket_for(604_800) == "168h"
    assert horizon_bucket_for(10_000_000) == "168h"


def test_base_price_mid_needs_both_sides_else_last():
    assert base_price_of({"yes_bid_prob": "0.40", "yes_ask_prob": "0.44"}) == Decimal("0.42")
    assert base_price_of(
        {"yes_bid_prob": "0.40", "yes_ask_prob": None, "last_prob": "0.6100"}
    ) == Decimal("0.6100")
    assert base_price_of({"last_prob": "0.5"}) == Decimal("0.5")
    assert base_price_of({}) is None
    assert base_price_of({"yes_bid_prob": "junk", "last_prob": ""}) is None


# ---------------------------------------------------------------------------
# (e) scoring: decimal strings, skips, group resolution
# ---------------------------------------------------------------------------

def test_score_rows_decimal_strings_and_group_curve(store):
    store.upsert_markets("kalshi", [
        _open_market("M-MID", bid="0.40", ask="0.44"),          # mid 0.42, 6h bucket
        # 168h bucket has NO trained curve in the fixture artifact -> the
        # market is outside trained support and must be SKIPPED, not
        # extrapolated through the global *|* curve (honesty over coverage).
        _open_market("M-FAR", last="0.6100", close_ts=NOW + 30 * DAY),
        # ...but a bucket with a trained bucket-wide curve scores fine.
        # 30h to close → floors into the 24h bucket (TTC ∈ [24h, 72h)).
        _open_market("M-LAST", last="0.6100", close_ts=NOW + 30 * 3600),
    ], NOW)
    artifact = _artifact()
    artifact["curves"]["*|24h"] = [(0.0, 0.0), (1.0, 1.0)]
    rows = score_rows(store, artifact, NOW)
    by_id = {r["venue_market_id"]: r for r in rows}
    assert set(by_id) == {"M-MID", "M-LAST"}, "M-FAR outside support is skipped"

    mid = by_id["M-MID"]
    assert mid["horizon_bucket"] == "6h"
    assert mid["category_group"] == "sports"
    # sports|6h curve: 0.1 + 0.8 * 0.42 = 0.436
    assert mid["model_p"] == "0.4360"
    assert mid["base_price"] == "0.42"

    last = by_id["M-LAST"]
    assert last["horizon_bucket"] == "24h"
    assert last["model_p"] == "0.6100"      # identity *|24h bucket curve
    assert last["base_price"] == "0.6100"

    for r in rows:  # stored/served values are decimal STRINGS, never floats
        assert isinstance(r["model_p"], str)
        assert isinstance(r["base_price"], str)
        assert re.fullmatch(r"0\.\d{4}", r["model_p"])
        Decimal(r["model_p"]), Decimal(r["base_price"])  # parseable


def test_shadow_model_is_still_scored_for_self_grading(store):
    # Shadow models (failed validation gate) MUST still be scored + logged so
    # `pmkt model grade` can measure whether they'd have been good — the UI
    # hides them, but score_rows does not (design restored 2026-07-12).
    store.upsert_markets("kalshi", [_open_market("M-MID", bid="0.40", ask="0.44")], NOW)
    rows = score_rows(store, _artifact(shadow=True), NOW)
    assert len(rows) == 1
    assert rows[0]["model_version"].startswith("shadow-")
    assert rows[0]["model_p"] == "0.4360"


def test_score_skips_out_of_support_markets(store):
    store.upsert_markets("kalshi", [
        _open_market("M-GOOD", last="0.5"),
        _open_market("M-PIN", last="0.0010"),                       # < 0.005
        _open_market("M-PIN2", bid="0.9990", ask="1.0000"),         # mid > 0.995
        _open_market("M-NOPRICE"),                                  # no price at all
        _open_market("M-SHORT", last="0.5", close_ts=NOW + 7200,
                     open_ts=NOW + 7200 - DAY // 2),                # sub-24h life
        _open_market("M-CLOSING", last="0.5", close_ts=NOW),        # close_ts <= now
        _open_market("M-SETTLED", last="0.5", status="settled"),    # not open
    ], NOW)
    rows = score_rows(store, _artifact(), NOW)
    assert {r["venue_market_id"] for r in rows} == {"M-GOOD"}


# ---------------------------------------------------------------------------
# forecasts table: upsert + read
# ---------------------------------------------------------------------------

def test_upsert_forecasts_one_current_row_per_market(store):
    row = {
        "venue_market_id": "M-1", "model_version": "kalshi-flb-v1",
        "model_p": "0.4360", "base_price": "0.42",
        "horizon_bucket": "6h", "category_group": "sports",
    }
    store.upsert_forecasts("kalshi", [row], NOW)
    store.upsert_forecasts("kalshi", [dict(row, model_p="0.5000")], NOW + 60)
    got = store.latest_forecasts("kalshi")
    assert len(got) == 1
    assert got[0]["model_p"] == "0.5000"
    assert got[0]["created_ts"] == NOW + 60
    assert got[0]["base_price"] == "0.42"
    assert isinstance(got[0]["model_p"], str)


# ---------------------------------------------------------------------------
# snapshot: first run of the UTC day writes, later runs skip
# ---------------------------------------------------------------------------

def test_snapshot_written_once_per_day(tmp_path, store):
    store.upsert_markets("kalshi", [_open_market("M-MID", bid="0.40", ask="0.44")], NOW)
    rows = score_rows(store, _artifact(), NOW)

    path = write_snapshot(tmp_path, rows, NOW)
    assert path == snapshot_path(tmp_path, NOW)
    assert write_snapshot(tmp_path, rows, NOW + 3600) is None  # same UTC day: skip
    next_day = write_snapshot(tmp_path, rows, NOW + 86_400)
    assert next_day is not None and next_day != path

    got = duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{path}', hive_partitioning=false)"
    ).fetchall()
    cols = [c[0] for c in duckdb.connect().execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}', hive_partitioning=false)"
    ).fetchall()]
    assert cols == ["venue_id", "venue_market_id", "model_version", "model_p",
                    "base_price", "horizon_bucket", "category_group", "created_ts"]
    assert len(got) == 1
    assert got[0][cols.index("model_p")] == "0.4360"
    assert got[0][cols.index("created_ts")] == NOW


def test_snapshot_empty_rows_is_noop(tmp_path):
    assert write_snapshot(tmp_path, [], NOW) is None
    assert not snapshot_path(tmp_path, NOW).exists()


# ---------------------------------------------------------------------------
# score_once end-to-end: artifact loading, cache, upsert + snapshot
# ---------------------------------------------------------------------------

def test_score_once_end_to_end(tmp_path, store, monkeypatch):
    monkeypatch.setattr(score_mod, "_artifact_cache", None)
    write_artifact(tmp_path, _artifact())
    store.upsert_markets("kalshi", [_open_market("M-MID", bid="0.40", ask="0.44")], NOW)

    report = score_once(tmp_path, store, NOW)
    assert report == {"forecasts": 1, "snapshot": 1}
    assert store.latest_forecasts("kalshi")[0]["model_version"] == "kalshi-flb-v1"
    assert snapshot_path(tmp_path, NOW).exists()

    # second run same day: forecasts refresh, snapshot skipped
    report2 = score_once(tmp_path, store, NOW + 60)
    assert report2 == {"forecasts": 1, "snapshot": 0}


def test_score_once_without_artifact_is_noop(tmp_path, store, monkeypatch):
    monkeypatch.setattr(score_mod, "_artifact_cache", None)
    assert score_once(tmp_path, store, NOW) == {"forecasts": 0, "snapshot": 0}
    assert store.latest_forecasts("kalshi") == []


def test_artifact_cache_reloads_on_new_version(tmp_path, monkeypatch):
    monkeypatch.setattr(score_mod, "_artifact_cache", None)
    write_artifact(tmp_path, _artifact(version=1))
    a1 = load_current_artifact(tmp_path)
    assert a1["model_version"] == "kalshi-flb-v1"
    assert load_current_artifact(tmp_path) is a1  # cached: same object

    time.sleep(0.01)
    write_artifact(tmp_path, _artifact(version=2))
    assert load_current_artifact(tmp_path)["model_version"] == "kalshi-flb-v2"
