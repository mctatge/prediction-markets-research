"""Grading: snapshot-vs-outcome Brier math on a tiny hand-computed fixture."""

import pytest

from pmkt.model.grade import format_report, grade
from pmkt.model.score import write_snapshot
from pmkt.state import HotStore

CLOSE = 2_000_000_000
DAY = 86_400


def _forecast(mid, model_p, base_price, *, group="sports", bucket="6h",
              version="kalshi-flb-v1"):
    return {
        "venue_market_id": mid, "model_version": version, "model_p": model_p,
        "base_price": base_price, "horizon_bucket": bucket, "category_group": group,
    }


def _settled(mid, result, *, close_ts=CLOSE):
    return {
        "venue_market_id": mid, "status_norm": "settled", "result": result,
        "open_ts": close_ts - 10 * DAY, "close_ts": close_ts,
    }


def test_grade_brier_math_and_lead_filter(tmp_path):
    store = HotStore(tmp_path / "db" / "hot.sqlite")
    store.upsert_markets("kalshi", [
        _settled("M-A", "yes"),
        _settled("M-B", "no"),
        _settled("M-LATE", "yes", close_ts=CLOSE + DAY),
        _settled("M-VOID", "scalar"),   # non-binary result: ungradable
        # M-OPEN has a forecast but never settles: ungradable
    ], 1)
    store.close()

    # day 1 snapshot, made 2h before close (passes the >=1h lead filter)
    write_snapshot(tmp_path, [
        _forecast("M-A", "0.8000", "0.6000"),
        _forecast("M-B", "0.3000", "0.2000", group="crypto", bucket="24h"),
        _forecast("M-VOID", "0.5000", "0.5000"),
        _forecast("M-OPEN", "0.5000", "0.5000"),
    ], CLOSE - 7200)
    # day 2 snapshot, made 100s before M-LATE's close: excluded by the lead filter
    write_snapshot(tmp_path, [_forecast("M-LATE", "0.9990", "0.9990")],
                   CLOSE + DAY - 100)

    report = grade(tmp_path)
    assert report["n_graded"] == 2

    overall = [r for r in report["rows"]
               if r["category_group"] is None and r["horizon_bucket"] is None]
    assert len(overall) == 1
    o = overall[0]
    assert o["model_version"] == "kalshi-flb-v1"
    assert o["n"] == 2
    # M-A: y=1 -> model (0.8-1)^2=0.04, market (0.6-1)^2=0.16
    # M-B: y=0 -> model 0.09, market 0.04
    assert o["brier_model"] == pytest.approx((0.04 + 0.09) / 2)
    assert o["brier_market"] == pytest.approx((0.16 + 0.04) / 2)
    assert o["brier_delta"] == pytest.approx(0.065 - 0.10)

    by_cell = {
        (r["category_group"], r["horizon_bucket"]): r for r in report["rows"]
    }
    assert by_cell[("sports", "6h")]["n"] == 1
    assert by_cell[("sports", "6h")]["brier_model"] == pytest.approx(0.04)
    assert by_cell[("crypto", "24h")]["brier_market"] == pytest.approx(0.04)

    text = format_report(report)
    assert "kalshi-flb-v1" in text
    assert "not trading authorization" in text.lower()


def test_grade_without_snapshots(tmp_path):
    HotStore(tmp_path / "db" / "hot.sqlite").close()
    report = grade(tmp_path)
    assert report == {"rows": [], "n_graded": 0, "note": "no forecast snapshots yet"}
    assert "no graded forecasts yet" in format_report(report)
