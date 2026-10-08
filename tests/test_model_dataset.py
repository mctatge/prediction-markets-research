"""Dataset builder: the lookahead rule, window filters, category mapping.

Builds a synthetic data dir (hot store + curated trades parquet) and runs the
real ``build_training_frame`` through ``pmkt.lake.connect`` — the exact
production path.
"""

import duckdb
import pytest

from pmkt.lake import connect
from pmkt.model.dataset import (
    HORIZON_LOOKBACK_S,
    Observation,
    build_training_frame,
    category_group,
)
from pmkt.state import HotStore

CLOSE = 2_000_000_000  # settled close_ts for the test markets
H1 = 3_600
H6 = 21_600


def _store(tmp_path):
    return HotStore(tmp_path / "db" / "hot.sqlite")


def _market(mid, *, result="yes", open_ts=CLOSE - 300_000, close_ts=CLOSE,
            status="settled", series="KXTEST"):
    return {
        "venue_market_id": mid, "venue_event_id": f"{mid}-EV",
        "venue_series_id": series, "title": mid, "market_type": "binary",
        "status_norm": status, "open_ts": open_ts, "close_ts": close_ts,
        "result": result,
    }


def _write_trades(data_dir, rows):
    """rows: (trade_id, market_id, ts, price). Writes one curated partition
    with the exact build_trades output schema."""
    out = data_dir / "curated" / "trades" / "venue=kalshi" / "date=2033-05-18"
    out.mkdir(parents=True)
    conn = duckdb.connect()
    conn.execute(
        """CREATE TEMP TABLE t (venue_id VARCHAR, venue_trade_id VARCHAR,
           venue_market_id VARCHAR, ts BIGINT, price_prob DECIMAL(9,6),
           no_price_prob DECIMAL(9,6), "size" DECIMAL(18,2),
           taker_outcome_side VARCHAR, taker_book_side VARCHAR,
           is_block BOOLEAN, parser_version INTEGER)"""
    )
    conn.executemany(
        "INSERT INTO t VALUES ('kalshi', ?, ?, ?, ?, ?, 1.00, 'yes', 'bid', false, 1)",
        [(tid, mid, ts, str(p), str(round(1 - p, 6))) for tid, mid, ts, p in rows],
    )
    conn.execute(f"COPY t TO '{out / 'part-0.parquet'}' (FORMAT parquet)")
    conn.close()


def _frame(tmp_path) -> list[Observation]:
    conn = connect(tmp_path)
    try:
        return build_training_frame(conn)
    finally:
        conn.close()


def _by_horizon(frame, mid):
    return {o.horizon_bucket: o for o in frame if o.venue_market_id == mid}


# ---------------------------------------------------------------------------
# (b) THE LOOKAHEAD RULE
# ---------------------------------------------------------------------------

def test_trade_after_horizon_cutoff_is_invisible(tmp_path):
    store = _store(tmp_path)
    store.upsert_series("kalshi", [{"venue_series_id": "KXTEST", "category": "Sports"}], 1)
    store.upsert_markets("kalshi", [_market("M-LEAK"), _market("M-OK")], 1)
    store.close()
    _write_trades(tmp_path, [
        # M-LEAK: its ONLY trades are AFTER close - 1h -> no 1h observation,
        # even though there are plenty of prints and they'd pass every other
        # filter. (They're also after close-6h etc., so no obs at all.)
        ("l1", "M-LEAK", CLOSE - H1 + 1, 0.90),
        ("l2", "M-LEAK", CLOSE - H1 + 2, 0.91),
        ("l3", "M-LEAK", CLOSE - H1 + 3, 0.92),
        # M-OK: 3 trades inside the 1h window; last (by ts) is 0.40
        ("k1", "M-OK", CLOSE - H1 - 300, 0.30),
        ("k2", "M-OK", CLOSE - H1 - 200, 0.35),
        ("k3", "M-OK", CLOSE - H1 - 100, 0.40),
    ])
    frame = _frame(tmp_path)
    assert not [o for o in frame if o.venue_market_id == "M-LEAK"]
    ok = _by_horizon(frame, "M-OK")
    assert set(ok) == {"1h"}
    assert ok["1h"].p_market == pytest.approx(0.40)
    assert ok["1h"].y == 1


def test_last_pre_cutoff_trade_wins_not_post_cutoff(tmp_path):
    """A market with tape on BOTH sides of close-h: the feature must be the
    last trade at ts <= close-h; the newer post-cutoff prints (with a wildly
    different price) must not bleed in."""
    store = _store(tmp_path)
    store.upsert_series("kalshi", [{"venue_series_id": "KXTEST", "category": "Sports"}], 1)
    store.upsert_markets("kalshi", [_market("M-BOTH", result="no")], 1)
    store.close()
    _write_trades(tmp_path, [
        ("a1", "M-BOTH", CLOSE - H1 - 500, 0.50),
        ("a2", "M-BOTH", CLOSE - H1 - 400, 0.52),
        ("a3", "M-BOTH", CLOSE - H1, 0.55),        # boundary: ts == close-h is allowed
        ("a4", "M-BOTH", CLOSE - H1 + 1, 0.99),    # 1s past the cutoff: invisible
        ("a5", "M-BOTH", CLOSE - 10, 0.995),
    ])
    frame = _frame(tmp_path)
    obs = _by_horizon(frame, "M-BOTH")["1h"]
    assert obs.p_market == pytest.approx(0.55)
    assert obs.y == 0


def test_stale_trades_outside_lookback_dont_count(tmp_path):
    """3 trades exist before close-h, but older than the bucket's bounded
    lookback window -> no observation (a week-old print is not a 1h-out
    price)."""
    store = _store(tmp_path)
    store.upsert_series("kalshi", [{"venue_series_id": "KXTEST", "category": "Sports"}], 1)
    store.upsert_markets("kalshi", [_market("M-STALE", open_ts=CLOSE - 3_000_000)], 1)
    store.close()
    lb = HORIZON_LOOKBACK_S["1h"]
    _write_trades(tmp_path, [
        ("s1", "M-STALE", CLOSE - H1 - lb - 30, 0.60),
        ("s2", "M-STALE", CLOSE - H1 - lb - 20, 0.61),
        ("s3", "M-STALE", CLOSE - H1 - lb - 10, 0.62),
    ])
    assert not [o for o in _frame(tmp_path) if o.horizon_bucket == "1h"]


# ---------------------------------------------------------------------------
# window/universe filters
# ---------------------------------------------------------------------------

def test_min_three_trades_in_window(tmp_path):
    store = _store(tmp_path)
    store.upsert_series("kalshi", [{"venue_series_id": "KXTEST", "category": "Sports"}], 1)
    store.upsert_markets("kalshi", [_market("M-THIN"), _market("M-3")], 1)
    store.close()
    _write_trades(tmp_path, [
        ("t1", "M-THIN", CLOSE - H1 - 200, 0.30),
        ("t2", "M-THIN", CLOSE - H1 - 100, 0.31),   # only 2 prints: excluded
        ("u1", "M-3", CLOSE - H1 - 300, 0.30),
        ("u2", "M-3", CLOSE - H1 - 200, 0.31),
        ("u3", "M-3", CLOSE - H1 - 100, 0.32),      # exactly 3: included
    ])
    frame = [o for o in _frame(tmp_path) if o.horizon_bucket == "1h"]
    assert {o.venue_market_id for o in frame} == {"M-3"}


def test_extreme_prices_and_universe_filters(tmp_path):
    store = _store(tmp_path)
    store.upsert_series("kalshi", [{"venue_series_id": "KXTEST", "category": "Sports"}], 1)
    store.upsert_markets("kalshi", [
        _market("M-PIN"),                                     # last trade 0.999: excluded
        _market("M-EDGE"),                                    # last trade 0.995: included
        _market("M-SHORT", open_ts=CLOSE - 86_399),           # sub-24h life: excluded
        _market("M-SCALAR", result="scalar"),                 # non-binary label: excluded
        _market("M-OPEN", status="open", result=None),        # not settled: excluded
    ], 1)
    store.close()
    tape = []
    for mid, price in [("M-PIN", 0.999), ("M-EDGE", 0.995), ("M-SHORT", 0.5),
                       ("M-SCALAR", 0.5), ("M-OPEN", 0.5)]:
        tape += [(f"{mid}-{i}", mid, CLOSE - H1 - 100 + i, price) for i in range(3)]
    _write_trades(tmp_path, tape)
    frame = [o for o in _frame(tmp_path) if o.horizon_bucket == "1h"]
    assert {o.venue_market_id for o in frame} == {"M-EDGE"}


def test_multiple_horizons_from_one_market(tmp_path):
    store = _store(tmp_path)
    store.upsert_series("kalshi", [{"venue_series_id": "KXTEST", "category": "Crypto"}], 1)
    store.upsert_markets("kalshi", [_market("M-MULTI", open_ts=CLOSE - 1_000_000)], 1)
    store.close()
    tape = [(f"h1-{i}", "M-MULTI", CLOSE - H1 - 100 - i, 0.42 - i * 0.01) for i in range(3)]
    tape += [(f"h6-{i}", "M-MULTI", CLOSE - H6 - 100 - i, 0.30 - i * 0.01) for i in range(3)]
    _write_trades(tmp_path, tape)
    by_h = _by_horizon(_frame(tmp_path), "M-MULTI")
    assert set(by_h) == {"1h", "6h"}
    assert by_h["1h"].p_market == pytest.approx(0.42)  # last print of each window
    assert by_h["6h"].p_market == pytest.approx(0.30)
    assert by_h["1h"].category_group == "crypto"
    # 1h window trades are NOT visible to the 6h observation (they're newer
    # than close-6h): 6h must use only its own window's last print.


# ---------------------------------------------------------------------------
# category mapping
# ---------------------------------------------------------------------------

def test_category_group_mapping_explicit():
    assert category_group("Sports") == "sports"
    assert category_group("Crypto") == "crypto"
    assert category_group("Financials") == "financials"
    assert category_group("Commodities") == "financials"
    assert category_group("Companies") == "financials"
    assert category_group("Economics") == "economics"
    assert category_group("Politics") == "politics"
    assert category_group("Elections") == "politics"
    assert category_group("World") == "politics"
    assert category_group("Entertainment") == "entertainment"
    assert category_group("Mentions") == "entertainment"
    assert category_group("Climate and Weather") == "climate"
    assert category_group("Science and Technology") == "other"
    assert category_group("Exotics") == "other"
    assert category_group("never-seen-before") == "other"
    assert category_group(None) == "other"
    assert category_group("") == "other"


def test_category_falls_back_series_then_event_then_other(tmp_path):
    store = _store(tmp_path)
    store.upsert_series("kalshi", [{"venue_series_id": "KXTEST", "category": "Economics"}], 1)
    store.upsert_events("kalshi", [{"venue_event_id": "M-EVCAT-EV", "category": "Crypto"}], 1)
    store.upsert_markets("kalshi", [
        _market("M-SER"),                       # series category wins
        _market("M-EVCAT", series="KXNOPE"),    # no series row -> event category
        _market("M-NONE", series="KXNOPE"),     # neither -> other
    ], 1)
    store.close()
    tape = []
    for mid in ("M-SER", "M-EVCAT", "M-NONE"):
        tape += [(f"{mid}-{i}", mid, CLOSE - H1 - 100 + i, 0.5) for i in range(3)]
    _write_trades(tmp_path, tape)
    groups = {o.venue_market_id: o.category_group
              for o in _frame(tmp_path) if o.horizon_bucket == "1h"}
    assert groups == {"M-SER": "economics", "M-EVCAT": "crypto", "M-NONE": "other"}


def test_missing_curated_trades_raises(tmp_path):
    store = _store(tmp_path)
    store.close()
    conn = connect(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="build-trades"):
            build_training_frame(conn)
    finally:
        conn.close()
