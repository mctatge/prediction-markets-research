"""Screener UI API — contract tests against a seeded tmp hot store + raw lake.

The API must return the stored TEXT decimal strings verbatim — a float
anywhere in a price field is a decimal-string format violation, asserted globally via
json.loads(parse_float=...).
"""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from pmkt.config import Config
from pmkt.rawlake import RawLake
from pmkt.state import HotStore
from pmkt.ui import create_app

NOW = int(time.time())
TOKEN = "1110001"  # P1 primary CLOB token (digits-only, like the real ids)

KALSHI_MARKETS = [
    {
        "venue_market_id": "KXHIGHNY-26JUL12-B55", "venue_event_id": "KXHIGHNY-26JUL12",
        "venue_series_id": "KXHIGHNY", "title": "High temp in NYC above 55?",
        "market_type": "binary", "status_norm": "open",
        "close_ts": NOW + 7200, "yes_bid_prob": "0.4200", "yes_ask_prob": "0.4400",
        "last_prob": "0.4300", "volume": "1523.00", "volume_24h": "410.00",
        "open_interest": "988.00", "liquidity": "12345.6789",
    },
    {
        "venue_market_id": "KXHIGHNY-26JUL12-B60", "venue_event_id": "KXHIGHNY-26JUL12",
        "venue_series_id": "KXHIGHNY", "title": "High temp in NYC above 60?",
        "market_type": "binary", "status_norm": "open",
        "close_ts": NOW + 7200, "yes_bid_prob": "0.1000", "yes_ask_prob": "0.2000",
        "last_prob": "0.1500", "volume": "10.00", "volume_24h": "5.00",
        "open_interest": "3.00", "liquidity": "500.00",
    },
    {  # settled — excluded from default screener
        "venue_market_id": "KXOLD-1", "title": "Old settled market",
        "venue_series_id": "KXOLD", "status_norm": "settled", "result": "yes",
        "last_prob": "1.0000", "volume": "50.00",
    },
    {  # every price/size NULL, no close, no title — the NULLs-last row
        "venue_market_id": "KXNULL-1", "venue_series_id": "KXNULL",
        "status_norm": "open",
    },
    {  # bid/ask unknown, last known — price-band fallback + unknown spread
        "venue_market_id": "KXLAST-1", "venue_series_id": "KXLAST",
        "title": "Only a last trade", "status_norm": "open",
        "last_prob": "0.4500", "volume_24h": "50.00", "liquidity": "10.00",
        "close_ts": NOW + 3600,
    },
    {  # literal % and _ in the title — LIKE-escaping target
        "venue_market_id": "KXPCT-100", "venue_event_id": "KXPCT-EV",
        "venue_series_id": "KXPCT", "title": "Will X gain 100% by _2027_?",
        "status_norm": "open", "yes_bid_prob": "0.7000", "yes_ask_prob": "0.7200",
        "last_prob": "0.7100", "volume": "700.00", "volume_24h": "100.00",
        "liquidity": "20.00", "close_ts": NOW + 9000,
    },
    {  # contains "100" but not "100%" — must NOT match an escaped q=100%
        "venue_market_id": "KXPCT-1000", "venue_event_id": "KXPCT-EV",
        "venue_series_id": "KXPCT", "title": "Will Y hit 1000 points?",
        "status_norm": "open", "yes_bid_prob": "0.0500", "yes_ask_prob": "0.0700",
        "volume": "800.00", "volume_24h": "200.00", "close_ts": NOW + 9000,
    },
    {  # contains a2027b — must NOT match an escaped q=_2027_
        "venue_market_id": "KXPCT-A2027", "venue_event_id": "KXPCT-EV",
        "venue_series_id": "KXPCT", "title": "Index a2027b milestone",
        "status_norm": "open", "last_prob": "0.8000", "volume_24h": "300.00",
        "close_ts": NOW + 9000,
    },
]

POLYMARKET_MARKETS = [
    {
        "venue_market_id": "0xc0ffee01", "venue_secondary_id": "501234",
        "venue_event_id": "912345", "venue_series_id": "btc-price",
        "slug": "will-btc-hit-100k", "title": "Will BTC hit 100k?",
        "market_type": "binary", "status_norm": "open", "close_ts": NOW + 86400,
        "yes_bid_prob": "0.315", "yes_ask_prob": "0.345", "last_prob": "0.33",
        "volume": "150000.5", "volume_24h": "5000.25", "liquidity": "88000.1234",
        "neg_risk": 0,
    },
    {  # 5m churn — dropped by exclude_churn (default true)
        "venue_market_id": "0xchurn5m", "slug": "btc-updown-5m-x",
        "title": "BTC up in the next 5 minutes?", "status_norm": "open",
        "recurrence": "5m", "yes_bid_prob": "0.50", "yes_ask_prob": "0.52",
        "volume_24h": "999999",
    },
    {  # no market slug, has series slug — event-slug URL fallback
        "venue_market_id": "0xnoslug1", "venue_series_id": "some-series-slug",
        "title": "Slugless market", "status_norm": "open", "last_prob": "0.60",
        "volume_24h": "1.5", "close_ts": NOW + 50000,
    },
    {  # no slug at all — bare homepage URL
        "venue_market_id": "0xnoslug2", "title": "Fully slugless",
        "status_norm": "open",
    },
]


def _seed_history_captures(raw_dir):
    fetched1 = datetime.fromtimestamp(NOW - 120, tz=timezone.utc)
    fetched2 = datetime.fromtimestamp(NOW - 60, tz=timezone.utc)
    t_old, t1, t2 = NOW - 200_000, NOW - 600, NOW - 300

    def candles_payload(close_t1):
        return {
            "markets": [
                {
                    "market_ticker": "KXHIGHNY-26JUL12-B55",
                    "candlesticks": [
                        {"end_period_ts": t_old,  # outside a 24h window
                         "price": {"close_dollars": "0.1000"},
                         "yes_bid": {"close_dollars": "0.0900"},
                         "yes_ask": {"close_dollars": "0.1100"}},
                        {"end_period_ts": t1,
                         "price": {"close_dollars": close_t1},
                         "yes_bid": {"close_dollars": "0.4200"},
                         "yes_ask": {"close_dollars": "0.4400"}},
                        {"end_period_ts": t2,  # no trades → mid of bid/ask
                         "price": None,
                         "yes_bid": {"close_dollars": "0.4200"},
                         "yes_ask": {"close_dollars": "0.4500"}},
                    ],
                },
                {  # other market in the same batch — must be filtered out
                    "market_ticker": "KXOTHER-1",
                    "candlesticks": [
                        {"end_period_ts": t1, "price": {"close_dollars": "0.9999"},
                         "yes_bid": None, "yes_ask": None},
                    ],
                },
            ]
        }

    with RawLake(raw_dir) as lake:
        lake.append(
            venue="kalshi", endpoint="markets_candlesticks",
            params={"market_tickers": "KXHIGHNY-26JUL12-B55,KXOTHER-1",
                    "start_ts": NOW - 5400, "end_ts": NOW, "period_interval": 1},
            http_status=200, elapsed_ms=50,
            payload=candles_payload("0.4300"), fetched_at=fetched1,
        )
        # overlapping re-fetch: same t1 with a newer close — dedupe keeps this
        lake.append(
            venue="kalshi", endpoint="markets_candlesticks",
            params={"market_tickers": "KXHIGHNY-26JUL12-B55",
                    "start_ts": NOW - 5400, "end_ts": NOW, "period_interval": 1},
            http_status=200, elapsed_ms=50,
            payload=candles_payload("0.4400"), fetched_at=fetched2,
        )
        lake.append(
            venue="polymarket", endpoint="clob_prices_history",
            params={"_body": {"markets": [TOKEN, "999888777"],
                    "start_ts": NOW - 5400, "end_ts": NOW, "fidelity": 1}},
            http_status=200, elapsed_ms=200,
            payload={"history": {
                TOKEN: [{"t": NOW - 500, "p": 0.31}, {"t": NOW - 400, "p": 0.32}],
                "999888777": [{"t": NOW - 500, "p": 0.99}],
            }},
            fetched_at=fetched2,
        )


@pytest.fixture(scope="module")
def data_dir(tmp_path_factory):
    root = tmp_path_factory.mktemp("uidata")
    store = HotStore(root / "db" / "hot.sqlite")
    store.upsert_series("kalshi", [
        {"venue_series_id": "KXHIGHNY", "title": "NYC high temp", "category": "Climate"},
        {"venue_series_id": "KXPCT", "title": "Percent things", "category": "Economics"},
    ], NOW)
    store.upsert_events("kalshi", [
        {"venue_event_id": "KXHIGHNY-26JUL12", "venue_series_id": "KXHIGHNY",
         "title": "NYC high temp Jul 12", "category": "Climate", "status": "open"},
        {"venue_event_id": "KXPCT-EV", "venue_series_id": "KXPCT",
         "title": "Percent event", "category": "Economics", "status": "open"},
    ], NOW)
    store.upsert_events("polymarket", [
        {"venue_event_id": "912345", "venue_series_id": "btc-price",
         "title": "BTC price", "category": "Crypto", "status": "open",
         "mutually_exclusive": 0},
    ], NOW)
    store.upsert_markets("kalshi", KALSHI_MARKETS, NOW)
    store.upsert_markets("polymarket", POLYMARKET_MARKETS, NOW)
    store.upsert_instruments("polymarket", [
        {"venue_market_id": "0xc0ffee01", "outcome_index": 0,
         "outcome_name": "Yes", "venue_token_id": TOKEN, "is_primary": True},
        {"venue_market_id": "0xc0ffee01", "outcome_index": 1,
         "outcome_name": "No", "venue_token_id": "2220002", "is_primary": False},
    ], NOW)
    store.set_state("kalshi", "prices", watermark_ts=NOW - 100, now_ts=NOW)
    store.set_state("kalshi", "metadata", watermark_ts=NOW - 900, now_ts=NOW)
    store.set_state("polymarket", "prices", watermark_ts=NOW - 200, now_ts=NOW)
    # the one NULL prices_updated_ts row (upsert always stamps it)
    store.conn.execute(
        "UPDATE markets SET prices_updated_ts=NULL WHERE venue_market_id='KXNULL-1'"
    )
    store.conn.commit()
    store.close()
    _seed_history_captures(root / "raw")
    return root


@pytest.fixture(scope="module")
def client(data_dir):
    app = create_app(Config(data_dir=data_dir), ui_dist=data_dir / "no-dist")
    with TestClient(app) as c:
        yield c


def _get(client, url, **params):
    resp = client.get(url, params=params or None)
    assert resp.status_code == 200, resp.text
    return resp


def _rows_by_id(payload):
    return {r["market_id"]: r for r in payload["rows"]}


# ---------------------------------------------------------------------------
# default filtering
# ---------------------------------------------------------------------------

def test_default_open_and_exclude_churn(client):
    payload = _get(client, "/api/screener").json()
    ids = set(_rows_by_id(payload))
    assert "KXOLD-1" not in ids          # settled filtered by default status
    assert "0xchurn5m" not in ids        # churn recurrence filtered by default
    assert payload["total"] == len(payload["rows"]) == 10
    assert all(r["status"] == "open" for r in payload["rows"])
    assert isinstance(payload["generated_at"], int)


def test_status_and_churn_toggles(client):
    settled = _get(client, "/api/screener", status="settled").json()
    assert set(_rows_by_id(settled)) == {"KXOLD-1"}
    everything = _get(
        client, "/api/screener", status="all", exclude_churn="false"
    ).json()
    assert everything["total"] == 12
    assert "0xchurn5m" in _rows_by_id(everything)


def test_venue_and_category_filters(client):
    kalshi = _get(client, "/api/screener", venue="kalshi").json()
    assert {r["venue"] for r in kalshi["rows"]} == {"kalshi"}
    climate = _get(client, "/api/screener", category="Climate").json()
    assert set(_rows_by_id(climate)) == {"KXHIGHNY-26JUL12-B55", "KXHIGHNY-26JUL12-B60"}
    assert climate["total"] == 2
    crypto = _get(client, "/api/screener", category="Crypto").json()
    assert set(_rows_by_id(crypto)) == {"0xc0ffee01"}


# ---------------------------------------------------------------------------
# sorting — every column, both directions, NULLs last
# ---------------------------------------------------------------------------

def _sort_key(row, sort):
    def dec(text):
        return None if text is None else Decimal(text)

    if sort == "vol24h":
        return dec(row["volume_24h"])
    if sort == "volume":
        return dec(row["volume"])
    if sort == "price":  # mid with last fallback — mirrors the SQL filter expr
        return dec(row["mid"]) if row["mid"] is not None else dec(row["last"])
    if sort == "spread":
        return dec(row["spread"])
    if sort == "close_ts":
        return row["close_ts"]
    if sort == "open_interest":
        return dec(row["open_interest"])
    if sort == "liquidity":
        return dec(row["liquidity"])
    if sort == "updated":
        return row["prices_updated_ts"]
    if sort == "title":
        return None if row["title"] is None else row["title"].lower()
    raise AssertionError(sort)


@pytest.mark.parametrize("direction", ["desc", "asc"])
@pytest.mark.parametrize(
    "sort",
    ["vol24h", "volume", "price", "spread", "close_ts",
     "open_interest", "liquidity", "updated", "title"],
)
def test_sort_columns_nulls_last(client, sort, direction):
    payload = _get(
        client, "/api/screener", sort=sort, dir=direction, status="all",
        exclude_churn="false",
    ).json()
    keys = [_sort_key(r, sort) for r in payload["rows"]]
    assert len(keys) == 12
    # NULL keys form a contiguous tail regardless of direction
    non_null = [k for k in keys if k is not None]
    assert keys[: len(non_null)] == non_null, f"{sort} {direction}: NULLs not last"
    assert any(k is None for k in keys), f"{sort}: seed data has no NULL key"
    expected = sorted(non_null, reverse=(direction == "desc"))
    assert non_null == expected, f"{sort} {direction}: not sorted"


# ---------------------------------------------------------------------------
# q search + LIKE escaping
# ---------------------------------------------------------------------------

def test_q_escapes_percent(client):
    ids = set(_rows_by_id(_get(client, "/api/screener", q="100%").json()))
    assert ids == {"KXPCT-100"}  # literal "100%", not LIKE %100(anything)%


def test_q_escapes_underscore(client):
    ids = set(_rows_by_id(_get(client, "/api/screener", q="_2027_").json()))
    assert ids == {"KXPCT-100"}  # literal underscores; a2027b must not match


def test_q_matches_ids_and_slug(client):
    by_slug = set(_rows_by_id(_get(client, "/api/screener", q="will-btc").json()))
    assert by_slug == {"0xc0ffee01"}
    by_event = set(_rows_by_id(_get(client, "/api/screener", q="kxpct-ev").json()))
    assert by_event == {"KXPCT-100", "KXPCT-1000", "KXPCT-A2027"}  # case-insensitive


# ---------------------------------------------------------------------------
# numeric filters
# ---------------------------------------------------------------------------

def test_price_band_mid_with_last_fallback(client):
    payload = _get(
        client, "/api/screener", price_min=0.3, price_max=0.5
    ).json()
    ids = set(_rows_by_id(payload))
    assert "KXHIGHNY-26JUL12-B55" in ids   # mid 0.43
    assert "0xc0ffee01" in ids             # mid 0.330
    assert "KXLAST-1" in ids               # no bid/ask → last 0.4500 fallback
    assert "KXPCT-100" not in ids          # mid 0.71 outside band
    assert "KXNULL-1" not in ids           # no price at all → excluded


def test_spread_max_passes_unknown_spreads(client):
    payload = _get(client, "/api/screener", spread_max=0.05).json()
    ids = set(_rows_by_id(payload))
    assert "KXHIGHNY-26JUL12-B55" in ids   # spread 0.0200
    assert "KXHIGHNY-26JUL12-B60" not in ids  # spread 0.1000 — filtered
    assert "KXLAST-1" in ids               # unknown spread — passes
    assert "KXNULL-1" in ids               # unknown spread — passes


def test_vol24h_and_liquidity_and_close_within(client):
    vol = _get(client, "/api/screener", vol24h_min=150).json()
    assert set(_rows_by_id(vol)) == {"KXPCT-1000", "KXPCT-A2027",
                                     "KXHIGHNY-26JUL12-B55", "0xc0ffee01"}
    liq = _get(client, "/api/screener", liquidity_min=1000).json()
    assert set(_rows_by_id(liq)) == {"KXHIGHNY-26JUL12-B55", "0xc0ffee01"}
    soon = _get(client, "/api/screener", close_within_h=2).json()
    assert set(_rows_by_id(soon)) == {"KXHIGHNY-26JUL12-B55",
                                      "KXHIGHNY-26JUL12-B60", "KXLAST-1"}


# ---------------------------------------------------------------------------
# limit clamp
# ---------------------------------------------------------------------------

def test_limit_clamped_at_200(tmp_path):
    store = HotStore(tmp_path / "db" / "hot.sqlite")
    store.upsert_markets("kalshi", [
        {"venue_market_id": f"KXBULK-{i}", "venue_series_id": "KXBULK",
         "title": f"Bulk {i}", "status_norm": "open", "last_prob": "0.5000"}
        for i in range(205)
    ], NOW)
    store.close()
    with TestClient(create_app(Config(data_dir=tmp_path),
                               ui_dist=tmp_path / "no-dist")) as c:
        payload = _get(c, "/api/screener", limit=999).json()
    assert payload["total"] == 205
    assert len(payload["rows"]) == 200  # clamped, not rejected


def test_offset_pages(client):
    first = _get(client, "/api/screener", sort="title", dir="asc", limit=3).json()
    second = _get(client, "/api/screener", sort="title", dir="asc",
                  limit=3, offset=3).json()
    assert first["total"] == second["total"]
    assert not set(_rows_by_id(first)) & set(_rows_by_id(second))


# ---------------------------------------------------------------------------
# decimal-string fidelity (data invariants)
# ---------------------------------------------------------------------------

def test_no_floats_anywhere_and_text_verbatim(client):
    resp = _get(client, "/api/screener", status="all", exclude_churn="false")

    def reject_float(text):
        raise AssertionError(f"float in payload: {text}")

    payload = json.loads(resp.text, parse_float=reject_float)
    rows = {r["market_id"]: r for r in payload["rows"]}

    k1 = rows["KXHIGHNY-26JUL12-B55"]
    assert k1["yes_bid"] == "0.4200"       # byte-equal to the stored TEXT
    assert k1["yes_ask"] == "0.4400"
    assert k1["last"] == "0.4300"
    assert k1["mid"] == "0.4300"           # Decimal-computed, venue scale kept
    assert k1["spread"] == "0.0200"
    assert k1["liquidity"] == "12345.6789"
    assert k1["volume"] == "1523.00"
    assert k1["model_p"] is None and k1["edge"] is None

    p1 = rows["0xc0ffee01"]
    assert p1["yes_bid"] == "0.315"
    assert p1["mid"] == "0.330"
    assert p1["spread"] == "0.030"

    knull = rows["KXNULL-1"]
    assert knull["mid"] is None and knull["spread"] is None


# ---------------------------------------------------------------------------
# venue URLs
# ---------------------------------------------------------------------------

def test_urls(client):
    rows = _rows_by_id(_get(client, "/api/screener", status="all",
                            exclude_churn="false").json())
    assert rows["KXHIGHNY-26JUL12-B55"]["url"] == "https://kalshi.com/markets/KXHIGHNY"
    assert rows["0xc0ffee01"]["url"] == "https://polymarket.com/market/will-btc-hit-100k"
    assert rows["0xnoslug1"]["url"] == "https://polymarket.com/event/some-series-slug"
    assert rows["0xnoslug2"]["url"] == "https://polymarket.com"


# ---------------------------------------------------------------------------
# market detail + siblings
# ---------------------------------------------------------------------------

def test_market_detail_and_siblings(client):
    payload = _get(client, "/api/market/kalshi/KXHIGHNY-26JUL12-B60").json()
    assert payload["row"]["market_id"] == "KXHIGHNY-26JUL12-B60"
    sibs = [s["market_id"] for s in payload["siblings"]]
    # same event, mid desc (0.43 then 0.15), the row itself included
    assert sibs == ["KXHIGHNY-26JUL12-B55", "KXHIGHNY-26JUL12-B60"]


def test_market_detail_no_event(client):
    payload = _get(client, "/api/market/kalshi/KXOLD-1").json()
    assert [s["market_id"] for s in payload["siblings"]] == ["KXOLD-1"]


def test_market_404s(client):
    assert client.get("/api/market/kalshi/NOPE").status_code == 404
    assert client.get("/api/market/bogus/KXOLD-1").status_code == 404


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------

def test_history_kalshi(client):
    resp = _get(client, "/api/market/kalshi/KXHIGHNY-26JUL12-B55/history", hours=24)
    points = json.loads(
        resp.text, parse_float=lambda t: pytest.fail(f"float point: {t}")
    )["points"]
    assert [p["p"] for p in points] == ["0.4400", "0.4350"]
    # dedupe kept the later capture's t1 close; t2 is the bid/ask midpoint;
    # the 0.9999 close belongs to KXOTHER-1 and the old candle is out-of-window
    assert points[0]["t"] < points[1]["t"]
    assert all(isinstance(p["t"], int) for p in points)


def test_history_polymarket(client):
    resp = _get(client, "/api/market/polymarket/0xc0ffee01/history", hours=24)
    points = resp.json()["points"]
    assert [(p["t"], p["p"]) for p in points] == [
        (NOW - 500, "0.31"), (NOW - 400, "0.32"),
    ]


def test_history_no_primary_token_is_empty(client):
    resp = _get(client, "/api/market/polymarket/0xnoslug1/history")
    assert resp.json() == {"points": []}


def test_history_validation(client):
    assert client.get(
        "/api/market/kalshi/KXHIGHNY-26JUL12-B55/history", params={"hours": 200}
    ).status_code == 422
    assert client.get("/api/market/kalshi/NOPE/history").status_code == 404


# ---------------------------------------------------------------------------
# meta
# ---------------------------------------------------------------------------

def test_meta_shape(client):
    meta = _get(client, "/api/meta").json()
    assert meta["venues"] == ["kalshi", "polymarket"]
    assert meta["counts"] == {
        "open_total": 11, "open_kalshi": 7, "open_polymarket": 4,
    }
    cats = {(c["venue"], c["category"]): c["n"] for c in meta["categories"]}
    assert cats[("kalshi", "Climate")] == 2
    assert cats[("kalshi", "Economics")] == 3
    assert cats[("polymarket", "Crypto")] == 1
    fresh = {(f["venue"], f["endpoint"]): f["watermark_ts"] for f in meta["freshness"]}
    assert fresh[("kalshi", "prices")] == NOW - 100
    assert fresh[("polymarket", "prices")] == NOW - 200


# ---------------------------------------------------------------------------
# static / SPA serving
# ---------------------------------------------------------------------------

def test_spa_serving_and_fallback(data_dir, tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html><body>pmkt ui</body></html>")
    (dist / "app.js").write_text("console.log('x')")
    with TestClient(create_app(Config(data_dir=data_dir), ui_dist=dist)) as c:
        assert "pmkt ui" in c.get("/").text
        assert c.get("/app.js").status_code == 200
        # client-side route → SPA fallback to index.html, not 404
        assert "pmkt ui" in c.get("/screener/some/route").text
        # API routes take precedence over the mount
        assert c.get("/api/meta").status_code == 200
        assert c.get("/api/market/kalshi/NOPE").status_code == 404


# ---------------------------------------------------------------------------
# read-only guarantee
# ---------------------------------------------------------------------------

def test_hot_store_connection_refuses_writes(data_dir):
    from pmkt.ui.app import _open_ro

    conn = _open_ro(data_dir / "db" / "hot.sqlite")
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute(
                "INSERT INTO collector_state (venue, endpoint, params_hash, updated_at)"
                " VALUES ('x', 'y', '', 0)"
            )
    finally:
        conn.close()


def _mk_artifact(*, model_version="kalshi-flb-v1"):
    """Minimal valid artifact: identity global curve + a steep, distinct
    sports|6h curve so live evaluation is observable and drift-free."""
    return {
        "version": 1, "model_version": model_version,
        "shadow": model_version.startswith("shadow-"),
        "trained_at": NOW, "train_window": [NOW - 10 * 86400, NOW],
        "n_train": 100, "n_test": 30, "k": 2000.0,
        "horizons": ["1h", "6h", "24h", "72h", "168h"],
        "curves": {"*|*": [[0.0, 0.0], [1.0, 1.0]],
                   "sports|6h": [[0.0, 0.1], [1.0, 0.9]]},
        "fallback_map": {"sports|6h": "sports|6h"},
        "mz": {"psi": 0.0, "alpha": 0.0, "n": 130},
        "metrics": {"gate_pass": True},
    }


def _seed_kalshi_markets(root):
    store = HotStore(root / "db" / "hot.sqlite")
    base = {"venue_event_id": "KXE-1", "venue_series_id": "KXE",
            "status_norm": "open", "open_ts": NOW - 90000, "close_ts": NOW + 8 * 3600,
            "volume_24h": "10.00"}  # 8h out → floors into the sports|6h bucket
    store.upsert_series("kalshi", [
        {"venue_series_id": "KXE", "title": "E", "category": "Sports"}], NOW)
    store.upsert_markets("kalshi", [
        {**base, "venue_market_id": "KXA-1", "title": "A?",  # mid 0.43, 6h bucket
         "yes_bid_prob": "0.4200", "yes_ask_prob": "0.4400"},
        {**base, "venue_market_id": "KXC-3", "title": "C?",  # mid 0.11, bigger edge
         "yes_bid_prob": "0.1000", "yes_ask_prob": "0.1200"},
        {**base, "venue_market_id": "KXD-4", "title": "D?",  # 168h bucket: untrained
         "close_ts": NOW + 200 * 3600,
         "yes_bid_prob": "0.5000", "yes_ask_prob": "0.5200"},
    ], NOW)
    store.close()


def test_screener_serves_live_model_forecasts(tmp_path):
    """model_p / edge are computed LIVE from the CURRENT price via the loaded
    artifact — never a frozen forecast (a stale model_p vs a drifted price is
    fake edge). sports|6h curve [[0,0.1],[1,0.9]] at mid 0.43 → 0.444; edge =
    model_p − live base; untrained-horizon market → null."""
    from decimal import Decimal
    from pmkt.model.calibrate import write_artifact

    root = tmp_path / "fdata"
    _seed_kalshi_markets(root)
    write_artifact(root, _mk_artifact())

    app = create_app(Config(data_dir=root), ui_dist=root / "no-dist")
    with TestClient(app) as client:
        rows = client.get("/api/screener", params={"venue": "kalshi"}).json()["rows"]
        by_id = {r["market_id"]: r for r in rows}
        a = by_id["KXA-1"]
        # live: 0.1 + 0.43*0.8 = 0.444, decimal string, edge vs the SAME live base
        assert a["model_p"] == "0.4440" and isinstance(a["model_p"], str)
        assert Decimal(a["edge"]) == Decimal("0.4440") - Decimal(a["mid"])
        # untrained 168h horizon → out of support → null (honesty over coverage)
        assert by_id["KXD-4"]["model_p"] is None and by_id["KXD-4"]["edge"] is None

        # edge sort: biggest |model_p − base| first, null edge always last
        for direction in ("desc", "asc"):
            srows = client.get(
                "/api/screener",
                params={"venue": "kalshi", "sort": "edge", "dir": direction},
            ).json()["rows"]
            assert srows[-1]["market_id"] == "KXD-4"
        desc = client.get(
            "/api/screener", params={"venue": "kalshi", "sort": "edge", "dir": "desc"},
        ).json()["rows"]
        assert desc[0]["market_id"] == "KXC-3"  # |0.188-0.11| > |0.444-0.43|

        # detail drawer carries the same live model_p
        detail = client.get("/api/market/kalshi/KXA-1").json()
        assert detail["row"]["model_p"] == "0.4440"


def test_screener_hides_shadow_model(tmp_path):
    """A shadow-tagged CURRENT artifact yields no model_p/edge anywhere."""
    root = tmp_path / "sdata"
    _seed_kalshi_markets(root)
    from pmkt.model.calibrate import write_artifact

    write_artifact(root, _mk_artifact(model_version="shadow-kalshi-flb-v9"))
    app = create_app(Config(data_dir=root), ui_dist=root / "no-dist")
    with TestClient(app) as client:
        rows = client.get("/api/screener", params={"venue": "kalshi"}).json()["rows"]
        assert rows and all(r["model_p"] is None and r["edge"] is None for r in rows)


def test_screener_no_model_serves_null_edge(tmp_path):
    """No artifact loaded → model_p/edge null, screener still works."""
    root = tmp_path / "ndata"
    _seed_kalshi_markets(root)
    app = create_app(Config(data_dir=root), ui_dist=root / "no-dist")
    with TestClient(app) as client:
        rows = client.get("/api/screener", params={"venue": "kalshi"}).json()["rows"]
        assert rows and all(r["model_p"] is None for r in rows)


# ---- /api/decisions (shadow ledger, M4) ------------------------------------


def test_decisions_endpoint_empty(client):
    r = client.get("/api/decisions")
    assert r.status_code == 200
    body = r.json()
    assert body["rows"] == []
    assert body["summary"]["decisions"] == 0
    assert body["summary"]["graded"] == 0
    assert body["summary"]["win_rate"] is None


def test_decisions_endpoint_rows_and_summary(client, data_dir):
    import json as _json

    from pmkt.state import HotStore

    store = HotStore(data_dir / "db" / "hot.sqlite")
    try:
        store.insert_decisions([{
            "venue_id": "polymarket", "venue_market_id": "0xd1",
            "venue_token_id": "tokD", "ts": 1_784_000_000, "action": "PASS",
            "theta": "inf", "ev": "-0.0180", "model_p": "0.5660",
            "base_prob": "0.5100", "bid_prob": "0.5000", "ask_prob": "0.5200",
            "bid_size": "100", "ask_size": "100", "spread_prob": "0.0200",
            "fee_rate": "0.05", "fee_prob": "0.0125",
            "skilled_ofi": "0.8000", "n_skilled_trades": 5,
            "skilled_volume": "100.00", "ofi_age_s": 60, "alpha": "0.07",
            "category": "Sports", "close_ts": 1_784_100_000,
            "gates_json": _json.dumps({"spread_ok": True}),
            "passed_gates": 1, "decide_version": 1,
        }], 1_784_000_000)
        (did,) = store.conn.execute(
            "SELECT decision_id FROM decisions"
        ).fetchone()
        store.insert_decision_grades([{
            "decision_id": did, "side": "BUY_YES", "won": 1,
            "entry_prob": "0.5200", "ret_per_dollar": "0.8721",
            "ev_gap": "0.4340",
        }], 1_784_100_000)
    finally:
        store.close()

    body = client.get("/api/decisions").json()
    assert body["summary"] == {
        "decisions": 1, "passed_gates": 1, "graded": 1,
        "last_pass_ts": 1_784_000_000, "win_rate": 1.0,
        "mean_ret_per_dollar": 0.8721,
        "equity": [{"ts": 1_784_000_000, "cum_ret": 0.8721}],
    }
    (row,) = body["rows"]
    assert row["side"] == "BUY_YES" and row["action"] == "PASS"
    assert row["graded"]["won"] is True
    assert row["gates"] == {"spread_ok": True}
