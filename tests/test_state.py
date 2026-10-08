from decimal import Decimal

from pmkt.state import HotStore


def make_store(tmp_path):
    return HotStore(tmp_path / "db" / "hot.sqlite")


def market_row(ticker, **overrides):
    row = {
        "venue_market_id": ticker,
        "venue_event_id": "EV-1",
        "venue_series_id": "SER",
        "title": "t?",
        "market_type": "binary",
        "status_norm": "open",
        "open_ts": 1,
        "close_ts": 2,
        "yes_bid_prob": Decimal("0.4200"),
        "yes_ask_prob": Decimal("0.4400"),
        "last_prob": None,
        "volume": Decimal("10.00"),
        "volume_24h": None,
        "open_interest": None,
        "liquidity": None,
        "result": None,
    }
    row.update(overrides)
    return row


def test_market_upsert_preserves_first_seen_and_stores_decimal_text(tmp_path):
    store = make_store(tmp_path)
    store.upsert_markets("kalshi", [market_row("M-1")], now_ts=100)
    store.upsert_markets(
        "kalshi", [market_row("M-1", yes_bid_prob=Decimal("0.5000"))], now_ts=200
    )
    row = store.conn.execute("SELECT * FROM markets").fetchone()
    assert row["first_seen_ts"] == 100
    assert row["last_seen_ts"] == 200
    assert row["yes_bid_prob"] == "0.5000"  # TEXT, exact — never float
    store.close()


def test_mark_unseen_markets_closed(tmp_path):
    store = make_store(tmp_path)
    store.upsert_markets("kalshi", [market_row("M-old")], now_ts=100)
    # New sweep at ts=200 sees only M-new.
    store.upsert_markets("kalshi", [market_row("M-new")], now_ts=200)
    n = store.mark_unseen_markets_closed("kalshi", sweep_ts=200)
    assert n == 1
    assert store.open_market_tickers("kalshi") == ["M-new"]
    store.close()


def test_active_market_filter(tmp_path):
    store = make_store(tmp_path)
    store.upsert_markets(
        "kalshi",
        [
            market_row("M-dead", volume_24h=Decimal("0.00"), open_interest=Decimal("0.00")),
            market_row("M-vol", volume_24h=Decimal("12.00"), open_interest=Decimal("0.00")),
            market_row("M-oi", volume_24h=None, open_interest=Decimal("5.00")),
            market_row("M-closed", status_norm="closed", volume_24h=Decimal("99.00")),
        ],
        now_ts=100,
    )
    assert store.active_market_tickers("kalshi") == ["M-oi", "M-vol"]
    assert len(store.open_market_tickers("kalshi")) == 3
    store.close()


def test_price_update_coalesces_nulls(tmp_path):
    store = make_store(tmp_path)
    store.upsert_markets("kalshi", [market_row("M-1")], now_ts=100)
    store.update_market_prices(
        "kalshi",
        [{"venue_market_id": "M-1", "last_prob": Decimal("0.4300"),
          "yes_bid_prob": None, "yes_ask_prob": None, "volume": None,
          "open_interest": Decimal("55.00")}],
        now_ts=150,
    )
    row = store.conn.execute("SELECT * FROM markets").fetchone()
    assert row["last_prob"] == "0.4300"
    assert row["yes_bid_prob"] == "0.4200", "None must not clobber existing price"
    assert row["open_interest"] == "55.00"
    assert row["prices_updated_ts"] == 150
    store.close()


def test_collector_state_roundtrip(tmp_path):
    store = make_store(tmp_path)
    assert store.get_state("kalshi", "trades") is None
    store.set_state("kalshi", "trades", cursor="abc", watermark_ts=123, now_ts=1000)
    row = store.get_state("kalshi", "trades")
    assert row["cursor"] == "abc" and row["watermark_ts"] == 123
    # watermark_ts=None keeps the old watermark (mid-pagination saves).
    store.set_state("kalshi", "trades", cursor="def", watermark_ts=None, now_ts=1001)
    row = store.get_state("kalshi", "trades")
    assert row["cursor"] == "def" and row["watermark_ts"] == 123
    store.close()
