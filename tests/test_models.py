from decimal import Decimal

from pmkt.kalshi.models import Candlestick, Market, Trade, to_unix_ts

# Shapes per docs.kalshi.com post 2026-03-12 fixed-point migration:
# prices are dollar strings, counts are *_fp strings.

MARKET_PAYLOAD = {
    "ticker": "KXHIGHNY-26JUL10-B55",
    "event_ticker": "KXHIGHNY-26JUL10",
    "market_type": "binary",
    "title": "High temp in NYC on Jul 10: 55-56°F?",
    "status": "open",
    "open_time": "2026-07-09T14:00:00Z",
    "close_time": "2026-07-10T23:00:00Z",
    "yes_bid_dollars": "0.4200",
    "yes_ask_dollars": "0.4400",
    "no_bid_dollars": "0.5600",
    "no_ask_dollars": "0.5800",
    "last_price_dollars": "0.4300",
    "volume_fp": "1523.00",
    "volume_24h_fp": "410.00",
    "open_interest_fp": "988.00",
    "liquidity_dollars": "12345.6789",
    "some_future_field": {"nested": True},
}


def test_market_parses_dollar_strings_to_decimal():
    m = Market.model_validate(MARKET_PAYLOAD)
    assert m.yes_bid_dollars == Decimal("0.4200")
    assert m.yes_ask_dollars == Decimal("0.4400")
    assert isinstance(m.volume_fp, Decimal)
    # Never float: 0.4200 must round-trip exactly.
    assert str(m.yes_bid_dollars) == "0.4200"


def test_market_hot_row_projection():
    row = Market.model_validate(MARKET_PAYLOAD).hot_row()
    assert row["venue_market_id"] == "KXHIGHNY-26JUL10-B55"
    assert row["venue_series_id"] == "KXHIGHNY"  # ticker-prefix fallback
    assert row["status_norm"] == "open"
    assert row["open_ts"] == to_unix_ts("2026-07-09T14:00:00Z")
    assert row["yes_bid_prob"] == Decimal("0.4200")


def test_status_normalization():
    # Live-verified 2026-07-10: /markets?status=open returns objects whose own
    # status field says "active".
    cases = {"active": "open", "initialized": "unopened", "determined": "closed",
             "finalized": "settled", "settled": "settled", "weird_new": "weird_new"}
    for raw, norm in cases.items():
        m = Market.model_validate({"ticker": "T", "status": raw})
        assert m.hot_row()["status_norm"] == norm, raw


def test_market_tolerates_missing_prices():
    m = Market.model_validate({"ticker": "T-1", "status": "unopened"})
    assert m.yes_bid_dollars is None
    assert m.hot_row()["yes_bid_prob"] is None


def test_trade_parsing():
    t = Trade.model_validate(
        {
            "trade_id": "abc-123",
            "ticker": "KXHIGHNY-26JUL10-B55",
            "count_fp": "10.00",
            "yes_price_dollars": "0.4300",
            "no_price_dollars": "0.5700",
            "taker_outcome_side": "yes",
            "taker_book_side": "ask",
            "created_time": "2026-07-10T15:04:05Z",
        }
    )
    assert t.count_fp == Decimal("10.00")
    assert t.yes_price_dollars + t.no_price_dollars == Decimal("1.0000")
    assert t.created_ts == to_unix_ts("2026-07-10T15:04:05Z")


def test_candlestick_ohlc_key_tolerance():
    # Accept both `close` and `close_dollars` inside OHLC sub-objects.
    for close_key in ("close", "close_dollars"):
        c = Candlestick.model_validate(
            {
                "end_period_ts": 1783100400,
                "yes_bid": {close_key: "0.4200", "open" : "0.4100"},
                "yes_ask": {close_key: "0.4400"},
                "price": None,  # nullable when no trades in period
                "volume_fp": "0.00",
                "open_interest_fp": "988.00",
            }
        )
        assert c.yes_bid_close == Decimal("0.4200")
        assert c.trade_close is None
        assert c.open_interest_fp == Decimal("988.00")


def test_subpenny_precision_survives():
    # Subpenny ticks ($0.001) and 4-6 decimal strings must not lose precision.
    m = Market.model_validate({"ticker": "T", "yes_bid_dollars": "0.041500"})
    assert m.yes_bid_dollars == Decimal("0.041500")
    assert format(m.yes_bid_dollars, "f") == "0.041500"
