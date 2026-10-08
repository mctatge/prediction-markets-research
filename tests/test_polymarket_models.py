from decimal import Decimal

from pmkt.polymarket.models import (
    GammaEvent,
    GammaMarket,
    PricePoint,
    gamma_status_norm,
    to_unix_ts,
)

# Shapes live-verified 2026-07-11 against gamma-api.polymarket.com:
# outcomes/outcomePrices/clobTokenIds are JSON-ENCODED STRINGS (data invariants),
# prices are 0-1 floats, ids come as strings.

MARKET_PAYLOAD = {
    "id": "516950",
    "slug": "kraken-ipo-in-2025",
    "conditionId": "0x5b70123b2c37355840b38bc60752919dae7ca5fe11d5e5184aa69be01b9db458",
    "questionID": "0x90c45939ee51f61d0c19d912e982aedc20a881ac43fda288f9141b33dbfac481",
    "question": "Kraken IPO in 2025?",
    "outcomes": '["Yes", "No"]',
    "outcomePrices": '["0.415", "0.585"]',
    "clobTokenIds": '["1062296681027161498322092502223408476622", "330077650793253821034588986803836915038"]',
    "active": True,
    "closed": False,
    "archived": False,
    "acceptingOrders": True,
    "umaResolutionStatus": None,
    "negRisk": False,
    "bestBid": 0.41,
    "bestAsk": 0.42,
    "lastTradePrice": 0.415,
    "volumeNum": 494516.53254,
    "volume24hr": 82.27435700000001,
    "liquidityNum": 9401.9763,
    "startDate": "2024-12-31T19:05:03.54Z",
    "endDate": "2025-12-31T12:00:00Z",
    "some_future_field": {"nested": True},
}


def test_json_string_fields_are_decoded():
    m = GammaMarket.model_validate(MARKET_PAYLOAD)
    assert m.outcome_names == ["Yes", "No"]
    assert m.token_ids == [
        "1062296681027161498322092502223408476622",
        "330077650793253821034588986803836915038",
    ]
    assert m.outcome_probs == [Decimal("0.415"), Decimal("0.585")]


def test_json_fields_also_accept_real_arrays_and_null():
    m = GammaMarket.model_validate(
        {**MARKET_PAYLOAD, "outcomes": ["Up", "Down"], "outcomePrices": None,
         "clobTokenIds": ["111", "222"]}
    )
    assert m.outcome_names == ["Up", "Down"]
    assert m.outcome_probs == []
    assert m.token_ids == ["111", "222"]
    # Garbage string degrades to empty, never raises (one bad market must
    # not kill a sweep).
    bad = GammaMarket.model_validate({**MARKET_PAYLOAD, "clobTokenIds": "not json"})
    assert bad.token_ids == []


def test_float_prices_become_exact_decimals():
    m = GammaMarket.model_validate(MARKET_PAYLOAD)
    # Decimal(str(x)), never Decimal(float): 0.41 must be exactly "0.41".
    assert m.best_bid == Decimal("0.41")
    assert format(m.best_bid, "f") == "0.41"
    assert isinstance(m.volume_24hr, Decimal)
    row = m.hot_row()
    assert row["yes_bid_prob"] == Decimal("0.41")
    assert row["last_prob"] == Decimal("0.415")


def test_status_norm_mapping():
    # (active, closed, acceptingOrders, umaResolutionStatus) -> status_norm
    cases = [
        ((True, False, True, None), "open"),
        ((True, False, False, None), "paused"),
        ((False, False, None, None), "unopened"),
        ((True, True, False, "proposed"), "closed"),
        ((True, True, False, "resolved"), "settled"),
        ((True, True, False, None), "closed"),
    ]
    for args, expected in cases:
        assert gamma_status_norm(*args) == expected, args


def test_hot_row_id_zoo():
    row = GammaMarket.model_validate(MARKET_PAYLOAD).hot_row()
    # venue_market_id = conditionId (the CLOB/Data/on-chain join key);
    # the Gamma numeric id is only the secondary id (data invariants).
    assert row["venue_market_id"].startswith("0x5b70123b")
    assert row["venue_secondary_id"] == "516950"
    assert row["slug"] == "kraken-ipo-in-2025"
    assert row["status_norm"] == "open"
    assert row["open_ts"] == to_unix_ts("2024-12-31T19:05:03.54Z")
    assert row["neg_risk"] == 0


def test_instrument_rows_primary_is_outcome_zero():
    rows = GammaMarket.model_validate(MARKET_PAYLOAD).instrument_rows()
    assert len(rows) == 2
    assert rows[0]["outcome_index"] == 0
    assert rows[0]["outcome_name"] == "Yes"
    assert rows[0]["is_primary"] is True
    assert rows[1]["is_primary"] is False
    assert all(r["venue_market_id"].startswith("0x") for r in rows)


EVENT_PAYLOAD = {
    "id": "692720",
    "slug": "doge-updown-5m-1783890300",
    "title": "Dogecoin Up or Down - July 12, 5:05PM-5:10PM ET",
    "active": True,
    "closed": False,
    "negRisk": False,
    "seriesSlug": "doge-up-or-down-5m",
    # series[].recurrence live-observed as "daily" on a 5-MINUTE series —
    # the tag/slug rules must win over it.
    "series": [{"slug": "doge-up-or-down-5m", "recurrence": "daily"}],
    "tags": [
        {"id": "1", "label": "Up or Down", "slug": "up-or-down"},
        {"id": "2", "label": "Recurring", "slug": "recurring"},
        {"id": "3", "label": "Crypto", "slug": "crypto"},
        {"id": "4", "label": "5M", "slug": "5M"},
    ],
    "markets": [MARKET_PAYLOAD],
}


def test_event_recurrence_class_from_cadence_tag():
    assert GammaEvent.model_validate(EVENT_PAYLOAD).recurrence_class() == "5m"


def test_event_recurrence_class_from_series_slug_suffix():
    payload = {
        **EVENT_PAYLOAD,
        "tags": [{"label": "Recurring", "slug": "recurring"}],
        "series": [],
    }
    assert GammaEvent.model_validate(payload).recurrence_class() == "5m"  # -5m suffix
    hourly = {**payload, "seriesSlug": "btc-up-or-down-hourly"}
    assert GammaEvent.model_validate(hourly).recurrence_class() == "hourly"


def test_event_recurrence_none_for_one_offs_and_bare_series():
    one_off = {**EVENT_PAYLOAD, "tags": [{"label": "Crypto", "slug": "crypto"}],
               "series": None, "seriesSlug": None}
    assert GammaEvent.model_validate(one_off).recurrence_class() is None
    # The "recurring" tag GATES classification: sports/one-off events sit in
    # series whose recurrence field reads "daily" (measured: ~3k events would
    # be mislabeled) — without the tag they must stay None.
    sports = {**one_off, "seriesSlug": "fifa-world-cup",
              "series": [{"slug": "fifa-world-cup", "recurrence": "daily"}]}
    assert GammaEvent.model_validate(sports).recurrence_class() is None


def test_event_recurrence_falls_back_to_series_recurrence():
    payload = {**EVENT_PAYLOAD, "tags": [{"label": "Recurring", "slug": "recurring"}],
               "seriesSlug": "some-series", "series": [{"slug": "some-series", "recurrence": "daily"}]}
    assert GammaEvent.model_validate(payload).recurrence_class() == "daily"


def test_event_hot_row():
    row = GammaEvent.model_validate(EVENT_PAYLOAD).hot_row()
    assert row["venue_event_id"] == "692720"
    assert row["venue_series_id"] == "doge-up-or-down-5m"
    assert row["mutually_exclusive"] == 0
    assert row["category"] == "Up or Down"
    neg = GammaEvent.model_validate({**EVENT_PAYLOAD, "negRisk": True}).hot_row()
    assert neg["mutually_exclusive"] == 1


def test_price_point_decimal_exactness():
    pt = PricePoint.model_validate({"t": 1783801024, "p": 0.08125})
    assert pt.p == Decimal("0.08125")
    assert format(pt.p, "f") == "0.08125"


def test_timestamp_forms():
    # live doge-updown event: slug epoch 1783890300 == 2026-07-12T21:05:00Z
    assert to_unix_ts("2026-07-12T21:05:00Z") == 1783890300
    # Gamma's spaced closedTime form with a bare +00 offset
    assert to_unix_ts("2026-01-01 07:30:39+00") is not None
    assert to_unix_ts(None) is None
    assert to_unix_ts("not a date") is None
