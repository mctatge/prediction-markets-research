from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from pmkt.kalshi.client import (
    KalshiAPIError,
    KalshiPublicClient,
    _parse_batch_candles,
    _retry_delay,
)
from pmkt.ratelimit import TokenBucket
from pmkt.rawlake import RawLake, read_raw_file

BASE = "https://external-api.kalshi.com/trade-api/v2"


def make_client(tmp_path):
    lake = RawLake(tmp_path / "raw")
    client = KalshiPublicClient(BASE, lake, TokenBucket(1000, 1000))
    return lake, client


@respx.mock
async def test_markets_pagination_follows_cursor_until_empty(tmp_path):
    route = respx.get(f"{BASE}/markets").mock(
        side_effect=[
            httpx.Response(200, json={"markets": [{"ticker": "A"}], "cursor": "next1"}),
            httpx.Response(200, json={"markets": [{"ticker": "B"}], "cursor": ""}),
        ]
    )
    lake, client = make_client(tmp_path)
    tickers = []
    async with client:
        async for markets, _cursor in client.iter_markets(status="open"):
            tickers += [m.ticker for m in markets]
    lake.close()

    assert tickers == ["A", "B"]
    assert route.call_count == 2
    first_params = dict(route.calls[0].request.url.params)
    assert first_params["limit"] == "1000"
    assert first_params["mve_filter"] == "exclude"
    assert "cursor" not in first_params
    assert dict(route.calls[1].request.url.params)["cursor"] == "next1"


@respx.mock
async def test_every_response_lands_in_raw_lake_including_errors(tmp_path):
    respx.get(f"{BASE}/series").mock(
        side_effect=[
            httpx.Response(429, json={"error": "rate_limited"}, headers={"Retry-After": "0"}),
            httpx.Response(200, json={"series": [{"ticker": "KXHIGHNY"}]}),
        ]
    )
    lake, client = make_client(tmp_path)
    async with client:
        series = await client.get_series_catalog()
    lake.close()

    assert [s.ticker for s in series] == ["KXHIGHNY"]
    records = []
    for f in sorted((tmp_path / "raw").rglob("*.jsonl.zst")):
        records += read_raw_file(f)
    statuses = sorted(r["http_status"] for r in records)
    assert statuses == [200, 429], "the 429 must be captured too"


@respx.mock
async def test_non_retryable_error_raises_after_capture(tmp_path):
    respx.get(f"{BASE}/markets/candlesticks").mock(
        return_value=httpx.Response(400, json={"error": "bad ticker"})
    )
    lake, client = make_client(tmp_path)
    with pytest.raises(KalshiAPIError) as exc:
        async with client:
            await client.get_batch_candlesticks(["X"], start_ts=1, end_ts=2)
    lake.close()
    assert exc.value.status == 400
    records = []
    for f in (tmp_path / "raw").rglob("*.jsonl.zst"):
        records += read_raw_file(f)
    assert records and records[0]["http_status"] == 400


def test_retry_delay_forms():
    # delta-seconds
    assert _retry_delay("2", 1) == 2.0
    # server value clamped, never an hour-long sleep
    assert _retry_delay("3600", 1) == 60.0
    # HTTP-date form (RFC 9110) must not raise
    future = (datetime.now(timezone.utc) + timedelta(seconds=10)).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    assert 0 < _retry_delay(future, 1) <= 60.0
    # past HTTP-date -> clamped to 0, not negative
    assert _retry_delay("Wed, 21 Oct 2020 07:28:00 GMT", 1) == 0.0
    # garbage -> exponential fallback
    assert _retry_delay("soon", 2) == 4.0
    assert _retry_delay(None, 3) == 8.0


@respx.mock
async def test_empty_page_with_live_cursor_does_not_truncate(tmp_path):
    # Contract: empty/null cursor = done. An empty PAGE mid-stream must not
    # stop enumeration (it would cascade into mass false market closes).
    respx.get(f"{BASE}/markets").mock(
        side_effect=[
            httpx.Response(200, json={"markets": [{"ticker": "A"}], "cursor": "c1"}),
            httpx.Response(200, json={"markets": [], "cursor": "c2"}),
            httpx.Response(200, json={"markets": [{"ticker": "B"}], "cursor": ""}),
        ]
    )
    lake, client = make_client(tmp_path)
    tickers = []
    async with client:
        async for markets, _ in client.iter_markets():
            tickers += [m.ticker for m in markets]
    lake.close()
    assert tickers == ["A", "B"]


@respx.mock
async def test_consecutive_empty_pages_cap_prevents_infinite_loop(tmp_path):
    route = respx.get(f"{BASE}/markets").mock(
        return_value=httpx.Response(200, json={"markets": [], "cursor": "again"})
    )
    lake, client = make_client(tmp_path)
    async with client:
        async for _markets, _ in client.iter_markets():
            pass
    lake.close()
    assert route.call_count == 3  # MAX_CONSECUTIVE_EMPTY_PAGES


def test_parse_batch_candles_live_shape_and_wrong_shape_guard(caplog):
    live = {
        "markets": [
            {"market_ticker": "T-1", "candlesticks": [
                {"end_period_ts": 1, "yes_bid": {"close_dollars": "0.4200"}}]},
        ]
    }
    parsed = _parse_batch_candles(live)
    assert parsed[0].ticker == "T-1"
    assert len(parsed[0].candlesticks) == 1

    # Wrong shape (bare candle list, no per-market objects) must warn, not
    # silently return [].
    import logging
    with caplog.at_level(logging.WARNING):
        assert _parse_batch_candles({"candlesticks": [{"end_period_ts": 1}]}) == []
    assert "unrecognized" in caplog.text

    # Legitimately empty batch: no warning needed.
    assert _parse_batch_candles({"markets": []}) == []


async def test_batch_candles_input_validation(tmp_path):
    lake, client = make_client(tmp_path)
    async with client:
        with pytest.raises(ValueError):
            await client.get_batch_candlesticks(["T"] * 101, start_ts=1, end_ts=2)
        with pytest.raises(ValueError):
            await client.get_batch_candlesticks(["T"], start_ts=1, end_ts=2, period_interval=5)
    lake.close()
