from decimal import Decimal

import httpx
import pytest
import respx

from pmkt.polymarket.client import (
    GAMMA_BASE_URL,
    CLOB_BASE_URL,
    EnumerationTruncated,
    PolymarketAPIError,
    PolymarketClient,
    _parse_batch_prices,
)
from pmkt.ratelimit import TokenBucket
from pmkt.rawlake import RawLake, read_raw_file


def make_client(tmp_path):
    lake = RawLake(tmp_path / "raw")
    client = PolymarketClient(lake, TokenBucket(1000, 1000))
    return lake, client


def event(i, **over):
    return {"id": str(i), "slug": f"ev-{i}", "markets": [], **over}


@respx.mock
async def test_keyset_pagination_follows_after_cursor_and_stops(tmp_path):
    route = respx.get(f"{GAMMA_BASE_URL}/events/keyset").mock(
        side_effect=[
            httpx.Response(200, json={"events": [event(1)], "next_cursor": "c1"}),
            httpx.Response(200, json={"events": [event(2)], "next_cursor": "c2"}),
            httpx.Response(200, json={"events": [event(3)]}),  # absent = last page
        ]
    )
    lake, client = make_client(tmp_path)
    ids = []
    async with client:
        async for events, _cursor in client.iter_events_keyset():
            ids += [e.id for e in events]
    lake.close()

    assert ids == ["1", "2", "3"]
    assert route.call_count == 3
    p1 = dict(route.calls[0].request.url.params)
    # closed must be passed EXPLICITLY — the server default silently drops
    # resolved markets (data invariants); the live sweep wants closed=false.
    assert p1["active"] == "true" and p1["closed"] == "false"
    assert p1["limit"] == "500"
    assert "after_cursor" not in p1 and "cursor" not in p1
    p2 = dict(route.calls[1].request.url.params)
    assert p2["after_cursor"] == "c1"  # the EXACT param name; cursor= is the trap
    assert "cursor" not in p2
    assert dict(route.calls[2].request.url.params)["after_cursor"] == "c2"


@respx.mock
async def test_keyset_repeated_page_guard_raises_on_page1_trap(tmp_path):
    # Regression guard for the cursor= trap: if the server ignores the requested cursor
    # param and serves page 1 forever, the identical-page fingerprint must
    # RAISE — a silent stop would end collect_metadata's loop normally and
    # mass-close every market the truncated sweep never reached.
    respx.get(f"{GAMMA_BASE_URL}/events/keyset").mock(
        return_value=httpx.Response(
            200, json={"events": [event(1), event(2)], "next_cursor": "always-more"}
        )
    )
    lake, client = make_client(tmp_path)
    pages = []
    with pytest.raises(EnumerationTruncated):
        async with client:
            async for events, _cursor in client.iter_events_keyset():
                pages.append([e.id for e in events])
    lake.close()
    assert pages == [["1", "2"]], "the repeated page must not be yielded twice"


@respx.mock
async def test_keyset_stuck_cursor_guard(tmp_path):
    # next_cursor that never advances (== the requested cursor) must stop too,
    # even if page contents were to differ.
    respx.get(f"{GAMMA_BASE_URL}/events/keyset").mock(
        side_effect=[
            httpx.Response(200, json={"events": [event(1)], "next_cursor": "cX"}),
            httpx.Response(200, json={"events": [event(2)], "next_cursor": "cX"}),
        ]
    )
    lake, client = make_client(tmp_path)
    ids = []
    with pytest.raises(EnumerationTruncated):
        async with client:
            async for events, _cursor in client.iter_events_keyset():
                ids += [e.id for e in events]
    lake.close()
    assert ids == ["1", "2"]


@respx.mock
async def test_keyset_consecutive_empty_pages_cap(tmp_path):
    # Empty page + live cursor mid-stream is NOT termination (a hiccup must
    # not truncate enumeration and cascade into mass false closes), but
    # forever-empty pages must not loop forever either.
    route = respx.get(f"{GAMMA_BASE_URL}/events/keyset").mock(
        side_effect=[
            httpx.Response(200, json={"events": [event(1)], "next_cursor": "c1"}),
            httpx.Response(200, json={"events": [], "next_cursor": "c2"}),
            httpx.Response(200, json={"events": [event(2)], "next_cursor": "c3"}),
        ]
        + [
            httpx.Response(200, json={"events": [], "next_cursor": f"e{i}"})
            for i in range(10)
        ]
    )
    lake, client = make_client(tmp_path)
    ids = []
    with pytest.raises(EnumerationTruncated):
        async with client:
            async for events, _cursor in client.iter_events_keyset():
                ids += [e.id for e in events]
    lake.close()
    assert ids == ["1", "2"], "mid-stream empty page must not truncate"
    assert route.call_count == 6  # 3 real pages + 3 consecutive empties


@respx.mock
async def test_keyset_max_pages_cap(tmp_path):
    route = respx.get(f"{GAMMA_BASE_URL}/events/keyset").mock(
        side_effect=[
            httpx.Response(200, json={"events": [event(i)], "next_cursor": f"c{i}"})
            for i in range(10)
        ]
    )
    lake, client = make_client(tmp_path)
    n = 0
    async with client:
        async for events, _cursor in client.iter_events_keyset(max_pages=2):
            n += len(events)
    lake.close()
    assert n == 2
    assert route.call_count == 2


@respx.mock
async def test_every_response_lands_in_raw_lake_including_errors(tmp_path):
    respx.get(f"{GAMMA_BASE_URL}/events/keyset").mock(
        side_effect=[
            httpx.Response(429, json={"error": "throttled"}, headers={"Retry-After": "0"}),
            httpx.Response(200, json={"events": [event(1)]}),
        ]
    )
    lake, client = make_client(tmp_path)
    async with client:
        async for _events, _cursor in client.iter_events_keyset():
            pass
    lake.close()

    records = []
    for f in sorted((tmp_path / "raw").rglob("*.jsonl.zst")):
        records += read_raw_file(f)
    assert sorted(r["http_status"] for r in records) == [200, 429]
    assert all(r["venue"] == "polymarket" for r in records)
    assert all(r["endpoint"] == "gamma_events" for r in records)


@respx.mock
async def test_batch_prices_history_request_and_parse(tmp_path):
    token = "1101718727435143943097144182299058368052"
    respx.post(f"{CLOB_BASE_URL}/batch-prices-history").mock(
        return_value=httpx.Response(
            200,
            json={"history": {token: [
                {"t": 1783801085, "p": 0.08},
                {"t": 1783801024, "p": 0.08125},  # out of order on purpose
            ]}},
        )
    )
    lake, client = make_client(tmp_path)
    async with client:
        history = await client.get_batch_prices_history(
            [token], start_ts=1783800000, end_ts=1783803600, fidelity=1
        )
    lake.close()

    points = history[token]
    assert [pt.t for pt in points] == [1783801024, 1783801085], "sorted by t"
    assert points[0].p == Decimal("0.08125")  # float -> exact Decimal

    records = []
    for f in (tmp_path / "raw").rglob("*.jsonl.zst"):
        records += read_raw_file(f)
    (rec,) = records
    assert rec["endpoint"] == "clob_prices_history"
    # the POST body is captured alongside the response (raw = replayable)
    assert rec["params"]["_body"]["markets"] == [token]
    assert rec["params"]["_body"]["start_ts"] == 1783800000


async def test_batch_prices_history_validates_chunk_size(tmp_path):
    lake, client = make_client(tmp_path)
    async with client:
        with pytest.raises(ValueError):
            await client.get_batch_prices_history(["t"] * 21, start_ts=1, end_ts=2)
        assert await client.get_batch_prices_history([], start_ts=1, end_ts=2) == {}
    lake.close()


@respx.mock
async def test_non_retryable_error_raises_after_capture(tmp_path):
    respx.post(f"{CLOB_BASE_URL}/batch-prices-history").mock(
        return_value=httpx.Response(400, json={"error": "invalid filters"})
    )
    lake, client = make_client(tmp_path)
    with pytest.raises(PolymarketAPIError) as exc:
        async with client:
            await client.get_batch_prices_history(["t1"], start_ts=1, end_ts=2)
    lake.close()
    assert exc.value.status == 400
    records = []
    for f in (tmp_path / "raw").rglob("*.jsonl.zst"):
        records += read_raw_file(f)
    assert records and records[0]["http_status"] == 400


@respx.mock
async def test_get_trades_request_and_parse(tmp_path):
    from pmkt.polymarket.client import DATA_BASE_URL
    condition_id = "0xcond123abc"
    respx.get(f"{DATA_BASE_URL}/trades").mock(
        return_value=httpx.Response(200, json=[
            {"proxyWallet": "0xw1", "side": "BUY", "price": "0.42",
             "size": "10.0", "conditionId": condition_id, "asset": "tok1",
             "timestamp": 1783800000, "transactionHash": "0xtx1"},
            {"proxyWallet": "0xw2", "side": "SELL", "price": "0.58",
             "size": "5.0", "conditionId": condition_id, "asset": "tok2",
             "timestamp": 1783800100, "transactionHash": "0xtx2"},
        ])
    )
    lake, client = make_client(tmp_path)
    async with client:
        trades = await client.get_trades(condition_id, limit=1000, offset=0)
    lake.close()

    assert len(trades) == 2
    assert trades[0].proxy_wallet == "0xw1"
    assert trades[0].side == "BUY"
    assert trades[0].price == Decimal("0.42")
    assert trades[0].timestamp == 1783800000
    assert trades[1].proxy_wallet == "0xw2"

    records = []
    for f in (tmp_path / "raw").rglob("*.jsonl.zst"):
        records += read_raw_file(f)
    (rec,) = records
    assert rec["endpoint"] == "data_api_trades"
    assert rec["params"]["market"] == condition_id


@respx.mock
async def test_get_trades_empty_response(tmp_path):
    from pmkt.polymarket.client import DATA_BASE_URL
    respx.get(f"{DATA_BASE_URL}/trades").mock(
        return_value=httpx.Response(200, json=[])
    )
    lake, client = make_client(tmp_path)
    async with client:
        trades = await client.get_trades("0xcond1")
    lake.close()
    assert trades == []


def test_parse_batch_prices_shapes(caplog):
    live = {"history": {"tok": [{"t": 2, "p": 0.5}, {"t": 1, "p": 0.4}]}}
    parsed = _parse_batch_prices(live)
    assert [pt.t for pt in parsed["tok"]] == [1, 2]

    # documented alternate shape: token -> {"history": [...]}
    alt = {"tok": {"history": [{"t": 5, "p": 0.9}]}}
    assert _parse_batch_prices(alt)["tok"][0].p == Decimal("0.9")

    # recognized-but-empty is silent; junk warns and returns {}
    assert _parse_batch_prices({"history": {}}) == {}
    import logging
    with caplog.at_level(logging.WARNING):
        assert _parse_batch_prices({"unexpected": [1, 2]}) == {}
    assert "unrecognized" in caplog.text

    # one malformed point is dropped, the rest survive
    messy = {"history": {"tok": [{"t": 1, "p": 0.4}, {"bad": True}]}}
    assert len(_parse_batch_prices(messy)["tok"]) == 1
