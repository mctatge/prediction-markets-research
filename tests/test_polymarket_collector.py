"""Per-token price-coverage semantics — the price-history coverage rules.

Review findings 2026-07-11 (covered by regression tests): a global prices
watermark silently and permanently lost minute history when (a) a chunk
failed but the watermark advanced anyway, or (b) a token newly entered the
universe and started at the global watermark instead of a deep window.
Coverage now rides on instruments.price_covered_ts per token.
"""

import time

import pytest

from pmkt.polymarket import collector
from pmkt.polymarket.client import PolymarketAPIError
from pmkt.polymarket.models import PricePoint, VENUE_ID
from pmkt.state import HotStore

NOW = 1_783_800_000


class FakePricesClient:
    """Records requested windows per token set; poisons selected tokens."""

    def __init__(self, poisoned: set[str] | None = None):
        self.calls: list[tuple[tuple[str, ...], int, int]] = []
        self.poisoned = poisoned or set()

    async def get_batch_prices_history(self, token_ids, *, start_ts, end_ts, fidelity):
        self.calls.append((tuple(token_ids), start_ts, end_ts))
        if self.poisoned & set(token_ids):
            raise PolymarketAPIError(400, "clob/batch", "poisoned token")
        return {
            t: [PricePoint(t=end_ts - 1, p="0.42")] for t in token_ids
        }


def seed(store, tokens):
    store.upsert_markets(
        VENUE_ID,
        [
            {
                "venue_market_id": f"0xc{i}",
                "status_norm": "open",
                "volume_24h": "5.0",
            }
            for i, _t in enumerate(tokens)
        ],
        now_ts=NOW - 500,
    )
    store.upsert_instruments(
        VENUE_ID,
        [
            {
                "venue_market_id": f"0xc{i}",
                "outcome_index": 0,
                "outcome_name": "Yes",
                "venue_token_id": t,
                "is_primary": 1,
            }
            for i, t in enumerate(tokens)
        ],
        now_ts=NOW - 500,
    )


def covered(store, token):
    row = store.conn.execute(
        "SELECT price_covered_ts FROM instruments WHERE venue_token_id=?", (token,)
    ).fetchone()
    return row[0]


async def test_new_tokens_get_deep_first_window_and_coverage_advances(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    seed(store, ["tokA", "tokB"])
    client = FakePricesClient()

    result = await collector.collect_prices(client, store)

    # Never-covered tokens reach back the full 24h clamp, not a global watermark.
    first_window_start = client.calls[0][1]
    assert first_window_start == NOW - collector.PRICE_MAX_LOOKBACK_S
    assert result["failed_chunks"] == 0
    assert covered(store, "tokA") == NOW and covered(store, "tokB") == NOW

    # Steady state: next sweep starts from per-token coverage minus overlap.
    client2 = FakePricesClient()
    monkeypatch.setattr(time, "time", lambda: NOW + 900)
    await collector.collect_prices(client2, store)
    assert client2.calls[0][1] == NOW - collector.PRICE_OVERLAP_S


async def test_failed_chunk_keeps_coverage_and_poison_is_isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    # 3 tokens in one chunk of 20; tokBAD poisons the full-chunk call, the
    # half-split retry must still land coverage for the healthy half.
    seed(store, ["tokAAA", "tokBAD", "tokZZZ"])
    client = FakePricesClient(poisoned={"tokBAD"})

    result = await collector.collect_prices(client, store)

    assert result["failed_chunks"] >= 1
    # The half without the poison advanced; the poisoned half did not.
    assert covered(store, "tokAAA") == NOW  # first half [tokAAA] clean
    assert covered(store, "tokBAD") is None
    assert covered(store, "tokZZZ") is None or covered(store, "tokZZZ") == NOW

    # Next sweep: unpoisoned client — the previously failed tokens refetch
    # from their old (deep) window, nothing was skipped past.
    client2 = FakePricesClient()
    monkeypatch.setattr(time, "time", lambda: NOW + 900)
    await collector.collect_prices(client2, store)
    assert covered(store, "tokBAD") == NOW + 900
    bad_call = next(c for c in client2.calls if "tokBAD" in c[0])
    assert bad_call[1] == (NOW + 900) - collector.PRICE_MAX_LOOKBACK_S


async def test_empty_universe_is_a_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    client = FakePricesClient()
    result = await collector.collect_prices(client, store)
    assert result == {"tokens": 0, "points": 0, "updated": 0}
    assert client.calls == []


# ---------------------------------------------------------------------------
# Trades sweep (Data-API /trades — wallet-attributed)
# ---------------------------------------------------------------------------


class FakeTradesClient:
    """Records get_trades calls; returns canned trades or raises."""

    def __init__(self, responses: dict[str, list[list]] | None = None,
                 errors: set[str] | None = None):
        self.calls: list[tuple[str, int, int]] = []
        self.responses = responses or {}
        self.errors = errors or set()

    async def get_trades(self, condition_id, *, limit=1000, offset=0):
        self.calls.append((condition_id, limit, offset))
        if condition_id in self.errors:
            from pmkt.polymarket.client import PolymarketAPIError
            raise PolymarketAPIError(500, "/trades", "test error")
        from pmkt.polymarket.models import DataAPITrade
        pages = self.responses.get(condition_id, [])
        page_idx = offset // limit
        if page_idx >= len(pages):
            return []
        return [DataAPITrade.model_validate(t) for t in pages[page_idx]]


def _pm_trade(ts, *, wallet="0xabc", side="BUY", price="0.50", size="10.0",
              cid="0xcond1", asset="tok1", tx_hash="0xtx1"):
    return {
        "proxyWallet": wallet, "side": side, "price": price, "size": size,
        "conditionId": cid, "asset": asset, "timestamp": ts,
        "transactionHash": tx_hash,
    }


def seed_pm_markets(store, condition_ids, volume_24h="100.0"):
    store.upsert_markets(
        VENUE_ID,
        [
            {
                "venue_market_id": cid,
                "status_norm": "open",
                "volume_24h": volume_24h,
            }
            for cid in condition_ids
        ],
        now_ts=NOW - 500,
    )


async def test_trades_initializes_watermark_on_first_run(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    client = FakeTradesClient()
    result = await collector.collect_trades(client, store)
    assert result.get("initialized") is True
    assert client.calls == []
    state = store.get_state(VENUE_ID, collector.STATE_TRADES)
    assert state["watermark_ts"] == NOW


async def test_trades_empty_universe_is_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    store.set_state(VENUE_ID, collector.STATE_TRADES, watermark_ts=NOW - 900, now_ts=NOW - 900)
    client = FakeTradesClient()
    result = await collector.collect_trades(client, store)
    assert result == {"markets": 0, "trades": 0, "pages": 0}
    assert client.calls == []


async def test_trades_fetches_per_market_and_counts_new(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    watermark = NOW - 900
    store.set_state(VENUE_ID, collector.STATE_TRADES, watermark_ts=watermark, now_ts=watermark)
    seed_pm_markets(store, ["0xc1", "0xc2"])
    client = FakeTradesClient(responses={
        "0xc1": [[_pm_trade(NOW - 100, cid="0xc1"), _pm_trade(NOW - 200, cid="0xc1"),
                   _pm_trade(watermark - 50, cid="0xc1")]],  # 2 new, 1 old
        "0xc2": [[_pm_trade(NOW - 50, cid="0xc2")]],          # 1 new
    })
    result = await collector.collect_trades(client, store)
    assert result["markets"] == 2
    assert result["trades"] == 3
    assert result["pages"] == 2
    assert len(client.calls) == 2
    state = store.get_state(VENUE_ID, collector.STATE_TRADES)
    assert state["watermark_ts"] == NOW


async def test_trades_paginates_when_all_new(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    watermark = NOW - 3600
    store.set_state(VENUE_ID, collector.STATE_TRADES, watermark_ts=watermark, now_ts=watermark)
    seed_pm_markets(store, ["0xc1"])
    page1 = [_pm_trade(NOW - i, cid="0xc1", tx_hash=f"0xtx{i}") for i in range(1000)]
    page2 = [_pm_trade(NOW - 1500, cid="0xc1"), _pm_trade(watermark - 10, cid="0xc1")]
    client = FakeTradesClient(responses={"0xc1": [page1, page2]})
    result = await collector.collect_trades(client, store)
    assert result["pages"] == 2
    assert result["trades"] == 1001  # all 1000 from page1 + 1 new from page2
    assert client.calls[1] == ("0xc1", collector.TRADES_LIMIT, 1000)


async def test_trades_skips_errored_market(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    watermark = NOW - 900
    store.set_state(VENUE_ID, collector.STATE_TRADES, watermark_ts=watermark, now_ts=watermark)
    seed_pm_markets(store, ["0xbad", "0xgood"])
    client = FakeTradesClient(
        responses={"0xgood": [[_pm_trade(NOW - 50, cid="0xgood")]]},
        errors={"0xbad"},
    )
    result = await collector.collect_trades(client, store)
    assert result["markets"] == 2
    assert result["trades"] == 1


async def test_trades_respects_page_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(collector, "TRADES_PAGE_BUDGET", 2)
    store = HotStore(tmp_path / "hot.sqlite")
    watermark = NOW - 900
    store.set_state(VENUE_ID, collector.STATE_TRADES, watermark_ts=watermark, now_ts=watermark)
    seed_pm_markets(store, ["0xc1", "0xc2", "0xc3"])
    client = FakeTradesClient(responses={
        "0xc1": [[_pm_trade(NOW - 10, cid="0xc1")]],
        "0xc2": [[_pm_trade(NOW - 20, cid="0xc2")]],
        "0xc3": [[_pm_trade(NOW - 30, cid="0xc3")]],
    })
    result = await collector.collect_trades(client, store)
    assert result["markets"] == 2  # budget=2 pages, 1 page each → 2 markets
    assert result["pages"] == 2
    assert len(client.calls) == 2  # 3rd market not reached


# --------------------------------------------------------------------------
# Recently-closed recrawl — Gamma endDate-window semantics (live-probed
# 2026-07-31). Every assertion below encodes a way the previous single
# min-only walk silently captured the WRONG markets rather than failing:
# it paged endDate-2028 rows and never reached the recent closures the
# decision grader was waiting on.
# --------------------------------------------------------------------------


class FakeClosedClient:
    """Records the endDate windows requested; serves `per_slice` markets."""

    def __init__(self, per_slice: int = 0, by_cid: dict | None = None):
        self.windows: list[tuple[str, str | None, int]] = []
        self.cid_calls: list[list[str]] = []
        self.per_slice = per_slice
        self.by_cid = by_cid or {}

    async def get_recently_closed_markets(
        self, end_date_min, *, end_date_max=None, limit=100, offset=0
    ):
        self.windows.append((end_date_min, end_date_max, offset))
        remaining = max(0, self.per_slice - offset)
        return [{"endDate": end_date_max}] * min(limit, remaining)

    async def get_markets_by_condition_ids(self, condition_ids):
        self.cid_calls.append(list(condition_ids))
        return [self.by_cid[c] for c in condition_ids if c in self.by_cid]


def _seed_closed_decision(store, venue_id, market_id, *, now):
    store.upsert_markets(
        venue_id,
        [{"venue_market_id": market_id, "status_norm": "closed",
          "close_ts": now - 3600}],
        now_ts=now - 3600,
    )
    store.insert_decisions([{
        "venue_id": venue_id, "venue_market_id": market_id, "ts": now - 3600,
        "action": "PASS", "theta": "inf", "gates_json": "{}",
        "passed_gates": 1, "decide_version": 1,
    }], now_ts=now - 3600)


async def test_closed_recrawl_always_bounds_the_window(tmp_path, monkeypatch):
    """end_date_max on EVERY request, full timestamps, and lo < hi.

    A bare date parses as T00:00:00Z (so a date-only max keeps only markets
    ending at exactly midnight), and min == max is a 422 'invalid time
    range'. Both are silent-wrong-data bugs, not loud ones."""
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    client = FakeClosedClient()
    await collector.capture_closed_markets(client, store)

    assert client.windows, "recrawl made no requests"
    for lo, hi, _ in client.windows:
        assert hi is not None, "unbounded upper bound → pages endDate-2028 rows"
        assert lo.endswith("Z") and hi.endswith("Z"), "bare dates parse as midnight"
        assert "T" in lo and "T" in hi
        assert lo < hi, "min == max is a Gamma 422"


async def test_closed_recrawl_slices_tile_the_window(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    client = FakeClosedClient()
    await collector.capture_closed_markets(client, store)

    # Cold start (no watermark) walks the full backstop window, one slice per
    # step, plus one slice ahead of now for already-resolved future endDates.
    expected = collector.CLOSED_RECRAWL_WINDOW_S // collector._CLOSED_RECRAWL_SLICE_S + 1
    assert len(client.windows) == expected
    highs = [hi for _, hi, _ in client.windows]
    assert highs == sorted(highs, reverse=True), "should walk newest-first"


async def test_closed_recrawl_is_incremental_after_a_successful_pass(
    tmp_path, monkeypatch
):
    """The 4-day window is a sleep-gap BACKSTOP, not the steady-state cost:
    a full pass captures ~58k markets / 29MB of raw."""
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    store.set_state(
        VENUE_ID, collector.STATE_CLOSED_RECRAWL,
        watermark_ts=NOW - 2 * collector._CLOSED_RECRAWL_SLICE_S, now_ts=NOW,
    )
    client = FakeClosedClient()
    await collector.capture_closed_markets(client, store)

    # 2 slices of elapsed time + 1 overlap + 1 ahead of now.
    assert len(client.windows) == 4
    assert store.get_state(VENUE_ID, collector.STATE_CLOSED_RECRAWL)["watermark_ts"] == NOW


async def test_closed_recrawl_watermark_holds_when_a_pass_fails(tmp_path, monkeypatch):
    """A failed pass must re-walk its gap, never skip the closures it missed."""
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    stale = NOW - 3 * collector._CLOSED_RECRAWL_SLICE_S
    store.set_state(
        VENUE_ID, collector.STATE_CLOSED_RECRAWL, watermark_ts=stale, now_ts=stale
    )

    class Boom(FakeClosedClient):
        async def get_recently_closed_markets(self, *a, **kw):
            raise PolymarketAPIError(500, "/markets", "boom")

    with pytest.raises(PolymarketAPIError):
        await collector.capture_closed_markets(Boom(), store)
    assert store.get_state(
        VENUE_ID, collector.STATE_CLOSED_RECRAWL
    )["watermark_ts"] == stale


async def test_closed_recrawl_targets_polymarket_ledger_ids_only(tmp_path, monkeypatch):
    """The ledger went dual-venue 2026-07-21 and Kalshi tickers were being
    posted to Gamma, which silently returns nothing for them (116/830 ids
    on 2026-07-31). Kalshi finals arrive via collect_settlements."""
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    _seed_closed_decision(store, VENUE_ID, "0xcondA", now=NOW)
    _seed_closed_decision(store, "kalshi", "KXMLBGAME-26JUL22-MIA", now=NOW)

    client = FakeClosedClient()
    result = await collector.capture_closed_markets(client, store)

    posted = [cid for call in client.cid_calls for cid in call]
    assert posted == ["0xcondA"]
    assert result["ledger_targeted"] == 0  # by_cid empty → nothing returned


async def test_closed_recrawl_records_a_slice_that_exhausts_the_offset_cap(
    tmp_path, monkeypatch, caplog
):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(collector, "CLOSED_RECRAWL_WINDOW_S",
                        collector._CLOSED_RECRAWL_SLICE_S)
    store = HotStore(tmp_path / "hot.sqlite")
    # Every slice serves more than the ceiling → every slice caps.
    client = FakeClosedClient(per_slice=collector._CLOSED_RECRAWL_OFFSET_CAP + 500)
    with caplog.at_level("WARNING"):
        await collector.capture_closed_markets(client, store)
    assert "offset cap" in caplog.text
    for _, _, offset in client.windows:
        assert offset <= collector._CLOSED_RECRAWL_OFFSET_CAP
