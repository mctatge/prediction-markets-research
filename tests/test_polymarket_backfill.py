"""PM trade-tape bridge backfill: volume-ordered crawl, done-set resume,
mid-market cursor, failure policy (abort vs hard-skip), completion."""

import time

from pmkt.polymarket import backfill
from pmkt.polymarket.client import PolymarketAPIError
from pmkt.state import HotStore

NOW = 1_784_100_000
FRESH_TS = backfill.V1_END_TS + 50_000   # inside the gap window
V1_TS = backfill.V1_END_TS - 50_000      # covered by the v1 corpus


class FakeTrade:
    def __init__(self, ts):
        self.timestamp = ts


class FakePMClient:
    """Serves canned per-market tapes (newest-first), records calls."""

    def __init__(self, tapes):
        self.tapes = tapes  # {conditionId: [FakeTrade, ...]}
        self.calls = []
        self.fail = {}  # {conditionId: http_status_to_raise}

    async def get_trades(self, condition_id, *, limit=1000, offset=0,
                         endpoint="data_api_trades"):
        assert endpoint == backfill.BACKFILL_ENDPOINT, (
            "bridge captures must land in their own raw partition"
        )
        self.calls.append((condition_id, offset))
        if condition_id in self.fail:
            raise PolymarketAPIError(self.fail[condition_id], "http://test", "boom")
        tape = self.tapes.get(condition_id, [])
        return tape[offset:offset + limit]


def seed_market(store, cid, volume=None, close_ts=FRESH_TS, recurrence=None):
    store.conn.execute(
        """INSERT INTO markets (venue_id, venue_market_id, close_ts, volume,
                                recurrence, first_seen_ts, last_seen_ts)
           VALUES ('polymarket',?,?,?,?,0,0)""",
        (cid, close_ts, volume, recurrence),
    )
    store.conn.commit()


def ended_tape(n):
    """A tape whose last page reaches back into v1 coverage (crawl stops)."""
    return [FakeTrade(FRESH_TS) for _ in range(n)] + [FakeTrade(V1_TS)]


async def test_volume_order_then_complete(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    seed_market(store, "0xaa", volume="100")
    seed_market(store, "0xbb", volume="5000")
    seed_market(store, "0xcc", volume=None)  # NULL volume sorts last
    seed_market(store, "0xdd", volume="9", recurrence="5m")  # churn: excluded
    seed_market(store, "0xee", volume="9", close_ts=V1_TS)  # pre-gap: excluded
    client = FakePMClient({cid: ended_tape(3) for cid in ("0xaa", "0xbb", "0xcc")})

    r1 = await backfill.backfill_trades(client, store)
    assert [c[0] for c in client.calls] == ["0xbb", "0xaa", "0xcc"]
    assert (r1["markets"], r1["total_markets"]) == (3, 3)
    assert r1["done"] == 1, "queue fully drained in-slice -> complete now"
    state = store.get_state("polymarket", backfill.STATE_BACKFILL_TRADES)
    assert state["cursor"] is None and state["watermark_ts"] == NOW

    calls_before = len(client.calls)
    r2 = await backfill.backfill_trades(client, store)  # watermark no-op
    assert r2["done"] == 1 and len(client.calls) == calls_before


async def test_budget_persists_mid_market_cursor(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(backfill, "TRADES_PAGE_BUDGET", 2)
    store = HotStore(tmp_path / "hot.sqlite")
    seed_market(store, "0xbb", volume="5000")
    # 2500 fresh trades: full, full, partial -> 3 pages to finish
    client = FakePMClient({"0xbb": [FakeTrade(FRESH_TS)] * 2500})

    r1 = await backfill.backfill_trades(client, store)
    assert (r1["pages"], r1["markets"], r1["done"]) == (2, 0, 0)
    state = store.get_state("polymarket", backfill.STATE_BACKFILL_TRADES)
    assert state["cursor"] == "0xbb:2000"
    assert not store.pm_backfill_is_done("polymarket", "0xbb")

    r2 = await backfill.backfill_trades(client, store)
    assert client.calls[-1] == ("0xbb", 2000), "resumed at the persisted offset"
    assert (r2["markets"], r2["done"]) == (1, 1)
    assert store.pm_backfill_is_done("polymarket", "0xbb")
    assert store.get_state(
        "polymarket", backfill.STATE_BACKFILL_TRADES
    )["cursor"] is None


async def test_stale_positional_cursor_resumes_without_seeding(tmp_path, monkeypatch):
    """A pre-volume-ordering cursor must NOT infer done-ness for markets
    below it — everything gets (re)crawled; only the cursor market jumps
    the volume queue."""
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    seed_market(store, "0xaa", volume="1")
    seed_market(store, "0xbb", volume="1")
    seed_market(store, "0xcc", volume="9000")
    store.set_state(
        "polymarket", backfill.STATE_BACKFILL_TRADES,
        cursor="0xbb:0", now_ts=NOW,
    )
    client = FakePMClient({cid: ended_tape(2) for cid in ("0xaa", "0xbb", "0xcc")})

    r1 = await backfill.backfill_trades(client, store)
    assert [c[0] for c in client.calls] == ["0xbb", "0xcc", "0xaa"]
    assert r1["markets"] == 3 and r1["done"] == 1
    assert store.pm_backfill_done_count("polymarket") == 3, "no phantom seeds"


async def test_offset_cap_ends_market(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    seed_market(store, "0xbb", volume="5000")
    client = FakePMClient({"0xbb": [FakeTrade(FRESH_TS)] * 5000})

    r1 = await backfill.backfill_trades(client, store)
    assert r1["pages"] == 3, "stops at the 3k per-market offset cap"
    assert max(c[1] for c in client.calls) == 2000
    assert store.pm_backfill_is_done("polymarket", "0xbb")


async def test_retryable_failure_aborts_slice_without_marking_done(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    seed_market(store, "0xaa", volume="9000")
    seed_market(store, "0xbb", volume="100")
    client = FakePMClient({"0xaa": ended_tape(2), "0xbb": ended_tape(2)})
    client.fail["0xaa"] = 500  # retries exhausted upstream

    r1 = await backfill.backfill_trades(client, store)
    assert client.calls == [("0xaa", 0)], "slice aborts; queue is not walked"
    assert (r1["pages"], r1["markets"], r1["done"]) == (1, 0, 0)
    assert not store.pm_backfill_is_done("polymarket", "0xaa")
    state = store.get_state("polymarket", backfill.STATE_BACKFILL_TRADES)
    assert state is None or state["watermark_ts"] is None

    client.fail.clear()  # outage over -> next slice retries the same market
    r2 = await backfill.backfill_trades(client, store)
    assert [c[0] for c in client.calls[1:]] == ["0xaa", "0xbb"]
    assert r2["markets"] == 2 and r2["done"] == 1


async def test_hard_4xx_skips_market_as_done(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    seed_market(store, "0xaa", volume="9000")
    seed_market(store, "0xbb", volume="100")
    client = FakePMClient({"0xbb": ended_tape(2)})
    client.fail["0xaa"] = 404  # API permanently rejects this market

    r1 = await backfill.backfill_trades(client, store)
    assert [c[0] for c in client.calls] == ["0xaa", "0xbb"], "slice continues"
    assert r1["markets"] == 2 and r1["done"] == 1
    assert store.pm_backfill_is_done("polymarket", "0xaa")


async def test_limit_truncated_batch_defers_completion(tmp_path, monkeypatch):
    """A LIMIT-sized fetch can't prove the queue is the tail of the crawl —
    completion must wait for the next slice's requery."""
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(backfill, "MARKET_FETCH_LIMIT", 2)
    store = HotStore(tmp_path / "hot.sqlite")
    seed_market(store, "0xaa", volume="100")
    seed_market(store, "0xbb", volume="5000")
    client = FakePMClient({cid: ended_tape(2) for cid in ("0xaa", "0xbb")})

    r1 = await backfill.backfill_trades(client, store)
    assert r1["markets"] == 2 and r1["done"] == 0
    assert store.get_state(
        "polymarket", backfill.STATE_BACKFILL_TRADES
    )["watermark_ts"] is None

    r2 = await backfill.backfill_trades(client, store)  # empty requery -> done
    assert r2["done"] == 1
    assert store.get_state(
        "polymarket", backfill.STATE_BACKFILL_TRADES
    )["watermark_ts"] == NOW
