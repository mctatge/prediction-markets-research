"""Backfill slice semantics: cursor resume, page budgets, done markers,
gap-before-historical phase ordering with frozen bounds."""

import time

from pmkt.kalshi import backfill
from pmkt.state import HotStore

NOW = 1_783_900_000


class FakeHistClient:
    """Serves `total_pages` pages per iterator, records calls."""

    def __init__(self, total_pages=5):
        self.total = total_pages
        self.calls = []

    def _pages(self, kind, start_cursor):
        start = int(start_cursor or 0)

        async def gen():
            for page in range(start, self.total):
                next_cursor = str(page + 1) if page + 1 < self.total else None
                yield [{"id": f"{kind}-{page}"}] * 3, next_cursor

        return gen()

    def iter_historical_markets(self, cursor=None, **kw):
        self.calls.append(("hist_markets", cursor))

        async def gen():
            start = int(cursor or 0)
            for page in range(start, self.total):
                next_cursor = str(page + 1) if page + 1 < self.total else None
                yield [FakeMarket(f"M-{page}-{i}") for i in range(3)], next_cursor

        return gen()

    def iter_trades(self, min_ts=None, max_ts=None, cursor=None):
        self.calls.append(("live_trades", min_ts, max_ts, cursor))
        return self._pages("gap", cursor)

    def iter_historical_trades(self, min_ts=None, max_ts=None, cursor=None):
        self.calls.append(("hist_trades", min_ts, max_ts, cursor))
        return self._pages("hist", cursor)

    async def get_historical_cutoff(self):
        self.calls.append(("cutoff",))
        return {"trades_created_ts": "2026-07-01T00:00:00Z"}  # RFC3339, live-verified


class FakeMarket:
    def __init__(self, ticker):
        self.ticker = ticker

    def hot_row(self):
        return {"venue_market_id": self.ticker, "status_norm": "settled"}


async def test_backfill_markets_budget_resume_and_done(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(backfill, "MARKETS_PAGE_BUDGET", 2)
    store = HotStore(tmp_path / "hot.sqlite")
    client = FakeHistClient(total_pages=5)

    r1 = await backfill.backfill_markets(client, store)
    assert (r1["pages"], r1["done"]) == (2, 0)
    assert store.get_state("kalshi", backfill.STATE_MARKETS)["cursor"] == "2"

    r2 = await backfill.backfill_markets(client, store)  # resumes at page 2
    assert client.calls[-1] == ("hist_markets", "2")
    r3 = await backfill.backfill_markets(client, store)
    assert r3["done"] == 1
    state = store.get_state("kalshi", backfill.STATE_MARKETS)
    assert state["cursor"] is None and state["watermark_ts"] == NOW
    # 5 pages × 3 markets landed, statuses settled
    n, = store.conn.execute(
        "SELECT COUNT(*) FROM markets WHERE venue_id='kalshi' AND status_norm='settled'"
    ).fetchone()
    assert n == 15

    # done + fresh watermark -> no-op (no new client calls)
    calls_before = len(client.calls)
    r4 = await backfill.backfill_markets(client, store)
    assert r4["done"] == 1 and len(client.calls) == calls_before

    # …until the weekly recrawl window elapses
    monkeypatch.setattr(
        time, "time", lambda: NOW + backfill.MARKETS_RECRAWL_INTERVAL_S + 1
    )
    await backfill.backfill_markets(client, store)
    assert len(client.calls) > calls_before


async def test_backfill_trades_gap_runs_first_with_frozen_bounds(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(backfill, "TRADES_PAGE_BUDGET", 10)
    store = HotStore(tmp_path / "hot.sqlite")
    client = FakeHistClient(total_pages=2)

    r1 = await backfill.backfill_trades(client, store)
    assert r1.get("phase_gap") == 1 and r1["done"] == 1
    # bounds frozen from the cutoff endpoint, gap queried live-tier with them
    gap_call = next(c for c in client.calls if c[0] == "live_trades")
    assert gap_call[1] == 1_782_864_000 - 3600  # 2026-07-01 UTC
    assert gap_call[2] == backfill.LIVE_CAPTURE_START_TS

    r2 = await backfill.backfill_trades(client, store)
    assert r2.get("phase_hist") == 1 and r2["done"] == 1
    hist_call = next(c for c in client.calls if c[0] == "hist_trades")
    # historical phase bounded by the SAME frozen C0 (+overlap), cutoff not refetched
    assert hist_call[2] == 1_782_864_000 + 3600
    assert sum(1 for c in client.calls if c == ("cutoff",)) == 1

    r3 = await backfill.backfill_trades(client, store)
    assert r3 == {"pages": 0, "trades": 0, "done": 1}


async def test_backfill_trades_budget_persists_cursor_mid_phase(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(backfill, "TRADES_PAGE_BUDGET", 1)
    store = HotStore(tmp_path / "hot.sqlite")
    client = FakeHistClient(total_pages=3)

    r1 = await backfill.backfill_trades(client, store)
    assert r1.get("phase_gap") == 1 and r1["done"] == 0
    assert store.get_state("kalshi", backfill.STATE_TRADES_GAP)["cursor"] == "1"
    r2 = await backfill.backfill_trades(client, store)
    assert client.calls[-1][3] == "1", "gap phase resumed from persisted cursor"
    assert store.get_state("kalshi", backfill.STATE_TRADES_HIST) is None, (
        "historical phase must not start until the gap phase is done"
    )
