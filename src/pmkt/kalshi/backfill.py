"""Kalshi historical backfills — budgeted, cursor-resumable loop jobs.

The live tier silently excludes data older than a moving ~3-month cutoff
(data invariants), so depth comes from /historical/*. Both jobs run as slices
inside `pmkt run` (same pattern as the live trades sweep): a page budget per
cycle, the cursor persisted in collector_state after every page, so any
interruption — crash, restart, laptop sleep — costs at most one page and the
next cycle continues where this one stopped. When a crawl reaches its natural
end it records a done-watermark and becomes a near-free no-op.

Two jobs:
- backfill_markets: every settled market since the exchange opened (mid-2021),
  carrying `result` — the resolution data the favorite-longshot research
  needs. Re-crawls weekly (new settlements age past the cutoff continuously).
- backfill_trades: the full maker/taker-labeled tape, two phases with bounds
  FROZEN at first run (the cutoff moves; reusing cursors across changing
  query params is undefined, and a moving lower bound would race data
  migrating between tiers):
  (1) GAP first — the live-tier span [cutoff C0, start of the recorded continuous
      capture]. Runs first because the live tier silently drops data as the
      cutoff advances; anything it loses mid-crawl migrates to historical,
      which phase (2) covers by construction.
  (2) HIST second — the historical tier from the beginning up to C0.
  Seams overlap by an hour on each boundary; curated build_trades dedupes.
"""

from __future__ import annotations

import logging
import time

from ..state import HotStore
from .client import MAX_BATCH_CANDLE_TICKERS, KalshiPublicClient
from .models import VENUE_ID, to_unix_ts

logger = logging.getLogger(__name__)

MARKETS_PAGE_BUDGET = 100   # 1000 markets/page ≈ 100k markets per slice
TRADES_PAGE_BUDGET = 300    # 1000 trades/page ≈ 20s of budget at 15 req/s
MARKETS_RECRAWL_INTERVAL_S = 7 * 24 * 3600

# Where the recorded continuous live trades capture began (first sweep 2026-07-11
# 00:19:55 UTC with a 1h lookback ⇒ coverage from ~1783725595). The gap
# phase overlaps ~100s into covered territory; curated dedupe absorbs it.
LIVE_CAPTURE_START_TS = 1_783_725_700

STATE_MARKETS = "backfill_markets"
STATE_TRADES_HIST = "backfill_trades_hist"
STATE_TRADES_GAP = "backfill_trades_gap"

# 2026-07-17→19 collector outage: the price sweep's 24h lookback clamp
# permanently skipped this 1-min candle window (collector.py warning
# "1-min candles 1784305468..1784385987 skipped"). Bounds frozen ±overlap;
# well inside the live tier (~2-month cutoff), so the live batch endpoint
# serves it. curated build_price_snapshots dedupes the seams.
CANDLE_GAP_START_TS = 1_784_305_468 - 120
CANDLE_GAP_END_TS = 1_784_385_987 + 600
# 90-min windows: 100 tickers × 90 candles = 9,000 ≤ the 10k response cap.
CANDLE_WINDOW_S = 90 * 60
CANDLES_REQUEST_BUDGET = 300  # ≈20s of budget at 15 req/s per slice
STATE_CANDLES_GAP = "backfill_candles_gap"


def _done(row) -> bool:
    return bool(row and row["watermark_ts"] and not row["cursor"])


async def backfill_markets(client: KalshiPublicClient, store: HotStore) -> dict[str, int]:
    """One budgeted slice of the /historical/markets crawl."""
    now_ts = int(time.time())
    state = store.get_state(VENUE_ID, STATE_MARKETS)
    if _done(state) and now_ts - state["watermark_ts"] < MARKETS_RECRAWL_INTERVAL_S:
        return {"pages": 0, "markets": 0, "done": 1}
    cursor = state["cursor"] if state else None

    pages = n_markets = 0
    finished = False
    async for markets, next_cursor in client.iter_historical_markets(cursor=cursor):
        store.upsert_markets(VENUE_ID, [m.hot_row() for m in markets], now_ts)
        n_markets += len(markets)
        pages += 1
        # Persist progress every page; watermark only on natural end.
        store.set_state(
            VENUE_ID, STATE_MARKETS, cursor=next_cursor,
            watermark_ts=now_ts if not next_cursor else None,
            now_ts=int(time.time()),
        )
        if not next_cursor:
            finished = True
            break
        if pages >= MARKETS_PAGE_BUDGET:
            break
    logger.info(
        "historical markets backfill: %d pages, %d markets%s",
        pages, n_markets,
        " — CRAWL COMPLETE" if finished else " (budget hit — next cycle continues)",
    )
    return {"pages": pages, "markets": n_markets, "done": int(finished)}


async def backfill_candles(client: KalshiPublicClient, store: HotStore) -> dict[str, int]:
    """One budgeted slice of the 2026-07-17→19 candle-gap backfill. Candles
    land in the raw lake only (client captures every response before parsing
    — data invariants); build_price_snapshots folds them in. Hot-store market
    prices are NOT touched — these candles are days stale by construction.
    Cursor = last completed ticker chunk's final ticker (keyset resume);
    watermark only on natural end, after which this is a permanent no-op."""
    state = store.get_state(VENUE_ID, STATE_CANDLES_GAP)
    if _done(state):
        return {"chunks": 0, "candles": 0, "done": 1}
    cursor = state["cursor"] if state else None

    windows_per_chunk = -(-(CANDLE_GAP_END_TS - CANDLE_GAP_START_TS) // CANDLE_WINDOW_S)
    max_chunks = max(1, CANDLES_REQUEST_BUDGET // windows_per_chunk)
    tickers = store.candle_gap_tickers(
        VENUE_ID, CANDLE_GAP_START_TS, CANDLE_GAP_END_TS,
        after=cursor, limit=max_chunks * MAX_BATCH_CANDLE_TICKERS,
    )
    if not tickers:
        store.set_state(
            VENUE_ID, STATE_CANDLES_GAP, cursor=None,
            watermark_ts=int(time.time()), now_ts=int(time.time()),
        )
        logger.info("candle gap backfill: no tickers past cursor — GAP COMPLETE")
        return {"chunks": 0, "candles": 0, "done": 1}

    n_candles = chunks = 0
    for i in range(0, len(tickers), MAX_BATCH_CANDLE_TICKERS):
        chunk = tickers[i : i + MAX_BATCH_CANDLE_TICKERS]
        window_start = CANDLE_GAP_START_TS
        while window_start < CANDLE_GAP_END_TS:
            window_end = min(window_start + CANDLE_WINDOW_S, CANDLE_GAP_END_TS)
            for mc in await client.get_batch_candlesticks(
                chunk, start_ts=window_start, end_ts=window_end, period_interval=1
            ):
                n_candles += len(mc.candlesticks)
            window_start = window_end
        chunks += 1
        # Chunk fully fetched — advance the keyset cursor past it.
        store.set_state(
            VENUE_ID, STATE_CANDLES_GAP, cursor=chunk[-1],
            watermark_ts=None, now_ts=int(time.time()),
        )
    finished = len(tickers) < max_chunks * MAX_BATCH_CANDLE_TICKERS
    if finished:
        store.set_state(
            VENUE_ID, STATE_CANDLES_GAP, cursor=None,
            watermark_ts=int(time.time()), now_ts=int(time.time()),
        )
    logger.info(
        "candle gap backfill: %d chunks, %d candles%s",
        chunks, n_candles,
        " — GAP COMPLETE" if finished else " (budget hit — next cycle continues)",
    )
    return {"chunks": chunks, "candles": n_candles, "done": int(finished)}


async def _trades_bounds_c0(client: KalshiPublicClient, store: HotStore) -> int:
    """C0 = trades cutoff frozen at the backfill's very first slice. Both
    phases' query bounds derive from it and never move afterwards — cursors
    are only ever reused against identical query params."""
    bounds = store.get_state(VENUE_ID, "backfill_trades", params_hash="bounds")
    if bounds and bounds["watermark_ts"]:
        return bounds["watermark_ts"]
    payload = await client.get_historical_cutoff()
    # Live-verified 2026-07-11: cutoff timestamps arrive as RFC3339 STRINGS
    # ('2026-05-12T00:00:00Z'), not unix seconds.
    c0 = to_unix_ts(payload.get("trades_created_ts"))
    if not c0:
        raise RuntimeError(f"could not read trades_created_ts from /historical/cutoff: {payload}")
    store.set_state(
        VENUE_ID, "backfill_trades", params_hash="bounds",
        watermark_ts=c0, now_ts=int(time.time()),
    )
    logger.info("trades backfill bounds frozen: C0=%d, live capture start=%d",
                c0, LIVE_CAPTURE_START_TS)
    return c0


async def backfill_trades(client: KalshiPublicClient, store: HotStore) -> dict[str, int]:
    """One budgeted slice of the trade-tape backfill (gap phase first, then
    the historical tier — see module docstring). Trades land in the raw lake
    only; `pmkt lake build-trades` folds them into the curated table
    (endpoint historical_trades is included there)."""
    now_ts = int(time.time())

    gap = store.get_state(VENUE_ID, STATE_TRADES_GAP)
    if not _done(gap):
        c0 = await _trades_bounds_c0(client, store)
        pages = n_trades = 0
        finished = False
        cursor = gap["cursor"] if gap else None
        async for trades, next_cursor in client.iter_trades(
            min_ts=c0 - 3600, max_ts=LIVE_CAPTURE_START_TS, cursor=cursor
        ):
            n_trades += len(trades)
            pages += 1
            store.set_state(
                VENUE_ID, STATE_TRADES_GAP, cursor=next_cursor,
                watermark_ts=now_ts if not next_cursor else None,
                now_ts=int(time.time()),
            )
            if not next_cursor:
                finished = True
                break
            if pages >= TRADES_PAGE_BUDGET:
                break
        logger.info(
            "gap trades backfill (C0→live-capture start): %d pages, %d trades%s",
            pages, n_trades,
            " — GAP COMPLETE" if finished else " (budget hit — next cycle continues)",
        )
        return {"phase_gap": 1, "pages": pages, "trades": n_trades, "done": int(finished)}

    hist = store.get_state(VENUE_ID, STATE_TRADES_HIST)
    if _done(hist):
        return {"pages": 0, "trades": 0, "done": 1}

    c0 = await _trades_bounds_c0(client, store)
    pages = n_trades = 0
    finished = False
    cursor = hist["cursor"] if hist else None
    async for trades, next_cursor in client.iter_historical_trades(
        max_ts=c0 + 3600, cursor=cursor
    ):
        n_trades += len(trades)
        pages += 1
        store.set_state(
            VENUE_ID, STATE_TRADES_HIST, cursor=next_cursor,
            watermark_ts=now_ts if not next_cursor else None,
            now_ts=int(time.time()),
        )
        if not next_cursor:
            finished = True
            break
        if pages >= TRADES_PAGE_BUDGET:
            break
    logger.info(
        "historical trades backfill (start→C0): %d pages, %d trades%s",
        pages, n_trades,
        " — HISTORICAL TIER COMPLETE, tape is contiguous"
        if finished else " (budget hit — next cycle continues)",
    )
    return {"phase_hist": 1, "pages": pages, "trades": n_trades, "done": int(finished)}
