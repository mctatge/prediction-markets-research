"""Kalshi collection jobs: metadata sweep, price sweep, trades sweep.

Raw capture happens inside the client on every response; these jobs maintain
the SQLite hot store (rebuildable projection) and cursors/watermarks.
Default cadences: metadata hourly, prices 5 min, trades 15 min.
Environment variables in config.py override these intervals.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..state import HotStore
from .client import MAX_BATCH_CANDLE_TICKERS, KalshiPublicClient
from .models import VENUE_ID, MarketCandles

logger = logging.getLogger(__name__)

# 1-min candles, 100 tickers/request, ≤10,000 candles/response ⇒ ≤100 minutes
# of lookback per request. 90 for margin; longer outages iterate windows.
PRICE_WINDOW_S = 90 * 60
PRICE_OVERLAP_S = 120
PRICE_MAX_LOOKBACK_S = 24 * 3600
TRADES_OVERLAP_S = 60
# Modest first-run lookback: at current exchange volume the tape runs hundreds
# of pages/day — deep history is the /historical/* backfill phase's job.
TRADES_FIRST_RUN_LOOKBACK_S = 3600
# The tape paginates NEWEST-FIRST (live-verified 2026-07-11): a request window
# must be fully consumed before its trades are safely "ours", so the sweep
# works in bounded slices and advances the watermark per completed slice.
TRADES_SLICE_S = 900  # ≈100-200 pages even at peak (~412k trades/hour)
# Cap pages per invocation so a long catch-up (laptop slept, big backlog)
# can't monopolize the sequential job loop — production incident 2026-07-11:
# one unbounded sweep starved metadata+prices for 16 hours.
TRADES_PAGE_BUDGET = 600


async def collect_metadata(client: KalshiPublicClient, store: HotStore) -> dict[str, int]:
    """Hourly: /series catalog + open events + open markets, paged."""
    sweep_ts = int(time.time())

    series = await client.get_series_catalog()
    store.upsert_series(
        VENUE_ID,
        [
            {
                "venue_series_id": s.ticker,
                "title": s.title,
                "category": s.category,
                "tags": s.tags,
                "fee_type": s.fee_type,
                "fee_multiplier": s.fee_multiplier,
            }
            for s in series
        ],
        sweep_ts,
    )

    event_series: dict[str, str] = {}
    n_events = 0
    async for events, _cursor in client.iter_events(status="open"):
        store.upsert_events(
            VENUE_ID,
            [
                {
                    "venue_event_id": e.event_ticker,
                    "venue_series_id": e.series_ticker,
                    "title": e.title,
                    "mutually_exclusive": e.mutually_exclusive,
                    "category": e.category,
                    "status": "open",
                }
                for e in events
            ],
            sweep_ts,
        )
        for e in events:
            if e.series_ticker:
                event_series[e.event_ticker] = e.series_ticker
        n_events += len(events)

    n_markets = 0
    async for markets, _cursor in client.iter_markets(status="open"):
        rows = []
        for m in markets:
            row = m.hot_row()
            mapped = event_series.get(m.event_ticker or "")
            if mapped:
                row["venue_series_id"] = mapped
            rows.append(row)
        store.upsert_markets(VENUE_ID, rows, sweep_ts)
        n_markets += len(markets)

    n_closed = store.mark_unseen_markets_closed(VENUE_ID, sweep_ts)
    store.set_state(VENUE_ID, "metadata", watermark_ts=sweep_ts, now_ts=sweep_ts)
    logger.info(
        "metadata sweep: %d series, %d open events, %d open markets, %d marked closed",
        len(series), n_events, n_markets, n_closed,
    )
    return {
        "series": len(series),
        "events": n_events,
        "markets": n_markets,
        "marked_closed": n_closed,
    }


async def collect_prices(client: KalshiPublicClient, store: HotStore) -> dict[str, int]:
    """Every 5 min: batch 1-min candles over active open markets (any 24h
    volume or open interest — dead listings are covered by the hourly metadata
    sweep). Candle history self-backfills across downtime (windowed, ≤24h)."""
    now_ts = int(time.time())
    tickers = store.active_market_tickers(VENUE_ID)
    if not tickers:
        logger.warning("price sweep: no active markets in hot store — run metadata first")
        return {"markets": 0, "candles": 0}

    state = store.get_state(VENUE_ID, "prices")
    watermark = state["watermark_ts"] if state and state["watermark_ts"] else None
    desired_start = (watermark or now_ts - PRICE_WINDOW_S) - PRICE_OVERLAP_S
    start_ts = max(desired_start, now_ts - PRICE_MAX_LOOKBACK_S)
    if start_ts > desired_start:
        logger.warning(
            "price sweep: lookback clamped to 24h — 1-min candles %d..%d skipped "
            "(recoverable later via /historical backfill)",
            desired_start, start_ts,
        )

    n_candles = 0
    latest: dict[str, MarketCandles] = {}
    for i in range(0, len(tickers), MAX_BATCH_CANDLE_TICKERS):
        chunk = tickers[i : i + MAX_BATCH_CANDLE_TICKERS]
        window_start = start_ts
        while window_start < now_ts:
            window_end = min(window_start + PRICE_WINDOW_S, now_ts)
            for mc in await client.get_batch_candlesticks(
                chunk, start_ts=window_start, end_ts=window_end, period_interval=1
            ):
                n_candles += len(mc.candlesticks)
                if mc.candlesticks:
                    latest[mc.ticker] = mc
            window_start = window_end

    price_rows: list[dict[str, Any]] = []
    for ticker, mc in latest.items():
        last = mc.candlesticks[-1]
        price_rows.append(
            {
                "venue_market_id": ticker,
                "yes_bid_prob": last.yes_bid_close,
                "yes_ask_prob": last.yes_ask_close,
                "last_prob": last.trade_close,
                # candle volume is per-period — cumulative volume comes from
                # the metadata sweep; only open interest is a level here.
                "open_interest": last.open_interest_fp,
            }
        )
    if price_rows:
        store.update_market_prices(VENUE_ID, price_rows, now_ts)

    store.set_state(VENUE_ID, "prices", watermark_ts=now_ts, now_ts=now_ts)
    logger.info(
        "price sweep: %d active markets, %d candles, %d markets updated",
        len(tickers), n_candles, len(price_rows),
    )
    return {"markets": len(tickers), "candles": n_candles, "updated": len(price_rows)}


async def collect_trades(client: KalshiPublicClient, store: HotStore) -> dict[str, int]:
    """Every 15 min: public trade tape, in bounded slices. Trades land in the
    raw lake only (curated trades table is a compaction-phase concern); this
    job advances the watermark one completed slice at a time, so an
    interruption — crash, page budget, laptop sleep — costs at most one
    slice of refetch and never blocks the other jobs."""
    now_ts = int(time.time())
    state = store.get_state(VENUE_ID, "trades")
    watermark = (
        state["watermark_ts"]
        if state and state["watermark_ts"]
        else now_ts - TRADES_FIRST_RUN_LOOKBACK_S
    )

    pages = 0
    n_trades = 0
    slices_done = 0
    caught_up = True
    while watermark < now_ts:
        if pages >= TRADES_PAGE_BUDGET:
            caught_up = False
            break
        slice_end = min(watermark + TRADES_SLICE_S, now_ts)
        slice_complete = True
        async for trades, cursor in client.iter_trades(
            min_ts=watermark - TRADES_OVERLAP_S, max_ts=slice_end
        ):
            pages += 1
            n_trades += len(trades)
            if pages >= TRADES_PAGE_BUDGET and cursor:
                # Budget exhausted mid-slice: the newest-first tape means the
                # slice's older trades are still unfetched — do NOT advance.
                slice_complete = False
                break
        if not slice_complete:
            caught_up = False
            break
        watermark = slice_end
        slices_done += 1
        store.set_state(
            VENUE_ID, "trades", cursor=None, watermark_ts=watermark, now_ts=int(time.time())
        )

    logger.info(
        "trades sweep: %d trades, %d pages, %d slices, watermark → %d%s",
        n_trades, pages, slices_done, watermark,
        "" if caught_up else " (page budget hit — next cycle continues)",
    )
    return {"trades": n_trades, "pages": pages, "caught_up": int(caught_up)}


SETTLEMENTS_PAGE_BUDGET = 300
SETTLEMENTS_OVERLAP_S = 3600
# Live-tier settlements exist from the historical cutoff forward; older ones
# come from backfill_markets. Frozen at the trades-backfill C0 (2026-05-12).
SETTLEMENTS_EPOCH_TS = 1_778_544_000


async def collect_settlements(client: KalshiPublicClient, store: HotStore) -> dict[str, int]:
    """Every 30 min: live-tier settled markets (status=settled, windowed by
    min_settled_ts). This is where RESULTS for recently-resolved markets come
    from — the live metadata sweep only enumerates open markets, and
    /historical/markets only has settlements older than the moving cutoff, so
    without this job everything settling in between carries no outcome. Those
    outcomes are the training labels for the calibration model.

    Cursor-resumable with a page budget (first catch-up from the epoch is a
    few hundred pages); the watermark advances only when an enumeration
    completes, so an interrupted sweep re-covers its window."""
    now_ts = int(time.time())
    state = store.get_state(VENUE_ID, "settlements")
    watermark = state["watermark_ts"] if state and state["watermark_ts"] else None
    cursor = state["cursor"] if state else None
    min_settled = (watermark or SETTLEMENTS_EPOCH_TS) - SETTLEMENTS_OVERLAP_S

    pages = n_markets = 0
    finished = False
    async for markets, next_cursor in client.iter_markets(
        status="settled", min_settled_ts=min_settled, cursor=cursor
    ):
        store.upsert_markets(VENUE_ID, [m.hot_row() for m in markets], now_ts)
        n_markets += len(markets)
        pages += 1
        store.set_state(
            VENUE_ID, "settlements", cursor=next_cursor,
            # watermark only moves on a COMPLETED enumeration; sweep-start
            # time (not max settled seen) keeps the window race-free.
            watermark_ts=now_ts if not next_cursor else None,
            now_ts=int(time.time()),
        )
        if not next_cursor:
            finished = True
            break
        if pages >= SETTLEMENTS_PAGE_BUDGET:
            break
    logger.info(
        "settlements sweep: %d pages, %d settled markets since %d%s",
        pages, n_markets, min_settled,
        "" if finished else " (budget hit — next cycle continues)",
    )
    return {"pages": pages, "markets": n_markets, "done": int(finished)}
