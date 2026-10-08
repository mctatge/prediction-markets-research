"""Polymarket trade-tape bridge backfill — fills the gap between the v1
on-chain corpus (arXiv 2606.04217, ends 2026-04-28) and the forward
Data-API collector (collect_trades, 15-min cadence).

Per-market crawl via Data-API /trades?market=conditionId — the global feed
is useless (86% churn, 3-min reach, no time-windowing). Runs as budgeted
slices in the pmkt run loop (10-min cadence). Markets are crawled in
lifetime-volume order so the signal-relevant tape lands weeks before the
thin tail; because that ordering shifts between slices, progress is the
hot store's pm_backfill_done SET, with a `conditionId:offset` cursor for
the in-progress market only. There is deliberately NO migration from the
old positional conditionId cursor: inferring "done" from a cursor position
can mark never-crawled markets complete (the universe grows under the
cursor), and a wrong done-row is a permanent silent hole while a re-crawl
is a free dedup. The trade tape is durable (retention verified to
mid-2024) so there is no deadline pressure.

Failure policy: a failed fetch consumes page budget (an outage ends the
slice instead of walking the queue), and only a hard 4xx — the API
permanently rejecting this market/offset — marks a market done without a
completed crawl. Retry-exhausted transport/429/5xx errors abort the slice
so the next cycle retries the same market.

Overlap with both the v1 corpus timestamps and the forward collector's
captures is harmless: build_pm_trades deduplicates on the md5 trade key.
"""

from __future__ import annotations

import logging
import time

from ..state import HotStore
from .client import PolymarketAPIError, PolymarketClient
from .models import VENUE_ID

logger = logging.getLogger(__name__)

V1_END_TS = 1_777_334_400  # 2026-04-28 00:00:00 UTC (v2 CTF migration)

TRADES_PAGE_BUDGET = 200  # ~80s at 2.5 req/s; fits in a 10-min cycle
TRADES_LIMIT = 1000
# Bridge captures land in their own raw partition so the live OFI scorer's
# data_api_trades scan never opens these bulk historical pages (a slice
# writes ~200MB/day of 1MB pages — scanning them OOM'd ofi_score).
# build_pm_trades reads both endpoints and dedupes on the md5 key.
BACKFILL_ENDPOINT = "data_api_trades_backfill"
# Fetch headroom over the page budget (each market costs >= 1 page). Also
# the completion sentinel: fewer remaining rows than this means the queue
# is the entire tail of the crawl.
MARKET_FETCH_LIMIT = 500
STATE_BACKFILL_TRADES = "backfill_trades"


def _parse_cursor(cursor: str | None) -> tuple[str | None, int]:
    """Parse 'conditionId:offset' → (conditionId, offset)."""
    if not cursor:
        return None, 0
    parts = cursor.rsplit(":", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0], int(parts[1])
    return cursor, 0


async def backfill_trades(
    client: PolymarketClient, store: HotStore
) -> dict[str, int]:
    """One budgeted slice of the PM trade-tape bridge backfill.

    Crawls each not-yet-done market's /trades tape from newest to oldest
    until reaching the v1 corpus coverage boundary (2026-04-28), highest
    lifetime volume first. Raw capture happens inside the client on every
    response (data invariants)."""
    now_ts = int(time.time())
    state = store.get_state(VENUE_ID, STATE_BACKFILL_TRADES)

    if state and state["watermark_ts"] and not state["cursor"]:
        return {"pages": 0, "trades": 0, "markets": 0, "done": 1}

    cursor_cid, cursor_offset = _parse_cursor(
        state["cursor"] if state else None
    )

    universe = store.pm_backfill_universe_count(VENUE_ID, v1_end_ts=V1_END_TS)
    if not universe:
        logger.warning(
            "pm trades backfill: no markets in hot store — run metadata first"
        )
        return {"pages": 0, "trades": 0, "markets": 0}

    remaining = store.pm_backfill_markets(
        VENUE_ID, v1_end_ts=V1_END_TS, limit=MARKET_FETCH_LIMIT
    )

    # In-progress market resumes first (mid-tape offset), then the
    # volume-ordered rest.
    queue: list[tuple[str, int]] = []
    if cursor_cid and not store.pm_backfill_is_done(VENUE_ID, cursor_cid):
        queue.append((cursor_cid, cursor_offset))
    queue.extend((cid, 0) for cid in remaining if cid != cursor_cid)

    if not queue:
        store.set_state(
            VENUE_ID, STATE_BACKFILL_TRADES,
            cursor=None, watermark_ts=now_ts, now_ts=int(time.time()),
        )
        logger.info(
            "pm trades backfill: CRAWL COMPLETE — %d/%d markets done",
            store.pm_backfill_done_count(VENUE_ID), universe,
        )
        return {"pages": 0, "trades": 0, "markets": 0, "done": 1}

    n_pages = 0
    n_trades = 0
    n_markets = 0
    aborted = False

    for cid, offset in queue:
        if n_pages >= TRADES_PAGE_BUDGET:
            break

        market_trades = 0
        market_done = False

        while n_pages < TRADES_PAGE_BUDGET:
            try:
                trades = await client.get_trades(
                    cid, limit=TRADES_LIMIT, offset=offset,
                    endpoint=BACKFILL_ENDPOINT,
                )
            except PolymarketAPIError as exc:
                n_pages += 1  # failures consume budget: outages end slices
                if 400 <= exc.status < 500 and exc.status != 429:
                    # permanent rejection of this market/offset — skip it
                    logger.warning(
                        "pm trades backfill: %s rejected (HTTP %d) at "
                        "offset=%d — skipping market",
                        cid[:16], exc.status, offset,
                    )
                    market_done = True
                else:
                    # transport/429/5xx retries exhausted — likely an outage;
                    # end the slice, the next cycle retries this market
                    logger.warning(
                        "pm trades backfill: fetch failed for %s offset=%d "
                        "(%s) — aborting slice", cid[:16], offset, exc,
                    )
                    aborted = True
                break

            n_pages += 1

            if not trades:
                market_done = True
                break

            market_trades += len(trades)

            oldest_ts = min(
                (t.timestamp for t in trades if t.timestamp), default=None
            )
            if oldest_ts is not None and oldest_ts <= V1_END_TS:
                market_done = True
                break

            if len(trades) < TRADES_LIMIT:
                market_done = True
                break

            offset += len(trades)
            if offset >= 3_000:
                logger.warning(
                    "pm trades backfill: offset cap 3k for %s (%d trades) — "
                    "some mid-gap trades may be unreachable via Data-API",
                    cid[:16], market_trades,
                )
                market_done = True
                break

            store.set_state(
                VENUE_ID, STATE_BACKFILL_TRADES,
                cursor=f"{cid}:{offset}", now_ts=int(time.time()),
            )

        n_trades += market_trades

        if not market_done:
            break  # abort or budget hit mid-market — resume next slice

        store.pm_backfill_mark_done(VENUE_ID, cid, now_ts=int(time.time()))
        store.set_state(
            VENUE_ID, STATE_BACKFILL_TRADES,
            cursor=None, now_ts=int(time.time()),
        )
        n_markets += 1

    # The queue is the whole tail of the crawl unless the fetch came back
    # LIMIT-truncated; completing all of it completes the crawl.
    finished = (
        n_markets == len(queue) and len(remaining) < MARKET_FETCH_LIMIT
    )
    if finished:
        store.set_state(
            VENUE_ID, STATE_BACKFILL_TRADES,
            cursor=None, watermark_ts=now_ts, now_ts=int(time.time()),
        )

    done_total = store.pm_backfill_done_count(VENUE_ID)
    suffix = " — CRAWL COMPLETE" if finished else (
        " (budget hit — next cycle continues)"
        if n_pages >= TRADES_PAGE_BUDGET and not aborted else ""
    )
    logger.info(
        "pm trades backfill: %d pages, %d trades, %d markets — %d/%d done%s",
        n_pages, n_trades, n_markets, done_total, universe, suffix,
    )
    return {
        "pages": n_pages,
        "trades": n_trades,
        "markets": n_markets,
        "total_markets": universe,
        "done": int(finished),
    }
