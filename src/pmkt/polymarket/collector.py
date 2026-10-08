"""Polymarket collection jobs: metadata, price history and trade sweeps.

Raw capture happens inside the client on every response; these jobs maintain
the SQLite hot store (rebuildable projection) and watermarks. Collection
intervals are configured through environment variables in config.py.

Fine-grained price history has been observed to be pruned after resolution;
some data missed live may be unrecoverable, so jobs aim to survive
bad rows: one malformed event/market/token is logged and skipped, never
allowed to kill a sweep. The closed=true historical crawl runs as a budgeted
backfill job (backfill_metadata). These collection jobs use public read APIs.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..state import HotStore
from .client import MAX_BATCH_PRICE_TOKENS, PolymarketAPIError, PolymarketClient
from .models import VENUE_ID

logger = logging.getLogger(__name__)

# prices-history windowing: fidelity=1 (minute points), 20 tokens/request.
# A window bounds points-per-response (~20 tokens × window-minutes) and lets
# catch-up after downtime iterate instead of one giant request.
PRICE_WINDOW_S = 4 * 3600
PRICE_OVERLAP_S = 120
# Minute data survives while a market is LIVE, so a longer clamp than
# Kalshi's would work — but resolved-in-the-gap markets lose theirs, hence
# continuous collection. 24h bounds worst-case catch-up volume; it is also
# the never-covered deep first window (~6 windows × chunk, one-time cost).
PRICE_MAX_LOOKBACK_S = 24 * 3600


def _upsert_event_page(
    store: HotStore, events: list, sweep_ts: int
) -> tuple[int, int, int, int]:
    """Upsert one keyset page of events (+embedded markets, instruments).
    Shared by the live sweep and the closed=true backfill. One bad event must
    not kill a sweep (data invariants: missed live data is unrecoverable)."""
    event_rows: list[dict[str, Any]] = []
    market_rows: list[dict[str, Any]] = []
    instrument_rows: list[dict[str, Any]] = []
    n_skipped = 0
    for event in events:
        try:
            event_rows.append(event.hot_row())
            recurrence = event.recurrence_class()
            for market in event.markets:
                if not market.condition_id or not market.token_ids:
                    # placeholder/pre-deployment market — raw lake has it
                    n_skipped += 1
                    continue
                row = market.hot_row()
                row["venue_event_id"] = event.id
                row["venue_series_id"] = event.effective_series_slug
                row["recurrence"] = recurrence
                market_rows.append(row)
                instrument_rows.extend(market.instrument_rows())
        except Exception:
            n_skipped += 1
            logger.exception("skipping malformed event %s", getattr(event, "id", "?"))
    store.upsert_events(VENUE_ID, event_rows, sweep_ts)
    store.upsert_markets(VENUE_ID, market_rows, sweep_ts)
    store.upsert_instruments(VENUE_ID, instrument_rows, sweep_ts)
    return len(event_rows), len(market_rows), len(instrument_rows), n_skipped


async def collect_metadata(client: PolymarketClient, store: HotStore) -> dict[str, int]:
    """Hourly: enumerate the LIVE universe (active=true, closed=false) via
    Gamma /events/keyset with embedded markets[]; upsert events, markets and
    per-outcome instruments. Recurring 5-min/hourly crypto churn markets are
    captured too (raw + hot rows are cheap) but tagged with a derived
    recurrence class so the price sweep can exclude them."""
    sweep_ts = int(time.time())

    n_events = n_markets = n_instruments = n_skipped = 0
    async for events, _cursor in client.iter_events_keyset(active=True, closed=False):
        e, m, i, s = _upsert_event_page(store, events, sweep_ts)
        n_events += e
        n_markets += m
        n_instruments += i
        n_skipped += s

    # Only runs after a COMPLETE enumeration — a failed page raises out of
    # the loop above, so an interrupted sweep can never mass-close markets.
    n_closed = store.mark_unseen_markets_closed(VENUE_ID, sweep_ts)
    store.set_state(VENUE_ID, "metadata", watermark_ts=sweep_ts, now_ts=sweep_ts)
    logger.info(
        "polymarket metadata sweep: %d events, %d markets, %d instruments, "
        "%d skipped, %d marked closed",
        n_events, n_markets, n_instruments, n_skipped, n_closed,
    )
    return {
        "events": n_events,
        "markets": n_markets,
        "instruments": n_instruments,
        "skipped": n_skipped,
        "marked_closed": n_closed,
    }


async def collect_prices(client: PolymarketClient, store: HotStore) -> dict[str, int]:
    """Every 5–15 min: batch minute-fidelity prices-history over the active
    primary tokens (open markets with activity, fast-churn series excluded —
    see HotStore.active_primary_tokens).

    Coverage is tracked PER TOKEN (instruments.price_covered_ts), not by a
    global watermark, because this venue prunes minute history at resolution
    (data invariants) and the two global-watermark failure modes are silent and
    permanent: (a) a failed chunk's tokens would lose the window the watermark
    skipped past; (b) a token newly entering the universe (markets appear up
    to one metadata interval after their first trade) would start at the
    global watermark and never fetch its earliest prices. New tokens
    (covered_ts NULL) get a deep first window instead; failed chunks keep
    their old marker and re-fetch next sweep (bounded by the 24h clamp)."""
    now_ts = int(time.time())
    rows = store.active_primary_tokens(VENUE_ID)
    if not rows:
        logger.warning("polymarket price sweep: no active tokens in hot store — run metadata first")
        return {"tokens": 0, "points": 0, "updated": 0}
    market_by_token = {t: m for t, m, _c in rows}
    covered_by_token = {t: c for t, _m, c in rows}

    n_points = 0
    n_failed_chunks = 0
    latest: dict[str, Any] = {}  # token_id -> newest PricePoint

    def chunk_start(chunk: list[str]) -> int:
        starts = []
        for t in chunk:
            covered = covered_by_token[t]
            if covered is None:
                # Never covered: reach back the full clamp so a market's
                # earliest (often news-driven) minutes aren't lost to the
                # discovery lag between first trade and metadata sweep.
                starts.append(now_ts - PRICE_MAX_LOOKBACK_S)
            else:
                starts.append(covered - PRICE_OVERLAP_S)
        return max(min(starts), now_ts - PRICE_MAX_LOOKBACK_S)

    async def fetch_chunk(chunk: list[str]) -> bool:
        """Fetch all windows for one chunk; True = clean, coverage advanced."""
        nonlocal n_points
        window_start = chunk_start(chunk)
        while window_start < now_ts:
            window_end = min(window_start + PRICE_WINDOW_S, now_ts)
            history = await client.get_batch_prices_history(
                chunk, start_ts=window_start, end_ts=window_end, fidelity=1
            )
            for token_id, points in history.items():
                n_points += len(points)
                if points and (
                    token_id not in latest or points[-1].t >= latest[token_id].t
                ):
                    latest[token_id] = points[-1]
            window_start = window_end
        store.set_price_covered(VENUE_ID, chunk, now_ts)
        return True

    # active_primary_tokens orders by covered_ts (NULLs first), so chunks
    # group tokens with similar start windows.
    tokens = [t for t, _m, _c in rows]
    for i in range(0, len(tokens), MAX_BATCH_PRICE_TOKENS):
        chunk = tokens[i : i + MAX_BATCH_PRICE_TOKENS]
        try:
            await fetch_chunk(chunk)
        except PolymarketAPIError:
            logger.exception(
                "price chunk failed (%d tokens) — splitting so one poisoned "
                "token can't starve the rest", len(chunk),
            )
            # One bad token/window 400ing forever must not permanently stall
            # 19 healthy tokens' coverage: retry each half once.
            for half in (chunk[: len(chunk) // 2], chunk[len(chunk) // 2 :]):
                if not half:
                    continue
                try:
                    await fetch_chunk(half)
                except PolymarketAPIError:
                    n_failed_chunks += 1
                    logger.exception(
                        "price half-chunk failed (%d tokens) — coverage NOT "
                        "advanced; refetched next sweep", len(half),
                    )

    price_rows: list[dict[str, Any]] = []
    for token_id, point in latest.items():
        market_id = market_by_token.get(token_id)
        if market_id is None:
            logger.warning("price history for unknown token %s — skipping", token_id)
            continue
        price_rows.append({"venue_market_id": market_id, "last_prob": point.p})
    if price_rows:
        store.update_market_prices(VENUE_ID, price_rows, now_ts)

    # Informational only (pmkt status) — correctness rides on per-token
    # price_covered_ts, which never advances past a failed fetch.
    store.set_state(VENUE_ID, "prices", watermark_ts=now_ts, now_ts=now_ts)
    logger.info(
        "polymarket price sweep: %d tokens, %d points, %d markets updated, %d failed chunks",
        len(tokens), n_points, len(price_rows), n_failed_chunks,
    )
    return {
        "tokens": len(tokens),
        "points": n_points,
        "updated": len(price_rows),
        "failed_chunks": n_failed_chunks,
    }


# ---------------------------------------------------------------------------
# Trades sweep (Data-API /trades — wallet-attributed trade tape)
# ---------------------------------------------------------------------------
# Per-market crawl over active non-churn PM markets. The global feed is
# unsuitable for historical coverage (86% churn, 3-min reach in a July 2026 probe). Each market's tape
# paginates with offset (cap 10k). The sweep processes markets in descending
# volume order and stops at the page budget, so the highest-signal markets
# are always covered first. Trades on lower-volume markets that exceed the
# budget may be fetched on later sweeps, subject to API retention and pagination
# limits. Historical retention was observed to mid-2024 in July 2026.
TRADES_PAGE_BUDGET = 400
TRADES_LIMIT = 1000
STATE_TRADES = "data_api_trades"


async def collect_trades(
    client: PolymarketClient, store: HotStore
) -> dict[str, int]:
    """15-min: per-market trade sweep over active non-churn PM markets.
    Raw capture happens inside the client on every response (data invariants).
    The curated pm_trades projection rebuilds from raw."""
    now_ts = int(time.time())
    state = store.get_state(VENUE_ID, STATE_TRADES)
    watermark_ts = state["watermark_ts"] if state else None

    if watermark_ts is None:
        store.set_state(VENUE_ID, STATE_TRADES, watermark_ts=now_ts, now_ts=now_ts)
        logger.info("polymarket trades: initialized watermark at %d", now_ts)
        return {"markets": 0, "trades": 0, "pages": 0, "initialized": True}

    condition_ids = store.pm_trade_markets(VENUE_ID)
    if not condition_ids:
        logger.warning(
            "polymarket trades: no active trade markets — run metadata first"
        )
        return {"markets": 0, "trades": 0, "pages": 0}

    n_pages = 0
    n_trades = 0
    n_markets = 0

    for cid in condition_ids:
        if n_pages >= TRADES_PAGE_BUDGET:
            break
        offset = 0
        market_new = 0
        while n_pages < TRADES_PAGE_BUDGET:
            try:
                trades = await client.get_trades(cid, limit=TRADES_LIMIT, offset=offset)
            except PolymarketAPIError:
                logger.exception(
                    "polymarket trades: fetch failed for %s offset=%d — skipping",
                    cid[:16], offset,
                )
                break
            n_pages += 1
            if not trades:
                break
            new_in_page = sum(
                1 for t in trades if t.timestamp and t.timestamp > watermark_ts
            )
            market_new += new_in_page
            if new_in_page < len(trades):
                break
            if len(trades) < TRADES_LIMIT:
                break
            offset += len(trades)
            if offset >= 3_000:
                logger.warning(
                    "polymarket trades: offset cap 3k for %s — some trades "
                    "may await next cycle", cid[:16],
                )
                break
        n_trades += market_new
        n_markets += 1

    store.set_state(VENUE_ID, STATE_TRADES, watermark_ts=now_ts, now_ts=now_ts)
    logger.info(
        "polymarket trade sweep: %d/%d markets, %d new trades, %d pages",
        n_markets, len(condition_ids), n_trades, n_pages,
    )
    return {"markets": n_markets, "trades": n_trades, "pages": n_pages}


BACKFILL_PAGE_BUDGET = 40  # ~100 events/page (server clamp) ≈ 16s at 2.5 req/s
STATE_BACKFILL = "backfill_metadata"


async def backfill_metadata(client: PolymarketClient, store: HotStore) -> dict[str, int]:
    """One budgeted slice of the closed=true historical crawl (data invariants:
    the server default silently excludes every resolved market — this crawl
    is how resolutions/outcomes since Oct 2020 enter the store at all).

    Cursor-resumable via collector_state; a natural end records a done
    watermark and the job becomes a no-op. Minute price history for these
    markets is long pruned (data invariants) — this is metadata + final
    outcomePrices, which is exactly what resolution research needs. NEVER
    calls mark_unseen_markets_closed: enumeration here says nothing about
    what is currently open."""
    now_ts = int(time.time())
    state = store.get_state(VENUE_ID, STATE_BACKFILL)
    if state and state["watermark_ts"] and not state["cursor"]:
        return {"pages": 0, "events": 0, "done": 1}
    cursor = state["cursor"] if state else None

    pages = n_events = n_markets = n_skipped = 0
    finished = False
    async for events, next_cursor in client.iter_events_keyset(
        active=None, closed=True, cursor=cursor
    ):
        e, m, _i, s = _upsert_event_page(store, events, now_ts)
        n_events += e
        n_markets += m
        n_skipped += s
        pages += 1
        store.set_state(
            VENUE_ID, STATE_BACKFILL, cursor=next_cursor,
            watermark_ts=now_ts if not next_cursor else None,
            now_ts=int(time.time()),
        )
        if not next_cursor:
            finished = True
            break
        if pages >= BACKFILL_PAGE_BUDGET:
            break
    logger.info(
        "polymarket closed=true backfill: %d pages, %d events, %d markets, %d skipped%s",
        pages, n_events, n_markets, n_skipped,
        " — CRAWL COMPLETE" if finished else " (budget hit — next cycle continues)",
    )
    return {"pages": pages, "events": n_events, "markets": n_markets, "done": int(finished)}


# --------------------------------------------------------------------------
# Recently-closed markets recrawl (resolutions capture)
# --------------------------------------------------------------------------

CLOSED_RECRAWL_WINDOW_S = 4 * 86400  # cover weekend sleep gaps
_CLOSED_RECRAWL_PAGE = 100
_CLOSED_RECRAWL_OFFSET_CAP = 2000  # Observed Gamma limit/offset ceiling
STATE_CLOSED_RECRAWL = "closed_recrawl"
# Slice width for the endDate walk. Polymarket closes ~14k markets/DAY (5-min
# crypto up/down churn), so a day-wide slice buries its 2000-row budget in the
# newest few hours; hourly slices ran ~600 markets each in the 2026-07-31
# measurement, comfortably inside the ceiling. Any slice that still caps is
# named in the warning below.
_CLOSED_RECRAWL_SLICE_S = 3600


async def capture_closed_markets(
    client: PolymarketClient, store: HotStore
) -> dict[str, int]:
    """Capture final outcomePrices for markets that ENDED recently.

    The 3h metadata sweep enumerates ACTIVE events only, so a market whose
    whole event leaves enumeration is never re-captured after resolving —
    its final 1/0 prices never reach raw, pm_resolutions never gains it,
    and the decision grader can never grade it (found live 2026-07-19).
    This recrawl runs before each resolutions_refresh; the captures land in
    raw ``gamma_markets_closed`` (data invariants) and the extractor reads
    them alongside gamma_events. ``store`` is read for the ledger's own
    closed markets (targeted pass) and carries this job's own watermark;
    it writes no market rows."""
    import time as _time

    now = _time.time()

    def _stamp(epoch: float) -> str:
        return _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime(epoch))

    # Walk the window in fixed slices, newest first, each slice getting its own
    # offset budget. Three semantics live-probed 2026-07-31, all of which the
    # previous single min-only walk got wrong — it captured ~nothing useful:
    #   1. end_date_min is an unbounded LOWER bound and ascending=false sorts
    #      newest-endDate-first, so every row it paged was a long-dated closed
    #      market (endDate 2028-01-01); the recent closures this job exists for
    #      sat far beyond the 2000 ceiling. end_date_max bounds the top.
    #   2. A BARE DATE parses as T00:00:00Z, so end_date_max='2026-07-31' keeps
    #      only markets ending at exactly midnight — full timestamps required.
    #   3. end_date_min == end_date_max is a 422 ('invalid time range'), so a
    #      slice must span real time, never a single instant.
    # Slices run to one slice AHEAD of now: a resolved market can carry an
    # endDate slightly in the future (35 such rows measured).
    # Only walk back to the last successful pass (with one slice of overlap),
    # falling back to the full window on cold start or after a sleep gap.
    # The window is the BACKSTOP, not the steady-state cost: at the healthy 6h
    # cadence this is ~7 slices, and a full-window pass captures ~58k markets
    # / 29MB of raw — re-crawling 4 days every 6h would bank ~42GB/yr of the
    # same churn markets over and over.
    state = store.get_state(VENUE_ID, STATE_CLOSED_RECRAWL)
    since = state["watermark_ts"] if state else None
    lookback = CLOSED_RECRAWL_WINDOW_S
    if since is not None:
        lookback = min(
            CLOSED_RECRAWL_WINDOW_S,
            max(_CLOSED_RECRAWL_SLICE_S, now - since + _CLOSED_RECRAWL_SLICE_S),
        )

    n_markets = 0
    pages = 0
    capped: list[str] = []
    n_slices = int(lookback // _CLOSED_RECRAWL_SLICE_S)
    for i in range(-1, n_slices):
        hi = now - i * _CLOSED_RECRAWL_SLICE_S
        lo_s, hi_s = _stamp(hi - _CLOSED_RECRAWL_SLICE_S), _stamp(hi)
        offset = 0
        while offset < _CLOSED_RECRAWL_OFFSET_CAP:
            batch = await client.get_recently_closed_markets(
                lo_s,
                end_date_max=hi_s,
                limit=_CLOSED_RECRAWL_PAGE,
                offset=offset,
            )
            pages += 1
            n_markets += len(batch)
            if len(batch) < _CLOSED_RECRAWL_PAGE:
                break
            offset += len(batch)
        else:
            capped.append(lo_s)
    if capped:
        logger.warning(
            "closed-markets recrawl hit the %d offset cap on %d slice(s) "
            "(oldest %s) — those slices' earliest closures are missing this "
            "pass; narrow _CLOSED_RECRAWL_SLICE_S or add a volume floor",
            _CLOSED_RECRAWL_OFFSET_CAP, len(capped), capped[-1],
        )
    # Targeted pass: the decision ledger's own closed markets, by
    # condition_ids — immune to the offset cap (Friday's decided markets sat
    # beyond 2000 rows of weekend closures; found live 2026-07-19). The set
    # is dozens of ids — one or two extra requests. POLYMARKET ONLY: the
    # ledger went dual-venue 2026-07-21 and Kalshi tickers were being posted
    # to Gamma, which silently returns nothing for them (116 of 830 ids on
    # 2026-07-31). Kalshi finals arrive via collect_settlements instead.
    ledger_cids = [
        r[0] for r in store.conn.execute(
            """SELECT DISTINCT d.venue_market_id FROM decisions d
               JOIN markets m ON m.venue_id = d.venue_id
                             AND m.venue_market_id = d.venue_market_id
               WHERE d.venue_id = 'polymarket'
                 AND d.passed_gates = 1 AND m.close_ts < ?""",
            (int(now),),
        )
    ]
    n_targeted = 0
    for start in range(0, len(ledger_cids), 20):
        batch = await client.get_markets_by_condition_ids(ledger_cids[start:start + 20])
        n_targeted += len(batch)
    # Advance only after BOTH passes succeeded — an exception anywhere above
    # leaves the watermark where it was, so the next pass re-walks the gap
    # instead of skipping the closures it missed.
    store.set_state(
        VENUE_ID, STATE_CLOSED_RECRAWL, watermark_ts=int(now), now_ts=int(now)
    )
    logger.info(
        "closed-markets recrawl: %d markets ended since %s (%d pages, "
        "%d slices, %d capped), %d/%d ledger markets targeted",
        n_markets, _stamp(now - lookback), pages,
        n_slices + 1, len(capped), n_targeted, len(ledger_cids),
    )
    return {
        "closed_markets": n_markets, "pages": pages,
        "ledger_targeted": n_targeted,
    }
