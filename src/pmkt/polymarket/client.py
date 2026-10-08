"""Polymarket public read-only client — Gamma (metadata) + CLOB (prices).

Every response — including errors — is appended to the raw lake BEFORE
parsing (data invariants). All requests flow through one shared token bucket
(data invariants). Polymarket rate limiting is Cloudflare THROTTLING (requests
queue and slow down rather than clean-429ing), so the discipline is
prevention: conservative request rate, a hard 30s timeout, and strictly
sequential requests in v1 — no concurrency.

All reads are public; nothing here touches auth, wallets, or orders
(data invariants — if an endpoint needs a signature, it does not belong in
this client).
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any, AsyncIterator

import httpx

from ..ratelimit import TokenBucket
from ..rawlake import RawLake
from .models import VENUE_ID, DataAPITrade, GammaEvent, PricePoint, to_decimal

logger = logging.getLogger(__name__)

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
CLOB_BASE_URL = "https://clob.polymarket.com"
DATA_BASE_URL = "https://data-api.polymarket.com"

# Keyset caps: docs say events ≤500 / markets ≤100 (422 on offset=), but the
# server silently CLAMPS events to 100/page (live-measured 2026-07-11:
# limit=500 requested, 100 returned, every page). The request still asks for 500 —
# harmless today, free capacity if the clamp is ever lifted. Budget sweeps
# at ~100 events/request.
MAX_EVENTS_KEYSET_LIMIT = 500
MAX_MARKETS_KEYSET_LIMIT = 100
# batch-prices-history hard cap.
MAX_BATCH_PRICE_TOKENS = 20

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
MAX_RETRY_DELAY_S = 60.0
# An empty page with a live cursor is not termination (stopping would let a
# server hiccup silently truncate enumeration → mass false market closes),
# but cap consecutive empties so a pathological cursor can't loop forever.
MAX_CONSECUTIVE_EMPTY_PAGES = 3


def _retry_delay(retry_after: str | None, attempt: int) -> float:
    """Retry-After per RFC 9110 is delta-seconds OR an HTTP-date; anything
    unparseable falls back to exponential backoff. Server values are clamped.
    (Cloudflare rarely sends one — the fallback is the normal path here.)"""
    fallback = min(2.0**attempt, 30.0)
    if not retry_after:
        return fallback
    try:
        delay = float(retry_after)
    except ValueError:
        try:
            target = parsedate_to_datetime(retry_after)
            delay = (target - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError):
            return fallback
    return min(max(delay, 0.0), MAX_RETRY_DELAY_S)


class PolymarketAPIError(Exception):
    def __init__(self, status: int, url: str, body: Any) -> None:
        super().__init__(f"Polymarket API {status} on {url}: {str(body)[:300]}")
        self.status = status


class EnumerationTruncated(PolymarketAPIError):
    """Keyset enumeration stopped defensively before its natural end (cursor
    regression, stuck cursor, or an empty-page run). MUST propagate as an
    error, never a normal generator return: a truncated sweep that ended
    normally would let collect_metadata mass-close every market it never
    reached (~74k rows), emptying the price-sweep universe for hours —
    silent, unrecoverable minute-history loss (data invariants)."""

    def __init__(self, reason: str) -> None:
        Exception.__init__(self, f"keyset enumeration truncated: {reason}")
        self.status = 0


class PolymarketClient:
    def __init__(
        self,
        lake: RawLake,
        bucket: TokenBucket,
        gamma_base_url: str = GAMMA_BASE_URL,
        clob_base_url: str = CLOB_BASE_URL,
        data_base_url: str = DATA_BASE_URL,
        timeout_s: float = 30.0,
    ) -> None:
        self._gamma = gamma_base_url.rstrip("/")
        self._clob = clob_base_url.rstrip("/")
        self._data = data_base_url.rstrip("/")
        self._lake = lake
        self._bucket = bucket
        self._http = httpx.AsyncClient(
            timeout=timeout_s,
            headers={"User-Agent": "pmkt-collector/0.1 (personal research)"},
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "PolymarketClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # -- transport -----------------------------------------------------------

    async def _request(
        self,
        endpoint: str,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_body: Any = None,
    ) -> Any:
        params = {k: v for k, v in (params or {}).items() if v is not None}
        last_error: Exception | None = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            await self._bucket.acquire()
            await self._lake.wait_for_capture()
            started = time.monotonic()
            try:
                response = await self._http.request(
                    method, url, params=params or None, json=json_body
                )
            except httpx.TransportError as exc:
                # Cloudflare throttling shows up as slow/queued responses that
                # hit the hard timeout — back off and retry, don't hammer.
                last_error = exc
                logger.warning("transport error on %s (attempt %d): %s", url, attempt, exc)
                await asyncio.sleep(min(2**attempt, 30))
                continue
            elapsed_ms = int((time.monotonic() - started) * 1000)

            try:
                payload: Any = response.json()
            except ValueError:
                payload = {"_non_json_body": response.text[:10_000]}

            # Capture before parsing — errors included (data invariants).
            await self._lake.append_when_ready(
                venue=VENUE_ID,
                endpoint=endpoint,
                params={**params, "_body": json_body} if json_body else params,
                http_status=response.status_code,
                elapsed_ms=elapsed_ms,
                payload=payload,
            )

            if response.status_code == 200:
                return payload
            if response.status_code in RETRYABLE_STATUS and attempt < MAX_ATTEMPTS:
                delay = _retry_delay(response.headers.get("Retry-After"), attempt)
                logger.warning(
                    "HTTP %d on %s (attempt %d), retrying in %.1fs",
                    response.status_code, url, attempt, delay,
                )
                await asyncio.sleep(delay)
                continue
            raise PolymarketAPIError(response.status_code, url, payload)
        raise PolymarketAPIError(0, url, f"exhausted retries: {last_error}")

    # -- metadata (Gamma) ------------------------------------------------------

    async def iter_events_keyset(
        self,
        active: bool | None = True,
        closed: bool = False,
        limit: int = MAX_EVENTS_KEYSET_LIMIT,
        max_pages: int | None = None,
        cursor: str | None = None,
    ) -> AsyncIterator[tuple[list[GammaEvent], str | None]]:
        """Enumerate events (with embedded markets[]) via /events/keyset.

        Cursor contract (live-verified 2026-07-11): pass `after_cursor` — the
        EXACT param name matters, `cursor=` is silently ignored and returns
        page 1 forever. `next_cursor` is absent/empty on the last page.
        Two independent guards against the page-1 infinite loop:
        (1) a next_cursor identical to the cursor just sent stops the crawl;
        (2) a page whose event ids repeat the previous page's stops the crawl.
        `closed` must be passed explicitly on every call — the server default
        silently drops resolved markets (data invariants). `active=None` omits
        the flag (the closed=true backfill wants ALL events regardless).
        `cursor` resumes a previous crawl (budgeted backfill slices).
        """
        prev_ids: tuple[str, ...] | None = None
        pages = 0
        empty_pages = 0
        while True:
            payload = await self._request(
                "gamma_events",
                "GET",
                f"{self._gamma}/events/keyset",
                params={
                    "limit": limit,
                    "active": None if active is None else str(active).lower(),
                    "closed": str(closed).lower(),
                    "after_cursor": cursor,
                },
            )
            raw_events = payload.get("events") or [] if isinstance(payload, dict) else []
            events = [GammaEvent.model_validate(e) for e in raw_events]
            next_cursor = (payload.get("next_cursor") or None) if isinstance(payload, dict) else None
            page_ids = tuple(e.id for e in events)

            if prev_ids is not None and page_ids and page_ids == prev_ids:
                raise EnumerationTruncated(
                    "same page served twice (server-side cursor regression — "
                    "the live-verified `cursor=` trap shape)"
                )
            yield events, next_cursor

            pages += 1
            if max_pages is not None and pages >= max_pages:
                logger.info("events keyset: stopping at max_pages=%d", max_pages)
                return
            if not next_cursor:
                return  # natural end of enumeration
            if next_cursor == cursor:
                raise EnumerationTruncated("next_cursor did not advance")
            empty_pages = 0 if events else empty_pages + 1
            if empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES:
                raise EnumerationTruncated(
                    f"{empty_pages} consecutive empty pages with a live cursor"
                )
            prev_ids = page_ids or prev_ids
            cursor = next_cursor

    # -- prices (CLOB) ---------------------------------------------------------

    async def get_batch_prices_history(
        self,
        token_ids: list[str],
        start_ts: int,
        end_ts: int,
        fidelity: int = 1,
    ) -> dict[str, list[PricePoint]]:
        """POST /batch-prices-history — ≤20 token ids per call, snake_case
        unix-SECOND bounds, fidelity in minutes. Live-verified response shape
        (2026-07-11): {"history": {token_id: [{"t": s, "p": float}, ...]}}
        (docs sketch token→{"history": [...]} — both shapes are handled)."""
        if not token_ids:
            return {}
        if len(token_ids) > MAX_BATCH_PRICE_TOKENS:
            raise ValueError(f"max {MAX_BATCH_PRICE_TOKENS} token ids per batch request")
        payload = await self._request(
            "clob_prices_history",
            "POST",
            f"{self._clob}/batch-prices-history",
            json_body={
                "markets": token_ids,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "fidelity": fidelity,
            },
        )
        return _parse_batch_prices(payload)


    # -- trades (Data API) ----------------------------------------------------

    async def get_books(
        self, token_ids: list[str], *, endpoint: str = "clob_books"
    ) -> dict[str, dict[str, Any]]:
        """POST /books — batch order-book snapshot (≤ 20 token ids).

        Returns {token_id: {bid, ask, bid_size, ask_size}} with Decimal
        prices (data invariants). Best bid/ask are the LAST elements of the
        arrays (py-clob-client issue #180 — production returns best-at-END).
        Raw capture via _request (data invariants); the full depth ladder
        lives in the raw record for later fillability analysis."""
        payload = await self._request(
            endpoint,
            "POST",
            f"{self._clob}/books",
            json_body=[{"token_id": t} for t in token_ids],
        )
        books: dict[str, dict[str, Any]] = {}
        if not isinstance(payload, list):
            return books
        for book in payload:
            if not isinstance(book, dict):
                continue
            tok = book.get("asset_id")
            if not tok:
                continue

            def _best(levels: Any) -> tuple[Decimal | None, Decimal | None]:
                if not isinstance(levels, list) or not levels:
                    return None, None
                lvl = levels[-1]  # best at END
                try:
                    return (
                        to_decimal(lvl.get("price")),
                        to_decimal(lvl.get("size")),
                    )
                except (AttributeError, ArithmeticError, TypeError, ValueError):
                    return None, None

            bid, bid_size = _best(book.get("bids"))
            ask, ask_size = _best(book.get("asks"))
            books[str(tok)] = {
                "bid": bid, "ask": ask,
                "bid_size": bid_size, "ask_size": ask_size,
            }
        return books

    async def get_trades(
        self,
        condition_id: str,
        *,
        limit: int = 1000,
        offset: int = 0,
        endpoint: str = "data_api_trades",
    ) -> list[DataAPITrade]:
        """GET /trades from the Data API — public wallet-attributed trade tape.

        Per-market crawl via ``market=conditionId``. The global feed is useless
        (86% churn, 3-min reach in a July 2026 probe, no time-windowing). Limit
        effectively caps at ~1000; offset pages cleanly up to 10,000.
        ``takerOnly`` defaults to true on the API (maker rows hidden); for
        forward skilled-OFI only uses the aggressor wallet.

        ``endpoint`` names the raw-lake partition for the capture: the bridge
        backfill passes its own so its bulk historical pages never sit in the
        partitions the live OFI scorer scans (fat 1MB pages OOM'd it)."""
        payload = await self._request(
            endpoint,
            "GET",
            f"{self._data}/trades",
            params={
                "market": condition_id,
                "limit": limit,
                "offset": offset,
            },
        )
        return _parse_trades(payload)

    async def get_recently_closed_markets(
        self,
        end_date_min: str,
        *,
        end_date_max: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Any]:
        """GET Gamma /markets?closed=true for markets that ENDED recently.

        Fills the resolutions capture gap: the 3h metadata sweep enumerates
        ACTIVE events only, so a market whose whole event closes is never
        re-captured with its final 1/0 outcomePrices — pm_resolutions can
        never grade it (found live 2026-07-19: Friday's decided markets were
        absent two days after resolving). Pass closed=true explicitly;
        the observed limit/offset cap is approximately 2000.

        ``end_date_max`` is REQUIRED in practice, not optional decoration:
        ``end_date_min`` is an unbounded lower bound and ``ascending=false``
        sorts newest-endDate-first, so a min-only call pages long-dated
        closed markets (live-probed 2026-07-31: every row endDate
        2028-01-01) and never reaches the recent closures this exists for.
        Both bounds are date-INCLUSIVE, but ``min == max`` returns empty —
        pass a range, not a single day (also live-probed).

        Returns the RAW market dicts (already raw-captured by _request under
        ``gamma_markets_closed``); the resolutions extractor is the consumer."""
        params: dict[str, Any] = {
            "closed": "true",
            "end_date_min": end_date_min,
            "order": "endDate",
            "ascending": "false",
            "limit": limit,
            "offset": offset,
        }
        if end_date_max is not None:
            params["end_date_max"] = end_date_max
        payload = await self._request(
            "gamma_markets_closed", "GET", f"{self._gamma}/markets", params=params
        )
        return payload if isinstance(payload, list) else []

    async def get_markets_by_condition_ids(
        self, condition_ids: list[str]
    ) -> list[Any]:
        """GET Gamma /markets for SPECIFIC markets via repeated
        ``condition_ids`` params (dict-of-list — httpx expands; a
        comma-joined string returns 0 rows, live-verified 2026-07-19).
        Used by the closed-markets recrawl to guarantee the decision
        ledger's own markets are captured even when the windowed recrawl
        hits the ~2000 offset cap. ≤20 ids per call."""
        payload = await self._request(
            "gamma_markets_closed",
            "GET",
            f"{self._gamma}/markets",
            params={
                "condition_ids": condition_ids,
                "closed": "true",
                "limit": 100,
            },
        )
        return payload if isinstance(payload, list) else []


def _parse_trades(payload: Any) -> list[DataAPITrade]:
    """Parse Data-API /trades response. The response is a JSON array of flat
    trade objects. One malformed row is dropped with a warning, not fatal
    (resilient parsing: one bad row must not kill a sweep)."""
    trades_list: list | None = None
    if isinstance(payload, list):
        trades_list = payload
    elif isinstance(payload, dict):
        # Defensive: handle a wrapper if the API ever adds one.
        for key in ("data", "trades"):
            if isinstance(payload.get(key), list):
                trades_list = payload[key]
                break
        if trades_list is None and not payload:
            return []
    if trades_list is None:
        if payload:
            logger.warning(
                "unrecognized data-api/trades payload shape: %s",
                type(payload).__name__,
            )
        return []
    out: list[DataAPITrade] = []
    for item in trades_list:
        try:
            out.append(DataAPITrade.model_validate(item))
        except Exception:
            logger.warning("dropping malformed trade: %r", str(item)[:200])
    return out


def _parse_batch_prices(payload: Any) -> dict[str, list[PricePoint]]:
    """token_id → sorted price points. Tolerates both the live shape
    {"history": {token: [pts]}} and the documented {token: {"history": [pts]}};
    one malformed point is dropped with a warning, not fatal."""
    mapping: dict[str, Any] | None = None
    if isinstance(payload, dict):
        history = payload.get("history")
        if isinstance(history, dict):
            mapping = history
        else:
            alt = {
                k: v.get("history")
                for k, v in payload.items()
                if isinstance(v, dict) and isinstance(v.get("history"), list)
            }
            if alt:
                mapping = alt
    if mapping is None:
        if payload:
            logger.warning(
                "unrecognized batch-prices-history payload shape: %s",
                list(payload)[:5] if isinstance(payload, dict) else type(payload).__name__,
            )
        return {}
    out: dict[str, list[PricePoint]] = {}
    for token_id, points in mapping.items():
        parsed: list[PricePoint] = []
        for point in points or []:
            try:
                parsed.append(PricePoint.model_validate(point))
            except Exception:  # one bad point must not kill the sweep
                logger.warning("dropping malformed price point for %s: %r", token_id, point)
        parsed.sort(key=lambda pt: pt.t)
        out[str(token_id)] = parsed
    return out
