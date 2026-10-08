"""Kalshi public REST client (Trade API v2, no auth required).

Every response — including 429s and errors — is appended to the raw lake
BEFORE parsing (data invariants). All requests flow through a token bucket
(data invariants); 429s honor Retry-After and back off exponentially.

Auth (RSA-PSS headers) is only needed for WebSocket/portfolio and is a later
phase — this client covers the full public collection surface.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, AsyncIterator
from urllib.parse import urlparse

import httpx

from ..ratelimit import TokenBucket
from ..rawlake import RawLake
from .auth import KalshiSigner
from .models import VENUE_ID, Candlestick, Event, Market, MarketCandles, Series, Trade

logger = logging.getLogger(__name__)

MAX_MARKETS_LIMIT = 1000
MAX_EVENTS_LIMIT = 200
MAX_TRADES_LIMIT = 1000
MAX_BATCH_CANDLE_TICKERS = 100
CANDLE_INTERVALS_MIN = (1, 60, 1440)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
MAX_RETRY_DELAY_S = 60.0
# Documented cursor contract: empty/null cursor = done. An empty PAGE with a
# live cursor is not termination — but cap consecutive empties defensively.
MAX_CONSECUTIVE_EMPTY_PAGES = 3


def _retry_delay(retry_after: str | None, attempt: int) -> float:
    """Retry-After per RFC 9110 is delta-seconds OR an HTTP-date; anything
    unparseable falls back to exponential backoff. Server values are clamped."""
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


class KalshiAPIError(Exception):
    def __init__(self, status: int, path: str, body: Any) -> None:
        super().__init__(f"Kalshi API {status} on {path}: {str(body)[:300]}")
        self.status = status


class KalshiPublicClient:
    def __init__(
        self,
        base_url: str,
        lake: RawLake,
        bucket: TokenBucket,
        timeout_s: float = 30.0,
        signer: KalshiSigner | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._lake = lake
        self._bucket = bucket
        # Sign every request when a key is configured: authenticated calls
        # draw from the account token bucket (Basic ≥ 20 reads/s) instead of
        # the tight undocumented unauthenticated budget.
        self._signer = signer
        # Signing path must include the /trade-api/v2 prefix, no query params.
        self._sign_prefix = urlparse(self._base_url).path
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=timeout_s,
            headers={"User-Agent": "pmkt-collector/0.1 (personal research)"},
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "KalshiPublicClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # -- transport -----------------------------------------------------------

    async def _get(self, endpoint: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = {k: v for k, v in (params or {}).items() if v is not None}
        last_error: Exception | None = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            await self._bucket.acquire()
            await self._lake.wait_for_capture()
            headers = (
                self._signer.headers("GET", self._sign_prefix + path)
                if self._signer
                else None
            )
            started = time.monotonic()
            try:
                response = await self._http.get(path, params=params, headers=headers)
            except httpx.TransportError as exc:
                last_error = exc
                logger.warning("transport error on %s (attempt %d): %s", path, attempt, exc)
                await asyncio.sleep(min(2**attempt, 30))
                continue
            elapsed_ms = int((time.monotonic() - started) * 1000)

            try:
                payload: Any = response.json()
            except ValueError:
                payload = {"_non_json_body": response.text[:10_000]}

            # Capture before parsing — errors included.
            await self._lake.append_when_ready(
                venue=VENUE_ID,
                endpoint=endpoint,
                params=params,
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
                    response.status_code, path, attempt, delay,
                )
                await asyncio.sleep(delay)
                continue
            raise KalshiAPIError(response.status_code, path, payload)
        raise KalshiAPIError(0, path, f"exhausted retries: {last_error}")

    # -- metadata ------------------------------------------------------------

    async def get_series_catalog(self) -> list[Series]:
        """Full series catalog — single request, no pagination."""
        payload = await self._get("series", "/series")
        return [Series.model_validate(s) for s in payload.get("series") or []]

    async def iter_events(
        self, status: str | None = None, cursor: str | None = None
    ) -> AsyncIterator[tuple[list[Event], str | None]]:
        empty_pages = 0
        while True:
            payload = await self._get(
                "events",
                "/events",
                {"limit": MAX_EVENTS_LIMIT, "status": status, "cursor": cursor},
            )
            events = [Event.model_validate(e) for e in payload.get("events") or []]
            cursor = payload.get("cursor") or None
            yield events, cursor
            if not self._more_pages("events", cursor, len(events), empty_pages):
                return
            empty_pages = 0 if events else empty_pages + 1

    async def iter_markets(
        self,
        status: str | None = None,
        mve_filter: str | None = "exclude",  # parlay markets pollute the universe
        cursor: str | None = None,
        min_settled_ts: int | None = None,
        max_settled_ts: int | None = None,
    ) -> AsyncIterator[tuple[list[Market], str | None]]:
        empty_pages = 0
        while True:
            payload = await self._get(
                "markets",
                "/markets",
                {
                    "limit": MAX_MARKETS_LIMIT,
                    "status": status,
                    "mve_filter": mve_filter,
                    "cursor": cursor,
                    "min_settled_ts": min_settled_ts,
                    "max_settled_ts": max_settled_ts,
                },
            )
            markets = [Market.model_validate(m) for m in payload.get("markets") or []]
            cursor = payload.get("cursor") or None
            yield markets, cursor
            if not self._more_pages("markets", cursor, len(markets), empty_pages):
                return
            empty_pages = 0 if markets else empty_pages + 1

    # -- prices --------------------------------------------------------------

    async def get_batch_candlesticks(
        self,
        tickers: list[str],
        start_ts: int,
        end_ts: int,
        period_interval: int = 1,
    ) -> list[MarketCandles]:
        """Batch candles — the sweep workhorse. ≤100 tickers per request,
        ≤10,000 candles per response, unix-second timestamps."""
        if len(tickers) > MAX_BATCH_CANDLE_TICKERS:
            raise ValueError(f"max {MAX_BATCH_CANDLE_TICKERS} tickers per batch request")
        if period_interval not in CANDLE_INTERVALS_MIN:
            raise ValueError(f"period_interval must be one of {CANDLE_INTERVALS_MIN}")
        payload = await self._get(
            "markets_candlesticks",
            "/markets/candlesticks",
            {
                "market_tickers": ",".join(tickers),
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
            },
        )
        return _parse_batch_candles(payload)

    # -- trades ----------------------------------------------------------------

    async def iter_trades(
        self,
        min_ts: int | None = None,
        max_ts: int | None = None,
        cursor: str | None = None,
    ) -> AsyncIterator[tuple[list[Trade], str | None]]:
        empty_pages = 0
        while True:
            payload = await self._get(
                "trades",
                "/markets/trades",
                {
                    "limit": MAX_TRADES_LIMIT,
                    "min_ts": min_ts,
                    "max_ts": max_ts,
                    "cursor": cursor,
                },
            )
            trades = [Trade.model_validate(t) for t in payload.get("trades") or []]
            cursor = payload.get("cursor") or None
            yield trades, cursor
            if not self._more_pages("trades", cursor, len(trades), empty_pages):
                return
            empty_pages = 0 if trades else empty_pages + 1

    @staticmethod
    def _more_pages(what: str, cursor: str | None, n_items: int, empty_pages: int) -> bool:
        """Documented contract: empty/null cursor = done. An empty page with a
        live cursor mid-stream is NOT termination (stopping there would silently
        truncate enumeration — and the metadata sweep would then mass-close
        every market after the truncation point). Cap consecutive empties so a
        pathological cursor can't loop forever."""
        if not cursor:
            return False
        if n_items == 0 and empty_pages + 1 >= MAX_CONSECUTIVE_EMPTY_PAGES:
            logger.warning(
                "%s pagination: %d consecutive empty pages with a live cursor — stopping",
                what, empty_pages + 1,
            )
            return False
        return True

    # -- misc public ----------------------------------------------------------

    async def iter_historical_markets(
        self,
        mve_filter: str | None = "exclude",
        cursor: str | None = None,
    ) -> AsyncIterator[tuple[list[Market], str | None]]:
        """Settled/archived markets from the historical tier (data invariants) —
        same object shape as live /markets, incl. `result`. No auth required;
        cursor resumes a previous crawl."""
        empty_pages = 0
        while True:
            payload = await self._get(
                "historical_markets",
                "/historical/markets",
                {"limit": MAX_MARKETS_LIMIT, "mve_filter": mve_filter, "cursor": cursor},
            )
            markets = [Market.model_validate(m) for m in payload.get("markets") or []]
            cursor = payload.get("cursor") or None
            yield markets, cursor
            if not cursor:
                return
            empty_pages = 0 if markets else empty_pages + 1
            if empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES:
                logger.warning("historical markets: %d consecutive empty pages — stopping", empty_pages)
                return

    async def iter_historical_trades(
        self,
        min_ts: int | None = None,
        max_ts: int | None = None,
        cursor: str | None = None,
    ) -> AsyncIterator[tuple[list[Trade], str | None]]:
        """Full public trade tape older than the live/historical cutoff.
        Same fields as /markets/trades (maker/taker labeling intact)."""
        empty_pages = 0
        while True:
            payload = await self._get(
                "historical_trades",
                "/historical/trades",
                {
                    "limit": MAX_TRADES_LIMIT,
                    "min_ts": min_ts,
                    "max_ts": max_ts,
                    "cursor": cursor,
                },
            )
            trades = [Trade.model_validate(t) for t in payload.get("trades") or []]
            cursor = payload.get("cursor") or None
            yield trades, cursor
            if not cursor:
                return
            empty_pages = 0 if trades else empty_pages + 1
            if empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES:
                logger.warning("historical trades: %d consecutive empty pages — stopping", empty_pages)
                return

    async def get_orderbook(self, ticker: str) -> dict[str, Any]:
        """Public orderbook (bids only — asks derived as 1 − opposite bid)."""
        return await self._get("orderbook", f"/markets/{ticker}/orderbook")

    async def get_historical_cutoff(self) -> dict[str, Any]:
        """Partition timestamps for routing backfills to /historical/* (data invariants)."""
        return await self._get("historical_cutoff", "/historical/cutoff")

    async def get_api_limits(self) -> dict[str, Any]:
        """Authenticated: current API usage tier + token-bucket budgets.
        Doubles as the auth smoke test (read-only account metadata)."""
        return await self._get("api_limits", "/account/limits")


def _parse_batch_candles(payload: dict[str, Any]) -> list[MarketCandles]:
    """Parse the batch response. Live-verified shape (2026-07-10):
    {"markets": [{"market_ticker": "...", "candlesticks": [...]}]}.
    Fallback keys stay for schema drift, but a key only counts as a match if
    its entries actually look like per-market objects — otherwise a wrong
    shape (e.g. the single-market endpoint's bare candle list) would silently
    return [] without the unrecognized-payload warning."""
    for key in ("markets", "markets_candlesticks", "market_candlesticks"):
        entries = payload.get(key)
        if not isinstance(entries, list):
            continue
        if not entries:
            return []
        out = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            ticker = entry.get("market_ticker") or entry.get("ticker")
            if ticker is None:
                continue
            candles = [
                Candlestick.model_validate(c) for c in entry.get("candlesticks") or []
            ]
            out.append(MarketCandles(ticker=ticker, candlesticks=candles))
        if out:
            return out
    logger.warning("unrecognized batch candlestick payload shape: %s", list(payload))
    return []
