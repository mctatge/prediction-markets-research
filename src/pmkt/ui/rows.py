"""ScreenerRow shaping — hot-store row → API-contract dict.

data invariants: every price/size field in the payload is a decimal STRING. The
stored TEXT columns are returned verbatim (byte-equal to what the collector
wrote); the two derived fields (mid, spread) are computed with ``Decimal``
from those strings. Nothing ever passes through float on its way into a
response — SQL CASTs exist only for filtering/sorting and are never selected.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

KALSHI_HOME = "https://kalshi.com"
POLYMARKET_HOME = "https://polymarket.com"


def _dec(text: str | None) -> Decimal | None:
    if text is None or text == "":
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _fmt(value: Decimal) -> str:
    return format(value, "f")


def _requantize(value: Decimal, like: Decimal) -> Decimal:
    """Restore trailing zeros lost to Decimal division when exact.

    (0.4200 + 0.4400) / 2 == Decimal("0.43"); the contract's examples carry
    the venue scale ("0.4300"). Quantize back to the operands' scale only
    when it changes nothing — an inexact quantize is never taken.
    """
    try:
        q = value.quantize(Decimal(1).scaleb(like.as_tuple().exponent))
    except (InvalidOperation, ValueError):
        return value
    return q if q == value else value


def mid_spread(bid_text: str | None, ask_text: str | None) -> tuple[str | None, str | None]:
    """(mid, spread) decimal strings from the stored TEXT bid/ask.

    Both are defined only when BOTH sides are quoted (contract: null =
    unknown). The price-band FILTER falls back to last — the row's ``mid``
    field does not (the frontend has ``last`` separately).
    """
    bid, ask = _dec(bid_text), _dec(ask_text)
    if bid is None or ask is None:
        return None, None
    total = bid + ask
    mid = _requantize(total / 2, total)
    return _fmt(mid), _fmt(ask - bid)


def market_url(
    venue: str,
    series_id: str | None,
    slug: str | None,
    venue_series_slug: str | None,
) -> str:
    """Venue page URL per the contract.

    kalshi   → https://kalshi.com/markets/{series_id}
    polymarket → /market/{slug}; the hot store does not persist the EVENT
    slug, so the contract's "event slug fallback" uses the stored
    series/event slug (markets.venue_series_id — Gamma's seriesSlug, which
    resolves at polymarket.com/event/…); with neither, the bare homepage.
    """
    if venue == "kalshi":
        return f"{KALSHI_HOME}/markets/{series_id}" if series_id else KALSHI_HOME
    if slug:
        return f"{POLYMARKET_HOME}/market/{slug}"
    if venue_series_slug:
        return f"{POLYMARKET_HOME}/event/{venue_series_slug}"
    return POLYMARKET_HOME


_EDGE_MAX_SPREAD = Decimal("0.20")  # mirror model.score.MAX_BOOK_SPREAD


def _edge(
    model_p: str | None, mid: str | None, spread: str | None, last: str | None
) -> str | None:
    """edge = model_p − base, where base follows the SAME rule scoring uses:
    mid only when the book is real (spread ≤ 0.20), else last trade. A wide/
    empty book's mid≈0.5 would manufacture fake edge (observed live
    2026-07-12: 0.00/0.98 index ladders topped the edge sort). 4-place
    signed string."""
    if model_p is None:
        return None
    base = None
    if mid is not None and spread is not None and _dec(spread) <= _EDGE_MAX_SPREAD:
        base = _dec(mid)
    if base is None:
        base = _dec(last)
    if base is None:
        return None
    edge = Decimal(model_p) - base
    return format(edge.quantize(Decimal("0.0001")), "f")


def shape_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """One hot-store markets row (+ joined ``category`` and, when the query
    provides them, forecast columns) → ScreenerRow."""
    keys = row.keys()
    model_p = row["model_p"] if "model_p" in keys else None
    skilled_ofi = row["skilled_ofi"] if "skilled_ofi" in keys else None
    n_skilled = row["n_skilled_trades"] if "n_skilled_trades" in keys else None
    exec_edge = row["exec_edge"] if "exec_edge" in keys else None
    mid, spread = mid_spread(row["yes_bid_prob"], row["yes_ask_prob"])
    return {
        "venue": row["venue_id"],
        "market_id": row["venue_market_id"],
        "event_id": row["venue_event_id"],
        "series_id": row["venue_series_id"],
        "title": row["title"],
        "category": row["category"],
        "status": row["status_norm"],
        "yes_bid": row["yes_bid_prob"],
        "yes_ask": row["yes_ask_prob"],
        "last": row["last_prob"],
        "mid": mid,
        "spread": spread,
        "volume": row["volume"],
        "volume_24h": row["volume_24h"],
        "open_interest": row["open_interest"],
        "liquidity": row["liquidity"],
        "close_ts": row["close_ts"],
        "prices_updated_ts": row["prices_updated_ts"],
        "recurrence": row["recurrence"],
        "slug": row["slug"],
        "model_p": model_p,  # decimal string when a live model covers this market
        "edge": _edge(model_p, mid, spread, row["last_prob"]),
        "exec_edge": exec_edge,  # fee-adjusted: edge minus spread crossing + taker fee
        "skilled_ofi": skilled_ofi,  # signed OFI from skilled wallets, PM-only
        "n_skilled_trades": n_skilled,
        "url": market_url(
            row["venue_id"], row["venue_series_id"], row["slug"], row["venue_series_id"]
        ),
    }


def row_mid_key(shaped: Mapping[str, Any]) -> Decimal | None:
    """Sort key for sibling ordering (mid desc, nulls last) — Decimal only."""
    return _dec(shaped["mid"])
