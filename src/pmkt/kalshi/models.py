"""Kalshi response models (post 2026-03-12 fixed-point migration).

All prices arrive as dollar STRINGS (`yes_bid_dollars: "0.4200"`) and counts
as `*_fp` strings ("10.00") — parsed to Decimal, never float (data invariants).
A $0–1 dollar price on a 1-unit-payout contract IS the 0–1 probability, so no
further normalization is needed beyond Decimal parsing (data invariants).

Models are tolerant (`extra="allow"`): the raw lake keeps every field; these
classes only lift what the curated projection needs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

VENUE_ID = "kalshi"


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def to_unix_ts(value: Any) -> int | None:
    """RFC3339 string or unix seconds → unix seconds (UTC)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


class _Tolerant(BaseModel):
    model_config = ConfigDict(extra="allow")


class Series(_Tolerant):
    ticker: str
    title: str | None = None
    category: str | None = None
    tags: list[str] | None = None
    fee_type: str | None = None
    fee_multiplier: Any = None
    settlement_sources: Any = None


class Event(_Tolerant):
    event_ticker: str
    series_ticker: str | None = None
    title: str | None = None
    mutually_exclusive: bool | None = None
    category: str | None = None


# Kalshi's market-object status enum differs from the /markets?status= query
# param (query "open" ↔ object "active" — live-verified 2026-07-10). Map to
# the venue-agnostic status_norm; unknown values pass through untouched.
STATUS_NORM_MAP = {
    "active": "open",
    "initialized": "unopened",
    "determined": "closed",  # outcome known, not yet settled
    "finalized": "settled",
}


class Market(_Tolerant):
    ticker: str
    event_ticker: str | None = None
    market_type: str | None = None
    title: str | None = None
    status: str | None = None
    open_time: str | None = None
    close_time: str | None = None
    result: str | None = None
    yes_bid_dollars: Decimal | None = None
    yes_ask_dollars: Decimal | None = None
    no_bid_dollars: Decimal | None = None
    no_ask_dollars: Decimal | None = None
    last_price_dollars: Decimal | None = None
    liquidity_dollars: Decimal | None = None
    volume_fp: Decimal | None = None
    volume_24h_fp: Decimal | None = None
    open_interest_fp: Decimal | None = None

    _dec = field_validator(
        "yes_bid_dollars",
        "yes_ask_dollars",
        "no_bid_dollars",
        "no_ask_dollars",
        "last_price_dollars",
        "liquidity_dollars",
        "volume_fp",
        "volume_24h_fp",
        "open_interest_fp",
        mode="before",
    )(to_decimal)

    def hot_row(self) -> dict[str, Any]:
        """Project into the HotStore markets-table shape."""
        return {
            "venue_market_id": self.ticker,
            "venue_event_id": self.event_ticker,
            # Authoritative series comes from the events map; ticker prefix
            # ("KXHIGHNY-26JUL10-B55" → "KXHIGHNY") is the fallback.
            "venue_series_id": (self.event_ticker or self.ticker).split("-")[0],
            "title": self.title,
            "market_type": self.market_type,
            "status_norm": STATUS_NORM_MAP.get(self.status or "", self.status),
            "open_ts": to_unix_ts(self.open_time),
            "close_ts": to_unix_ts(self.close_time),
            "yes_bid_prob": self.yes_bid_dollars,
            "yes_ask_prob": self.yes_ask_dollars,
            "last_prob": self.last_price_dollars,
            "volume": self.volume_fp,
            "volume_24h": self.volume_24h_fp,
            "open_interest": self.open_interest_fp,
            "liquidity": self.liquidity_dollars,
            "result": self.result or None,
        }


class Trade(_Tolerant):
    trade_id: str
    ticker: str
    count_fp: Decimal | None = None
    yes_price_dollars: Decimal | None = None
    no_price_dollars: Decimal | None = None
    taker_outcome_side: str | None = None
    taker_book_side: str | None = None
    is_block_trade: bool | None = None
    created_time: str | None = None

    _dec = field_validator(
        "count_fp", "yes_price_dollars", "no_price_dollars", mode="before"
    )(to_decimal)

    @property
    def created_ts(self) -> int | None:
        return to_unix_ts(self.created_time)


import logging

_logger = logging.getLogger(__name__)


def _ohlc_value(side: dict[str, Any] | None, key: str) -> Decimal | None:
    """Read an OHLC component tolerantly: `close_dollars` (live-verified
    2026-07-11) or bare `close` (pre-migration shape). Guard the 0–1 range:
    if the bare key ever carried legacy integer CENTS, passing it through
    would store e.g. probability "42" (data invariants) — refuse instead."""
    if not isinstance(side, dict):
        return None
    value = to_decimal(side.get(f"{key}_dollars", side.get(key)))
    if value is not None and not (0 <= value <= 1):
        _logger.warning("OHLC %s=%s outside 0-1 — dropping (cents-shaped payload?)", key, value)
        return None
    return value


class Candlestick(_Tolerant):
    """One candle from /markets/candlesticks or the per-series endpoint.

    Sub-objects: yes_bid / yes_ask OHLC always present; `price` (trade OHLC +
    mean/VWAP) nullable when the period had no trades.
    """

    end_period_ts: int
    yes_bid: dict[str, Any] | None = None
    yes_ask: dict[str, Any] | None = None
    price: dict[str, Any] | None = None
    volume_fp: Decimal | None = None
    open_interest_fp: Decimal | None = None

    _dec = field_validator("volume_fp", "open_interest_fp", mode="before")(to_decimal)

    @property
    def yes_bid_close(self) -> Decimal | None:
        return _ohlc_value(self.yes_bid, "close")

    @property
    def yes_ask_close(self) -> Decimal | None:
        return _ohlc_value(self.yes_ask, "close")

    @property
    def trade_close(self) -> Decimal | None:
        return _ohlc_value(self.price, "close")

    @property
    def trade_mean(self) -> Decimal | None:
        return _ohlc_value(self.price, "mean")


class MarketCandles(BaseModel):
    """Candles for one market out of a batch response."""

    ticker: str
    candlesticks: list[Candlestick] = Field(default_factory=list)
