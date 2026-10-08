"""Polymarket Gamma/CLOB response models (live-verified 2026-07-11).

The traps these models exist to absorb:
- Gamma returns `outcomes`, `outcomePrices`, `clobTokenIds` as JSON-ENCODED
  STRINGS, not arrays (data invariants) — decoded defensively in validators
  (they may occasionally arrive as real arrays; both shapes are handled).
- Prices are venue-native 0–1 FLOATS — converted to Decimal via
  Decimal(str(x)) at the model boundary; nothing downstream ever sees a
  Python float (data invariants).
- The ID zoo (data invariants): one market carries a Gamma numeric `id`, a
  `slug`, a `conditionId` (the CLOB/Data/on-chain join key), a `questionID`
  (UMA) and TWO clobTokenIds. venue_market_id = conditionId;
  venue_secondary_id = Gamma id; token ids land in the instruments table.

Models are tolerant (`extra="allow"`): the raw lake keeps every field; these
classes only lift what the curated projection needs.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)

VENUE_ID = "polymarket"


def to_decimal(value: Any) -> Decimal | None:
    """Venue-native float → exact Decimal via str() — never Decimal(float),
    which would drag in binary-representation noise (0.1 → 0.1000000000...55)."""
    if value is None or value == "":
        return None
    return Decimal(str(value))


def to_unix_ts(value: Any) -> int | None:
    """ISO-8601 string (Gamma, e.g. "2026-07-12T21:10:00Z" or with offset)
    or unix seconds → unix seconds UTC. Gamma occasionally emits a spaced
    form ("2026-01-01 07:30:39+00" — seen in closedTime); tolerate it."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().replace("Z", "+00:00")
    if text.endswith("+00"):
        text += ":00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        logger.warning("unparseable Gamma timestamp: %r", value)
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def decode_json_list(value: Any) -> list[Any] | None:
    """data invariants: Gamma list fields arrive as JSON-encoded strings
    ('["Yes", "No"]'). Decode defensively — real arrays pass through, and a
    malformed string degrades to None (skip the market) rather than raising."""
    if value is None or value == "":
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            logger.warning("undecodable Gamma JSON-string list: %r", value[:120])
            return None
        return decoded if isinstance(decoded, list) else None
    return None


class _Tolerant(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


# status_norm mapping (live-verified against Gamma flag combinations):
#   closed=true + umaResolutionStatus="resolved"  -> settled
#   closed=true otherwise (resolution pending)    -> closed
#   active=true, closed=false, acceptingOrders=t  -> open
#   active=true, closed=false, acceptingOrders=f  -> paused  (deployed, halted/pre-open)
#   active=false, closed=false                    -> unopened (pending deployment)
# `archived` is a UI-visibility flag, orthogonal to lifecycle — ignored.
# Gamma exposes no explicit void marker; UMA 50/50 voids look settled (both
# outcomePrices 0.5) — resolution truth is a later phase (resolutions table).
def gamma_status_norm(
    active: bool | None,
    closed: bool | None,
    accepting_orders: bool | None,
    uma_resolution_status: str | None,
) -> str:
    if closed:
        return "settled" if uma_resolution_status == "resolved" else "closed"
    if active is False:
        return "unopened"
    if accepting_orders is False:
        return "paused"
    return "open"


class GammaMarket(_Tolerant):
    """One binary market from Gamma (top-level or embedded in an event)."""

    id: str  # Gamma numeric id, serialized as a string
    slug: str | None = None
    condition_id: str | None = Field(default=None, alias="conditionId")
    question_id: str | None = Field(default=None, alias="questionID")
    question: str | None = None
    outcomes: Any = None  # JSON-string or array -> list[str]
    outcome_prices: Any = Field(default=None, alias="outcomePrices")
    clob_token_ids: Any = Field(default=None, alias="clobTokenIds")
    active: bool | None = None
    closed: bool | None = None
    archived: bool | None = None
    accepting_orders: bool | None = Field(default=None, alias="acceptingOrders")
    uma_resolution_status: str | None = Field(default=None, alias="umaResolutionStatus")
    neg_risk: bool | None = Field(default=None, alias="negRisk")
    best_bid: Decimal | None = Field(default=None, alias="bestBid")
    best_ask: Decimal | None = Field(default=None, alias="bestAsk")
    last_trade_price: Decimal | None = Field(default=None, alias="lastTradePrice")
    volume_num: Decimal | None = Field(default=None, alias="volumeNum")
    volume_24hr: Decimal | None = Field(default=None, alias="volume24hr")
    liquidity_num: Decimal | None = Field(default=None, alias="liquidityNum")
    start_date: str | None = Field(default=None, alias="startDate")
    end_date: str | None = Field(default=None, alias="endDate")

    _dec = field_validator(
        "best_bid",
        "best_ask",
        "last_trade_price",
        "volume_num",
        "volume_24hr",
        "liquidity_num",
        mode="before",
    )(to_decimal)

    _lists = field_validator(
        "outcomes", "outcome_prices", "clob_token_ids", mode="before"
    )(decode_json_list)

    @property
    def outcome_names(self) -> list[str]:
        return [str(o) for o in self.outcomes or []]

    @property
    def token_ids(self) -> list[str]:
        """Both CLOB token ids, aligned with the outcomes array. Empty-string
        placeholders (pre-deployment markets) are dropped."""
        return [str(t) for t in self.clob_token_ids or [] if t]

    @property
    def outcome_probs(self) -> list[Decimal | None]:
        out: list[Decimal | None] = []
        for p in self.outcome_prices or []:
            try:
                out.append(to_decimal(p))
            except (InvalidOperation, ValueError):
                out.append(None)
        return out

    @property
    def status_norm(self) -> str:
        return gamma_status_norm(
            self.active, self.closed, self.accepting_orders, self.uma_resolution_status
        )

    def hot_row(self) -> dict[str, Any]:
        """Project into the HotStore markets-table shape. Event linkage and
        the derived recurrence class come from the enclosing event — the
        collector fills venue_event_id / venue_series_id / recurrence.
        bestBid/bestAsk/lastTradePrice quote the PRIMARY token (outcome 0,
        the YES-equivalent) and are already 0–1 probabilities."""
        return {
            "venue_market_id": self.condition_id,
            "venue_secondary_id": self.id,
            "slug": self.slug,
            "title": self.question,
            "market_type": "binary",
            "status_norm": self.status_norm,
            "open_ts": to_unix_ts(self.start_date),
            "close_ts": to_unix_ts(self.end_date),
            "yes_bid_prob": self.best_bid,
            "yes_ask_prob": self.best_ask,
            "last_prob": self.last_trade_price,
            "volume": self.volume_num,
            "volume_24h": self.volume_24hr,
            "liquidity": self.liquidity_num,
            "neg_risk": 1 if self.neg_risk else 0,
            "result": None,  # resolution truth (UMA) is a later phase
        }

    def instrument_rows(self) -> list[dict[str, Any]]:
        """One instrument per outcome token. is_primary = outcome index 0
        (the YES/Up leg — the one bestBid/bestAsk and the price sweep track);
        the sibling is redundant for prices (p_no = 1 − p_yes) but keeps its
        token id on record (books/WS/trades key on token id, data invariants)."""
        tokens = self.token_ids
        names = self.outcome_names
        return [
            {
                "venue_market_id": self.condition_id,
                "outcome_index": i,
                "outcome_name": names[i] if i < len(names) else None,
                "venue_token_id": token,
                "is_primary": i == 0,
            }
            for i, token in enumerate(tokens)
        ]


# Recurring high-frequency series (crypto up/down): thousands of 5-min/15-min
# /hourly markets churn through the live universe daily (measured 2026-07-11:
# 3,009 of 46,990 open markets). Their metadata is still captured (cheap), but
# the price sweep excludes classes '5m'/'15m'/'hourly' (see
# HotStore.active_primary_tokens). Detection is event-level, measured over a
# full 9,449-event live sweep: EVERY fast-churn event carries tag slug
# "recurring" plus a cadence tag ("5M"/"15M"/"1H"), so the "recurring" tag
# gates classification. series[].recurrence alone is UNRELIABLE both ways —
# 5m/15m series were observed with recurrence="daily", and one-off markets
# ("will-trump-resign-before-2027") sit in series with recurrence="daily" —
# so without the tag it classifies nothing, and with the tag it is only the
# last-resort cadence fallback.
_CADENCE_BY_TAG = {
    "5m": "5m",
    "15m": "15m",
    "1h": "hourly",
    "hourly": "hourly",
    "daily": "daily",
    "weekly": "weekly",
    "monthly": "monthly",
}
_CADENCE_BY_SLUG_SUFFIX = {
    "-5m": "5m",
    "-15m": "15m",
    "-1h": "hourly",
    "-hourly": "hourly",
    "-daily": "daily",
    "-weekly": "weekly",
    "-monthly": "monthly",
}


class GammaEvent(_Tolerant):
    """One Gamma event with embedded markets[] — the enumeration unit."""

    id: str
    slug: str | None = None
    ticker: str | None = None
    title: str | None = None
    active: bool | None = None
    closed: bool | None = None
    archived: bool | None = None
    neg_risk: bool | None = Field(default=None, alias="negRisk")
    neg_risk_augmented: bool | None = Field(default=None, alias="negRiskAugmented")
    series_slug: str | None = Field(default=None, alias="seriesSlug")
    series: list[dict[str, Any]] | None = None
    tags: list[dict[str, Any]] | None = None
    markets: list[GammaMarket] = Field(default_factory=list)

    @property
    def tag_slugs(self) -> set[str]:
        return {
            str(t.get("slug", "")).lower()
            for t in self.tags or []
            if isinstance(t, dict) and t.get("slug")
        }

    @property
    def effective_series_slug(self) -> str | None:
        if self.series_slug:
            return self.series_slug
        for s in self.series or []:
            if isinstance(s, dict) and s.get("slug"):
                return str(s["slug"])
        return None

    def recurrence_class(self) -> str | None:
        """Derived churn class for the whole event (None = one-off market).
        The "recurring" TAG gates classification: without it nothing is
        classified — sports/one-off events sit in series whose recurrence
        field reads "daily" and would otherwise mislabel ~3k events
        (measured 2026-07-11). Cadence comes from tags first, then the
        series-slug suffix, then series[].recurrence as a last resort."""
        tags = self.tag_slugs
        if "recurring" not in tags:
            return None
        for tag, cadence in _CADENCE_BY_TAG.items():
            if tag in tags:
                return cadence
        slug = (self.effective_series_slug or "").lower()
        for suffix, cadence in _CADENCE_BY_SLUG_SUFFIX.items():
            if slug.endswith(suffix):
                return cadence
        series_recurrence = next(
            (
                str(s["recurrence"]).lower()
                for s in self.series or []
                if isinstance(s, dict) and s.get("recurrence")
            ),
            None,
        )
        return series_recurrence or "recurring"  # cadence unknown — NOT price-exempt

    def hot_row(self) -> dict[str, Any]:
        """Project into the HotStore events-table shape. negRisk maps onto
        mutually_exclusive: a negRisk event's binary markets are one
        mutually-exclusive multi-outcome group (No converts to sibling Yes)."""
        tags = self.tags or []
        first_label = None
        for t in tags:
            if isinstance(t, dict) and t.get("label"):
                first_label = str(t["label"])
                break
        return {
            "venue_event_id": self.id,
            "venue_series_id": self.effective_series_slug,
            "title": self.title,
            "mutually_exclusive": 1 if self.neg_risk else 0,
            "category": first_label,
            "status": "open",  # enumerated from the live (active, not-closed) sweep
        }


class DataAPITrade(_Tolerant):
    """One trade from Data-API /trades (live-verified schema 2026-07-13, flat
    JSON — NOT nested). Prices are 0–1 probability (already venue-native,
    data invariants). No native trade id; key on
    md5(txHash|asset|proxyWallet|side|size|price|ts) (data invariants)."""

    proxy_wallet: str | None = Field(default=None, alias="proxyWallet")
    side: str | None = None  # 'BUY' or 'SELL'
    asset: str | None = None  # clobTokenId
    condition_id: str | None = Field(default=None, alias="conditionId")
    size: Decimal | None = None  # shares
    price: Decimal | None = None  # 0–1 probability
    timestamp: int | None = None  # unix SECONDS
    transaction_hash: str | None = Field(default=None, alias="transactionHash")

    _dec = field_validator("size", "price", mode="before")(to_decimal)


class PricePoint(BaseModel):
    """One point from /prices-history: unix-seconds t, 0–1 float p → Decimal."""

    t: int
    p: Decimal

    _dec = field_validator("p", mode="before")(to_decimal)
