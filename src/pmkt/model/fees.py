"""Historical fee-schedule estimates for paper evaluation.

Kalshi uses 0.07 * P * (1-P); Polymarket uses a category-rate estimate.
Actual fees can vary by market, date, rounding and contract count. These
functions do not query current rates and are not a certified execution model.
"""

from __future__ import annotations

import sqlite3

KALSHI_TAKER_RATE = 0.07

_PM_CRYPTO = frozenset({
    "crypto", "bitcoin", "ethereum", "solana", "xrp", "dogecoin",
    "bnb", "stablecoins", "airdrops", "hyperliquid", "token launch",
    "crypto prices", "crypto culture", "crypto legal", "crypto listings",
    "crypto treasury",
})

_PM_GEOPOLITICS_KEYWORDS = ("geopolitic",)

_PM_FINANCE_KEYWORDS = ("financ", "earning", "ipo", "banking", "equit")

_PM_POLITICS_KEYWORDS = (
    "politic", "election", "midterm", "senat", "congress",
    "trump", "biden", "president", "court", "scotus",
)

_PM_TECH_EXACT = frozenset({
    "tech", "ai", "openai", "spacex", "anthropic", "claude", "claude 5",
    "quantum", "ai benchmarks", "ai regulation",
})


def pm_fee_rate(category: str | None) -> float:
    """Polymarket taker fee rate by category (documented schedule)."""
    if category is None:
        return 0.05
    c = category.lower()
    if c in _PM_CRYPTO or "crypto" in c:
        return 0.07
    if any(k in c for k in _PM_GEOPOLITICS_KEYWORDS):
        return 0.0
    if any(k in c for k in _PM_FINANCE_KEYWORDS):
        return 0.04
    if any(k in c for k in _PM_POLITICS_KEYWORDS):
        return 0.04
    if c in _PM_TECH_EXACT:
        return 0.04
    return 0.05


def taker_fee_rate(venue: str, category: str | None) -> float:
    if venue == "kalshi":
        return KALSHI_TAKER_RATE
    if venue == "polymarket":
        return pm_fee_rate(category)
    return 0.05


def register_fee_udf(conn: sqlite3.Connection) -> None:
    """Register ``pmkt_exec_edge(venue, category, model_p, base, bid, ask)``
    — returns the fee-adjusted executable edge as a 4-dp signed string,
    or NULL when the needed inputs are missing."""

    def _exec_edge(
        venue: str | None,
        category: str | None,
        model_p_val: str | float | None,
        base: float | None,
        bid_str: str | None,
        ask_str: str | None,
    ) -> str | None:
        if model_p_val is None or base is None:
            return None
        model_p = float(model_p_val)
        rate = taker_fee_rate(venue or "polymarket", category)
        if model_p >= base:
            if ask_str is None:
                return None
            ask = float(ask_str)
            fee = rate * ask * (1.0 - ask)
            return f"{model_p - ask - fee:.4f}"
        else:
            if bid_str is None:
                return None
            bid = float(bid_str)
            fee = rate * (1.0 - bid) * bid
            return f"{model_p - bid + fee:.4f}"

    conn.create_function("pmkt_exec_edge", 6, _exec_edge)
