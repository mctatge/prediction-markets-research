"""Experimental selected-wallet flow fields for the local screener.

Historical category coefficients are retained for reproducibility. They were
not supported as a meaningful executable advantage in the decision-time
midpoint rerun; they are not certified probabilities or trading signals.
The offline example uses synthetic data and labels these estimates explicitly.
See docs/findings.md and docs/methodology.md for limitations.
"""

from __future__ import annotations

import sqlite3

from ..model.fees import _PM_CRYPTO

ALPHA_SPORTS = 0.07
ALPHA_CRYPTO = 0.03
ALPHA_PRICE_ACTION = 0.03
ALPHA_DEFAULT = 0.00

# Corpus category_refined 'Price Action' = these Gamma categories
# (measured 2026-07-16 on daily_aligned 2025-06): recurring price-level /
# direction markets.
_PRICE_ACTION_EXACT = frozenset({
    "up or down", "1h", "weekly", "hit price", "4h", "daily",
})
_PRICE_ACTION_KEYWORDS = ("up or down", "updown", "hit price")

_CRYPTO_EXTRA = frozenset({"ripple", "recurring", "crypto summit", "eth", "btc"})

_SPORTS_EXACT = frozenset({
    "sports", "tennis", "basketball", "baseball", "soccer", "football",
    "cricket", "golf", "hockey", "boxing", "cycling", "wrestling",
    "pickleball", "table tennis", "horse racing", "chess",
    "nba", "nfl", "mlb", "nhl", "ncaa", "mls", "wnba", "pga",
    "nba playoffs", "nfl playoffs", "mlb playoffs", "nhl playoffs",
    "nba finals", "nba draft", "nfl draft", "ncaa basketball",
    "epl", "premier league", "la liga", "bundesliga", "ligue 1",
    "champions league", "europa league", "formula 1", "f1",
    "ufc", "mma", "combat sports",
    "esports", "dota 2", "valorant", "overwatch", "rocket league",
    "starcraft 2", "cs2", "apex legends",
})

_SPORTS_KEYWORDS = (
    "sport", "basketball", "baseball", "soccer", "football", "cricket",
    "golf", "hockey", "boxing", "cycling", "wrestling", "tennis",
    "nba ", "nfl ", "mlb ", "nhl ", "ncaa ", "ufc ", "mma ",
    "playoff", "olympics", "world cup", "super bowl", "stanley cup",
    "premier league", "champions league", "europa league",
    "esport", "dota", "valorant", "overwatch",
    "counter strike", "counter-strike", "league of legends",
    "rocket league", "starcraft",
)


def is_sports_category(category: str | None) -> bool:
    if category is None:
        return False
    cl = category.lower().strip()
    if cl in _SPORTS_EXACT:
        return True
    return any(kw in cl for kw in _SPORTS_KEYWORDS)


def _is_price_action(cl: str) -> bool:
    return cl in _PRICE_ACTION_EXACT or any(
        kw in cl for kw in _PRICE_ACTION_KEYWORDS
    )


def _is_crypto(cl: str) -> bool:
    return cl in _PM_CRYPTO or cl in _CRYPTO_EXTRA or "crypto" in cl


def ofi_alpha_for(venue: str | None, category: str | None) -> float:
    if venue != "polymarket":
        return 0.0
    if is_sports_category(category):
        return ALPHA_SPORTS
    cl = (category or "").lower().strip()
    if _is_price_action(cl):
        return ALPHA_PRICE_ACTION
    if _is_crypto(cl):
        return ALPHA_CRYPTO
    return ALPHA_DEFAULT


def register_ofi_udf(conn: sqlite3.Connection) -> None:
    """Register OFI UDFs:
    - ``pmkt_skilled_ofi(venue_id, venue_market_id)`` → decimal STRING or NULL
    - ``pmkt_n_skilled(venue_id, venue_market_id)`` → INTEGER
    - ``pmkt_ofi_alpha(venue_id, category)`` → REAL (per-category alpha)
    """
    ofi_cache: dict[str, str | None] = {}
    n_cache: dict[str, int] = {}

    try:
        for row in conn.execute(
            "SELECT venue_market_id, skilled_ofi, n_skilled_trades FROM skilled_ofi"
        ):
            ofi_cache[row[0]] = row[1]
            n_cache[row[0]] = row[2] if row[2] is not None else 0
    except Exception:
        pass

    def _ofi(venue_id, venue_market_id):
        if venue_id != "polymarket":
            return None
        return ofi_cache.get(venue_market_id)

    def _n(venue_id, venue_market_id):
        if venue_id != "polymarket":
            return None
        return n_cache.get(venue_market_id)

    conn.create_function("pmkt_skilled_ofi", 2, _ofi)
    conn.create_function("pmkt_n_skilled", 2, _n)
    conn.create_function("pmkt_ofi_alpha", 2, ofi_alpha_for)
