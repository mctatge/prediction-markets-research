"""Externalized alias maps + series list for the cross-venue matcher.

These bridge Kalshi's team/country CODES (market-ticker suffixes, e.g. ``CHW``,
``PDX``, ``SUI``) to Polymarket's slug tokens (e.g. ``cws``, ``por``, ``che``).
They are hand-maintained and sports-only — the maintenance/coverage ceiling of
the structural matcher. Extend here as
new team codes appear; anything not aliased falls back to ``code.lower()``.

Ported verbatim from backtests/transfer_probe.py::rebuild_crosswalk so the
frozen crosswalk artifact and the live matcher share one source of truth.
"""

from __future__ import annotations

# The five sports series with deterministic slug/ticker structure (precise
# auto-matching). Non-sports needs fuzzy semantic matching → deferred.
SPORTS_SERIES: tuple[str, ...] = (
    "KXMLBGAME", "KXWNBAGAME", "KXWCGAME", "KXATPMATCH", "KXWTAMATCH",
)

# Kalshi ticker code -> Polymarket slug token, per sport.
WNBA_ALIAS = {"PDX": "por", "GS": "gsv", "NY": "nyl"}
WC_ALIAS = {
    "SUI": "che", "POR": "prt", "NED": "nld", "GER": "deu",
    "PAR": "pry", "CRO": "hrv",
}
MLB_ALIAS = {"CHW": "cws", "AZ": "ari"}

# Sports whose structural match is unambiguous enough to auto-confirm from day
# one: clean 2-way moneyline, exact alias, 1:1 sides. World Cup (3-way, draw
# leg) and tennis (surname-prefix, fuzzy) stay candidate-only (manual gate)
# until the post-settlement QA loop earns them trust. Widen cautiously.
AUTO_CONFIRM_SPORTS = frozenset({"mlb", "wnba"})

_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
