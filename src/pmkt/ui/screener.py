"""Screener query builder — validated params → parameterized SQLite SQL.

All user input is bound as SQL parameters (``?``); the only strings ever
interpolated into SQL are fixed expressions chosen from closed dicts below.
CAST(text AS REAL) appears ONLY inside WHERE/ORDER BY — the SELECT list
returns the stored TEXT columns untouched (data invariants).
"""

from __future__ import annotations

from typing import Any

CHURN_CLASSES = ("5m", "15m", "hourly")

#: mid with last fallback — the price-band filter and "price" sort key.
MID_EXPR = (
    "CASE WHEN m.yes_bid_prob IS NOT NULL AND m.yes_ask_prob IS NOT NULL "
    "THEN (CAST(m.yes_bid_prob AS REAL) + CAST(m.yes_ask_prob AS REAL)) / 2.0 "
    "ELSE CAST(m.last_prob AS REAL) END"
)

#: yes_ask − yes_bid; NULL when either side is unquoted.
SPREAD_EXPR = (
    "CASE WHEN m.yes_bid_prob IS NOT NULL AND m.yes_ask_prob IS NOT NULL "
    "THEN CAST(m.yes_ask_prob AS REAL) - CAST(m.yes_bid_prob AS REAL) END"
)

#: Joined category: Polymarket events carry it; Kalshi events mostly do with
#: series as the authoritative fallback (measured 2026-07-12: series covers
#: 100% of open Kalshi rows). Both joins hit UNIQUE indexes — no fanout.
CATEGORY_EXPR = "COALESCE(e.category, s.category)"

#: The price the model corrects against and edge is measured from: mid when
#: the book is real (spread ≤ 0.20), else last trade. Mirrors
#: model.score.base_price_of / rows._edge so model_p and edge stay consistent.
BASE_EXPR = (
    f"CASE WHEN ({SPREAD_EXPR}) <= 0.20 THEN ({MID_EXPR}) "
    "ELSE CAST(m.last_prob AS REAL) END"
)

SKILLED_OFI_EXPR = "pmkt_skilled_ofi(m.venue_id, m.venue_market_id)"

N_SKILLED_EXPR = "pmkt_n_skilled(m.venue_id, m.venue_market_id)"

OFI_ALPHA_EXPR = f"pmkt_ofi_alpha(m.venue_id, {CATEGORY_EXPR})"

_OFI_MODEL_P = (
    f"printf('%.4f', MIN(1.0, MAX(0.0, "
    f"({BASE_EXPR}) + {OFI_ALPHA_EXPR} * CAST({SKILLED_OFI_EXPR} AS REAL))))"
)

MODEL_P_EXPR = (
    "CASE WHEN m.venue_id = 'kalshi' THEN "
    f"pmkt_model_p({CATEGORY_EXPR}, m.open_ts, m.close_ts, "
    "m.yes_bid_prob, m.yes_ask_prob, m.last_prob) "
    f"WHEN {SKILLED_OFI_EXPR} IS NOT NULL THEN {_OFI_MODEL_P} "
    "END"
)

#: Fee-adjusted executable edge via UDF: model_p − execution_price − taker_fee.
#: Signed model comparison after assumed execution costs. Its sign alone
#: does not establish profitable execution. NULL when a required input is missing.
EXEC_EDGE_EXPR = (
    f"pmkt_exec_edge(m.venue_id, {CATEGORY_EXPR}, "
    f"{MODEL_P_EXPR}, {BASE_EXPR}, "
    f"m.yes_bid_prob, m.yes_ask_prob)"
)

#: sort name → (null-check expression, order expression). NULLs sort last
#: regardless of direction via a leading `<null-check> IS NULL` key.
SORT_EXPRS: dict[str, tuple[str, str]] = {
    "vol24h": ("m.volume_24h", "CAST(m.volume_24h AS REAL)"),
    "volume": ("m.volume", "CAST(m.volume AS REAL)"),
    "price": (MID_EXPR, MID_EXPR),
    "spread": (SPREAD_EXPR, SPREAD_EXPR),
    "close_ts": ("m.close_ts", "m.close_ts"),
    "open_interest": ("m.open_interest", "CAST(m.open_interest AS REAL)"),
    "liquidity": ("m.liquidity", "CAST(m.liquidity AS REAL)"),
    "updated": ("m.prices_updated_ts", "m.prices_updated_ts"),
    "title": ("m.title", "m.title COLLATE NOCASE"),
    # |edge| = |model_p − base|, both computed LIVE from the current price
    # (see MODEL_P_EXPR / BASE_EXPR). Biggest model-vs-market disagreements
    # first. NULL model_p (no live forecast) sorts last.
    "edge": (
        MODEL_P_EXPR,
        f"ABS(CAST({MODEL_P_EXPR} AS REAL) - ({BASE_EXPR}))",
    ),
    "skilled_ofi": (
        SKILLED_OFI_EXPR,
        f"ABS(CAST({SKILLED_OFI_EXPR} AS REAL))",
    ),
    "exec_edge": (
        "exec_edge",
        "ABS(CAST(exec_edge AS REAL))",
    ),
}

#: sorts whose expressions reference the joined category (events/series) and
#: therefore cannot use the joins-outside-LIMIT fast path.
JOIN_SORTS = frozenset({"edge", "exec_edge"})

SORT_NAMES = tuple(SORT_EXPRS)
STATUS_VALUES = ("open", "closed", "settled", "all")
VENUES = ("kalshi", "polymarket")

JOINS = (
    " LEFT JOIN events e ON e.venue_id = m.venue_id"
    " AND e.venue_event_id = m.venue_event_id"
    " LEFT JOIN series s ON s.venue_id = m.venue_id"
    " AND s.venue_series_id = m.venue_series_id"
)

_MARKET_COLUMNS = (
    "m.venue_id, m.venue_market_id, m.venue_event_id, m.venue_series_id, "
    "m.title, m.status_norm, m.yes_bid_prob, m.yes_ask_prob, m.last_prob, "
    "m.volume, m.volume_24h, m.open_interest, m.liquidity, "
    "m.close_ts, m.prices_updated_ts, m.recurrence, m.slug"
)

#: model_p computed live by the pmkt_model_p UDF (registered per request).
#: skilled_ofi + n_skilled_trades from the pmkt_skilled_ofi/pmkt_n_skilled UDFs.
_FORECAST_COLUMNS = (
    f"{MODEL_P_EXPR} AS model_p, "
    f"{SKILLED_OFI_EXPR} AS skilled_ofi, "
    f"{N_SKILLED_EXPR} AS n_skilled_trades, "
    f"{EXEC_EDGE_EXPR} AS exec_edge"
)


def like_pattern(q: str) -> str:
    r"""Substring LIKE pattern with %, _ and \ escaped (ESCAPE '\')."""
    escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def build_where(
    *,
    venue: str | None,
    category: str | None,
    q: str | None,
    status: str,
    price_min: float | None,
    price_max: float | None,
    vol24h_min: float | None,
    liquidity_min: float | None,
    spread_max: float | None,
    close_within_h: float | None,
    exclude_churn: bool,
    now_ts: int,
) -> tuple[str, list[Any]]:
    """WHERE clause + bound args. ``category`` requires the events/series
    joins to be present in the enclosing query."""
    clauses: list[str] = []
    args: list[Any] = []

    if status != "all":
        clauses.append("m.status_norm = ?")
        args.append(status)
    if venue is not None:
        clauses.append("m.venue_id = ?")
        args.append(venue)
    if category is not None:
        clauses.append(f"{CATEGORY_EXPR} = ?")
        args.append(category)
    if q:
        pattern = like_pattern(q)
        clauses.append(
            "(m.title LIKE ? ESCAPE '\\' OR m.venue_market_id LIKE ? ESCAPE '\\'"
            " OR m.venue_event_id LIKE ? ESCAPE '\\' OR m.slug LIKE ? ESCAPE '\\')"
        )
        args.extend([pattern] * 4)
    if price_min is not None:
        clauses.append(f"({MID_EXPR}) >= ?")
        args.append(price_min)
    if price_max is not None:
        clauses.append(f"({MID_EXPR}) <= ?")
        args.append(price_max)
    if vol24h_min is not None:
        clauses.append("CAST(m.volume_24h AS REAL) >= ?")
        args.append(vol24h_min)
    if liquidity_min is not None:
        clauses.append("CAST(m.liquidity AS REAL) >= ?")
        args.append(liquidity_min)
    if spread_max is not None:
        # Rows with an unknown spread PASS (contract) — only a known-wide
        # spread filters a row out.
        clauses.append(f"(({SPREAD_EXPR}) IS NULL OR ({SPREAD_EXPR}) <= ?)")
        args.append(spread_max)
    if close_within_h is not None:
        # close_ts within N hours from now; NULL close_ts excluded. Markets
        # whose close_ts already passed are excluded too (a "closing soon"
        # scan should not surface stale not-yet-marked-closed rows).
        clauses.append("m.close_ts IS NOT NULL AND m.close_ts >= ? AND m.close_ts <= ?")
        args.extend([now_ts, now_ts + int(close_within_h * 3600)])
    if exclude_churn:
        placeholders = ",".join("?" * len(CHURN_CLASSES))
        clauses.append(f"(m.recurrence IS NULL OR m.recurrence NOT IN ({placeholders}))")
        args.extend(CHURN_CLASSES)

    return (" AND ".join(clauses) or "1=1"), args


def _order_by(sort: str, direction: str) -> str:
    null_expr, order_expr = SORT_EXPRS[sort]
    sql_dir = "ASC" if direction == "asc" else "DESC"
    # market_id (surrogate int PK) as the final key keeps paging stable.
    return f"({null_expr}) IS NULL ASC, {order_expr} {sql_dir}, m.market_id"


def build_rows_query(
    where: str,
    args: list[Any],
    *,
    sort: str,
    direction: str,
    limit: int,
    offset: int,
    category_filtered: bool,
) -> tuple[str, list[Any]]:
    """Page query. Without a category filter the (indexed but per-row) event/
    series joins run only over the returned page, not the full scan — the
    joins move outside the LIMIT. ORDER BY is re-applied on the outer level
    because a join does not guarantee inner ordering."""
    order = _order_by(sort, direction)
    if category_filtered or sort in JOIN_SORTS:
        sql = (
            f"SELECT {_MARKET_COLUMNS}, {_FORECAST_COLUMNS}, m.market_id, {CATEGORY_EXPR} AS category"
            f" FROM markets m{JOINS} WHERE {where}"
            f" ORDER BY {order} LIMIT ? OFFSET ?"
        )
        return sql, [*args, limit, offset]
    inner = (
        f"SELECT m.* FROM markets m WHERE {where}"
        f" ORDER BY {order} LIMIT ? OFFSET ?"
    )
    sql = (
        f"SELECT {_MARKET_COLUMNS}, {_FORECAST_COLUMNS}, m.market_id, {CATEGORY_EXPR} AS category"
        f" FROM ({inner}) AS m{JOINS} ORDER BY {order}"
    )
    return sql, [*args, limit, offset]


def build_count_query(
    where: str, args: list[Any], *, category_filtered: bool
) -> tuple[str, list[Any]]:
    joins = JOINS if category_filtered else ""
    return f"SELECT COUNT(*) FROM markets m{joins} WHERE {where}", list(args)


def build_detail_query(*, by_event: bool) -> str:
    """Single market (by venue+venue_market_id) or all siblings (by
    venue+venue_event_id) — always joined for category."""
    key = "m.venue_event_id = ?" if by_event else "m.venue_market_id = ?"
    return (
        f"SELECT {_MARKET_COLUMNS}, {_FORECAST_COLUMNS}, {CATEGORY_EXPR} AS category"
        f" FROM markets m{JOINS} WHERE m.venue_id = ? AND {key}"
    )
