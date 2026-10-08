"""Training dataset for the FLB recalibration model.

Built over ``pmkt.lake.connect(data_dir)``: settled Kalshi markets from the
hot store labeled by ``result``, priced by the curated ``trades`` view.

THE LOOKAHEAD RULE IS ABSOLUTE: an observation at horizon ``h`` may only see
trades with ``ts <= close_ts - h``. The feature is the LAST trade price at or
before that instant, restricted to a bounded lookback window (one horizon
step), so a stale week-old print can't masquerade as a 1h-out price. Any
trade newer than ``close_ts - h`` is invisible to the h-horizon observation
by construction of the SQL window — tested with a synthetic lake in
tests/test_model_dataset.py.

Filters (paper-aligned, Bürgi-Deng-Whelan):
- settled, result in ('yes','no') — scalar/void markets carry no binary label
- close_ts - open_ts >= 24h — excludes hourly-reset/churn markets
- >= 3 trades inside the lookback window — thin-print noise guard
- feature price in [0.005, 0.995] — sub-tick pins carry no calibration signal

Floats are fine here (model-internal math); nothing from this module is
stored or served.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import duckdb

logger = logging.getLogger(__name__)

VENUE_ID = "kalshi"
MIN_DURATION_S = 86400
MIN_TRADES_IN_WINDOW = 3
PRICE_LO = 0.005
PRICE_HI = 0.995

#: Horizon buckets (label -> seconds before close). Ordered smallest-first.
HORIZON_BUCKETS: list[tuple[str, int]] = [
    ("1h", 3_600),
    ("6h", 21_600),
    ("24h", 86_400),
    ("72h", 259_200),
    ("168h", 604_800),
]

#: Lookback window per bucket = distance to the NEXT-larger bucket (the
#: "one horizon step" rule) so adjacent buckets read disjoint stretches of
#: tape; the largest bucket reuses its own width (168h) — there is no larger
#: step to borrow.
HORIZON_LOOKBACK_S: dict[str, int] = {
    "1h": 18_000,     # 6h - 1h
    "6h": 64_800,     # 24h - 6h
    "24h": 172_800,   # 72h - 24h
    "72h": 345_600,   # 168h - 72h
    "168h": 604_800,  # its own width
}

#: Explicit mapping of the observed Kalshi category strings (hot.series /
#: hot.events, inspected on production data 2026-07-12 — 18 distinct values)
#: to coarse model groups. Anything unlisted (new venue categories, NULL)
#: lands in 'other'. Choices worth recording:
#: - Commodities/Companies -> financials (asset-price / corporate-event
#:   markets trade like financials, tiny n on their own)
#: - Elections/World -> politics (paper's 'politics' bucket)
#: - Mentions -> entertainment (media/speech prop markets)
#: - Science and Technology, Social, Health, Transportation, Exotics,
#:   Education -> other (tiny n, no paper prior)
CATEGORY_GROUPS: dict[str, str] = {
    "Sports": "sports",
    "Crypto": "crypto",
    "Financials": "financials",
    "Commodities": "financials",
    "Companies": "financials",
    "Economics": "economics",
    "Politics": "politics",
    "Elections": "politics",
    "World": "politics",
    "Entertainment": "entertainment",
    "Mentions": "entertainment",
    "Climate and Weather": "climate",
}
DEFAULT_GROUP = "other"
GROUPS = sorted(set(CATEGORY_GROUPS.values()) | {DEFAULT_GROUP})


def category_group(raw_category: str | None) -> str:
    return CATEGORY_GROUPS.get(raw_category or "", DEFAULT_GROUP)


@dataclass(frozen=True)
class Observation:
    venue_market_id: str
    horizon_bucket: str
    category_group: str
    p_market: float   # last trade price in the window (float: model-internal)
    y: int            # 1 if result='yes' else 0
    close_ts: int     # settled market close (time-split key)


# One query per horizon bucket. The window predicate
#   ts <= close_ts - h  AND  ts >= close_ts - h - lookback
# is the entire lookahead defense: it lives in ONE place, here.
# n_win/rn are computed per market over window-eligible trades only.
_OBS_SQL = """
WITH windowed AS (
    SELECT
        m.venue_market_id,
        m.close_ts,
        CASE WHEN m.result = 'yes' THEN 1 ELSE 0 END        AS y,
        COALESCE(s.category, e.category)                     AS category,
        t.price_prob,
        count(*)     OVER (PARTITION BY m.venue_market_id)  AS n_win,
        row_number() OVER (
            PARTITION BY m.venue_market_id
            ORDER BY t.ts DESC, t.venue_trade_id DESC
        )                                                    AS rn
    FROM hot.markets m
    JOIN trades t
      ON t.venue_id = m.venue_id
     AND t.venue_market_id = m.venue_market_id
     AND t.ts <= m.close_ts - {h}
     AND t.ts >= m.close_ts - {h} - {lookback}
    LEFT JOIN hot.series s
      ON s.venue_id = m.venue_id AND s.venue_series_id = m.venue_series_id
    LEFT JOIN hot.events e
      ON e.venue_id = m.venue_id AND e.venue_event_id = m.venue_event_id
    WHERE m.venue_id = '{venue}'
      AND m.status_norm = 'settled'
      AND m.result IN ('yes', 'no')
      AND m.open_ts IS NOT NULL AND m.close_ts IS NOT NULL
      AND m.close_ts - m.open_ts >= {min_duration}
)
SELECT venue_market_id, close_ts, y, category,
       CAST(price_prob AS DOUBLE) AS p_market
FROM windowed
WHERE rn = 1
  AND n_win >= {min_trades}
  AND price_prob >= {price_lo} AND price_prob <= {price_hi}
"""


def build_training_frame(conn: duckdb.DuckDBPyConnection) -> list[Observation]:
    """(market × horizon) observations from a ``pmkt.lake.connect`` connection.

    Requires the curated ``trades`` view (build_trades has run) and the
    attached ``hot`` store; raises RuntimeError with a actionable message if
    either is missing.
    """
    _require_surface(conn)
    frame: list[Observation] = []
    for bucket, h_seconds in HORIZON_BUCKETS:
        sql = _OBS_SQL.format(
            venue=VENUE_ID,
            h=h_seconds,
            lookback=HORIZON_LOOKBACK_S[bucket],
            min_duration=MIN_DURATION_S,
            min_trades=MIN_TRADES_IN_WINDOW,
            price_lo=PRICE_LO,
            price_hi=PRICE_HI,
        )
        rows = conn.execute(sql).fetchall()
        for market_id, close_ts, y, category, p_market in rows:
            frame.append(
                Observation(
                    venue_market_id=market_id,
                    horizon_bucket=bucket,
                    category_group=category_group(category),
                    p_market=float(p_market),
                    y=int(y),
                    close_ts=int(close_ts),
                )
            )
        logger.info("dataset: horizon %s -> %d observations", bucket, len(rows))
    frame.sort(key=lambda o: (o.close_ts, o.venue_market_id, o.horizon_bucket))
    return frame


def _require_surface(conn: duckdb.DuckDBPyConnection) -> None:
    views = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    if "trades" not in views:
        raise RuntimeError(
            "curated trades view missing — run `pmkt lake build-trades` first"
        )
    try:
        conn.execute("SELECT 1 FROM hot.markets LIMIT 1")
    except duckdb.Error as exc:
        raise RuntimeError(f"hot store not attached/readable: {exc}") from exc
