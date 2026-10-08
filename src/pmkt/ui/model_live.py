"""Live model evaluation for the screener.

The calibration model is a deterministic function of price, so model_p and
edge are computed against the CURRENT price at request time — never read from
a frozen forecast. A stale stored model_p compared to a drifted live price is
not an edge, it is stale-price noise (confirmed live 2026-07-12: the top
|edge| rows were simply whatever moved most since the last scoring run).

The frozen ``forecasts`` table + daily parquet snapshots still exist, but only
for calibration GRADING (forecast-as-made, immutably) — not for the live view.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ..model.score import evaluate_market, load_current_artifact


def register_model_udf(conn: sqlite3.Connection, data_dir: Path, now_ts: int) -> None:
    """Register ``pmkt_model_p(category, open_ts, close_ts, bid, ask, last)``
    on ``conn``, returning the calibrated probability as a decimal STRING
    (data invariants) or NULL when no non-shadow model is loaded or the market is
    out of support. ``now_ts`` is fixed for the request, so the function is
    deterministic within a query."""
    artifact = load_current_artifact(data_dir)  # None if no model; mtime-cached
    # Shadow models (failed the validation gate) are still scored+logged for
    # self-grading, but must NEVER surface an edge in the UI.
    if artifact is not None and artifact.get("model_version", "").startswith("shadow-"):
        artifact = None

    def pmkt_model_p(category, open_ts, close_ts, bid, ask, last):
        if artifact is None:
            return None
        forecast = evaluate_market(
            artifact,
            category=category,
            close_ts=close_ts,
            open_ts=open_ts,
            yes_bid_prob=bid,
            yes_ask_prob=ask,
            last_prob=last,
            now_ts=now_ts,
        )
        return forecast["model_p"] if forecast is not None else None

    conn.create_function("pmkt_model_p", 6, pmkt_model_p)
