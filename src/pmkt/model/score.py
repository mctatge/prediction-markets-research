"""Scoring job: calibrated model_p for open Kalshi markets.

``model_score(client, store)`` matches the run-loop job signature (client is
unused — scoring is pure hot-store + artifact); the sync core is
``score_rows`` for tests and ad-hoc runs. Writes:

1. hot-store ``forecasts`` table — ONE current row per market (upsert), the
   UI edge-column join target. model_p and base_price are decimal STRINGS
   (data invariants); floats never leave this module.
2. daily parquet snapshot ``curated/forecasts/date=YYYY-MM-DD/part-0.parquet``
   — the calibration-grading log. Choice:
   write ONCE per UTC day — the first scoring run of the day that produces
   rows writes the file, later runs skip (simple, idempotent, no same-day
   union rewrites; intraday drift is not worth grading against at v1 volume,
   ~30-60k rows/day). grade.py accrues these against settled outcomes.

Out-of-support markets get NO forecast — honesty over coverage:
- not open, close_ts <= now, or lifetime < 24h (training universe filter)
- no usable price (mid needs BOTH yes_bid and yes_ask; else last_prob)
- base price outside [0.005, 0.995] (outside the fitted curve's support —
  extreme pins would be served as flat-extension guesses, so they are skipped)
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from ..state import HotStore
from .calibrate import current_artifact_path, interpolate, load_artifact, resolve_curve
from .dataset import (
    HORIZON_BUCKETS,
    MIN_DURATION_S,
    PRICE_HI,
    PRICE_LO,
    VENUE_ID,
    category_group,
)

logger = logging.getLogger(__name__)

_PRICE_LO = Decimal(str(PRICE_LO))
_PRICE_HI = Decimal(str(PRICE_HI))

#: (artifact path, mtime, parsed artifact) — reload only when the file changes.
_artifact_cache: tuple[Path, float, dict[str, Any]] | None = None


def load_current_artifact(data_dir: Path) -> dict[str, Any] | None:
    """CURRENT artifact, cached on (path, mtime). None if no model trained."""
    global _artifact_cache
    path = current_artifact_path(data_dir)
    if path is None:
        return None
    mtime = path.stat().st_mtime
    if _artifact_cache and _artifact_cache[0] == path and _artifact_cache[1] == mtime:
        return _artifact_cache[2]
    artifact = load_artifact(path)
    _artifact_cache = (path, mtime, artifact)
    logger.info("loaded model artifact %s (%s)", path.name, artifact["model_version"])
    return artifact


def horizon_bucket_for(seconds_to_close: int) -> str:
    """Map time-to-close onto the bucket whose TRAINING window contains it.

    dataset.py trains bucket h on trades with time-to-close in [h, next_h)
    (HORIZON_LOOKBACK_S = distance to the next bucket — disjoint windows), so a
    market at time-to-close t belongs in the bucket with the largest
    h_seconds <= t (FLOOR), clamped up to the smallest bucket for t below it.

    A ceiling (the old "nearest"-labelled `<= h`) mis-served everything in the
    lower part of a range: a 5h-out price got the 6h curve (trained on 6-24h
    prices) instead of the 1h curve whose [1h,6h) window holds it — and a
    25h-out market got the untrained 72h bucket (→ skipped) instead of the 24h
    curve that covers it (review-confirmed 2026-07-12). FLB magnitude grows
    with horizon, so the wrong-horizon curve mis-sizes the correction; the
    time-split validation buckets each obs correctly, so a ceiling served
    forecasts the reported skill never measured."""
    bucket = HORIZON_BUCKETS[0][0]  # clamp-up floor: t below the smallest bucket
    for name, h_seconds in HORIZON_BUCKETS:
        if seconds_to_close >= h_seconds:
            bucket = name
        else:
            break
    return bucket


def _decimal(text: str | None) -> Decimal | None:
    if text is None or text == "":
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


MAX_BOOK_SPREAD = Decimal("0.20")  # paper's training filter; wider = no book


def base_price_of(row: dict[str, Any]) -> Decimal | None:
    """Mid of yes_bid/yes_ask when BOTH exist AND the book is real (spread
    ≤ 0.20, the same hygiene the training data applies) — an empty/wide book
    yields mid≈0.50 regardless of value, and correcting THAT manufactures
    fake edge (observed live 2026-07-12: 0.00/0.98 index ladders topped the
    edge sort). Else last trade price, else None."""
    bid = _decimal(row.get("yes_bid_prob"))
    ask = _decimal(row.get("yes_ask_prob"))
    if bid is not None and ask is not None and (ask - bid) <= MAX_BOOK_SPREAD:
        return (bid + ask) / 2
    return _decimal(row.get("last_prob"))


def evaluate_market(
    artifact: dict[str, Any],
    *,
    category: str | None,
    close_ts: int | None,
    open_ts: int | None,
    yes_bid_prob: str | None,
    yes_ask_prob: str | None,
    last_prob: str | None,
    now_ts: int,
) -> dict[str, Any] | None:
    """The model as a pure function of ONE market's current state → forecast,
    or None if out of support. THE single evaluation path: the scoring job and
    the live UI both call this, so a market's forecast is identical whether
    read from the forecasts table or computed live against the current price
    (which is the whole point — a frozen model_p compared to a drifted live
    price produces stale-price noise).

    Pure eval — does NOT gate on shadow: the scoring job logs shadow-model
    forecasts too (for self-grading via `pmkt model grade`); only the UI hides
    shadow (model_live.register_model_udf)."""
    if open_ts is None or close_ts is None or close_ts <= now_ts:
        return None
    if close_ts - open_ts < MIN_DURATION_S:  # sub-24h/hourly-reset churn
        return None
    base = base_price_of(
        {"yes_bid_prob": yes_bid_prob, "yes_ask_prob": yes_ask_prob,
         "last_prob": last_prob}
    )
    if base is None or not (_PRICE_LO <= base <= _PRICE_HI):
        return None
    bucket = horizon_bucket_for(close_ts - now_ts)
    group = category_group(category)
    curves = artifact["curves"]
    # A bucket with NO trained curve at all
    # (v1: 72h/168h — tape too shallow) is outside support; extrapolating a
    # short-horizon global curve onto months-out markets is not a forecast.
    if f"*|{bucket}" not in curves and f"{group}|{bucket}" not in curves:
        return None
    curve = resolve_curve(curves, group, bucket)
    model_p = interpolate(curve, float(base))  # float math is model-internal
    return {
        "model_version": artifact["model_version"],
        "model_p": f"{model_p:.4f}",       # decimal STRING, 4 places
        "base_price": format(base, "f"),   # exact decimal string
        "horizon_bucket": bucket,
        "category_group": group,
    }


def score_rows(
    store: HotStore, artifact: dict[str, Any], now_ts: int
) -> list[dict[str, Any]]:
    """Sync core: forecast rows for all in-support open Kalshi markets."""
    markets = store.conn.execute(
        """SELECT m.venue_market_id, m.yes_bid_prob, m.yes_ask_prob, m.last_prob,
                  m.open_ts, m.close_ts, s.category
           FROM markets m
           LEFT JOIN series s
             ON s.venue_id = m.venue_id AND s.venue_series_id = m.venue_series_id
           WHERE m.venue_id = ? AND m.status_norm = 'open'
             AND m.open_ts IS NOT NULL AND m.close_ts IS NOT NULL
             AND m.close_ts > ?
             AND m.close_ts - m.open_ts >= ?""",
        (VENUE_ID, now_ts, MIN_DURATION_S),
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for m in markets:
        forecast = evaluate_market(
            artifact,
            category=m["category"],
            close_ts=m["close_ts"],
            open_ts=m["open_ts"],
            yes_bid_prob=m["yes_bid_prob"],
            yes_ask_prob=m["yes_ask_prob"],
            last_prob=m["last_prob"],
            now_ts=now_ts,
        )
        if forecast is not None:
            rows.append({"venue_market_id": m["venue_market_id"], **forecast})
    logger.info(
        "scored %d/%d in-support open markets", len(rows), len(markets)
    )
    return rows


def snapshot_path(data_dir: Path, now_ts: int) -> Path:
    date = time.strftime("%Y-%m-%d", time.gmtime(now_ts))
    return Path(data_dir) / "curated" / "forecasts" / f"date={date}" / "part-0.parquet"


def write_snapshot(
    data_dir: Path, rows: list[dict[str, Any]], now_ts: int
) -> Path | None:
    """First scoring run of the UTC day persists its forecasts to parquet;
    later runs are no-ops (see module docstring). Atomic tmp+rename."""
    from .. import duck  # local import: keep score importable without duckdb

    if not rows:
        return None
    path = snapshot_path(data_dir, now_ts)
    if path.exists():
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / ".part-0.tmp.parquet"
    # Daemon-resident (model_score runs inside `pmkt run`): guardrailed spill
    # dir + memory ceiling, never bare duckdb.connect() defaults (cwd-relative
    # ".tmp" spill, 80%-of-RAM limit — incident 2026-07-12→15).
    conn = duck.connect(Path(data_dir))
    try:
        conn.execute(
            """CREATE TEMP TABLE snap (
                   venue_id VARCHAR, venue_market_id VARCHAR,
                   model_version VARCHAR, model_p VARCHAR, base_price VARCHAR,
                   horizon_bucket VARCHAR, category_group VARCHAR,
                   created_ts BIGINT)"""
        )
        conn.executemany(
            "INSERT INTO snap VALUES (?,?,?,?,?,?,?,?)",
            [
                (
                    VENUE_ID,
                    r["venue_market_id"],
                    r["model_version"],
                    r["model_p"],
                    r["base_price"],
                    r["horizon_bucket"],
                    r["category_group"],
                    now_ts,
                )
                for r in rows
            ],
        )
        conn.execute(
            f"COPY (SELECT * FROM snap ORDER BY venue_market_id) TO '{tmp}' "
            "(FORMAT parquet, COMPRESSION zstd)"
        )
    finally:
        conn.close()
    tmp.replace(path)
    logger.info("forecast snapshot: %d rows -> %s", len(rows), path)
    return path


def score_once(data_dir: Path, store: HotStore, now_ts: int | None = None) -> dict[str, int]:
    """Load CURRENT artifact, score, upsert forecasts, snapshot. Sync."""
    now_ts = int(time.time()) if now_ts is None else now_ts
    artifact = load_current_artifact(Path(data_dir))
    if artifact is None:
        logger.warning("model_score: no trained artifact (data/models/CURRENT) — skipping")
        return {"forecasts": 0, "snapshot": 0}
    rows = score_rows(store, artifact, now_ts)
    if rows:
        store.upsert_forecasts(VENUE_ID, rows, now_ts)
        pruned = store.prune_stale_forecasts(VENUE_ID, now_ts)
        if pruned:
            logger.info("pruned %d stale forecasts (left model support)", pruned)
    snap = write_snapshot(Path(data_dir), rows, now_ts)
    return {"forecasts": len(rows), "snapshot": int(snap is not None)}


async def model_score(client: Any, store: HotStore) -> dict[str, int]:
    """Run-loop job (hourly, scheduled AFTER collect_settlements). ``client``
    is unused — present to match the loop's job(client, store) signature."""
    from ..config import Config  # data_dir resolution identical to the CLI's

    return score_once(Config().data_dir, store)
