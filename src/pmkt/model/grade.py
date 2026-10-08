"""Grade logged forecasts against settled outcomes.

Joins the daily forecast snapshots (curated/forecasts parquet — written by
score.py, one file per UTC day) against settled yes/no outcomes in the hot
store, and reports Brier(model_p) vs Brier(base_price) by model_version /
category_group / horizon_bucket.

Only forecasts made >= 1h before close are graded: a forecast seconds before
close is trivially right and would flatter the model. Few graded rows exist
in the early days — that's fine, the log accrues.

DOUBLE math here is evaluation/display only, never money (the snapshots keep
the exact decimal strings). Grading output is not trading authorization
(data invariants).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MIN_LEAD_S = 3600  # forecast must precede close by at least this


def _snapshot_glob(data_dir: Path) -> str:
    return str(Path(data_dir) / "curated" / "forecasts" / "date=*" / "part-0.parquet")


_GRADE_SQL = """
WITH graded AS (
    SELECT
        f.model_version,
        f.category_group,
        f.horizon_bucket,
        CAST(f.model_p AS DOUBLE)     AS model_p,
        CAST(f.base_price AS DOUBLE)  AS base_p,
        CASE WHEN m.result = 'yes' THEN 1.0 ELSE 0.0 END AS y
    FROM read_parquet('{glob}') f
    JOIN hot.markets m
      ON m.venue_id = f.venue_id AND m.venue_market_id = f.venue_market_id
    WHERE m.status_norm = 'settled'
      AND m.result IN ('yes', 'no')
      AND f.created_ts <= m.close_ts - {min_lead}
)
SELECT model_version, category_group, horizon_bucket,
       count(*)                    AS n,
       avg((model_p - y) * (model_p - y)) AS brier_model,
       avg((base_p  - y) * (base_p  - y)) AS brier_market
FROM graded
GROUP BY GROUPING SETS (
    (model_version),
    (model_version, category_group),
    (model_version, horizon_bucket),
    (model_version, category_group, horizon_bucket)
)
ORDER BY model_version, category_group NULLS FIRST, horizon_bucket NULLS FIRST
"""


def grade(data_dir: Path) -> dict[str, Any]:
    """Report dict: {'rows': [...], 'n_graded': int}. Each row has
    model_version, category_group|None, horizon_bucket|None (None = subtotal
    across that dimension), n, brier_model, brier_market, brier_delta."""
    from .. import duck

    data_dir = Path(data_dir)
    if not list(
        (data_dir / "curated" / "forecasts").glob("date=*/part-0.parquet")
    ):
        return {"rows": [], "n_graded": 0, "note": "no forecast snapshots yet"}

    conn = duck.connect(data_dir)
    try:
        hot = data_dir / "db" / "hot.sqlite"
        conn.execute(f"ATTACH '{hot}' AS hot (TYPE sqlite, READ_ONLY)")
        raw_rows = conn.execute(
            _GRADE_SQL.format(glob=_snapshot_glob(data_dir), min_lead=MIN_LEAD_S)
        ).fetchall()
    finally:
        conn.close()

    rows = [
        {
            "model_version": mv,
            "category_group": cg,
            "horizon_bucket": hb,
            "n": n,
            "brier_model": bm,
            "brier_market": bk,
            "brier_delta": (bm - bk) if bm is not None and bk is not None else None,
        }
        for mv, cg, hb, n, bm, bk in raw_rows
    ]
    n_graded = sum(
        r["n"] for r in rows
        if r["category_group"] is None and r["horizon_bucket"] is None
    )
    return {"rows": rows, "n_graded": n_graded}


def format_report(report: dict[str, Any]) -> str:
    """Readable table for `pmkt model grade`."""
    if not report["rows"]:
        return f"no graded forecasts yet ({report.get('note', 'log accrues daily')})"
    lines = [
        f"graded forecasts: {report['n_graded']} "
        f"(settled yes/no, forecast >= {MIN_LEAD_S // 3600}h before close)",
        "",
        f"{'model_version':<24} {'category':<14} {'horizon':<8} "
        f"{'n':>7} {'brier_model':>12} {'brier_mkt':>12} {'delta':>9}",
    ]
    for r in report["rows"]:
        lines.append(
            f"{r['model_version']:<24} "
            f"{r['category_group'] or 'ALL':<14} "
            f"{r['horizon_bucket'] or 'ALL':<8} "
            f"{r['n']:>7} "
            f"{r['brier_model']:>12.5f} "
            f"{r['brier_market']:>12.5f} "
            f"{r['brier_delta']:>+9.5f}"
        )
    lines.append("")
    lines.append("delta < 0 = lower Brier score than the chosen baseline. Not trading authorization or evidence of executable profit.")
    return "\n".join(lines)
