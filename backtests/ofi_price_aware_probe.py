#!/usr/bin/env python3
"""Exploratory price-aware variants of skilled taker flow.

Compares outcome-level flow variants with final last-trade residuals and a
Brier-score parameter grid. Wallet labels come from a prebuilt skill table;
selection-time label availability is not enforced here. Full-outcome flow and
near-final price baselines are not prospective entry signals. Output cannot
establish executable follower returns. No historical dataset is bundled.

Usage: uv run python backtests/ofi_price_aware_probe.py
See docs/methodology.md and backtests/README.md.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pmkt.ui.ofi_live import is_sports_category  # noqa: E402

DATA_DIR = ROOT / "data"
V1_DIRS = [
    DATA_DIR / "curated" / "polymarket-v1" / "daily_aligned",
    DATA_DIR / "external" / "polymarket-v1" / "daily_aligned",
]
WALLET_SKILLS = DATA_DIR / "curated" / "wallet_skills" / "part-0.parquet"

TEST_RESOLVED_FROM = "2025-01-01"
MIN_SKILLED_TRADES = 3
Z_SKILLED = 1.96
ALPHA_GRID = [0.005, 0.01, 0.02, 0.05, 0.10, 0.25]


@dataclass
class VariantResult:
    name: str
    corr: float | None
    best_alpha: float | None
    delta_brier_pct: float | None
    n_outcomes: int
    sports_corr: float | None
    sports_n: int


def _v1_glob() -> str:
    for d in V1_DIRS:
        if d.is_dir() and any(d.glob("*.parquet")):
            files = sorted(d.glob("*.parquet"))
            return str(d / "*.parquet") if len(files) > 200 else ", ".join(
                f"'{f}'" for f in files
            )
    raise FileNotFoundError(
        "Polymarket-v1 daily_aligned not found under data/curated/ or data/external/"
    )


def _brier(y: float, p: float) -> float:
    return (y - p) ** 2


def _best_brier(
    rows: list[tuple[float, float, float, str]],
    ofi_idx: int,
) -> tuple[float | None, float | None]:
    """Return (best_alpha, delta_brier_pct) for one OFI column index."""
    usable = [
        (_f(y), _f(p), _f(ofis[ofi_idx]), cat)
        for y, p, *ofis, cat in rows
        if ofis[ofi_idx] is not None
    ]
    if not usable:
        return None, None

    market_brier = sum(_brier(y, p) for y, p, _, _ in usable) / len(usable)
    if market_brier == 0:
        return None, None

    best_alpha: float | None = None
    best_model_brier = market_brier

    for alpha in ALPHA_GRID:
        model_brier = 0.0
        for y, p, ofi, _ in usable:
            pred = min(1.0, max(0.0, p + alpha * ofi))
            model_brier += _brier(y, pred)
        model_brier /= len(usable)
        if model_brier <= best_model_brier:
            best_model_brier = model_brier
            best_alpha = alpha

    delta = 100.0 * (best_model_brier - market_brier) / market_brier
    return best_alpha, delta


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den_x = sum((x - mx) ** 2 for x in xs) ** 0.5
    den_y = sum((y - my) ** 2 for y in ys) ** 0.5
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def _f(x) -> float:
    return float(x) if x is not None else 0.0


def _corr_for(rows: list[tuple], ofi_idx: int, sports_only: bool = False) -> tuple[float | None, int]:
    xs: list[float] = []
    ys: list[float] = []
    for y, p, *ofis, cat in rows:
        ofi = ofis[ofi_idx]
        if ofi is None:
            continue
        if sports_only and not is_sports_category(cat):
            continue
        xs.append(_f(ofi))
        ys.append(_f(y) - _f(p))
    return _pearson(xs, ys), len(xs)


def _fetch_outcomes(conn: duckdb.DuckDBPyConnection, v1_glob: str) -> list[tuple]:
    sql = f"""
    WITH skilled AS (
      SELECT lower(wallet) AS wallet
      FROM read_parquet('{WALLET_SKILLS}')
      WHERE z_score > {Z_SKILLED}
    ),
    resolved AS (
      SELECT
        condition_id,
        outcome_label,
        price,
        block_timestamp,
        usdc_amount,
        D,
        lower(taker) AS taker,
        category_refined,
        winning_outcome_label,
        resolved_at
      FROM read_parquet('{v1_glob}')
      WHERE winning_outcome_label IS NOT NULL
        AND winning_outcome_label != ''
        AND price > 0 AND price < 1
        AND usdc_amount > 0
        AND resolved_at >= TIMESTAMP '{TEST_RESOLVED_FROM}'
    ),
    outcome_meta AS (
      SELECT
        condition_id,
        outcome_label,
        arg_max(price, block_timestamp) AS p_last,
        max(CASE WHEN outcome_label = winning_outcome_label THEN 1.0 ELSE 0.0 END) AS y,
        any_value(category_refined) AS category_refined
      FROM resolved
      GROUP BY condition_id, outcome_label
    ),
    skilled_trades AS (
      SELECT
        r.condition_id,
        r.outcome_label,
        r.price,
        r.usdc_amount,
        r.D,
        om.p_last,
        om.y,
        om.category_refined
      FROM resolved r
      INNER JOIN skilled s ON r.taker = s.wallet
      INNER JOIN outcome_meta om
        ON r.condition_id = om.condition_id
       AND r.outcome_label = om.outcome_label
    ),
    ofi AS (
      SELECT
        condition_id,
        outcome_label,
        any_value(y) AS y,
        any_value(p_last) AS p_last,
        any_value(category_refined) AS category_refined,
        count(*) AS n_skilled_trades,
        sum(D * usdc_amount) / sum(usdc_amount) AS standard_ofi,
        sum(D * usdc_amount * abs(price - p_last))
          / nullif(sum(usdc_amount * abs(price - p_last)), 0) AS dist_weighted_ofi,
        sum(CASE WHEN D * (p_last - price) > 0 THEN D * usdc_amount ELSE 0 END)
          / nullif(
              sum(CASE WHEN D * (p_last - price) > 0 THEN usdc_amount ELSE 0 END),
              0
            ) AS edge_filtered_ofi,
        sum(D * (price - p_last) * usdc_amount) / sum(usdc_amount) AS price_distance_signal
      FROM skilled_trades
      GROUP BY condition_id, outcome_label
      HAVING count(*) >= {MIN_SKILLED_TRADES}
    )
    SELECT
      y,
      p_last,
      standard_ofi,
      dist_weighted_ofi,
      edge_filtered_ofi,
      price_distance_signal,
      category_refined
    FROM ofi
    """
    return conn.execute(sql).fetchall()


def main() -> None:
    v1_glob = _v1_glob()
    print(f"v1 corpus: {v1_glob}")
    print(f"wallet skills: {WALLET_SKILLS}")
    print(f"test window: resolved >= {TEST_RESOLVED_FROM}, min skilled trades = {MIN_SKILLED_TRADES}")
    print()

    conn = duckdb.connect()
    t0 = time.time()
    print("Running DuckDB aggregation (full v1 scan)…")
    rows = _fetch_outcomes(conn, v1_glob)
    conn.close()
    print(f"  {len(rows):,} outcomes with skilled OFI in {time.time() - t0:.1f}s")
    print()

    variants = [
        ("standard", 0),
        ("dist_weighted", 1),
        ("edge_filtered", 2),
        ("price_distance_signal", 3),
    ]

    results: list[VariantResult] = []
    for name, idx in variants:
        corr, n = _corr_for(rows, idx)
        sports_corr, sports_n = _corr_for(rows, idx, sports_only=True)
        best_alpha, delta_brier = _best_brier(rows, idx)
        results.append(
            VariantResult(name, corr, best_alpha, delta_brier, n, sports_corr, sports_n)
        )

    # Summary table
    hdr = f"{'variant':<24} {'corr':>8} {'best_α':>8} {'ΔBrier%':>9} {'n':>8}  {'Sports corr':>12} {'Sports n':>9}"
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        corr_s = f"{r.corr:+.4f}" if r.corr is not None else "   n/a"
        alpha_s = f"{r.best_alpha:.3f}" if r.best_alpha is not None else "     —"
        db_s = f"{r.delta_brier_pct:+.2f}" if r.delta_brier_pct is not None else "      n/a"
        sc_s = f"{r.sports_corr:+.4f}" if r.sports_corr is not None else "         n/a"
        print(
            f"{r.name:<24} {corr_s:>8} {alpha_s:>8} {db_s:>9} {r.n_outcomes:>8,}  {sc_s:>12} {r.sports_n:>9,}"
        )

    print()
    std_sports = results[0].sports_corr
    print("Sports vs standard (+0.045 benchmark):")
    for r in results[1:]:
        if r.sports_corr is None or std_sports is None:
            verdict = "n/a"
        elif r.sports_corr > std_sports:
            verdict = f"BEATS standard ({r.sports_corr:+.4f} > {std_sports:+.4f})"
        else:
            verdict = f"does not beat standard ({r.sports_corr:+.4f} ≤ {std_sports:+.4f})"
        print(f"  {r.name}: {verdict}")

    # Per-category breakdown for all variants
    print()
    print("Per-category correlation (category_refined):")
    cats: dict[str, list[tuple]] = {}
    for row in rows:
        cat = row[6] or "Other"
        cats.setdefault(cat, []).append(row)

    cat_hdr = f"{'category':<16} {'n':>7}  " + "  ".join(f"{v[0][:8]:>8}" for v in variants)
    print(cat_hdr)
    print("-" * len(cat_hdr))
    for cat in sorted(cats, key=lambda c: -len(cats[c])):
        sub = cats[cat]
        corrs = []
        for _, idx in variants:
            c, _ = _corr_for(sub, idx)
            corrs.append(f"{c:+.3f}" if c is not None else "     n/a")
        print(f"{cat:<16} {len(sub):>7,}  " + "  ".join(f"{c:>8}" for c in corrs))


if __name__ == "__main__":
    main()
