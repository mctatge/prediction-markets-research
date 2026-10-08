"""Exploratory category, size-weighting, and recency variants of wallet flow.

Historical analysis with known limitations retained for inspection. Wallet
selection filters fill timestamps before 2025 but does not require outcomes
to have resolved before selection. Flow aggregation still uses the event-axis
D field; it is not consistently aligned to each trade's outcome token. The
baseline is the last trade across the full outcome history, and aggregate
flow is not restricted to a prospective decision window.

These limitations prevent interpreting output as an ex-ante follower strategy
or executable profit. Historical documentation previously overstated the
corrections present in this file; the implementation is preserved rather than
silently changing the experiment. No historical dataset is bundled.

Usage: uv run --extra research python backtests/ofi_deepening_backtest.py
See docs/methodology.md and backtests/README.md.
"""

import sys
from pathlib import Path
import time
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pmkt.lake import connect
from pmkt.config import Config

def main():
    t0 = time.time()
    conn = connect(Config().data_dir)
    print(f"[{time.time()-t0:.1f}s] connected to lake")

    # Step 1: skilled wallet hash set — pre-2025 fill timestamps (label cutoff is not enforced)
    conn.execute("""
        CREATE TEMP TABLE skilled_wallets AS
        WITH resolved AS (
          SELECT taker, maker,
            CASE WHEN outcome_label = winning_outcome_label THEN 1 ELSE 0 END AS won,
            CASE WHEN taker_direction = 'BUY' THEN 1 ELSE -1 END AS taker_sign,
            usdc_amount, price
          FROM pm_v1_trades
          WHERE block_timestamp < 1735689600
            AND winning_outcome_label IS NOT NULL AND winning_outcome_label != ''
            AND price > 0 AND price < 1 AND usdc_amount > 0
        ),
        unfolded AS (
          SELECT taker AS wallet, taker_sign AS sign, usdc_amount, price, won FROM resolved
          UNION ALL
          SELECT maker AS wallet, -taker_sign AS sign, usdc_amount, price, won FROM resolved
        ),
        trades AS (
          SELECT wallet, sign * usdc_amount * (won - price) / price AS pnl FROM unfolded
        )
        SELECT lower(wallet) AS wallet FROM trades GROUP BY wallet
        HAVING count(*) >= 10
           AND sum(pnl) / NULLIF(sqrt(sum(pnl*pnl)), 0) > 1.96
    """)
    n_skilled = conn.execute("SELECT COUNT(*) FROM skilled_wallets").fetchone()[0]
    print(f"[{time.time()-t0:.1f}s] {n_skilled:,} skilled wallets loaded")

    # Step 2: outcome baselines (p_last from ALL trades, one scan)
    print("computing p_last per outcome (full scan)...")
    conn.execute("""
        CREATE TEMP TABLE outcome_base AS
        SELECT
            condition_id,
            outcome_label,
            ANY_VALUE(winning_outcome_label) AS winning_outcome_label,
            ANY_VALUE(category_refined) AS category_refined,
            -- last trade price = market's probability estimate
            LAST(price ORDER BY block_timestamp) AS p_last,
            COUNT(*) AS n_total
        FROM pm_v1_trades
        WHERE resolved_at IS NOT NULL
          AND winning_outcome_label IS NOT NULL
          AND EXTRACT(YEAR FROM close_at) >= 2025
        GROUP BY condition_id, outcome_label
    """)
    n_outcomes = conn.execute("SELECT COUNT(*) FROM outcome_base").fetchone()[0]
    print(f"[{time.time()-t0:.1f}s] {n_outcomes:,} outcomes with p_last")

    # Step 3: skilled taker OFI variants (semi-join with 75k wallets)
    print("computing OFI variants from skilled taker trades...")
    conn.execute("""
        CREATE TEMP TABLE outcome_ofi AS
        SELECT
            condition_id,
            outcome_label,

            -- V1: event-axis OFI (known token-alignment limitation)
            SUM(D * usdc_amount) / NULLIF(SUM(usdc_amount), 0)
                AS ofi_standard,

            -- V2: Log-size weighted OFI
            SUM(D * ln(1.0 + usdc_amount)) / NULLIF(SUM(ln(1.0 + usdc_amount)), 0)
                AS ofi_log_size,

            -- V3: Exponential decay, half-life 6h (21600s)
            SUM(D * usdc_amount * exp(-0.6931 * (epoch(close_at) - block_timestamp) / 21600.0))
              / NULLIF(SUM(usdc_amount * exp(-0.6931 * (epoch(close_at) - block_timestamp) / 21600.0)), 0)
                AS ofi_decay_6h,

            -- V4: Exponential decay, half-life 24h (86400s)
            SUM(D * usdc_amount * exp(-0.6931 * (epoch(close_at) - block_timestamp) / 86400.0))
              / NULLIF(SUM(usdc_amount * exp(-0.6931 * (epoch(close_at) - block_timestamp) / 86400.0)), 0)
                AS ofi_decay_24h,

            -- V5: Window 6h (trades in last 6h before close only)
            SUM(D * usdc_amount) FILTER (
                WHERE epoch(close_at) - block_timestamp <= 21600
            ) / NULLIF(SUM(usdc_amount) FILTER (
                WHERE epoch(close_at) - block_timestamp <= 21600
            ), 0)
                AS ofi_window_6h,

            -- V6: Window 24h (trades in last 24h before close only)
            SUM(D * usdc_amount) FILTER (
                WHERE epoch(close_at) - block_timestamp <= 86400
            ) / NULLIF(SUM(usdc_amount) FILTER (
                WHERE epoch(close_at) - block_timestamp <= 86400
            ), 0)
                AS ofi_window_24h,

            -- V7: Log-size + decay 24h combo
            SUM(D * ln(1.0 + usdc_amount) * exp(-0.6931 * (epoch(close_at) - block_timestamp) / 86400.0))
              / NULLIF(SUM(ln(1.0 + usdc_amount) * exp(-0.6931 * (epoch(close_at) - block_timestamp) / 86400.0)), 0)
                AS ofi_log_decay_24h,

            COUNT(*) AS n_skilled,
            -- coverage stats for windowed variants
            COUNT(*) FILTER (WHERE epoch(close_at) - block_timestamp <= 21600) AS n_skilled_6h,
            COUNT(*) FILTER (WHERE epoch(close_at) - block_timestamp <= 86400) AS n_skilled_24h

        FROM (
          SELECT * EXCLUDE (D),
            -- ROW-aligned direction: +1 = taker buys THIS outcome row's
            -- token. The corpus D column is event-primary-axis (inverted
            -- for outcome_seq=2) — using it here flipped half the rows.
            CASE WHEN taker_direction = 'BUY' THEN 1 ELSE -1 END AS D
          FROM pm_v1_trades
        )
        WHERE resolved_at IS NOT NULL
          AND winning_outcome_label IS NOT NULL
          AND EXTRACT(YEAR FROM close_at) >= 2025
          AND lower(taker) IN (SELECT wallet FROM skilled_wallets)
        GROUP BY condition_id, outcome_label, close_at
        HAVING COUNT(*) >= 3
    """)
    n_ofi = conn.execute("SELECT COUNT(*) FROM outcome_ofi").fetchone()[0]
    print(f"[{time.time()-t0:.1f}s] {n_ofi:,} outcomes with OFI (>= 3 skilled trades)")

    # Step 4: join and pull into Python
    rows = conn.execute("""
        SELECT
            b.category_refined,
            CASE WHEN b.outcome_label = b.winning_outcome_label THEN 1.0 ELSE 0.0 END AS y,
            b.p_last,
            o.ofi_standard,
            o.ofi_log_size,
            o.ofi_decay_6h,
            o.ofi_decay_24h,
            o.ofi_window_6h,
            o.ofi_window_24h,
            o.ofi_log_decay_24h,
            o.n_skilled,
            o.n_skilled_6h,
            o.n_skilled_24h
        FROM outcome_base b
        INNER JOIN outcome_ofi o USING (condition_id, outcome_label)
        WHERE b.p_last > 0.005 AND b.p_last < 0.995
    """).fetchall()
    conn.close()
    print(f"[{time.time()-t0:.1f}s] {len(rows):,} rows pulled into Python")

    # Parse into arrays
    categories = np.array([r[0] or "Other" for r in rows])
    y = np.array([float(r[1]) for r in rows])
    p_last = np.array([float(r[2]) for r in rows])

    variant_names = [
        "standard", "log_size", "decay_6h", "decay_24h",
        "window_6h", "window_24h", "log_decay_24h",
    ]
    ofi = {}
    for i, name in enumerate(variant_names):
        vals = np.array([float(r[3 + i]) if r[3 + i] is not None else 0.0 for r in rows])
        ofi[name] = vals

    n_skilled = np.array([int(r[10]) for r in rows])
    n_skilled_6h = np.array([int(r[11]) for r in rows])
    n_skilled_24h = np.array([int(r[12]) for r in rows])

    unique_cats = sorted(set(categories))
    print(f"\nCategories: {unique_cats}")
    print(f"Outcomes per category:")
    for cat in unique_cats:
        mask = categories == cat
        print(f"  {cat:15s}  n={mask.sum():6d}  base_rate={y[mask].mean():.3f}")

    # ----------------------------------------------------------------
    # Grid search: alpha per (variant, category) + global
    # ----------------------------------------------------------------
    alphas = np.arange(0.01, 0.26, 0.01)

    def brier(p, y_):
        return np.mean((p - y_) ** 2)

    def search_alpha(p_base, ofi_vals, y_, label=""):
        """Find best alpha, return (best_alpha, brier_market, brier_model, delta)."""
        brier_mkt = brier(p_base, y_)
        best_alpha, best_brier = 0.0, brier_mkt
        for a in alphas:
            model_p = np.clip(p_base + a * ofi_vals, 0.0, 1.0)
            b = brier(model_p, y_)
            if b < best_brier:
                best_alpha, best_brier = a, b
        delta_pct = (best_brier - brier_mkt) / brier_mkt * 100
        return best_alpha, brier_mkt, best_brier, delta_pct

    # Correlation analysis first
    print("\n" + "=" * 80)
    print("CORRELATION: corr(ofi_variant, y - p_last)")
    print("=" * 80)
    residual = y - p_last
    header = f"{'variant':20s} {'global':>8s}"
    for cat in unique_cats:
        header += f" {cat[:8]:>8s}"
    print(header)
    for vname in variant_names:
        vals = ofi[vname]
        line = f"{vname:20s} {np.corrcoef(vals, residual)[0,1]:8.4f}"
        for cat in unique_cats:
            mask = categories == cat
            if mask.sum() > 10:
                line += f" {np.corrcoef(vals[mask], residual[mask])[0,1]:8.4f}"
            else:
                line += f" {'n/a':>8s}"
        print(line)

    # Full grid search
    print("\n" + "=" * 80)
    print("ALPHA GRID SEARCH: best alpha, Brier market, Brier model, % change")
    print("Exploratory fit criterion: Brier(model) <= Brier(market) per category")
    print("=" * 80)

    results = {}  # (variant, category_or_global) -> (alpha, brier_mkt, brier_mdl, delta%)

    for vname in variant_names:
        print(f"\n--- {vname} ---")
        vals = ofi[vname]

        # Global
        a, bm, bmod, d = search_alpha(p_last, vals, y, f"{vname}/global")
        gate = "PASS" if bmod <= bm else "FAIL"
        corr_val = np.corrcoef(vals, residual)[0, 1]
        print(f"  GLOBAL   n={len(y):6d}  alpha={a:.2f}  Brier mkt={bm:.5f}  "
              f"model={bmod:.5f}  delta={d:+.2f}%  corr={corr_val:+.4f}  [{gate}]")
        results[(vname, "GLOBAL")] = (a, bm, bmod, d, gate, len(y))

        # Per-category
        for cat in unique_cats:
            mask = categories == cat
            n = mask.sum()
            if n < 30:
                print(f"  {cat:15s}  n={n:6d}  SKIP (too few)")
                continue
            a, bm, bmod, d = search_alpha(p_last[mask], vals[mask], y[mask])
            gate = "PASS" if bmod <= bm else "FAIL"
            corr_val = np.corrcoef(vals[mask], residual[mask])[0, 1]
            print(f"  {cat:15s}  n={n:6d}  alpha={a:.2f}  Brier mkt={bm:.5f}  "
                  f"model={bmod:.5f}  delta={d:+.2f}%  corr={corr_val:+.4f}  [{gate}]")
            results[(vname, cat)] = (a, bm, bmod, d, gate, n)

    # ----------------------------------------------------------------
    # Summary: best variant per category
    # ----------------------------------------------------------------
    print("\n" + "=" * 80)
    print("BEST VARIANT PER CATEGORY (gate-passing only)")
    print("=" * 80)
    all_scopes = ["GLOBAL"] + unique_cats
    for scope in all_scopes:
        passing = []
        for vname in variant_names:
            key = (vname, scope)
            if key in results:
                a, bm, bmod, d, gate, n = results[key]
                if gate == "PASS":
                    passing.append((d, vname, a, bm, bmod, n))
        if not passing:
            print(f"  {scope:15s}  NO PASSING VARIANTS")
            continue
        passing.sort()  # most negative delta = most improvement
        best = passing[0]
        d, vname, a, bm, bmod, n = best
        print(f"  {scope:15s}  best={vname:20s}  alpha={a:.2f}  "
              f"delta={d:+.2f}%  n={n}")

    # ----------------------------------------------------------------
    # Exploratory parameters; not validated for prospective use
    # ----------------------------------------------------------------
    print("\n" + "=" * 80)
    print("EXPLORATORY PARAMETERS — NOT VALIDATED FOR EXECUTION")
    print("=" * 80)

    # For each category, pick the best passing variant.
    # If no variant passes for a category, fall back to global alpha.
    # The "standard" variant is the fallback for comparison with the historical baseline.
    print("\nPer-category alpha (best passing variant, fallback to global):")
    global_best = None
    for vname in variant_names:
        key = (vname, "GLOBAL")
        if key in results:
            a, bm, bmod, d, gate, n = results[key]
            if gate == "PASS" and (global_best is None or d < global_best[0]):
                global_best = (d, vname, a)

    if global_best:
        print(f"  GLOBAL fallback: variant={global_best[1]}, alpha={global_best[2]:.2f} "
              f"(delta={global_best[0]:+.2f}%)")
    else:
        print("  GLOBAL fallback: alpha=0.05 (historical baseline; no variant passes globally)")

    for cat in unique_cats:
        passing = []
        for vname in variant_names:
            key = (vname, cat)
            if key in results:
                a, bm, bmod, d, gate, n = results[key]
                if gate == "PASS":
                    passing.append((d, vname, a, n))
        if not passing:
            fb = f"alpha={global_best[2]:.2f}" if global_best else "alpha=0.05"
            print(f"  {cat:15s}  → fallback to global ({fb}) — no category-specific improvement")
        else:
            passing.sort()
            d, vname, a, n = passing[0]
            print(f"  {cat:15s}  → variant={vname:20s}  alpha={a:.2f}  "
                  f"delta={d:+.2f}%  n={n}")

    # ----------------------------------------------------------------
    # Window coverage analysis (for deciding on lookback_s)
    # ----------------------------------------------------------------
    print("\n" + "=" * 80)
    print("WINDOW COVERAGE: % of outcomes with >= 3 skilled trades in window")
    print("=" * 80)
    for cat in all_scopes:
        mask = categories == cat if cat != "GLOBAL" else np.ones(len(y), dtype=bool)
        n = mask.sum()
        if n < 30:
            continue
        cov_6h = (n_skilled_6h[mask] >= 3).sum() / n * 100
        cov_24h = (n_skilled_24h[mask] >= 3).sum() / n * 100
        cov_all = 100.0  # by construction (HAVING >= 3)
        print(f"  {cat:15s}  6h={cov_6h:5.1f}%  24h={cov_24h:5.1f}%  all={cov_all:5.1f}%")

    print(f"\n[{time.time()-t0:.1f}s] done")


if __name__ == "__main__":
    main()
