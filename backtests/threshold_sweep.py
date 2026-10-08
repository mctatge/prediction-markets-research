"""Exploratory fee-aware threshold sensitivity study.

Buckets skilled-wallet flow into decision windows and compares modeled fees,
spreads, and entry thresholds. The historical corpus contains trade prints,
not executable order books. Maker fills at stale prints can manufacture
apparent profits; Kalshi fee scenarios applied to Polymarket prices are
transfer assumptions, not Kalshi executions. Wallet selection filters fill
timestamps without requiring labels to resolve before selection.

The study can expose sensitivity and failure modes. It cannot certify positive
expected profit. Entry-price freshness, fill probability, historical fee
regimes, selection-time labels, and correlated outcomes require separate
validation. No historical dataset is bundled. Large scans may use gigabytes
of memory and temporary disk; this is not part of the bounded offline demo.

Usage: uv run python backtests/threshold_sweep.py
See docs/methodology.md and backtests/README.md.
"""
from __future__ import annotations

import math
import sys
import time

import duckdb

sys.path.insert(0, "src")
from pmkt.model.fees import KALSHI_TAKER_RATE, pm_fee_rate  # noqa: E402

GLOB = "data/external/polymarket-v1/daily_aligned/*.parquet"
SPLIT = 1_735_689_600       # 2025-01-01 UTC — train/test boundary
VALIDATE = 1_759_276_800    # 2025-10-01 UTC — derive/validate boundary
Z = 1.96
BUCKET_S = 21_600           # 6h; 24h window = 4 buckets
MIN_SKILLED_TRADES = 3      # live ofi.py gate
PRICE_LO, PRICE_HI = 0.05, 0.95
CUTOFFS = [round(c / 10, 1) for c in range(1, 10)]
HALF_SPREAD = 0.01          # taker sensitivity: entry at p ± δ

# Category groups (report unit). 'sports' mirrors the deepening backtest's
# signal-bearing group; everything else is a control.
SPORTS = ("Sports", "Tennis", "Basketball", "Soccer", "Baseball", "Hockey")

t0 = time.time()
con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=4")
con.execute("SET preserve_insertion_order=false")


def p(msg: str = "") -> None:
    print(f"[{time.time()-t0:7.1f}s] {msg}" if msg else "", flush=True)


# ── Step 1: train-period skill scores (identical to skill_flow_backtest) ──
p("Step 1: pre-2025 skill scores...")
con.execute(f"""
CREATE TEMP TABLE skills AS
WITH resolved AS (
  SELECT taker, maker,
    CASE WHEN outcome_label = winning_outcome_label THEN 1 ELSE 0 END AS won,
    CASE WHEN taker_direction = 'BUY' THEN 1 ELSE -1 END AS taker_sign,
    usdc_amount, price
  FROM read_parquet('{GLOB}')
  WHERE block_timestamp < {SPLIT}
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
SELECT wallet,
  CASE WHEN sqrt(sum(pnl*pnl)) > 0 THEN sum(pnl)/sqrt(sum(pnl*pnl)) ELSE 0 END AS z
FROM trades GROUP BY wallet HAVING count(*) >= 10
""")
(n_skilled,) = con.execute(
    f"SELECT count(*) FILTER (WHERE z > {Z}) FROM skills"
).fetchone()
p(f"  {n_skilled:,} skilled wallets")

# ── Step 2: 6h buckets per test outcome ───────────────────────────────────
p("Step 2: bucketing test-period trades (6h bins)...")
con.execute(f"""
CREATE TEMP TABLE buckets AS
WITH test_trades AS (
  SELECT condition_id, outcome_label, category,
    CASE WHEN outcome_label = winning_outcome_label THEN 1 ELSE 0 END AS y,
    CASE WHEN taker_direction = 'BUY' THEN 1 ELSE -1 END AS taker_sign,
    usdc_amount, price, block_timestamp,
    CAST(block_timestamp // {BUCKET_S} AS BIGINT) AS bkt,
    COALESCE(s.z, 0) > {Z} AS is_skilled
  FROM read_parquet('{GLOB}') t
  LEFT JOIN skills s ON s.wallet = t.taker
  WHERE block_timestamp >= {SPLIT}
    AND winning_outcome_label IS NOT NULL AND winning_outcome_label != ''
    AND price > 0 AND price < 1 AND usdc_amount > 0
)
SELECT condition_id, outcome_label, bkt,
  MAX(category)                                            AS category,
  MAX(y)                                                   AS y,
  SUM(usdc_amount) FILTER (WHERE is_skilled AND taker_sign = 1)  AS sk_buy,
  SUM(usdc_amount) FILTER (WHERE is_skilled AND taker_sign = -1) AS sk_sell,
  COUNT(*) FILTER (WHERE is_skilled)                       AS sk_n,
  arg_max(price, block_timestamp)                          AS p_close
FROM test_trades
GROUP BY condition_id, outcome_label, bkt
""")
(n_bkt,) = con.execute("SELECT count(*) FROM buckets").fetchone()
p(f"  {n_bkt:,} (outcome, bucket) rows")

# ── Step 3: trailing-24h decision candidates ──────────────────────────────
p("Step 3: rolling 24h windows -> decision candidates...")
con.execute(f"""
CREATE TEMP TABLE candidates AS
WITH rolled AS (
  SELECT condition_id, outcome_label, category, y, bkt,
    SUM(COALESCE(sk_buy, 0)) OVER w  AS w_buy,
    SUM(COALESCE(sk_sell, 0)) OVER w AS w_sell,
    SUM(sk_n) OVER w                 AS w_n,
    LAST_VALUE(p_close) OVER w       AS p_entry,
    MAX(bkt) OVER (PARTITION BY condition_id, outcome_label) AS last_bkt
  FROM buckets
  WINDOW w AS (
    PARTITION BY condition_id, outcome_label ORDER BY bkt
    RANGE BETWEEN 3 PRECEDING AND CURRENT ROW
  )
)
SELECT condition_id, outcome_label, category, y, bkt, p_entry, w_n,
  (w_buy - w_sell) / NULLIF(w_buy + w_sell, 0) AS ofi
FROM rolled
WHERE bkt < last_bkt                    -- final-bucket exclusion
  AND w_n >= {MIN_SKILLED_TRADES}
  AND (w_buy + w_sell) > 0
  AND p_entry > {PRICE_LO} AND p_entry < {PRICE_HI}
""")
(n_cand,) = con.execute("SELECT count(*) FROM candidates").fetchone()
p(f"  {n_cand:,} gate-passing decision candidates")

# ── Step 4: sweep cutoffs × modes × halves ────────────────────────────────
# One position per condition_id: earliest qualifying candidate either
# direction. Executed side: OFI>0 buys the outcome at p; OFI<0 buys the
# complement at 1-p (won_exec = 1-y). Fees on the EXECUTED price.
p("Step 4: sweeping cutoffs...")

MODES = {
    "pm_taker": ("pm", 1.0, HALF_SPREAD),
    "pm_maker": ("pm", 0.0, 0.0),
    "kalshi_taker": ("kalshi", 1.0, HALF_SPREAD),
    "kalshi_maker": ("kalshi", 0.25, 0.0),
}

con.create_function(
    "pm_rate", lambda c: pm_fee_rate(c), ["VARCHAR"], "DOUBLE"
)


def sweep(cutoff: float, venue: str, fee_mult: float, delta: float):
    """Per (group, half): n, mean pnl/$, sd — one row per condition_id."""
    rate_expr = (
        "pm_rate(category)" if venue == "pm" else str(KALSHI_TAKER_RATE)
    )
    return con.execute(f"""
WITH firsts AS (
  SELECT *,
    row_number() OVER (PARTITION BY condition_id ORDER BY bkt, outcome_label) AS rn
  FROM candidates
  WHERE ABS(ofi) >= {cutoff}
),
decided AS (
  SELECT
    category,
    CASE WHEN category IN {SPORTS!r} THEN 'sports'
         WHEN category IN ('Esports', 'Games') THEN 'esports'
         ELSE 'other' END AS grp,
    CASE WHEN bkt * {BUCKET_S} < {VALIDATE} THEN 'derive' ELSE 'validate' END AS half,
    CASE WHEN ofi > 0 THEN LEAST({PRICE_HI}, p_entry + {delta})
         ELSE LEAST({PRICE_HI}, (1.0 - p_entry) + {delta}) END AS px,
    CASE WHEN ofi > 0 THEN y ELSE 1 - y END AS won
  FROM firsts WHERE rn = 1
),
pnl AS (
  SELECT grp, half,
    (won - px - {fee_mult} * {rate_expr} * px * (1.0 - px)) / px AS ret
  FROM decided WHERE px > 0 AND px < 1
)
SELECT grp, half, count(*), avg(ret), stddev(ret)
FROM pnl GROUP BY grp, half
""").fetchall()


def ci_lo(mean: float, sd: float, n: int) -> float:
    return mean - 1.96 * sd / math.sqrt(n) if n > 1 and sd is not None else float("-inf")


results: dict[tuple, dict] = {}
for cutoff in CUTOFFS:
    for mode, (venue, fee_mult, delta) in MODES.items():
        for grp, half, n, mean, sd in sweep(cutoff, venue, fee_mult, delta):
            results[(mode, cutoff, grp, half)] = {
                "n": n, "mean": mean, "sd": sd,
                "ci_lo": ci_lo(mean, sd, n),
            }
    p(f"  cutoff {cutoff:.1f} done")

# ── Report ────────────────────────────────────────────────────────────────
print()
print("=" * 78)
print("POST-FEE PnL PER DOLLAR vs |OFI| CUTOFF — sports group")
print("(one position per market, first qualifying 24h-window decision)")
print("=" * 78)
for mode in MODES:
    print(f"\n--- {mode} ---")
    print(f"{'cutoff':>7} | {'derive':^30} | {'validate':^30}")
    print(f"{'':>7} | {'n':>7} {'pnl/$':>9} {'ci_lo':>9} | {'n':>7} {'pnl/$':>9} {'ci_lo':>9}")
    for cutoff in CUTOFFS:
        d = results.get((mode, cutoff, "sports", "derive"), {})
        v = results.get((mode, cutoff, "sports", "validate"), {})
        print(
            f"{cutoff:>7.1f} | {d.get('n', 0):>7,} {d.get('mean', float('nan')):>+9.4f} "
            f"{d.get('ci_lo', float('nan')):>+9.4f} | {v.get('n', 0):>7,} "
            f"{v.get('mean', float('nan')):>+9.4f} {v.get('ci_lo', float('nan')):>+9.4f}"
        )

print("\n" + "=" * 78)
print("VERDICT (sports): smallest cutoff with 95% CI lower bound > 0 in BOTH halves")
print("=" * 78)
any_theta = False
for mode in MODES:
    theta = None
    for cutoff in CUTOFFS:
        d = results.get((mode, cutoff, "sports", "derive"))
        v = results.get((mode, cutoff, "sports", "validate"))
        if (
            d and v and d["n"] >= 100 and v["n"] >= 100
            and d["ci_lo"] > 0 and v["ci_lo"] > 0
        ):
            theta = cutoff
            break
    if theta is not None:
        d = results[(mode, theta, "sports", "derive")]
        v = results[(mode, theta, "sports", "validate")]
        print(
            f"  {mode:>13}: θ = {theta:.1f}  "
            f"(derive {d['mean']:+.4f}/$ n={d['n']:,}; "
            f"validate {v['mean']:+.4f}/$ n={v['n']:,})"
        )
        any_theta = True
    else:
        print(f"  {mode:>13}: No cutoff meets the criterion under this scenario")
if not any_theta:
    print("\n  => No mode meets the criterion under the modeled costs and inputs.")

print("\nControl-group comparison at cutoff 0.5:")
for grp in ("esports", "other"):
    for mode in ("pm_maker",):
        d = results.get((mode, 0.5, grp, "derive"), {})
        v = results.get((mode, 0.5, grp, "validate"), {})
        print(
            f"  {grp:>8}/{mode}: derive {d.get('mean', float('nan')):+.4f}/$ "
            f"(n={d.get('n', 0):,}), validate {v.get('mean', float('nan')):+.4f}/$ "
            f"(n={v.get('n', 0):,})"
        )
print(f"\nDone in {time.time()-t0:.1f}s")
