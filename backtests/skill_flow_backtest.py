"""Historical outcome-level skilled-wallet flow analysis.

Uses taker flow and outcome labels to compare aggregate signals with final
last-trade prices. The skill cohort filters pre-2025 fills without requiring
labels to resolve before selection. Flow across the whole outcome history is
not restricted to a prospective decision time. Results are exploratory
associations, not executable follower returns. No dataset is bundled.

Usage: uv run python backtests/skill_flow_backtest.py
See docs/methodology.md and backtests/README.md.
"""
from __future__ import annotations
import duckdb

GLOB = "data/external/polymarket-v1/daily_aligned/*.parquet"
SPLIT = 1735689600  # 2025-01-01 UTC
Z = 1.96

con = duckdb.connect()
con.execute("SET threads=4")

def p(msg=""): print(msg, flush=True)

# ── Step 1: train-period skill scores (reuse existing logic) ──────────────

p("Step 1: train-period skill scores...")
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
  CASE WHEN sqrt(sum(pnl*pnl)) > 0 THEN sum(pnl)/sqrt(sum(pnl*pnl)) ELSE 0 END AS z,
  count(*) AS n
FROM trades GROUP BY wallet HAVING count(*) >= 10
""")
stats = con.execute(f"""
  SELECT count(*),
    count(*) FILTER (WHERE z > {Z}),
    count(*) FILTER (WHERE z < -{Z})
  FROM skills
""").fetchone()
p(f"  {stats[0]:,} wallets, {stats[1]:,} skilled, {stats[2]:,} anti-skilled")

# ── Step 2: outcome-level signals (taker-only, no window functions) ───────

p("Step 2a: per-outcome last-trade price...")
con.execute(f"""
CREATE TEMP TABLE last_price AS
WITH numbered AS (
  SELECT condition_id, outcome_label, price,
    row_number() OVER (
      PARTITION BY condition_id, outcome_label
      ORDER BY block_timestamp DESC
    ) AS rn
  FROM read_parquet('{GLOB}')
  WHERE block_timestamp >= {SPLIT}
    AND winning_outcome_label IS NOT NULL AND winning_outcome_label != ''
    AND price > 0 AND price < 1
)
SELECT condition_id, outcome_label, price AS p_last
FROM numbered WHERE rn = 1
""")
n_lp = con.execute("SELECT count(*) FROM last_price").fetchone()[0]
p(f"  {n_lp:,} (condition_id, outcome) pairs with last-trade price")

p("Step 2b: per-outcome skilled OFI + ground truth...")
con.execute(f"""
CREATE TEMP TABLE outcome_signals AS
WITH test_trades AS (
  SELECT condition_id, outcome_label, winning_outcome_label,
    taker,
    CASE WHEN taker_direction = 'BUY' THEN 1 ELSE -1 END AS taker_sign,
    usdc_amount, price
  FROM read_parquet('{GLOB}')
  WHERE block_timestamp >= {SPLIT}
    AND winning_outcome_label IS NOT NULL AND winning_outcome_label != ''
    AND price > 0 AND price < 1 AND usdc_amount > 0
),
with_tier AS (
  SELECT t.*,
    COALESCE(s.z, 0) AS wallet_z,
    CASE WHEN s.z > {Z} THEN 'skilled'
         WHEN s.z < -{Z} THEN 'anti'
         ELSE 'unskilled' END AS tier
  FROM test_trades t
  LEFT JOIN skills s ON s.wallet = t.taker
)
SELECT
  w.condition_id, w.outcome_label,
  CASE WHEN MAX(w.winning_outcome_label) = w.outcome_label THEN 1 ELSE 0 END AS y,
  lp.p_last,
  COUNT(*) AS n_trades,
  -- Taker OFI by tier: net buy/sell direction, volume-weighted
  SUM(w.taker_sign * w.usdc_amount) FILTER (WHERE tier='skilled')
    / NULLIF(SUM(w.usdc_amount) FILTER (WHERE tier='skilled'), 0) AS skilled_ofi,
  SUM(w.taker_sign * w.usdc_amount) FILTER (WHERE tier='unskilled')
    / NULLIF(SUM(w.usdc_amount) FILTER (WHERE tier='unskilled'), 0) AS unskilled_ofi,
  SUM(w.taker_sign * w.usdc_amount) FILTER (WHERE tier='anti')
    / NULLIF(SUM(w.usdc_amount) FILTER (WHERE tier='anti'), 0) AS anti_ofi,
  -- Z-weighted OFI (skilled only)
  SUM(w.wallet_z * w.taker_sign * w.usdc_amount) FILTER (WHERE tier='skilled')
    / NULLIF(SUM(ABS(w.wallet_z) * w.usdc_amount) FILTER (WHERE tier='skilled'), 0) AS z_ofi,
  SUM(w.usdc_amount) FILTER (WHERE tier='skilled') AS skilled_vol
FROM with_tier w
JOIN last_price lp ON lp.condition_id = w.condition_id AND lp.outcome_label = w.outcome_label
GROUP BY w.condition_id, w.outcome_label, lp.p_last
HAVING COUNT(*) >= 20
""")

summary = con.execute("""
  SELECT count(*), avg(y), count(*) FILTER (WHERE skilled_ofi IS NOT NULL), avg(p_last)
  FROM outcome_signals
""").fetchone()
p(f"  {summary[0]:,} outcomes, base rate={summary[1]:.3f}, "
  f"{summary[2]:,} with skilled participation, avg p_last={summary[3]:.3f}")

# ── Test B: correlations ──────────────────────────────────────────────────

p("\n=== TEST B: SKILLED OFI vs OUTCOME RESIDUAL ===")
corrs = con.execute("""
  SELECT
    corr(skilled_ofi, y) AS c_sy,
    corr(skilled_ofi, y - p_last) AS c_sr,
    corr(unskilled_ofi, y - p_last) AS c_ur,
    corr(anti_ofi, y - p_last) AS c_ar,
    corr(z_ofi, y - p_last) AS c_zr,
    count(*) AS n
  FROM outcome_signals
  WHERE skilled_ofi IS NOT NULL AND p_last > 0.01 AND p_last < 0.99
""").fetchone()

p(f"  N outcomes with skilled participation: {corrs[5]:,}")
p(f"  corr(skilled_ofi, y):              {corrs[0]:+.4f}")
p(f"  corr(skilled_ofi, y - p_last):     {corrs[1]:+.4f}  <- KEY")
p(f"  corr(unskilled_ofi, y - p_last):   {corrs[2]:+.4f}")
p(f"  corr(anti_ofi, y - p_last):        {corrs[3]:+.4f}")
p(f"  corr(z_weighted_ofi, y - p_last):  {corrs[4]:+.4f}")

# Quintile breakdown
p("\n  Skilled OFI quintile -> outcome rate vs market price:")
p(f"  {'Q':>3} {'mean_ofi':>9} {'P(win)':>8} {'p_mkt':>8} {'edge':>8} {'n':>8}")
for row in con.execute("""
  WITH q AS (
    SELECT *, ntile(5) OVER (ORDER BY skilled_ofi) AS qnt
    FROM outcome_signals
    WHERE skilled_ofi IS NOT NULL AND p_last > 0.01 AND p_last < 0.99
  )
  SELECT qnt, count(*), round(avg(skilled_ofi),3), round(avg(y),4),
    round(avg(p_last),4), round(avg(y)-avg(p_last),4)
  FROM q GROUP BY qnt ORDER BY qnt
""").fetchall():
    q, n, ofi, rate, mp, edge = row
    p(f"  Q{q:>1}  {ofi:>+8.3f}  {rate:>7.4f}  {mp:>7.4f}  {edge:>+7.4f}  {n:>7,}")

# ── Test C: Brier score ──────────────────────────────────────────────────

p("\n=== TEST C: BRIER — market price vs skill-adjusted ===")
p(f"  {'alpha':>8} {'Brier_adj':>10} {'Brier_mkt':>10} {'Delta':>10} {'pct':>8}")

results = []
for a10 in range(-10, 21):
    alpha = a10 / 20.0
    row = con.execute(f"""
      SELECT
        avg(power(y - LEAST(0.99, GREATEST(0.01, p_last + {alpha} * skilled_ofi)), 2)),
        avg(power(y - p_last, 2)),
        count(*)
      FROM outcome_signals
      WHERE skilled_ofi IS NOT NULL AND p_last > 0.01 AND p_last < 0.99
    """).fetchone()
    ba, bm, n = row
    d = ba - bm
    results.append((alpha, ba, bm, d, n))
    if a10 % 4 == 0 or abs(alpha) < 0.01:
        pct = d/bm*100 if bm else 0
        mark = " <- baseline" if abs(alpha) < 0.01 else ""
        p(f"  {alpha:>+8.3f} {ba:>10.6f} {bm:>10.6f} {d:>+10.6f} {pct:>+7.2f}%{mark}")

best = min(results, key=lambda r: r[1])
pct_imp = abs(best[3])/best[2]*100 if best[2] else 0
p(f"\n  Best alpha={best[0]:+.3f}: Brier {best[1]:.6f} vs market {best[2]:.6f}")
if best[3] < -0.0001:
    p(f"  -> Skill flow IMPROVES Brier by {pct_imp:.2f}% (Delta={best[3]:+.6f})")
else:
    p(f"  -> Skill flow does NOT improve Brier")

# ── Summary ──────────────────────────────────────────────────────────────

resid = corrs[1] if corrs[1] is not None else 0
p(f"\n=== VERDICT ===")
p(f"Test B: corr(skilled_ofi, y - p_last) = {resid:+.4f}")
p(f"Test C: best Brier delta = {best[3]:+.6f} at alpha={best[0]:+.3f} ({pct_imp:.2f}%)")
signal = (resid > 0.02) and (best[3] < -0.001)
p(f"{'SIGNAL: skilled flow adds info beyond market price' if signal else 'NO CLEAR SIGNAL beyond market price'}")
