"""Decision-time flow and price-proxy sensitivity study.

Compares lifetime flow with trailing flow ending at a specified decision time,
and contrasts final prices with decision-time last trades and a trade-derived
mid proxy. The proxy averages buy and sell VWAPs; it is not an observed order
book or executable entry price.

Wallet selection filters pre-2025 fill timestamps but does not require labels
to be resolved before that cutoff. This is a partial timing correction, not
proof of a fully ex-ante wallet cohort. No result from this script establishes
executable follower profit. No historical dataset is bundled.

Usage: uv run python backtests/ofi_decision_time_probe.py [H_hours] [W_hours] [MID_W_hours]
See docs/findings.md and backtests/README.md for interpretation limits.
"""
from __future__ import annotations

import sys
import duckdb

GLOB = "data/external/polymarket-v1/daily_aligned/*.parquet"
SPLIT = 1735689600  # 2025-01-01 UTC — train(<) / test(>=)
Z = 1.96
MIN_TOTAL_TRADES = 20   # match original universe
MIN_SKILLED_DT = 3      # live decide.py gate

H = int(float(sys.argv[1]) * 3600) if len(sys.argv) > 1 else 6 * 3600    # decision horizon before close
W = int(float(sys.argv[2]) * 3600) if len(sys.argv) > 2 else 24 * 3600   # trailing OFI (flow) window at t0
MID_W = int(float(sys.argv[3]) * 3600) if len(sys.argv) > 3 else 3600    # SHORT price window (de-bounce, low lag)


def p(msg=""):
    print(msg, flush=True)


con = duckdb.connect()
con.execute("SET threads=4")
con.execute("PRAGMA temp_directory='data/tmp_duck'")

p(f"Decision-time OFI probe — H={H/3600:.0f}h before close, W={W/3600:.0f}h trailing window")
p(f"corpus={GLOB}  split={SPLIT}\n")

# ── Step 1: pre-2025 skill z-scores (identical recipe to skill_flow_backtest) ──
p("Step 1: pre-2025 skill scores (taker+maker unfolded)...")
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
trades AS (SELECT wallet, sign * usdc_amount * (won - price) / price AS pnl FROM unfolded)
SELECT wallet,
  CASE WHEN sqrt(sum(pnl*pnl)) > 0 THEN sum(pnl)/sqrt(sum(pnl*pnl)) ELSE 0 END AS z
FROM trades GROUP BY wallet HAVING count(*) >= 10
""")
nsk = con.execute(f"SELECT count(*) FILTER (WHERE z>{Z}), count(*) FROM skills").fetchone()
p(f"  {nsk[1]:,} wallets, {nsk[0]:,} skilled\n")

# ── Step 2: single scan → per-outcome life + decision-time signals ────────────
p("Step 2: per-outcome signals (one scan: whole-life + decision-time)...")
flo = f"(epoch(close_at) - {H} - {W})"    # flow-window start
t0 = f"(epoch(close_at) - {H})"            # decision time
mlo = f"(epoch(close_at) - {H} - {MID_W})" # short price-window start
inflow = f"block_timestamp > {flo} AND block_timestamp <= {t0}"
inmid = f"block_timestamp > {mlo} AND block_timestamp <= {t0}"
con.execute(f"""
CREATE TEMP TABLE sig AS
WITH tt AS (
  SELECT condition_id, outcome_label, winning_outcome_label, close_at, category,
         block_timestamp, taker, taker_direction, usdc_amount, price,
         CASE WHEN taker_direction='BUY' THEN 1 ELSE -1 END AS taker_sign
  FROM read_parquet('{GLOB}')
  WHERE block_timestamp >= {SPLIT}
    AND winning_outcome_label IS NOT NULL AND winning_outcome_label != ''
    AND price > 0 AND price < 1 AND usdc_amount > 0 AND close_at IS NOT NULL
),
wt AS (
  SELECT tt.*, CASE WHEN s.z > {Z} THEN 1 ELSE 0 END AS is_skilled
  FROM tt LEFT JOIN skills s ON s.wallet = tt.taker
)
SELECT
  condition_id, outcome_label,
  MAX(CASE WHEN outcome_label = winning_outcome_label THEN 1 ELSE 0 END) AS y,
  any_value(category) AS category,
  COUNT(*) AS n_total,
  -- ORIGINAL whole-life
  arg_max(price, block_timestamp) AS p_last_settle,
  SUM(taker_sign*usdc_amount) FILTER (WHERE is_skilled=1)
    / NULLIF(SUM(usdc_amount) FILTER (WHERE is_skilled=1),0) AS ofi_life,
  -- DECISION-TIME price: last trade at or before t0 (instantaneous, bounces)
  arg_max(price, block_timestamp) FILTER (WHERE block_timestamp <= {t0}) AS p0_dt,
  -- SHORT-window mid proxy (low lag, de-bounced): buy vwap (≈ask) & sell vwap (≈bid)
  SUM(price*usdc_amount) FILTER (WHERE taker_direction='BUY'  AND {inmid})
    / NULLIF(SUM(usdc_amount) FILTER (WHERE taker_direction='BUY'  AND {inmid}),0) AS buy_vwap,
  SUM(price*usdc_amount) FILTER (WHERE taker_direction='SELL' AND {inmid})
    / NULLIF(SUM(usdc_amount) FILTER (WHERE taker_direction='SELL' AND {inmid}),0) AS sell_vwap,
  -- de-bounced INSTANTANEOUS mid: mid of the last buy & last sell at/ before t0
  arg_max(price, block_timestamp) FILTER (WHERE taker_direction='BUY'  AND block_timestamp <= {t0}) AS last_buy,
  arg_max(price, block_timestamp) FILTER (WHERE taker_direction='SELL' AND block_timestamp <= {t0}) AS last_sell,
  -- DECISION-TIME skilled OFI over trailing FLOW window (t0-W, t0]
  SUM(taker_sign*usdc_amount) FILTER (WHERE is_skilled=1 AND {inflow})
    / NULLIF(SUM(usdc_amount) FILTER (WHERE is_skilled=1 AND {inflow}),0) AS ofi_dt,
  COUNT(*) FILTER (WHERE is_skilled=1 AND {inflow}) AS n_sk_dt
FROM wt
GROUP BY condition_id, outcome_label
HAVING COUNT(*) >= {MIN_TOTAL_TRADES}
""")
tot = con.execute("SELECT count(*) FROM sig").fetchone()[0]
p(f"  {tot:,} test outcomes (>= {MIN_TOTAL_TRADES} trades)\n")

# Two clean mid proxies (fallback to decision-time last trade when a side missing):
#   p_mid   = short-window (MID_W) buy/sell VWAP mid  (low lag, de-bounced)
#   p_mid_i = mid of the last buy & last sell         (instantaneous, de-bounced)
con.execute("""
CREATE TEMP TABLE ev AS
SELECT *,
  CASE WHEN buy_vwap IS NOT NULL AND sell_vwap IS NOT NULL
       THEN (buy_vwap + sell_vwap)/2.0 ELSE p0_dt END AS p_mid,
  CASE WHEN last_buy IS NOT NULL AND last_sell IS NOT NULL
       THEN (last_buy + last_sell)/2.0 ELSE p0_dt END AS p_mid_i
FROM sig
""")

def corr(expr_x, expr_y, where):
    return con.execute(
        f"SELECT corr({expr_x}, {expr_y}), count(*) FROM ev WHERE {where}"
    ).fetchone()

p("=== ATTRIBUTION: corr(skilled OFI, outcome residual) ===")
# (1) reproduce original: whole-life flow vs near-settlement last price
c1 = corr("ofi_life", "y - p_last_settle",
          "ofi_life IS NOT NULL AND p_last_settle > 0.01 AND p_last_settle < 0.99")
p(f"  (1) ofi_life vs y - p_last_settle    : {c1[0]:+.4f}   n={c1[1]:,}   [reproduce original ~+0.045]")
# (2) whole-life flow but decision-time price (still has flow lookahead)
c2 = corr("ofi_life", "y - p0_dt",
          "ofi_life IS NOT NULL AND p0_dt > 0.01 AND p0_dt < 0.99")
p(f"  (2) ofi_life vs y - p0_dt            : {c2[0]:+.4f}   n={c2[1]:,}   [price->decision-time; flow still lookahead]")
w_dt = f"ofi_dt IS NOT NULL AND n_sk_dt >= {MIN_SKILLED_DT} AND p0_dt > 0.01 AND p0_dt < 0.99"
w_mid = f"ofi_dt IS NOT NULL AND n_sk_dt >= {MIN_SKILLED_DT} AND p_mid > 0.01 AND p_mid < 0.99"
w_midi = f"ofi_dt IS NOT NULL AND n_sk_dt >= {MIN_SKILLED_DT} AND p_mid_i > 0.01 AND p_mid_i < 0.99"
p("  -- HONEST (trailing flow cut at t0, no lookahead): --")
# (3) last-trade base (instantaneous, bounces -> dilutes corr)
c3 = corr("ofi_dt", "y - p0_dt", w_dt)
p(f"  (3) ofi_dt   vs y - p0_dt            : {c3[0]:+.4f}   n={c3[1]:,}   [base=last trade @t0 (bouncy)]")
# (4a) SHORT-window vwap mid (low lag)
c4 = corr("ofi_dt", "y - p_mid", w_mid)
p(f"  (4a) ofi_dt  vs y - p_mid (1h vwap)  : {c4[0]:+.4f}   n={c4[1]:,}   [base=short-window MID]")
# (4b) instantaneous de-bounced mid (last buy/last sell) — the cleanest baseline
c4i = corr("ofi_dt", "y - p_mid_i", w_midi)
p(f"  (4b) ofi_dt  vs y - p_mid_i (inst.)  : {c4i[0]:+.4f}   n={c4i[1]:,}   [base=INSTANT de-bounced MID] <- cleanest")

# ── LEAKAGE CHECK: does OFI just predict the price proxy's own lag? ───────────
# If a mid proxy LAGS the true t0 price, directional OFI mechanically "predicts"
# (fresh_price - lagging_mid). p0_dt is the freshest point estimate at t0, so
# corr(ofi_dt, p0_dt - p_mid) ~ 0 means the proxy is not lagging (clean).
p("\n=== LEAKAGE CHECK: corr(ofi_dt, p0_dt - mid)  (~0 = proxy not lagging) ===")
lk = corr("ofi_dt", "p0_dt - p_mid", w_mid)
lki = corr("ofi_dt", "p0_dt - p_mid_i", w_midi)
p(f"  short-window vwap mid : {lk[0]:+.4f}   (inflates 4a if >>0)")
p(f"  instantaneous mid     : {lki[0]:+.4f}   (inflates 4b if >>0)")

mags = con.execute(f"""
  SELECT avg(abs(y - p_last_settle)) FILTER (WHERE p_last_settle>0.01 AND p_last_settle<0.99),
         avg(abs(y - p0_dt))         FILTER (WHERE {w_dt}),
         avg(abs(y - p_mid_i))       FILTER (WHERE {w_midi})
  FROM ev
""").fetchone()
p(f"\n  mean |residual|: settlement={mags[0]:.3f}  decision-p0={mags[1]:.3f}  decision-mid_i={mags[2]:.3f}")

# ── Brier: does OFI improve the decision-time baseline? ───────────────────────
def brier_grid(base_col, where, label):
    p(f"\n=== BRIER: {label} (base={base_col}) ===")
    best = None
    for a10 in range(-6, 13):
        a = a10 / 20.0
        row = con.execute(f"""
          SELECT avg(power(y - LEAST(0.99, GREATEST(0.01, {base_col} + {a}*ofi_dt)),2)),
                 avg(power(y - {base_col},2)), count(*)
          FROM ev WHERE {where}
        """).fetchone()
        ba, bm, n = row
        if best is None or ba < best[1]:
            best = (a, ba, bm, n)
        if a10 % 2 == 0:
            p(f"  alpha={a:+.2f}  Brier_adj={ba:.6f}  Brier_base={bm:.6f}  d={ba-bm:+.6f}")
    d = best[1] - best[2]
    pct = d/best[2]*100 if best[2] else 0
    p(f"  BEST alpha={best[0]:+.2f}: Brier {best[1]:.6f} vs base {best[2]:.6f}  "
      f"({'IMPROVES' if d < -1e-5 else 'no improvement'} {pct:+.2f}%)  n={best[3]:,}")
    return best

brier_grid("p0_dt", w_dt, "decision-time last-trade base")
brier_grid("p_mid_i", w_midi, "decision-time INSTANT MID proxy base")

# ── per-category (instant-mid base) — where, if anywhere, does it survive? ────
p("\n=== per-category corr(ofi_dt, y - p_mid_i), n>=200 ===")
for row in con.execute(f"""
  SELECT category, corr(ofi_dt, y - p_mid_i) c, count(*) n
  FROM ev WHERE {w_midi} GROUP BY category HAVING count(*) >= 200 ORDER BY n DESC LIMIT 12
""").fetchall():
    cat, c, n = row
    p(f"  {str(cat)[:22]:<22} corr={c:+.4f}  n={n:,}")

p("\n=== VERDICT ===")
p(f"original-style (1): {c1[0]:+.4f}   ->   honest decision-time instant-mid (4b): {c4i[0]:+.4f}")
p(f"leakage (should be ~0): corr(ofi, p0-mid_i) = {lki[0]:+.4f}")
