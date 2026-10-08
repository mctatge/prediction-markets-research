#!/usr/bin/env python3
"""Hold-to-resolution study of selected-wallet flow.

Selects wallets using trades placed and resolved before 2025-01-01, then
measures subsequent flow in March-June 2025. Outcome labels have an explicit
availability cutoff. Entry is the last observed trade at or before the
specified delay, not an executable ask. Settlement payoff is binary; entry
fees are modeled using historical assumptions.

A historical run reported about 1.89 cents per contract before fees across
17,706 events from a 221-wallet cohort. Per-dollar aggregate return was
negative. Mid-price-band analysis was selected after inspecting results and
is exploratory. Spreads of a few cents can absorb the apparent contract-level
margin. No historical dataset is bundled and this run is not reproduced by
the synthetic demo. A prospective paper ledger is needed to evaluate entry,
liquidity, latency, costs, and clustered uncertainty.

Usage: uv run python backtests/ofi_resolution_probe.py
See docs/findings.md and backtests/README.md.
"""
from __future__ import annotations

import glob
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
_V1_CANDIDATES = [
    DATA_DIR / "external" / "polymarket-v1" / "daily_aligned",
    DATA_DIR / "curated" / "polymarket-v1" / "daily_aligned",
]
TMP = DATA_DIR / ".duckdb-tmp"
TRAIN_YEARS = ["2022", "2023", "2024"]
TEST_PATTERN = "2025-0[3-6]-*.parquet"   # honest 4-month subset
EPOCH_2025 = 1735689600                   # 2025-01-01T00:00:00Z
Z = 1.96
MIN_TRAIN_TRADES = 20
DELAYS = [0, 300, 900, 3600]              # entry lag seconds: now / 5m / 15m / 60m
PM_FEE_LO, PM_FEE_HI = 0.04, 0.07         # documented PM taker rate range


def _base_dir() -> Path:
    for d in _V1_CANDIDATES:
        if d.is_dir() and any(d.glob(TEST_PATTERN)):
            return d
    raise FileNotFoundError("Polymarket-v1 daily_aligned not found (external/ or curated/)")


def _read(base: Path, patterns: list[str]) -> str:
    parts = [f"'{base / p}'" for p in patterns if list(base.glob(p))]
    if not parts:
        raise FileNotFoundError(f"no files for {patterns} under {base}")
    return "read_parquet([" + ", ".join(parts) + "])"


def main() -> None:
    base = _base_dir()
    TMP.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{TMP}'")
    train_src = _read(base, [f"{y}-*.parquet" for y in TRAIN_YEARS])
    test_src = _read(base, [TEST_PATTERN])
    skills_out = DATA_DIR / "curated" / "wallet_skills" / "part-0.parquet"

    nr = con.execute(f"SELECT DISTINCT neg_risk FROM {test_src} LIMIT 10").fetchall()
    print("neg_risk distinct:", [r[0] for r in nr])
    binary = "lower(CAST(neg_risk AS VARCHAR)) IN ('false','f','0')"

    # ---- Step 1: Pre-2025 fills and resolved labels ----
    t0 = time.time()
    con.execute(f"""
    CREATE TEMP TABLE skilled AS
    WITH pre AS (
      SELECT lower(taker) AS w,
             (CASE WHEN taker_direction='BUY' THEN 1 ELSE -1 END)
               * ((CASE WHEN outcome_label=winning_outcome_label THEN 1.0 ELSE 0.0 END) - price)
               * usdc_amount AS pnl
      FROM {train_src}
      WHERE block_timestamp < {EPOCH_2025}
        AND resolved_at < TIMESTAMP '2025-01-01'
        AND winning_outcome_label IS NOT NULL AND winning_outcome_label<>''
        AND price>0 AND price<1 AND usdc_amount>0 AND taker_direction IN ('BUY','SELL')
    )
    SELECT w FROM pre GROUP BY w
    HAVING count(*)>={MIN_TRAIN_TRADES}
       AND sum(pnl)/sqrt(nullif(sum(pnl*pnl),0)) > {Z}
    """)
    n_sk = con.execute("SELECT count(*) FROM skilled").fetchone()[0]
    print(f"cutoff-qualified wallets (pre-2025, >={MIN_TRAIN_TRADES} trades, z>{Z}): {n_sk:,}"
          f"  ({time.time()-t0:.1f}s)")
    print(f"  (prebuilt wallet_skills at {skills_out} is full-corpus labels may overlap a 2025 test; "
          f"this recompute is the OOS-clean cohort)\n")

    # ---- Step 2: 2025 test corpus, standard-binary ----
    t0 = time.time()
    con.execute(f"""
    CREATE TEMP TABLE px_base AS
    SELECT asset_id, block_timestamp AS ts, price, taker, taker_direction, usdc_amount,
           CASE WHEN outcome_label=winning_outcome_label THEN 1.0 ELSE 0.0 END AS ywin
    FROM {test_src}
    WHERE {binary} AND price>0 AND price<1 AND usdc_amount>0
      AND winning_outcome_label IS NOT NULL AND winning_outcome_label<>''
    """)
    con.execute("""CREATE TEMP TABLE prices AS
      SELECT asset_id, ts, arg_max(price, usdc_amount) AS price FROM px_base GROUP BY asset_id, ts""")
    con.execute("CREATE INDEX ip ON prices(asset_id, ts)")
    con.execute("CREATE TEMP TABLE ymeta AS SELECT asset_id, any_value(ywin) AS y FROM px_base GROUP BY asset_id")
    npx = con.execute("SELECT count(*) FROM px_base").fetchone()[0]
    print(f"test trades (standard-binary): {npx:,}  ({time.time()-t0:.1f}s)")

    con.execute(f"""
    CREATE TEMP TABLE ev0 AS
    WITH b AS (
      SELECT p.asset_id, (p.ts - (p.ts % 60)) + 60 AS t0,
             CASE WHEN p.taker_direction='BUY' THEN 1 ELSE -1 END AS dir, p.usdc_amount
      FROM px_base p JOIN skilled s ON lower(p.taker)=s.w
    ),
    bar AS (SELECT asset_id, t0, sum(dir*usdc_amount) net, sum(usdc_amount) gross, count(*) n
            FROM b GROUP BY asset_id, t0)
    SELECT bar.asset_id, t0,
           CASE WHEN net>0 THEN 1 WHEN net<0 THEN -1 ELSE 0 END AS s,
           abs(net)/nullif(gross,0) AS aofi, n AS n_skilled, y.y AS y
    FROM bar JOIN ymeta y ON bar.asset_id=y.asset_id
    WHERE net<>0 AND EXISTS (SELECT 1 FROM prices p WHERE p.asset_id=bar.asset_id AND p.ts>t0)
    """)
    con.execute("CREATE OR REPLACE TEMP TABLE ev AS SELECT row_number() OVER () AS eid, * FROM ev0")
    con.execute("CREATE OR REPLACE TEMP TABLE acc AS SELECT * FROM ev")
    for d in DELAYS:
        con.execute(f"""CREATE OR REPLACE TEMP TABLE ptmp AS
          SELECT e.eid, p.price AS pe FROM ev e
          ASOF JOIN prices p ON e.asset_id=p.asset_id AND p.ts <= (e.t0 + {d})""")
        con.execute(f"CREATE OR REPLACE TEMP TABLE acc2 AS SELECT a.*, t.pe AS pe_{d} FROM acc a JOIN ptmp t ON a.eid=t.eid")
        con.execute("DROP TABLE acc; ALTER TABLE acc2 RENAME TO acc")
    n_ev = con.execute("SELECT count(*) FROM acc").fetchone()[0]
    print(f"skilled-flow events (standard-binary, forward-active): {n_ev:,}  ({time.time()-t0:.1f}s)\n")

    def agg(where: str, sign: str = "s", delay: int = 0):
        pe = f"pe_{delay}"
        return con.execute(f"""
        SELECT count(*), avg({sign}*(y-{pe}))*100,
               avg(CASE WHEN {sign}*(y-{pe})>0 THEN 1.0 ELSE 0.0 END),
               avg(({sign}*(y-{pe})) - {PM_FEE_LO}*{pe}*(1-{pe}))*100,
               avg(({sign}*(y-{pe})) - {PM_FEE_HI}*{pe}*(1-{pe}))*100,
               avg(({sign}*(y-{pe})) / (CASE WHEN {sign}>0 THEN {pe} ELSE 1-{pe} END))
        FROM acc WHERE {where} AND {pe}>0.03 AND {pe}<0.97""").fetchone()

    print("=" * 96)
    print("HOLD-TO-RESOLUTION edge, entry at bar close (delay 0), by |OFI| strength "
          "(cents/contract; selection-time labels)")
    print("=" * 96)
    hdr = f"{'bucket':<26}{'n':>9}{'pre-fee¢':>10}{'win%':>7}{'net@.04¢':>10}{'net@.07¢':>10}{'$/staked':>10}"
    print(hdr); print("-" * len(hdr))
    for name, w in [("all skilled events", "TRUE"), ("|OFI| >= 0.5", "aofi>=0.5"),
                    ("|OFI| = 1.0 (one-sided)", "aofi>=0.999"),
                    ("|OFI|>=0.5 & n_skilled>=3", "aofi>=0.5 AND n_skilled>=3")]:
        r = agg(w)
        if r[0]:
            print(f"{name:<26}{r[0]:>9,}{r[1]:>+10.3f}{r[2]*100:>6.1f}%{r[3]:>+10.3f}{r[4]:>+10.3f}{r[5]:>+10.4f}")

    print("\nBy ENTRY-PRICE BAND (all skilled, delay 0) — exploratory subgroup comparison; per-dollar weighting differs")
    print(f"{'entry price band':<20}{'n':>9}{'pre-fee¢':>10}{'win%':>7}{'$/staked':>10}")
    for lo, hi in [(0.03, 0.15), (0.15, 0.35), (0.35, 0.65), (0.65, 0.85), (0.85, 0.97)]:
        r = con.execute(f"""SELECT count(*), avg(s*(y-pe_0))*100,
            avg(CASE WHEN s*(y-pe_0)>0 THEN 1.0 ELSE 0.0 END),
            avg((s*(y-pe_0))/(CASE WHEN s>0 THEN pe_0 ELSE 1-pe_0 END))
            FROM acc WHERE pe_0>={lo} AND pe_0<{hi}""").fetchone()
        if r[0]:
            print(f"{f'[{lo:.2f}, {hi:.2f})':<20}{r[0]:>9,}{r[1]:>+10.3f}{r[2]*100:>6.1f}%{r[3]:>+10.4f}")

    print("\nEDGE vs ENTRY DELAY  (|OFI|>=0.5; pre-fee ¢/contract) — is latency material to resolution?")
    print(f"{'':<20}" + "".join(f"{('now' if d==0 else f'{d//60}m'):>10}" for d in DELAYS))
    print(f"{'pre-fee edge':<20}" + "".join(f"{agg('aofi>=0.5', delay=d)[1]:>+10.3f}" for d in DELAYS))

    print("\nPLACEBOS (|OFI|>=0.5, delay 0):")
    fade = agg("aofi>=0.5", sign="(-s)")
    con.execute("""CREATE OR REPLACE TEMP TABLE acc2 AS
      SELECT *, (CASE WHEN (hash(asset_id||t0::VARCHAR)%2)=0 THEN 1 ELSE -1 END) AS srnd FROM acc""")
    con.execute("DROP TABLE acc; ALTER TABLE acc2 RENAME TO acc")
    shuf = agg("aofi>=0.5", sign="srnd")
    print(f"  fade skilled (-s):  pre-fee {fade[1]:+.3f}¢   win {fade[2]*100:.1f}%   (should LOSE)")
    print(f"  sign-shuffled:      pre-fee {shuf[1]:+.3f}¢   win {shuf[2]*100:.1f}%   (should be ~0)")


if __name__ == "__main__":
    sys.exit(main())
