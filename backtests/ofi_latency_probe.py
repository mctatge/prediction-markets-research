#!/usr/bin/env python3
"""Price-response timing study for skilled-wallet flow.

Measures directional price changes after a one-minute flow bar becomes
observable. Trades are signed on their own token axis. Forward prices use
last observed trades rather than executable quotes. Skilled wallets are read
from a prebuilt wallet_skills table; this script does not establish that the
table's outcome labels were available before the test period.

Historical output reported a mean directional move of about 0.43 cents by
120 minutes, with 48% observed by two minutes and 74% by fifteen minutes.
These are price-response statistics, not follower returns. Last-trade entry,
cohort timing, spreads, fills, and historical fee assumptions limit inference.
The full corpus is not bundled and the reported run is not reproduced here.

Usage: uv run python backtests/ofi_latency_probe.py
See docs/findings.md and backtests/README.md before interpreting results.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
# v1 corpus lives under external/ (bulk) or curated/; sample a few 2025 months.
_V1_CANDIDATES = [
    DATA_DIR / "external" / "polymarket-v1" / "daily_aligned",
    DATA_DIR / "curated" / "polymarket-v1" / "daily_aligned",
]
SAMPLE_MONTHS = "2025-0[3-6]-*.parquet"  # honest subset, not the full corpus
SKILLS = DATA_DIR / "curated" / "wallet_skills" / "part-0.parquet"
TMP = DATA_DIR / ".duckdb-tmp"

Z = 1.96
HORIZONS = [60, 120, 300, 600, 900, 1800, 3600, 7200]  # seconds
HLABEL = {60: "1m", 120: "2m", 300: "5m", 600: "10m", 900: "15m",
          1800: "30m", 3600: "60m", 7200: "120m"}
MIN_ASSET_TRADES = 30


def _v1_glob() -> str:
    for d in _V1_CANDIDATES:
        if d.is_dir() and any(d.glob(SAMPLE_MONTHS)):
            return str(d / SAMPLE_MONTHS)
    raise FileNotFoundError(
        f"Polymarket-v1 daily_aligned ({SAMPLE_MONTHS}) not found under "
        f"data/external/ or data/curated/"
    )


def _build_events(con: duckdb.DuckDBPyConnection, skilled: bool) -> None:
    """events(asset_id, t0, s) for skilled (or unskilled) takers."""
    join = "JOIN sk s ON b.taker = s.w" if skilled else "ANTI JOIN sk s ON b.taker = s.w"
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE ev AS
    WITH b AS (
      SELECT asset_id,
             (block_timestamp - (block_timestamp % 60)) + 60 AS t0,  -- bar close
             CASE WHEN taker_direction='BUY' THEN 1 ELSE -1 END AS dir,
             usdc_amount, lower(taker) AS taker
      FROM px_base
      WHERE taker_direction IN ('BUY','SELL')
    ),
    flt AS (SELECT b.* FROM b {join}),
    bar AS (
      SELECT asset_id, t0, sum(dir*usdc_amount) AS sflow
      FROM flt GROUP BY asset_id, t0
    )
    SELECT asset_id, t0,
           CASE WHEN sflow>0 THEN 1 WHEN sflow<0 THEN -1 ELSE 0 END AS s
    FROM bar WHERE sflow <> 0
    """)


def _price_at(con: duckdb.DuckDBPyConnection, target_expr: str, out_col: str) -> None:
    """ASOF: last price at or before (t0 + h) per event -> ptmp(eid, out_col)."""
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE ptmp AS
    SELECT e.rowid AS eid, p.price AS {out_col}
    FROM ev_r e
    ASOF JOIN prices p ON e.asset_id = p.asset_id AND p.ts <= ({target_expr})
    """)


def _profile(con: duckdb.DuckDBPyConnection, skilled: bool, shuffle: bool = False):
    _build_events(con, skilled)
    sign_expr = (
        "CASE WHEN (hash(e.asset_id || e.t0::VARCHAR) % 2)=0 THEN 1 ELSE -1 END"
        if shuffle else "e.s"
    )
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE ev_r AS
    SELECT row_number() OVER () AS rowid, e.asset_id, e.t0, {sign_expr} AS s
    FROM ev e
    JOIN liquid l ON e.asset_id = l.asset_id
    WHERE EXISTS (SELECT 1 FROM prices p
                  WHERE p.asset_id=e.asset_id AND p.ts > e.t0 AND p.ts <= e.t0+3600)
    """)
    con.execute("CREATE OR REPLACE TEMP TABLE acc AS SELECT rowid AS eid, asset_id, t0, s FROM ev_r")
    _price_at(con, "e.t0", "p0")
    con.execute("CREATE OR REPLACE TEMP TABLE acc2 AS SELECT a.*, t.p0 FROM acc a JOIN ptmp t ON a.eid=t.eid")
    con.execute("DROP TABLE acc; ALTER TABLE acc2 RENAME TO acc")
    for h in HORIZONS:
        _price_at(con, f"e.t0 + {h}", "ph")
        con.execute(f"CREATE OR REPLACE TEMP TABLE acc2 AS SELECT a.*, t.ph AS p_{h} FROM acc a JOIN ptmp t ON a.eid=t.eid")
        con.execute("DROP TABLE acc; ALTER TABLE acc2 RENAME TO acc")
    sel = ", ".join(f"avg(s*(p_{h}-p0))*100 AS r_{h}" for h in HORIZONS)
    row = con.execute(f"SELECT count(*) AS n, {sel} FROM acc").fetchone()
    return row[0], {h: row[i + 1] for i, h in enumerate(HORIZONS)}


def main() -> None:
    v1_glob = _v1_glob()
    print(f"sample: {v1_glob}")
    TMP.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{TMP}'")

    t0 = time.time()
    con.execute(f"""
    CREATE TEMP TABLE px_base AS
    SELECT asset_id, block_timestamp, price, taker, taker_direction, usdc_amount
    FROM read_parquet('{v1_glob}')
    WHERE price>0 AND price<1 AND usdc_amount>0 AND block_timestamp IS NOT NULL
    """)
    con.execute(f"CREATE TEMP TABLE sk AS SELECT lower(wallet) w FROM read_parquet('{SKILLS}') WHERE z_score>{Z}")
    con.execute(f"CREATE TEMP TABLE liquid AS SELECT asset_id FROM px_base GROUP BY asset_id HAVING count(*)>={MIN_ASSET_TRADES}")
    # one price per (asset, second); on ties take the largest-value trade then max price
    con.execute("""
    CREATE TEMP TABLE prices AS
      SELECT asset_id, block_timestamp AS ts, arg_max(price, usdc_amount) AS price
      FROM px_base GROUP BY asset_id, block_timestamp
    """)
    con.execute("CREATE INDEX ip ON prices(asset_id, ts)")
    npx = con.execute("SELECT count(*) FROM px_base").fetchone()[0]
    print(f"  loaded {npx:,} trades, price series built, in {time.time()-t0:.1f}s\n")

    runs = [("SKILLED flow", True, False),
            ("UNSKILLED flow (control)", False, False),
            ("SKILLED, sign-shuffled (placebo)", True, True)]
    results = {}
    for name, skilled, shuffle in runs:
        t = time.time()
        n, rr = _profile(con, skilled, shuffle)
        results[name] = (n, rr)
        print(f"[{name}]  n_events={n:,}  ({time.time()-t:.1f}s)")

    print("\n" + "=" * 92)
    print("Directional price response after an observable skilled-flow minute "
          "(cents; + = price moves the way flow leaned)")
    print("=" * 92)
    hdr = f"{'run':<34}" + "".join(f"{HLABEL[h]:>8}" for h in HORIZONS)
    print(hdr); print("-" * len(hdr))
    for name, (n, rr) in results.items():
        print(f"{name:<34}" + "".join(f"{rr[h]:>+8.3f}" for h in HORIZONS))

    n, rr = results["SKILLED flow"]
    long = rr[7200]
    print("\nFRACTION OF EVENTUAL MOVE (120m) ALREADY REALIZED BY HORIZON h  [skilled]:")
    print(f"{'':<34}" + "".join(f"{HLABEL[h]:>8}" for h in HORIZONS))
    print(f"{'fraction captured':<34}"
          + "".join(f"{(rr[h]/long if long else 0):>8.2f}" for h in HORIZONS))
    if long:
        print(f"\n  eventual (120m) skilled move = {long:+.3f} cents")
        print(f"  realized by 2m = {rr[120]/long*100:.0f}%  |  by 15m = {rr[900]/long*100:.0f}%"
              f"  |  remaining after 15m = {(long-rr[900])/long*100:.0f}%")


if __name__ == "__main__":
    sys.exit(main())
