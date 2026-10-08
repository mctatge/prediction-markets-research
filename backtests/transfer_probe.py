"""Cross-venue lead-lag and skilled-flow transfer probe.

Tests whether Polymarket price changes or wallet flow precede Kalshi quote
changes, using confirmed equivalent contracts. It compares quote changes,
flow controls, and modeled entry costs. Venue availability is outside the
scope of this analysis.

Requires a separately reviewed venue_crosswalk_sports.csv mapping beside this
script and a compatible local data lake. The mapping must identify equivalent
settlement conditions; candidate matches are not confirmed matches. Analysis
uses confirmed=1 rows only. No historical mapping or market dataset is bundled.

The decision grid is five minutes. Entry quotes come from Kalshi candle
closes; fill assumptions remain simulated. The original sample covered about
one week of sports events, which limits generalization. A positive result
requires independent testing with fresh executable quotes, realistic latency,
liquidity, fees, and correlated outcomes. This script never places orders.

Usage: uv run python backtests/transfer_probe.py
  --refresh: re-extract from local raw captures instead of cached extracts.
  --rebuild-crosswalk: generate candidate mappings for separate review.
Cache files are written under data/scratch/transfer_probe/.
See docs/methodology.md and docs/findings.md for interpretation limits.
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np

HERE = Path(__file__).resolve().parent
CSV_PATH = HERE / "venue_crosswalk_sports.csv"
DATA = Path(os.environ.get("PMKT_DATA_DIR", "data"))
CACHE = DATA / "scratch" / "transfer_probe"

GRID_S = 300            # 5-min decision/lead-lag grid
MAX_SPREAD = 0.20       # quote-spread filter
OFI_WINDOW_S = 3600     # trailing 60m decision-time OFI
MIN_SKILLED = 3         # live ofi.py gate
Z_SKILLED = 1.96
THETAS = (0.3, 0.5, 0.8)
EV_RUNWAY_S = 1800      # decisions must leave 30m of quote runway
KALSHI_TAKER = 0.07
KALSHI_MAKER = 0.25 * KALSHI_TAKER

t0 = time.time()


def p(msg: str = "") -> None:
    print(f"[{time.time()-t0:7.1f}s] {msg}" if msg else "", flush=True)


# ---------------------------------------------------------------- extraction

def _date_files(venue: str, endpoint: str) -> list[tuple[str, str, list[Path]]]:
    """(date, tier, files) per raw date partition; parquet tier wins a date
    (exactly-once, mirrors lake._discover_raw)."""
    out: list[tuple[str, str, list[Path]]] = []
    proot = DATA / "raw-parquet" / f"venue={venue}" / f"endpoint={endpoint}"
    jroot = DATA / "raw" / f"venue={venue}" / f"endpoint={endpoint}"
    pdates = {d.name.split("=")[1] for d in proot.glob("date=*")} if proot.exists() else set()
    jdates = {d.name.split("=")[1] for d in jroot.glob("date=*")} if jroot.exists() else set()
    for d in sorted(pdates | jdates):
        if d in pdates:
            files = sorted((proot / f"date={d}").rglob("*.parquet"))
            tier = "parquet"
        else:  # skip the live collector's open .tmp file
            files = sorted(f for f in (jroot / f"date={d}").rglob("*.jsonl*")
                           if not f.name.endswith(".tmp"))
            tier = "jsonl"
        if files:
            out.append((d, tier, files))
    return out


def _src(tier: str, files: list[Path]) -> str:
    lst = "[" + ",".join(f"'{f}'" for f in files) + "]"
    if tier == "parquet":
        return f"read_parquet({lst})"
    return (
        f"read_json({lst}, format='newline_delimited', "
        "columns={'v':'BIGINT','venue':'VARCHAR','endpoint':'VARCHAR',"
        "'params':'VARCHAR','fetched_at':'VARCHAR','http_status':'INTEGER',"
        "'elapsed_ms':'BIGINT','payload':'VARCHAR'}, "
        "maximum_object_size=33554432)"
    )


def extract(rows: list[dict], refresh: bool) -> None:
    """Raw lake -> cached extracts, filtered to crosswalk contracts.

    Bounded per-file scans (the whole-lake JSON explode OOMs; see the
    invalid JSON in historical payloads) with a payload LIKE prefilter
    on series prefixes — batch candle requests chunk tickers alphabetically,
    so the prefilter is highly selective."""
    CACHE.mkdir(parents=True, exist_ok=True)
    done = all((CACHE / f).exists() for f in
               ("kalshi_bars.parquet", "pm_points.parquet", "pm_tape.parquet"))
    if done and not refresh:
        p(f"extracts cached in {CACHE} (use --refresh to re-extract)")
        return

    tickers = sorted({r["kalshi_ticker"] for r in rows})
    prim_toks = sorted({r["pm_primary_token_id"] for r in rows if r["pm_primary_token_id"]})
    cids = sorted({r["pm_condition_id"] for r in rows})
    prefixes = sorted({t.split("-")[0] + "-" for t in tickers})

    con = duckdb.connect()
    con.execute("SET threads=2; SET memory_limit='5GB'; "
                "SET enable_progress_bar=false; SET preserve_insertion_order=false")
    con.execute("CREATE TEMP TABLE want_tick(t VARCHAR)")
    con.executemany("INSERT INTO want_tick VALUES (?)", [(t,) for t in tickers])
    con.execute("CREATE TEMP TABLE want_tok(t VARCHAR)")
    con.executemany("INSERT INTO want_tok VALUES (?)", [(t,) for t in prim_toks])

    con.execute("""CREATE TEMP TABLE kbars (ticker VARCHAR, ts BIGINT,
        bid DOUBLE, ask DOUBLE, tlast DOUBLE, thigh DOUBLE, tlow DOUBLE,
        vol DOUBLE, fetched_at VARCHAR)""")
    like_pre = " OR ".join(f"payload LIKE '%{x}%'" for x in prefixes)
    for d, tier, files in _date_files("kalshi", "markets_candlesticks"):
        for f in files:
            con.execute(f"""
            INSERT INTO kbars
            WITH caps AS (
              SELECT fetched_at,
                     unnest(CAST(json_extract(payload,'$.markets') AS JSON[])) mk
              FROM {_src(tier, [f])}
              WHERE http_status=200 AND ({like_pre})
                AND json_extract(payload,'$.markets') IS NOT NULL
            ),
            hit AS (
              SELECT fetched_at, mk->>'market_ticker' ticker,
                     unnest(CAST(json_extract(mk,'$.candlesticks') AS JSON[])) c
              FROM caps WHERE mk->>'market_ticker' IN (SELECT t FROM want_tick)
            )
            SELECT ticker, CAST(c->>'end_period_ts' AS BIGINT),
                   -- prices are fixed-point dollar strings
                   TRY_CAST(json_extract_string(c,'$.yes_bid.close_dollars') AS DOUBLE),
                   TRY_CAST(json_extract_string(c,'$.yes_ask.close_dollars') AS DOUBLE),
                   TRY_CAST(json_extract_string(c,'$.price.close_dollars') AS DOUBLE),
                   TRY_CAST(json_extract_string(c,'$.price.high_dollars') AS DOUBLE),
                   TRY_CAST(json_extract_string(c,'$.price.low_dollars') AS DOUBLE),
                   TRY_CAST(c->>'volume_fp' AS DOUBLE), fetched_at
            FROM hit""")
        p(f"  kalshi candles {d} ({tier})")
    con.execute(f"""
    COPY (SELECT ticker, ts, bid, ask, tlast, thigh, tlow, vol FROM (
      SELECT *, row_number() OVER (PARTITION BY ticker, ts ORDER BY fetched_at DESC) rn
      FROM kbars) WHERE rn=1 ORDER BY ticker, ts
    ) TO '{CACHE / "kalshi_bars.parquet"}' (FORMAT PARQUET)""")

    con.execute("CREATE TEMP TABLE ppoints (token VARCHAR, ts BIGINT, p DOUBLE, fetched_at VARCHAR)")
    tok_arr = "[" + ",".join(f"'{t}'" for t in prim_toks) + "]"
    for d, tier, files in _date_files("polymarket", "clob_prices_history"):
        for f in files:
            con.execute(f"""
            INSERT INTO ppoints
            WITH caps AS (
              -- params (requested-token list) is tiny vs payload: prune first
              SELECT fetched_at, payload FROM {_src(tier, [f])}
              WHERE http_status=200 AND list_has_any(
                    CAST(json_extract(params,'$._body.markets') AS VARCHAR[]),
                    {tok_arr})
            ),
            keyed AS (
              SELECT fetched_at, payload,
                     unnest(json_keys(json_extract(payload,'$.history'))) tok
              FROM caps
            ),
            hit AS (
              SELECT fetched_at, tok,
                     unnest(CAST(json_extract(payload,'$.history."' || tok || '"') AS JSON[])) pt
              FROM keyed WHERE tok IN (SELECT t FROM want_tok)
            )
            SELECT tok, CAST(pt->>'t' AS BIGINT), CAST(pt->>'p' AS DOUBLE), fetched_at
            FROM hit""")
        p(f"  pm prices {d} ({tier})")
    con.execute(f"""
    COPY (SELECT token, ts, p FROM (
      SELECT *, row_number() OVER (PARTITION BY token, ts ORDER BY fetched_at DESC) rn
      FROM ppoints) WHERE rn=1 ORDER BY token, ts
    ) TO '{CACHE / "pm_points.parquet"}' (FORMAT PARQUET)""")

    tr = sorted((DATA / "curated" / "pm_trades").rglob("*.parquet"))
    ws = sorted((DATA / "curated" / "wallet_skills").rglob("*.parquet"))
    cid_list = "(" + ",".join(f"'{c}'" for c in cids) + ")"
    con.execute(f"""
    COPY (
      SELECT t.venue_market_id cid, t.venue_token_id token, t.ts,
             CAST(t.size AS DOUBLE) size, t.taker_side side, w.z_score z
      FROM read_parquet({[str(f) for f in tr]}) t
      LEFT JOIN read_parquet({[str(f) for f in ws]}) w ON w.wallet = t.proxy_wallet
      WHERE t.venue_market_id IN {cid_list} ORDER BY cid, ts
    ) TO '{CACHE / "pm_tape.parquet"}' (FORMAT PARQUET)""")
    p("extracts written")


# ------------------------------------------------------------------- grids

def load_grid(con: duckdb.DuckDBPyConnection) -> None:
    """Joint 5-min grid per contract: Kalshi mid + oriented PM price.

    Both series must have an observation INSIDE the same 5-min bucket (last
    obs in bucket wins) — no carry-forward, so neither side is ever more
    than one bucket stale. Bars where either venue is quiet are dropped."""
    con.execute(f"""
    CREATE TEMP TABLE grid AS
    WITH k5 AS (
      SELECT x.kalshi_ticker tick, (kb.ts // {GRID_S}) * {GRID_S} t5,
             arg_max(kb.bid, kb.ts) bid, arg_max(kb.ask, kb.ts) ask
      FROM kb JOIN xw x ON x.kalshi_ticker = kb.ticker
      WHERE kb.bid IS NOT NULL AND kb.ask IS NOT NULL
      GROUP BY 1, 2
    ),
    p5 AS (
      SELECT token, (ts // {GRID_S}) * {GRID_S} t5, arg_max(p, ts) p
      FROM pp GROUP BY 1, 2
    )
    SELECT x.event_key, x.sport, x.game_date, x.kalshi_ticker tick,
           x.slate, k5.t5, (k5.bid + k5.ask) * 0.5 kmid, k5.bid, k5.ask,
           CASE WHEN CAST(x.pm_is_primary AS INT) = 1 THEN p5.p ELSE 1.0 - p5.p END pmp
    FROM k5
    JOIN xw x ON x.kalshi_ticker = k5.tick
    JOIN p5 ON p5.token = x.pm_primary_token_id AND p5.t5 = k5.t5
    WHERE k5.ask - k5.bid <= {MAX_SPREAD}
    """)


# --------------------------------------------------------------- statistics

def cluster_ols(X: np.ndarray, y: np.ndarray, clusters: np.ndarray):
    """OLS with cluster-robust (CR0 sandwich) SEs. Returns (beta, se, t)."""
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ (X.T @ y)
    resid = y - X @ beta
    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(clusters):
        m = clusters == g
        s = X[m].T @ resid[m]
        meat += np.outer(s, s)
    V = XtX_inv @ meat @ XtX_inv
    se = np.sqrt(np.diag(V))
    return beta, se, beta / se


def slate_corr(pairs_by_slate: dict) -> tuple[float, float, int]:
    """Mean and SE of per-slate correlations (cluster-robust by design)."""
    cs = []
    for _slate, (a, b) in pairs_by_slate.items():
        a, b = np.asarray(a), np.asarray(b)
        if len(a) >= 20 and a.std() > 0 and b.std() > 0:
            cs.append(float(np.corrcoef(a, b)[0, 1]))
    if not cs:
        return float("nan"), float("nan"), 0
    cs = np.array(cs)
    return float(cs.mean()), float(cs.std(ddof=1) / math.sqrt(len(cs))) if len(cs) > 1 else float("nan"), len(cs)


# --------------------------------------------------------------------- main

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--rebuild-crosswalk", metavar="OUT_CSV",
                    help="regenerate candidate crosswalk rows (see module doc)")
    args = ap.parse_args()
    if args.rebuild_crosswalk:
        rebuild_crosswalk(Path(args.rebuild_crosswalk))
        return

    rows = [r for r in csv.DictReader(open(CSV_PATH)) if r["confirmed"] == "1"]
    p(f"crosswalk: {len(rows)} confirmed contracts, "
      f"{len({r['event_key'] for r in rows})} events")
    extract(rows, args.refresh)

    con = duckdb.connect()
    con.execute("SET threads=4; SET enable_progress_bar=false")
    con.execute(f"""CREATE TEMP TABLE xw AS
        SELECT *, sport || '|' || game_date AS slate
        FROM read_csv('{CSV_PATH}', all_varchar=true) WHERE confirmed='1'""")
    con.execute(f"CREATE TEMP VIEW kb AS SELECT * FROM read_parquet('{CACHE / 'kalshi_bars.parquet'}')")
    con.execute(f"CREATE TEMP VIEW pp AS SELECT * FROM read_parquet('{CACHE / 'pm_points.parquet'}')")
    con.execute(f"CREATE TEMP VIEW tape AS SELECT * FROM read_parquet('{CACHE / 'pm_tape.parquet'}')")
    load_grid(con)
    n_grid, n_ticks = con.execute("SELECT count(*), count(DISTINCT tick) FROM grid").fetchone()
    p(f"joint 5-min grid: {n_grid} bars over {n_ticks} contracts")

    # ---- Layer A: lead-lag, one leg per event (most joint bars, tiebreak ticker)
    con.execute("""CREATE TEMP TABLE lead_leg AS
      SELECT event_key, arg_max(tick, (n, tick)) tick FROM (
        SELECT event_key, tick, count(*) n FROM grid GROUP BY 1, 2
      ) GROUP BY 1""")
    g = con.execute("""
      SELECT g.slate, g.tick, g.t5, g.kmid, g.pmp FROM grid g
      JOIN lead_leg l ON l.tick = g.tick ORDER BY g.tick, g.t5""").fetchall()
    by_tick: dict[str, list] = defaultdict(list)
    for slate, tick, t5, kmid, pmp in g:
        by_tick[tick].append((slate, t5, kmid, pmp))

    # consecutive 5-min diffs per contract
    dk_at: dict[tuple, float] = {}
    dp_at: dict[tuple, float] = {}
    level_at: dict[tuple, tuple] = {}
    slate_of: dict[str, str] = {}
    for tick, seq in by_tick.items():
        slate_of[tick] = seq[0][0]
        for i in range(1, len(seq)):
            _, t5, kmid, pmp = seq[i]
            _, t5p, kmidp, pmpp = seq[i - 1]
            level_at[(tick, t5)] = (kmid, pmp)
            if t5 - t5p == GRID_S:
                dk_at[(tick, t5)] = kmid - kmidp
                dp_at[(tick, t5)] = pmp - pmpp
        if seq:
            _, t5, kmid, pmp = seq[0]
            level_at[(tick, t5)] = (kmid, pmp)

    def xcorr_table(dk: dict, dp: dict, lags_bars, unit_s: int) -> None:
        print(f"{'lag':>8s} {'slate-mean r':>12s} {'SE':>8s} {'slates':>7s}")
        for lag in lags_bars:
            by_slate: dict[str, tuple[list, list]] = defaultdict(lambda: ([], []))
            for (tick, t), d in dk.items():
                dpv = dp.get((tick, t - lag * unit_s))
                if dpv is not None:
                    a, b = by_slate[slate_of[tick]]
                    a.append(dpv)
                    b.append(d)
            m, se, ns = slate_corr(by_slate)
            print(f"{lag*unit_s//60:>6d}m {m:>12.4f} {se:>8.4f} {ns:>7d}")

    print("\n== Layer A: lead-lag cross-correlation (one leg/event, 5-min grid) ==")
    print("corr( d_pm[t - lag], d_kalshi[t] ): positive at lag>0 means PM LEADS")
    xcorr_table(dk_at, dp_at, (-6, -3, -2, -1, 0, 1, 2, 3, 6), GRID_S)

    # 1-min variant: is there ANY lead window under the 5-min bar?
    g1 = con.execute("""
      WITH k1 AS (
        SELECT kb.ticker tick, kb.ts - kb.ts % 60 t1,
               arg_max((kb.bid + kb.ask) * 0.5, kb.ts) kmid
        FROM kb JOIN lead_leg l ON l.tick = kb.ticker
        WHERE kb.bid IS NOT NULL AND kb.ask IS NOT NULL
          AND kb.ask - kb.bid <= 0.20
        GROUP BY 1, 2
      ),
      p1 AS (
        SELECT x.kalshi_ticker tick, pp.ts - pp.ts % 60 t1,
               CASE WHEN CAST(x.pm_is_primary AS INT) = 1
                    THEN arg_max(pp.p, pp.ts) ELSE 1.0 - arg_max(pp.p, pp.ts)
               END pmp
        FROM pp JOIN xw x ON x.pm_primary_token_id = pp.token
        JOIN lead_leg l ON l.tick = x.kalshi_ticker
        GROUP BY 1, 2, x.pm_is_primary
      )
      SELECT k1.tick, k1.t1, k1.kmid, p1.pmp FROM k1
      JOIN p1 ON p1.tick = k1.tick AND p1.t1 = k1.t1
      ORDER BY k1.tick, k1.t1""").fetchall()
    dk1: dict[tuple, float] = {}
    dp1: dict[tuple, float] = {}
    prev1: dict[str, tuple] = {}
    for tick, t1, kmid, pmp in g1:
        pv = prev1.get(tick)
        if pv is not None and t1 - pv[0] == 60:
            dk1[(tick, t1)] = kmid - pv[1]
            dp1[(tick, t1)] = pmp - pv[2]
        prev1[tick] = (t1, kmid, pmp)
    print("\n-- 1-min grid variant (same construction) --")
    xcorr_table(dk1, dp1, (-5, -3, -2, -1, 0, 1, 2, 3, 5), 60)

    # error-correction: does the gap pull Kalshi, or PM?
    def ecm(dep: dict, own_lag: dict, label: str) -> None:
        X, y, cl = [], [], []
        for (tick, t5), d in dep.items():
            prev = level_at.get((tick, t5 - GRID_S))
            own = own_lag.get((tick, t5 - GRID_S))
            if prev is None or own is None:
                continue
            kmid, pmp = prev
            X.append([1.0, pmp - kmid, own])
            y.append(d)
            cl.append(slate_of[tick])
        X, y = np.array(X), np.array(y)
        cl = np.array(cl)
        b, se, t = cluster_ols(X, y, cl)
        print(f"  {label}: gap coef {b[1]:+.4f} (t={t[1]:+.1f})  "
              f"own-lag {b[2]:+.3f} (t={t[2]:+.1f})  n={len(y)} "
              f"clusters={len(set(cl.tolist()))}")

    print("\n== Layer A2: error-correction (gap = pm − kalshi at t−5m) ==")
    print("Kalshi adjusting toward PM => positive gap coef in d_kalshi eq")
    ecm(dk_at, dk_at, "d_kalshi[t] ~ gap[t-1] + d_kalshi[t-1]")
    ecm(dp_at, dp_at, "d_pm[t]     ~ gap[t-1] + d_pm[t-1]    ")

    # ---- Layer B: skilled OFI vs forward Kalshi moves ------------------
    tape_rows = con.execute("""
      SELECT x.kalshi_ticker, t.token, t.ts, t.size, t.side, t.z
      FROM tape t JOIN xw x ON x.pm_condition_id = t.cid
      ORDER BY x.kalshi_ticker, t.ts""").fetchall()
    tape_by_tick: dict[str, list] = defaultdict(list)
    side_tok = {r["kalshi_ticker"]: r["pm_token_id"] for r in rows}
    for tick, token, ts, size, side, z in tape_rows:
        # sign toward THIS contract: BUY of the contract's own token = +1;
        # any trade on the sibling token flips (binary markets only). Uses
        # the trade's token directly — no event-primary axis anywhere.
        sgn = (1 if side == "BUY" else -1) * (1 if token == side_tok[tick] else -1)
        tape_by_tick[tick].append((ts, sgn, size, z))

    grid_rows = con.execute(
        "SELECT tick, slate, t5, kmid, bid, ask, pmp FROM grid ORDER BY tick, t5").fetchall()
    kmid_at: dict[tuple, float] = {}
    quote_at: dict[tuple, tuple] = {}
    for tick, slate, t5, kmid, bid, ask, pmp in grid_rows:
        kmid_at[(tick, t5)] = kmid
        quote_at[(tick, t5)] = (bid, ask)
        slate_of.setdefault(tick, slate)

    def ofi_at(tick: str, t5: int) -> tuple[float, int] | None:
        num = den = 0.0
        n_sk = 0
        for ts, sgn, size, z in tape_by_tick.get(tick, ()):
            if t5 - OFI_WINDOW_S < ts <= t5 and z is not None and z > Z_SKILLED:
                num += sgn * size
                den += size
                n_sk += 1
        if n_sk < MIN_SKILLED or den <= 0:
            return None
        return num / den, n_sk

    print("\n== Layer B: PM skilled OFI (trailing 60m) vs FORWARD Kalshi mid move ==")
    decisions: list[tuple] = []          # (tick, t5, ofi, n_sk, gap, kmid)
    for tick, slate, t5, kmid, bid, ask, pmp in grid_rows:
        o = ofi_at(tick, t5)
        if o is None:
            continue
        decisions.append((tick, t5, o[0], o[1], pmp - kmid, kmid))
    by_sport_n = defaultdict(int)
    for tick, *_ in decisions:
        by_sport_n[slate_of[tick].split("|")[0]] += 1
    p(f"decision points with fresh skilled OFI: {len(decisions)} "
      f"({len({d[0] for d in decisions})} contracts; {dict(by_sport_n)})")

    for h in (300, 900, 1800, 3600):
        by_slate = defaultdict(lambda: ([], []))
        pooled = []
        for tick, t5, ofi, _n, _gap, kmid in decisions:
            fwd = kmid_at.get((tick, t5 + h))
            if fwd is None:
                continue
            a, b = by_slate[slate_of[tick]]
            a.append(ofi)
            b.append(fwd - kmid)
            pooled.append((ofi, fwd - kmid))
        m, se, ns = slate_corr(by_slate)
        print(f"  h={h//60:>3d}m  slate-mean corr(OFI, dK_fwd) = {m:+.4f} "
              f"(SE {se:.4f}, slates {ns}, n {len(pooled)})")

    # incremental to the observable PM gap?
    for h in (900, 3600):
        X, y, cl = [], [], []
        for tick, t5, ofi, _n, gap, kmid in decisions:
            fwd = kmid_at.get((tick, t5 + h))
            if fwd is None or gap is None:
                continue
            X.append([1.0, gap, ofi])
            y.append(fwd - kmid)
            cl.append(slate_of[tick])
        if len(y) < 50:
            continue
        b, se, t = cluster_ols(np.array(X), np.array(y), np.array(cl))
        print(f"  h={h//60:>3d}m  dK_fwd ~ gap + OFI: gap {b[1]:+.4f} (t={t[1]:+.1f}) "
              f"OFI {b[2]:+.5f} (t={t[2]:+.1f})  n={len(y)}")

    # ---- Layer C: fee-adjusted EV, first-decision-per-contract ---------
    print("\n== Layer C: fee-adjusted EV on Kalshi at PM-signal decisions ==")
    print("(settled contracts; ONE entry per EVENT at its first |OFI|>=theta;")
    print(" maker fills ADVERSARIAL: only if a later candle trades through)")
    result_of = {r["kalshi_ticker"]: r["kalshi_result"] for r in rows
                 if r["kalshi_status"] == "settled" and r["kalshi_result"] in ("yes", "no")}
    bars_by_tick: dict[str, list] = defaultdict(list)
    for tick, ts, tlow, thigh in con.execute(
            "SELECT ticker, ts, tlow, thigh FROM kb WHERE tlow IS NOT NULL").fetchall():
        bars_by_tick[tick].append((ts, tlow, thigh))
    last_bar = dict(con.execute("SELECT ticker, max(ts) FROM kb GROUP BY 1").fetchall())

    event_of = {r["kalshi_ticker"]: r["event_key"] for r in rows}

    def run_ev(theta: float, flip: bool) -> dict[str, list]:
        """ONE entry per EVENT (first qualifying decision across its legs;
        both legs of a game carry the same information). flip=True is the
        direction placebo: fade the skilled flow — if fading also profits,
        the PnL is entry mechanics, not signal."""
        first_by_event: dict[str, tuple] = {}
        for tick, t5, ofi, _n, _gap, _kmid in decisions:
            if tick not in result_of or abs(ofi) < theta:
                continue
            if last_bar.get(tick, 0) - t5 < EV_RUNWAY_S:
                continue
            cur = first_by_event.get(event_of[tick])
            if cur is None or t5 < cur[1]:
                first_by_event[event_of[tick]] = (tick, t5, ofi)
        entries: dict[str, list] = {"taker": [], "maker": []}
        for tick, t5, ofi in first_by_event.values():
            bid, ask = quote_at[(tick, t5)]
            y = 1.0 if result_of[tick] == "yes" else 0.0
            direction = -ofi if flip else ofi
            if direction > 0:            # buy YES
                q_t, y_t = ask, y
                lim = bid                # join the bid
                filled = any(ts > t5 + GRID_S and tlow is not None and tlow < lim
                             for ts, tlow, _ in bars_by_tick.get(tick, ()))
                q_m, y_m = lim, y
            else:                        # buy NO == sell YES
                q_t, y_t = 1 - bid, 1 - y
                lim = ask                # offer at the ask
                filled = any(ts > t5 + GRID_S and thigh is not None and thigh > lim
                             for ts, _, thigh in bars_by_tick.get(tick, ()))
                q_m, y_m = 1 - lim, 1 - y
            fee_t = KALSHI_TAKER * q_t * (1 - q_t)
            entries["taker"].append((tick, q_t, y_t - q_t - fee_t))
            if filled:
                fee_m = KALSHI_MAKER * q_m * (1 - q_m)
                entries["maker"].append((tick, q_m, y_m - q_m - fee_m))
        return entries

    def report_ev(label: str, theta: float, entries: dict) -> None:
        for mode in ("taker", "maker"):
            es = entries[mode]
            if not es:
                print(f"  {label} theta={theta}: {mode}: no entries")
                continue
            slates = defaultdict(lambda: [0.0, 0.0])
            for tick, q, pnl in es:
                s = slates[slate_of[tick]]
                s[0] += pnl
                s[1] += q
            perdollar = np.array([v[0] / v[1] for v in slates.values() if v[1] > 0])
            mean = perdollar.mean()
            se = (perdollar.std(ddof=1) / math.sqrt(len(perdollar))
                  if len(perdollar) > 1 else float("nan"))
            wins = sum(1 for e in es if e[2] > 0)
            fill = ("" if mode == "taker" else
                    f"  [fills {len(es)}/{len(entries['taker'])}]")
            print(f"  {label} theta={theta}: {mode:5s} n={len(es):3d} events, "
                  f"win={wins/len(es):.0%}, pnl/$ = {mean:+.4f} ± {1.96*se:.4f} "
                  f"(95% CI, {len(perdollar)} slates){fill}")

    for theta in THETAS:
        report_ev("follow", theta, run_ev(theta, flip=False))
    print("  -- direction placebo (fade the skilled flow) --")
    report_ev("FADE  ", 0.5, run_ev(0.5, flip=True))

    print("\nDone. Results depend on timing, contract matching and simulated fills.")
    print("Prospective paper evaluation is needed before making execution claims.")


# ------------------------------------------------- crosswalk candidate build
# (condensed copy of the reviewed builder; the separately reviewed CSV is the frozen
# artifact — this exists so the pairing logic itself stays reproducible)

WNBA_ALIAS = {"PDX": "por", "GS": "gsv", "NY": "nyl"}
WC_ALIAS = {"SUI": "che", "POR": "prt", "NED": "nld", "GER": "deu",
            "PAR": "pry", "CRO": "hrv"}
MLB_ALIAS = {"CHW": "cws", "AZ": "ari"}


def rebuild_crosswalk(out: Path) -> None:
    import re
    import sqlite3
    db = sqlite3.connect(DATA / "db" / "hot.sqlite")
    db.row_factory = sqlite3.Row
    con = duckdb.connect()
    pm_res = {}
    res_files = sorted((DATA / "curated" / "pm_resolutions").rglob("*.parquet"))
    if res_files:
        for cid, idx, label in con.execute(
                "SELECT venue_market_id, winning_outcome_index, winning_outcome_label "
                f"FROM read_parquet({[str(f) for f in res_files]})").fetchall():
            pm_res[cid] = (idx, label)

    kev = defaultdict(list)
    for r in db.execute("""
        SELECT venue_series_id s, venue_event_id ev, venue_market_id t, title,
               status_norm, result, close_ts FROM markets
        WHERE venue_id='kalshi' AND venue_series_id IN
          ('KXMLBGAME','KXWNBAGAME','KXWCGAME','KXATPMATCH','KXWTAMATCH')
          AND venue_event_id GLOB '*-26JUL1[0-9]*'"""):
        kev[r["ev"]].append(r)

    pm_ml, pm_wc, pm_tn = {}, {}, defaultdict(list)
    ml_re = re.compile(r"^(mlb|wnba)-([a-z0-9]+)-([a-z0-9]+)-(2026-07-\d{2})$")
    wc_re = re.compile(r"^fifwc-([a-z]{3})-([a-z]{3})-(2026-07-\d{2})-([a-z]{3}|draw)$")
    tn_re = re.compile(r"^(atp|wta)-([a-z0-9]+)-([a-z0-9]+)-(2026-07-\d{2})$")
    seen: dict = {}
    for r in db.execute("""
        SELECT m.venue_market_id cid, m.slug, m.title, m.status_norm,
               i.venue_token_id tok, i.outcome_index oi, i.outcome_name oname,
               i.is_primary FROM markets m JOIN instruments i
          ON i.venue_id=m.venue_id AND i.venue_market_id=m.venue_market_id
        WHERE m.venue_id='polymarket' AND (m.slug GLOB 'mlb-*-2026-07-1[0-9]'
           OR m.slug GLOB 'wnba-*-2026-07-1[0-9]'
           OR m.slug GLOB 'fifwc-*-2026-07-1[0-9]-*'
           OR m.slug GLOB 'atp-*-2026-07-1[0-9]' OR m.slug GLOB 'wta-*-2026-07-1[0-9]')"""):
        slug = r["slug"]
        for rx, book in ((ml_re, pm_ml), (wc_re, pm_wc), (tn_re, None)):
            m = rx.match(slug)
            if not m:
                continue
            key = m.groups()
            skey = (rx.pattern, key)
            if skey not in seen:
                d = seen[skey] = {"cid": r["cid"], "slug": slug, "title": r["title"],
                                  "status": r["status_norm"], "outcomes": {},
                                  "date": key[-1] if rx is tn_re else None}
                if book is not None:
                    book[key] = d
                else:
                    pm_tn[key[:3]].append(d)
            seen[skey]["outcomes"][r["oi"]] = (r["tok"], r["oname"], r["is_primary"])
            break

    out_rows = []

    def emit(ev, sport, date, kmkt, side, pmd, oi, method, note):
        tok, oname, is_prim = pmd["outcomes"].get(oi, (None, None, None))
        prim = next((t for _i, (t, _n, ip) in pmd["outcomes"].items() if ip == 1), None)
        res = pm_res.get(pmd["cid"], (None, None))
        agree = ""
        if kmkt["status_norm"] == "settled" and res[0] is not None:
            agree = "1" if ((kmkt["result"] == "yes") == (res[0] == oi)) else "0"
        out_rows.append({
            "event_key": ev, "sport": sport, "game_date": date,
            "kalshi_ticker": kmkt["t"], "kalshi_side": side,
            "kalshi_title": kmkt["title"], "kalshi_status": kmkt["status_norm"],
            "kalshi_result": kmkt["result"] or "", "pm_slug": pmd["slug"],
            "pm_condition_id": pmd["cid"], "pm_token_id": tok or "",
            "pm_outcome_index": oi, "pm_outcome_name": oname or "",
            "pm_is_primary": is_prim, "pm_primary_token_id": prim or "",
            "resolution_agree": agree, "match_method": method, "notes": note})

    for ev, mkts in sorted(kev.items()):
        series = mkts[0]["s"]
        m = re.match(r"^[A-Z]+-26JUL(\d{2})(\d{4})?([A-Z0-9]+?)(G\d)?$", ev)
        if not m:
            continue
        day, _hhmm, codes, dh = m.groups()
        date = f"2026-07-{day}"
        sufs = {t["t"].rsplit("-", 1)[1] for t in mkts} - {"TIE"}
        teams = next(((a, codes[len(a):]) for a in sorted(sufs, key=len, reverse=True)
                      if codes.startswith(a) and codes[len(a):] in sufs), None)
        if series in ("KXMLBGAME", "KXWNBAGAME", "KXWCGAME"):
            if not teams or dh:      # doubleheaders ambiguous on PM: skip
                continue
            away, home = teams
            if series == "KXWCGAME":
                a_pm = WC_ALIAS.get(away, away.lower())
                h_pm = WC_ALIAS.get(home, home.lower())
                for kmkt in mkts:
                    side = kmkt["t"].rsplit("-", 1)[1]
                    leg = "draw" if side == "TIE" else WC_ALIAS.get(side, side.lower())
                    pmd = pm_wc.get((a_pm, h_pm, date, leg))
                    if pmd:
                        yes_oi = next((i for i, (_t, n, _p) in pmd["outcomes"].items()
                                       if n == "Yes"), 0)
                        emit(ev, "wc", date, kmkt, side, pmd, yes_oi, "slug-code", f"leg={leg}")
            else:
                sport = "mlb" if series == "KXMLBGAME" else "wnba"
                alias = MLB_ALIAS if sport == "mlb" else WNBA_ALIAS
                key = (sport, alias.get(away, away.lower()), alias.get(home, home.lower()), date)
                pmd = pm_ml.get(key)
                if pmd:
                    for kmkt in mkts:
                        side = kmkt["t"].rsplit("-", 1)[1]
                        oi = 0 if alias.get(side, side.lower()) == key[1] else 1
                        emit(ev, sport, date, kmkt, side, pmd, oi, "slug-code",
                             f"away={away} home={home}")
        else:
            tour = "atp" if series == "KXATPMATCH" else "wta"
            mm = re.search(r"win the (.+?) vs (.+?): ", mkts[0]["title"])
            if not mm:
                continue
            s1, s2 = (s.strip().lower().replace(" ", "") for s in mm.groups())
            cands = []
            for (t, p1, p2), lst in pm_tn.items():
                if t == tour and ((s1.startswith(p1) and s2.startswith(p2))
                                  or (s1.startswith(p2) and s2.startswith(p1))):
                    for d in lst:
                        dd = abs(int(d["date"][-2:]) - int(date[-2:]))
                        if dd <= 1:
                            cands.append((dd, id(d), d))
            cands.sort(key=lambda x: x[:2])
            if not cands or (len(cands) > 1 and cands[0][0] == cands[1][0]
                             and cands[0][2] is not cands[1][2]):
                continue
            pmd = cands[0][2]
            for kmkt in mkts:
                mfull = re.match(r"Will (.+?) win the ", kmkt["title"])
                if not mfull:
                    continue
                full = mfull.group(1).strip().lower()
                oi = next((i for i, (_t, n, _p) in pmd["outcomes"].items()
                           if n and (n.lower() in full or full in n.lower()
                                     or full.split()[-1] in n.lower())), None)
                if oi is None:
                    continue
                emit(ev, tour, date, kmkt, kmkt["t"].rsplit("-", 1)[1], pmd, oi,
                     "surname", f"kalshi_date={date} pm_date={pmd['date']}")

    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)
    agree = [r for r in out_rows if r["resolution_agree"] != ""]
    print(f"candidates: {len(out_rows)} contracts, "
          f"{len({r['event_key'] for r in out_rows})} events, "
          f"resolution-checked {len(agree)} "
          f"disagree {sum(1 for r in agree if r['resolution_agree'] == '0')}")
    print("NOTE: candidates only — review before replacing a confirmed CSV "
          "(coverage/confirm columns come from the review pass).")


if __name__ == "__main__":
    main()
