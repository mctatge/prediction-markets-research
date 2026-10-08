"""Skilled OFI scorer: wallet-skill-weighted order-flow imbalance per market.

Joins recent Polymarket trade tape against historical wallet skill scores
(wallet_skills, from the v1 corpus randomization test) to compute signed OFI
by skill tier per market.

Historical whole-lifetime flow comparisons overstated forecasting gains.
Decision-time corrections reduced those gains, and trade-derived price
baselines did not establish executable follower returns. These flow statistics
remain experimental; see docs/findings.md and docs/methodology.md.

Runs every 15 min in the collector loop (AFTER pm collect_trades). Results →
hot-store ``skilled_ofi`` table → screener column + sort.

Trade source: the ROLLING WINDOW at ``curated/ofi_window/`` (part-0.parquet +
state.json), a rebuildable projection of raw ``data_api_trades`` captures
(storage invariants). Each pass appends only trades from raw jsonl part files it
has not consumed yet, dedupes on the derived md5 venue_trade_id (same key
recipe as build_pm_trades), prunes trades that aged out of the 24h window,
and rewrites the parquet atomically. Rationale: every 15-min forward sweep
re-captures full 1000-trade first pages per active market, so a naive 24h raw
scan re-parses ~30M mostly-duplicate trade objects per pass (~580k exploded
rows PER HOUR-DIR, measured 2026-07-16) and OOMs once the uncompacted jsonl
portion grows — the window holds only the ~1-2M unique in-window trades.

Two designs rejected for the incremental cursor (review 2026-07-16):
- Capture-level dedup (newest capture per market+offset): the sweep stops
  paginating a market once a page contains pre-watermark trades, so deeper
  offsets are never re-fetched — keeping only the newest offset-0 page drops
  every trade that migrated past offset 1000 since its capture, i.e. most of
  the 24h window on exactly the high-flow markets. Trade-level union dedupe
  has no such gap: coverage = union of ALL captures = the old full scan.
- A max-fetched_at timestamp watermark: raw part files only become VISIBLE on
  hourly rotation (live writers hold a .tmp), and a late-rotating or
  crash-recovered file can appear carrying captures up to an hour (recovery:
  arbitrarily) older than a watermark another writer already advanced past.
  The consumed-FILE-SET cursor is immune: rotated files are immutable, so any
  newly visible file is by definition unconsumed.
"""

from __future__ import annotations

from ..locking import fcntl
import json
import logging
import os
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)

VENUE_ID = "polymarket"
# 24h trailing window, RESTORED with the incremental rolling window (the
# interim 6h shrink existed only because the old full raw scan OOM'd —
# the window table holds unique in-window trades, so 24h is cheap again).
# Env knob kept for ops experiments.
LOOKBACK_S = int(os.environ.get("PMKT_OFI_LOOKBACK_S", "86400"))
MIN_SKILLED_TRADES = 3  # suppress OFI below this threshold
Z_SKILLED = 1.96
Z_ANTI = -1.96

WINDOW_ENDPOINT = "data_api_trades"  # bridge tape (data_api_trades_backfill)
                                     # stays excluded — incident 2026-07-15b
#: Bump when the window schema or key recipe changes meaning; a version
#: mismatch in state.json forces a full rebuild from raw.
OFI_WINDOW_VERSION = 1

_WINDOW_COLUMNS = (
    "venue_market_id VARCHAR, venue_token_id VARCHAR, ts BIGINT, "
    '"size" DECIMAL(18,6), taker_side VARCHAR, proxy_wallet VARCHAR, '
    "venue_trade_id VARCHAR, fetched_at VARCHAR"
)

# Parse one batch of raw captures into window-shaped rows. {source} is a
# file-scoped SELECT (lake._raw_jsonl_select / _raw_parquet_select) — never
# the connect() raw view, which would re-parse every uncompacted jsonl file
# per batch (read_json has no statistics to prune with).
_WINDOW_STAGE_SQL = """
WITH exploded AS (
    SELECT fetched_at AS _fetched_at,
           unnest(CAST(payload AS JSON[])) AS trade
    FROM ({source})
    WHERE venue = 'polymarket' AND endpoint = 'data_api_trades'
      AND http_status = 200
      AND json_type(payload) = 'ARRAY'
      AND {batch_where}
      -- Deliberately NO capture-level GREATEST(json_extract_string(...))
      -- page prune here (the old full-scan had one): json_extract on a
      -- vector of ~0.6MB payload VARCHARs allocates GBs and OOM'd the 4GB
      -- session on a single 400-capture parquet slice (measured 2026-07-16),
      -- while plain unnest of the same slice runs in 0.6s. All-old pages
      -- cost unnest+filter instead; the per-trade ts predicate below is the
      -- correctness layer either way.
)
SELECT * FROM (
    SELECT
        trade ->> 'conditionId'                 AS venue_market_id,
        trade ->> 'asset'                       AS venue_token_id,
        CAST(trade ->> 'timestamp' AS BIGINT)   AS ts,
        CAST(trade ->> 'size' AS DECIMAL(18,6)) AS "size",
        trade ->> 'side'                        AS taker_side,
        lower(trade ->> 'proxyWallet')          AS proxy_wallet,
        md5(
            COALESCE(trade ->> 'transactionHash', '') || '|' ||
            COALESCE(trade ->> 'asset', '') || '|' ||
            COALESCE(trade ->> 'proxyWallet', '') || '|' ||
            COALESCE(trade ->> 'side', '') || '|' ||
            COALESCE(trade ->> 'size', '') || '|' ||
            COALESCE(trade ->> 'price', '') || '|' ||
            COALESCE(trade ->> 'timestamp', '')
        )                                       AS venue_trade_id,
        _fetched_at                             AS fetched_at
    FROM exploded
)
-- Trade-level window: fetched_at is capture recency, not trade recency —
-- pre-endpoint-split bridge captures carried old trades under a fresh fetch.
WHERE ts IS NOT NULL AND venue_market_id IS NOT NULL AND ts >= ?
"""

# Merge staged rows into the window. Stage-internal dedupe keeps the row from
# the latest capture (same tiebreak as build_pm_trades); against rows already
# in the window the FIRST-seen row wins instead — equivalent, because the md5
# key covers every parsed field except conditionId, and conditionId is
# determined by asset (instruments UNIQUE(venue_id, venue_token_id)), so two
# captures of one venue_trade_id can never disagree on any kept column.
_WINDOW_MERGE_SQL = """
INSERT INTO ofi_win
SELECT s.* FROM (
    SELECT * FROM ofi_stage
    QUALIFY row_number() OVER (
        PARTITION BY venue_trade_id ORDER BY fetched_at DESC
    ) = 1
) s
ANTI JOIN ofi_win w USING (venue_trade_id)
"""

# Aggregation over the merged window — semantics identical to the original
# full-raw-scan scorer (pinned by tests/test_model_ofi.py).
_OFI_AGG_SQL = """
WITH directed AS (
    SELECT
        w.venue_market_id,
        w.proxy_wallet,
        w."size" AS size,
        -- direction: +1 = buying YES (bullish), -1 = selling YES (bearish)
        -- BUY on YES token (is_primary=1) → +1; SELL on YES → -1
        -- BUY on NO token (is_primary=0) → -1; SELL on NO → +1
        (CASE WHEN w.taker_side = 'BUY' THEN 1 ELSE -1 END)
        * (CASE WHEN COALESCE(i.is_primary, 1) = 1 THEN 1 ELSE -1 END)
            AS direction,
        ws.z_score
    FROM ofi_win w
    LEFT JOIN hot.instruments i
        ON i.venue_id = 'polymarket' AND i.venue_token_id = w.venue_token_id
    LEFT JOIN wallet_skills ws
        ON ws.wallet = w.proxy_wallet
    WHERE w.ts >= ?
)
SELECT
    venue_market_id,
    -- Skilled (z > 1.96)
    CASE WHEN SUM(size) FILTER (WHERE z_score > ?) > 0
         THEN SUM(direction * size) FILTER (WHERE z_score > ?)
            / SUM(size) FILTER (WHERE z_score > ?)
    END AS skilled_ofi,
    -- Unskilled (|z| <= 1.96 or NULL)
    CASE WHEN SUM(size) FILTER (WHERE z_score IS NULL OR (z_score >= ? AND z_score <= ?)) > 0
         THEN SUM(direction * size) FILTER (WHERE z_score IS NULL OR (z_score >= ? AND z_score <= ?))
            / SUM(size) FILTER (WHERE z_score IS NULL OR (z_score >= ? AND z_score <= ?))
    END AS unskilled_ofi,
    -- Anti-skilled (z < -1.96)
    CASE WHEN SUM(size) FILTER (WHERE z_score < ?) > 0
         THEN SUM(direction * size) FILTER (WHERE z_score < ?)
            / SUM(size) FILTER (WHERE z_score < ?)
    END AS anti_ofi,
    -- Z-weighted OFI
    CASE WHEN SUM(ABS(z_score) * size) FILTER (WHERE z_score IS NOT NULL) > 0
         THEN SUM(z_score * direction * size) FILTER (WHERE z_score IS NOT NULL)
            / SUM(ABS(z_score) * size) FILTER (WHERE z_score IS NOT NULL)
    END AS z_weighted_ofi,
    -- Volume and counts
    CAST(SUM(size) FILTER (WHERE z_score > ?) AS DECIMAL(18,2)) AS skilled_volume,
    CAST(SUM(size) AS DECIMAL(18,2))                             AS total_volume,
    COUNT(*) FILTER (WHERE z_score > ?)                          AS n_skilled_trades,
    COUNT(*)                                                     AS n_total_trades
FROM directed
WHERE direction IS NOT NULL
GROUP BY venue_market_id
HAVING COUNT(*) > 0
"""

# Positional params for _OFI_AGG_SQL.
# Order: trade-ts window + z thresholds:
# skilled(3) + unskilled(6) + anti(3) + skilled_vol(1) + skilled_count(1)
def _agg_params(cutoff_ts: int) -> list[Any]:
    s, a = Z_SKILLED, Z_ANTI
    return [
        cutoff_ts,        # trade-ts window over the merged rolling table
        s, s, s,          # skilled_ofi: FILTER × 3
        a, s, a, s, a, s, # unskilled_ofi: FILTER × 3 (each uses both thresholds)
        a, a, a,          # anti_ofi: FILTER × 3
        s,                # skilled_volume
        s,                # n_skilled_trades
    ]


# --------------------------------------------------------------------------
# Rolling window maintenance
# --------------------------------------------------------------------------

def _window_dir(data_dir: Path) -> Path:
    from ..lake import curated_root

    return curated_root(data_dir) / "ofi_window"


def _load_window_state(wdir: Path) -> dict[str, Any] | None:
    """state.json, or None if missing/unreadable/version-mismatched (any of
    which forces a full rebuild — never trust a unverifiable cursor)."""
    try:
        state = json.loads((wdir / "state.json").read_text())
    except (OSError, json.JSONDecodeError):
        return None
    if state.get("version") != OFI_WINDOW_VERSION:
        return None
    if not isinstance(state.get("consumed"), list):
        return None
    return state


def _write_window_state(
    wdir: Path, consumed: list[str], window_rows: int, now_ts: int
) -> None:
    state = {
        "version": OFI_WINDOW_VERSION,
        "updated_ts": now_ts,
        "window_rows": window_rows,
        "consumed": consumed,
    }
    tmp = wdir / f".state-{uuid.uuid4().hex[:8]}.json"
    tmp.write_text(json.dumps(state, indent=1))
    os.replace(tmp, wdir / "state.json")


def _visible_window_files(data_dir: Path, cutoff_ts: int) -> dict[str, Path]:
    """rel-key → path for every ROTATED data_api_trades jsonl part whose
    (date, hour) partition can still contain in-window captures. Captures in
    partitions strictly before the cutoff hour were fetched before the
    cutoff, so they cannot carry trades with ts >= cutoff (a capture only
    contains trades older than its fetch) — same boundary the old full-scan
    fetched_at filter drew. Live ``.tmp`` writers are invisible until they
    rotate, exactly as they are to every other lake reader."""
    from ..lake import raw_root

    root = raw_root(data_dir) / "venue=polymarket" / f"endpoint={WINDOW_ENDPOINT}"
    floor_date = time.strftime("%Y-%m-%d", time.gmtime(cutoff_ts))
    floor_hour = time.strftime("%H", time.gmtime(cutoff_ts))
    out: dict[str, Path] = {}
    for f in sorted(root.glob("date=*/hour=*/part-*.jsonl.zst")):
        date = f.parent.parent.name.split("=", 1)[1]
        hour = f.parent.name.split("=", 1)[1]
        if (date, hour) >= (floor_date, floor_hour):
            out[f"{date}/{hour}/{f.name}"] = f
    return out


def _incremental_batches(new_files: list[Path]) -> Iterator[tuple[str, str]]:
    """One file-scoped jsonl batch per (date, hour) dir — bounds the parse
    peak (~2.2GB RSS per hour-dir at threads=2, measured 2026-07-16).
    Incremental passes read jsonl ONLY: parquet compaction of an in-window
    date changes nothing here because the consumed jsonl files stay on disk
    (raw is never deleted), so the diff stays empty across the rewrite."""
    from ..lake import _raw_jsonl_select

    by_hour: dict[tuple[str, str], list[Path]] = {}
    for f in new_files:
        date = f.parent.parent.name.split("=", 1)[1]
        hour = f.parent.name.split("=", 1)[1]
        by_hour.setdefault((date, hour), []).append(f)
    for (_date, _hour), files in sorted(by_hour.items()):
        yield _raw_jsonl_select(files), "1=1"


def _bootstrap_batches(
    data_dir: Path, cutoff_ts: int, cutoff_str: str
) -> Iterator[tuple[str, str]]:
    """Full-window batches over the exactly-once parquet-else-jsonl split
    (lake._discover_raw). Compacted dates scan their own parquet part(s) in
    24 fetched_at-range hour slices (plain-column predicate → zone-map
    pruning); uncompacted (date, hour) dirs scan just their own files."""
    from ..lake import _discover_raw, _raw_jsonl_select, _raw_parquet_select, _sql_str

    cutoff_date = cutoff_str[:10]
    cutoff_hour = cutoff_str[11:13]
    parquet_files, jsonl_files = _discover_raw(
        data_dir, [WINDOW_ENDPOINT], ["polymarket"]
    )

    pq_by_date: dict[str, list[Path]] = {}
    for f in parquet_files:
        date = f.parent.name.split("=", 1)[1]
        if date >= cutoff_date:
            pq_by_date.setdefault(date, []).append(f)
    for date, files in sorted(pq_by_date.items()):
        src = _raw_parquet_select(files)
        # 15-min slices, not hours: a peak HOUR of forward-sweep captures
        # (~1200 × ~0.6MB pages) OOMs the 4GB session even at threads=2 —
        # the JSON DOM for a whole 128MB payload row group dwarfs the
        # strings themselves (measured 2026-07-16). Empty slices cost ~0s
        # (zone maps exclude them at the metadata check).
        for q in range(96):
            h, m = divmod(q * 15, 60)
            lo = max(f"{date}T{h:02d}:{m:02d}", cutoff_str)
            hi = f"{date}T{h:02d}:{m + 15:02d}" if m < 45 else f"{date}T{h + 1:02d}"
            if hi <= cutoff_str:
                continue
            yield src, f"fetched_at >= {_sql_str(lo)} AND fetched_at < {_sql_str(hi)}"

    jl_by_hour: dict[tuple[str, str], list[Path]] = {}
    for f in jsonl_files:
        date = f.parent.parent.name.split("=", 1)[1]
        hour = f.parent.name.split("=", 1)[1]
        if (date, hour) >= (cutoff_date, cutoff_hour):
            jl_by_hour.setdefault((date, hour), []).append(f)
    for (_date, _hour), files in sorted(jl_by_hour.items()):
        yield _raw_jsonl_select(files), "1=1"


def _refresh_window(
    conn: Any,
    data_dir: Path,
    cutoff_ts: int,
    cutoff_str: str,
    now_ts: int,
    rebuild: bool = False,
) -> dict[str, Any]:
    """Bring the rolling window up to date; leaves temp table ``ofi_win``
    holding exactly the deduped in-window trades for aggregation.

    Serialized by an flock on the window dir: two concurrent passes (daemon
    subprocess + a manual `pmkt model ofi`) interleaving their part/state
    writes could otherwise pair one pass's parquet with the other's consumed
    set, silently holing the window. Crash-safety inside the lock is by write
    ORDER: parquet replace first, state second — a crash between them only
    re-parses files the next pass (the md5 anti-join absorbs re-parses).
    """
    import duckdb

    from ..compact import PARQUET_PART_NAME
    from ..lake import _sql_str

    wdir = _window_dir(data_dir)
    wdir.mkdir(parents=True, exist_ok=True)
    lock = open(wdir / ".lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        part = wdir / PARQUET_PART_NAME
        state = None if rebuild else _load_window_state(wdir)
        bootstrap = rebuild or state is None or not part.exists()

        loaded = 0
        if not bootstrap:
            try:
                # Prune-at-load: trades that aged out of the window never
                # enter the session and vanish from the parquet on rewrite.
                conn.execute(
                    "CREATE TEMP TABLE ofi_win AS SELECT * FROM read_parquet("
                    f"{_sql_str(part)}) WHERE ts >= ?",
                    [cutoff_ts],
                )
                (loaded,) = conn.execute("SELECT count(*) FROM ofi_win").fetchone()
            except duckdb.Error as exc:
                logger.warning(
                    "ofi window parquet unreadable (%s) — rebuilding from raw", exc
                )
                conn.execute("DROP TABLE IF EXISTS ofi_win")
                bootstrap = True
                state = None
        if bootstrap:
            conn.execute(f"CREATE TEMP TABLE ofi_win ({_WINDOW_COLUMNS})")

        visible = _visible_window_files(data_dir, cutoff_ts)
        if bootstrap:
            batches = list(_bootstrap_batches(data_dir, cutoff_ts, cutoff_str))
            n_new_files = len(visible)
        else:
            consumed = set(state["consumed"])
            new_files = [p for key, p in sorted(visible.items()) if key not in consumed]
            batches = list(_incremental_batches(new_files))
            n_new_files = len(new_files)

        # Stage AND merge one batch at a time: temp tables cannot spill, so
        # accumulating every batch's pre-dedupe rows before a single global
        # merge holds the whole day's duplicate flood in memory at once and
        # OOMs the 4GB session (measured 2026-07-16 on the first bootstrap
        # attempt). Per-batch merging keeps the session at one batch's parse
        # + the deduped window.
        staged = 0
        for source, batch_where in batches:
            conn.execute(f"CREATE OR REPLACE TEMP TABLE ofi_stage ({_WINDOW_COLUMNS})")
            conn.execute(
                "INSERT INTO ofi_stage "
                + _WINDOW_STAGE_SQL.format(source=source, batch_where=batch_where),
                [cutoff_ts],
            )
            (n,) = conn.execute("SELECT count(*) FROM ofi_stage").fetchone()
            staged += n
            conn.execute(_WINDOW_MERGE_SQL)
        conn.execute("DROP TABLE IF EXISTS ofi_stage")
        (total,) = conn.execute("SELECT count(*) FROM ofi_win").fetchone()

        for stale in wdir.glob(".build-*"):
            stale.unlink()
        tmp = wdir / f".build-{uuid.uuid4().hex[:8]}.parquet"
        conn.execute(
            "COPY (SELECT * FROM ofi_win ORDER BY ts, venue_trade_id) TO "
            f"{_sql_str(tmp)} (FORMAT parquet, COMPRESSION zstd)"
        )
        os.replace(tmp, part)
        # consumed := everything currently visible in range. On bootstrap
        # that includes jsonl superseded by parquet — sound, because verified
        # compaction guarantees the parquet carries exactly those records.
        _write_window_state(wdir, sorted(visible), total, now_ts)
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()

    stats = {
        "bootstrap": bootstrap,
        "window_rows": total,
        "loaded_rows": loaded,
        "staged_rows": staged,
        "new_files": n_new_files,
    }
    logger.info(
        "ofi window: %d rows (%d loaded + %d staged from %d new file(s)%s)",
        total, loaded, staged, n_new_files, ", bootstrap" if bootstrap else "",
    )
    return stats


def _fmt_dec(value: Any) -> str | None:
    """Format a DuckDB Decimal/float as a 4-place decimal string, or None."""
    if value is None:
        return None
    try:
        return format(Decimal(str(value)).quantize(Decimal("0.0001")), "f")
    except (InvalidOperation, ValueError):
        return None


def _fmt_vol(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return format(Decimal(str(value)).quantize(Decimal("0.01")), "f")
    except (InvalidOperation, ValueError):
        return None


def compute_ofi(
    data_dir: Path,
    lookback_s: int = LOOKBACK_S,
    now_ts: int | None = None,
    rebuild: bool = False,
) -> list[dict[str, Any]]:
    """DuckDB: rolling trade window × wallet_skills → per-market skilled OFI.

    Refreshes the incremental window first (bootstrap from raw when absent),
    then aggregates over it. ``rebuild=True`` forces the bootstrap path —
    the escape hatch for the one known incremental gap: a crash-orphaned
    .tmp part recovered long after the fact lands in an hour partition the
    window may already have aged past caring about, but if in doubt, rebuild.
    """
    from ..lake import connect

    now_ts = int(time.time()) if now_ts is None else now_ts
    data_dir = Path(data_dir)

    cutoff_ts = now_ts - lookback_s
    cutoff_str = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cutoff_ts))

    conn = connect(
        data_dir,
        attach_hot=True,
        venues=["polymarket"],
        endpoints=[WINDOW_ENDPOINT],
    )
    try:
        # Fat-payload staging (0.6–1MB 1000-trade pages): each in-flight
        # pipeline holds a row group's payload strings PLUS their JSON DOMs,
        # so thread count — not batch size — sets the peak. threads=1 keeps
        # a worst-case fat batch under ~2GB (measured 2026-07-16: a peak
        # hour at threads=2 blows the 4GB duck.connect daemon guardrail;
        # threads=1 stages it in 6s). Restored to 2 for the aggregation,
        # which touches only slim window rows.
        conn.execute("SET threads = 1")
        _refresh_window(conn, data_dir, cutoff_ts, cutoff_str, now_ts, rebuild)
        conn.execute("SET threads = 2")
        result = conn.execute(_OFI_AGG_SQL, _agg_params(cutoff_ts)).fetchall()
    finally:
        conn.close()

    rows: list[dict[str, Any]] = []
    for r in result:
        n_skilled = r[7] if r[7] is not None else 0
        if n_skilled < MIN_SKILLED_TRADES:
            continue
        rows.append({
            "venue_market_id": r[0],
            "skilled_ofi": _fmt_dec(r[1]),
            "unskilled_ofi": _fmt_dec(r[2]),
            "anti_ofi": _fmt_dec(r[3]),
            "z_weighted_ofi": _fmt_dec(r[4]),
            "skilled_volume": _fmt_vol(r[5]),
            "total_volume": _fmt_vol(r[6]),
            "n_skilled_trades": n_skilled,
            "n_total_trades": r[8] if r[8] is not None else 0,
        })

    logger.info(
        "ofi: %d markets with skilled flow (≥%d skilled trades) from %d total",
        len(rows), MIN_SKILLED_TRADES, len(result),
    )
    return rows


def ofi_score_once(
    data_dir: Path, store: Any, now_ts: int | None = None, rebuild: bool = False
) -> dict[str, int]:
    """One OFI scoring pass: refresh window + compute + upsert to hot store.

    The prune runs on EVERY successful pass, not just when rows exist: a
    pass that finds zero qualifying markets means all previous signals dried
    up, and leaving them in the table shows stale OFI as live on the
    screener's edge column (review finding 2026-07-15)."""
    now_ts = int(time.time()) if now_ts is None else now_ts
    rows = compute_ofi(data_dir, now_ts=now_ts, rebuild=rebuild)
    if rows:
        store.upsert_skilled_ofi(VENUE_ID, rows, now_ts)
    pruned = store.prune_stale_ofi(VENUE_ID, now_ts)
    if pruned:
        logger.info("pruned %d stale OFI rows", pruned)
    return {"ofi_markets": len(rows)}


OFI_SUBPROCESS_TIMEOUT_S = 600


async def ofi_score(client: Any, store: Any) -> dict[str, int]:
    """Run-loop job (15 min, after pm collect_trades). client/store unused —
    the scan runs in a SUBPROCESS (`pmkt model ofi`, own HotStore session).

    Isolation is the point, not convenience: the DuckDB scan OOM'd under the
    bridge-backfill flood and its in-process unwind twice segfaulted the
    daemon's SQLite (EXC_BAD_ACCESS, incident 2026-07-15b). In a child
    process a scorer failure costs one job cycle, never the collector."""
    import asyncio
    import sys
    from pathlib import Path

    pmkt_bin = Path(sys.executable).parent / "pmkt"
    if not pmkt_bin.exists():
        raise RuntimeError(f"ofi subprocess: {pmkt_bin} does not exist")
    # The joins (hot.instruments 3.7M × wallet_skills 1.3M) still want more
    # than the 4GB in-daemon guardrail even with the small window source;
    # the child is isolated and sequential, so a bigger transient ceiling
    # is safe.
    child_env = os.environ | {
        "PMKT_DUCKDB_MEMORY_LIMIT": os.environ.get("PMKT_OFI_MEMORY_LIMIT", "8GB"),
        "PMKT_DUCKDB_THREADS": os.environ.get("PMKT_OFI_THREADS", "2"),
    }
    proc = await asyncio.create_subprocess_exec(
        str(pmkt_bin), "model", "ofi",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=child_env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=OFI_SUBPROCESS_TIMEOUT_S
        )
    except TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(
            f"ofi subprocess timed out after {OFI_SUBPROCESS_TIMEOUT_S}s"
        ) from None
    except asyncio.CancelledError:
        proc.kill()  # daemon shutdown must not orphan the child
        raise
    if proc.returncode != 0:
        tail = stderr.decode(errors="replace")[-400:]
        raise RuntimeError(
            f"ofi subprocess exited {proc.returncode}: {tail}"
        )
    # Success heartbeat — without it, a wedged-at-stale state is invisible
    # in the daemon log (the child's own log lines go to ITS stderr).
    summary = stdout.decode(errors="replace").strip()
    logger.info("ofi subprocess ok: %s", " ".join(summary.split()) or "(no output)")
    return {}
