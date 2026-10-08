"""DuckDB research surface over the raw lake + curated projections + hot store.

``connect(data_dir)`` returns an in-memory DuckDB connection with:

- ``raw_parquet`` — compacted partitions (raw-parquet/venue=*/endpoint=*/date=*/part-0.parquet)
- ``raw_jsonl``   — NOT-yet-compacted partitions, read straight from raw/*.jsonl.zst
- ``raw``         — UNION ALL of the two. **Exactly-once rule:** a raw
  partition key (venue, endpoint, date) is served from Parquet if its
  part-0.parquet exists, otherwise from its JSONL files — never both. The
  split is decided per partition at connect() time by directory listing, so a
  fresh connect() is needed to see files that landed later.
- ``trades``      — curated trades projection, if any partitions have been built.
- ``hot``         — the SQLite hot store, ATTACHed READ_ONLY (live collector
  state joins years of lake in one statement). Best-effort: skipped with a
  warning if the sqlite extension or the DB file is unavailable.

Caveats (documented, deliberate):
- ``payload``/``params`` in ``raw_jsonl`` are re-rendered by DuckDB's yyjson
  (raw UTF-8, not \\uXXXX-escaped), while ``raw_parquet`` carries the Python
  canonical serialization. VALUES are identical; bytes may differ across the
  two sources — always ``json_extract`` payloads, never byte-compare them.
- Live ``*.jsonl.zst.tmp`` writers are invisible until they rotate (they are
  incomplete by definition; rawlake recovery owns crashed ones).
- ``fetched_at`` is ISO-8601 UTC text; ``date``/``hour`` columns are derived
  from it (identical to the hive partition values by construction).

Curated projections (versioned pure functions over raw, data invariants — a
parser bug means: bump the version constant, fix the SQL, re-run the builder,
and the raw lake rebuilds the table from scratch):
- v1 TRADES (build_trades) over raw kalshi ``endpoint=trades``.
- v2 PRICE_SNAPSHOTS (build_price_snapshots) over raw kalshi
  ``endpoint=markets_candlesticks`` — a bid/ask/last/volume/OI time-series.
Prices are cast STRING → DECIMAL directly (DuckDB decimal-from-string parsing
is exact); nothing ever passes through DOUBLE (data invariants).
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

import duckdb

from . import duck
from .compact import PARQUET_PART_NAME

logger = logging.getLogger(__name__)

#: Bump when the raw→trades parse changes meaning; then rebuild all partitions.
PARSER_VERSION = 1

_RAW_JSON_COLUMNS = (
    "{'v': 'BIGINT', 'venue': 'VARCHAR', 'endpoint': 'VARCHAR', "
    "'params': 'JSON', 'fetched_at': 'VARCHAR', 'http_status': 'INTEGER', "
    "'elapsed_ms': 'BIGINT', 'payload': 'JSON'}"
)
# Biggest real JSON line observed is a ~4.6MB gamma_events page; 16MB is
# generous headroom. 128MB made read_json reserve giant per-file buffers and
# OOM the 17-file/1GB trades backfill scan instantly (2026-07-12).
_MAX_OBJECT_SIZE = 16 * 1024 * 1024
_RAW_VIEW_COLUMNS = (
    "v BIGINT, venue VARCHAR, endpoint VARCHAR, params VARCHAR, "
    "fetched_at VARCHAR, http_status INTEGER, elapsed_ms BIGINT, "
    'payload VARCHAR, "hour" VARCHAR, "date" VARCHAR'
)


def raw_root(data_dir: Path) -> Path:
    return Path(data_dir) / "raw"


def raw_parquet_root(data_dir: Path) -> Path:
    return Path(data_dir) / "raw-parquet"


def curated_root(data_dir: Path) -> Path:
    return Path(data_dir) / "curated"


def external_root(data_dir: Path) -> Path:
    return Path(data_dir) / "external"


def hot_db_path(data_dir: Path) -> Path:
    return Path(data_dir) / "db" / "hot.sqlite"


def _sql_str(path: Path | str) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _sql_list(paths: list[Path]) -> str:
    return "[" + ", ".join(_sql_str(p) for p in paths) + "]"


def _partition_key(date_dir: Path) -> tuple[str, str, str]:
    return (
        date_dir.parent.parent.name.split("=", 1)[1],  # venue
        date_dir.parent.name.split("=", 1)[1],         # endpoint
        date_dir.name.split("=", 1)[1],                # date
    )


def _discover_raw(
    data_dir: Path,
    endpoints: list[str] | None = None,
    venues: list[str] | None = None,
) -> tuple[list[Path], list[Path]]:
    """(parquet files, jsonl files) obeying the exactly-once rule.

    ``endpoints``/``venues`` restrict DISCOVERY by partition directory, so an
    endpoint-focused query never opens unrelated files at all — a WHERE
    endpoint=... on the full view still scans every capture (the WS firehose
    day OOM'd build_trades at 12.7GB before this existed, 2026-07-12)."""

    def _keep(key: tuple[str, str, str]) -> bool:
        venue, endpoint, _date = key
        return (venues is None or venue in venues) and (
            endpoints is None or endpoint in endpoints
        )

    parquet_files: list[Path] = []
    compacted: set[tuple[str, str, str]] = set()
    for pq in sorted(raw_parquet_root(data_dir).glob(
        f"venue=*/endpoint=*/date=*/{PARQUET_PART_NAME}"
    )):
        key = _partition_key(pq.parent)
        if not _keep(key):
            continue
        # part-0 is the gate (finalized last by compact); a gated partition
        # may carry additional size-bounded part-K files — serve them all.
        parquet_files.extend(sorted(pq.parent.glob("part-*.parquet")))
        compacted.add(key)
    jsonl_files: list[Path] = []
    for f in sorted(raw_root(data_dir).glob(
        "venue=*/endpoint=*/date=*/hour=*/part-*.jsonl.zst"
    )):
        key = _partition_key(f.parent.parent)
        if _keep(key) and key not in compacted:
            jsonl_files.append(f)
    return parquet_files, jsonl_files


def _empty_raw_select() -> str:
    casts = []
    for coldef in _RAW_VIEW_COLUMNS.split(", "):
        name, typ = coldef.rsplit(" ", 1)
        casts.append(f"CAST(NULL AS {typ}) AS {name}")
    return f"SELECT {', '.join(casts)} WHERE false"


def _raw_parquet_select(parquet_files: list[Path]) -> str:
    """Raw-view-shaped SELECT over compacted raw parquet parts.

    Shared by the connect() view and file-scoped builder batches
    (build_pm_trades). fetched_at is a plain column here, so range
    predicates on it push down into the parquet scan — payload is only
    decoded for surviving rows."""
    if not parquet_files:
        return _empty_raw_select()
    return (
        "SELECT v, venue, endpoint, params, fetched_at, http_status, elapsed_ms, "
        "payload, hour, substr(fetched_at, 1, 10) AS \"date\" "
        f"FROM read_parquet({_sql_list(parquet_files)})"
    )


def _raw_jsonl_select(jsonl_files: list[Path]) -> str:
    """Raw-view-shaped SELECT over uncompacted jsonl.zst captures.

    read_json decompresses and JSON-parses EVERY listed file on EVERY query
    — no statistics, nothing to prune with — so callers control cost purely
    through the file list. Builders that batch (build_pm_trades) must give
    each batch its own files rather than WHERE-filtering a larger list."""
    if not jsonl_files:
        return _empty_raw_select()
    return (
        "SELECT v, venue, endpoint, CAST(params AS VARCHAR) AS params, fetched_at, "
        "http_status, elapsed_ms, CAST(payload AS VARCHAR) AS payload, "
        'substr(fetched_at, 12, 2) AS "hour", substr(fetched_at, 1, 10) AS "date" '
        f"FROM read_json({_sql_list(jsonl_files)}, columns={_RAW_JSON_COLUMNS}, "
        f"format='newline_delimited', maximum_object_size={_MAX_OBJECT_SIZE})"
    )


def connect(
    data_dir: Path,
    *,
    attach_hot: bool = True,
    endpoints: list[str] | None = None,
    venues: list[str] | None = None,
) -> duckdb.DuckDBPyConnection:
    """DuckDB connection with raw/raw_parquet/raw_jsonl (+ trades, hot) wired up.

    File lists are a snapshot taken now — reconnect to see newer captures.
    ``endpoints``/``venues`` restrict which raw partitions back the views
    (partition-level pruning; see _discover_raw).
    """
    data_dir = Path(data_dir)
    # Guardrailed session (absolute spill dir, memory ceiling, thread cap —
    # duck.connect): this connection also runs INSIDE the collector daemon
    # every 15 min (ofi_score), where DuckDB's 80%-of-RAM default
    # memory_limit could exhaust the machine next to the live collector.
    # Raise PMKT_DUCKDB_MEMORY_LIMIT/PMKT_DUCKDB_THREADS for big ad-hoc
    # research scans.
    conn = duck.connect(data_dir)
    conn.execute("SET TimeZone='UTC'")  # strftime/timestamptz render in UTC

    parquet_files, jsonl_files = _discover_raw(data_dir, endpoints, venues)

    conn.execute(f"CREATE VIEW raw_parquet AS {_raw_parquet_select(parquet_files)}")
    conn.execute(f"CREATE VIEW raw_jsonl AS {_raw_jsonl_select(jsonl_files)}")

    conn.execute(
        "CREATE VIEW raw AS SELECT * FROM raw_parquet UNION ALL SELECT * FROM raw_jsonl"
    )

    trades_files = sorted(
        curated_root(data_dir).glob(f"trades/venue=*/date=*/{PARQUET_PART_NAME}")
    )
    if trades_files:
        conn.execute(
            "CREATE VIEW trades AS SELECT * FROM read_parquet("
            f"{_sql_list(trades_files)}, hive_partitioning=false)"
        )

    # hive_partitioning=false: the venue=/date= dirs are NOT re-materialized as
    # columns — venue_id lives inside the file; consumers derive date from ts.
    snapshot_files = sorted(
        curated_root(data_dir).glob(f"price_snapshots/venue=*/date=*/{PARQUET_PART_NAME}")
    )
    if snapshot_files:
        conn.execute(
            "CREATE VIEW price_snapshots AS SELECT * FROM read_parquet("
            f"{_sql_list(snapshot_files)}, hive_partitioning=false)"
        )

    # External datasets: Polymarket-v1 (arXiv 2606.04217) wallet-attributed trade tape
    pm_v1_dir = external_root(data_dir) / "polymarket-v1" / "daily_aligned"
    if pm_v1_dir.is_dir():
        pm_v1_files = sorted(pm_v1_dir.glob("*.parquet"))
        if pm_v1_files:
            conn.execute(
                "CREATE VIEW pm_v1_trades AS SELECT * FROM read_parquet("
                f"{_sql_list(pm_v1_files)})"
            )

    # Curated Polymarket wallet-attributed trades (Data-API forward collector)
    pm_trades_files = sorted(
        curated_root(data_dir).glob(f"pm_trades/date=*/{PARQUET_PART_NAME}")
    )
    if pm_trades_files:
        conn.execute(
            "CREATE VIEW pm_trades AS SELECT * FROM read_parquet("
            f"{_sql_list(pm_trades_files)}, hive_partitioning=false)"
        )

    # Curated Polymarket resolutions (conditionId → winning token/outcome)
    res_file = curated_root(data_dir) / "pm_resolutions" / PARQUET_PART_NAME
    if res_file.exists():
        conn.execute(
            "CREATE VIEW pm_resolutions AS SELECT * FROM read_parquet("
            f"{_sql_str(res_file)})"
        )

    # Curated wallet skill scores
    ws_file = curated_root(data_dir) / "wallet_skills" / PARQUET_PART_NAME
    if ws_file.exists():
        conn.execute(
            f"CREATE VIEW wallet_skills AS SELECT * FROM read_parquet({_sql_str(ws_file)})"
        )

    # Rolling OFI trade window (maintained by pmkt model ofi — a rebuildable
    # cache of the deduped trailing-24h Data-API tape; registered for
    # debugging, the scorer itself works on its own temp copy).
    ofi_file = curated_root(data_dir) / "ofi_window" / PARQUET_PART_NAME
    if ofi_file.exists():
        conn.execute(
            f"CREATE VIEW ofi_window AS SELECT * FROM read_parquet({_sql_str(ofi_file)})"
        )

    # Cross-venue matched contract pairs (Kalshi <-> Polymarket, same outcome)
    xw_file = curated_root(data_dir) / "venue_crosswalk" / PARQUET_PART_NAME
    if xw_file.exists():
        conn.execute(
            f"CREATE VIEW venue_crosswalk AS SELECT * FROM read_parquet({_sql_str(xw_file)})"
        )

    hot_path = hot_db_path(data_dir)
    if attach_hot and hot_path.exists():
        try:
            conn.execute(f"ATTACH {_sql_str(hot_path)} AS hot (TYPE sqlite, READ_ONLY)")
        except duckdb.Error as exc:
            logger.warning("could not ATTACH hot store %s: %s", hot_path, exc)

    return conn


def run_sql(data_dir: Path, sql: str) -> duckdb.DuckDBPyRelation:
    """Ad-hoc research/CLI helper: one-shot SQL over a fresh connect().

    Returns a DuckDB relation (lazily evaluated; it keeps the underlying
    database alive). ``.show()`` / ``.df()`` / ``.fetchall()`` as needed.
    For long sessions hold a persistent ``connect()`` connection instead.
    """
    return connect(data_dir).sql(sql)


# --------------------------------------------------------------------------
# Curated projection v1: trades
# --------------------------------------------------------------------------

# Pure-function parse over raw captures (data invariants). Notes:
# - http_status=200 only; error/429 captures stay raw-only.
# - The tape overlaps by design (60 s sweep overlap + slice refetch after
#   interruption), so the same venue_trade_id appears in multiple captures:
#   dedupe keeps the row from the LATEST capture (deterministic tiebreak).
# - Trades are partitioned by TRADE timestamp date (not capture date) so
#   midnight-straddling refetches dedupe into one partition.
# - CAST(string AS DECIMAL) is exact; nothing passes through DOUBLE.
_TRADES_STAGE_SQL = """
CREATE TEMP TABLE trades_exploded AS
WITH exploded AS (
    -- unnest in the SELECT list, NOT via LATERAL: a lateral join replicates
    -- the parent row (including the full multi-hundred-KB payload) once per
    -- exploded trade, which OOMs a real day (measured 2026-07-11: 2.7M
    -- trades -> >12 GB). This form drops the payload right after the array
    -- extraction and runs the same day in ~3 s.
    SELECT fetched_at AS _fetched_at,
           unnest(CAST(json_extract(payload, '$.trades') AS JSON[])) AS trade
    FROM raw
    WHERE venue = 'kalshi' AND endpoint IN ('trades', 'historical_trades') AND http_status = 200
)
SELECT
    'kalshi'                                                  AS venue_id,
    trade ->> 'trade_id'                                      AS venue_trade_id,
    trade ->> 'ticker'                                        AS venue_market_id,
    CAST(epoch_ms(CAST(trade ->> 'created_time' AS TIMESTAMPTZ)) // 1000
         AS BIGINT)                                           AS ts,
    CAST(trade ->> 'yes_price_dollars' AS DECIMAL(9,6))       AS price_prob,
    CAST(trade ->> 'no_price_dollars' AS DECIMAL(9,6))        AS no_price_prob,
    CAST(trade ->> 'count_fp' AS DECIMAL(18,2))               AS "size",
    trade ->> 'taker_outcome_side'                            AS taker_outcome_side,
    trade ->> 'taker_book_side'                               AS taker_book_side,
    CAST(trade ->> 'is_block_trade' AS BOOLEAN)               AS is_block,
    {parser_version}                                          AS parser_version,
    _fetched_at
FROM exploded
"""

_TRADES_DEDUPE_SQL = """
CREATE TEMP TABLE trades_stage AS
SELECT * EXCLUDE (_fetched_at),
       strftime(to_timestamp(ts), '%Y-%m-%d') AS trade_date
FROM trades_exploded
QUALIFY row_number() OVER (
    PARTITION BY venue_trade_id
    ORDER BY _fetched_at DESC, CAST(ts AS VARCHAR)
) = 1
"""

_TRADES_OUT_COLUMNS = (
    "venue_id, venue_trade_id, venue_market_id, ts, price_prob, no_price_prob, "
    '"size", taker_outcome_side, taker_book_side, is_block, parser_version'
)


def build_trades(
    data_dir: Path, dates: list[str] | None = None
) -> dict[str, Any]:
    """Rebuild the curated trades table from raw kalshi trades captures.

    Scans ALL trades captures visible in the ``raw`` view (Parquet-compacted
    and not-yet-compacted alike — a trade for day D can arrive in a capture
    days later, so a capture-window shortcut would silently drop late fills),
    dedupes on venue_trade_id, and writes one Parquet per TRADE date at
    ``curated/trades/venue=kalshi/date=D/part-0.parquet``. ``dates`` limits
    which partitions are (re)written, not what is scanned. Existing curated
    partitions are overwritten — curated is a rebuildable projection, and
    rebuild-on-parser-bump is the disaster-recovery story (data invariants).
    """
    data_dir = Path(data_dir)
    conn = connect(
        data_dir, attach_hot=False,
        venues=["kalshi"], endpoints=["trades", "historical_trades"],
    )
    # Full-payload JSON parsing is memory-hungry; bound worker parallelism so
    # peak stays comfortable while the 18M+-trade stage spills as needed.
    conn.execute("SET threads=4")
    try:
        n_captures, n_shapeless = conn.execute(
            "SELECT count(*), "
            "count(*) FILTER (WHERE json_extract(payload, '$.trades') IS NULL) "
            "FROM raw WHERE venue='kalshi' AND endpoint IN ('trades','historical_trades') AND http_status=200"
        ).fetchone()
        if n_shapeless:
            logger.warning(
                "build_trades: %d/%d captures have no 'trades' key — "
                "possible payload shape drift, investigate raw",
                n_shapeless, n_captures,
            )
        conn.execute(_TRADES_STAGE_SQL.format(parser_version=PARSER_VERSION))
        (rows_raw,) = conn.execute("SELECT count(*) FROM trades_exploded").fetchone()
        conn.execute(_TRADES_DEDUPE_SQL)
        (rows_unique,) = conn.execute("SELECT count(*) FROM trades_stage").fetchone()

        all_dates = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT trade_date FROM trades_stage ORDER BY 1"
            ).fetchall()
        ]
        targets = all_dates if dates is None else [d for d in all_dates if d in set(dates)]

        partitions: dict[str, int] = {}
        for date in targets:
            out_dir = curated_root(data_dir) / "trades" / "venue=kalshi" / f"date={date}"
            out_dir.mkdir(parents=True, exist_ok=True)
            for stale in out_dir.glob(".build-*"):
                stale.unlink()
            tmp = out_dir / f".build-{uuid.uuid4().hex[:8]}.parquet"
            conn.execute(
                f"COPY (SELECT {_TRADES_OUT_COLUMNS} FROM trades_stage "
                f"WHERE trade_date = {_sql_str(date)} "
                f"ORDER BY ts, venue_trade_id) "
                f"TO {_sql_str(tmp)} (FORMAT parquet, COMPRESSION zstd)"
            )
            (n,) = conn.execute(
                f"SELECT count(*) FROM read_parquet({_sql_str(tmp)})"
            ).fetchone()
            (expected,) = conn.execute(
                f"SELECT count(*) FROM trades_stage WHERE trade_date = {_sql_str(date)}"
            ).fetchone()
            if n != expected:
                tmp.unlink(missing_ok=True)
                raise RuntimeError(
                    f"build_trades {date}: parquet has {n} rows, stage has {expected}"
                )
            os.replace(tmp, out_dir / PARQUET_PART_NAME)
            partitions[date] = n
    finally:
        conn.close()

    report = {
        "captures": n_captures,
        "captures_without_trades": n_shapeless,
        "rows_raw": rows_raw,
        "rows_unique": rows_unique,
        "duplicates_dropped": rows_raw - rows_unique,
        "dedupe_ratio": round(rows_raw / rows_unique, 4) if rows_unique else None,
        "partitions": partitions,
        "parser_version": PARSER_VERSION,
        "built_at": int(time.time()),
    }
    logger.info(
        "build_trades: %d captures → %d raw rows → %d unique (%.2f%% dupes), "
        "%d partition(s), parser v%d",
        n_captures, rows_raw, rows_unique,
        100 * (rows_raw - rows_unique) / rows_raw if rows_raw else 0.0,
        len(partitions), PARSER_VERSION,
    )
    return report


# --------------------------------------------------------------------------
# Curated projection v2: price_snapshots (bid/ask/last/volume/OI time-series)
# --------------------------------------------------------------------------

#: Bump when the raw→price_snapshots parse changes meaning; rebuild all partitions.
SNAPSHOTS_PARSER_VERSION = 1

# Pure-function parse over raw captures (data invariants). Design notes:
# - KEYING: rows carry the NATURAL key (venue_id, venue_market_id, ts, source)
#   plus venue_token_id/outcome_index — NOT the hot-store instrument_id. That
#   surrogate has ZERO Kalshi rows (Kalshi markets are priced on the market row
#   directly) and is a rowid reassigned on every hot-store rebuild; keying an
#   immutable Parquet lake on it would silently orphan history (the lake is the
#   system of record, data invariants). This mirrors the trades table, which the
#   model already joins on natural keys.
# - v1 SOURCE is Kalshi 1-minute candles (endpoint=markets_candlesticks). Each
#   candle CLOSE is one 'candle'-source snapshot: yes_bid/yes_ask/price closes →
#   bid/ask/last prob; volume_fp is the per-PERIOD (per-minute) count, not
#   cumulative; open_interest_fp is the level at period end. Candles carry no
#   volume_24h or liquidity → those columns are NULL on 'candle' rows. A candle
#   with no trade in its minute has no price.close_dollars → last_prob is NULL
#   (bid/ask still present).
# - The candle sweep re-captures a rolling window of minutes, so the same
#   (market, minute) candle appears in many captures (~37% dupes measured):
#   dedupe on (venue_id, venue_market_id, venue_token_id, ts, source) keeping
#   the LATEST capture. Partition by the candle PERIOD date (from ts), not the
#   capture date, so minutes straddling midnight land in one partition.
# - mid_prob = (bid+ask)/2 but written as * 0.5 in DECIMAL: DuckDB's `/ 2`
#   silently promotes DECIMAL → DOUBLE (verified), which the precision rules forbid.
# - DEFERRED sources (not in v1): the 'poll' source (endpoint=markets) would add
#   volume_24h + liquidity, but that endpoint interleaves the 30-min settled
#   sweep (thousands of settled-market objects per page) with the hourly open
#   sweep, and scanning it to filter status='active' OOMs — needs a bounded
#   per-date scan or a collector-side open/settled endpoint split first.
#   Polymarket (clob_prices_history) is a single price p with no bid/ask and has
#   no model consumer yet.
# Staged per (date, hour) batch with FILE-SCOPED sources: candlestick captures
# carry ~39k market objects each (one JSON string per market), and DuckDB
# materialises a full 2048-row vector of them at once — a global scan of all
# captures allocates GBs before any row filter applies, OOMing even at 8GB
# once the partition spans ~34M raw rows (2026-07-16 incident, double the v1
# 17M measured build). Same pattern as build_pm_trades / _pm_trades_batches.
_SNAPSHOTS_STAGE_SELECT = """
WITH markets AS (
    -- unnest in the SELECT list, never LATERAL (LATERAL replicates the full
    -- payload per element → OOM; see build_trades). Two-level unnest: the
    -- per-market array first, then each market's candlesticks array.
    SELECT fetched_at AS _fetched_at,
           unnest(CAST(json_extract(payload, '$.markets') AS JSON[])) AS mkt
    FROM ({source})
    WHERE venue = 'kalshi' AND endpoint = 'markets_candlesticks' AND http_status = 200
      AND {batch_where}
),
candles AS (
    SELECT _fetched_at,
           mkt ->> 'market_ticker' AS venue_market_id,
           unnest(CAST(json_extract(mkt, '$.candlesticks') AS JSON[])) AS c
    FROM markets
)
SELECT
    'kalshi'                                                  AS venue_id,
    venue_market_id,
    CAST(NULL AS VARCHAR)                                     AS venue_token_id,
    0                                                         AS outcome_index,
    CAST(c ->> 'end_period_ts' AS BIGINT)                     AS ts,
    CAST(c -> 'yes_bid' ->> 'close_dollars' AS DECIMAL(9,6))  AS bid_prob,
    CAST(c -> 'yes_ask' ->> 'close_dollars' AS DECIMAL(9,6))  AS ask_prob,
    CAST(c -> 'price'   ->> 'close_dollars' AS DECIMAL(9,6))  AS last_prob,
    CAST(c ->> 'volume_fp' AS DECIMAL(18,2))                  AS volume,
    CAST(NULL AS DECIMAL(18,2))                               AS volume_24h,
    CAST(c ->> 'open_interest_fp' AS DECIMAL(18,2))           AS open_interest,
    CAST(NULL AS DECIMAL(18,4))                               AS liquidity,
    'candle'                                                  AS source,
    {parser_version}                                          AS parser_version,
    _fetched_at
FROM candles
"""

_SNAPSHOTS_DEDUPE_SQL = """
CREATE TEMP TABLE snapshots_stage AS
SELECT * EXCLUDE (_fetched_at),
       CASE WHEN bid_prob IS NOT NULL AND ask_prob IS NOT NULL
            THEN CAST((bid_prob + ask_prob) * CAST(0.5 AS DECIMAL(2,1))
                      AS DECIMAL(12,7))
       END                                                    AS mid_prob,
       strftime(to_timestamp(ts), '%Y-%m-%d')                AS snapshot_date
FROM snapshots_exploded
-- Drop malformed candles (no usable key/timestamp) explicitly rather than let a
-- NULL ts derive a NULL snapshot_date → a bogus date=None partition. Kalshi
-- always ships end_period_ts + market_ticker, so this is defence-in-depth (0
-- rows filtered across the real 17M-row build), not a live path.
WHERE ts IS NOT NULL AND venue_market_id IS NOT NULL
QUALIFY row_number() OVER (
    PARTITION BY venue_id, venue_market_id, venue_token_id, ts, source
    ORDER BY _fetched_at DESC, CAST(ts AS VARCHAR)
) = 1
"""

_SNAPSHOTS_OUT_COLUMNS = (
    "venue_id, venue_market_id, venue_token_id, outcome_index, ts, "
    "bid_prob, ask_prob, mid_prob, last_prob, "
    "volume, volume_24h, open_interest, liquidity, source, parser_version"
)


def _snapshots_batches(data_dir: Path) -> Iterator[tuple[str, str]]:
    """(source_select, batch_where) pairs covering every candlestick capture exactly once.

    Mirrors _pm_trades_batches: compacted parquet dates are sliced into 24
    fetched_at-range hour sub-batches (the plain column pushes down into
    parquet stats); each uncompacted (date, hour) gets its own jsonl file
    list so no file is re-parsed by more than one batch."""
    parquet_files, jsonl_files = _discover_raw(
        data_dir, ["markets_candlesticks"], ["kalshi"]
    )

    pq_by_date: dict[str, list[Path]] = {}
    for f in parquet_files:
        pq_by_date.setdefault(_partition_key(f.parent)[2], []).append(f)
    jl_by_hour: dict[tuple[str, str], list[Path]] = {}
    for f in jsonl_files:
        date = f.parent.parent.name.split("=", 1)[1]
        hour = f.parent.name.split("=", 1)[1]
        jl_by_hour.setdefault((date, hour), []).append(f)

    for date, files in sorted(pq_by_date.items()):
        src = _raw_parquet_select(files)
        for h in range(24):
            lo = _sql_str(f"{date}T{h:02d}")
            hi = _sql_str(f"{date}T{h + 1:02d}")
            yield src, f"fetched_at >= {lo} AND fetched_at < {hi}"
    for (_date, _hour), files in sorted(jl_by_hour.items()):
        yield _raw_jsonl_select(files), "1=1"


def build_price_snapshots(
    data_dir: Path, dates: list[str] | None = None
) -> dict[str, Any]:
    """Rebuild the curated price_snapshots table from raw kalshi candle captures.

    Scans ALL markets_candlesticks captures (Parquet-compacted and not-yet-
    compacted) in file-scoped (_snapshots_batches) per-(date, hour) batches:
    each candlestick capture contains ~39k market objects whose JSON strings
    a full-table scan materialises all at once, OOMing even at 8GB once the
    partition spans ~34M raw rows (2026-07-16; mirrors build_pm_trades fix).

    Dedupes on (venue_id, venue_market_id, venue_token_id, ts, source), keeps
    the latest capture, and writes one Parquet per (venue, candle-period date)
    at ``curated/price_snapshots/venue=V/date=D/part-0.parquet``. ``dates``
    limits which partitions are (re)written, not what is scanned. Existing
    curated partitions are overwritten — curated is a rebuildable projection
    (data invariants).

    v1 is Kalshi candle-only; see the module notes for deferred sources.
    """
    data_dir = Path(data_dir)
    conn = duck.connect(data_dir)
    conn.execute("SET TimeZone='UTC'")
    _fat_payload_session(conn)
    try:
        conn.execute(
            "CREATE TEMP TABLE snapshots_exploded AS "
            + _SNAPSHOTS_STAGE_SELECT.format(
                source=_empty_raw_select(),
                parser_version=SNAPSHOTS_PARSER_VERSION,
                batch_where="1=1",
            )
        )
        n_captures = 0
        n_shapeless = 0
        for source, batch_where in _snapshots_batches(data_dir):
            c, s = conn.execute(
                "SELECT count(*), "
                "count(*) FILTER (WHERE json_extract(payload, '$.markets') IS NULL) "
                f"FROM ({source}) WHERE venue='kalshi' "
                f"AND endpoint='markets_candlesticks' AND http_status=200 AND {batch_where}"
            ).fetchone()
            n_captures += c
            n_shapeless += s
            if c - s > 0:
                conn.execute(
                    "INSERT INTO snapshots_exploded "
                    + _SNAPSHOTS_STAGE_SELECT.format(
                        source=source,
                        parser_version=SNAPSHOTS_PARSER_VERSION,
                        batch_where=batch_where,
                    )
                )
        if n_shapeless:
            logger.warning(
                "build_price_snapshots: %d/%d captures have no 'markets' key — "
                "possible payload shape drift, investigate raw",
                n_shapeless, n_captures,
            )
        (rows_raw,) = conn.execute("SELECT count(*) FROM snapshots_exploded").fetchone()
        conn.execute(_SNAPSHOTS_DEDUPE_SQL)
        (rows_unique,) = conn.execute("SELECT count(*) FROM snapshots_stage").fetchone()

        all_pairs = conn.execute(
            "SELECT DISTINCT venue_id, snapshot_date FROM snapshots_stage ORDER BY 1, 2"
        ).fetchall()
        wanted = None if dates is None else set(dates)
        targets = [
            (v, d) for v, d in all_pairs if wanted is None or d in wanted
        ]

        partitions: dict[str, int] = {}
        for venue, date in targets:
            out_dir = (
                curated_root(data_dir) / "price_snapshots"
                / f"venue={venue}" / f"date={date}"
            )
            out_dir.mkdir(parents=True, exist_ok=True)
            for stale in out_dir.glob(".build-*"):
                stale.unlink()
            tmp = out_dir / f".build-{uuid.uuid4().hex[:8]}.parquet"
            conn.execute(
                f"COPY (SELECT {_SNAPSHOTS_OUT_COLUMNS} FROM snapshots_stage "
                f"WHERE venue_id = {_sql_str(venue)} AND snapshot_date = {_sql_str(date)} "
                f"ORDER BY venue_market_id, ts) "
                f"TO {_sql_str(tmp)} (FORMAT parquet, COMPRESSION zstd)"
            )
            (n,) = conn.execute(
                f"SELECT count(*) FROM read_parquet({_sql_str(tmp)})"
            ).fetchone()
            (expected,) = conn.execute(
                f"SELECT count(*) FROM snapshots_stage "
                f"WHERE venue_id = {_sql_str(venue)} AND snapshot_date = {_sql_str(date)}"
            ).fetchone()
            if n != expected:
                tmp.unlink(missing_ok=True)
                raise RuntimeError(
                    f"build_price_snapshots {venue}/{date}: parquet has {n} rows, "
                    f"stage has {expected}"
                )
            os.replace(tmp, out_dir / PARQUET_PART_NAME)
            partitions[f"{venue}/{date}"] = n
    finally:
        conn.close()

    report = {
        "captures": n_captures,
        "captures_without_markets": n_shapeless,
        "rows_raw": rows_raw,
        "rows_unique": rows_unique,
        "duplicates_dropped": rows_raw - rows_unique,
        "dedupe_ratio": round(rows_raw / rows_unique, 4) if rows_unique else None,
        "partitions": partitions,
        "parser_version": SNAPSHOTS_PARSER_VERSION,
        "built_at": int(time.time()),
    }
    logger.info(
        "build_price_snapshots: %d captures → %d raw rows → %d unique (%.2f%% dupes), "
        "%d partition(s), parser v%d",
        n_captures, rows_raw, rows_unique,
        100 * (rows_raw - rows_unique) / rows_raw if rows_raw else 0.0,
        len(partitions), SNAPSHOTS_PARSER_VERSION,
    )
    return report


# --------------------------------------------------------------------------
# Curated projection v3: wallet_skills (Gómez-Cram randomization test)
# --------------------------------------------------------------------------

WALLET_SKILLS_VERSION = 3  # v3: + v2-era resolved taker rows (pm_trades × pm_resolutions)
_WALLET_SKILLS_MIN_TRADES = 10
_WALLET_SKILLS_Z_THRESHOLD = 1.96

# v1 CTF Exchange end / v2 migration (2026-04-28 00:00 UTC) — the coverage
# boundary between the arXiv corpus and the collected Data-API tape. Same instant
# as polymarket.backfill.V1_END_TS; duplicated to keep lake import-light.
PM_V1_END_TS = 1_777_334_400

# Analytical randomization test: under H0 (no directional skill), independently
# flipping each trade's direction gives a simulated PnL distribution with
# mean=0, var=Σ(PnL_i²). The z-score = Σ(PnL_i) / √Σ(PnL_i²). No Monte
# Carlo needed.
#
# PnL per trade (pre-fees):
#   sign  = +1 if BUY, -1 if SELL
#   won   = 1 if outcome resolved to this token, 0 otherwise
#   pnl   = sign * usdc_amount * (won - price) / price
#
# Each v1 fill is unfolded into taker + maker rows so a wallet's skill score
# reflects all activity regardless of execution role. v2-era rows (the collected
# Data-API tape, ts > PM_V1_END_TS, outcomes via pm_resolutions) are
# taker-only — takerOnly=true is all the Data-API serves — so post-migration
# activity contributes aggressive flow only. usdc = size × price (Data-API
# size is shares).
_WALLET_SKILLS_V2_UNION = """
  UNION ALL
  SELECT lower(t.proxy_wallet)                             AS wallet,
         CASE WHEN t.taker_side = 'BUY' THEN 1 ELSE -1 END AS sign,
         -- cast operands BEFORE multiplying: DECIMAL(18,6) x DECIMAL(9,6)
         -- overflows DECIMAL on million-share fills (hit in production)
         CAST(t."size" AS DOUBLE) * CAST(t.price_prob AS DOUBLE)
                                                           AS usdc_amount,
         CAST(t.price_prob AS DOUBLE)                      AS price,
         CASE WHEN t.venue_token_id = r.winning_token_id
              THEN 1 ELSE 0 END                            AS won,
         t.venue_market_id                                 AS condition_id
  FROM pm_trades t
  JOIN pm_resolutions r ON r.venue_market_id = t.venue_market_id
  WHERE t.ts > {v1_end_ts}
    AND t.price_prob > 0 AND t.price_prob < 1
    AND t."size" > 0
    AND t.proxy_wallet IS NOT NULL
    AND t.venue_token_id IS NOT NULL
    AND COALESCE(hash(lower(t.proxy_wallet)) % {buckets}, 0) = {bucket}
"""

# Wallet-hash bucketed build (2026-08-05, third fold in this lineage). The
# monolithic query OOM'd 2026-08-01 in a NEW way after the v2 tape grew 19x:
# at a 7GB ceiling it exhausted swap and wedged the collector for 25h; at 5GB
# it wrote 123GB of DuckDB spill and saturated the disk. Three stacked causes,
# all fixed here:
#   1. `resolved` was a SELECT * CTE referenced by BOTH unfold branches —
#      ~550M resolved rows x all 24 corpus columns staged for reuse. The
#      branches below each scan pm_v1_trades directly with the filters
#      duplicated, so no multi-reference CTE exists to materialize, and only
#      the columns the math needs leave the scan.
#   2. `category`/`block_timestamp` rode the ~1.2B-row unfold but were never
#      read downstream — dropped.
#   3. The (wallet, market) group state was global. The whole computation now
#      folds by COALESCE(hash(lower(wallet)) % {buckets}, 0): every row's
#      contribution belongs to exactly ONE wallet (taker and maker rows
#      attribute to different wallets, so each branch filters on ITS OWN
#      wallet column), a wallet lives entirely in its bucket, and per-pass
#      group state drops to ~1/N. The hash MUST wrap the same lower()
#      expression as the grouping key — hashing the raw column would split
#      case-variant spellings of one wallet across buckets and emit duplicate
#      rows. (The COALESCE is version-proofing only: DuckDB 1.5.4 hash(NULL)
#      returns a non-NULL constant, so NULL wallets already route to one
#      bucket without it — verified 2026-08-05.) Cost: the corpus is scanned
#      2N+N times, but daily_aligned is 554MB of parquet with column pruning
#      — measured ~55s/bucket, ~15 min/build, vs. a job whose global group
#      state (~142M (wallet,market) groups ≈ 13-14GB) cannot finish at ANY
#      ceiling on a 16GB box. N=16 buckets peak ~2.8GB RSS each.
# The two-level fold (2026-07-21: per-market first, so the wallet rollup
# never runs a grouped count(DISTINCT)) is preserved inside each bucket.
# Output is identical to the monolithic query: sums are associative,
# HAVING is per-wallet, and the final COPY re-sorts the assembled stage.
_WALLET_SKILLS_BUCKETS = 16

_WALLET_SKILLS_BUCKET_SQL = """
WITH unfolded AS (
  SELECT lower(taker) AS wallet,
         CASE WHEN taker_direction = 'BUY' THEN 1 ELSE -1 END AS sign,
         usdc_amount, price,
         CASE WHEN outcome_label = winning_outcome_label
              THEN 1 ELSE 0 END AS won,
         condition_id
  FROM pm_v1_trades
  WHERE winning_outcome_label IS NOT NULL
    AND winning_outcome_label != ''
    AND price > 0 AND price < 1
    AND usdc_amount > 0
    AND COALESCE(hash(lower(taker)) % {buckets}, 0) = {bucket}
  UNION ALL
  SELECT lower(maker) AS wallet,
         CASE WHEN taker_direction = 'BUY' THEN -1 ELSE 1 END AS sign,
         usdc_amount, price,
         CASE WHEN outcome_label = winning_outcome_label
              THEN 1 ELSE 0 END AS won,
         condition_id
  FROM pm_v1_trades
  WHERE winning_outcome_label IS NOT NULL
    AND winning_outcome_label != ''
    AND price > 0 AND price < 1
    AND usdc_amount > 0
    AND COALESCE(hash(lower(maker)) % {buckets}, 0) = {bucket}
{v2_union}),
trades AS (
  SELECT wallet,
         sign * usdc_amount * (won - price) / price AS pnl,
         usdc_amount, condition_id
  FROM unfolded
),
per_market AS (
  SELECT wallet, condition_id,
         count(*)         AS m_trades,
         sum(usdc_amount) AS m_volume,
         sum(pnl)         AS m_pnl,
         sum(pnl * pnl)   AS m_pnl2
  FROM trades
  GROUP BY wallet, condition_id
)
SELECT
  wallet,
  sum(m_trades)                AS n_trades,
  count(*)                     AS n_markets,
  sum(m_volume)                AS total_volume,
  sum(m_pnl)                   AS actual_pnl,
  sqrt(sum(m_pnl2))            AS sigma,
  CASE WHEN sqrt(sum(m_pnl2)) > 0
       THEN sum(m_pnl) / sqrt(sum(m_pnl2))
       ELSE 0 END              AS z_score,
  {version}                    AS scorer_version
FROM per_market
GROUP BY wallet
HAVING sum(m_trades) >= {min_trades}
"""

_WALLET_SKILLS_OUT_COLUMNS = (
    "wallet, n_trades, n_markets, total_volume, actual_pnl, sigma, z_score, "
    "scorer_version"
)


def build_wallet_skills(data_dir: Path) -> dict[str, Any]:
    """Build per-wallet skill scores from the Polymarket-v1 dataset.

    Reads the ``pm_v1_trades`` view (arXiv 2606.04217 daily_aligned layer),
    computes the Gómez-Cram analytical randomization z-score for every wallet
    with enough trades, and writes a single Parquet at
    ``curated/wallet_skills/part-0.parquet``.
    """
    data_dir = Path(data_dir)
    conn = connect(data_dir, attach_hot=False)
    # One-shot research build, never daemon-resident: the 602M-row corpus
    # unfolds to 1.2B rows before the per-wallet aggregation. Since the
    # 2026-08-05 bucketed fold each pass holds ~1/N of the group state, so
    # 6GB fits beside the live collector on the 16GB box (the pre-fold 10GB
    # default was itself an OOM at the 2026-08-01 corpus size — see the
    # _WALLET_SKILLS_BUCKET_SQL comment). PMKT_DUCKDB_MEMORY_LIMIT still wins.
    conn.execute(
        "SET memory_limit = "
        f"'{os.environ.get('PMKT_DUCKDB_MEMORY_LIMIT', '6GB')}'"
    )

    has_v1 = False
    try:
        conn.execute("SELECT 1 FROM pm_v1_trades LIMIT 0")
        has_v1 = True
    except duckdb.CatalogException:
        pass
    if not has_v1:
        raise RuntimeError(
            "pm_v1_trades view not available — download the Polymarket-v1 "
            "dataset to data/external/polymarket-v1/daily_aligned/ first"
        )

    # v2-era rows need both the curated tape and the resolutions projection;
    # without them the build degrades to v1-only (fresh envs, tests).
    has_v2 = True
    for view in ("pm_trades", "pm_resolutions"):
        try:
            conn.execute(f"SELECT 1 FROM {view} LIMIT 0")
        except duckdb.CatalogException:
            has_v2 = False
    if not has_v2:
        logger.warning(
            "build_wallet_skills: pm_trades/pm_resolutions not available — "
            "scoring v1 corpus only (run build-pm-trades + build-pm-resolutions "
            "for the merged tape)"
        )

    try:
        (n_input,) = conn.execute("SELECT count(*) FROM pm_v1_trades").fetchone()
        (n_resolved,) = conn.execute(
            "SELECT count(*) FROM pm_v1_trades "
            "WHERE winning_outcome_label IS NOT NULL AND winning_outcome_label != ''"
        ).fetchone()
        # The v2 boundary is the corpus's ACTUAL last fill, not the nominal
        # migration midnight: daily_aligned runs ~7h past 2026-04-28 00:00
        # UTC (max block_timestamp 07:00:40), and the bridge crawls into that
        # window too — a fixed midnight boundary double-counts the same fills
        # as v1 taker rows AND v2 rows, inflating |z| (review 2026-07-15;
        # ~1.6M fills at full bridge coverage). Using the corpus max instead
        # undercounts only genuine v2-exchange fills inside the migration
        # window — the safe direction.
        v1_end_ts = PM_V1_END_TS
        n_v2_resolved = 0
        if has_v2:
            (v1_max,) = conn.execute(
                "SELECT max(block_timestamp) FROM pm_v1_trades"
            ).fetchone()
            if v1_max is not None:
                v1_end_ts = max(PM_V1_END_TS, int(v1_max))
            (n_v2_resolved,) = conn.execute(
                "SELECT count(*) FROM pm_trades t "
                "JOIN pm_resolutions r ON r.venue_market_id = t.venue_market_id "
                f"WHERE t.ts > {v1_end_ts}"
            ).fetchone()

        for bucket in range(_WALLET_SKILLS_BUCKETS):
            body = _WALLET_SKILLS_BUCKET_SQL.format(
                version=WALLET_SKILLS_VERSION,
                min_trades=_WALLET_SKILLS_MIN_TRADES,
                buckets=_WALLET_SKILLS_BUCKETS,
                bucket=bucket,
                v2_union=(
                    _WALLET_SKILLS_V2_UNION.format(
                        v1_end_ts=v1_end_ts,
                        buckets=_WALLET_SKILLS_BUCKETS,
                        bucket=bucket,
                    )
                    if has_v2 else ""
                ),
            )
            conn.execute(
                ("CREATE TEMP TABLE wallet_skills_stage AS " if bucket == 0
                 else "INSERT INTO wallet_skills_stage ") + body
            )
            logger.info(
                "build_wallet_skills: bucket %d/%d done",
                bucket + 1, _WALLET_SKILLS_BUCKETS,
            )
        (n_wallets,) = conn.execute(
            "SELECT count(*) FROM wallet_skills_stage"
        ).fetchone()
        (n_skilled,) = conn.execute(
            f"SELECT count(*) FROM wallet_skills_stage WHERE z_score > {_WALLET_SKILLS_Z_THRESHOLD}"
        ).fetchone()
        (n_anti,) = conn.execute(
            f"SELECT count(*) FROM wallet_skills_stage WHERE z_score < -{_WALLET_SKILLS_Z_THRESHOLD}"
        ).fetchone()

        out_dir = curated_root(data_dir) / "wallet_skills"
        out_dir.mkdir(parents=True, exist_ok=True)
        for stale in out_dir.glob(".build-*"):
            stale.unlink()
        tmp = out_dir / f".build-{uuid.uuid4().hex[:8]}.parquet"
        conn.execute(
            f"COPY (SELECT {_WALLET_SKILLS_OUT_COLUMNS} FROM wallet_skills_stage "
            f"ORDER BY z_score DESC) "
            f"TO {_sql_str(tmp)} (FORMAT parquet, COMPRESSION zstd)"
        )
        (n_written,) = conn.execute(
            f"SELECT count(*) FROM read_parquet({_sql_str(tmp)})"
        ).fetchone()
        if n_written != n_wallets:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(
                f"build_wallet_skills: parquet has {n_written} rows, "
                f"stage has {n_wallets}"
            )
        os.replace(tmp, out_dir / PARQUET_PART_NAME)
    finally:
        conn.close()

    report = {
        "input_trades": n_input,
        "resolved_trades": n_resolved,
        "v2_resolved_trades": n_v2_resolved,
        "v2_included": int(has_v2),
        "wallets_scored": n_wallets,
        "skilled": n_skilled,
        "skilled_pct": round(100 * n_skilled / n_wallets, 1) if n_wallets else 0,
        "anti_skilled": n_anti,
        "anti_skilled_pct": round(100 * n_anti / n_wallets, 1) if n_wallets else 0,
        "min_trades": _WALLET_SKILLS_MIN_TRADES,
        "z_threshold": _WALLET_SKILLS_Z_THRESHOLD,
        "scorer_version": WALLET_SKILLS_VERSION,
        "built_at": int(time.time()),
    }
    logger.info(
        "build_wallet_skills: %d input trades (%d resolved) → %d wallets "
        "(≥%d trades), %d skilled (%.1f%%), %d anti-skilled (%.1f%%), v%d",
        n_input, n_resolved, n_wallets,
        _WALLET_SKILLS_MIN_TRADES,
        n_skilled, report["skilled_pct"],
        n_anti, report["anti_skilled_pct"],
        WALLET_SKILLS_VERSION,
    )
    return report


# --------------------------------------------------------------------------
# Curated projection v4: pm_trades (Polymarket Data-API wallet-attributed)
# --------------------------------------------------------------------------

PM_TRADES_PARSER_VERSION = 1

# Pure-function parse over raw captures (data invariants). Notes:
# - The Data-API /trades response is a JSON ARRAY of flat trade objects
#   (NOT nested under a key like Kalshi's {"trades": [...]}). DuckDB's
#   json_type(payload) = 'ARRAY' guards against error responses.
# - No native trade id (data invariants): derive via md5 over the key fields.
#   One txHash can have many fills, so txHash alone isn't unique.
# - Prices are 0–1 probability (already venue-native, data invariants).
#   CAST(string AS DECIMAL) is exact; nothing passes through DOUBLE.
# - Dedup: the sweep re-fetches a rolling overlap (watermark-based), so the
#   same trade appears in multiple captures — dedupe on venue_trade_id,
#   keeping the row from the LATEST capture.
# Staged per (date, hour) batch with FILE-SCOPED sources: bridge-backfill
# pages run ~1MB each (1000 trades + display junk), and DuckDB's JSON cast
# materializes whole payload strings per 2048-row vector — a single global
# scan allocates GBs regardless of memory_limit (OOM'd at 10GB on two days of
# bridge captures, 2026-07-15). One hour of captures per INSERT keeps the
# parse peak bounded; the extracted rows are slim so the cumulative temp
# table stays small. Each batch reads ONLY its own files ({source} from
# _pm_trades_batches): batching via WHERE on the connect() raw view instead
# re-decompressed and re-JSON-parsed every uncompacted jsonl file on every
# one of the 24 × n_dates batch queries — read_json has no statistics to
# prune with — making a rebuild O(24 × dates × uncompacted bytes), multi-hour
# whenever compaction fell behind (review finding 2026-07-16).
_PM_TRADES_STAGE_SELECT = """
WITH exploded AS (
    SELECT fetched_at AS _fetched_at,
           unnest(CAST(payload AS JSON[])) AS trade
    FROM ({source})
    WHERE venue = 'polymarket'
      AND endpoint IN ('data_api_trades', 'data_api_trades_backfill')
      AND http_status = 200
      AND json_type(payload) = 'ARRAY'
      AND {batch_where}
)
SELECT
    'polymarket'                                              AS venue_id,
    md5(
        COALESCE(trade ->> 'transactionHash', '') || '|' ||
        COALESCE(trade ->> 'asset', '') || '|' ||
        COALESCE(trade ->> 'proxyWallet', '') || '|' ||
        COALESCE(trade ->> 'side', '') || '|' ||
        COALESCE(trade ->> 'size', '') || '|' ||
        COALESCE(trade ->> 'price', '') || '|' ||
        COALESCE(trade ->> 'timestamp', '')
    )                                                         AS venue_trade_id,
    trade ->> 'conditionId'                                   AS venue_market_id,
    trade ->> 'asset'                                         AS venue_token_id,
    CAST(trade ->> 'timestamp' AS BIGINT)                     AS ts,
    CAST(trade ->> 'price' AS DECIMAL(9,6))                   AS price_prob,
    CAST(trade ->> 'size' AS DECIMAL(18,6))                   AS "size",
    trade ->> 'side'                                          AS taker_side,
    lower(trade ->> 'proxyWallet')                             AS proxy_wallet,
    trade ->> 'transactionHash'                               AS tx_hash,
    {parser_version}                                          AS parser_version,
    _fetched_at
FROM exploded
"""

# Dedupe folded into hash buckets of the dedupe KEY. A global QUALIFY window
# over the whole exploded corpus pins its entire sort/hash state at once and
# stopped fitting once the forward tape + bridge backfill roughly doubled the
# capture count (live 2026-07-31: OOM at 6.5/6.5GiB, right after the INSERT
# side was fixed). Bucketing on venue_trade_id is EXACTLY equivalent — every
# duplicate of a trade shares its key, so it lands in the same bucket — while
# bounding the window's working set to 1/N. Same fold family as
# _WALLET_SKILLS_BUCKET_SQL (2026-08-05; its two-level predecessor 2026-07-21)
# (2026-07-21) and the compaction part-groups (2026-07-19).
# COALESCE keeps NULL-keyed rows in one bucket so they collapse to a single
# row exactly as the global PARTITION BY did.
_PM_TRADES_DEDUPE_BUCKETS = 16

_PM_TRADES_DEDUPE_SELECT = """
SELECT * EXCLUDE (_fetched_at),
       strftime(to_timestamp(ts), '%Y-%m-%d') AS trade_date
FROM pm_trades_exploded
WHERE ts IS NOT NULL AND venue_market_id IS NOT NULL
  AND COALESCE(hash(venue_trade_id) % {buckets}, 0) = {bucket}
QUALIFY row_number() OVER (
    PARTITION BY venue_trade_id
    ORDER BY _fetched_at DESC
) = 1
"""

_PM_TRADES_OUT_COLUMNS = (
    "venue_id, venue_trade_id, venue_market_id, venue_token_id, ts, "
    'price_prob, "size", taker_side, proxy_wallet, tx_hash, parser_version'
)


def _fat_payload_session(conn: "duckdb.DuckDBPyConnection") -> None:
    """Session knobs for builders that JSON-parse fat capture payloads.

    Bridge/gamma pages run ~1MB each and DuckDB parses a full 2048-row
    vector of them at once: ~1.2GB of strings + 3-5x parse overhead PER
    VECTOR, irrespective of memory_limit or batch size. These builders are
    one-shot manual runs (never daemon-resident), so they get a higher
    ceiling than duck.connect's 4GB daemon default, and threads=2 caps how
    many fat vectors are in flight. PMKT_DUCKDB_MEMORY_LIMIT still wins.

    preserve_insertion_order=false because the staging tables these build
    grow with the raw lake and the INSERT side is what OOMs first (live
    2026-07-31: build_pm_trades died mid-INSERT at 5.4/5.5GiB after the
    forward tape + bridge backfill roughly doubled the capture count).
    Holding insert order buys nothing here — every consumer dedupes with an
    explicit ORDER BY (_fetched_at DESC) and then writes date-partitioned
    output, so row order out of the staging table is never load-bearing."""
    mem = os.environ.get("PMKT_DUCKDB_MEMORY_LIMIT", "8GB")
    conn.execute(f"SET memory_limit = '{mem}'")
    conn.execute("SET threads = 2")
    conn.execute("SET preserve_insertion_order = false")


def _pm_trades_batches(
    data_dir: Path, endpoints: list[str]
) -> Iterator[tuple[str, str]]:
    """(source_select, batch_where) pairs covering every capture exactly once.

    File-scoped batching (see the staging comment above): each compacted date
    scans its own parquet part(s) in 24 fetched_at-range hour slices — a
    plain-column predicate the parquet reader prunes with — and each
    uncompacted (date, hour) scans just that hour dir's jsonl files with no
    residual filter, so no jsonl file is opened by more than one batch.
    _discover_raw supplies the exactly-once parquet-else-jsonl split, keyed
    on part-0.parquet presence (an empty date dir left by a failed compaction
    must not drop that day's jsonl)."""
    parquet_files, jsonl_files = _discover_raw(data_dir, endpoints, ["polymarket"])

    pq_by_date: dict[str, list[Path]] = {}
    for f in parquet_files:
        pq_by_date.setdefault(_partition_key(f.parent)[2], []).append(f)
    jl_by_hour: dict[tuple[str, str], list[Path]] = {}
    for f in jsonl_files:
        date = f.parent.parent.name.split("=", 1)[1]
        hour = f.parent.name.split("=", 1)[1]
        jl_by_hour.setdefault((date, hour), []).append(f)

    for date, files in sorted(pq_by_date.items()):
        src = _raw_parquet_select(files)
        for h in range(24):
            # fetched_at is ISO-8601 ('2026-07-15T03:…'), so hour slices are
            # lexicographic prefix ranges; 'T24' upper-bounds hour 23.
            lo = _sql_str(f"{date}T{h:02d}")
            hi = _sql_str(f"{date}T{h + 1:02d}")
            yield src, f"fetched_at >= {lo} AND fetched_at < {hi}"
    for (_date, _hour), files in sorted(jl_by_hour.items()):
        yield _raw_jsonl_select(files), "1=1"


def build_pm_trades(
    data_dir: Path, dates: list[str] | None = None
) -> dict[str, Any]:
    """Rebuild the curated pm_trades table from raw Polymarket Data-API captures.

    Scans ALL data_api_trades + data_api_trades_backfill captures in
    file-scoped (date, hour) batches (_pm_trades_batches), dedupes on the
    derived venue_trade_id (md5 hash — no native id), and writes one Parquet
    per trade date at ``curated/pm_trades/date=D/part-0.parquet``. ``dates``
    limits which partitions are (re)written, not what is scanned. Existing
    curated partitions are overwritten — curated is a rebuildable projection
    (data invariants).
    """
    data_dir = Path(data_dir)
    endpoints = ["data_api_trades", "data_api_trades_backfill"]
    conn = duck.connect(data_dir)
    conn.execute("SET TimeZone='UTC'")  # trade_date renders via strftime
    _fat_payload_session(conn)
    try:
        conn.execute(
            "CREATE TEMP TABLE pm_trades_exploded AS "
            + _PM_TRADES_STAGE_SELECT.format(
                source=_empty_raw_select(),
                parser_version=PM_TRADES_PARSER_VERSION,
                batch_where="1=1",
            )
        )
        n_captures = 0
        n_shapeless = 0
        for source, batch_where in _pm_trades_batches(data_dir, endpoints):
            c, s = conn.execute(
                "SELECT count(*), "
                "count(*) FILTER (WHERE json_type(payload) != 'ARRAY' "
                "OR payload IS NULL) "
                f"FROM ({source}) WHERE http_status=200 AND {batch_where}"
            ).fetchone()
            n_captures += c
            n_shapeless += s
            if c - s > 0:
                conn.execute(
                    "INSERT INTO pm_trades_exploded "
                    + _PM_TRADES_STAGE_SELECT.format(
                        source=source,
                        parser_version=PM_TRADES_PARSER_VERSION,
                        batch_where=batch_where,
                    )
                )
        if n_shapeless:
            logger.warning(
                "build_pm_trades: %d/%d captures are not JSON arrays — "
                "possible payload shape drift, investigate raw",
                n_shapeless, n_captures,
            )
        (rows_raw,) = conn.execute("SELECT count(*) FROM pm_trades_exploded").fetchone()
        for bucket in range(_PM_TRADES_DEDUPE_BUCKETS):
            body = _PM_TRADES_DEDUPE_SELECT.format(
                buckets=_PM_TRADES_DEDUPE_BUCKETS, bucket=bucket
            )
            conn.execute(
                ("CREATE TEMP TABLE pm_trades_stage AS " if bucket == 0
                 else "INSERT INTO pm_trades_stage ") + body
            )
        (rows_unique,) = conn.execute("SELECT count(*) FROM pm_trades_stage").fetchone()

        all_dates = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT trade_date FROM pm_trades_stage ORDER BY 1"
            ).fetchall()
        ]
        targets = all_dates if dates is None else [d for d in all_dates if d in set(dates)]

        partitions: dict[str, int] = {}
        for date in targets:
            out_dir = curated_root(data_dir) / "pm_trades" / f"date={date}"
            out_dir.mkdir(parents=True, exist_ok=True)
            for stale in out_dir.glob(".build-*"):
                stale.unlink()
            tmp = out_dir / f".build-{uuid.uuid4().hex[:8]}.parquet"
            conn.execute(
                f"COPY (SELECT {_PM_TRADES_OUT_COLUMNS} FROM pm_trades_stage "
                f"WHERE trade_date = {_sql_str(date)} "
                f"ORDER BY ts, venue_trade_id) "
                f"TO {_sql_str(tmp)} (FORMAT parquet, COMPRESSION zstd)"
            )
            (n,) = conn.execute(
                f"SELECT count(*) FROM read_parquet({_sql_str(tmp)})"
            ).fetchone()
            (expected,) = conn.execute(
                f"SELECT count(*) FROM pm_trades_stage WHERE trade_date = {_sql_str(date)}"
            ).fetchone()
            if n != expected:
                tmp.unlink(missing_ok=True)
                raise RuntimeError(
                    f"build_pm_trades {date}: parquet has {n} rows, stage has {expected}"
                )
            os.replace(tmp, out_dir / PARQUET_PART_NAME)
            partitions[date] = n
    finally:
        conn.close()

    report = {
        "captures": n_captures,
        "captures_non_array": n_shapeless,
        "rows_raw": rows_raw,
        "rows_unique": rows_unique,
        "duplicates_dropped": rows_raw - rows_unique,
        "dedupe_ratio": round(rows_raw / rows_unique, 4) if rows_unique else None,
        "partitions": partitions,
        "parser_version": PM_TRADES_PARSER_VERSION,
        "built_at": int(time.time()),
    }
    logger.info(
        "build_pm_trades: %d captures → %d raw rows → %d unique (%.2f%% dupes), "
        "%d partition(s), parser v%d",
        n_captures, rows_raw, rows_unique,
        100 * (rows_raw - rows_unique) / rows_raw if rows_raw else 0.0,
        len(partitions), PM_TRADES_PARSER_VERSION,
    )
    return report

# --------------------------------------------------------------------------
# Curated projection v5: pm_resolutions (conditionId → winning token/outcome)
# --------------------------------------------------------------------------

PM_RESOLUTIONS_PARSER_VERSION = 1

# Winning-outcome extraction from raw Gamma event captures (the closed=true
# backfill is the resolutions dataset — data invariants). Notes:
# - Gamma double-encodes outcomePrices/clobTokenIds/outcomes as JSON STRINGS
#   inside the market object (data invariants): extract_string then re-CAST.
# - Latest capture per conditionId wins (UMA disputes can flip a proposal —
#   never treat the first snapshot as final).
# - Decided = outcomePrices EXACTLY 1 and 0 (DECIMAL compare, no float).
#   Gamma sets exact 1/0 only at UMA finality; a looser >=0.99 gate would
#   freeze a pre-resolution favorite as the winner for closed-but-unresolved
#   markets (review 2026-07-15 — `closed` means trading ended, NOT resolved,
#   and the closed=true crawl is single-pass so no later capture corrects
#   it). Voided (0.5/0.5), disputed, near-certain-but-open, and non-binary
#   shapes are EXCLUDED, not guessed.
# Extraction happens in PYTHON, not DuckDB: gamma keyset pages run multi-MB
# (100 events × embedded markets), and DuckDB JSON-parses a whole 2048-row
# vector of payloads at once — the parse spike OOMs regardless of batching
# (8GB, measured 2026-07-15). Streaming one payload at a time bounds memory
# by a single page; the slim staged rows are then deduped/decided in SQL.
_PM_RESOLUTIONS_SQL = """
CREATE TEMP TABLE pm_resolutions_stage AS
WITH latest AS (
    -- JSONL staging, not CSV: a Gamma outcome label broke the CSV
    -- round-trip (9 columns on one row, 2026-07-17 refresh failures) —
    -- JSON has no quoting ambiguity.
    SELECT * FROM read_json({stage}, format='newline_delimited', columns={{
        'venue_market_id': 'VARCHAR',
        'p0': 'DECIMAL(9,6)', 'p1': 'DECIMAL(9,6)',
        'tok0': 'VARCHAR', 'tok1': 'VARCHAR',
        'out0': 'VARCHAR', 'out1': 'VARCHAR',
        '_fetched_at': 'VARCHAR'
    }})
    QUALIFY row_number() OVER (
        PARTITION BY venue_market_id ORDER BY _fetched_at DESC
    ) = 1
)
SELECT
    'polymarket'                                       AS venue_id,
    venue_market_id,
    CASE WHEN p0 = 1 THEN 0 ELSE 1 END                 AS winning_outcome_index,
    CASE WHEN p0 = 1 THEN tok0 ELSE tok1 END           AS winning_token_id,
    CASE WHEN p0 = 1 THEN out0 ELSE out1 END           AS winning_outcome_label,
    _fetched_at                                        AS source_fetched_at,
    {parser_version}                                   AS parser_version
FROM latest
WHERE (p0 = 1 AND p1 = 0) OR (p1 = 1 AND p0 = 0)
"""

_RES_QUANT = Decimal("0.000001")


def _iter_payload_markets(payload: Any) -> Iterator[Any]:
    """Market dicts from either raw shape: gamma_events pages
    ({'events': [{'markets': [...]}]}) or gamma_markets_closed pages
    (a flat LIST of market dicts from /markets?closed=true)."""
    if isinstance(payload, list):
        yield from payload
        return
    if not isinstance(payload, dict):
        return
    for ev in payload.get("events") or []:
        if isinstance(ev, dict):
            yield from ev.get("markets") or []


def _extract_resolution_rows(
    payload: Any, fetched_at: str | None
) -> Iterator[dict[str, Any]]:
    """Slim resolution candidates from one gamma capture payload
    (gamma_events or gamma_markets_closed — see _iter_payload_markets).

    Gamma double-encodes outcomePrices/clobTokenIds/outcomes as JSON strings
    (data invariants); prices parse via Decimal (data invariants). Non-binary,
    open, and malformed markets are skipped, not guessed."""
    for mkt in _iter_payload_markets(payload):
        if not isinstance(mkt, dict) or not mkt.get("closed"):
            continue
        cid = mkt.get("conditionId")
        if not cid:
            continue
        try:
            prices = json.loads(mkt.get("outcomePrices") or "null")
            tokens = json.loads(mkt.get("clobTokenIds") or "null")
            outcomes = json.loads(mkt.get("outcomes") or "null")
        except (TypeError, ValueError):
            continue
        if not (
            isinstance(prices, list) and isinstance(tokens, list)
            and len(prices) == 2 and len(tokens) == 2
        ):
            continue
        try:
            p0 = Decimal(str(prices[0])).quantize(_RES_QUANT)
            p1 = Decimal(str(prices[1])).quantize(_RES_QUANT)
        except (InvalidOperation, TypeError, ValueError):
            continue
        if not isinstance(outcomes, list):
            outcomes = []
        yield {
            "venue_market_id": str(cid),
            "p0": p0, "p1": p1,
            "tok0": str(tokens[0]), "tok1": str(tokens[1]),
            "out0": None if len(outcomes) < 1 or outcomes[0] is None
                    else str(outcomes[0]),
            "out1": None if len(outcomes) < 2 or outcomes[1] is None
                    else str(outcomes[1]),
            "_fetched_at": fetched_at,
        }


def _stage_gamma_resolutions(data_dir: Path, stage: Path) -> int:
    """Stream every gamma_events capture through Python into a slim staging
    JSONL file (one payload in memory at a time; no pyarrow dependency). Exactly-once
    across storage tiers: compacted dates read from parquet, uncompacted from
    jsonl (jsonl is retained after compaction — reading both would
    double-count). Parquet payloads are fetched via a single-threaded,
    memory-capped DuckDB cursor in small batches — reading VARCHARs streams
    row-group-at-a-time; it is the vectorized JSON parse that blows up, and
    that happens here in Python instead."""
    from .rawlake import read_raw_file

    # Both gamma capture shapes carry resolutions: events pages (metadata
    # sweeps + the closed=true backfill) and the recently-closed /markets
    # recrawl that plugs the active-only-sweep capture gap (2026-07-19).
    endpoints = ("gamma_events", "gamma_markets_closed")

    n = 0
    with open(stage, "w", encoding="utf-8") as fh:

        def emit(payload: Any, fetched_at: str | None) -> None:
            nonlocal n
            for row in _extract_resolution_rows(payload, fetched_at):
                fh.write(json.dumps({
                    "venue_market_id": row["venue_market_id"],
                    "p0": format(row["p0"], "f"),
                    "p1": format(row["p1"], "f"),
                    "tok0": row["tok0"], "tok1": row["tok1"],
                    "out0": row["out0"], "out1": row["out1"],
                    "_fetched_at": row["_fetched_at"],
                }, separators=(",", ":")) + "\n")
                n += 1

        for endpoint in endpoints:
            pq_root = (
                data_dir / "raw-parquet" / "venue=polymarket" / f"endpoint={endpoint}"
            )
            jsonl_root = (
                data_dir / "raw" / "venue=polymarket" / f"endpoint={endpoint}"
            )
            # Key the compacted set on the finalized part FILE, not the date
            # dir: compact mkdirs the dir before COPY/verify, and a failed
            # compaction leaves it empty indefinitely (41 cycles in the
            # 2026-07-12→15 incident). Dir-keyed skipping would silently drop
            # that whole day's jsonl (review 2026-07-16; mirrors _discover_raw).
            compacted_dates = {
                p.parent.name.split("=", 1)[1]
                for p in pq_root.glob(f"date=*/{PARQUET_PART_NAME}")
            }

            for f in sorted(jsonl_root.glob("date=*/hour=*/*.jsonl.zst")):
                date = f.parent.parent.name.split("=", 1)[1]
                if date in compacted_dates:
                    continue
                for rec in read_raw_file(f):
                    if rec.get("http_status") != 200:
                        continue
                    emit(rec.get("payload"), rec.get("fetched_at"))

            for f in sorted(pq_root.glob(f"date=*/{PARQUET_PART_NAME}")):
                reader = duckdb.connect()
                try:
                    reader.execute("SET memory_limit = '2GB'")
                    reader.execute("SET threads = 1")
                    cur = reader.execute(
                        "SELECT payload, fetched_at, http_status FROM read_parquet(?)",
                        [str(f)],
                    )
                    while batch := cur.fetchmany(32):
                        for payload, fetched, status in batch:
                            if status != 200 or not payload:
                                continue
                            try:
                                obj = json.loads(payload)
                            except ValueError:
                                continue
                            emit(obj, fetched)
                finally:
                    reader.close()
    return n

_PM_RESOLUTIONS_OUT_COLUMNS = (
    "venue_id, venue_market_id, winning_outcome_index, winning_token_id, "
    "winning_outcome_label, source_fetched_at, parser_version"
)


def build_pm_resolutions(data_dir: Path) -> dict[str, Any]:
    """Rebuild the curated pm_resolutions table from raw Gamma captures.

    One row per DECIDED binary market: conditionId → winning clobTokenId /
    outcome index / label, from the latest capture of each settled market.
    Writes a single Parquet at ``curated/pm_resolutions/part-0.parquet``
    (registered as the ``pm_resolutions`` view). Rebuildable projection
    (data invariants); the skills v3 merged derivation joins pm_trades on
    winning_token_id."""
    data_dir = Path(data_dir)
    out_dir = curated_root(data_dir) / "pm_resolutions"
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob(".build-*"):
        stale.unlink()
    stage = out_dir / f".build-stage-{uuid.uuid4().hex[:8]}.jsonl"
    n_candidates = _stage_gamma_resolutions(data_dir, stage)
    logger.info(
        "build_pm_resolutions: staged %d closed-market candidates", n_candidates
    )

    # The decide/dedup pass reads only the slim stage parquet — a bare
    # guardrailed session, no raw views needed.
    conn = duck.connect(data_dir)
    try:
        conn.execute(
            _PM_RESOLUTIONS_SQL.format(
                stage=_sql_str(stage),
                parser_version=PM_RESOLUTIONS_PARSER_VERSION,
            )
        )
        (n_resolved,) = conn.execute(
            "SELECT count(*) FROM pm_resolutions_stage"
        ).fetchone()
        (n_dupe_check,) = conn.execute(
            "SELECT count(*) - count(DISTINCT venue_market_id) "
            "FROM pm_resolutions_stage"
        ).fetchone()
        if n_dupe_check:
            raise RuntimeError(
                f"build_pm_resolutions: {n_dupe_check} duplicate conditionIds "
                "in stage — latest-capture dedup is broken"
            )

        tmp = out_dir / f".build-{uuid.uuid4().hex[:8]}.parquet"
        conn.execute(
            f"COPY (SELECT {_PM_RESOLUTIONS_OUT_COLUMNS} FROM pm_resolutions_stage "
            f"ORDER BY venue_market_id) "
            f"TO {_sql_str(tmp)} (FORMAT parquet, COMPRESSION zstd)"
        )
        (n_written,) = conn.execute(
            f"SELECT count(*) FROM read_parquet({_sql_str(tmp)})"
        ).fetchone()
        if n_written != n_resolved:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(
                f"build_pm_resolutions: parquet has {n_written} rows, "
                f"stage has {n_resolved}"
            )
        os.replace(tmp, out_dir / PARQUET_PART_NAME)
    finally:
        conn.close()
        stage.unlink(missing_ok=True)

    report = {
        "candidate_rows": n_candidates,
        "resolved_markets": n_resolved,
        "parser_version": PM_RESOLUTIONS_PARSER_VERSION,
        "built_at": int(time.time()),
    }
    logger.info(
        "build_pm_resolutions: %d candidates → %d decided markets, parser v%d",
        n_candidates, n_resolved, PM_RESOLUTIONS_PARSER_VERSION,
    )
    return report


# --------------------------------------------------------------------------
# Curated projection v6: venue_crosswalk (cross-venue matched contract pairs)
# --------------------------------------------------------------------------
# The live cross-venue matcher pairs
# a Kalshi binary market with the Polymarket token for the SAME outcome so the
# dual-venue shadow ledger can log a decision on both venues for one event.
#
# Runs as a SUBPROCESS (`pmkt lake build-crosswalk`, 6h cadence — mirrors
# build_pm_resolutions): reads the hot store READ-ONLY (never a second writer;
# data invariants single-writer rule) and pm_resolutions PARQUET-ONLY via DuckDB (NEVER
# ATTACH hot.sqlite through DuckDB, to avoid mixed SQLite implementations
# opening the same file in-process). Structural matching lives in crosswalk.py (I/O-free,
# unit-testable). Human confirmations + machine QA quarantines persist in the
# editable data/crosswalk_confirmed.csv, which this builder merges; decide reads
# the confirmed rows PARQUET-ONLY.

VENUE_CROSSWALK_VERSION = 1
CROSSWALK_WINDOW_DAYS = 7   # forward: open games to match & trade ahead
CROSSWALK_QA_DAYS = 3       # backward: recently-settled games for resolution QA

_CROSSWALK_SEED_CSV = (
    Path(__file__).resolve().parents[2] / "backtests" / "venue_crosswalk_sports.csv"
)

_CROSSWALK_OVERRIDE_COLUMNS = (
    "kalshi_ticker", "pm_token_id", "confirmed", "source", "note", "updated_ts",
)

_CROSSWALK_JSON_COLUMNS = (
    "{'event_key':'VARCHAR','sport':'VARCHAR','game_date':'VARCHAR',"
    "'kalshi_ticker':'VARCHAR','kalshi_side':'VARCHAR','kalshi_title':'VARCHAR',"
    "'kalshi_status':'VARCHAR','kalshi_result':'VARCHAR',"
    "'pm_condition_id':'VARCHAR','pm_slug':'VARCHAR','pm_token_id':'VARCHAR',"
    "'pm_outcome_index':'INTEGER','pm_outcome_name':'VARCHAR',"
    "'pm_primary_token_id':'VARCHAR','pm_status':'VARCHAR',"
    "'pm_winning_index':'INTEGER','resolution_agree':'VARCHAR',"
    "'match_method':'VARCHAR','notes':'VARCHAR',"
    "'confirmed':'INTEGER','auto_confirmed':'INTEGER','updated_ts':'BIGINT'}"
)

_CROSSWALK_OUT_COLUMNS = (
    "event_key, sport, game_date, kalshi_ticker, kalshi_side, kalshi_title, "
    "kalshi_status, kalshi_result, pm_condition_id, pm_slug, pm_token_id, "
    "pm_outcome_index, pm_outcome_name, pm_primary_token_id, pm_status, "
    "pm_winning_index, resolution_agree, match_method, notes, confirmed, "
    "auto_confirmed, updated_ts"
)

# The output columns, in order, for the JSONL stage dump before COPY.
_CROSSWALK_ROW_KEYS = (
    "event_key", "sport", "game_date", "kalshi_ticker", "kalshi_side",
    "kalshi_title", "kalshi_status", "kalshi_result", "pm_condition_id",
    "pm_slug", "pm_token_id", "pm_outcome_index", "pm_outcome_name",
    "pm_primary_token_id", "pm_status", "pm_winning_index", "resolution_agree",
    "match_method", "notes", "confirmed", "auto_confirmed", "updated_ts",
)


def crosswalk_overrides_path(data_dir: Path) -> Path:
    return Path(data_dir) / "crosswalk_confirmed.csv"


def read_venue_crosswalk(
    data_dir: Path, *, confirmed: int | None = None, status: str | None = None
) -> list[dict[str, Any]]:
    """Read crosswalk rows from the parquet (no view/hot attach needed).
    ``confirmed`` / ``status`` filter to confirmed rows or a Kalshi status."""
    path = curated_root(data_dir) / "venue_crosswalk" / PARQUET_PART_NAME
    if not path.exists():
        return []
    conds = []
    params: list[Any] = []
    if confirmed is not None:
        conds.append("confirmed = ?")
        params.append(int(confirmed))
    if status is not None:
        conds.append("kalshi_status = ?")
        params.append(status)
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    conn = duck.connect(data_dir)
    try:
        cur = conn.execute(
            f"SELECT {_CROSSWALK_OUT_COLUMNS} FROM read_parquet({_sql_str(path)})"
            f"{where} ORDER BY game_date, event_key, kalshi_ticker",
            params,
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _hot_ro_conn(data_dir: Path):
    """Read-only sqlite3 connection to the hot store (data invariants single-writer:
    a subprocess reader NEVER writes hot.sqlite). ``mode=ro`` cannot write or
    run recovery; query_only is a belt-and-suspenders guard."""
    import sqlite3

    path = hot_db_path(data_dir)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _read_hot_crosswalk_inputs(
    data_dir: Path, min_ts: int, max_ts: int
) -> tuple[list[dict], list[dict]]:
    """Kalshi sports markets + Polymarket sports instruments in the close-ts
    window, read read-only from the hot store."""
    from .crosswalk_aliases import SPORTS_SERIES

    conn = _hot_ro_conn(data_dir)
    try:
        placeholders = ",".join("?" for _ in SPORTS_SERIES)
        kalshi_rows = [
            {
                "series": r["series"], "event": r["event"], "ticker": r["ticker"],
                "title": r["title"], "status_norm": r["status_norm"],
                "result": r["result"], "close_ts": r["close_ts"],
            }
            for r in conn.execute(
                f"""SELECT venue_series_id series, venue_event_id event,
                           venue_market_id ticker, title, status_norm, result,
                           close_ts
                    FROM markets
                    WHERE venue_id='kalshi' AND venue_series_id IN ({placeholders})
                      AND status_norm IN ('open','settled','closed')
                      AND (close_ts IS NULL OR close_ts BETWEEN ? AND ?)""",
                (*SPORTS_SERIES, min_ts, max_ts),
            )
        ]
        # PM sports markets: slug prefixes the matcher's regexes recognize; the
        # regex date suffix filters out season-long props (mlb-scorigami-…).
        pm_inst_rows = [
            {
                "cid": r["cid"], "slug": r["slug"], "title": r["title"],
                "status_norm": r["status_norm"], "tok": r["tok"],
                "oi": r["oi"], "oname": r["oname"], "is_primary": r["is_primary"],
            }
            for r in conn.execute(
                """SELECT m.venue_market_id cid, m.slug, m.title, m.status_norm,
                          i.venue_token_id tok, i.outcome_index oi,
                          i.outcome_name oname, i.is_primary
                   FROM markets m
                   JOIN instruments i
                     ON i.venue_id = m.venue_id
                    AND i.venue_market_id = m.venue_market_id
                   WHERE m.venue_id='polymarket'
                     AND (m.slug GLOB 'mlb-*' OR m.slug GLOB 'wnba-*'
                          OR m.slug GLOB 'fifwc-*' OR m.slug GLOB 'atp-*'
                          OR m.slug GLOB 'wta-*')
                     AND (m.close_ts IS NULL OR m.close_ts BETWEEN ? AND ?)""",
                (min_ts, max_ts),
            )
        ]
    finally:
        conn.close()
    return kalshi_rows, pm_inst_rows


def _load_pm_resolutions_for(
    data_dir: Path, cids: list[str]
) -> dict[str, tuple[int | None, str | None]]:
    """conditionId → (winning_outcome_index, label) for the matched PM markets.
    Parquet-only DuckDB read (IN-list), never a hot ATTACH."""
    res_file = curated_root(data_dir) / "pm_resolutions" / PARQUET_PART_NAME
    if not res_file.exists() or not cids:
        return {}
    conn = duck.connect(data_dir)
    try:
        placeholders = ",".join("?" for _ in cids)
        rows = conn.execute(
            f"""SELECT venue_market_id, winning_outcome_index, winning_outcome_label
                FROM read_parquet({_sql_str(res_file)})
                WHERE venue_market_id IN ({placeholders})""",
            cids,
        ).fetchall()
    finally:
        conn.close()
    return {cid: (idx, label) for cid, idx, label in rows}


def crosswalk_seed_overrides(data_dir: Path) -> int:
    """Seed data/crosswalk_confirmed.csv from the vetted (hand-reviewed)
    backtests CSV — its 337/337 settled cross-venue agreement is the evidence
    that structural precision is ~100%, which is what justifies cautious
    auto-confirm. Idempotent: only writes if the override file is missing."""
    import csv

    path = crosswalk_overrides_path(data_dir)
    if path.exists():
        return 0
    now = int(time.time())
    seeded = []
    if _CROSSWALK_SEED_CSV.exists():
        with open(_CROSSWALK_SEED_CSV, newline="") as fh:
            for r in csv.DictReader(fh):
                if r.get("confirmed") == "1" and r.get("pm_token_id"):
                    seeded.append({
                        "kalshi_ticker": r["kalshi_ticker"],
                        "pm_token_id": r["pm_token_id"],
                        "confirmed": "1", "source": "seed",
                        "note": r.get("confirm_basis") or "",
                        "updated_ts": now,
                    })
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(_CROSSWALK_OVERRIDE_COLUMNS))
        w.writeheader()
        w.writerows(seeded)
    logger.info("crosswalk: seeded %d confirmed overrides -> %s", len(seeded), path)
    return len(seeded)


def crosswalk_overrides_load(data_dir: Path) -> dict[tuple[str, str], int]:
    """Merged human confirmations + QA quarantines: (kalshi_ticker,
    pm_token_id) → confirmed (0/1), latest updated_ts wins per key."""
    import csv

    path = crosswalk_overrides_path(data_dir)
    if not path.exists():
        return {}
    latest: dict[tuple[str, str], tuple[int, int]] = {}
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            key = (r.get("kalshi_ticker") or "", r.get("pm_token_id") or "")
            if not key[0] or not key[1]:
                continue
            try:
                ts = int(r.get("updated_ts") or 0)
                confirmed = int(r.get("confirmed") or 0)
            except ValueError:
                continue
            if key not in latest or ts >= latest[key][1]:
                latest[key] = (confirmed, ts)
    return {k: v[0] for k, v in latest.items()}


def _append_qa_quarantines(data_dir: Path, qa_rows: list[dict], now: int) -> None:
    """Append QA-disagreement quarantines to the overrides file (confirmed=0,
    source='qa'). A confirmed match whose settled resolutions disagree across
    venues is a matcher bug — kill the pairing and alert."""
    import csv

    if not qa_rows:
        return
    path = crosswalk_overrides_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    new = path.exists() is False
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(_CROSSWALK_OVERRIDE_COLUMNS))
        if new:
            w.writeheader()
        for r in qa_rows:
            w.writerow({
                "kalshi_ticker": r["kalshi_ticker"],
                "pm_token_id": r["pm_token_id"], "confirmed": "0", "source": "qa",
                "note": (
                    f"QA quarantine {r['game_date']} {r['event_key']}: kalshi "
                    f"result={r['kalshi_result']} vs pm winning_index="
                    f"{r['pm_winning_index']} DISAGREE"
                ),
                "updated_ts": now,
            })
    logger.error(
        "crosswalk QA: quarantined %d confirmed match(es) whose settled "
        "resolutions DISAGREE across venues (matcher bug) — %s",
        len(qa_rows),
        ", ".join(sorted({r["event_key"] for r in qa_rows})),
    )


def build_venue_crosswalk(
    data_dir: Path,
    window_days: int = CROSSWALK_WINDOW_DAYS,
    qa_days: int = CROSSWALK_QA_DAYS,
) -> dict[str, Any]:
    """Rebuild curated/venue_crosswalk/part-0.parquet: cross-venue matched
    contract pairs for events in [now−qa_days, now+window_days].

    Rebuildable projection (data invariants). Confirmed rows (usable by decide)
    vs candidate rows (manual review). Post-settlement resolution-agreement QA
    quarantines confirmed matches that disagree across venues. Registered as
    the ``venue_crosswalk`` view in connect()."""
    from .crosswalk import apply_confirmation, match_crosswalk, qa_disagreements

    data_dir = Path(data_dir)
    now = int(time.time())
    min_ts = now - (qa_days + 2) * 86400
    max_ts = now + (window_days + 2) * 86400
    min_date = time.strftime("%Y-%m-%d", time.gmtime(now - qa_days * 86400))
    max_date = time.strftime("%Y-%m-%d", time.gmtime(now + window_days * 86400))

    crosswalk_seed_overrides(data_dir)  # idempotent: first run only

    kalshi_rows, pm_inst_rows = _read_hot_crosswalk_inputs(data_dir, min_ts, max_ts)
    cids = sorted({r["cid"] for r in pm_inst_rows})
    pm_res = _load_pm_resolutions_for(data_dir, cids)

    rows = match_crosswalk(
        kalshi_rows, pm_inst_rows, pm_res, min_date=min_date, max_date=max_date
    )
    overrides = crosswalk_overrides_load(data_dir)
    apply_confirmation(rows, overrides)

    # Post-settlement QA: quarantine confirmed matches that disagree, then
    # re-apply so the written parquet reflects the quarantine immediately.
    qa = qa_disagreements(rows)
    if qa:
        _append_qa_quarantines(data_dir, qa, now)
        apply_confirmation(rows, crosswalk_overrides_load(data_dir))

    for r in rows:
        r["updated_ts"] = now

    out_dir = curated_root(data_dir) / "venue_crosswalk"
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob(".build-*"):
        stale.unlink()
    stage = out_dir / f".build-stage-{uuid.uuid4().hex[:8]}.jsonl"
    with open(stage, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(
                {c: r.get(c) for c in _CROSSWALK_ROW_KEYS},
                separators=(",", ":"),
            ) + "\n")

    conn = duck.connect(data_dir)
    try:
        tmp = out_dir / f".build-{uuid.uuid4().hex[:8]}.parquet"
        conn.execute(
            f"COPY (SELECT {_CROSSWALK_OUT_COLUMNS} FROM read_json({_sql_str(stage)}, "
            f"format='newline_delimited', columns={_CROSSWALK_JSON_COLUMNS}) "
            f"ORDER BY game_date, event_key, kalshi_ticker) "
            f"TO {_sql_str(tmp)} (FORMAT parquet, COMPRESSION zstd)"
        )
        (n_written,) = conn.execute(
            f"SELECT count(*) FROM read_parquet({_sql_str(tmp)})"
        ).fetchone()
        if n_written != len(rows):
            tmp.unlink(missing_ok=True)
            raise RuntimeError(
                f"build_venue_crosswalk: parquet has {n_written} rows, "
                f"staged {len(rows)}"
            )
        os.replace(tmp, out_dir / PARQUET_PART_NAME)
    finally:
        conn.close()
        stage.unlink(missing_ok=True)

    # Coverage: open Kalshi sports events (closing within the forward window)
    # with no confirmed match — logged so coverage gaps stay visible.
    matched_events = {r["event_key"] for r in rows}
    confirmed_events = {r["event_key"] for r in rows if r["confirmed"] == 1}
    open_events = {
        r["event"] for r in kalshi_rows
        if r["status_norm"] == "open" and r["close_ts"]
        and now <= r["close_ts"] <= max_ts
    }
    unmatched = sorted(open_events - matched_events)
    n_confirmed = sum(1 for r in rows if r["confirmed"] == 1)
    report = {
        "version": VENUE_CROSSWALK_VERSION,
        "window": [min_date, max_date],
        "contract_rows": len(rows),
        "confirmed_contracts": n_confirmed,
        "candidate_contracts": len(rows) - n_confirmed,
        "matched_events": len(matched_events),
        "confirmed_events": len(confirmed_events),
        "qa_quarantined": len(qa),
        "kalshi_open_events_in_window": len(open_events),
        "unmatched_open_events": len(unmatched),
        "built_at": now,
    }
    logger.info(
        "build_venue_crosswalk: %d contracts (%d confirmed) over %d events; "
        "%d/%d open Kalshi events unmatched%s",
        len(rows), n_confirmed, len(matched_events),
        len(unmatched), len(open_events),
        (" [sample: " + ", ".join(unmatched[:8]) + "]") if unmatched else "",
    )
    return report
