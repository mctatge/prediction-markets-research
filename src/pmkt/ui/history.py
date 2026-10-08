"""Price history for the detail chart — raw-lake extraction via DuckDB.

kalshi: candle closes from ``markets_candlesticks`` captures (trade close,
falling back to the yes_bid/yes_ask close midpoint). polymarket: minute
points from ``clob_prices_history`` captures for the market's primary token.

Prices come out of the payload as the JSON literal text (``->>``) and stay
strings end to end; the only arithmetic (bid/ask midpoint) runs in Decimal
(data invariants).

Scan strategy: the lake's ``raw`` view unions EVERY endpoint's files, so a
query against it would parse today's entire uncompacted JSONL (including the
WebSocket firehose) — instead the needed (venue, endpoint, date) partitions
are discovered directly with the same exactly-once rule lake.py uses
(Parquet iff compacted, else JSONL), and a ``params LIKE ?`` pre-filter skips
payload parsing for captures that can't contain the market. The DuckDB
connection still comes from ``pmkt.lake.connect`` (temp spill dir, UTC).
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .. import lake
from ..compact import PARQUET_PART_NAME

logger = logging.getLogger(__name__)

MAX_POINTS = 2000
CACHE_TTL_S = 120.0
TIMEOUT_S = 8.0

_RAW_JSON_COLUMNS = (
    "{'params': 'JSON', 'fetched_at': 'VARCHAR', "
    "'http_status': 'INTEGER', 'payload': 'JSON'}"
)
_MAX_OBJECT_SIZE = 128 * 1024 * 1024

KALSHI_ENDPOINT = "markets_candlesticks"
POLYMARKET_ENDPOINT = "clob_prices_history"


class TTLCache:
    """Tiny in-process TTL cache; thread-safe (endpoint runs in a pool)."""

    def __init__(self, ttl_s: float = CACHE_TTL_S) -> None:
        self._ttl = ttl_s
        self._lock = threading.Lock()
        self._items: dict[Any, tuple[float, Any]] = {}

    def get(self, key: Any) -> Any | None:
        now = time.monotonic()
        with self._lock:
            entry = self._items.get(key)
            if entry is None:
                return None
            expires, value = entry
            if expires < now:
                del self._items[key]
                return None
            return value

    def set(self, key: Any, value: Any) -> None:
        now = time.monotonic()
        with self._lock:
            # opportunistic prune keeps the dict from growing unbounded
            for stale in [k for k, (exp, _) in self._items.items() if exp < now]:
                del self._items[stale]
            self._items[key] = (now + self._ttl, value)


def _window_dates(since_ts: int, now_ts: int) -> list[str]:
    """UTC capture dates that can contain points in [since_ts, now_ts].
    A capture is partitioned by FETCH date and never contains points newer
    than its fetch time, so dates >= the window start date are sufficient."""
    start = datetime.fromtimestamp(since_ts, tz=timezone.utc).date()
    end = datetime.fromtimestamp(now_ts, tz=timezone.utc).date()
    out = []
    day = start
    while day <= end:
        out.append(day.isoformat())
        day += timedelta(days=1)
    return out


def _partition_files(
    data_dir: Path, venue: str, endpoint: str, dates: list[str]
) -> tuple[list[Path], list[Path]]:
    """(parquet, jsonl) files for the partitions, exactly-once per date:
    the compacted Parquet if present, otherwise that date's JSONL parts."""
    parquet: list[Path] = []
    jsonl: list[Path] = []
    pq_root = lake.raw_parquet_root(data_dir) / f"venue={venue}" / f"endpoint={endpoint}"
    raw_dir = lake.raw_root(data_dir) / f"venue={venue}" / f"endpoint={endpoint}"
    for date in dates:
        part = pq_root / f"date={date}" / PARQUET_PART_NAME
        if part.exists():
            # part-0 gates the partition; include its sibling part-K files.
            parquet.extend(sorted(part.parent.glob("part-*.parquet")))
        else:
            jsonl.extend(sorted((raw_dir / f"date={date}").glob("hour=*/part-*.jsonl.zst")))
    return parquet, jsonl


def _sql_list(paths: list[Path]) -> str:
    return "[" + ", ".join("'" + str(p).replace("'", "''") + "'" for p in paths) + "]"


def _src_sql(parquet: list[Path], jsonl: list[Path]) -> tuple[str, int]:
    """UNION-ALL source CTE body over the two arms; each arm binds one
    ``params LIKE ? ESCAPE '\\'`` pre-filter. Returns (sql, n_patterns)."""
    arms = []
    if parquet:
        arms.append(
            "SELECT fetched_at, payload FROM read_parquet("
            f"{_sql_list(parquet)}) "
            "WHERE http_status = 200 AND params LIKE ? ESCAPE '\\'"
        )
    if jsonl:
        arms.append(
            "SELECT fetched_at, CAST(payload AS VARCHAR) AS payload FROM read_json("
            f"{_sql_list(jsonl)}, columns={_RAW_JSON_COLUMNS}, "
            f"format='newline_delimited', maximum_object_size={_MAX_OBJECT_SIZE}) "
            "WHERE http_status = 200 AND CAST(params AS VARCHAR) LIKE ? ESCAPE '\\'"
        )
    return " UNION ALL ".join(arms), len(arms)


def _like_literal(needle: str) -> str:
    escaped = needle.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


_KALSHI_SQL = """
WITH src AS ({src}),
mkts AS (
    -- unnest in the SELECT list, not via LATERAL: drops the payload right
    -- after array extraction (same OOM guard as lake.build_trades)
    SELECT fetched_at,
           unnest(CAST(json_extract(payload, '$.markets') AS JSON[])) AS mkt
    FROM src
),
candles AS (
    SELECT fetched_at,
           unnest(CAST(json_extract(mkt, '$.candlesticks') AS JSON[])) AS c
    FROM mkts
    WHERE COALESCE(mkt ->> 'market_ticker', mkt ->> 'ticker') = ?
),
pts AS (
    SELECT CAST(c ->> 'end_period_ts' AS BIGINT)  AS t,
           c -> 'price'   ->> 'close_dollars'     AS trade_close,
           c -> 'yes_bid' ->> 'close_dollars'     AS bid_close,
           c -> 'yes_ask' ->> 'close_dollars'     AS ask_close,
           fetched_at
    FROM candles
)
SELECT t, trade_close, bid_close, ask_close
FROM pts
WHERE t >= ? AND t <= ?
QUALIFY row_number() OVER (PARTITION BY t ORDER BY fetched_at DESC) = 1
ORDER BY t
"""

_POLYMARKET_SQL = """
WITH src AS ({src}),
pts AS (
    -- live shape {{"history": {{token: [pts]}}}} with the documented
    -- {{token: {{"history": [pts]}}}} fallback (see polymarket/client.py)
    SELECT fetched_at,
           unnest(CAST(COALESCE(
               json_extract(payload, '$.history."{token}"'),
               json_extract(payload, '$."{token}".history')) AS JSON[])) AS pt
    FROM src
),
vals AS (
    SELECT CAST(pt ->> 't' AS BIGINT) AS t, pt ->> 'p' AS p, fetched_at
    FROM pts
)
SELECT t, p
FROM vals
WHERE t >= ? AND t <= ? AND p IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY t ORDER BY fetched_at DESC) = 1
ORDER BY t
"""


def _in_unit_range(text: str) -> bool:
    try:
        return Decimal(0) <= Decimal(text) <= Decimal(1)
    except InvalidOperation:
        return False


def _kalshi_point(
    trade_close: str | None, bid_close: str | None, ask_close: str | None
) -> str | None:
    """Contract p for one candle: trade close, else bid/ask close midpoint,
    else the single quoted side. Decimal arithmetic only; a value outside
    0–1 is dropped (cents-shape guard, mirrors kalshi/models.py)."""
    for value in (trade_close, bid_close, ask_close):
        if value is not None and not _in_unit_range(value):
            return None
    if trade_close is not None:
        return trade_close
    if bid_close is not None and ask_close is not None:
        bid, ask = Decimal(bid_close), Decimal(ask_close)
        total = bid + ask
        mid = total / 2
        try:
            q = mid.quantize(Decimal(1).scaleb(total.as_tuple().exponent))
            if q == mid:
                mid = q
        except InvalidOperation:
            pass
        return format(mid, "f")
    return bid_close if bid_close is not None else ask_close


def _thin(points: list[dict[str, Any]], max_points: int = MAX_POINTS) -> list[dict[str, Any]]:
    """Evenly-strided subsample keeping the first and last point."""
    n = len(points)
    if n <= max_points:
        return points
    step = (n - 1) / (max_points - 1)
    indexes = sorted({round(i * step) for i in range(max_points)} | {n - 1})
    return [points[i] for i in indexes]


def fetch_history(
    data_dir: Path,
    venue: str,
    market_id: str,
    hours: float,
    *,
    token_id: str | None = None,
    now_ts: int | None = None,
    conn_holder: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Synchronous worker (runs in a thread pool; the caller owns timeout
    handling). Returns contract points [{"t": int, "p": str}, ...] asc.

    ``conn_holder`` receives the live DuckDB connection under "conn" so the
    caller can ``interrupt()`` it from the event loop on timeout.
    """
    now_ts = int(time.time()) if now_ts is None else now_ts
    since_ts = now_ts - int(hours * 3600)
    dates = _window_dates(since_ts, now_ts)

    if venue == "kalshi":
        endpoint, needle = KALSHI_ENDPOINT, market_id
    else:
        if not token_id:
            return []
        endpoint, needle = POLYMARKET_ENDPOINT, token_id

    parquet, jsonl = _partition_files(data_dir, venue, endpoint, dates)
    if not parquet and not jsonl:
        return []
    src, n_patterns = _src_sql(parquet, jsonl)

    conn = lake.connect(data_dir, attach_hot=False)
    if conn_holder is not None:
        conn_holder["conn"] = conn
    try:
        pattern_args = [_like_literal(needle)] * n_patterns
        if venue == "kalshi":
            sql = _KALSHI_SQL.format(src=src)
            rows = conn.execute(
                sql, [*pattern_args, market_id, since_ts, now_ts]
            ).fetchall()
            points = []
            for t, trade_close, bid_close, ask_close in rows:
                p = _kalshi_point(trade_close, bid_close, ask_close)
                if p is not None:
                    points.append({"t": int(t), "p": p})
        else:
            if not token_id.isdigit():  # defense-in-depth: ids are numeric strings
                return []
            sql = _POLYMARKET_SQL.format(src=src, token=token_id)
            rows = conn.execute(sql, [*pattern_args, since_ts, now_ts]).fetchall()
            points = [{"t": int(t), "p": str(p)} for t, p in rows]
        return _thin(points)
    finally:
        if conn_holder is not None:
            conn_holder.pop("conn", None)
        conn.close()
