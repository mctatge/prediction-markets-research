"""DuckDB lake surface: exactly-once raw view, hot attach, curated trades."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from pmkt.compact import PARQUET_PART_NAME, compact_day
from pmkt.lake import (
    PARSER_VERSION,
    PM_TRADES_PARSER_VERSION,
    SNAPSHOTS_PARSER_VERSION,
    build_pm_trades,
    build_price_snapshots,
    build_trades,
    connect,
    run_sql,
)
from pmkt.rawlake import RawLake

DAY = "2026-07-10"
DAY2 = "2026-07-11"
TODAY = "2026-07-12"


def _ts(day, hour=12, minute=0, second=0):
    y, m, d = (int(x) for x in day.split("-"))
    return datetime(y, m, d, hour, minute, second, tzinfo=timezone.utc)


def _trade(tid, *, ticker="KXTEST-26JUL10-B50", yes="0.4275", no="0.5725",
           count="3.00", created="2026-07-10T12:00:00.990306Z",
           outcome="no", book="ask", block=False):
    return {
        "trade_id": tid, "ticker": ticker, "count_fp": count,
        "yes_price_dollars": yes, "no_price_dollars": no,
        "taker_outcome_side": outcome, "taker_book_side": book,
        "taker_side": outcome,  # legacy field, ignored by the parser
        "is_block_trade": block, "created_time": created,
    }


def _capture(lake, trades, *, fetched, endpoint="trades", status=200, payload=None):
    lake.append(
        venue="kalshi", endpoint=endpoint, params={"limit": 1000},
        http_status=status, elapsed_ms=10,
        payload=payload if payload is not None else {"cursor": "", "trades": trades},
        fetched_at=fetched,
    )


def _epoch(day, hour=12, minute=0, second=0):
    return int(_ts(day, hour, minute, second).timestamp())


def _candle(period_ts, *, bid="0.0200", ask="0.0300", last="0.0250",
            volume="10.00", oi="100.00"):
    """One Kalshi candlestick. last=None ⇒ a no-trade minute (price dict carries
    only previous_dollars, no close_dollars → last_prob NULL)."""
    price = {"previous_dollars": "0.0250"} if last is None else {"close_dollars": last}
    return {
        "end_period_ts": period_ts,
        "volume_fp": volume, "open_interest_fp": oi,
        "yes_bid": {"close_dollars": bid},
        "yes_ask": {"close_dollars": ask},
        "price": price,
    }


def _candle_capture(lake, markets, *, fetched, endpoint="markets_candlesticks",
                    status=200, payload=None):
    """markets: list of (ticker, [candle, ...])."""
    if payload is None:
        payload = {"markets": [
            {"market_ticker": t, "candlesticks": cs} for t, cs in markets
        ]}
    lake.append(
        venue="kalshi", endpoint=endpoint, params={"period_interval": 1},
        http_status=status, elapsed_ms=10, payload=payload, fetched_at=fetched,
    )


# ---------------------------------------------------------------------------
# raw view: exactly-once across parquet + jsonl
# ---------------------------------------------------------------------------

def test_raw_view_exactly_once(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        for minute in range(3):
            _capture(lake, [_trade(f"a{minute}")], fetched=_ts(DAY, 3, minute))
        for minute in range(2):
            _capture(lake, [_trade(f"b{minute}")], fetched=_ts(DAY2, 4, minute))

    compact_day(raw, tmp_path / "raw-parquet", DAY, today=TODAY)  # DAY only

    conn = connect(tmp_path)
    assert conn.execute("SELECT count(*) FROM raw_parquet").fetchone()[0] == 3
    assert conn.execute("SELECT count(*) FROM raw_jsonl").fetchone()[0] == 2
    assert conn.execute("SELECT count(*) FROM raw").fetchone()[0] == 5
    # No record is served twice: fetched_at is unique per capture here.
    assert conn.execute(
        "SELECT count(DISTINCT fetched_at) FROM raw"
    ).fetchone()[0] == 5
    # Partition-derived columns line up on both sides of the union.
    assert conn.execute(
        'SELECT count(*) FROM raw WHERE "date" = ? AND hour = ?', [DAY, "03"]
    ).fetchone()[0] == 3
    assert conn.execute(
        'SELECT count(*) FROM raw WHERE "date" = ? AND hour = ?', [DAY2, "04"]
    ).fetchone()[0] == 2


def test_raw_view_payload_values_identical_across_sources(tmp_path):
    raw = tmp_path / "raw"
    payload = {"cursor": "", "trades": [_trade("x", yes="0.0001")], "uni": "☃é"}
    with RawLake(raw) as lake:
        _capture(lake, None, fetched=_ts(DAY, 1), payload=payload)
        _capture(lake, None, fetched=_ts(DAY2, 1), payload=payload)
    compact_day(raw, tmp_path / "raw-parquet", DAY, today=TODAY)

    conn = connect(tmp_path)
    texts = [r[0] for r in conn.execute(
        'SELECT payload FROM raw ORDER BY "date"'
    ).fetchall()]
    # Bytes may differ across sources (Python canonical vs yyjson rendering)…
    # …but parsed VALUES must be identical.
    assert json.loads(texts[0]) == payload
    assert json.loads(texts[1]) == payload


def test_raw_view_empty_lake(tmp_path):
    conn = connect(tmp_path)
    assert conn.execute("SELECT count(*) FROM raw").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM raw_parquet").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM raw_jsonl").fetchone()[0] == 0


def test_hot_store_attaches_read_only(tmp_path):
    from pmkt.state import HotStore

    store = HotStore(tmp_path / "db" / "hot.sqlite")
    store.upsert_markets(
        "kalshi",
        [{"venue_market_id": "KXTEST-26JUL10-B50", "status_norm": "open",
          "last_prob": "0.4275"}],
        now_ts=1780000000,
    )
    store.close()

    conn = connect(tmp_path)
    attached = [r[0] for r in conn.execute(
        "SELECT database_name FROM duckdb_databases()"
    ).fetchall()]
    if "hot" not in attached:
        pytest.skip("duckdb sqlite extension unavailable in this environment")
    ticker, prob = conn.execute(
        "SELECT venue_market_id, last_prob FROM hot.markets"
    ).fetchone()
    assert (ticker, prob) == ("KXTEST-26JUL10-B50", "0.4275")


def test_run_sql(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _capture(lake, [_trade("t1")], fetched=_ts(DAY, 2))
    rel = run_sql(tmp_path, "SELECT endpoint, count(*) FROM raw GROUP BY 1")
    assert rel.fetchall() == [("trades", 1)]


# ---------------------------------------------------------------------------
# curated trades projection
# ---------------------------------------------------------------------------

def test_build_trades_dedupe_and_decimal_exactness(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        # Sweeps overlap by design: trade "dup" appears in both captures.
        _capture(lake, [_trade("t-a", yes="0.4275", no="0.5725", count="3.00"),
                        _trade("dup", yes="0.0001", no="0.9999", count="0.01")],
                 fetched=_ts(DAY, 12, 15))
        _capture(lake, [_trade("dup", yes="0.0001", no="0.9999", count="0.01"),
                        _trade("t-b", yes="0.999999", no="0.000001",
                               count="12345678901234.56", block=True)],
                 fetched=_ts(DAY, 12, 30))

    report = build_trades(tmp_path)
    assert report["captures"] == 2
    assert report["rows_raw"] == 4
    assert report["rows_unique"] == 3
    assert report["duplicates_dropped"] == 1
    assert report["parser_version"] == PARSER_VERSION
    assert report["partitions"] == {DAY: 3}

    conn = connect(tmp_path)
    rows = conn.execute(
        "SELECT venue_id, venue_trade_id, venue_market_id, ts, price_prob, "
        'no_price_prob, "size", taker_outcome_side, taker_book_side, is_block, '
        "parser_version FROM trades ORDER BY venue_trade_id"
    ).fetchall()
    assert [r[1] for r in rows] == ["dup", "t-a", "t-b"]

    dup = rows[0]
    assert dup[0] == "kalshi"
    assert dup[3] == int(_ts(DAY, 12, 0).timestamp())  # created_time, floor(s)
    # Decimal exactness: values arrive as decimal.Decimal, never through float.
    assert isinstance(dup[4], Decimal) and dup[4] == Decimal("0.0001")
    assert dup[5] == Decimal("0.9999")
    assert dup[6] == Decimal("0.01")
    assert (dup[7], dup[8], dup[9], dup[10]) == ("no", "ask", False, PARSER_VERSION)

    t_b = rows[2]
    assert t_b[4] == Decimal("0.999999") and t_b[5] == Decimal("0.000001")
    assert t_b[6] == Decimal("12345678901234.56")
    assert t_b[9] is True


def test_build_trades_partitions_by_trade_date_across_midnight(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        # Captured just after midnight UTC on DAY2, containing trades from
        # both sides of midnight; the DAY-23:59 trade also appeared in a
        # DAY-hour-23 capture (overlap) — it must dedupe into ONE row in the
        # DAY partition.
        straddler = _trade("late", created=f"{DAY}T23:59:59.500000Z")
        _capture(lake, [straddler], fetched=_ts(DAY, 23, 59, 59))
        _capture(lake, [straddler, _trade("early", created=f"{DAY2}T00:01:00Z")],
                 fetched=_ts(DAY2, 0, 1, 30))

    report = build_trades(tmp_path)
    assert report["partitions"] == {DAY: 1, DAY2: 1}
    conn = connect(tmp_path)
    assert conn.execute(
        "SELECT count(*) FROM trades WHERE venue_trade_id = 'late'"
    ).fetchone()[0] == 1


def test_build_trades_reads_both_parquet_and_jsonl(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _capture(lake, [_trade("from-compacted")], fetched=_ts(DAY, 8))
        _capture(lake, [_trade("from-jsonl", created=f"{DAY2}T09:00:00Z")],
                 fetched=_ts(DAY2, 9))
    compact_day(raw, tmp_path / "raw-parquet", DAY, today=TODAY)

    report = build_trades(tmp_path)
    assert report["rows_unique"] == 2
    assert report["partitions"] == {DAY: 1, DAY2: 1}


def test_build_trades_dates_filter_limits_writes_not_scan(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _capture(lake, [_trade("d1")], fetched=_ts(DAY, 8))
        _capture(lake, [_trade("d2", created=f"{DAY2}T09:00:00Z")],
                 fetched=_ts(DAY2, 9))

    report = build_trades(tmp_path, dates=[DAY2])
    assert report["rows_unique"] == 2, "scan covers everything"
    assert report["partitions"] == {DAY2: 1}, "writes only the requested date"
    base = tmp_path / "curated" / "trades" / "venue=kalshi"
    assert (base / f"date={DAY2}" / PARQUET_PART_NAME).exists()
    assert not (base / f"date={DAY}").exists()


def test_build_trades_tolerates_empty_error_and_shapeless_captures(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _capture(lake, [], fetched=_ts(DAY, 1))                        # empty page
        _capture(lake, None, fetched=_ts(DAY, 2), status=429,
                 payload={"error": "rate limited"})                    # excluded
        _capture(lake, None, fetched=_ts(DAY, 3),
                 payload={"unexpected": "shape"})                      # shape drift
        _capture(lake, [_trade("ok")], fetched=_ts(DAY, 4))

    report = build_trades(tmp_path)
    assert report["captures"] == 3          # the 429 is excluded
    assert report["captures_without_trades"] == 1
    assert report["rows_unique"] == 1
    assert report["partitions"] == {DAY: 1}


def test_build_trades_rebuild_overwrites_partition(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _capture(lake, [_trade("t1")], fetched=_ts(DAY, 8))
    first = build_trades(tmp_path)

    with RawLake(raw) as lake:
        _capture(lake, [_trade("t2")], fetched=_ts(DAY, 9))
    second = build_trades(tmp_path)  # pure function over raw: rebuild wins
    assert first["partitions"] == {DAY: 1}
    assert second["partitions"] == {DAY: 2}
    conn = connect(tmp_path)
    assert conn.execute("SELECT count(*) FROM trades").fetchone()[0] == 2


# ---------------------------------------------------------------------------
# curated price_snapshots projection
# ---------------------------------------------------------------------------

def test_build_price_snapshots_dedupe_decimal_and_mid(tmp_path):
    raw = tmp_path / "raw"
    ts_a = _epoch(DAY, 12, 5)
    ts_b = _epoch(DAY, 12, 6)
    with RawLake(raw) as lake:
        # The candle sweep re-captures a rolling window: the same (market,
        # minute) candle appears in two overlapping captures → dedupe to one.
        kxa = _candle(ts_a, bid="0.020001", ask="0.030000", last="0.025000",
                      volume="10.00", oi="100.00")
        _candle_capture(lake, [("KXA", [kxa])], fetched=_ts(DAY, 12, 6))
        _candle_capture(
            lake,
            [("KXA", [kxa]),
             # no-trade minute: last=None ⇒ last_prob NULL, mid still computed
             ("KXB", [_candle(ts_b, bid="0.999999", ask="1.000000", last=None,
                              volume="5.00", oi="7.00")])],
            fetched=_ts(DAY, 12, 11),
        )

    report = build_price_snapshots(tmp_path)
    assert report["captures"] == 2
    assert report["rows_raw"] == 3
    assert report["rows_unique"] == 2
    assert report["duplicates_dropped"] == 1
    assert report["parser_version"] == SNAPSHOTS_PARSER_VERSION
    assert report["partitions"] == {f"kalshi/{DAY}": 2}

    conn = connect(tmp_path)
    rows = conn.execute(
        "SELECT venue_id, venue_market_id, venue_token_id, outcome_index, ts, "
        "bid_prob, ask_prob, mid_prob, last_prob, volume, volume_24h, "
        "open_interest, liquidity, source, parser_version "
        "FROM price_snapshots ORDER BY venue_market_id, ts"
    ).fetchall()
    a, b = rows

    assert a[0] == "kalshi" and a[1] == "KXA"
    assert a[2] is None and a[3] == 0          # venue_token_id NULL, outcome 0
    assert a[4] == ts_a
    # Decimal exactness: values arrive as decimal.Decimal, never through float.
    assert isinstance(a[5], Decimal) and a[5] == Decimal("0.020001")
    assert a[6] == Decimal("0.030000")
    # mid = (bid+ask)/2, exact to the half-tick 7th place, Decimal not float.
    assert isinstance(a[7], Decimal) and a[7] == Decimal("0.0250005")
    assert a[8] == Decimal("0.025000")
    assert a[9] == Decimal("10.00") and a[11] == Decimal("100.00")
    assert a[10] is None and a[12] is None     # volume_24h, liquidity NULL for candle
    assert a[13] == "candle" and a[14] == SNAPSHOTS_PARSER_VERSION

    assert b[1] == "KXB"
    assert b[8] is None                        # no-trade minute → last_prob NULL
    assert isinstance(b[7], Decimal) and b[7] == Decimal("0.9999995")  # mid still set


def test_build_price_snapshots_partitions_by_period_date_across_midnight(tmp_path):
    raw = tmp_path / "raw"
    late_ts = _epoch(DAY, 23, 59)   # candle PERIOD falls on DAY
    early_ts = _epoch(DAY2, 0, 1)   # candle PERIOD falls on DAY2
    with RawLake(raw) as lake:
        # Captured just after midnight on DAY2, carrying candles from both sides
        # of midnight; the DAY-23:59 candle also appeared in a DAY capture
        # (overlap) → must dedupe into ONE row in the DAY partition.
        _candle_capture(lake, [("KXA", [_candle(late_ts)])],
                        fetched=_ts(DAY, 23, 59, 59))
        _candle_capture(lake, [("KXA", [_candle(late_ts), _candle(early_ts)])],
                        fetched=_ts(DAY2, 0, 2))

    report = build_price_snapshots(tmp_path)
    assert report["partitions"] == {f"kalshi/{DAY}": 1, f"kalshi/{DAY2}": 1}
    conn = connect(tmp_path)
    assert conn.execute(
        "SELECT count(*) FROM price_snapshots WHERE ts = ?", [late_ts]
    ).fetchone()[0] == 1


def test_build_price_snapshots_reads_parquet_and_jsonl(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _candle_capture(lake, [("KXA", [_candle(_epoch(DAY, 8))])],
                        fetched=_ts(DAY, 8))
        _candle_capture(lake, [("KXB", [_candle(_epoch(DAY2, 9))])],
                        fetched=_ts(DAY2, 9))
    compact_day(raw, tmp_path / "raw-parquet", DAY, today=TODAY)

    report = build_price_snapshots(tmp_path)
    assert report["rows_unique"] == 2
    assert report["partitions"] == {f"kalshi/{DAY}": 1, f"kalshi/{DAY2}": 1}


def test_build_price_snapshots_tolerates_empty_error_and_shapeless(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _candle_capture(lake, [], fetched=_ts(DAY, 1))                 # empty markets
        _candle_capture(lake, [("KXA", [])], fetched=_ts(DAY, 2))      # market, no candles
        _candle_capture(lake, None, fetched=_ts(DAY, 3), status=429,
                        payload={"error": "rate limited"})             # excluded (non-200)
        _candle_capture(lake, None, fetched=_ts(DAY, 4),
                        payload={"unexpected": "shape"})               # no 'markets' key
        _candle_capture(lake, [("KXA", [_candle(_epoch(DAY, 5))])], fetched=_ts(DAY, 6))

    report = build_price_snapshots(tmp_path)
    assert report["captures"] == 4          # the 429 is excluded
    assert report["captures_without_markets"] == 1
    assert report["rows_unique"] == 1
    assert report["partitions"] == {f"kalshi/{DAY}": 1}


def test_build_price_snapshots_drops_malformed_candle_no_bogus_partition(tmp_path):
    raw = tmp_path / "raw"
    good = _candle(_epoch(DAY, 7))
    malformed = _candle(_epoch(DAY, 8))
    del malformed["end_period_ts"]          # no usable timestamp
    with RawLake(raw) as lake:
        _candle_capture(lake, [("KXA", [good, malformed])], fetched=_ts(DAY, 9))

    report = build_price_snapshots(tmp_path)
    # Malformed row is dropped explicitly; only the good candle survives, and no
    # date=None partition is created.
    assert report["rows_unique"] == 1
    assert report["partitions"] == {f"kalshi/{DAY}": 1}
    assert not (tmp_path / "curated" / "price_snapshots" / "venue=kalshi" / "date=None").exists()


# ---------------------------------------------------------------------------
# curated pm_trades projection (Polymarket Data-API wallet-attributed)
# ---------------------------------------------------------------------------

def _pm_trade_obj(*, wallet="0xabc", side="BUY", price="0.50", size="10.0",
                  cid="0xcond1", asset="tok123", ts=None, tx_hash="0xtx1"):
    if ts is None:
        ts = _epoch(DAY, 12)
    return {
        "proxyWallet": wallet, "side": side, "price": price, "size": size,
        "conditionId": cid, "asset": asset, "timestamp": ts,
        "transactionHash": tx_hash,
    }


def _pm_trade_capture(lake, trades, *, fetched, endpoint="data_api_trades",
                       status=200, payload=None):
    lake.append(
        venue="polymarket", endpoint=endpoint,
        params={"market": "0xcond1", "limit": 1000, "offset": 0},
        http_status=status, elapsed_ms=10,
        payload=payload if payload is not None else trades,
        fetched_at=fetched,
    )


def test_build_pm_trades_dedupe_and_decimal(tmp_path):
    raw = tmp_path / "raw"
    t1 = _pm_trade_obj(wallet="0xw1", side="BUY", price="0.4275", size="3.00",
                        tx_hash="0xtx1")
    dup = _pm_trade_obj(wallet="0xw2", side="SELL", price="0.0001", size="0.01",
                         tx_hash="0xtx2")
    t2 = _pm_trade_obj(wallet="0xw3", side="BUY", price="0.999999",
                        size="12345.56", tx_hash="0xtx3")
    with RawLake(raw) as lake:
        _pm_trade_capture(lake, [t1, dup], fetched=_ts(DAY, 12, 15))
        _pm_trade_capture(lake, [dup, t2], fetched=_ts(DAY, 12, 30))

    report = build_pm_trades(tmp_path)
    assert report["captures"] == 2
    assert report["rows_raw"] == 4
    assert report["rows_unique"] == 3
    assert report["duplicates_dropped"] == 1
    assert report["parser_version"] == PM_TRADES_PARSER_VERSION
    assert report["partitions"] == {DAY: 3}

    conn = connect(tmp_path)
    rows = conn.execute(
        "SELECT venue_id, venue_trade_id, venue_market_id, venue_token_id, ts, "
        'price_prob, "size", taker_side, proxy_wallet, tx_hash, parser_version '
        "FROM pm_trades ORDER BY proxy_wallet"
    ).fetchall()
    assert len(rows) == 3

    w1 = rows[0]
    assert w1[0] == "polymarket"
    assert w1[2] == "0xcond1"  # venue_market_id = conditionId
    assert w1[3] == "tok123"   # venue_token_id = asset
    assert isinstance(w1[5], Decimal) and w1[5] == Decimal("0.427500")
    assert w1[7] == "BUY"
    assert w1[8] == "0xw1"
    assert w1[9] == "0xtx1"
    assert w1[10] == PM_TRADES_PARSER_VERSION

    w2 = rows[1]
    assert isinstance(w2[5], Decimal) and w2[5] == Decimal("0.000100")

    w3 = rows[2]
    assert isinstance(w3[5], Decimal) and w3[5] == Decimal("0.999999")
    assert w3[6] == Decimal("12345.560000")


def test_build_pm_trades_partitions_by_trade_date(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _pm_trade_capture(
            lake,
            [_pm_trade_obj(ts=_epoch(DAY, 23, 59), tx_hash="0xlate")],
            fetched=_ts(DAY, 23, 59, 59),
        )
        _pm_trade_capture(
            lake,
            [_pm_trade_obj(ts=_epoch(DAY, 23, 59), tx_hash="0xlate"),
             _pm_trade_obj(ts=_epoch(DAY2, 0, 1), tx_hash="0xearly")],
            fetched=_ts(DAY2, 0, 1, 30),
        )

    report = build_pm_trades(tmp_path)
    assert report["partitions"] == {DAY: 1, DAY2: 1}
    conn = connect(tmp_path)
    assert conn.execute(
        "SELECT count(*) FROM pm_trades WHERE tx_hash = '0xlate'"
    ).fetchone()[0] == 1


def test_build_pm_trades_tolerates_errors_and_non_arrays(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _pm_trade_capture(lake, [], fetched=_ts(DAY, 1))                         # empty array
        _pm_trade_capture(lake, None, fetched=_ts(DAY, 2), status=429,
                          payload={"error": "rate limited"})                     # excluded
        _pm_trade_capture(lake, None, fetched=_ts(DAY, 3),
                          payload={"unexpected": "shape"})                       # not an array
        _pm_trade_capture(lake, [_pm_trade_obj(tx_hash="0xok")], fetched=_ts(DAY, 4))

    report = build_pm_trades(tmp_path)
    assert report["captures"] == 3           # the 429 is excluded
    assert report["captures_non_array"] == 1
    assert report["rows_unique"] == 1
    assert report["partitions"] == {DAY: 1}


def test_build_pm_trades_exactly_once_across_tiers_and_endpoints(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        # DAY: forward + bridge captures, compacted to parquet below (jsonl is
        # retained after compaction — reading both tiers would double-count).
        _pm_trade_capture(lake, [_pm_trade_obj(ts=_epoch(DAY, 10), tx_hash="0xfwd")],
                          fetched=_ts(DAY, 10))
        _pm_trade_capture(lake, [_pm_trade_obj(ts=_epoch(DAY, 11), tx_hash="0xbrdg")],
                          fetched=_ts(DAY, 11), endpoint="data_api_trades_backfill")
        # DAY2: stays uncompacted jsonl.
        _pm_trade_capture(lake, [_pm_trade_obj(ts=_epoch(DAY2, 9), tx_hash="0xjl")],
                          fetched=_ts(DAY2, 9))
    compact_day(raw, tmp_path / "raw-parquet", DAY, today=TODAY)

    report = build_pm_trades(tmp_path)
    assert report["captures"] == 3
    assert report["rows_raw"] == 3      # no parquet/jsonl double-count
    assert report["rows_unique"] == 3   # bridge endpoint ingested too
    assert report["partitions"] == {DAY: 2, DAY2: 1}


def test_build_pm_trades_failed_compaction_dir_does_not_drop_day(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _pm_trade_capture(lake, [_pm_trade_obj(tx_hash="0xa")], fetched=_ts(DAY, 5))
    # A failed compaction mkdirs the date dir but never finalizes
    # part-0.parquet — the day must still be served from jsonl.
    (tmp_path / "raw-parquet" / "venue=polymarket" / "endpoint=data_api_trades"
     / f"date={DAY}").mkdir(parents=True)

    report = build_pm_trades(tmp_path)
    assert report["captures"] == 1
    assert report["rows_unique"] == 1
    assert report["partitions"] == {DAY: 1}


def test_build_pm_trades_dates_filter(tmp_path):
    raw = tmp_path / "raw"
    with RawLake(raw) as lake:
        _pm_trade_capture(lake, [_pm_trade_obj(ts=_epoch(DAY), tx_hash="0xd1")],
                          fetched=_ts(DAY, 8))
        _pm_trade_capture(lake, [_pm_trade_obj(ts=_epoch(DAY2), tx_hash="0xd2")],
                          fetched=_ts(DAY2, 9))

    report = build_pm_trades(tmp_path, dates=[DAY2])
    assert report["rows_unique"] == 2
    assert report["partitions"] == {DAY2: 1}
    base = tmp_path / "curated" / "pm_trades"
    assert (base / f"date={DAY2}" / PARQUET_PART_NAME).exists()
    assert not (base / f"date={DAY}").exists()
