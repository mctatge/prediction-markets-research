"""Skilled OFI scorer: the 24h window is TRADE time, not capture time —
bridge-backfill captures (old tape under a fresh fetched_at) must not
enter the live signal, and all-old pages are pruned before unnesting.

Also pins the incremental rolling window (curated/ofi_window/): passes
append only unconsumed raw files, re-captured trades dedupe on the md5
venue_trade_id, aged trades prune out, and a missing/forced state falls
back to a full rebuild from raw."""

import json
from datetime import datetime, timezone

import duckdb

from pmkt.model.ofi import compute_ofi
from pmkt.rawlake import RawLake
from pmkt.state import HotStore

NOW = int(datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc).timestamp())
FRESH = NOW - 600            # inside the 24h window
STALE = NOW - 40 * 86400     # bridge-era trade, far outside

MKT_LIVE = "0xlive"
MKT_BRIDGE = "0xbridge"
TOK = "111"
TOK_BRIDGE = "222"
SKILLED = "0xwallet-skilled"


def _trade(cid, token, ts, *, side="BUY", wallet=SKILLED, tx="0xt1"):
    return {
        "conditionId": cid, "asset": token, "timestamp": ts,
        "size": "10.0", "price": "0.50", "side": side,
        "proxyWallet": wallet, "transactionHash": tx,
    }


def _setup(tmp_path):
    data = tmp_path
    # hot store: primary instruments for both markets
    store = HotStore(data / "db" / "hot.sqlite")
    store.conn.execute(
        """INSERT INTO instruments (venue_id, venue_market_id, outcome_index,
           venue_token_id, is_primary, updated_at)
           VALUES ('polymarket',?,0,?,1,0), ('polymarket',?,0,?,1,0)""",
        (MKT_LIVE, TOK, MKT_BRIDGE, TOK_BRIDGE),
    )
    store.conn.commit()
    store.conn.close()
    # wallet_skills curated parquet: one skilled wallet
    ws_dir = data / "curated" / "wallet_skills"
    ws_dir.mkdir(parents=True)
    duckdb.execute(
        f"""COPY (SELECT '{SKILLED}' AS wallet, CAST(5.0 AS DOUBLE) AS z_score)
            TO '{ws_dir / "part-0.parquet"}' (FORMAT parquet)"""
    )
    return data


def _capture(lake, trades, fetched_ts):
    lake.append(
        venue="polymarket", endpoint="data_api_trades",
        params={"market": trades[0]["conditionId"] if trades else "?"},
        http_status=200, elapsed_ms=10, payload=trades,
        fetched_at=datetime.fromtimestamp(fetched_ts, tz=timezone.utc),
    )


def test_bridge_tape_excluded_from_live_window(tmp_path):
    data = _setup(tmp_path)
    with RawLake(data / "raw") as lake:
        # live market: 3 fresh skilled BUYs (meets MIN_SKILLED_TRADES)
        _capture(lake, [
            _trade(MKT_LIVE, TOK, FRESH, tx=f"0xf{i}") for i in range(3)
        ], NOW - 300)
        # bridge capture: months-old tape for another market, fetched NOW —
        # all-old page, must be pruned before unnesting
        _capture(lake, [
            _trade(MKT_BRIDGE, TOK_BRIDGE, STALE, tx=f"0xb{i}") for i in range(5)
        ], NOW - 200)
        # mixed page on the live market (open-market bridge crawl: newest
        # trades are fresh, tail is old) — fresh row kept, old rows dropped
        _capture(lake, [
            _trade(MKT_LIVE, TOK, FRESH, side="SELL", tx="0xm0"),
            _trade(MKT_LIVE, TOK, STALE, tx="0xm1"),
            _trade(MKT_LIVE, TOK, STALE, tx="0xm2"),
        ], NOW - 100)

    rows = compute_ofi(data, now_ts=NOW)

    by_mkt = {r["venue_market_id"]: r for r in rows}
    assert MKT_BRIDGE not in by_mkt, "bridge-era tape must not enter live OFI"
    live = by_mkt[MKT_LIVE]
    # 3 fresh BUYs + 1 fresh SELL; the two stale rows of the mixed page dropped
    assert live["n_total_trades"] == 4
    assert live["n_skilled_trades"] == 4
    # OFI = (3*10 - 1*10) / 40 = 0.5
    assert live["skilled_ofi"] == "0.5000"


# --------------------------------------------------------------------------
# Incremental rolling window
# --------------------------------------------------------------------------

def _raw_files(data):
    root = data / "raw" / "venue=polymarket" / "endpoint=data_api_trades"
    return sorted(root.rglob("part-*.jsonl.zst"))


def _window_paths(data):
    wdir = data / "curated" / "ofi_window"
    return wdir / "part-0.parquet", wdir / "state.json"


def test_incremental_pass_appends_only_new_files(tmp_path):
    data = _setup(tmp_path)
    with RawLake(data / "raw") as lake:
        _capture(lake, [
            _trade(MKT_LIVE, TOK, FRESH, tx=f"0xf{i}") for i in range(3)
        ], NOW - 900)
    (first,) = compute_ofi(data, now_ts=NOW)
    assert first["skilled_ofi"] == "1.0000"

    # Remove the already-consumed raw file: the second pass can only produce
    # all four trades via window + new file. (Raw is never deleted in
    # production — this pins that the pass did NOT rescan old captures.)
    (only,) = _raw_files(data)
    only.unlink()

    with RawLake(data / "raw") as lake:
        _capture(lake, [
            _trade(MKT_LIVE, TOK, FRESH, side="SELL", tx="0xs0")
        ], NOW - 300)
    (live,) = compute_ofi(data, now_ts=NOW)
    assert live["n_total_trades"] == 4
    assert live["skilled_ofi"] == "0.5000"
    _, state_path = _window_paths(data)
    state = json.loads(state_path.read_text())
    # the deleted file left the visible set; only the new file remains consumed
    assert len(state["consumed"]) == 1


def test_recaptured_trades_not_double_counted(tmp_path):
    data = _setup(tmp_path)
    trades = [_trade(MKT_LIVE, TOK, FRESH, tx=f"0xf{i}") for i in range(3)]
    with RawLake(data / "raw") as lake:
        _capture(lake, trades, NOW - 900)
    compute_ofi(data, now_ts=NOW)
    # next sweep re-captures the same first page, plus one genuinely new trade
    with RawLake(data / "raw") as lake:
        _capture(
            lake, trades + [_trade(MKT_LIVE, TOK, FRESH, tx="0xnew")], NOW - 300
        )
    (live,) = compute_ofi(data, now_ts=NOW)
    assert live["n_total_trades"] == 4
    assert live["n_skilled_trades"] == 4


def test_window_prunes_aged_trades(tmp_path):
    data = _setup(tmp_path)
    with RawLake(data / "raw") as lake:
        _capture(lake, [
            _trade(MKT_LIVE, TOK, FRESH, tx=f"0xf{i}") for i in range(3)
        ], NOW - 300)
    assert compute_ofi(data, now_ts=NOW)
    # two days later every window row has aged out of the 24h trade-ts window
    assert compute_ofi(data, now_ts=NOW + 2 * 86400) == []
    part, _ = _window_paths(data)
    (n,) = duckdb.execute(f"SELECT count(*) FROM read_parquet('{part}')").fetchone()
    assert n == 0


def test_missing_state_rebuilds_from_raw(tmp_path):
    data = _setup(tmp_path)
    with RawLake(data / "raw") as lake:
        _capture(lake, [
            _trade(MKT_LIVE, TOK, FRESH, tx=f"0xf{i}") for i in range(3)
        ], NOW - 300)
    first = compute_ofi(data, now_ts=NOW)
    _, state_path = _window_paths(data)
    state_path.unlink()
    assert compute_ofi(data, now_ts=NOW) == first


def test_late_appearing_file_in_old_hour_is_picked_up(tmp_path):
    """A crash-recovered or late-rotated part can appear in an hour partition
    the scorer already consumed past; the consumed-file-set diff must still
    see it (a max-fetched_at timestamp watermark would silently skip it)."""
    data = _setup(tmp_path)
    with RawLake(data / "raw") as lake:
        _capture(lake, [
            _trade(MKT_LIVE, TOK, FRESH, tx=f"0xf{i}") for i in range(3)
        ], NOW - 300)
    compute_ofi(data, now_ts=NOW)
    with RawLake(data / "raw") as lake:  # lands hours before consumed captures
        _capture(lake, [
            _trade(MKT_LIVE, TOK, NOW - 7200, side="SELL", tx="0xlate")
        ], NOW - 7200)
    (live,) = compute_ofi(data, now_ts=NOW)
    assert live["n_total_trades"] == 4
    assert live["skilled_ofi"] == "0.5000"


def test_rebuild_flag_rescans_raw(tmp_path):
    data = _setup(tmp_path)
    with RawLake(data / "raw") as lake:
        _capture(lake, [
            _trade(MKT_LIVE, TOK, FRESH, tx=f"0xf{i}") for i in range(3)
        ], NOW - 300)
    assert compute_ofi(data, now_ts=NOW)
    (only,) = _raw_files(data)
    only.unlink()
    # incremental keeps serving from the window; --rebuild must rescan raw
    assert compute_ofi(data, now_ts=NOW) != []
    assert compute_ofi(data, now_ts=NOW, rebuild=True) == []
