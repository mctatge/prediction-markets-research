"""pm_resolutions extraction (winning token from raw Gamma captures) and
wallet-skills v3 (merged v1-corpus + v2-era resolved taker rows)."""

import json
from datetime import datetime, timezone

import duckdb
import pytest

from pmkt import lake
from pmkt.lake import build_pm_resolutions, build_wallet_skills, connect
from pmkt.rawlake import RawLake

V1_END = lake.PM_V1_END_TS
T_EARLY = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
T_LATE = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)


def _gamma_market(cid, prices, tokens, outcomes=("Yes", "No"), closed=True):
    # Gamma double-encodes list fields as JSON strings (data invariants)
    return {
        "conditionId": cid,
        "outcomePrices": json.dumps(list(prices)),
        "clobTokenIds": json.dumps(list(tokens)),
        "outcomes": json.dumps(list(outcomes)),
        "closed": closed,
    }


def _capture_events(lake_writer, markets, fetched):
    lake_writer.append(
        venue="polymarket", endpoint="gamma_events", params={"closed": "true"},
        http_status=200, elapsed_ms=10,
        payload={"events": [{"id": "1", "markets": markets}]},
        fetched_at=fetched,
    )


def test_closed_markets_recrawl_payloads_feed_resolutions(tmp_path):
    """gamma_markets_closed captures are flat market ARRAYS (from
    /markets?closed=true), not events pages — the extractor must read both,
    and a later recrawl capture must override an earlier events capture."""
    with RawLake(tmp_path / "raw") as lw:
        _capture_events(lw, [
            _gamma_market("0xgap", ("0.9", "0.1"), ("g1", "g2")),  # pre-final
        ], T_EARLY)
        lw.append(
            venue="polymarket", endpoint="gamma_markets_closed",
            params={"closed": "true"}, http_status=200, elapsed_ms=5,
            payload=[_gamma_market("0xgap", ("1", "0"), ("g1", "g2"))],
            fetched_at=T_LATE,
        )
    r = build_pm_resolutions(tmp_path)
    assert r["resolved_markets"] == 1
    conn = connect(tmp_path, attach_hot=False)
    row = conn.execute(
        "SELECT winning_token_id FROM pm_resolutions WHERE venue_market_id='0xgap'"
    ).fetchone()
    assert row == ("g1",), "recrawl capture decides the market"


def test_build_pm_resolutions_decided_latest_ambiguous(tmp_path):
    with RawLake(tmp_path / "raw") as lw:
        _capture_events(lw, [
            _gamma_market("0xwin0", ("1", "0"), ("t0a", "t0b")),
            _gamma_market("0xflip", ("0", "1"), ("t1a", "t1b")),  # early proposal
            _gamma_market("0xvoid", ("0.5", "0.5"), ("t2a", "t2b")),  # undecided
            _gamma_market("0xopen", ("0.7", "0.3"), ("t3a", "t3b"), closed=False),
            # closed but NOT UMA-final: near-certain prices must not mint a winner
            _gamma_market("0xnear", ("0.995", "0.005"), ("t4a", "t4b")),
        ], T_EARLY)
        # UMA dispute flips 0xflip; the LATEST capture must win
        _capture_events(lw, [
            _gamma_market("0xflip", ("1", "0"), ("t1a", "t1b")),
        ], T_LATE)

    r = build_pm_resolutions(tmp_path)
    assert r["resolved_markets"] == 2

    conn = connect(tmp_path, attach_hot=False)
    rows = {
        row[0]: (row[1], row[2]) for row in conn.execute(
            "SELECT venue_market_id, winning_outcome_index, winning_token_id "
            "FROM pm_resolutions"
        ).fetchall()
    }
    assert rows["0xwin0"] == (0, "t0a")
    assert rows["0xflip"] == (0, "t1a"), "later capture overrides the proposal"
    assert "0xvoid" not in rows and "0xopen" not in rows
    assert "0xnear" not in rows, "closed-but-unresolved must be excluded"


def _write_v1_fixture(data_dir, rows, block_ts=V1_END - 1000):
    """rows: (taker, maker, direction, usdc, price, outcome, winner, cid)"""
    d = data_dir / "external" / "polymarket-v1" / "daily_aligned"
    d.mkdir(parents=True)
    values = ", ".join(
        f"('{t}', '{m}', '{dr}', CAST({u} AS DOUBLE), CAST({p} AS DOUBLE), "
        f"'{o}', '{w}', '{c}', 'Sports', CAST({block_ts} AS BIGINT))"
        for (t, m, dr, u, p, o, w, c) in rows
    )
    duckdb.execute(
        f"COPY (SELECT * FROM (VALUES {values}) AS v(taker, maker, "
        "taker_direction, usdc_amount, price, outcome_label, "
        "winning_outcome_label, condition_id, category, block_timestamp)) "
        f"TO '{d / 'day.parquet'}' (FORMAT parquet)"
    )


def _write_pm_trades_fixture(data_dir, rows):
    """rows: (wallet, side, token, cid, price, size, ts)"""
    d = data_dir / "curated" / "pm_trades" / "date=2026-07-15"
    d.mkdir(parents=True)
    values = ", ".join(
        f"('polymarket', 'id{i}', '{c}', '{tok}', CAST({ts} AS BIGINT), "
        f"CAST({p} AS DECIMAL(9,6)), CAST({s} AS DECIMAL(18,6)), '{side}', "
        f"'{w}', '0xtx{i}', 1)"
        for i, (w, side, tok, c, p, s, ts) in enumerate(rows)
    )
    duckdb.execute(
        f"COPY (SELECT * FROM (VALUES {values}) AS v(venue_id, venue_trade_id, "
        "venue_market_id, venue_token_id, ts, price_prob, \"size\", taker_side, "
        "proxy_wallet, tx_hash, parser_version)) "
        f"TO '{d / 'part-0.parquet'}' (FORMAT parquet)"
    )


def _write_resolutions_fixture(data_dir, rows):
    """rows: (cid, win_index, win_token)"""
    d = data_dir / "curated" / "pm_resolutions"
    d.mkdir(parents=True)
    values = ", ".join(
        f"('polymarket', '{c}', {i}, '{tok}', 'Yes', TIMESTAMP '2026-07-11', 1)"
        for (c, i, tok) in rows
    )
    duckdb.execute(
        f"COPY (SELECT * FROM (VALUES {values}) AS v(venue_id, venue_market_id, "
        "winning_outcome_index, winning_token_id, winning_outcome_label, "
        "source_fetched_at, parser_version)) "
        f"TO '{d / 'part-0.parquet'}' (FORMAT parquet)"
    )


def test_wallet_skills_v3_merges_v2_resolved_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(lake, "_WALLET_SKILLS_MIN_TRADES", 1)
    # v1 era: wallet A takes (BUY) a winning outcome once
    _write_v1_fixture(tmp_path, [
        ("0xaaa", "0xmm", "BUY", 10, 0.5, "Yes", "Yes", "0xc1"),
    ])
    # v2 era: A buys the winning token again; B buys the losing token;
    # one pre-boundary row must be EXCLUDED (covered by the v1 corpus)
    _write_pm_trades_fixture(tmp_path, [
        ("0xAAA", "BUY", "tokWIN", "0xc2", 0.5, 20, V1_END + 100),
        ("0xbbb", "BUY", "tokLOSE", "0xc2", 0.5, 20, V1_END + 100),
        ("0xaaa", "BUY", "tokWIN", "0xc2", 0.5, 20, V1_END - 100),
    ])
    _write_resolutions_fixture(tmp_path, [("0xc2", 0, "tokWIN")])

    r = build_wallet_skills(tmp_path)
    assert r["v2_included"] == 1
    assert r["v2_resolved_trades"] == 2  # post-boundary resolved rows only

    conn = connect(tmp_path, attach_hot=False)
    rows = {
        w: (n, z) for w, n, z in conn.execute(
            "SELECT wallet, n_trades, z_score FROM wallet_skills"
        ).fetchall()
    }
    # A: 1 v1 taker row + 1 v1 maker-side unfold (0xmm) is a different wallet;
    # A itself: v1 taker + v2 buy = 2 rows (pre-boundary row excluded, and
    # the uppercase 0xAAA collapses into the same wallet)
    assert rows["0xaaa"][0] == 2
    assert rows["0xaaa"][1] > 0, "won both -> positive z"
    assert rows["0xbbb"][0] == 1
    assert rows["0xbbb"][1] < 0, "lost -> negative z"


def test_wallet_skills_boundary_tracks_corpus_max(tmp_path, monkeypatch):
    """The v1 corpus runs ~7h past the nominal migration midnight; v2 rows
    inside that window are the SAME fills and must not double-count."""
    monkeypatch.setattr(lake, "_WALLET_SKILLS_MIN_TRADES", 1)
    corpus_max = V1_END + 25_000  # corpus extends past the nominal boundary
    _write_v1_fixture(tmp_path, [
        ("0xaaa", "0xmm", "BUY", 10, 0.5, "Yes", "Yes", "0xc1"),
    ], block_ts=corpus_max)
    _write_pm_trades_fixture(tmp_path, [
        # inside the corpus overlap window -> excluded (would double-count)
        ("0xaaa", "BUY", "tokWIN", "0xc2", 0.5, 20, corpus_max - 100),
        # past the corpus's actual end -> included
        ("0xaaa", "BUY", "tokWIN", "0xc2", 0.5, 20, corpus_max + 100),
    ])
    _write_resolutions_fixture(tmp_path, [("0xc2", 0, "tokWIN")])

    r = build_wallet_skills(tmp_path)
    assert r["v2_resolved_trades"] == 1
    conn = connect(tmp_path, attach_hot=False)
    (n_trades,) = conn.execute(
        "SELECT n_trades FROM wallet_skills WHERE wallet = '0xaaa'"
    ).fetchone()
    assert n_trades == 2  # v1 taker row + ONE v2 row


def test_wallet_skills_degrades_to_v1_only(tmp_path, monkeypatch):
    monkeypatch.setattr(lake, "_WALLET_SKILLS_MIN_TRADES", 1)
    _write_v1_fixture(tmp_path, [
        ("0xaaa", "0xmm", "BUY", 10, 0.5, "Yes", "Yes", "0xc1"),
    ])
    r = build_wallet_skills(tmp_path)  # no pm_trades/pm_resolutions present
    assert r["v2_included"] == 0 and r["v2_resolved_trades"] == 0
    assert r["wallets_scored"] == 2  # taker + maker unfold
