"""Dual-venue shadow ledger end-to-end: build the crosswalk from a synthetic
hot store, log a PAIRED PM + Kalshi decision for the matched event (shared
event_key, PM-sourced signal), and grade the Kalshi leg from hot markets.result."""

import datetime as dt
import time
from decimal import Decimal

import duckdb

from pmkt import lake
from pmkt.model.decide import decide_once, grade_decisions
from pmkt.state import HotStore


class FakePMClient:
    def __init__(self, books):
        self.books = books

    async def get_books(self, token_ids, *, endpoint="clob_books"):
        return {t: self.books[t] for t in token_ids if t in self.books}


class FakeKalshiClient:
    def __init__(self, books):
        self.books = books  # {ticker: orderbook payload}
        self.calls = []

    async def get_orderbook(self, ticker):
        self.calls.append(ticker)
        return self.books.get(ticker, {"orderbook_fp": {}})


def _pm_book(bid="0.50", ask="0.52"):
    return {"bid": Decimal(bid), "ask": Decimal(ask),
            "bid_size": Decimal("100"), "ask_size": Decimal("100")}


def _k_book(yes_bid="0.50", no_bid="0.49"):
    return {"orderbook_fp": {
        "yes_dollars": [["0.10", "1"], [yes_bid, "500"]],
        "no_dollars": [["0.10", "1"], [no_bid, "500"]],
    }}


def _seed_matched_game(store, now):
    """One MLB game present on BOTH venues, with fresh PM skilled OFI."""
    game = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
    tk = game.strftime("%y%b%d").upper()           # e.g. 26JUL21
    iso = game.strftime("%Y-%m-%d")
    event = f"KXMLBGAME-{tk}DETPHI"
    close_ts = now + 30 * 3600
    cid, tok_det, tok_phi = "0xdetphi", "tokDET", "tokPHI"

    for suf in ("DET", "PHI"):
        store.conn.execute(
            """INSERT INTO markets (venue_id, venue_market_id, venue_series_id,
                   venue_event_id, title, status_norm, result, close_ts,
                   first_seen_ts, last_seen_ts)
               VALUES ('kalshi',?,?,?,?,'open','',?,0,0)""",
            (f"{event}-{suf}", "KXMLBGAME", event, f"Will {suf} win", close_ts),
        )
    store.conn.execute(
        """INSERT INTO markets (venue_id, venue_market_id, venue_event_id, slug,
               status_norm, close_ts, first_seen_ts, last_seen_ts)
           VALUES ('polymarket',?,?,?,'open',?,0,0)""",
        (cid, f"ev-{cid}", f"mlb-det-phi-{iso}", close_ts),
    )
    store.conn.execute(
        """INSERT INTO events (venue_id, venue_event_id, category, updated_at)
           VALUES ('polymarket',?,'Sports',0)""", (f"ev-{cid}",),
    )
    for oi, tok, name, prim in ((0, tok_det, "DET", 1), (1, tok_phi, "PHI", 0)):
        store.conn.execute(
            """INSERT INTO instruments (venue_id, venue_market_id, outcome_index,
                   venue_token_id, outcome_name, is_primary, updated_at)
               VALUES ('polymarket',?,?,?,?,?,0)""",
            (cid, oi, tok, name, prim),
        )
    store.conn.execute(
        """INSERT INTO skilled_ofi (venue_id, venue_market_id, skilled_ofi,
               n_skilled_trades, skilled_volume, updated_ts)
           VALUES ('polymarket',?, '0.8000', 5, '100.00', ?)""",
        (cid, now - 60),
    )
    store.conn.commit()
    return event, cid, tok_det


async def test_build_match_decide_grade_dual_venue(tmp_path, monkeypatch):
    monkeypatch.setenv("PMKT_DATA_DIR", str(tmp_path))
    now = int(time.time())
    store = HotStore(tmp_path / "db" / "hot.sqlite")
    event, cid, tok_det = _seed_matched_game(store, now)

    # 1) Build the crosswalk from the hot store → the MLB game auto-confirms.
    report = lake.build_venue_crosswalk(tmp_path)
    assert report["confirmed_contracts"] == 2, report
    confirmed = lake.read_venue_crosswalk(tmp_path, confirmed=1)
    det = next(r for r in confirmed if r["kalshi_side"] == "DET")
    assert det["pm_token_id"] == tok_det and det["auto_confirmed"] == 1
    assert det["pm_token_id"] == det["pm_primary_token_id"]  # primary-aligned

    # 2) One dual-venue decide pass: a PM decision AND a Kalshi decision for the
    #    same event, sharing event_key; the Kalshi leg's signal is PM-sourced.
    pm_client = FakePMClient({tok_det: _pm_book()})
    k_client = FakeKalshiClient({f"{event}-DET": _k_book()})
    res = await decide_once(pm_client, store, kalshi_client=k_client,
                            data_dir=tmp_path)
    assert res["candidates"] == 1 and res["kalshi_candidates"] == 1, res
    assert f"{event}-DET" in k_client.calls

    rows = {
        r["venue_id"]: r for r in store.conn.execute("SELECT * FROM decisions")
    }
    assert set(rows) == {"polymarket", "kalshi"}
    pm_row, k_row = rows["polymarket"], rows["kalshi"]
    assert pm_row["event_key"] == event == k_row["event_key"]  # paired
    assert k_row["venue_market_id"] == f"{event}-DET"
    assert k_row["venue_token_id"] is None
    assert k_row["signal_venue_id"] == "polymarket"
    assert k_row["signal_market_id"] == cid
    assert k_row["fee_rate"] == "0.07"          # Kalshi taker rate
    # Kalshi book mid 0.505, alpha 0.07, OFI +0.8 → model_p 0.561 > mid → BUY_YES
    assert k_row["base_prob"] == "0.5050" and k_row["model_p"] == "0.5610"
    assert k_row["passed_gates"] == 1

    # 3) Grade: DET wins on BOTH venues → both indicated BUY_YES legs won.
    store.conn.execute(
        "UPDATE markets SET result='yes', status_norm='settled' "
        "WHERE venue_id='kalshi' AND venue_market_id=?", (f"{event}-DET",),
    )
    store.conn.commit()
    _write_pm_resolution(tmp_path, cid, tok_det)

    g = grade_decisions(tmp_path, store, now_ts=now + 40 * 3600)
    assert g["graded"] == 2, g
    graded = {
        r["venue_id"]: gr for (r, gr) in [
            (dict(d), dict(d)) for d in store.conn.execute(
                "SELECT d.venue_id, g.side, g.won FROM decision_grades g "
                "JOIN decisions d ON d.decision_id=g.decision_id")
        ]
    }
    assert graded["kalshi"]["side"] == "BUY_YES" and graded["kalshi"]["won"] == 1
    assert graded["polymarket"]["won"] == 1


async def test_kalshi_grade_no_win_and_missing_result(tmp_path, monkeypatch):
    """Kalshi rows resolve from hot markets.result; unresolved rows wait, and a
    'no' result loses a BUY_YES leg — independent of pm_resolutions existing."""
    monkeypatch.setenv("PMKT_DATA_DIR", str(tmp_path))
    now = int(time.time())
    store = HotStore(tmp_path / "db" / "hot.sqlite")
    event, cid, tok_det = _seed_matched_game(store, now)
    lake.build_venue_crosswalk(tmp_path)

    pm_client = FakePMClient({tok_det: _pm_book()})
    k_client = FakeKalshiClient({f"{event}-DET": _k_book()})
    await decide_once(pm_client, store, kalshi_client=k_client, data_dir=tmp_path)

    # No resolutions yet (pm_resolutions parquet absent, Kalshi still open) → 0.
    assert grade_decisions(tmp_path, store, now_ts=now)["graded"] == 0

    # DET resolves NO on Kalshi → the BUY_YES Kalshi leg loses. No pm_resolutions
    # file at all, so only the Kalshi row grades.
    store.conn.execute(
        "UPDATE markets SET result='no', status_norm='settled' "
        "WHERE venue_id='kalshi' AND venue_market_id=?", (f"{event}-DET",),
    )
    store.conn.commit()
    g = grade_decisions(tmp_path, store, now_ts=now + 40 * 3600)
    assert g["graded"] == 1
    row = store.conn.execute(
        "SELECT d.venue_id, g.side, g.won FROM decision_grades g "
        "JOIN decisions d ON d.decision_id=g.decision_id"
    ).fetchone()
    assert row["venue_id"] == "kalshi" and row["side"] == "BUY_YES"
    assert row["won"] == 0


def _write_pm_resolution(data_dir, cid, winning_token):
    d = data_dir / "curated" / "pm_resolutions"
    d.mkdir(parents=True, exist_ok=True)
    duckdb.execute(
        "COPY (SELECT * FROM (VALUES "
        f"('polymarket', '{cid}', 0, '{winning_token}', 'Yes', "
        "TIMESTAMP '2026-07-20', 1)) AS v(venue_id, venue_market_id, "
        "winning_outcome_index, winning_token_id, winning_outcome_label, "
        "source_fetched_at, parser_version)) "
        f"TO '{d / 'part-0.parquet'}' (FORMAT parquet)"
    )
