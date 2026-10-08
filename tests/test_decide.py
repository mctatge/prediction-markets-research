"""Shadow decision ledger: gates, fresh-quote EV, append-only recording,
record-only default (theta=inf), and BUY when theta is set."""

import json
import time
from decimal import Decimal

from pmkt.model import decide as decide_mod
from pmkt.model.decide import decide_once
from pmkt.state import HotStore

NOW = 1_784_300_000


class FakePMClient:
    def __init__(self, books):
        self.books = books  # {token_id: {bid, ask, bid_size, ask_size}}
        self.calls = []

    async def get_books(self, token_ids, *, endpoint="clob_books"):
        self.calls.append(list(token_ids))
        return {t: self.books[t] for t in token_ids if t in self.books}


def _seed(store, cid, token, *, category="Sports", ofi="0.8000", n_sk=5,
          close_ts=NOW + 86400, ofi_ts=NOW - 60, status="open"):
    store.conn.execute(
        """INSERT INTO markets (venue_id, venue_market_id, venue_event_id,
                                status_norm, close_ts, first_seen_ts, last_seen_ts)
           VALUES ('polymarket',?,?,?,?,0,0)""",
        (cid, f"ev-{cid}", status, close_ts),
    )
    store.conn.execute(
        """INSERT INTO events (venue_id, venue_event_id, category, updated_at)
           VALUES ('polymarket',?,?,0)""",
        (f"ev-{cid}", category),
    )
    store.conn.execute(
        """INSERT INTO instruments (venue_id, venue_market_id, outcome_index,
                                    venue_token_id, is_primary, updated_at)
           VALUES ('polymarket',?,0,?,1,0)""",
        (cid, token),
    )
    store.conn.execute(
        """INSERT INTO skilled_ofi (venue_id, venue_market_id, skilled_ofi,
                                    n_skilled_trades, skilled_volume, updated_ts)
           VALUES ('polymarket',?,?,?,'100.00',?)""",
        (cid, ofi, n_sk, ofi_ts),
    )
    store.conn.commit()


def _book(bid="0.50", ask="0.52"):
    return {
        "bid": Decimal(bid), "ask": Decimal(ask),
        "bid_size": Decimal("100"), "ask_size": Decimal("100"),
    }


async def test_records_pass_rows_with_fresh_quote_ev(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    _seed(store, "0xa", "tokA")  # Sports, alpha=0.07, OFI +0.8
    client = FakePMClient({"tokA": _book()})

    r = await decide_once(client, store)
    assert r == {"candidates": 1, "recorded": 1, "passed_gates": 1, "buys": 0}

    row = store.conn.execute("SELECT * FROM decisions").fetchone()
    assert row["action"] == "PASS" and row["theta"] == "inf"
    assert row["passed_gates"] == 1
    # mid=0.51, model_p = 0.51 + 0.07*0.8 = 0.566 -> BUY_YES side economics:
    # ev = model_p - ask - 0.05*ask*(1-ask)
    assert row["model_p"] == "0.5660"
    assert row["base_prob"] == "0.5100"
    ev = Decimal("0.5660") - Decimal("0.52") - (
        Decimal("0.05") * Decimal("0.52") * Decimal("0.48")
    )
    assert row["ev"] == format(ev.quantize(Decimal("0.0001")), "f")
    assert json.loads(row["gates_json"])["quote_present"] is True
    # append-only: a second pass adds rows, never overwrites
    await decide_once(client, store)
    assert store.decisions_count() == 2


async def test_gates_and_prequote_filtering(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "hot.sqlite")
    _seed(store, "0xa", "tokA")                                # good
    _seed(store, "0xb", "tokB", category="Politics")           # alpha=0 -> skipped
    _seed(store, "0xc", "tokC", ofi_ts=NOW - 7200)             # stale OFI -> skipped
    _seed(store, "0xd", "tokD", close_ts=NOW + 600)            # too close -> skipped
    _seed(store, "0xe", "tokE", n_sk=1)                        # thin -> skipped
    _seed(store, "0xf", "tokF")                                # wide spread -> recorded, fails gates
    client = FakePMClient({
        "tokA": _book(),
        "tokF": _book(bid="0.30", ask="0.55"),
    })

    r = await decide_once(client, store)
    assert r["candidates"] == 2, "pre-quote gates filter before requoting"
    assert r["recorded"] == 2 and r["passed_gates"] == 1
    fails = store.conn.execute(
        "SELECT gates_json FROM decisions WHERE venue_market_id='0xf'"
    ).fetchone()
    gates = json.loads(fails[0])
    assert gates["spread_ok"] is False and gates["quote_present"] is True
    requoted = {t for call in client.calls for t in call}
    assert requoted == {"tokA", "tokF"}


async def test_theta_turns_pass_into_buy(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setenv("PMKT_DECIDE_THETA", "-1.0")  # everything clears
    store = HotStore(tmp_path / "hot.sqlite")
    _seed(store, "0xa", "tokA", ofi="0.8000")   # model_p > mid -> BUY_YES
    _seed(store, "0xb", "tokB", ofi="-0.8000")  # model_p < mid -> BUY_NO
    client = FakePMClient({"tokA": _book(), "tokB": _book()})

    r = await decide_once(client, store)
    assert r["buys"] == 2
    actions = {
        row["venue_market_id"]: row["action"]
        for row in store.conn.execute(
            "SELECT venue_market_id, action FROM decisions"
        )
    }
    assert actions == {"0xa": "BUY_YES", "0xb": "BUY_NO"}
