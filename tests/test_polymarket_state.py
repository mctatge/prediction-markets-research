"""HotStore additions for the Polymarket collector: instruments table,
in-place markets migration, and the active-primary-token price-sweep query."""

import sqlite3
from decimal import Decimal

from pmkt.state import HotStore

VENUE = "polymarket"


def make_store(tmp_path):
    return HotStore(tmp_path / "db" / "hot.sqlite")


def pm_market(cond_id, **overrides):
    row = {
        "venue_market_id": cond_id,
        "venue_event_id": "111",
        "venue_series_id": None,
        "venue_secondary_id": "516950",
        "slug": f"slug-{cond_id}",
        "title": "q?",
        "market_type": "binary",
        "status_norm": "open",
        "recurrence": None,
        "neg_risk": 0,
        "yes_bid_prob": Decimal("0.41"),
        "yes_ask_prob": Decimal("0.42"),
        "last_prob": Decimal("0.415"),
        "volume": Decimal("100.5"),
        "volume_24h": Decimal("10.0"),
        "liquidity": Decimal("500.0"),
    }
    row.update(overrides)
    return row


def instruments_for(cond_id, tok_prefix):
    return [
        {"venue_market_id": cond_id, "outcome_index": 0, "outcome_name": "Yes",
         "venue_token_id": f"{tok_prefix}-yes", "is_primary": True},
        {"venue_market_id": cond_id, "outcome_index": 1, "outcome_name": "No",
         "venue_token_id": f"{tok_prefix}-no", "is_primary": False},
    ]


def test_upsert_instruments_idempotent_on_token_id(tmp_path):
    store = make_store(tmp_path)
    store.upsert_instruments(VENUE, instruments_for("0xc1", "t1"), now_ts=100)
    # Re-sweep: same tokens, renamed outcome (negRiskAugmented placeholder
    # renames are real) — must update in place, not duplicate.
    rows2 = instruments_for("0xc1", "t1")
    rows2[0]["outcome_name"] = "Candidate A"
    store.upsert_instruments(VENUE, rows2, now_ts=200)

    rows = store.conn.execute(
        "SELECT * FROM instruments ORDER BY outcome_index"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["outcome_name"] == "Candidate A"
    assert rows[0]["is_primary"] == 1 and rows[1]["is_primary"] == 0
    assert rows[0]["updated_at"] == 200
    store.close()


def test_markets_migration_adds_columns_to_existing_db(tmp_path):
    """The production hot store predates the Polymarket columns — opening it
    must migrate in place, idempotently, without touching existing rows."""
    db = tmp_path / "db" / "hot.sqlite"
    db.parent.mkdir(parents=True)
    # Simulate the pre-Polymarket schema: markets without the new columns.
    conn = sqlite3.connect(db)
    conn.executescript(
        """CREATE TABLE markets (
               market_id INTEGER PRIMARY KEY,
               venue_id TEXT NOT NULL, venue_market_id TEXT NOT NULL,
               venue_event_id TEXT, venue_series_id TEXT, title TEXT,
               market_type TEXT, status_norm TEXT, open_ts INTEGER,
               close_ts INTEGER, yes_bid_prob TEXT, yes_ask_prob TEXT,
               last_prob TEXT, volume TEXT, volume_24h TEXT,
               open_interest TEXT, liquidity TEXT, result TEXT,
               prices_updated_ts INTEGER, first_seen_ts INTEGER NOT NULL,
               last_seen_ts INTEGER NOT NULL,
               UNIQUE (venue_id, venue_market_id));
           INSERT INTO markets (venue_id, venue_market_id, status_norm,
                                first_seen_ts, last_seen_ts)
           VALUES ('kalshi', 'OLD-TICKER', 'open', 1, 1);"""
    )
    conn.commit()
    conn.close()

    store = HotStore(db)  # first open migrates
    cols = {r["name"] for r in store.conn.execute("PRAGMA table_info(markets)")}
    assert {"venue_secondary_id", "slug", "recurrence", "neg_risk"} <= cols
    row = store.conn.execute("SELECT * FROM markets").fetchone()
    assert row["venue_market_id"] == "OLD-TICKER"
    assert row["venue_secondary_id"] is None
    store.close()

    store2 = HotStore(db)  # second open: migration must be a no-op
    store2.upsert_markets(VENUE, [pm_market("0xabc")], now_ts=100)
    got = store2.conn.execute(
        "SELECT * FROM markets WHERE venue_id=?", (VENUE,)
    ).fetchone()
    assert got["venue_secondary_id"] == "516950"
    assert got["slug"] == "slug-0xabc"
    assert got["yes_bid_prob"] == "0.41"  # TEXT, exact — never float
    store2.close()


def test_active_primary_tokens_query(tmp_path):
    store = make_store(tmp_path)
    store.upsert_markets(
        VENUE,
        [
            pm_market("0xlive"),                                   # in
            pm_market("0xclosed", status_norm="closed"),           # out: closed
            pm_market("0xdead", volume_24h=Decimal("0")),          # out: no 24h volume
            pm_market("0xliq", volume_24h=None,
                      liquidity=Decimal("9.5")),                   # out: liquidity alone
            # is NOT activity (rewards-seeded books hold liq>0 on ~90% of
            # open markets — measured 2026-07-11)
            pm_market("0x5m", recurrence="5m"),                    # out: churn
            pm_market("0x15m", recurrence="15m"),                  # out: churn
            pm_market("0xhr", recurrence="hourly"),                # out: churn
            pm_market("0xdaily", recurrence="daily"),              # in: slow recurrence
        ],
        now_ts=100,
    )
    for cond in ("0xlive", "0xclosed", "0xdead", "0xliq", "0x5m", "0x15m", "0xhr", "0xdaily"):
        store.upsert_instruments(VENUE, instruments_for(cond, f"tok{cond}"), now_ts=100)
    # A different venue's markets must never leak in.
    store.upsert_markets("kalshi", [pm_market("K-TICKER")], now_ts=100)

    rows = store.active_primary_tokens(VENUE)
    tokens = {t for t, _m, _c in rows}
    assert tokens == {"tok0xlive-yes", "tok0xdaily-yes"}
    # rows carry the owning market for last_prob updates
    assert {t: m for t, m, _c in rows}["tok0xlive-yes"] == "0xlive"
    # never-covered tokens surface covered_ts=None (deep first window)
    assert all(c is None for _t, _m, c in rows)

    # coverage advances only for the tokens named — and NULLs sort first so
    # uncovered tokens lead the sweep order
    store.set_price_covered(VENUE, ["tok0xlive-yes"], 12345)
    rows = store.active_primary_tokens(VENUE)
    assert rows[0][0] == "tok0xdaily-yes" and rows[0][2] is None
    assert rows[1] == ("tok0xlive-yes", "0xlive", 12345)
    # only primary legs — never the redundant No token
    assert not any(t.endswith("-no") for t in tokens)
    store.close()


def test_kalshi_rows_unaffected_by_new_columns(tmp_path):
    """Kalshi upserts don't pass the new keys — they must land as NULL and
    round-trip exactly as before (regression guard for the shared table)."""
    store = make_store(tmp_path)
    store.upsert_markets(
        "kalshi",
        [{
            "venue_market_id": "TICK-1", "venue_event_id": "EV", "title": "t?",
            "status_norm": "open", "yes_bid_prob": Decimal("0.4200"),
            "volume_24h": Decimal("5.00"), "open_interest": Decimal("1.00"),
        }],
        now_ts=100,
    )
    row = store.conn.execute("SELECT * FROM markets").fetchone()
    assert row["recurrence"] is None and row["slug"] is None
    assert store.active_market_tickers("kalshi") == ["TICK-1"]
    store.close()
