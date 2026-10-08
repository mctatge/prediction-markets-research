"""SQLite (WAL) hot/operational store.

Holds current market state for the future screener UI plus collector
cursors/watermarks. This is a rebuildable projection of the raw lake — the
lake is the system of record (data invariants).

Prices are stored as TEXT (decimal strings, 0–1 probability) — never float
(data invariants). Venue-native ids are UNIQUE-indexed columns, never
primary keys (data invariants). Timestamps are unix seconds UTC.
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS collector_state (
    venue        TEXT NOT NULL,
    endpoint     TEXT NOT NULL,
    params_hash  TEXT NOT NULL DEFAULT '',
    cursor       TEXT,
    watermark_ts INTEGER,
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (venue, endpoint, params_hash)
);

CREATE TABLE IF NOT EXISTS series (
    series_id       INTEGER PRIMARY KEY,
    venue_id        TEXT NOT NULL,
    venue_series_id TEXT NOT NULL,
    title           TEXT,
    category        TEXT,
    tags            TEXT,            -- JSON array
    fee_type        TEXT,
    fee_multiplier  TEXT,
    updated_at      INTEGER NOT NULL,
    UNIQUE (venue_id, venue_series_id)
);

CREATE TABLE IF NOT EXISTS events (
    event_id           INTEGER PRIMARY KEY,
    venue_id           TEXT NOT NULL,
    venue_event_id     TEXT NOT NULL,
    venue_series_id    TEXT,
    title              TEXT,
    mutually_exclusive INTEGER,
    category           TEXT,
    status             TEXT,
    updated_at         INTEGER NOT NULL,
    UNIQUE (venue_id, venue_event_id)
);

CREATE TABLE IF NOT EXISTS markets (
    market_id        INTEGER PRIMARY KEY,
    venue_id         TEXT NOT NULL,
    venue_market_id  TEXT NOT NULL,   -- Kalshi market ticker | Polymarket conditionId
    venue_event_id   TEXT,
    venue_series_id  TEXT,            -- from events map; needed for candle endpoints
    title            TEXT,
    market_type      TEXT,
    status_norm      TEXT,            -- unopened|open|paused|closed|settled|voided
    open_ts          INTEGER,
    close_ts         INTEGER,
    yes_bid_prob     TEXT,
    yes_ask_prob     TEXT,
    last_prob        TEXT,
    volume           TEXT,
    volume_24h       TEXT,
    open_interest    TEXT,
    liquidity        TEXT,
    result           TEXT,
    prices_updated_ts INTEGER,        -- when price columns last refreshed
    first_seen_ts    INTEGER NOT NULL,
    last_seen_ts     INTEGER NOT NULL,
    venue_secondary_id TEXT,          -- Polymarket Gamma numeric id
    slug             TEXT,            -- Polymarket market slug
    recurrence       TEXT,            -- derived churn class: 5m|hourly|daily|weekly|NULL
    neg_risk         INTEGER,         -- Polymarket negRisk flag (YES prices arb-linked)
    UNIQUE (venue_id, venue_market_id)
);
CREATE INDEX IF NOT EXISTS idx_markets_status ON markets (venue_id, status_norm);
-- Venue-LESS status filter (the screener default): idx_markets_status leads
-- with venue_id so it can't seek 'open' across venues, and the settlements
-- backfill grew this table to ~12M rows (mostly settled), turning the
-- venue-less open query into a full index scan (~11s, measured 2026-07-12).
-- status_norm alone is selective (~1% open) and restores it to seek speed.
CREATE INDEX IF NOT EXISTS idx_markets_statusnorm ON markets (status_norm);

CREATE TABLE IF NOT EXISTS forecasts (
    forecast_id     INTEGER PRIMARY KEY,
    venue_id        TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,   -- joins markets(venue_id, venue_market_id)
    model_version   TEXT NOT NULL,   -- 'kalshi-flb-vN' | 'shadow-…' (UI excludes shadow)
    model_p         TEXT NOT NULL,   -- experimental probability, decimal string
    base_price      TEXT NOT NULL,   -- market price the model corrected, decimal string
    horizon_bucket  TEXT,            -- 1h|6h|24h|72h|168h
    category_group  TEXT,            -- coarse group (see pmkt.model.dataset)
    created_ts      INTEGER NOT NULL,
    UNIQUE (venue_id, venue_market_id)
);

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id    INTEGER PRIMARY KEY,
    venue_id         TEXT NOT NULL,
    venue_market_id  TEXT NOT NULL,   -- joins markets(venue_id, venue_market_id)
    outcome_index    INTEGER NOT NULL,
    outcome_name     TEXT,
    venue_token_id   TEXT,            -- Polymarket clobTokenId; Kalshi NULL
    is_primary       INTEGER NOT NULL DEFAULT 0,  -- chart/snapshot this leg
    updated_at       INTEGER NOT NULL,
    price_covered_ts INTEGER,         -- per-token price-sweep coverage watermark:
                                      -- minute history fetched through here. NULL
                                      -- = never covered -> deep first window.
    UNIQUE (venue_id, venue_token_id)
);
CREATE INDEX IF NOT EXISTS idx_instruments_market ON instruments (venue_id, venue_market_id);

CREATE TABLE IF NOT EXISTS skilled_ofi (
    ofi_id          INTEGER PRIMARY KEY,
    venue_id        TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,   -- joins markets(venue_id, venue_market_id)
    skilled_ofi     TEXT,            -- signed OFI [-1,+1], decimal string
    unskilled_ofi   TEXT,
    anti_ofi        TEXT,
    z_weighted_ofi  TEXT,            -- z-score-weighted OFI
    skilled_volume  TEXT,            -- total USDC volume from skilled wallets
    total_volume    TEXT,            -- total USDC volume all wallets
    n_skilled_trades INTEGER,
    n_total_trades  INTEGER,
    updated_ts      INTEGER NOT NULL,
    UNIQUE (venue_id, venue_market_id)
);

-- Bridge-backfill progress is a SET, not a cursor: the crawl is ordered by
-- lifetime volume, which shifts between slices, so a positional cursor
-- cannot resume it. One row per fully-crawled market.
CREATE TABLE IF NOT EXISTS pm_backfill_done (
    venue_id        TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,   -- Polymarket conditionId
    done_ts         INTEGER NOT NULL,
    PRIMARY KEY (venue_id, venue_market_id)
);

-- Shadow decision ledger: APPEND-ONLY, deliberately unlike forecasts /
-- skilled_ofi (overwrite-and-prune caches). Every scored candidate at every
-- decide pass is frozen here with the fresh quote it was priced against;
-- the grader joins rows to settled outcomes. Never UPDATE or DELETE.
CREATE TABLE IF NOT EXISTS decisions (
    decision_id     INTEGER PRIMARY KEY,
    venue_id        TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,
    venue_token_id  TEXT,             -- token the action buys (primary leg)
    ts              INTEGER NOT NULL, -- decision time, unix s UTC
    action          TEXT NOT NULL,    -- BUY_YES | BUY_NO | PASS
    theta           TEXT NOT NULL,    -- threshold in effect ('inf' = record-only)
    ev              TEXT,             -- fee-adjusted EV at the fresh quote
    model_p         TEXT,
    base_prob       TEXT,             -- fresh mid the model corrected
    bid_prob        TEXT,
    ask_prob        TEXT,
    bid_size        TEXT,
    ask_size        TEXT,
    spread_prob     TEXT,
    fee_rate        TEXT,
    fee_prob        TEXT,
    skilled_ofi     TEXT,
    n_skilled_trades INTEGER,
    skilled_volume  TEXT,
    ofi_age_s       INTEGER,
    alpha           TEXT,
    category        TEXT,
    close_ts        INTEGER,
    event_key       TEXT,             -- cross-venue event group: a matched
                                      --   PM row + Kalshi row share it (dual-venue)
    signal_venue_id TEXT,             -- venue whose skilled OFI drove the call
                                      --   (Kalshi rows carry 'polymarket' — the
                                      --   signal is PM-side; Kalshi is anonymous)
    signal_market_id TEXT,            -- market id of the signal source
    gates_json      TEXT NOT NULL,    -- {"gate": bool, ...} at decision time
    passed_gates    INTEGER NOT NULL, -- 1 = every gate passed (EV meaningful)
    decide_version  INTEGER NOT NULL,
    created_ts      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_market
    ON decisions (venue_id, venue_market_id, ts);
CREATE INDEX IF NOT EXISTS idx_decisions_ts ON decisions (ts);

-- One grade per decision, written once when its market resolves
-- (INSERT OR IGNORE — grading is idempotent, never revised).
CREATE TABLE IF NOT EXISTS decision_grades (
    decision_id    INTEGER PRIMARY KEY,  -- references decisions(decision_id)
    graded_ts      INTEGER NOT NULL,
    side           TEXT NOT NULL,        -- BUY_YES | BUY_NO (indicated side)
    won            INTEGER NOT NULL,     -- indicated side won
    entry_prob     TEXT NOT NULL,        -- ask (YES) or 1-bid (NO), frozen quote
    ret_per_dollar TEXT NOT NULL,        -- post-fee (won - px - fee) / px
    ev_gap         TEXT                  -- realized/share − modeled ev
                                         --   = won − p(side held); mean over
                                         --   grades = shadow calibration error
);
"""

# Idempotent in-place migration for hot stores created before these columns
# existed (the production DB predates the Polymarket collector). SQLite has no
# ADD COLUMN IF NOT EXISTS, so guard with a PRAGMA duplicate-column check.
MIGRATION_COLUMNS = {
    "markets": (
        ("venue_secondary_id", "TEXT"),
        ("slug", "TEXT"),
        ("recurrence", "TEXT"),
        ("neg_risk", "INTEGER"),
    ),
    "instruments": (
        ("price_covered_ts", "INTEGER"),
    ),
    # Dual-venue ledger (docs/methodology.md): cross-venue event
    # grouping + signal provenance. The production decisions table predates
    # these, so add them in place.
    "decisions": (
        ("event_key", "TEXT"),
        ("signal_venue_id", "TEXT"),
        ("signal_market_id", "TEXT"),
    ),
}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return str(value)


class HotStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        # A manual single-sweep command may briefly write alongside the
        # process's loop; WAL serializes writers but the default busy timeout
        # (0) would surface as spurious 'database is locked' errors.
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Bring a pre-existing hot store up to the current schema in place.
        CREATE TABLE IF NOT EXISTS covers new tables; columns added to an
        existing table need a guarded ALTER (idempotent — safe on every open).
        The duplicate-column except guards the PRAGMA-check/ALTER race when
        two processes (e.g. the processes and `pmkt status`) open simultaneously."""
        for table, columns in MIGRATION_COLUMNS.items():
            existing = {
                row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")
            }
            for column, sql_type in columns:
                if column not in existing:
                    try:
                        self.conn.execute(
                            f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}"
                        )
                    except sqlite3.OperationalError as exc:
                        if "duplicate column" not in str(exc).lower():
                            raise
        self.conn.commit()

    # -- collector_state ----------------------------------------------------

    def get_state(
        self, venue: str, endpoint: str, params_hash: str = ""
    ) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM collector_state WHERE venue=? AND endpoint=? AND params_hash=?",
            (venue, endpoint, params_hash),
        ).fetchone()

    def set_state(
        self,
        venue: str,
        endpoint: str,
        *,
        params_hash: str = "",
        cursor: str | None = None,
        watermark_ts: int | None = None,
        now_ts: int,
    ) -> None:
        self.conn.execute(
            """INSERT INTO collector_state (venue, endpoint, params_hash, cursor, watermark_ts, updated_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(venue, endpoint, params_hash) DO UPDATE SET
                 cursor=excluded.cursor,
                 watermark_ts=COALESCE(excluded.watermark_ts, collector_state.watermark_ts),
                 updated_at=excluded.updated_at""",
            (venue, endpoint, params_hash, cursor, watermark_ts, now_ts),
        )
        self.conn.commit()

    # -- metadata upserts ---------------------------------------------------

    def upsert_series(self, venue_id: str, rows: list[dict[str, Any]], now_ts: int) -> None:
        self.conn.executemany(
            """INSERT INTO series (venue_id, venue_series_id, title, category, tags,
                                   fee_type, fee_multiplier, updated_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(venue_id, venue_series_id) DO UPDATE SET
                 title=excluded.title, category=excluded.category, tags=excluded.tags,
                 fee_type=excluded.fee_type, fee_multiplier=excluded.fee_multiplier,
                 updated_at=excluded.updated_at""",
            [
                (
                    venue_id,
                    r["venue_series_id"],
                    r.get("title"),
                    r.get("category"),
                    _text(r.get("tags")),
                    _text(r.get("fee_type")),
                    _text(r.get("fee_multiplier")),
                    now_ts,
                )
                for r in rows
            ],
        )
        self.conn.commit()

    def upsert_events(self, venue_id: str, rows: list[dict[str, Any]], now_ts: int) -> None:
        self.conn.executemany(
            """INSERT INTO events (venue_id, venue_event_id, venue_series_id, title,
                                   mutually_exclusive, category, status, updated_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(venue_id, venue_event_id) DO UPDATE SET
                 venue_series_id=excluded.venue_series_id, title=excluded.title,
                 mutually_exclusive=excluded.mutually_exclusive,
                 category=excluded.category, status=excluded.status,
                 updated_at=excluded.updated_at""",
            [
                (
                    venue_id,
                    r["venue_event_id"],
                    r.get("venue_series_id"),
                    r.get("title"),
                    r.get("mutually_exclusive"),
                    r.get("category"),
                    r.get("status"),
                    now_ts,
                )
                for r in rows
            ],
        )
        self.conn.commit()

    def upsert_markets(self, venue_id: str, rows: list[dict[str, Any]], now_ts: int) -> None:
        self.conn.executemany(
            """INSERT INTO markets (venue_id, venue_market_id, venue_event_id, venue_series_id,
                                    title, market_type, status_norm, open_ts, close_ts,
                                    yes_bid_prob, yes_ask_prob, last_prob,
                                    volume, volume_24h, open_interest, liquidity, result,
                                    venue_secondary_id, slug, recurrence, neg_risk,
                                    prices_updated_ts, first_seen_ts, last_seen_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(venue_id, venue_market_id) DO UPDATE SET
                 venue_event_id=excluded.venue_event_id,
                 venue_series_id=COALESCE(excluded.venue_series_id, markets.venue_series_id),
                 title=excluded.title, market_type=excluded.market_type,
                 status_norm=excluded.status_norm,
                 open_ts=excluded.open_ts, close_ts=excluded.close_ts,
                 yes_bid_prob=excluded.yes_bid_prob, yes_ask_prob=excluded.yes_ask_prob,
                 last_prob=excluded.last_prob, volume=excluded.volume,
                 volume_24h=excluded.volume_24h, open_interest=excluded.open_interest,
                 liquidity=excluded.liquidity, result=excluded.result,
                 venue_secondary_id=COALESCE(excluded.venue_secondary_id, markets.venue_secondary_id),
                 slug=COALESCE(excluded.slug, markets.slug),
                 recurrence=COALESCE(excluded.recurrence, markets.recurrence),
                 neg_risk=COALESCE(excluded.neg_risk, markets.neg_risk),
                 prices_updated_ts=excluded.prices_updated_ts,
                 last_seen_ts=excluded.last_seen_ts""",
            [
                (
                    venue_id,
                    r["venue_market_id"],
                    r.get("venue_event_id"),
                    r.get("venue_series_id"),
                    r.get("title"),
                    r.get("market_type"),
                    r.get("status_norm"),
                    r.get("open_ts"),
                    r.get("close_ts"),
                    _text(r.get("yes_bid_prob")),
                    _text(r.get("yes_ask_prob")),
                    _text(r.get("last_prob")),
                    _text(r.get("volume")),
                    _text(r.get("volume_24h")),
                    _text(r.get("open_interest")),
                    _text(r.get("liquidity")),
                    r.get("result"),
                    _text(r.get("venue_secondary_id")),
                    r.get("slug"),
                    r.get("recurrence"),
                    r.get("neg_risk"),
                    now_ts,
                    now_ts,
                    now_ts,
                )
                for r in rows
            ],
        )
        self.conn.commit()

    def upsert_instruments(
        self, venue_id: str, rows: list[dict[str, Any]], now_ts: int
    ) -> None:
        """Outcome instruments (the priced unit — e.g. one Polymarket CLOB
        token per outcome). Conflict target is (venue_id, venue_token_id):
        rows with a NULL token id (Kalshi's YES leg, if ever stored here)
        would never conflict and must not be upserted through this path."""
        self.conn.executemany(
            """INSERT INTO instruments (venue_id, venue_market_id, outcome_index,
                                        outcome_name, venue_token_id, is_primary, updated_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(venue_id, venue_token_id) DO UPDATE SET
                 venue_market_id=excluded.venue_market_id,
                 outcome_index=excluded.outcome_index,
                 outcome_name=excluded.outcome_name,
                 is_primary=excluded.is_primary,
                 updated_at=excluded.updated_at""",
            [
                (
                    venue_id,
                    r["venue_market_id"],
                    r["outcome_index"],
                    r.get("outcome_name"),
                    r["venue_token_id"],
                    1 if r.get("is_primary") else 0,
                    now_ts,
                )
                for r in rows
            ],
        )
        self.conn.commit()

    def update_market_prices(
        self, venue_id: str, rows: list[dict[str, Any]], now_ts: int
    ) -> None:
        """Refresh price columns from a candle sweep without touching metadata."""
        self.conn.executemany(
            """UPDATE markets SET
                 yes_bid_prob=COALESCE(?, yes_bid_prob),
                 yes_ask_prob=COALESCE(?, yes_ask_prob),
                 last_prob=COALESCE(?, last_prob),
                 volume=COALESCE(?, volume),
                 open_interest=COALESCE(?, open_interest),
                 prices_updated_ts=?
               WHERE venue_id=? AND venue_market_id=?""",
            [
                (
                    _text(r.get("yes_bid_prob")),
                    _text(r.get("yes_ask_prob")),
                    _text(r.get("last_prob")),
                    _text(r.get("volume")),
                    _text(r.get("open_interest")),
                    now_ts,
                    venue_id,
                    r["venue_market_id"],
                )
                for r in rows
            ],
        )
        self.conn.commit()

    def open_market_tickers(self, venue_id: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT venue_market_id FROM markets WHERE venue_id=? AND status_norm='open'"
            " ORDER BY venue_market_id",
            (venue_id,),
        ).fetchall()
        return [r["venue_market_id"] for r in rows]

    def active_market_tickers(self, venue_id: str) -> list[str]:
        """Open markets with any trading activity — the 5-min candle-sweep
        universe. Kalshi lists ~70k+ open markets but most are dead long-dated
        listings; the hourly metadata sweep still refreshes prices for all of
        them. CAST-to-REAL is only a filter — stored values stay exact TEXT."""
        rows = self.conn.execute(
            """SELECT venue_market_id FROM markets
               WHERE venue_id=? AND status_norm='open'
                 AND (CAST(volume_24h AS REAL) > 0 OR CAST(open_interest AS REAL) > 0)
               ORDER BY venue_market_id""",
            (venue_id,),
        ).fetchall()
        return [r["venue_market_id"] for r in rows]

    def candle_gap_tickers(
        self, venue_id: str, gap_start_ts: int, gap_end_ts: int,
        after: str | None, limit: int,
    ) -> list[str]:
        """Markets tradable during [gap_start_ts, gap_end_ts] with any lifetime
        activity — the candle gap-backfill universe. Keyset-paged on ticker
        (`after`) so a cursor stays valid while other jobs insert markets:
        rows landing before the cursor are skipped, never shifting resume
        position (unlike an index cursor). close_ts over-includes markets that
        settled early — they just return empty candle responses. CAST-to-REAL
        is only a filter — stored values stay exact TEXT."""
        rows = self.conn.execute(
            """SELECT venue_market_id FROM markets
               WHERE venue_id=?
                 AND (close_ts IS NULL OR close_ts > ?)
                 AND (open_ts IS NULL OR open_ts < ?)
                 AND (CAST(volume AS REAL) > 0 OR CAST(open_interest AS REAL) > 0)
                 AND venue_market_id > ?
               ORDER BY venue_market_id
               LIMIT ?""",
            (venue_id, gap_start_ts, gap_end_ts, after or "", limit),
        ).fetchall()
        return [r["venue_market_id"] for r in rows]

    def active_primary_tokens(self, venue_id: str) -> list[tuple[str, str]]:
        """(venue_token_id, venue_market_id) pairs for the price sweep: the
        primary leg of every open market with 24h volume. Recurring
        micro-markets ('5m'/'15m'/'hourly' crypto up-down churn — measured
        3,009 of 46,990 open markets, mostly born+dead between sweeps) are
        metadata-captured but excluded here or the sweep drowns; their
        recurrence class is derived at ingest (see polymarket/models.py).
        Activity = volume_24h > 0 ONLY: liquidity>0 holds for ~90% of open
        Polymarket markets (rewards-seeded books), so it filters nothing,
        and OI doesn't exist there — measured 2026-07-11: vol24h>0 keeps
        11.6k of 47k open markets (~580 batch requests per sweep). Dead
        listings still reprice hourly via the metadata sweep's bestBid/Ask.
        CAST-to-REAL is only a filter — stored values stay exact TEXT."""
        rows = self.conn.execute(
            """SELECT i.venue_token_id, i.venue_market_id, i.price_covered_ts
               FROM instruments i
               JOIN markets m
                 ON m.venue_id = i.venue_id AND m.venue_market_id = i.venue_market_id
               WHERE i.venue_id=? AND i.is_primary=1 AND i.venue_token_id IS NOT NULL
                 AND m.status_norm='open'
                 AND (m.recurrence IS NULL OR m.recurrence NOT IN ('5m','15m','hourly'))
                 AND CAST(m.volume_24h AS REAL) > 0
               ORDER BY i.price_covered_ts, i.venue_token_id""",
            (venue_id,),
        ).fetchall()
        return [
            (r["venue_token_id"], r["venue_market_id"], r["price_covered_ts"])
            for r in rows
        ]

    def pm_backfill_markets(
        self, venue_id: str, v1_end_ts: int = 0, limit: int = 500
    ) -> list[str]:
        """conditionIds of non-churn PM markets that could have gap-period
        trades (close_ts NULL or >= v1_end_ts) and are not yet fully crawled,
        ordered by lifetime volume DESC so high-flow markets land first
        (conditionId tie-break). Markets that closed before the v1 corpus end
        are fully covered and excluded; completed crawls live in
        pm_backfill_done. limit only bounds the fetch — a slice touches at
        most TRADES_PAGE_BUDGET markets since each costs >= 1 page."""
        rows = self.conn.execute(
            """SELECT m.venue_market_id FROM markets m
               LEFT JOIN pm_backfill_done d
                 ON d.venue_id = m.venue_id
                AND d.venue_market_id = m.venue_market_id
               WHERE m.venue_id=? AND m.venue_market_id IS NOT NULL
                 AND d.venue_market_id IS NULL
                 AND (m.recurrence IS NULL OR m.recurrence NOT IN ('5m','15m','hourly'))
                 AND (m.close_ts IS NULL OR m.close_ts >= ?)
               ORDER BY CAST(COALESCE(m.volume, '0') AS REAL) DESC,
                        m.venue_market_id
               LIMIT ?""",
            (venue_id, v1_end_ts, limit),
        ).fetchall()
        return [r["venue_market_id"] for r in rows]

    def pm_backfill_mark_done(
        self, venue_id: str, venue_market_id: str, now_ts: int
    ) -> None:
        self.conn.execute(
            """INSERT OR IGNORE INTO pm_backfill_done
               (venue_id, venue_market_id, done_ts) VALUES (?,?,?)""",
            (venue_id, venue_market_id, now_ts),
        )
        self.conn.commit()

    def pm_backfill_is_done(self, venue_id: str, venue_market_id: str) -> bool:
        return (
            self.conn.execute(
                "SELECT 1 FROM pm_backfill_done WHERE venue_id=? AND venue_market_id=?",
                (venue_id, venue_market_id),
            ).fetchone()
            is not None
        )

    def pm_backfill_done_count(self, venue_id: str) -> int:
        (n,) = self.conn.execute(
            "SELECT COUNT(*) FROM pm_backfill_done WHERE venue_id=?", (venue_id,)
        ).fetchone()
        return n

    def pm_backfill_universe_count(self, venue_id: str, v1_end_ts: int = 0) -> int:
        """Total crawl universe, done or not — the progress-log denominator."""
        (n,) = self.conn.execute(
            """SELECT COUNT(*) FROM markets
               WHERE venue_id=? AND venue_market_id IS NOT NULL
                 AND (recurrence IS NULL OR recurrence NOT IN ('5m','15m','hourly'))
                 AND (close_ts IS NULL OR close_ts >= ?)""",
            (venue_id, v1_end_ts),
        ).fetchone()
        return n

    def pm_trade_markets(self, venue_id: str) -> list[str]:
        """conditionIds of active non-churn PM markets with 24h volume, ordered
        by volume DESC (highest-volume markets processed first within the page
        budget, maximizing signal coverage for skilled OFI)."""
        rows = self.conn.execute(
            """SELECT venue_market_id FROM markets
               WHERE venue_id=? AND status_norm='open'
                 AND (recurrence IS NULL OR recurrence NOT IN ('5m','15m','hourly'))
                 AND CAST(volume_24h AS REAL) > 0
               ORDER BY CAST(volume_24h AS REAL) DESC""",
            (venue_id,),
        ).fetchall()
        return [r["venue_market_id"] for r in rows]

    def set_price_covered(
        self, venue_id: str, token_ids: list[str], covered_ts: int
    ) -> None:
        """Advance per-token price-sweep coverage — only for tokens whose
        chunk actually fetched cleanly. Tokens in failed chunks keep their old
        marker and are re-fetched from there next sweep (bounded by the 24h
        clamp), so a Cloudflare burst can't silently skip a window of minute
        history that resolution will later prune (data invariants)."""
        self.conn.executemany(
            "UPDATE instruments SET price_covered_ts=? WHERE venue_id=? AND venue_token_id=?",
            [(covered_ts, venue_id, t) for t in token_ids],
        )
        self.conn.commit()

    # -- forecasts (model layer) --------------------------------------------

    def upsert_forecasts(
        self, venue_id: str, rows: list[dict[str, Any]], now_ts: int
    ) -> None:
        """One CURRENT forecast row per market (UNIQUE venue_id+venue_market_id).
        ``model_p``/``base_price`` are stored as decimal STRINGS — the model
        layer does float math internally but never persists floats (data invariants
        rules 4/10). History for calibration grading lives in the daily
        parquet snapshots (pmkt.model.score), not here."""
        self.conn.executemany(
            """INSERT INTO forecasts (venue_id, venue_market_id, model_version,
                                      model_p, base_price, horizon_bucket,
                                      category_group, created_ts)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(venue_id, venue_market_id) DO UPDATE SET
                 model_version=excluded.model_version,
                 model_p=excluded.model_p,
                 base_price=excluded.base_price,
                 horizon_bucket=excluded.horizon_bucket,
                 category_group=excluded.category_group,
                 created_ts=excluded.created_ts""",
            [
                (
                    venue_id,
                    r["venue_market_id"],
                    r["model_version"],
                    _text(r["model_p"]),
                    _text(r["base_price"]),
                    r.get("horizon_bucket"),
                    r.get("category_group"),
                    now_ts,
                )
                for r in rows
            ],
        )
        self.conn.commit()

    def prune_stale_forecasts(self, venue_id: str, before_ts: int) -> int:
        """Drop forecast rows not refreshed by the latest scoring run — a
        market that left the model's support (settled, book went empty,
        horizon drifted out of trained buckets) must not keep serving its
        last forecast to the UI as if it were current."""
        cur = self.conn.execute(
            "DELETE FROM forecasts WHERE venue_id=? AND created_ts < ?",
            (venue_id, before_ts),
        )
        self.conn.commit()
        return cur.rowcount

    def latest_forecasts(self, venue_id: str) -> list[sqlite3.Row]:
        """Current forecast per market, for the UI edge column join. Returns
        ALL model versions including 'shadow-…' — the consumer decides what to
        show (the UI join excludes shadow versions)."""
        return self.conn.execute(
            "SELECT * FROM forecasts WHERE venue_id=? ORDER BY venue_market_id",
            (venue_id,),
        ).fetchall()

    # -- skilled OFI (model layer) -------------------------------------------

    def upsert_skilled_ofi(
        self, venue_id: str, rows: list[dict[str, Any]], now_ts: int
    ) -> None:
        self.conn.executemany(
            """INSERT INTO skilled_ofi
                   (venue_id, venue_market_id, skilled_ofi, unskilled_ofi,
                    anti_ofi, z_weighted_ofi, skilled_volume, total_volume,
                    n_skilled_trades, n_total_trades, updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(venue_id, venue_market_id) DO UPDATE SET
                 skilled_ofi=excluded.skilled_ofi,
                 unskilled_ofi=excluded.unskilled_ofi,
                 anti_ofi=excluded.anti_ofi,
                 z_weighted_ofi=excluded.z_weighted_ofi,
                 skilled_volume=excluded.skilled_volume,
                 total_volume=excluded.total_volume,
                 n_skilled_trades=excluded.n_skilled_trades,
                 n_total_trades=excluded.n_total_trades,
                 updated_ts=excluded.updated_ts""",
            [
                (
                    venue_id,
                    r["venue_market_id"],
                    _text(r.get("skilled_ofi")),
                    _text(r.get("unskilled_ofi")),
                    _text(r.get("anti_ofi")),
                    _text(r.get("z_weighted_ofi")),
                    _text(r.get("skilled_volume")),
                    _text(r.get("total_volume")),
                    r.get("n_skilled_trades"),
                    r.get("n_total_trades"),
                    now_ts,
                )
                for r in rows
            ],
        )
        self.conn.commit()

    # -- decisions (append-only shadow ledger) --------------------------------

    def insert_decisions(self, rows: list[dict[str, Any]], now_ts: int) -> int:
        """Append decision rows. The ledger is append-only by design — there
        is deliberately no update or delete method."""
        if not rows:
            return 0
        cols = (
            "venue_id", "venue_market_id", "venue_token_id", "ts", "action",
            "theta", "ev", "model_p", "base_prob", "bid_prob", "ask_prob",
            "bid_size", "ask_size", "spread_prob", "fee_rate", "fee_prob",
            "skilled_ofi", "n_skilled_trades", "skilled_volume", "ofi_age_s",
            "alpha", "category", "close_ts", "event_key", "signal_venue_id",
            "signal_market_id", "gates_json", "passed_gates",
            "decide_version",
        )
        self.conn.executemany(
            f"INSERT INTO decisions ({', '.join(cols)}, created_ts) "
            f"VALUES ({', '.join('?' * len(cols))}, ?)",
            [tuple(r.get(c) for c in cols) + (now_ts,) for r in rows],
        )
        self.conn.commit()
        return len(rows)

    def decisions_count(self) -> int:
        (n,) = self.conn.execute("SELECT COUNT(*) FROM decisions").fetchone()
        return n

    def insert_decision_grades(self, rows: list[dict[str, Any]], now_ts: int) -> int:
        """Record grades idempotently — a decision is graded exactly once."""
        if not rows:
            return 0
        cur = self.conn.executemany(
            """INSERT OR IGNORE INTO decision_grades
               (decision_id, graded_ts, side, won, entry_prob,
                ret_per_dollar, ev_gap)
               VALUES (?,?,?,?,?,?,?)""",
            [
                (
                    r["decision_id"], now_ts, r["side"], r["won"],
                    r["entry_prob"], r["ret_per_dollar"], r["ev_gap"],
                )
                for r in rows
            ],
        )
        self.conn.commit()
        return cur.rowcount

    def prune_stale_ofi(self, venue_id: str, before_ts: int) -> int:
        """Drop OFI rows not refreshed — a market whose skilled flow dried up
        should not keep showing a stale signal."""
        cur = self.conn.execute(
            "DELETE FROM skilled_ofi WHERE venue_id=? AND updated_ts < ?",
            (venue_id, before_ts),
        )
        self.conn.commit()
        return cur.rowcount

    def mark_unseen_markets_closed(
        self, venue_id: str, sweep_ts: int
    ) -> int:
        """Open markets not seen in the latest full open-market sweep are no
        longer open on the venue; flag them for the next metadata refresh."""
        cur = self.conn.execute(
            """UPDATE markets SET status_norm='closed'
               WHERE venue_id=? AND status_norm='open' AND last_seen_ts < ?""",
            (venue_id, sweep_ts),
        )
        self.conn.commit()
        return cur.rowcount

    def close(self) -> None:
        self.conn.close()
