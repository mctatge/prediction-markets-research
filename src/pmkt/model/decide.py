"""Paper decision ledger: hypothetical BUY YES, BUY NO or PASS records.

For eligible selected-wallet flow, obtains a book quote and records model
estimates, assumed fees, quote fields and gate results. The collector can
schedule this after a flow pass when explicitly enabled. Historical category
coefficients are experimental; positive modeled value is not evidence of
profitable execution. See docs/methodology.md and docs/findings.md.

The default threshold is infinity, so every action is PASS. Changing the
threshold changes recorded hypothetical actions only. This module submits no
orders. The grader can evaluate indicated sides for gate-qualified PASS rows;
those counterfactual grades are distinct from paper entries and live fills.

Gates include a positive configured coefficient, recent flow, at least three
selected-wallet trades, two-sided quotes, spread at most 0.10, midpoint in
[0.05, 0.95], and at least one hour until close. The synthetic demo adds
explicit quote-observation ordering, quote-age and available-size checks.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from decimal import Decimal
from typing import Any

from ..model.fees import taker_fee_rate
from ..ui.ofi_live import ofi_alpha_for

logger = logging.getLogger(__name__)

VENUE_ID = "polymarket"
KALSHI_VENUE_ID = "kalshi"
DECIDE_VERSION = 1

OFI_MAX_AGE_S = 1800
MIN_SKILLED_TRADES = 3
SPREAD_MAX = Decimal("0.10")
PRICE_LO = Decimal("0.05")
PRICE_HI = Decimal("0.95")
MIN_TIME_TO_CLOSE_S = 3600
MAX_CANDIDATES = 100  # ~5 books requests per pass at 20 tokens/request
BOOKS_BATCH = 20
MAX_KALSHI_REQUOTES = 60  # per-ticker book fetches/pass (no batch endpoint)

_Q4 = Decimal("0.0001")


def _theta() -> float:
    return float(os.environ.get("PMKT_DECIDE_THETA", "inf"))


def _fmt(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.quantize(_Q4), "f")


def _kalshi_best_quote(payload: Any) -> dict[str, Any]:
    """Top-of-book YES quote from a Kalshi orderbook response.

    Kalshi books are BIDS ONLY (``orderbook_fp`` → ``yes_dollars`` /
    ``no_dollars``), ascending, BEST BID LAST; the YES ask is derived from the
    opposite side: ``yes_ask = 1 − best_no_bid``. Prices are exact dollar
    strings → Decimal (data invariants). ``ask_size`` is the size resting at the
    best NO bid — the depth available to LIFT the YES ask."""
    ob = (payload or {}).get("orderbook_fp") or {}
    yes = ob.get("yes_dollars") or []
    no = ob.get("no_dollars") or []
    bid = Decimal(yes[-1][0]) if yes else None
    bid_size = Decimal(yes[-1][1]) if yes else None
    no_bid = Decimal(no[-1][0]) if no else None
    ask_size = Decimal(no[-1][1]) if no else None
    ask = (Decimal(1) - no_bid) if no_bid is not None else None
    return {"bid": bid, "ask": ask, "bid_size": bid_size, "ask_size": ask_size}


def _price_decision(
    venue_id: str, category: str | None, ofi: str, alpha: float,
    book: dict[str, Any],
) -> dict[str, Any]:
    """Fee-adjusted EV of the OFI-indicated side at the fresh top-of-book.
    The one shared pricing path for BOTH venues (that is the whole point of
    the dual-venue ledger: same signal + same math, different book + fee)."""
    bid, ask = book.get("bid"), book.get("ask")
    ofi_d = Decimal(ofi)
    alpha_d = Decimal(str(alpha))
    rate = Decimal(str(taker_fee_rate(venue_id, category)))
    mid = spread = model_p = ev = fee = side = None
    if bid is not None and ask is not None:
        spread = ask - bid
        mid = (bid + ask) / 2
        model_p = min(Decimal(1), max(Decimal(0), mid + alpha_d * ofi_d))
        if model_p >= mid:
            side = "BUY_YES"
            fee = rate * ask * (1 - ask)
            ev = model_p - ask - fee
        else:
            side = "BUY_NO"
            fee = rate * bid * (1 - bid)
            ev = bid - model_p - fee
    return {
        "bid": bid, "ask": ask, "mid": mid, "spread": spread,
        "model_p": model_p, "side": side, "ev": ev, "fee": fee, "rate": rate,
    }


def _gates(alpha: float, ofi_age: int, n_sk: int, d: dict[str, Any]) -> dict:
    """The per-decision gate results (time_to_close is pre-gated upstream)."""
    return {
        "category_promoted": alpha > 0.0,
        "ofi_fresh": ofi_age <= OFI_MAX_AGE_S,
        "signal_support": (n_sk or 0) >= MIN_SKILLED_TRADES,
        "quote_present": d["bid"] is not None and d["ask"] is not None,
        "spread_ok": d["spread"] is not None and d["spread"] <= SPREAD_MAX,
        "price_band": d["mid"] is not None and PRICE_LO <= d["mid"] <= PRICE_HI,
        "time_to_close": True,
    }


def _assemble_row(
    *, venue_id: str, venue_market_id: str, venue_token_id: str | None,
    now_ts: int, theta: float, d: dict[str, Any], book: dict[str, Any],
    ofi: str, n_sk: int, skilled_vol: Any, ofi_age: int, alpha: float,
    category: str | None, close_ts: int | None, event_key: str | None,
    signal_venue_id: str | None, signal_market_id: str | None,
    gates: dict, passed: bool, buy: bool,
) -> dict[str, Any]:
    return {
        "venue_id": venue_id, "venue_market_id": venue_market_id,
        "venue_token_id": venue_token_id, "ts": now_ts,
        "action": d["side"] if buy else "PASS",
        "theta": "inf" if math.isinf(theta) else f"{theta:.4f}",
        "ev": _fmt(d["ev"]), "model_p": _fmt(d["model_p"]),
        "base_prob": _fmt(d["mid"]), "bid_prob": _fmt(d["bid"]),
        "ask_prob": _fmt(d["ask"]), "bid_size": _fmt(book.get("bid_size")),
        "ask_size": _fmt(book.get("ask_size")), "spread_prob": _fmt(d["spread"]),
        "fee_rate": format(d["rate"], "f"), "fee_prob": _fmt(d["fee"]),
        "skilled_ofi": ofi, "n_skilled_trades": n_sk,
        "skilled_volume": skilled_vol, "ofi_age_s": ofi_age,
        "alpha": format(Decimal(str(alpha)), "f"), "category": category,
        "close_ts": close_ts, "event_key": event_key,
        "signal_venue_id": signal_venue_id, "signal_market_id": signal_market_id,
        "gates_json": json.dumps(gates, separators=(",", ":")),
        "passed_gates": int(passed), "decide_version": DECIDE_VERSION,
    }


_CANDIDATES_SQL = """
SELECT o.venue_market_id,
       o.skilled_ofi, o.n_skilled_trades, o.skilled_volume,
       o.updated_ts AS ofi_ts,
       COALESCE(e.category, s.category) AS category,
       m.close_ts,
       i.venue_token_id
FROM skilled_ofi o
JOIN markets m
  ON m.venue_id = o.venue_id AND m.venue_market_id = o.venue_market_id
LEFT JOIN events e
  ON e.venue_id = m.venue_id AND e.venue_event_id = m.venue_event_id
LEFT JOIN series s
  ON s.venue_id = m.venue_id AND s.venue_series_id = m.venue_series_id
JOIN instruments i
  ON i.venue_id = m.venue_id AND i.venue_market_id = m.venue_market_id
 AND i.is_primary = 1
WHERE o.venue_id = ? AND m.status_norm = 'open'
  AND o.skilled_ofi IS NOT NULL
"""

# PM skilled-OFI signal for a set of matched PM conditionIds — the signal
# applied to the paired Kalshi book (Kalshi trades are anonymous, no OFI).
_KALSHI_SIGNAL_SQL = """
SELECT o.venue_market_id,
       o.skilled_ofi, o.n_skilled_trades, o.skilled_volume,
       o.updated_ts AS ofi_ts,
       COALESCE(e.category, s.category) AS category
FROM skilled_ofi o
JOIN markets m
  ON m.venue_id = o.venue_id AND m.venue_market_id = o.venue_market_id
LEFT JOIN events e
  ON e.venue_id = m.venue_id AND e.venue_event_id = m.venue_event_id
LEFT JOIN series s
  ON s.venue_id = m.venue_id AND s.venue_series_id = m.venue_series_id
WHERE o.venue_id = 'polymarket' AND o.skilled_ofi IS NOT NULL
  AND o.venue_market_id IN ({ph})
"""


def _confirmed_primary_matches(data_dir: Any) -> list[dict[str, Any]]:
    """Confirmed crosswalk rows whose PM leg is the PRIMARY token — one row
    per Kalshi market, so the Kalshi decision bets the SAME economic side the
    PM decision does (PM OFI is signed toward the primary token). Parquet-only
    read (no hot ATTACH in-daemon — the 46h incident rule)."""
    from .. import lake

    seen: dict[str, dict[str, Any]] = {}
    for r in lake.read_venue_crosswalk(data_dir, confirmed=1):
        if r["pm_token_id"] and r["pm_token_id"] == r["pm_primary_token_id"]:
            seen.setdefault(r["kalshi_ticker"], r)
    return list(seen.values())


async def _kalshi_decisions(
    kalshi_client: Any, store: Any, matches: list[dict[str, Any]],
    now_ts: int, theta: float,
) -> tuple[list[dict[str, Any]], int, int]:
    """Apply the PM skilled-OFI signal to the paired Kalshi book, per matched
    event. Returns (rows, n_candidates, n_buy)."""
    if not matches:
        return [], 0, 0
    cids = sorted({r["pm_condition_id"] for r in matches})
    tickers = sorted({r["kalshi_ticker"] for r in matches})

    ph = ",".join("?" for _ in cids)
    sig_by_cid = {
        row["venue_market_id"]: dict(row)
        for row in store.conn.execute(_KALSHI_SIGNAL_SQL.format(ph=ph), cids)
    }
    ph_t = ",".join("?" for _ in tickers)
    km_by_ticker = {
        row["venue_market_id"]: dict(row)
        for row in store.conn.execute(
            f"SELECT venue_market_id, status_norm, close_ts FROM markets "
            f"WHERE venue_id='kalshi' AND venue_market_id IN ({ph_t})",
            tickers,
        )
    }

    candidates: list[dict[str, Any]] = []
    for r in matches:
        sig = sig_by_cid.get(r["pm_condition_id"])
        km = km_by_ticker.get(r["kalshi_ticker"])
        if sig is None or km is None or km["status_norm"] != "open":
            continue
        alpha = ofi_alpha_for(VENUE_ID, sig["category"])  # PM signal's alpha
        ofi_age = now_ts - (sig["ofi_ts"] or 0)
        n_sk = sig["n_skilled_trades"] or 0
        ttc_ok = km["close_ts"] is not None and (
            km["close_ts"] - now_ts >= MIN_TIME_TO_CLOSE_S
        )
        if not (
            alpha > 0.0 and ofi_age <= OFI_MAX_AGE_S
            and n_sk >= MIN_SKILLED_TRADES and ttc_ok
        ):
            continue
        candidates.append({
            **r, "sig": sig, "alpha": alpha, "ofi_age": ofi_age,
            "k_close_ts": km["close_ts"],
        })
    candidates.sort(
        key=lambda c: abs(Decimal(c["sig"]["skilled_ofi"])), reverse=True
    )
    candidates = candidates[:MAX_KALSHI_REQUOTES]

    rows: list[dict[str, Any]] = []
    n_buy = 0
    for c in candidates:
        sig = c["sig"]
        try:
            book = _kalshi_best_quote(
                await kalshi_client.get_orderbook(c["kalshi_ticker"])
            )
        except Exception:
            logger.exception(
                "kalshi requote failed for %s (recording quote-absent PASS)",
                c["kalshi_ticker"],
            )
            book = {"bid": None, "ask": None, "bid_size": None, "ask_size": None}
        # Primary-aligned: pm_token_id == primary, so the PM OFI already points
        # at the Kalshi YES side — no sign flip.
        d = _price_decision(
            KALSHI_VENUE_ID, sig["category"], sig["skilled_ofi"], c["alpha"], book
        )
        gates = _gates(c["alpha"], c["ofi_age"], sig["n_skilled_trades"], d)
        passed = all(gates.values())
        buy = (
            passed and d["ev"] is not None
            and not math.isinf(theta) and float(d["ev"]) >= theta
        )
        n_buy += int(buy)
        rows.append(_assemble_row(
            venue_id=KALSHI_VENUE_ID, venue_market_id=c["kalshi_ticker"],
            venue_token_id=None, now_ts=now_ts, theta=theta, d=d, book=book,
            ofi=sig["skilled_ofi"], n_sk=sig["n_skilled_trades"],
            skilled_vol=sig["skilled_volume"], ofi_age=c["ofi_age"],
            alpha=c["alpha"], category=sig["category"], close_ts=c["k_close_ts"],
            event_key=c["event_key"], signal_venue_id=VENUE_ID,
            signal_market_id=c["pm_condition_id"], gates=gates, passed=passed,
            buy=buy,
        ))
    return rows, len(candidates), n_buy


async def decide_once(
    client: Any, store: Any, kalshi_client: Any | None = None,
    data_dir: Any | None = None, now_ts: int | None = None,
) -> dict[str, int]:
    """One shadow-decision pass: gate → requote → EV → append to ledger.

    PM markets are scored on their own requoted CLOB books. When ``kalshi_client``
    and ``data_dir`` are supplied, the SAME skilled-OFI signal is additionally
    applied to the paired Kalshi book for every confirmed cross-venue match
    (the dual-venue ledger: which venue executes the signal better after each
    one's fees + spread). Both legs of a matched event share an ``event_key``.
    Record-only unless PMKT_DECIDE_THETA is set; never places orders (data invariants 1).
    """
    now_ts = int(time.time()) if now_ts is None else now_ts
    theta = _theta()
    if data_dir is None:
        from ..config import Config

        data_dir = Config().data_dir

    # Confirmed cross-venue matches: attach event_key to the PM leg + drive the
    # Kalshi leg. Read once (a stale/missing crosswalk just means PM-only).
    matches: list[dict[str, Any]] = []
    try:
        matches = _confirmed_primary_matches(data_dir)
    except Exception:
        logger.exception("crosswalk read failed — PM-only this pass")
    pm_event_key = {r["pm_condition_id"]: r["event_key"] for r in matches}

    candidates = []
    for row in store.conn.execute(_CANDIDATES_SQL, (VENUE_ID,)):
        alpha = ofi_alpha_for(VENUE_ID, row["category"])
        ofi_age = now_ts - (row["ofi_ts"] or 0)
        n_sk = row["n_skilled_trades"] or 0
        ttc_ok = row["close_ts"] is not None and (
            row["close_ts"] - now_ts >= MIN_TIME_TO_CLOSE_S
        )
        # Pre-quote gates: don't spend books-request budget on markets that
        # can never pass. These gates are re-recorded in gates_json below.
        if not (
            alpha > 0.0
            and ofi_age <= OFI_MAX_AGE_S
            and n_sk >= MIN_SKILLED_TRADES
            and ttc_ok
            and row["venue_token_id"]
        ):
            continue
        candidates.append(dict(row) | {"alpha": alpha, "ofi_age": ofi_age})

    candidates.sort(
        key=lambda c: abs(Decimal(c["skilled_ofi"])), reverse=True
    )
    n_prequote = len(candidates)
    candidates = candidates[:MAX_CANDIDATES]

    books: dict[str, dict[str, Any]] = {}
    for i in range(0, len(candidates), BOOKS_BATCH):
        chunk = [c["venue_token_id"] for c in candidates[i:i + BOOKS_BATCH]]
        books.update(await client.get_books(chunk))

    pm_rows: list[dict[str, Any]] = []
    n_buy = 0
    for c in candidates:
        book = books.get(c["venue_token_id"], {})
        d = _price_decision(VENUE_ID, c["category"], c["skilled_ofi"], c["alpha"], book)
        gates = _gates(c["alpha"], c["ofi_age"], c["n_skilled_trades"], d)
        passed = all(gates.values())
        buy = (
            passed and d["ev"] is not None
            and not math.isinf(theta) and float(d["ev"]) >= theta
        )
        n_buy += int(buy)
        pm_rows.append(_assemble_row(
            venue_id=VENUE_ID, venue_market_id=c["venue_market_id"],
            venue_token_id=c["venue_token_id"], now_ts=now_ts, theta=theta,
            d=d, book=book, ofi=c["skilled_ofi"], n_sk=c["n_skilled_trades"],
            skilled_vol=c["skilled_volume"], ofi_age=c["ofi_age"],
            alpha=c["alpha"], category=c["category"], close_ts=c["close_ts"],
            event_key=pm_event_key.get(c["venue_market_id"]),
            signal_venue_id=VENUE_ID,
            signal_market_id=c["venue_market_id"], gates=gates, passed=passed,
            buy=buy,
        ))

    kalshi_rows: list[dict[str, Any]] = []
    k_candidates = k_buy = 0
    if kalshi_client is not None:
        kalshi_rows, k_candidates, k_buy = await _kalshi_decisions(
            kalshi_client, store, matches, now_ts, theta
        )

    all_rows = pm_rows + kalshi_rows
    n_inserted = store.insert_decisions(all_rows, now_ts)
    if pm_rows:
        # Daily PM/OFI forecast snapshot for pmkt model grade (decay series).
        # PM-only: the snapshot joins pm_resolutions on the primary token id,
        # which Kalshi rows don't carry.
        try:
            write_pm_snapshot(data_dir, pm_rows, now_ts)
        except Exception:
            logger.exception("pm snapshot failed (decisions already recorded)")
    logger.info(
        "decide: PM %d candidates (%d pre-quote) %d passed %d BUY; "
        "Kalshi %d candidates %d BUY; %d recorded (theta=%s)",
        len(candidates), n_prequote, sum(r["passed_gates"] for r in pm_rows),
        n_buy, k_candidates, k_buy, n_inserted,
        "inf" if math.isinf(theta) else f"{theta:.4f}",
    )
    result = {
        "candidates": len(candidates),
        "recorded": n_inserted,
        "passed_gates": sum(r["passed_gates"] for r in pm_rows),
        "buys": n_buy,
    }
    if kalshi_client is not None:
        result |= {
            "kalshi_candidates": k_candidates,
            "kalshi_passed_gates": sum(r["passed_gates"] for r in kalshi_rows),
            "kalshi_buys": k_buy,
        }
    return result


async def decide(
    client: Any, store: Any, kalshi_client: Any | None = None
) -> dict[str, int]:
    """Run-loop job (15 min, right after ofi_score). ``client`` is the PM
    client; ``kalshi_client`` (passed by the loop) enables the dual-venue pass."""
    return await decide_once(client, store, kalshi_client=kalshi_client)


async def decide_grade(client: Any, store: Any) -> dict[str, Any]:
    """Run-loop job (hourly): grade resolved ledger rows. Light — pending
    rows via the daemon's own sqlite conn, resolutions via a DuckDB
    parquet-only read (no hot.sqlite attach in-daemon — see grade_decisions);
    safe in-daemon. client unused."""
    from ..config import Config

    return grade_decisions(Config().data_dir, store)


RESOLUTIONS_REFRESH_TIMEOUT_S = 1800
CROSSWALK_REFRESH_TIMEOUT_S = 900


async def _run_lake_builder(action: str, timeout_s: int, label: str) -> str:
    """Run `pmkt lake <action>` as a SUBPROCESS. Analytical builders get their
    own memory ceiling and a failure costs one cycle, never the collector; a
    subprocess also sidesteps the in-daemon double-SQLite lock hazard (the
    46h "malformed" incident). Returns the stdout summary; raises on failure."""
    import asyncio
    import sys
    from pathlib import Path

    pmkt_bin = Path(sys.executable).parent / "pmkt"
    if not pmkt_bin.exists():
        raise RuntimeError(f"{label} subprocess: {pmkt_bin} does not exist")
    proc = await asyncio.create_subprocess_exec(
        str(pmkt_bin), "lake", action,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=timeout_s
        )
    except TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(
            f"{label} subprocess timed out after {timeout_s}s"
        ) from None
    except asyncio.CancelledError:
        proc.kill()
        raise
    if proc.returncode != 0:
        tail = stderr.decode(errors="replace")[-400:]
        raise RuntimeError(f"{label} subprocess exited {proc.returncode}: {tail}")
    return stdout.decode(errors="replace").strip()


async def resolutions_refresh(client: Any, store: Any) -> dict[str, int]:
    """Run-loop job (6h): rebuild the pm_resolutions projection so newly
    resolved markets become gradeable. SUBPROCESS. client/store unused."""
    summary = await _run_lake_builder(
        "build-pm-resolutions", RESOLUTIONS_REFRESH_TIMEOUT_S, "resolutions"
    )
    logger.info(
        "resolutions refresh ok: %s", " ".join(summary.split())[-200:] or "(no output)"
    )
    return {}


async def crosswalk_refresh(client: Any, store: Any) -> dict[str, int]:
    """Run-loop job (6h): rebuild the venue_crosswalk projection so newly
    posted matched events (and post-settlement QA) stay current before games
    close. SUBPROCESS (`pmkt lake build-crosswalk`, reads hot RO). Sports
    schedules turn over daily → matches must be ready before close.
    client/store unused."""
    summary = await _run_lake_builder(
        "build-crosswalk", CROSSWALK_REFRESH_TIMEOUT_S, "crosswalk"
    )
    logger.info(
        "crosswalk refresh ok: %s", " ".join(summary.split())[-200:] or "(no output)"
    )
    return {}


# ── Grading — decisions × resolved outcomes → shadow track record ─────

PM_SNAPSHOT_MODEL_VERSION = "pm-ofi-v2"  # v2 = corrected per-category alphas


def _pm_resolutions_path(data_dir: Any) -> "Path":
    from pathlib import Path

    return Path(data_dir) / "curated" / "pm_resolutions" / "part-0.parquet"


def grade_decisions(
    data_dir: Any, store: Any, now_ts: int | None = None
) -> dict[str, Any]:
    """Grade every ungraded gate-passing decision whose market has resolved.

    Outcomes come from the curated pm_resolutions projection (hot-store
    `result` is NULL for Polymarket). Per decision: the INDICATED side
    (model_p vs fresh mid at decision time — PASS rows carry it implicitly,
    so any hypothetical θ can be evaluated retroactively), post-fee return
    per dollar at the frozen quote, and the realized-vs-modeled gap
    (won − model_p; its mean is the calibration error of the shadow model).
    Idempotent: each decision grades exactly once. Grading output is not
    trading authorization (data invariants)."""
    import time as _time

    from .. import duck

    now_ts = int(_time.time()) if now_ts is None else now_ts
    res_path = _pm_resolutions_path(data_dir)

    # Pending rows (both venues) come from the daemon's own sqlite connection;
    # DuckDB only ever reads parquet. NEVER attach hot.sqlite through DuckDB
    # inside the daemon: DuckDB's bundled SQLite closing the file destroys the
    # daemon connection's POSIX advisory locks (SQLite "How To Corrupt" — two
    # SQLite copies in one process), which let external writers run WAL
    # recovery underneath it — root cause of the 2026-07-17→19 spurious
    # "database disk image is malformed" incident. Kalshi resolutions read
    # from hot `markets.result` via store.conn for the same reason.
    pending_rows = store.conn.execute(
        """SELECT decision_id, venue_id, model_p, base_prob, bid_prob, ask_prob,
                  fee_rate, ev, venue_token_id, ts, skilled_ofi, category,
                  venue_market_id
           FROM decisions
           WHERE passed_gates = 1
             AND decision_id NOT IN (SELECT decision_id FROM decision_grades)"""
    ).fetchall()
    pm_pending = [r for r in pending_rows if r["venue_id"] == VENUE_ID]
    kalshi_pending = [r for r in pending_rows if r["venue_id"] == KALSHI_VENUE_ID]

    # Missing pm_resolutions only blocks the PM side; Kalshi rows still grade.
    # Preserve the historical single-venue message when nothing else can grade.
    if not res_path.exists() and not kalshi_pending:
        logger.warning(
            "decide grade: pm_resolutions missing — run "
            "`pmkt lake build-pm-resolutions` first"
        )
        return {"graded": 0, "note": "pm_resolutions missing"}

    # Polymarket outcomes: winning clobTokenId per conditionId (parquet-only).
    pm_win: dict[str, str] = {}
    pm_ids = sorted({r["venue_market_id"] for r in pm_pending})
    if pm_ids and res_path.exists():
        conn = duck.connect(data_dir)
        try:
            ph = ",".join("?" for _ in pm_ids)
            pm_win = dict(conn.execute(
                f"""SELECT venue_market_id, winning_token_id
                    FROM read_parquet('{res_path}')
                    WHERE venue_market_id IN ({ph})""",
                pm_ids,
            ).fetchall())
        finally:
            conn.close()

    # Kalshi outcomes: settled result yes/no from the hot store (store.conn RO).
    kalshi_win: dict[str, str] = {}
    kalshi_ids = sorted({r["venue_market_id"] for r in kalshi_pending})
    if kalshi_ids:
        ph = ",".join("?" for _ in kalshi_ids)
        kalshi_win = {
            row["venue_market_id"]: row["result"]
            for row in store.conn.execute(
                f"""SELECT venue_market_id, result FROM markets
                    WHERE venue_id = 'kalshi' AND result IN ('yes','no')
                      AND venue_market_id IN ({ph})""",
                kalshi_ids,
            )
        }

    # Unify: a decision is gradeable once the resolution indicates whether its YES side won.
    pending: list[tuple[Any, bool]] = []
    for r in pending_rows:
        if r["venue_id"] == KALSHI_VENUE_ID:
            res = kalshi_win.get(r["venue_market_id"])
            if res is None:
                continue
            won_yes = res == "yes"
        else:
            win_tok = pm_win.get(r["venue_market_id"])
            if win_tok is None:
                continue
            won_yes = win_tok == r["venue_token_id"]
        pending.append((r, won_yes))

    grades: list[dict[str, Any]] = []
    graded_full: list[dict[str, Any]] = []
    for r, won_yes in pending:
        ev = r["ev"]
        model_p_d, base_d = Decimal(r["model_p"]), Decimal(r["base_prob"])
        bid_d, ask_d = Decimal(r["bid_prob"]), Decimal(r["ask_prob"])
        rate = Decimal(r["fee_rate"])
        if model_p_d >= base_d:
            side, px, won = "BUY_YES", ask_d, won_yes
            fee = rate * ask_d * (1 - ask_d)
        else:
            side, px, won = "BUY_NO", 1 - bid_d, not won_yes
            fee = rate * bid_d * (1 - bid_d)
        if px <= 0:
            continue
        won_d = Decimal(1 if won else 0)
        realized = won_d - px - fee            # per share
        ret = realized / px                    # per dollar staked
        # realized − modeled EV per share; algebraically = won − p_side where
        # p_side is the model's probability of the SIDE held (model_p for
        # YES, 1−model_p for NO). Mean over grades = shadow calibration error.
        ev_gap = (
            realized - Decimal(ev) if ev is not None
            else won_d - (model_p_d if side == "BUY_YES" else 1 - model_p_d)
        )
        grades.append({
            "decision_id": r["decision_id"],
            "side": side,
            "won": int(won),
            "entry_prob": _fmt(px),
            "ret_per_dollar": _fmt(ret),
            "ev_gap": _fmt(ev_gap),
        })
        graded_full.append({
            "ts": r["ts"], "ret": float(ret), "won": int(won),
            "ofi": float(r["skilled_ofi"]),
            # resid is PRIMARY/YES-oriented (won_yes − base) to match skilled_ofi's
            # YES sign and the backtest's corr(ofi, y − p). Using the SIDE outcome
            # (won_d = `not won_yes` for BUY_NO) negated it on every BUY_NO row and
            # corrupted the ofi_outcome_resid_corr decay metric below.
            "resid": float((Decimal(1) if won_yes else Decimal(0)) - base_d),
            "category": r["category"],
            "ev": float(ev) if ev is not None else None,
            "realized_share": float(realized),
        })

    n_new = store.insert_decision_grades(grades, now_ts)

    summary: dict[str, Any] = {"graded": n_new, "pending_resolution": None}
    if graded_full:
        n = len(graded_full)
        mean_ret = sum(g["ret"] for g in graded_full) / n
        win_rate = sum(g["won"] for g in graded_full) / n
        gaps = [
            g["realized_share"] - g["ev"]
            for g in graded_full if g["ev"] is not None
        ]
        # OFI decay stat: corr(skilled_ofi, y − base) over this grade batch —
        # the forward continuation of the backtest residual correlation.
        ofis = [g["ofi"] for g in graded_full]
        resids = [g["resid"] for g in graded_full]
        mo, mr = sum(ofis) / n, sum(resids) / n
        cov = sum((o - mo) * (r - mr) for o, r in zip(ofis, resids))
        vo = sum((o - mo) ** 2 for o in ofis)
        vr = sum((r - mr) ** 2 for r in resids)
        corr = cov / (vo * vr) ** 0.5 if vo > 0 and vr > 0 else None
        summary.update({
            "n_graded_batch": n,
            "mean_ret_per_dollar": round(mean_ret, 4),
            "win_rate": round(win_rate, 4),
            "mean_ev_gap": round(sum(gaps) / len(gaps), 4) if gaps else None,
            "ofi_outcome_resid_corr": round(corr, 4) if corr is not None else None,
        })
    logger.info("decide grade: %s", summary)
    return summary


def write_pm_snapshot(data_dir: Any, rows: list[dict[str, Any]], now_ts: int):
    """Daily parquet snapshot of the PM/OFI shadow forecasts (first decide
    pass of the UTC day), mirroring model.score.write_snapshot but in its
    own tree (curated/forecasts_pm) with the primary token id included so
    grading can join pm_resolutions on winning_token_id."""
    import time as _time
    from pathlib import Path

    from .. import duck

    if not rows:
        return None
    date = _time.strftime("%Y-%m-%d", _time.gmtime(now_ts))
    path = Path(data_dir) / "curated" / "forecasts_pm" / f"date={date}" / "part-0.parquet"
    if path.exists():
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / ".part-0.tmp.parquet"
    conn = duck.connect(Path(data_dir))
    try:
        conn.execute(
            """CREATE TEMP TABLE snap (
                   venue_id VARCHAR, venue_market_id VARCHAR,
                   venue_token_id VARCHAR, model_version VARCHAR,
                   model_p VARCHAR, base_price VARCHAR, category VARCHAR,
                   created_ts BIGINT)"""
        )
        conn.executemany(
            "INSERT INTO snap VALUES (?,?,?,?,?,?,?,?)",
            [
                (
                    VENUE_ID, r["venue_market_id"], r["venue_token_id"],
                    PM_SNAPSHOT_MODEL_VERSION, r["model_p"], r["base_prob"],
                    r["category"], now_ts,
                )
                for r in rows
                if r["model_p"] is not None and r["base_prob"] is not None
            ],
        )
        conn.execute(
            f"COPY (SELECT * FROM snap ORDER BY venue_market_id) TO '{tmp}' "
            "(FORMAT parquet, COMPRESSION zstd)"
        )
    finally:
        conn.close()
    tmp.replace(path)
    logger.info("pm forecast snapshot: %d rows -> %s", len(rows), path)
    return path
