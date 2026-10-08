"""Bounded offline research example using synthetic wallet, flow and quote data.

The example reuses wallet-scoring SQL, signed flow aggregation, quote pricing,
paper-ledger storage and outcome grading. Synthetic results illustrate data
boundaries and costs; they provide no evidence of profitable wallet following.
"""

from __future__ import annotations

import json
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from .lake import _WALLET_SKILLS_BUCKET_SQL
from .model.decide import _assemble_row, _gates, _price_decision, grade_decisions
from .model.ofi import _OFI_AGG_SQL, _agg_params
from .state import HotStore

DEMO_SCHEMA_VERSION = 1
QUOTE_MAX_AGE_S = 60
MAX_HISTORY_ROWS = 100
MAX_FLOW_ROWS = 100
MAX_DEMO_BYTES = 5_000_000


def _rows(cursor) -> list[dict[str, Any]]:
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _jsonable(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def synthetic_history(selection_ts: int) -> list[dict[str, Any]]:
    """Include future labels and future fills to exercise cutoff exclusions."""
    history = []
    for wallet, wins in (("wallet-alpha", 7), ("wallet-balanced", 4), ("wallet-future", 8)):
        for index in range(8):
            history.append({
                "taker": wallet, "maker": "liquidity-provider", "taker_direction": "BUY",
                "usdc_amount": "10", "price": "0.40" if wallet != "wallet-balanced" else "0.50",
                "outcome_label": "Yes", "winning_outcome_label": "Yes" if index < wins else "No",
                "condition_id": f"history-{wallet}-{index}",
                "trade_ts": selection_ts - 86400 - index * 3600,
                "resolved_at": selection_ts + 3600 if wallet == "wallet-future" else selection_ts - 1800,
                "label_available_ts": selection_ts + 3660 if wallet == "wallet-future" else selection_ts - 1200,
            })
    history.append({
        **history[0], "taker": "wallet-future-fill", "condition_id": "history-future-fill",
        "trade_ts": selection_ts + 1, "resolved_at": selection_ts + 2,
        "label_available_ts": selection_ts + 3,
    })
    return history


def discover_wallets(conn, history: list[dict[str, Any]], selection_ts: int) -> tuple[list[dict], int]:
    """Score only fills whose outcome was observed before wallet selection."""
    if len(history) > MAX_HISTORY_ROWS:
        raise ValueError("demo history exceeds its fixed row budget")
    conn.execute("""CREATE TABLE pm_v1_trades (
        taker VARCHAR, maker VARCHAR, taker_direction VARCHAR, usdc_amount DOUBLE,
        price DOUBLE, outcome_label VARCHAR, winning_outcome_label VARCHAR, condition_id VARCHAR
    )""")
    eligible = [row for row in history if (
        row["trade_ts"] < selection_ts and row["resolved_at"] < selection_ts
        and row["label_available_ts"] < selection_ts
        and row["trade_ts"] <= row["resolved_at"] <= row["label_available_ts"]
    )]
    if eligible:
        conn.executemany("INSERT INTO pm_v1_trades VALUES (?,?,?,?,?,?,?,?)", [
            tuple(row[key] for key in (
                "taker", "maker", "taker_direction", "usdc_amount", "price",
                "outcome_label", "winning_outcome_label", "condition_id",
            )) for row in eligible
        ])
    sql = _WALLET_SKILLS_BUCKET_SQL.format(
        buckets=1, bucket=0, version=3, min_trades=8, v2_union="",
    )
    wallets = _rows(conn.execute(sql + " ORDER BY z_score DESC, wallet"))
    for row in wallets:
        row["z_score"] = format(row["z_score"], ".6f")
        row["selected"] = Decimal(row["z_score"]) > Decimal("1.96") and row["n_markets"] >= 8
        for field in ("total_volume", "actual_pnl", "sigma"):
            row[field] = format(row[field], ".6f")
    return wallets, len(eligible)


def selected_flow(conn, trades: list[dict], wallets: list[dict], instruments: list[tuple],
                  selection_ts: int, decision_ts: int) -> list[dict]:
    """Aggregate selected-wallet fills observed before the decision, on a YES axis."""
    if len(trades) > MAX_FLOW_ROWS:
        raise ValueError("demo flow exceeds its fixed row budget")
    conn.execute("CREATE SCHEMA hot")
    conn.execute("CREATE TABLE hot.instruments (venue_id VARCHAR, venue_token_id VARCHAR, is_primary INTEGER)")
    conn.executemany("INSERT INTO hot.instruments VALUES (?,?,?)", instruments)
    conn.execute("CREATE TABLE wallet_skills (wallet VARCHAR, z_score DOUBLE)")
    selected = [(row["wallet"], row["z_score"]) for row in wallets if row["selected"]]
    if selected:
        conn.executemany("INSERT INTO wallet_skills VALUES (?,?)", selected)
    conn.execute("""CREATE TABLE ofi_win (venue_market_id VARCHAR, proxy_wallet VARCHAR,
        size DECIMAL(18,6), taker_side VARCHAR, venue_token_id VARCHAR, ts BIGINT)""")
    eligible = [row for row in trades if (
        selection_ts <= row["ts"] <= row["observed_ts"] <= decision_ts
        and row["side"] in ("BUY", "SELL") and Decimal(row["size"]) > 0
    )]
    if eligible:
        conn.executemany("INSERT INTO ofi_win VALUES (?,?,?,?,?,?)", [
            (row["market_id"], row["wallet"], row["size"], row["side"], row["token_id"], row["ts"])
            for row in eligible
        ])
    flows = _rows(conn.execute(_OFI_AGG_SQL, _agg_params(selection_ts)))
    for row in flows:
        for name in ("skilled_ofi", "unskilled_ofi", "anti_ofi", "z_weighted_ofi"):
            if row[name] is not None:
                row[name] = format(Decimal(str(row[name])).quantize(Decimal("0.000001")), "f")
    return _jsonable(flows)


def follow_decision(flow: dict, quote: dict, *, decision_ts: int,
                    close_ts: int, observed_ts: int, leader_price: str) -> dict:
    """Record a paper decision at the follower quote, with explicit time ordering."""
    ofi = flow["skilled_ofi"]
    alpha = 0.07  # illustrative synthetic sensitivity, not a fitted or certified model
    book = {key: Decimal(quote[key]) if quote.get(key) is not None else None
            for key in ("bid", "ask", "bid_size", "ask_size")}
    priced = _price_decision("polymarket", "Sports", ofi or "0", alpha, book)
    gates = _gates(alpha, decision_ts - observed_ts, flow["n_skilled_trades"], priced)
    gates.update({
        "time_to_close": close_ts - decision_ts >= 3600,
        "observation_before_quote": observed_ts <= quote["ts"] <= decision_ts,
        "quote_fresh": 0 <= decision_ts - quote["ts"] <= QUOTE_MAX_AGE_S,
        "valid_book": book["bid"] is not None and book["ask"] is not None
            and Decimal(0) < book["bid"] <= book["ask"] < Decimal(1),
        "size_available": book["ask_size"] is not None and book["bid_size"] is not None
            and min(book["ask_size"], book["bid_size"]) >= 1,
    })
    passed = all(gates.values())
    row = _assemble_row(
        venue_id="polymarket", venue_market_id=flow["venue_market_id"],
        venue_token_id=quote["token_id"], now_ts=decision_ts, theta=0.0,
        d=priced, book=book, ofi=ofi, n_sk=flow["n_skilled_trades"],
        skilled_vol=flow["skilled_volume"], ofi_age=decision_ts - observed_ts,
        alpha=alpha, category="Sports", close_ts=close_ts, event_key=None,
        signal_venue_id="polymarket", signal_market_id=flow["venue_market_id"],
        gates=gates, passed=passed,
        buy=passed and priced["ev"] is not None and priced["ev"] > 0,
    )
    return {**row, "side": priced["side"], "gates": gates, "quote_ts": quote["ts"],
            "observed_ts": observed_ts, "leader_price": leader_price,
            "quote_source": "synthetic", "paper_only": True}


def run_demo(output: Path | None = None, *, as_of_ts: int | None = None) -> dict:
    """Create a new small workspace. Existing paths are never overwritten."""
    now = int(time.time()) if as_of_ts is None else as_of_ts
    if now < 172800:
        raise ValueError("demo timestamp must allow at least two days of prior history")
    if output is None:
        root = Path(tempfile.mkdtemp(prefix="pmkt-demo-"))
    else:
        root = Path(output).expanduser().absolute()
        root.mkdir(parents=True, exist_ok=False)
    selection_ts = now - 600
    history = synthetic_history(selection_ts)
    conn = duckdb.connect(config={
        "memory_limit": "128MB", "threads": "1", "max_temp_directory_size": "16MB",
        "autoinstall_known_extensions": "false", "autoload_known_extensions": "false",
    })
    store = None
    try:
        wallets, eligible_count = discover_wallets(conn, history, selection_ts)
        markets = [("demo-follow-yes", "1001", "1002"),
                   ("demo-follow-no", "2001", "2002"),
                   ("demo-stale-quote", "3001", "3002")]
        instruments = [("polymarket", token, primary) for _, yes, no in markets
                       for token, primary in ((yes, 1), (no, 0))]
        trades = []
        for market, yes, no in markets:
            for index in range(3):
                age = 240 - index * 20 if market == "demo-stale-quote" else 120 - index * 20
                trades.append({"market_id": market, "wallet": "wallet-alpha", "size": "10",
                               "side": "BUY", "token_id": no if market == "demo-follow-no" else yes,
                               "ts": now - age, "observed_ts": now - age + 2, "price": "0.40"})
        flows = selected_flow(conn, trades, wallets, instruments, selection_ts, now)
        store = HotStore(root / "db" / "hot.sqlite")
        store.upsert_events("polymarket", [{"venue_event_id": "demo-event", "title": "Synthetic research examples",
                            "category": "Sports", "status": "open"}], now)
        close_ts = now + 7200
        decisions = []
        for market, yes, no in markets:
            store.upsert_markets("polymarket", [{
                "venue_market_id": market, "venue_event_id": "demo-event",
                "title": f"[SIMULATED] {market.replace('demo-', '').replace('-', ' ')}",
                "status_norm": "open", "market_type": "binary", "close_ts": close_ts,
                "yes_bid_prob": "0.48", "yes_ask_prob": "0.52", "last_prob": "0.50",
                "volume": "30", "volume_24h": "30", "liquidity": "100",
            }], now)
            store.upsert_instruments("polymarket", [{
                "venue_market_id": market, "venue_token_id": token, "outcome_index": index,
                "outcome_name": label, "is_primary": index == 0,
            } for index, (token, label) in enumerate(((yes, "Yes"), (no, "No")))], now)
            flow = next(row for row in flows if row["venue_market_id"] == market)
            observed = max(row["observed_ts"] for row in trades if row["market_id"] == market)
            quote = {"bid": "0.48", "ask": "0.52", "bid_size": "10", "ask_size": "10",
                     "ts": now - (90 if market == "demo-stale-quote" else 5), "token_id": yes}
            decisions.append(follow_decision(flow, quote, decision_ts=now,
                                            close_ts=close_ts, observed_ts=observed, leader_price="0.40"))
        store.upsert_skilled_ofi("polymarket", flows, now)
        store.insert_decisions(decisions, now)
        store.set_state("polymarket", "synthetic_demo", watermark_ts=now, now_ts=now)
        # Outcomes are introduced only after all selection, flow and decision steps.
        resolutions = root / "curated" / "pm_resolutions"
        resolutions.mkdir(parents=True)
        conn.execute("CREATE TABLE resolutions (venue_market_id VARCHAR, winning_token_id VARCHAR)")
        conn.executemany("INSERT INTO resolutions VALUES (?,?)", [(market, yes) for market, yes, _ in markets])
        escaped = str(resolutions / "part-0.parquet").replace("'", "''")
        conn.execute(f"COPY resolutions TO '{escaped}' (FORMAT parquet)")
        grading = grade_decisions(root, store, now_ts=close_ts + 60)
        grades = [dict(row) for row in store.conn.execute("SELECT * FROM decision_grades ORDER BY decision_id")]
        report = _jsonable({
            "schema_version": DEMO_SCHEMA_VERSION, "data_mode": "synthetic", "paper_only": True,
            "as_of_ts": now, "skill_cutoff_ts": selection_ts,
            "selection_rule": "At least 8 prior resolved markets/trades and experimental z > 1.96; fills, resolution and label availability all precede selection.",
            "wallets": wallets, "selected_wallets": [row["wallet"] for row in wallets if row["selected"]],
            "flows": flows, "decisions": decisions, "grading": grading, "grades": grades,
            "evidence": {"history_rows": len(history), "eligible_history_rows": eligible_count,
                         "flow_rows": len(trades), "resolution_available_ts": close_ts + 60},
            "network_requests": 0, "live_orders": 0,
            "limitations": [
                "All wallets, fills, quotes and outcomes are synthetic; results are illustrative, not a profitable track record.",
                "The wallet z score and flow sensitivity are experimental; market dependence, selection bias and multiple testing remain unresolved.",
                "Follower prices use a later bid/ask quote, not the leader's fill; fees use an illustrative schedule estimate.",
                "Paper fills assume one share available at the quoted touch; no queue, latency, partial-fill or current per-market fee certification is modeled.",
                "A stale quote is rejected. Outcomes become available only after selection and decisions; the example includes both a win and a loss.",
            ],
        })
        (root / "demo-report.json").write_text(json.dumps(report, indent=2) + "\n")
        (root / "synthetic-history.json").write_text(json.dumps(history, indent=2) + "\n")
        (root / "synthetic-flow.json").write_text(json.dumps(trades, indent=2) + "\n")
    finally:
        if store is not None:
            store.close()
        conn.close()
    size = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
    if size > MAX_DEMO_BYTES:
        raise RuntimeError("demo exceeded its fixed output budget")
    return {"data_mode": "synthetic", "workspace": str(root), "report": str(root / "demo-report.json"),
            "selected_wallets": report["selected_wallets"], "decisions": len(decisions),
            "paper_entries": sum(row["action"] != "PASS" for row in decisions),
            "graded": grading["graded"], "output_bytes": size, "network_requests": 0, "live_orders": 0}
