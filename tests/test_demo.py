"""Offline research boundaries: prior labels, follower costs, gates and storage."""

import json
import socket
from decimal import Decimal

import duckdb
import httpx
import pytest
from fastapi.testclient import TestClient

from pmkt.cli import main
from pmkt.config import Config
from pmkt.demo import discover_wallets, follow_decision, run_demo, selected_flow, synthetic_history
from pmkt.state import HotStore
from pmkt.ui import create_app

NOW = 1791417600


def test_demo_is_offline_bounded_and_uses_real_ledger(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline demo attempted network access")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(httpx.AsyncClient, "request", forbidden)
    monkeypatch.setattr(httpx.Client, "request", forbidden)
    unused = tmp_path / "unused-data"
    monkeypatch.setenv("PMKT_DATA_DIR", str(unused))
    monkeypatch.setenv("KALSHI_API_KEY_ID", "synthetic-unused-key")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(tmp_path / "missing.pem"))
    root = tmp_path / "demo"
    summary = run_demo(root, as_of_ts=NOW)
    assert not unused.exists()
    assert summary["selected_wallets"] == ["wallet-alpha"]
    assert summary["decisions"] == 3 and summary["paper_entries"] == summary["graded"] == 2
    assert summary["network_requests"] == summary["live_orders"] == 0
    assert 0 < summary["output_bytes"] < 5_000_000
    report = json.loads((root / "demo-report.json").read_text())
    assert report["data_mode"] == "synthetic" and report["paper_only"]
    assert {row["won"] for row in report["grades"]} == {0, 1}
    no = next(row for row in report["grades"] if row["side"] == "BUY_NO")
    assert no["entry_prob"] == "0.5200" and no["won"] == 0
    assert no["ret_per_dollar"] == "-1.0240"
    store = HotStore(root / "db" / "hot.sqlite")
    try:
        assert store.decisions_count() == 3
        assert store.conn.execute("SELECT COUNT(*) FROM decision_grades").fetchone()[0] == 2
    finally:
        store.close()


def test_selection_excludes_future_labels_and_future_fills():
    cutoff = NOW - 600
    history = synthetic_history(cutoff)
    with duckdb.connect() as conn:
        wallets, eligible = discover_wallets(conn, history, cutoff)
    assert eligible == 16
    assert not {"wallet-future", "wallet-future-fill"} & {row["wallet"] for row in wallets}
    selected = [row["wallet"] for row in wallets if row["selected"]]
    assert selected == ["wallet-alpha"]
    # Change only information that was not available at selection time.
    for row in history:
        if row["label_available_ts"] >= cutoff:
            row["winning_outcome_label"] = "No"
    with duckdb.connect() as conn:
        changed, _ = discover_wallets(conn, history, cutoff)
    assert wallets == changed


def test_follower_uses_later_ask_and_fee_not_leader_fill():
    flow = {"venue_market_id": "demo", "skilled_ofi": "1", "n_skilled_trades": 3, "skilled_volume": "30"}
    quote = {"bid": "0.48", "ask": "0.52", "bid_size": "10", "ask_size": "10", "ts": NOW - 5, "token_id": "1001"}
    row = follow_decision(flow, quote, decision_ts=NOW, close_ts=NOW + 7200,
                          observed_ts=NOW - 80, leader_price="0.40")
    assert row["action"] == "BUY_YES" and row["paper_only"]
    assert row["ask_prob"] == "0.5200" and row["leader_price"] == "0.40"
    fee = Decimal("0.05") * Decimal("0.52") * Decimal("0.48")
    expected = Decimal("0.57") - Decimal("0.52") - fee
    assert row["fee_prob"] == format(fee.quantize(Decimal("0.0001")), "f")
    assert row["ev"] == format(expected.quantize(Decimal("0.0001")), "f")
    assert row["observed_ts"] <= row["quote_ts"] <= row["ts"]


@pytest.mark.parametrize("change,failed_gate", [
    ({"ts": NOW - 90}, "quote_fresh"),
    ({"ts": NOW - 100}, "observation_before_quote"),
    ({"ts": NOW + 1}, "observation_before_quote"),
    ({"ask": "0.40"}, "valid_book"),
    ({"ask_size": "0"}, "size_available"),
])
def test_follow_gate_rejects_stale_preobserved_future_or_invalid_quotes(change, failed_gate):
    flow = {"venue_market_id": "demo", "skilled_ofi": "1", "n_skilled_trades": 3, "skilled_volume": "30"}
    quote = {"bid": "0.48", "ask": "0.52", "bid_size": "10", "ask_size": "10", "ts": NOW - 5, "token_id": "1001", **change}
    row = follow_decision(flow, quote, decision_ts=NOW, close_ts=NOW + 7200,
                          observed_ts=NOW - 80, leader_price="0.40")
    assert row["action"] == "PASS" and not row["passed_gates"]
    assert row["gates"][failed_gate] is False


def test_demo_refuses_existing_output(tmp_path):
    root = tmp_path / "existing"
    root.mkdir()
    sentinel = root / "keep.txt"
    sentinel.write_text("preserve")
    with pytest.raises(FileExistsError):
        run_demo(root, as_of_ts=NOW)
    assert sentinel.read_text() == "preserve"
    assert list(root.iterdir()) == [sentinel]


def test_selected_flow_excludes_future_fills_and_unobserved_data():
    base = {"market_id": "test-market", "wallet": "selected", "size": "10", "side": "BUY",
            "token_id": "yes", "ts": NOW - 30, "observed_ts": NOW - 20}
    trades = [base, {**base, "token_id": "no"}, base,
              {**base, "ts": NOW + 1, "observed_ts": NOW + 2},
              {**base, "observed_ts": NOW + 1}, {**base, "wallet": "unselected"}]
    with duckdb.connect() as conn:
        flows = selected_flow(conn, trades, [{"wallet": "selected", "z_score": "2.5", "selected": True}],
                              [("polymarket", "yes", 1), ("polymarket", "no", 0)], NOW - 600, NOW)
    assert flows[0]["n_skilled_trades"] == 3 and flows[0]["n_total_trades"] == 4
    assert flows[0]["skilled_ofi"] == "0.333333"


@pytest.mark.parametrize("args", [["kalshi", "metadata"], ["polymarket", "prices"], ["run"], ["decide"],
                                 ["--allow-network", "run"], ["--allow-network", "kalshi", "ws"]])
def test_live_cli_requires_explicit_opt_in(args, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["pmkt", *args])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    assert not (tmp_path / "data").exists()


def test_empty_api_and_demo_api_read_without_writing(tmp_path):
    missing = tmp_path / "missing"
    with TestClient(create_app(Config(data_dir=missing), ui_dist=missing / "no-dist")) as client:
        meta = client.get("/api/meta").json()
        assert meta["setup_required"] and meta["data_mode"] == "local"
        for endpoint in ("/api/screener", "/api/decisions", "/api/research"):
            assert client.get(endpoint).status_code == 200
    assert not missing.exists()
    root = tmp_path / "demo"
    run_demo(root, as_of_ts=NOW)
    with TestClient(create_app(Config(data_dir=root), ui_dist=root / "no-dist")) as client:
        assert client.get("/api/meta").json()["data_mode"] == "synthetic"
        report = client.get("/api/research").json()
        assert report["selected_wallets"] == ["wallet-alpha"]
        assert len(client.get("/api/screener").json()["rows"]) == 3
        assert client.get("/api/decisions").json()["summary"]["graded"] == 2
