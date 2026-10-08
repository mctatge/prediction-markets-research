"""Decision grader: resolved outcomes → post-fee shadow PnL, side
recomputation for PASS rows, idempotence, and the PM forecast snapshot."""

import time
from decimal import Decimal

import duckdb

from pmkt.model import decide as decide_mod
from pmkt.model.decide import decide_once, grade_decisions
from pmkt.state import HotStore
from test_decide import NOW, FakePMClient, _book, _seed

GRADE_TS = NOW + 7200


def _write_resolutions(data_dir, rows):
    """rows: (cid, winning_token)"""
    d = data_dir / "curated" / "pm_resolutions"
    d.mkdir(parents=True)
    values = ", ".join(
        f"('polymarket', '{c}', 0, '{tok}', 'Yes', TIMESTAMP '2026-07-16', 1)"
        for (c, tok) in rows
    )
    duckdb.execute(
        f"COPY (SELECT * FROM (VALUES {values}) AS v(venue_id, venue_market_id, "
        "winning_outcome_index, winning_token_id, winning_outcome_label, "
        "source_fetched_at, parser_version)) "
        f"TO '{d / 'part-0.parquet'}' (FORMAT parquet)"
    )


async def test_grade_resolved_decisions(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "db" / "hot.sqlite")
    _seed(store, "0xa", "tokA", ofi="0.8000")   # indicated BUY_YES
    _seed(store, "0xb", "tokB", ofi="-0.8000")  # indicated BUY_NO
    client = FakePMClient({"tokA": _book(), "tokB": _book()})
    await decide_once(client, store)  # 2 PASS rows, both passed_gates

    # tokA's market resolves YES (primary wins) -> BUY_YES won.
    # tokB's market resolves YES too -> BUY_NO lost.
    _write_resolutions(tmp_path, [("0xa", "tokA"), ("0xb", "tokB")])

    r = grade_decisions(tmp_path, store, now_ts=GRADE_TS)
    assert r["graded"] == 2 and r["n_graded_batch"] == 2
    assert r["win_rate"] == 0.5

    rows = {
        row["side"]: row for row in store.conn.execute(
            "SELECT g.*, d.venue_market_id FROM decision_grades g "
            "JOIN decisions d ON d.decision_id = g.decision_id"
        )
    }
    yes = rows["BUY_YES"]
    assert yes["venue_market_id"] == "0xa" and yes["won"] == 1
    # entry at ask 0.52; fee 0.05*0.52*0.48; ret = (1 - px - fee)/px
    px = Decimal("0.52")
    fee = Decimal("0.05") * px * (Decimal(1) - px)
    exp = ((Decimal(1) - px - fee) / px).quantize(Decimal("0.0001"))
    assert yes["ret_per_dollar"] == format(exp, "f")
    no = rows["BUY_NO"]
    assert no["venue_market_id"] == "0xb" and no["won"] == 0
    # mean_ev_gap = mean(won − p_side): (1−0.566) + (0−(1−0.454)) = −0.112/2
    assert abs(r["mean_ev_gap"] - (-0.056)) < 1e-6

    # idempotent: second grade run adds nothing
    r2 = grade_decisions(tmp_path, store, now_ts=GRADE_TS + 60)
    assert r2["graded"] == 0


async def test_grade_skips_unresolved_and_missing_resolutions(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    store = HotStore(tmp_path / "db" / "hot.sqlite")
    _seed(store, "0xa", "tokA")
    client = FakePMClient({"tokA": _book()})
    await decide_once(client, store)

    r = grade_decisions(tmp_path, store, now_ts=GRADE_TS)
    assert r == {"graded": 0, "note": "pm_resolutions missing"}

    _write_resolutions(tmp_path, [("0xother", "tokX")])  # unrelated market
    r2 = grade_decisions(tmp_path, store, now_ts=GRADE_TS)
    assert r2["graded"] == 0


async def test_pm_snapshot_written_once_per_day(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(
        decide_mod, "Config",
        lambda: type("C", (), {"data_dir": tmp_path})(), raising=False,
    )
    # decide_once resolves Config lazily from ..config — patch that module
    import pmkt.config as config_mod

    monkeypatch.setattr(
        config_mod, "Config",
        lambda: type("C", (), {"data_dir": tmp_path})(),
    )
    store = HotStore(tmp_path / "db" / "hot.sqlite")
    _seed(store, "0xa", "tokA")
    client = FakePMClient({"tokA": _book()})

    await decide_once(client, store)
    snap = list((tmp_path / "curated" / "forecasts_pm").glob("date=*/part-0.parquet"))
    assert len(snap) == 1
    row = duckdb.execute(
        f"SELECT venue_id, venue_token_id, model_version, model_p "
        f"FROM read_parquet('{snap[0]}')"
    ).fetchone()
    assert row == ("polymarket", "tokA", "pm-ofi-v2", "0.5660")

    await decide_once(client, store)  # same day -> no second file, no error
    assert len(list(
        (tmp_path / "curated" / "forecasts_pm").glob("date=*/part-0.parquet")
    )) == 1
