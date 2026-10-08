"""Nightly compaction: raw JSONL day → one Parquet per partition, verified."""

import json
from datetime import datetime, timezone

import duckdb
import pytest

from pmkt.compact import (
    PARQUET_PART_NAME,
    CompactionError,
    _connect,
    _read_lines,
    compact_all_pending,
    compact_day,
)
from pmkt.rawlake import RawLake

# All synthetic captures use fixed past dates; inject "today" so the tests
# stay deterministic forever.
DAY = "2026-07-10"
DAY2 = "2026-07-11"
TODAY = "2026-07-12"

TRICKY_PAYLOAD = {
    "big_int": 2**80,
    "decimal_strings": ["0.4200", "0.123456", "12345678901234.56"],
    "unicode": "café ☃ 中文 — em-dash",
    "nested": {"a": [1, "2.50", None, True, {"deep": "0.0001"}]},
    "float": 0.1,
    "neg": -12345678901234567890,
}


def _append(lake, *, endpoint="trades", hour=3, minute=0, payload=None,
            params=None, status=200, day=DAY, venue="kalshi"):
    y, m, d = (int(x) for x in day.split("-"))
    lake.append(
        venue=venue, endpoint=endpoint, params=params or {"limit": 1000},
        http_status=status, elapsed_ms=42, payload=payload or {"ok": True},
        fetched_at=datetime(y, m, d, hour, minute, 30, 123456, tzinfo=timezone.utc),
    )


def _fill_day(raw_root):
    """Two endpoints, multiple hours, tricky payloads — one complete day."""
    with RawLake(raw_root) as lake:
        for hour in (3, 4, 23):
            _append(lake, endpoint="trades", hour=hour, payload={"trades": [], "h": hour})
        _append(lake, endpoint="markets", hour=5, payload=TRICKY_PAYLOAD)
        _append(lake, endpoint="markets", hour=5, minute=30,
                payload={"markets": [{"ticker": "T-1"}]}, params={"cursor": "c☃"})


def test_compact_day_writes_one_parquet_per_partition(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    _fill_day(raw)
    report = compact_day(raw, out, DAY, today=TODAY)

    assert report["written"] == 2 and report["deferred"] == 0
    assert report["rows"] == 5
    by_ep = {p["endpoint"]: p for p in report["partitions"]}
    assert by_ep["trades"]["rows"] == 3 and by_ep["trades"]["files"] == 3
    assert by_ep["markets"]["rows"] == 2 and by_ep["markets"]["files"] == 1

    trades_pq = out / "venue=kalshi" / "endpoint=trades" / f"date={DAY}" / PARQUET_PART_NAME
    assert trades_pq.exists()
    conn = duckdb.connect()
    rows = conn.execute(
        f"SELECT hour, http_status, elapsed_ms FROM read_parquet('{trades_pq}') ORDER BY hour"
    ).fetchall()
    assert rows == [("03", 200, 42), ("04", 200, 42), ("23", 200, 42)]


def test_session_guardrails_spill_dir_and_memory_limit(tmp_path):
    """Compaction sessions must spill to an owned ABSOLUTE dir under out_root
    (DuckDB's default is cwd-relative ".tmp", shared across connections — it
    vanished mid-spill in the 2026-07-12 incident) and run under an explicit
    memory ceiling instead of the 80%-of-RAM default that OOMed the daemon."""
    conn = _connect(tmp_path)
    try:
        assert (tmp_path / ".duckdb-tmp").is_dir()
        settings = dict(
            conn.execute(
                "SELECT name, value FROM duckdb_settings() WHERE name IN "
                "('temp_directory', 'preserve_insertion_order', 'memory_limit')"
            ).fetchall()
        )
        assert settings["temp_directory"] == str(tmp_path / ".duckdb-tmp")
        assert settings["preserve_insertion_order"] == "false"
        assert settings["memory_limit"] != ""  # explicit, never the RAM default
    finally:
        conn.close()


def test_bit_faithful_payload_and_params_roundtrip(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    _fill_day(raw)
    report = compact_day(raw, out, DAY, today=TODAY)
    # Every source line matched its canonical re-serialization byte-for-byte
    # (loads → dumps identity for lines produced by the writer).
    assert all(p["noncanonical_lines"] == 0 for p in report["partitions"])

    pq = out / "venue=kalshi" / "endpoint=markets" / f"date={DAY}" / PARQUET_PART_NAME
    conn = duckdb.connect()
    payload_txt, params_txt = conn.execute(
        f"SELECT payload, params FROM read_parquet('{pq}') ORDER BY fetched_at LIMIT 1"
    ).fetchone()
    # Byte-exact: the Parquet column IS the canonical serialization…
    assert payload_txt == json.dumps(TRICKY_PAYLOAD, separators=(",", ":"))
    # …and value-exact: big ints, decimal strings, unicode all survive.
    parsed = json.loads(payload_txt)
    assert parsed == TRICKY_PAYLOAD
    assert parsed["big_int"] == 2**80
    assert parsed["decimal_strings"][2] == "12345678901234.56"
    assert json.loads(params_txt) == {"limit": 1000}


def test_full_record_reconstructs_original_line(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    _fill_day(raw)
    compact_day(raw, out, DAY, today=TODAY)

    src = sorted((raw / "venue=kalshi" / "endpoint=markets" / f"date={DAY}").glob(
        "hour=*/part-*.jsonl.zst"
    ))
    original_lines, _ = _read_lines(src[0])
    pq = out / "venue=kalshi" / "endpoint=markets" / f"date={DAY}" / PARQUET_PART_NAME
    conn = duckdb.connect()
    rebuilt = []
    for v, venue, endpoint, params, fetched_at, status, ms, payload, _hour in conn.execute(
        "SELECT v, venue, endpoint, params, fetched_at, http_status, elapsed_ms, "
        f"payload, hour FROM read_parquet('{pq}') ORDER BY fetched_at"
    ).fetchall():
        rebuilt.append(json.dumps(
            {"v": v, "venue": venue, "endpoint": endpoint, "params": json.loads(params),
             "fetched_at": fetched_at, "http_status": status, "elapsed_ms": ms,
             "payload": json.loads(payload)},
            separators=(",", ":"),
        ))
    assert rebuilt == original_lines


def test_tmp_file_defers_partition(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    _fill_day(raw)
    # A crashed (or live) writer left a .tmp in the trades partition —
    # recovery owns it; compaction must defer the partition, not skip the file.
    tmp_file = raw / "venue=kalshi" / "endpoint=trades" / f"date={DAY}" / "hour=23" /\
        "part-zzz.jsonl.zst.tmp"
    tmp_file.write_bytes(b"torn")

    report = compact_day(raw, out, DAY, today=TODAY)
    by_ep = {p["endpoint"]: p for p in report["partitions"]}
    assert by_ep["trades"]["action"] == "deferred-tmp"
    assert not (out / "venue=kalshi" / "endpoint=trades" / f"date={DAY}" /
                PARQUET_PART_NAME).exists()
    assert by_ep["markets"]["action"] == "written"
    assert tmp_file.read_bytes() == b"torn", "raw .tmp never touched"


def test_refuses_incomplete_day(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    _fill_day(raw)
    with pytest.raises(ValueError, match="not a complete UTC day"):
        compact_day(raw, out, TODAY, today=TODAY)
    with pytest.raises(ValueError, match="not a complete UTC day"):
        compact_day(raw, out, "2027-01-01", today=TODAY)
    # And with the real clock: today is never compactable.
    real_today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with pytest.raises(ValueError):
        compact_day(raw, out, real_today)
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        compact_day(raw, out, "07/10/2026", today=TODAY)


def test_idempotent_skip_then_loud_mismatch(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    _fill_day(raw)
    compact_day(raw, out, DAY, today=TODAY)

    rerun = compact_day(raw, out, DAY, today=TODAY)
    assert rerun["written"] == 0 and rerun["skipped"] == 2
    assert rerun["rows"] == 5, "skip re-verifies row counts"

    # Raw grew after compaction (e.g. an orphan recovered late) → loud error,
    # never a silent overwrite.
    with RawLake(raw) as lake:
        _append(lake, endpoint="trades", hour=22, payload={"late": True})
    with pytest.raises(CompactionError, match="raw grew after compaction"):
        compact_day(raw, out, DAY, today=TODAY)


def test_source_files_never_modified(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    _fill_day(raw)
    before = {f: f.read_bytes() for f in raw.rglob("*.jsonl.zst")}
    compact_day(raw, out, DAY, today=TODAY)
    after = {f: f.read_bytes() for f in raw.rglob("*.jsonl.zst")}
    assert before == after
    files_after = {f for f in raw.rglob("*") if f.is_file()}
    assert files_after == set(before), "no files created/removed under raw_root"


def test_empty_partition_writes_zero_row_parquet(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    (raw / "venue=kalshi" / "endpoint=trades" / f"date={DAY}" / "hour=07").mkdir(
        parents=True
    )
    report = compact_day(raw, out, DAY, today=TODAY)
    (part,) = report["partitions"]
    assert part["action"] == "written" and part["rows"] == 0
    pq = out / "venue=kalshi" / "endpoint=trades" / f"date={DAY}" / PARQUET_PART_NAME
    conn = duckdb.connect()
    assert conn.execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0] == 0


def test_truncated_trailing_frame_is_salvaged(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    with RawLake(raw) as lake:
        _append(lake, endpoint="trades", hour=3, minute=0, payload={"i": 0})
        # Big record: its frame spans multiple zstd blocks, so a mid-frame
        # tear emits a PARTIAL line (no trailing newline) — the torn-tail case.
        _append(lake, endpoint="trades", hour=3, minute=1,
                payload={"blob": "x" * 300_000})
    f = next(raw.rglob("*.jsonl.zst"))
    data = f.read_bytes()
    frame_starts = [
        i for i in range(len(data) - 3) if data[i : i + 4] == b"\x28\xb5\x2f\xfd"
    ]
    assert len(frame_starts) >= 2
    # Tear inside the big record's frame at a point where the decompressor
    # emits some complete blocks but not the trailing newline (the exact
    # offset depends on zstd block layout, so search for it).
    import io

    import zstandard

    for cut in range(frame_starts[1] + 8, len(data), 4):
        buf = b""
        reader = zstandard.ZstdDecompressor().stream_reader(
            io.BytesIO(data[:cut]), read_across_frames=True
        )
        try:
            while chunk := reader.read(4096):
                buf += chunk
        except zstandard.ZstdError:
            pass
        if buf and not buf.endswith(b"\n"):
            f.write_bytes(data[:cut])
            break
    else:
        pytest.fail("could not construct a torn trailing frame")

    report = compact_day(raw, out, DAY, today=TODAY)
    (part,) = report["partitions"]
    assert part["rows"] == 1 and part["truncated_files"] == 1
    # Idempotent skip recounts with the same salvage semantics.
    rerun = compact_day(raw, out, DAY, today=TODAY)
    assert rerun["partitions"][0]["action"] == "skipped-existing"
    assert rerun["partitions"][0]["rows"] == 1


def test_venue_filter(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    with RawLake(raw) as lake:
        _append(lake, venue="kalshi", endpoint="trades", payload={"k": 1})
        _append(lake, venue="polymarket", endpoint="prices", payload={"p": 1})
    report = compact_day(raw, out, DAY, venue="kalshi", today=TODAY)
    assert [p["venue"] for p in report["partitions"]] == ["kalshi"]
    assert not (out / "venue=polymarket").exists()


def test_compact_all_pending(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    with RawLake(raw) as lake:
        _append(lake, day=DAY, payload={"d": 1})
        _append(lake, day=DAY2, payload={"d": 2})
        _append(lake, day=TODAY, payload={"d": 3})  # incomplete — must wait

    reports = compact_all_pending(raw, out, today=TODAY)
    assert [r["date"] for r in reports] == [DAY, DAY2]
    assert all(r["written"] == 1 for r in reports)
    assert not (out / "venue=kalshi" / "endpoint=trades" / f"date={TODAY}").exists()

    # Fully-compacted days are skipped without rework on the next run.
    assert compact_all_pending(raw, out, today=TODAY) == []
    # The day rolls over: TODAY becomes complete and gets picked up.
    reports = compact_all_pending(raw, out, today="2026-07-13")
    assert [r["date"] for r in reports] == [TODAY]


def test_compact_all_pending_retries_deferred_tmp(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    _fill_day(raw)
    tmp_file = raw / "venue=kalshi" / "endpoint=trades" / f"date={DAY}" / "hour=23" /\
        "part-zzz.jsonl.zst.tmp"
    tmp_file.write_bytes(b"live writer")
    (first,) = compact_all_pending(raw, out, today=TODAY)
    assert first["deferred"] == 1

    # Writer finalized (rotation/recovery) — next nightly picks the day up.
    tmp_file.unlink()
    (second,) = compact_all_pending(raw, out, today=TODAY)
    assert second["date"] == DAY and second["deferred"] == 0
    assert second["written"] == 1  # only the deferred partition was pending


def test_compact_all_pending_records_watermark(tmp_path):
    from pmkt.state import HotStore

    raw, out = tmp_path / "raw", tmp_path / "raw-parquet"
    _fill_day(raw)
    store = HotStore(tmp_path / "db" / "hot.sqlite")
    try:
        compact_all_pending(raw, out, today=TODAY, store=store)
        state = store.get_state("lake", "compact")
        assert state is not None and state["cursor"] == DAY
    finally:
        store.close()
