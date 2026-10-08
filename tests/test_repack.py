"""Lossless raw re-pack: decompressed-identical, crash-safe, skip-when-unsure."""

import contextlib
import errno
import fcntl
import fnmatch
import hashlib
import io
import json
import operator
import os
import random
import signal
import subprocess
import sys
import textwrap
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest
import zstandard

from pmkt import repack as rp
from pmkt.compact import PARQUET_PART_NAME, _partition_part_files, _read_lines, compact_day
from pmkt.lake import _discover_raw
from pmkt.rawlake import RawLake, read_raw_file

# Fixed past dates with an injected "today" keep the tests deterministic.
DAY = "2026-07-10"
DAY2 = "2026-07-11"
TODAY = "2026-07-12"
EP = "ws_trade"


# ---- fixtures ---------------------------------------------------------------

def _ws_frame(i: int) -> dict:
    # Shape of a live Kalshi ws_trade frame (dollar-string prices, decimal-string precision).
    return {
        "type": "trade", "sid": 1, "seq": i,
        "msg": {
            "trade_id": f"{i:08x}-ee49-470d-82d8-11418bda6fed",
            "market_ticker": f"KXMLBGAME-26SEP25NYYBOS-T{i % 37}",
            "yes_price_dollars": f"0.{10 + i % 80:02d}00",
            "no_price_dollars": f"0.{90 - i % 80:02d}00",
            "count_fp": f"{1 + i % 97}.00",
            "taker_side": "yes" if i % 3 else "no",
            "ts": 1_790_000_000 + i,
        },
    }


def _ts(day: str, hour: int, i: int = 0) -> datetime:
    y, m, d = (int(x) for x in day.split("-"))
    return datetime(y, m, d, hour, i % 60, i % 60, i, tzinfo=timezone.utc)


def _fill(data_dir: Path, *, day=DAY, hours=(14, 15), n=300, endpoint=EP, lakes=1) -> None:
    """Real per-record FLUSH_FRAME captures through RawLake (the REST default
    and the historical WS shape). ``lakes`` > 1 → several part files per hour."""
    for k in range(lakes):
        with RawLake(data_dir / "raw") as lake:
            for hour in hours:
                for i in range(n):
                    lake.append(
                        venue="kalshi", endpoint=endpoint,
                        params={"direction": "in", "channel": "trade", "sid": 1},
                        http_status=0, elapsed_ms=0, payload=_ws_frame(k * n + i),
                        fetched_at=_ts(day, hour, i),
                    )


def _line(day: str, hour: int, i: int, payload=None) -> bytes:
    rec = {"v": 1, "venue": "kalshi", "endpoint": EP, "params": {"sid": 1},
           "fetched_at": _ts(day, hour, i).isoformat(), "http_status": 0,
           "elapsed_ms": 0, "payload": payload if payload is not None else _ws_frame(i)}
    return (json.dumps(rec, separators=(",", ":")) + "\n").encode()


def _write_part(path: Path, chunks: list[bytes], *, mode=zstandard.FLUSH_FRAME,
                terminate=True) -> Path:
    """Replica of rawlake._PartWriter: level-6 stream, flushed per record.
    terminate=False leaves the frame open, like a SIGKILLed writer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fh:
        writer = zstandard.ZstdCompressor(level=6).stream_writer(fh, closefd=False)
        for c in chunks:
            writer.write(c)
            writer.flush(mode)
        if terminate:
            writer.close()
    return path


def _part(data_dir: Path, *, day=DAY, hour=14, name=None, endpoint=EP) -> Path:
    name = name or f"part-{day.replace('-', '')}T{hour:02d}0000-1-{random.getrandbits(32):08x}.jsonl.zst"
    return (data_dir / "raw" / "venue=kalshi" / f"endpoint={endpoint}" / f"date={day}"
            / f"hour={hour:02d}" / name)


def _mark_compacted(data_dir: Path, *, day=DAY, endpoint=EP) -> None:
    marker = (data_dir / "raw-parquet" / "venue=kalshi" / f"endpoint={endpoint}"
              / f"date={day}" / PARQUET_PART_NAME)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()


def _parts(data_dir: Path) -> list[Path]:
    return sorted((data_dir / "raw").rglob("part-*.jsonl.zst"))


def _frames(data: bytes) -> tuple[int, bool]:
    """(zstd frames in data, whether the last one is terminated)."""
    frames = 0
    while data:
        dobj = zstandard.ZstdDecompressor().decompressobj()
        dobj.decompress(data)
        frames += 1
        if not dobj.eof:
            return frames, False
        data = dobj.unused_data
    return frames, True


def _content(path: Path) -> bytes:
    """Test oracle: whole-file decode, one decompressobj per frame."""
    out, data = bytearray(), path.read_bytes()
    while data:
        dobj = zstandard.ZstdDecompressor().decompressobj()
        out += dobj.decompress(data)
        data = dobj.unused_data if dobj.eof else b""
    return bytes(out)


def _snapshot(root: Path) -> dict[str, tuple[bytes, int, int]]:
    return {
        str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns, p.stat().st_mode)
        for p in sorted(root.rglob("*")) if p.is_file() and not p.is_symlink()
    }


def _temps(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if rp._TEMP_INFIX in p.name]


def _manifest(data_dir: Path) -> list[dict]:
    path = rp.manifest_path(data_dir)
    return [json.loads(ln) for ln in path.read_text().splitlines()] if path.exists() else []


def _status_by_rel(data_dir: Path) -> dict[str, str]:
    return {r["rel"]: r["status"] for r in _manifest(data_dir)}


def _rel(data_dir: Path, path: Path) -> str:
    return path.relative_to(data_dir).as_posix()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _gated_repack_file(task):
    """Pool-worker stand-in for the interruption tests — spawned workers
    import it from this module. Runs the real repack_file, parked at a gate
    file BEFORE it or AFTER it (renamed, result not yet returned), so a test
    can interrupt the run at a known point. Leaves a <mode>-<pid>-* marker."""
    gate = Path(os.environ["PMKT_TEST_GATE"])
    mode = os.environ["PMKT_TEST_GATE_MODE"]
    marker = gate.parent / f"{mode}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    res = None if mode == "before" else rp.repack_file(task)
    marker.touch()
    while not gate.exists():
        time.sleep(0.01)
    return rp.repack_file(task) if mode == "before" else res


def _post_replace_failing_repack_file(task):
    """Pool-worker stand-in for the pooled hard-stop test. hour=00 runs the
    real repack_file with its step-9 read-back digest corrupted in THIS
    worker (a failed-post-replace after a real rename); every other task
    parks until the parent has recorded that failure, then runs the real
    repack_file — which must not rename anything any more."""
    if "/hour=00/" in task["rel"]:
        real, calls = rp._decode, []

        def decode(fh, sink=None):
            d = real(fh, sink)
            calls.append(1)
            # 1 original, 2 temp verify, 3 post-replace read-back
            return dict(d, content_sha256="0" * 64) if len(calls) == 3 else d

        rp._decode = decode
        try:
            return rp.repack_file(task)
        finally:
            rp._decode = real
    manifest = Path(task["intent_log"]).with_name(rp.MANIFEST_NAME)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not (
        manifest.exists() and "failed-post-replace" in manifest.read_text()
    ):
        time.sleep(0.01)
    return rp.repack_file(task)


def _start_gated_run(data_dir: Path, gate: Path, mode: str) -> subprocess.Popen:
    """The CLI with two pool workers running _gated_repack_file, in its own
    process group (so a test can deliver a terminal-style Ctrl-C)."""
    code = textwrap.dedent(f"""
        import sys
        sys.path.insert(0, {str(Path(__file__).parent)!r})
        import test_repack
        from pmkt import repack as rp

        rp.repack_file = test_repack._gated_repack_file
        sys.exit(rp.main(["--data-dir", {str(data_dir)!r}, "--workers", "2"]))
    """)
    env = {**os.environ, "PMKT_TEST_GATE": str(gate), "PMKT_TEST_GATE_MODE": mode}
    with open(gate.parent / f"run-{mode}.log", "w") as log:  # a pipe could fill and block
        return subprocess.Popen([sys.executable, "-c", code], stdout=log,
                                stderr=subprocess.STDOUT, env=env, start_new_session=True)


def _wait_for(predicate, proc: subprocess.Popen | None = None, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if proc is not None and proc.poll() is not None:
            raise AssertionError(f"run exited early with {proc.returncode}")
        if time.monotonic() > deadline:
            raise AssertionError("condition not met within timeout")
        time.sleep(0.02)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.fixture
def data_dir(tmp_path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    return d


# ---- the decoder itself --------------------------------------------------------

@pytest.mark.parametrize("chunk", [64, 512, 4096, 65536])
def test_decode_is_exact_for_every_shape_and_chunk_size(tmp_path, monkeypatch, chunk):
    """The stream_reader bug dropped trailing records of an unterminated
    FLUSH_BLOCK frame at small read sizes; _decode must be exact everywhere
    and must call termination correctly for each shape."""
    monkeypatch.setattr(rp, "_READ_CHUNK", chunk)
    monkeypatch.setattr(rp, "_FRAME_SLICE", max(chunk // 4, 16))
    lines = [_line(DAY, 14, i) for i in range(120)]
    expected = b"".join(lines)
    shapes = {
        "frame-closed": (zstandard.FLUSH_FRAME, True, True),
        "block-closed": (zstandard.FLUSH_BLOCK, True, True),
        "block-unterminated": (zstandard.FLUSH_BLOCK, False, False),
    }
    for name, (mode, terminate, want_terminated) in shapes.items():
        f = _write_part(tmp_path / f"{name}.jsonl.zst", lines, mode=mode, terminate=terminate)
        assert _frames(f.read_bytes())[1] is want_terminated
        with open(f, "rb") as fh:
            d = rp._decode(fh)
        assert d["error"] is None, name
        assert d["content_bytes"] == len(expected) and d["lines"] == 120, name
        assert d["content_sha256"] == hashlib.sha256(expected).hexdigest(), name
        assert d["cross_sha256"] == d["content_sha256"], name
        assert d["terminated"] is want_terminated, name
        assert d["compressed_sha256"] == hashlib.sha256(f.read_bytes()).hexdigest()


# ---- the happy path --------------------------------------------------------------

def test_round_trip_is_content_identical_and_metadata_preserved(data_dir):
    _fill(data_dir, hours=(14, 15), lakes=2)  # 4 part files
    _mark_compacted(data_dir)
    files = _parts(data_dir)
    assert len(files) == 4
    before = {}
    for k, f in enumerate(files):
        os.chmod(f, 0o640 if k % 2 else 0o444)
        mtime = 1_783_000_000_123_456_789 + k
        os.utime(f, ns=(mtime, mtime))
        lines, truncated = _read_lines(f)
        assert not truncated
        before[f] = {
            "sha": hashlib.sha256(_content(f)).hexdigest(), "records": read_raw_file(f),
            "lines": lines, "size": f.stat().st_size, "mtime": mtime,
            "mode": f.stat().st_mode,
        }

    summary = rp.repack(data_dir, today=TODAY, workers=2)  # the real process pool

    assert summary["ok"] and not summary["failures"]
    assert summary["totals"]["counts"] == {"repacked": 4}
    assert summary["endpoints"][EP]["freed"] > 0
    assert _parts(data_dir) == files  # same paths, same filenames
    for f in files:
        b, data = before[f], f.read_bytes()
        assert hashlib.sha256(_content(f)).hexdigest() == b["sha"]
        assert read_raw_file(f) == b["records"]
        assert _read_lines(f) == (b["lines"], False)
        n = duckdb.connect().execute(
            f"SELECT count(*) FROM read_json('{f}', format='newline_delimited', "
            "columns={'v': 'BIGINT'})"
        ).fetchone()[0]
        assert n == len(b["lines"])
        assert len(data) * 5 < b["size"]  # per-record frames → one frame: big win
        assert _frames(data) == (1, True)
        assert zstandard.get_frame_parameters(data).has_checksum
        st = f.stat()
        assert st.st_mtime_ns == b["mtime"]
        assert st.st_mode == b["mode"]
    assert not _temps(data_dir)

    rows = _manifest(data_dir)
    assert len(rows) == 4 and {r["status"] for r in rows} == {"repacked"}
    for r in rows:
        f = data_dir / r["rel"]
        assert r["content_sha256"] == before[f]["sha"]
        assert r["old_size"] == before[f]["size"] and r["new_size"] == f.stat().st_size
        assert r["new_compressed_sha256"] == hashlib.sha256(f.read_bytes()).hexdigest()
        assert r["lines"] == len(before[f]["lines"])
        assert r["level"] == 9 and r["zstandard_version"] == zstandard.__version__
        assert r["finished_at"].endswith("+00:00")


def test_temp_names_are_invisible_to_every_reader(data_dir):
    _fill(data_dir, hours=(14,), n=20)
    (f,) = _parts(data_dir)
    tmp = rp._temp_path(f)
    tmp.write_bytes(f.read_bytes())  # a well-formed zstd file under the temp name
    assert rp.is_repack_temp(tmp.name)
    assert tmp.name.startswith(".") and tmp.parent == f.parent
    for pattern in ("part-*.jsonl.zst", "*.jsonl.zst", "*.jsonl.zst.tmp", "*.zst", "*.tmp"):
        assert not fnmatch.fnmatchcase(tmp.name, pattern), pattern
    date_dir = f.parent.parent
    parts, tmps = _partition_part_files(date_dir)
    assert tmp not in parts and tmp not in tmps
    assert tmp not in _discover_raw(data_dir)[1]
    assert tmp not in list(date_dir.glob("hour=*/part-*.jsonl.zst"))  # history.py / ofi.py
    assert tmp not in list(date_dir.glob("hour=*/*.jsonl.zst"))        # lake pm_trades glob
    before = tmp.read_bytes()
    RawLake(data_dir / "raw")  # startup orphan recovery must leave it alone
    assert tmp.exists() and tmp.read_bytes() == before
    assert not list((data_dir / "raw").rglob("*.jsonl.zst.tmp"))


# ---- files that must never be touched ------------------------------------------------

def test_torn_trailing_garbage_and_undecodable_files_are_skipped_untouched(data_dir):
    _mark_compacted(data_dir)
    ok = [_line(DAY, 14, i) for i in range(30)]
    files = {}
    # Terminated frames, but the decoded stream ends mid-record.
    files["no-newline"] = _write_part(_part(data_dir, hour=1), ok + [b'{"v":1,"venue":"kal'])
    # Big final record spanning several blocks, file torn inside it.
    big = _line(DAY, 2, 0, payload={"blob": random.Random(0).randbytes(150_000).hex()})
    whole = _write_part(_part(data_dir, hour=2), ok + [big])
    head = len(_write_part(data_dir / "head.zst", ok).read_bytes())
    data = whole.read_bytes()
    whole.write_bytes(data[: head + (len(data) - head) * 3 // 4])
    files["torn-big-record"] = whole
    # Per-record frames, truncated inside the last (small) frame.
    f = _write_part(_part(data_dir, hour=3), ok)
    f.write_bytes(f.read_bytes()[:-7])
    files["truncated-frame"] = f
    # Complete frames followed by bytes that are not a zstd frame.
    f = _write_part(_part(data_dir, hour=4), ok)
    f.write_bytes(f.read_bytes() + b"\x00garbage-after-last-frame")
    files["trailing-bytes"] = f
    # Not zstd at all.
    f = _part(data_dir, hour=5)
    f.parent.mkdir(parents=True)
    f.write_bytes(b"this is not a zstd stream\n" * 10)
    files["not-zstd"] = f
    # Zero bytes (a writer killed before its first record).
    f = _part(data_dir, hour=6)
    f.parent.mkdir(parents=True)
    f.write_bytes(b"")
    files["empty"] = f
    (data_dir / "head.zst").unlink()
    snap = _snapshot(data_dir / "raw")

    summary = rp.repack(data_dir, today=TODAY, workers=0)

    status = _status_by_rel(data_dir)
    got = {k: status[_rel(data_dir, f)] for k, f in files.items()}
    assert got == {
        "no-newline": "skipped-torn",
        "torn-big-record": "skipped-torn",
        "truncated-frame": "skipped-unterminated",
        "trailing-bytes": "skipped-unterminated",
        "not-zstd": "skipped-undecodable",
        "empty": "skipped-empty",
    }
    assert summary["ok"]  # skips are not failures
    assert _snapshot(data_dir / "raw") == snap  # byte- and mtime-identical
    assert not _temps(data_dir)


def test_unterminated_flush_block_orphan_is_skipped_untouched(data_dir):
    """The shape RawLake._recover_orphans finalizes after SIGKILL: one open
    FLUSH_BLOCK frame. Readers salvage it; the re-pack must not rewrite it."""
    _mark_compacted(data_dir)
    lines = [_line(DAY, 14, i) for i in range(50)]
    orphan = _write_part(_part(data_dir, hour=14), lines, mode=zstandard.FLUSH_BLOCK,
                         terminate=False)
    closed = _write_part(_part(data_dir, hour=15), [_line(DAY, 15, i) for i in range(50)],
                         mode=zstandard.FLUSH_BLOCK)
    assert _frames(orphan.read_bytes()) == (1, False)
    assert _frames(closed.read_bytes()) == (1, True)
    assert len(read_raw_file(orphan)) == 50
    orphan_snap = (orphan.read_bytes(), orphan.stat().st_mtime_ns)
    closed_content = _content(closed)

    summary = rp.repack(data_dir, today=TODAY, workers=0)

    status = _status_by_rel(data_dir)
    assert status[_rel(data_dir, orphan)] == "skipped-unterminated"
    assert (orphan.read_bytes(), orphan.stat().st_mtime_ns) == orphan_snap
    # A CLOSED block-flushed file is fair game (re-packed or no-gain) — lossless either way.
    assert status[_rel(data_dir, closed)] in {"repacked", "skipped-no-gain"}
    assert _content(closed) == closed_content
    assert summary["ok"] and not _temps(data_dir)


def test_symlinked_and_hardlinked_parts_are_skipped(data_dir, tmp_path):
    _mark_compacted(data_dir)
    outside = _write_part(tmp_path / "elsewhere.jsonl.zst", [_line(DAY, 14, i) for i in range(20)])
    link = _part(data_dir, hour=14)
    link.parent.mkdir(parents=True)
    link.symlink_to(outside)
    hard = _write_part(_part(data_dir, hour=15), [_line(DAY, 15, i) for i in range(20)])
    os.link(hard, tmp_path / "second-name.jsonl.zst")
    outside_bytes, hard_bytes = outside.read_bytes(), hard.read_bytes()

    assert rp.repack(data_dir, today=TODAY, workers=0)["ok"]

    status = _status_by_rel(data_dir)
    assert status[_rel(data_dir, link)] == "skipped-symlink"
    assert status[_rel(data_dir, hard)] == "skipped-hardlinked"
    assert link.is_symlink() and outside.read_bytes() == outside_bytes
    assert hard.read_bytes() == hard_bytes


# ---- partition eligibility ----------------------------------------------------------------

def test_partition_with_tmp_writer_is_skipped(data_dir):
    _fill(data_dir, hours=(14, 15), n=20)
    _mark_compacted(data_dir)
    live = _part(data_dir, hour=15, name="part-live.jsonl.zst.tmp")
    live.write_bytes(b"in flight")
    snap = _snapshot(data_dir / "raw")

    summary = rp.repack(data_dir, today=TODAY, workers=0)

    assert summary["partitions_skipped"] == [{"endpoint": EP, "date": DAY, "reason": "tmp-present"}]
    assert summary["totals"]["counts"] == {} and not _manifest(data_dir)
    assert _snapshot(data_dir / "raw") == snap


def test_today_and_future_partitions_are_skipped(data_dir):
    _fill(data_dir, day=TODAY, hours=(1,), n=20)
    _fill(data_dir, day="2026-07-13", hours=(1,), n=20)
    _mark_compacted(data_dir, day=TODAY)
    _mark_compacted(data_dir, day="2026-07-13")
    snap = _snapshot(data_dir / "raw")

    summary = rp.repack(data_dir, today=TODAY, workers=0)

    assert {(p["date"], p["reason"]) for p in summary["partitions_skipped"]} == {
        (TODAY, "not-before-today"), ("2026-07-13", "not-before-today"),
    }
    assert _snapshot(data_dir / "raw") == snap


def test_uncompacted_partition_skipped_by_default_opt_in_processes_it(data_dir):
    _fill(data_dir, hours=(14,), n=50)
    snap = _snapshot(data_dir / "raw")

    summary = rp.repack(data_dir, today=TODAY, workers=0)
    assert summary["partitions_skipped"] == [{"endpoint": EP, "date": DAY, "reason": "not-compacted"}]
    assert _snapshot(data_dir / "raw") == snap

    summary = rp.repack(data_dir, today=TODAY, workers=0, require_compacted=False)
    assert summary["totals"]["counts"] == {"repacked": 1}


def test_endpoint_glob_and_date_filters(data_dir):
    _fill(data_dir, day=DAY, hours=(14,), n=20)
    _fill(data_dir, day=DAY2, hours=(14,), n=20)
    _fill(data_dir, day=DAY, hours=(14,), n=20, endpoint="trades")  # REST: not ws_*
    for day in (DAY, DAY2):
        _mark_compacted(data_dir, day=day)
    _mark_compacted(data_dir, endpoint="trades")

    summary = rp.repack(data_dir, today=TODAY, workers=0, dates=[DAY2])
    assert summary["totals"]["counts"] == {"repacked": 1}
    assert all(r["date"] == DAY2 for r in _manifest(data_dir))

    summary = rp.repack(data_dir, today=TODAY, workers=0, date_from=DAY, date_to=DAY)
    assert summary["totals"]["counts"] == {"repacked": 1}
    assert {r["endpoint"] for r in _manifest(data_dir)} == {EP}

    summary = rp.repack(data_dir, today=TODAY, workers=0, endpoint_glob="*", limit=1)
    assert summary["totals"]["counts"] == {"repacked": 1}  # only trades was left
    assert summary["already_repacked"] == 2


def test_limit_caps_a_run_with_more_eligible_files(data_dir):
    """--limit is the canary knob for a first run over the system of record:
    it must actually cut the work, not only pass when little is left."""
    _fill(data_dir, hours=(11, 12, 13, 14), n=40)
    _mark_compacted(data_dir)
    before = {_rel(data_dir, f): f.read_bytes() for f in _parts(data_dir)}
    assert len(before) == 4

    def changed() -> set[str]:
        return {rel for rel, b in before.items() if (data_dir / rel).read_bytes() != b}

    none = rp.repack(data_dir, today=TODAY, workers=0, limit=0)
    assert none["ok"] and none["totals"]["counts"] == {} and not changed()

    first = rp.repack(data_dir, today=TODAY, workers=0, limit=2)
    rows = _manifest(data_dir)
    assert first["ok"] and first["totals"]["counts"] == {"repacked": 2}
    assert len(rows) == 2 and changed() == {r["rel"] for r in rows}  # other 2 byte-identical

    rest = rp.repack(data_dir, today=TODAY, workers=0, limit=2)
    assert rest["already_repacked"] == 2 and rest["totals"]["counts"] == {"repacked": 2}
    assert changed() == set(before)


# ---- run modes ---------------------------------------------------------------------------

def test_dry_run_changes_nothing(data_dir):
    _fill(data_dir, hours=(14, 15), n=100)
    _mark_compacted(data_dir)
    snap = _snapshot(data_dir / "raw")

    summary = rp.repack(data_dir, today=TODAY, workers=0, dry_run=True)

    assert summary["totals"]["counts"] == {"would-repack": 2}
    assert 0 < summary["totals"]["bytes_after"] < summary["totals"]["bytes_before"]
    assert _snapshot(data_dir / "raw") == snap
    assert not rp.manifest_path(data_dir).exists()
    report = [json.loads(ln) for ln in Path(summary["report"]).read_text().splitlines()]
    assert [r["status"] for r in report] == ["would-repack"] * 2
    assert all(r["new_size"] < r["old_size"] for r in report)
    assert not _temps(data_dir)


def test_rerun_is_idempotent(data_dir):
    _fill(data_dir, hours=(14, 15), n=100)
    _mark_compacted(data_dir)
    first = rp.repack(data_dir, today=TODAY, workers=0)
    assert first["totals"]["counts"] == {"repacked": 2}
    snap, rows = _snapshot(data_dir / "raw"), _manifest(data_dir)

    again = rp.repack(data_dir, today=TODAY, workers=0)

    assert again["already_repacked"] == 2 and again["totals"]["counts"] == {}
    assert _snapshot(data_dir / "raw") == snap
    assert _manifest(data_dir) == rows


def test_verify_passes_then_fails_after_tampering(data_dir, monkeypatch):
    monkeypatch.setattr(os, "nice", lambda n: 0)  # main() must not renice pytest
    _fill(data_dir, day=DAY, hours=(14, 15), n=100)
    _fill(data_dir, day=DAY2, hours=(14,), n=100)
    _mark_compacted(data_dir, day=DAY)
    _mark_compacted(data_dir, day=DAY2)
    assert rp.repack(data_dir, today=TODAY, workers=0)["ok"]

    good = rp.verify(data_dir, workers=2)
    assert good["ok"] and good["totals"]["counts"] == {"ok": 3}
    assert rp.main(["--data-dir", str(data_dir), "--verify", "--workers", "0"]) == 0

    # Tamper: a perfectly valid single frame that is missing the last record.
    victim = sorted((data_dir / "raw").rglob(f"date={DAY}/hour=15/part-*.jsonl.zst"))[0]
    lines = _content(victim).splitlines(keepends=True)
    victim.write_bytes(zstandard.ZstdCompressor(level=9).compress(b"".join(lines[:-1])))

    bad = rp.verify(data_dir, workers=0)
    assert not bad["ok"] and bad["totals"]["counts"] == {"ok": 2, "mismatch": 1}
    assert bad["failures"][0]["rel"] == _rel(data_dir, victim)
    assert rp.main(["--data-dir", str(data_dir), "--verify", "--workers", "0"]) == 1
    # Date filters narrow the check; DAY2 alone is still clean.
    assert rp.verify(data_dir, dates=[DAY2], workers=0)["ok"]
    assert rp.main(["--data-dir", str(data_dir), "--verify", "--from", DAY, "--to", DAY,
                    "--workers", "0"]) == 1

    victim.write_bytes(victim.read_bytes()[:-9])  # now also torn
    assert not rp.verify(data_dir, workers=0)["ok"]


def test_lock_contention_is_refused(data_dir, monkeypatch):
    monkeypatch.setattr(os, "nice", lambda n: 0)
    _fill(data_dir, hours=(14,), n=20)
    _mark_compacted(data_dir)
    snap = _snapshot(data_dir / "raw")
    with open(data_dir / rp.LOCK_NAME, "a") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(rp.RepackLocked):
            rp.repack(data_dir, today=TODAY, workers=0)
        assert rp.main(["--data-dir", str(data_dir), "--workers", "0"]) == 2
    assert _snapshot(data_dir / "raw") == snap
    assert not rp.manifest_path(data_dir).exists()
    assert rp.repack(data_dir, today=TODAY, workers=0)["ok"]  # released → runs


def test_stale_temp_cleanup_only_removes_the_tools_own_pattern(data_dir, tmp_path):
    _fill(data_dir, day=DAY, hours=(14,), n=20)
    _fill(data_dir, day=DAY2, hours=(14,), n=20)
    _mark_compacted(data_dir, day=DAY)
    _mark_compacted(data_dir, day=DAY2)
    (part,) = sorted((data_dir / "raw").rglob(f"date={DAY}/hour=14/part-*.jsonl.zst"))
    hour = part.parent
    stale = hour / f".{part.name}.repack-deadbeef"
    decoys = [
        hour / f".{part.name}.repack-deadbeeg",       # not hex (APFS ignores case)
        hour / f".{part.name}.repack-dead",           # wrong length
        hour / f"{part.name}.repack-deadbeef",        # not dot-prefixed
        hour / ".compact-staging-1234.jsonl.zst",     # compact's own temp
        hour / ".other-dotfile",
        hour / "notes.txt",
    ]
    (other,) = sorted((data_dir / "raw").rglob(f"date={DAY2}/hour=14/part-*.jsonl.zst"))
    untargeted = other.parent / f".{other.name}.repack-cafef00d"  # date not selected
    pm = data_dir / "raw/venue=polymarket/endpoint=ws_x/date=2026-07-10/hour=14"
    pm.mkdir(parents=True)
    pm_temp = pm / ".part-x.jsonl.zst.repack-cafef00d"  # other venue: never targeted
    for f in [stale, *decoys, untargeted, pm_temp]:
        f.write_bytes(b"leftover")

    summary = rp.repack(data_dir, today=TODAY, workers=0, dates=[DAY])

    assert summary["stale_temps"] == [_rel(data_dir, stale)]
    assert not stale.exists()
    for f in [*decoys, untargeted, pm_temp]:
        assert f.read_bytes() == b"leftover", f.name
    assert summary["totals"]["counts"] == {"repacked": 1}


# ---- failure injection ----------------------------------------------------------------------

def test_crash_between_temp_write_and_replace_leaves_original_then_rerun_completes(data_dir):
    _fill(data_dir, hours=(14, 15), n=100)
    _mark_compacted(data_dir)
    snap = _snapshot(data_dir / "raw")
    contents = {f: _content(f) for f in _parts(data_dir)}
    code = textwrap.dedent(f"""
        import os
        from pmkt import repack as rp

        def killed(tmp, path):
            os._exit(137)  # like SIGKILL: no finally, no cleanup, no manifest line

        rp._replace_into_place = killed
        rp.repack({str(data_dir)!r}, today={TODAY!r}, workers=0)
    """)
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                          timeout=300)
    assert proc.returncode == 137, proc.stderr

    (temp,) = _temps(data_dir)  # the fully written, verified temp is left behind
    assert rp.is_repack_temp(temp.name)
    snap_now = {k: v for k, v in _snapshot(data_dir / "raw").items() if rp._TEMP_INFIX not in k}
    assert snap_now == snap  # every original untouched
    assert not _manifest(data_dir)

    summary = rp.repack(data_dir, today=TODAY, workers=0)
    assert summary["stale_temps"] == [_rel(data_dir, temp)]
    assert summary["ok"] and summary["totals"]["counts"] == {"repacked": 2}
    assert {f: _content(f) for f in _parts(data_dir)} == contents
    assert not _temps(data_dir)


def test_worker_exception_is_an_error_status_and_nonzero_exit(data_dir, monkeypatch):
    monkeypatch.setattr(os, "nice", lambda n: 0)
    _fill(data_dir, hours=(14,), n=50)
    _mark_compacted(data_dir)
    snap = _snapshot(data_dir / "raw")

    def boom(fh, sink=None):
        raise RuntimeError("injected decoder crash")

    monkeypatch.setattr(rp, "_decode", boom)
    summary_path = data_dir.parent / "summary.json"
    rc = rp.main(["--data-dir", str(data_dir), "--workers", "0",
                  "--json-summary", str(summary_path)])

    assert rc == 1
    (row,) = _manifest(data_dir)
    assert row["status"] == "error" and "injected decoder crash" in row["reason"]
    summary = json.loads(summary_path.read_text())
    assert not summary["ok"] and summary["totals"]["counts"] == {"error": 1}
    assert _snapshot(data_dir / "raw") == snap
    assert not _temps(data_dir)


def test_pool_worker_failures_surface_as_errors(tmp_path):
    results = []
    tasks = [{"path": str(tmp_path / "gone.jsonl.zst"), "rel": "gone", "level": 9}]
    # A worker that catches its own failure (missing file) ...
    rp._run_pool(rp.repack_file, tasks, 2, lambda r: results.append(r) or False)
    # ... and one whose exception escapes the worker process entirely.
    rp._run_pool(operator.itemgetter("no-such-key"), tasks, 2,
                 lambda r: results.append(r) or False)
    assert [r["status"] for r in results] == ["error", "error"]
    assert "FileNotFoundError" in results[0]["reason"]
    assert "KeyError" in results[1]["reason"]


def test_enospc_mid_write_leaves_original_and_removes_temp(data_dir, monkeypatch):
    _fill(data_dir, hours=(14,), n=200)
    _mark_compacted(data_dir)
    snap = _snapshot(data_dir / "raw")
    opened = []

    class FullDisk(io.FileIO):
        def write(self, b):
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

    def open_temp(tmp):
        opened.append(tmp)
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        return io.BufferedWriter(FullDisk(fd, "w"), buffer_size=64)

    monkeypatch.setattr(rp, "_open_temp", open_temp)
    summary = rp.repack(data_dir, today=TODAY, workers=0)

    assert not summary["ok"] and summary["totals"]["counts"] == {"error": 1}
    assert "No space left" in summary["failures"][0]["reason"]
    assert opened and not opened[0].exists()
    assert _snapshot(data_dir / "raw") == snap
    assert not _temps(data_dir)


def test_lossy_compressor_is_caught_by_verify(data_dir, monkeypatch):
    _fill(data_dir, hours=(14,), n=100)
    _mark_compacted(data_dir)
    snap = _snapshot(data_dir / "raw")

    class Lossy:
        def __init__(self, level):
            self._c = zstandard.ZstdCompressor(level=level, write_checksum=True)

        def compressobj(self):
            inner, first = self._c.compressobj(), [True]

            class Obj:
                def compress(self, data):
                    if first[0]:
                        first[0], data = False, data[1:]  # silently drop one byte
                    return inner.compress(data)

                def flush(self):
                    return inner.flush()

            return Obj()

    monkeypatch.setattr(rp, "_compressor", Lossy)
    summary = rp.repack(data_dir, today=TODAY, workers=0)

    assert not summary["ok"] and summary["totals"]["counts"] == {"failed-verify": 1}
    assert "content_sha256" in summary["failures"][0]["reason"]
    assert _snapshot(data_dir / "raw") == snap
    assert not _temps(data_dir)


def test_post_replace_mismatch_is_a_hard_stop(data_dir, monkeypatch):
    _fill(data_dir, hours=(14, 15, 16), n=50)
    _mark_compacted(data_dir)
    real, calls = rp._decode, []

    def decode(fh, sink=None):
        d = real(fh, sink)
        calls.append(1)
        if len(calls) == 3:  # 1 original, 2 temp verify, 3 post-replace read-back
            d = dict(d, content_sha256="0" * 64)
        return d

    monkeypatch.setattr(rp, "_decode", decode)
    summary = rp.repack(data_dir, today=TODAY, workers=0)

    assert summary["aborted"] and not summary["ok"]
    assert summary["totals"]["counts"] == {"failed-post-replace": 1}
    assert summary["not_started"] == 2  # the run stopped at the first hard error
    assert [r["status"] for r in _manifest(data_dir)] == ["failed-post-replace"]


def test_post_replace_mismatch_stops_pool_workers_before_any_further_rename(data_dir, monkeypatch):
    """The pooled hard stop (default workers=2): files a worker has already
    taken or the pool has queued when the failure is recorded must not be
    renamed after it — storage is suspect, each rename would risk another
    original before its own read-back could check it."""
    _fill(data_dir, hours=tuple(range(8)), n=60)
    _mark_compacted(data_dir)
    before = {_rel(data_dir, f): f.read_bytes() for f in _parts(data_dir)}
    contents = {rel: _content(data_dir / rel) for rel in before}
    # Spawned workers unpickle the stand-in from this module by reference.
    monkeypatch.setattr(rp, "repack_file", _post_replace_failing_repack_file)

    summary = rp.repack(data_dir, today=TODAY, workers=2)

    assert summary["aborted"] and not summary["ok"]
    changed = {rel for rel, b in before.items() if (data_dir / rel).read_bytes() != b}
    (failed,) = [r for r in _manifest(data_dir) if r["status"] == "failed-post-replace"]
    assert "/hour=00/" in failed["rel"]
    assert changed == {failed["rel"]}  # the failed one alone was renamed
    counts = summary["totals"]["counts"]
    assert set(counts) <= {"failed-post-replace", "skipped-interrupted"}
    assert counts.get("skipped-interrupted", 0) >= 1  # a parked task WAS in flight
    assert sum(counts.values()) + summary["not_started"] == len(before)
    assert {rel: _content(data_dir / rel) for rel in before} == contents
    assert not _temps(data_dir)


# ---- every guard in the verify chain is load-bearing ----------------------------------------

def test_primary_decoder_dropping_the_last_record_is_caught_on_the_original(data_dir, monkeypatch):
    """The bug class this tool exists to avoid: a primary decoder silently
    dropping a trailing record AT a record boundary. It fools every later
    check — the temp is compressed from the short stream and step 4 re-decodes
    it with the same decoder (common mode), and the stream still ends in a
    newline. Only the per-frame cross decode of the ORIGINAL catches it."""
    _fill(data_dir, hours=(14,), n=300)
    _mark_compacted(data_dir)
    (f,) = _parts(data_dir)
    last = _content(f).splitlines(keepends=True)[-1]
    snap = _snapshot(data_dir / "raw")
    real = rp._primary_decompressobj

    class DropsLastRecord:
        def __init__(self):
            self._inner = real()

        def decompress(self, data):
            # One small file = one input chunk = one output: the whole stream.
            return self._inner.decompress(data).replace(last, b"")

    monkeypatch.setattr(rp, "_primary_decompressobj", DropsLastRecord)
    summary = rp.repack(data_dir, today=TODAY, workers=0)

    assert not summary["ok"] and summary["totals"]["counts"] == {"failed-verify": 1}
    assert "disagree" in summary["failures"][0]["reason"]
    assert _snapshot(data_dir / "raw") == snap
    assert len(read_raw_file(f)) == 300  # nothing lost from the system of record
    assert not _temps(data_dir)


def _two_frame_compressor(tail_flush: int):
    """A compressor whose output is TWO frames: the first half of the stream
    in a finished frame, the rest in a second one ended by ``tail_flush``."""

    class TwoFrames:
        def __init__(self, level):
            self._level = level

        def compressobj(self):
            def new():
                return zstandard.ZstdCompressor(level=self._level, write_checksum=True).compressobj()

            first, state = new(), {}

            class Obj:
                def compress(self, data):
                    if "second" in state:
                        return state["second"].compress(data)
                    cut = data.index(b"\n", len(data) // 2) + 1
                    state["second"] = new()
                    return (first.compress(data[:cut]) + first.flush()
                            + state["second"].compress(data[cut:]))

                def flush(self):
                    return state["second"].flush(tail_flush)

            return Obj()

    return TwoFrames


@pytest.mark.parametrize(("tail_flush", "problem"), [
    (zstandard.COMPRESSOBJ_FLUSH_FINISH, "expected exactly 1 frame, found 2"),
    # frame 1 complete + frame 2 open: one complete frame, yet unterminated
    (zstandard.COMPRESSOBJ_FLUSH_BLOCK, "frame not terminated"),
], ids=["two-frames", "unterminated"])
def test_structurally_wrong_temp_is_never_renamed(data_dir, monkeypatch, tail_flush, problem):
    # Content-identical temps: only the structural check stands in the way.
    _fill(data_dir, hours=(14,), n=100)
    _mark_compacted(data_dir)
    snap = _snapshot(data_dir / "raw")

    monkeypatch.setattr(rp, "_compressor", _two_frame_compressor(tail_flush))
    summary = rp.repack(data_dir, today=TODAY, workers=0)

    assert not summary["ok"] and summary["totals"]["counts"] == {"failed-verify": 1}
    assert summary["failures"][0]["reason"] == problem
    assert _snapshot(data_dir / "raw") == snap
    assert not _temps(data_dir)


def test_temp_whose_decoders_disagree_is_never_renamed(data_dir, monkeypatch):
    _fill(data_dir, hours=(14,), n=100)
    _mark_compacted(data_dir)
    snap = _snapshot(data_dir / "raw")
    real, calls = rp._decode, []

    def decode(fh, sink=None):
        d = real(fh, sink)
        calls.append(1)
        if len(calls) == 2:  # 1 original, 2 temp verify
            d = dict(d, cross_sha256="0" * 64)
        return d

    monkeypatch.setattr(rp, "_decode", decode)
    summary = rp.repack(data_dir, today=TODAY, workers=0)

    assert not summary["ok"] and summary["totals"]["counts"] == {"failed-verify": 1}
    assert summary["failures"][0]["reason"] == "primary and per-frame decoders disagree"
    assert _snapshot(data_dir / "raw") == snap
    assert not _temps(data_dir)


def test_bytes_in_place_differing_from_the_verified_temp_are_a_hard_stop(data_dir, monkeypatch):
    # Content-identical, well-formed, but NOT the bytes that were verified:
    # only the compressed-digest comparison of step 9 notices.
    _fill(data_dir, hours=(14, 15), n=100)
    _mark_compacted(data_dir)
    contents = {f: _content(f) for f in _parts(data_dir)}
    real = rp._replace_into_place

    def replace(tmp, path):
        real(tmp, path)
        other = zstandard.ZstdCompressor(level=1, write_checksum=True).compress(_content(path))
        assert other != path.read_bytes()
        path.write_bytes(other)

    monkeypatch.setattr(rp, "_replace_into_place", replace)
    summary = rp.repack(data_dir, today=TODAY, workers=0)

    assert summary["aborted"] and not summary["ok"]
    assert summary["totals"]["counts"] == {"failed-post-replace": 1}
    assert "differ from the verified temp" in summary["failures"][0]["reason"]
    assert {f: _content(f) for f in _parts(data_dir)} == contents


# ---- every rename ends up in the manifest -----------------------------------------------------

def test_run_killed_after_a_rename_is_flagged_by_verify_then_recorded_from_its_intent(data_dir):
    _fill(data_dir, hours=(14, 15), n=100)
    _mark_compacted(data_dir)
    before = {_rel(data_dir, f): (_sha(f), _content(f)) for f in _parts(data_dir)}
    code = textwrap.dedent(f"""
        import os
        from pmkt import repack as rp

        real = rp._replace_into_place

        def killed(tmp, path):
            real(tmp, path)
            os._exit(137)  # renamed and durable; no step 9, no manifest line

        rp._replace_into_place = killed
        rp.repack({str(data_dir)!r}, today={TODAY!r}, workers=0)
    """)
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                          timeout=300)
    assert proc.returncode == 137, proc.stderr
    assert not _manifest(data_dir)
    (victim,) = [rel for rel, (sha, _) in before.items() if _sha(data_dir / rel) != sha]

    # A rewritten system-of-record file nothing records: verify must not say OK.
    bad = rp.verify(data_dir, workers=0)
    assert not bad["ok"] and bad["totals"]["counts"] == {"unrecorded": 1}
    assert bad["failures"][0]["rel"] == victim

    summary = rp.repack(data_dir, today=TODAY, workers=0)
    assert summary["ok"] and summary["totals"]["counts"] == {"repacked": 2}
    row = {r["rel"]: r for r in _manifest(data_dir)}[victim]
    assert "intent" in row["reason"]
    assert row["old_compressed_sha256"] == before[victim][0]  # provenance survived the kill
    assert row["new_compressed_sha256"] == _sha(data_dir / victim)
    assert 0 < row["new_size"] < row["old_size"]
    good = rp.verify(data_dir, workers=0)
    assert good["ok"] and good["totals"]["counts"] == {"ok": 2}
    assert {rel: _content(data_dir / rel) for rel in before} == {
        rel: content for rel, (_, content) in before.items()
    }


def test_error_after_the_rename_is_recorded_from_its_intent_on_rerun(data_dir, monkeypatch):
    _fill(data_dir, hours=(14,), n=100)
    _mark_compacted(data_dir)
    (f,) = _parts(data_dir)
    old_sha, content = _sha(f), _content(f)

    def replace_then_fail(tmp, path):
        os.replace(tmp, path)
        raise OSError(errno.EIO, "injected: directory fsync failed")

    with monkeypatch.context() as m:
        m.setattr(rp, "_replace_into_place", replace_then_fail)
        first = rp.repack(data_dir, today=TODAY, workers=0)
    assert not first["ok"] and first["totals"]["counts"] == {"error": 1}
    assert _sha(f) != old_sha and _content(f) == content  # renamed, losslessly
    assert rp.verify(data_dir, workers=0)["totals"]["counts"] == {"unrecorded": 1}

    again = rp.repack(data_dir, today=TODAY, workers=0)
    assert again["ok"] and again["totals"]["counts"] == {"repacked": 1}
    assert _manifest(data_dir)[-1]["old_compressed_sha256"] == old_sha
    assert rp.verify(data_dir, workers=0)["totals"]["counts"] == {"ok": 1}


def test_unrecorded_re_pack_output_is_recorded_not_hidden_as_no_gain(data_dir):
    """A re-pack output with neither a manifest line nor an intent (e.g. from
    a run that predates intents) must not pass as skipped-no-gain forever."""
    _fill(data_dir, hours=(14,), n=100)
    _mark_compacted(data_dir)
    (f,) = _parts(data_dir)
    # The detection premise: RawLake never writes a content checksum.
    assert not zstandard.get_frame_parameters(f.read_bytes()[:18]).has_checksum
    assert rp.verify(data_dir, workers=0)["ok"]  # nothing re-packed, nothing flagged
    assert rp.repack(data_dir, today=TODAY, workers=0)["ok"]
    rp.manifest_path(data_dir).unlink()
    rp.intents_path(data_dir).unlink()
    snap = _snapshot(data_dir / "raw")

    bad = rp.verify(data_dir, workers=0)
    assert not bad["ok"] and bad["totals"]["counts"] == {"unrecorded": 1}
    summary = rp.repack(data_dir, today=TODAY, workers=0)
    assert summary["ok"] and summary["totals"]["counts"] == {"repacked-unrecorded": 1}
    assert _snapshot(data_dir / "raw") == snap  # recorded as it is, never rewritten
    assert rp.verify(data_dir, workers=0)["totals"]["counts"] == {"ok": 1}
    assert rp.repack(data_dir, today=TODAY, workers=0)["already_repacked"] == 1


def test_ctrl_c_records_every_rename_and_starts_nothing_new(data_dir, tmp_path):
    _fill(data_dir, hours=tuple(range(8)), n=60)
    _mark_compacted(data_dir)
    before = {f: f.read_bytes() for f in _parts(data_dir)}
    contents = {f: _content(f) for f in before}
    gate = tmp_path / "gate"
    log_path = tmp_path / "run-after.log"
    proc = _start_gated_run(data_dir, gate, "after")
    try:
        # Both workers have renamed a file and hold its result un-returned.
        _wait_for(lambda: len(list(tmp_path.glob("after-*"))) >= 2, proc)
        os.killpg(proc.pid, signal.SIGINT)  # a terminal Ctrl-C: parent AND workers
        # The parent logs this right after setting the workers' stop event.
        _wait_for(lambda: "in-flight file(s), starting no new ones" in log_path.read_text(), proc)
        gate.touch()
        rc = proc.wait(timeout=120)
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
    log = log_path.read_text()
    assert rc == 130, log

    changed = {_rel(data_dir, f) for f, b in before.items() if f.read_bytes() != b}
    rows = _manifest(data_dir)
    # Exactly the two gated renames from before the Ctrl-C: the tasks the
    # pool had queued (submission window workers*2 = 4) started nothing and
    # renamed nothing after it — at most they record skipped-interrupted.
    assert len(changed) == 2, log
    assert changed == {r["rel"] for r in rows if r["status"] == "repacked"}, log
    assert {r["status"] for r in rows if r["rel"] not in changed} <= {"skipped-interrupted"}, log
    assert not _temps(data_dir)
    assert rp.verify(data_dir, workers=0)["ok"]

    again = rp.repack(data_dir, today=TODAY, workers=0)
    assert again["ok"] and again["already_repacked"] == len(changed)
    assert again["totals"]["counts"] == {"repacked": len(before) - len(changed)}
    assert {f: _content(f) for f in before} == contents


def test_workers_of_a_killed_run_rename_nothing_and_exit(data_dir, tmp_path):
    _fill(data_dir, hours=tuple(range(8)), n=60)
    _mark_compacted(data_dir)
    snap = _snapshot(data_dir / "raw")
    gate = tmp_path / "gate"
    proc = _start_gated_run(data_dir, gate, "before")
    pids: list[int] = []
    try:
        _wait_for(lambda: len(list(tmp_path.glob("before-*"))) >= 2, proc)
        pids = [int(p.name.split("-")[1]) for p in tmp_path.glob("before-*")]
        os.kill(proc.pid, signal.SIGKILL)  # the run dies outright: no drain, no finally
        proc.wait(timeout=30)
        gate.touch()  # the orphaned workers may carry on now
        _wait_for(lambda: not any(_alive(pid) for pid in pids), timeout=30)
    finally:
        for pid in pids:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
    # Nothing could have recorded a rename, so none happened.
    assert _snapshot(data_dir / "raw") == snap
    assert not _manifest(data_dir)


# ---- interplay with compaction and the CLI ------------------------------------------------------

def test_compaction_idempotency_still_skips_existing_after_repack(data_dir):
    raw, out = data_dir / "raw", data_dir / "raw-parquet"
    _fill(data_dir, hours=(3, 14, 23), n=120, lakes=2)
    first = compact_day(raw, out, DAY, today=TODAY)
    assert first["written"] == 1 and first["rows"] == 720

    summary = rp.repack(data_dir, today=TODAY, workers=0)  # real part-0 marker
    assert summary["totals"]["counts"] == {"repacked": 6}

    again = compact_day(raw, out, DAY, today=TODAY)
    (part,) = again["partitions"]
    assert part["action"] == "skipped-existing" and part["rows"] == 720


def test_module_cli_runs_with_a_process_pool(data_dir):
    _fill(data_dir, hours=(14, 15), n=100)
    _mark_compacted(data_dir)
    contents = {f: _content(f) for f in _parts(data_dir)}
    summary_path = data_dir.parent / "cli-summary.json"
    base = [sys.executable, "-m", "pmkt.repack", "--data-dir", str(data_dir), "--workers", "2"]

    dry = subprocess.run([*base, "--dry-run"], capture_output=True, text=True, timeout=300)
    assert dry.returncode == 0, dry.stderr
    run = subprocess.run([*base, "--date", DAY, "--json-summary", str(summary_path)],
                         capture_output=True, text=True, timeout=300)
    assert run.returncode == 0, run.stderr
    assert json.loads(summary_path.read_text())["totals"]["counts"] == {"repacked": 2}
    check = subprocess.run([*base, "--verify"], capture_output=True, text=True, timeout=300)
    assert check.returncode == 0, check.stderr
    assert {f: _content(f) for f in _parts(data_dir)} == contents

    bad = subprocess.run([*base, "--date", DAY, "--from", DAY], capture_output=True, text=True)
    assert bad.returncode == 2  # argparse usage error
