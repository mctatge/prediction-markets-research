import json
import logging
import random
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest
import zstandard

from pmkt.compact import _read_lines
from pmkt.rawlake import (
    BLOCK_READER_LOCK,
    RawLake,
    block_reader_live,
    hold_block_reader_lock,
    read_raw_file,
)


def test_append_and_readback(tmp_path):
    ts = datetime(2026, 7, 10, 14, 30, tzinfo=timezone.utc)
    with RawLake(tmp_path) as lake:
        for i in range(3):
            lake.append(
                venue="kalshi",
                endpoint="markets",
                params={"limit": 1000, "cursor": f"c{i}"},
                http_status=200,
                elapsed_ms=42,
                payload={"markets": [{"ticker": f"T-{i}"}]},
                fetched_at=ts,
            )

    part_dir = tmp_path / "venue=kalshi" / "endpoint=markets" / "date=2026-07-10" / "hour=14"
    files = list(part_dir.glob("*.jsonl.zst"))
    assert len(files) == 1, "one part file per partition, no .tmp left behind"
    assert not list(part_dir.glob("*.tmp"))

    records = read_raw_file(files[0])
    assert len(records) == 3
    assert records[0]["v"] == 1
    assert records[0]["venue"] == "kalshi"
    assert records[2]["params"]["cursor"] == "c2"
    assert records[1]["payload"]["markets"][0]["ticker"] == "T-1"


def test_hour_rotation_creates_new_partition(tmp_path):
    with RawLake(tmp_path) as lake:
        lake.append(
            venue="kalshi", endpoint="trades", params={}, http_status=200,
            elapsed_ms=1, payload={},
            fetched_at=datetime(2026, 7, 10, 14, 59, tzinfo=timezone.utc),
        )
        lake.append(
            venue="kalshi", endpoint="trades", params={}, http_status=200,
            elapsed_ms=1, payload={},
            fetched_at=datetime(2026, 7, 10, 15, 0, tzinfo=timezone.utc),
        )

    base = tmp_path / "venue=kalshi" / "endpoint=trades" / "date=2026-07-10"
    assert list((base / "hour=14").glob("*.jsonl.zst"))
    assert list((base / "hour=15").glob("*.jsonl.zst"))
    # The hour=14 file was finalized (renamed from .tmp) at rotation time.
    assert not list((base / "hour=14").glob("*.tmp"))


def test_orphaned_tmp_recovered_on_startup(tmp_path):
    # Simulate a hard crash: records written, process dies before close().
    lake = RawLake(tmp_path)
    lake.append(
        venue="kalshi", endpoint="trades", params={}, http_status=200,
        elapsed_ms=1, payload={"n": 1},
        fetched_at=datetime(2026, 7, 11, 3, 0, tzinfo=timezone.utc),
    )
    # Abandon without close — the .tmp is orphaned like after SIGKILL.
    lake._writers.clear()
    assert list(tmp_path.rglob("*.zst.tmp"))
    assert not list(tmp_path.rglob("*.jsonl.zst"))

    # Next startup finalizes the orphan; its records are readable.
    RawLake(tmp_path)
    assert not list(tmp_path.rglob("*.zst.tmp"))
    files = list(tmp_path.rglob("*.jsonl.zst"))
    assert len(files) == 1
    assert read_raw_file(files[0])[0]["payload"] == {"n": 1}


def test_recovery_skips_tmp_files_held_by_live_writer(tmp_path):
    # A live collector is mid-hour with an open part file...
    live = RawLake(tmp_path)
    live.append(
        venue="kalshi", endpoint="trades", params={}, http_status=200,
        elapsed_ms=1, payload={"live": True},
        fetched_at=datetime(2026, 7, 11, 20, 0, tzinfo=timezone.utc),
    )
    # ...when another pmkt invocation constructs its own RawLake (observed
    # live 2026-07-11: recovery stole the process's open file). The flock must
    # protect it.
    RawLake(tmp_path)
    assert len(list(tmp_path.rglob("*.zst.tmp"))) == 1, "live file untouched"
    assert not list(tmp_path.rglob("*.jsonl.zst"))

    live.close()  # graceful close still finalizes normally
    assert not list(tmp_path.rglob("*.zst.tmp"))
    files = list(tmp_path.rglob("*.jsonl.zst"))
    assert read_raw_file(files[0])[0]["payload"] == {"live": True}


def test_read_raw_file_salvages_truncated_trailing_frame(tmp_path):
    with RawLake(tmp_path) as lake:
        for i in range(3):
            lake.append(
                venue="kalshi", endpoint="x", params={}, http_status=200,
                elapsed_ms=1, payload={"i": i},
                fetched_at=datetime(2026, 7, 11, 5, 0, tzinfo=timezone.utc),
            )
    f = next(tmp_path.rglob("*.jsonl.zst"))
    # Tear the final frame mid-body, as a mid-write power loss would: each
    # record is one zstd frame (FLUSH_FRAME), so cut just past the last
    # frame's magic so its content is unrecoverable.
    data = f.read_bytes()
    frame_starts = [
        i for i in range(len(data) - 3) if data[i : i + 4] == b"\x28\xb5\x2f\xfd"
    ]
    # One frame per record plus a trailing empty frame from close().
    assert len(frame_starts) >= 3
    f.write_bytes(data[: frame_starts[2] + 6])
    records = read_raw_file(f)
    assert [r["payload"]["i"] for r in records] == [0, 1]


def test_part_filenames_unique_within_same_second(tmp_path):
    ts = datetime(2026, 7, 11, 6, 0, 0, tzinfo=timezone.utc)
    for _ in range(2):
        with RawLake(tmp_path) as lake:
            lake.append(
                venue="kalshi", endpoint="x", params={}, http_status=200,
                elapsed_ms=1, payload={}, fetched_at=ts,
            )
    # Two same-second opens of the same partition must not overwrite.
    assert len(list(tmp_path.rglob("*.jsonl.zst"))) == 2


def test_decimal_payload_serializes(tmp_path):
    from decimal import Decimal

    with RawLake(tmp_path) as lake:
        lake.append(
            venue="kalshi", endpoint="x", params={}, http_status=200,
            elapsed_ms=1, payload={"p": Decimal("0.4200")},
            fetched_at=datetime(2026, 7, 10, 1, 0, tzinfo=timezone.utc),
        )
    files = list(tmp_path.rglob("*.jsonl.zst"))
    assert read_raw_file(files[0])[0]["payload"]["p"] == "0.4200"


# -- per-record flush mode (FLUSH_FRAME default vs FLUSH_BLOCK for live WS) ----

WS_TS = datetime(2026, 9, 25, 14, 0, tzinfo=timezone.utc)


def _ws_trade_frame(i: int) -> dict:
    # Shape and size of a live Kalshi ws_trade frame (~540 B once wrapped in
    # the capture envelope) — prices are dollar strings (data invariants).
    return {
        "type": "trade",
        "sid": 1,
        "seq": i,
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


def _append_ws(lake: RawLake, payload: dict) -> None:
    lake.append(
        venue="kalshi", endpoint="ws_trade",
        params={"direction": "in", "channel": "trade", "sid": 1},
        http_status=0, elapsed_ms=0, payload=payload, fetched_at=WS_TS,
    )


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


def _duckdb_payloads(path) -> list[dict]:
    rows = duckdb.connect().execute(
        f"SELECT CAST(payload AS VARCHAR) FROM read_json('{path}', "
        "format='newline_delimited', columns={'payload': 'JSON'})"
    ).fetchall()
    return [json.loads(r[0]) for r in rows]


def _kill_writer(lake: RawLake) -> Path:
    """Simulate SIGKILL mid-hour: the fd goes away without the zstd frame
    ever being ended. Returns the orphaned .tmp path."""
    (writer,) = lake._writers.values()
    lake._writers.clear()
    writer._fh.close()
    return writer.tmp_path


def test_default_flush_mode_is_frame_per_record(tmp_path):
    lake = RawLake(tmp_path)
    assert lake.flush_mode == zstandard.FLUSH_FRAME  # REST: byte-for-byte unchanged
    with lake:
        for i in range(3):
            _append_ws(lake, _ws_trade_frame(i))
    (f,) = tmp_path.rglob("*.jsonl.zst")
    frames, terminated = _frames(f.read_bytes())
    assert frames >= 3 and terminated  # one frame per record (+ close()'s empty one)
    with pytest.raises(ValueError):
        RawLake(tmp_path, flush_mode=99)


def test_block_flush_is_much_smaller_for_small_ws_records(tmp_path):
    sizes = {}
    for mode in (zstandard.FLUSH_FRAME, zstandard.FLUSH_BLOCK):
        root = tmp_path / str(mode)
        with RawLake(root, flush_mode=mode) as lake:
            for i in range(2000):
                _append_ws(lake, _ws_trade_frame(i))
        (f,) = root.rglob("*.jsonl.zst")
        sizes[mode] = f.stat().st_size
        if mode == zstandard.FLUSH_BLOCK:
            assert _frames(f.read_bytes()) == (1, True)  # one frame, ended at close
    # Measured ~83% smaller on live ws_trade; require at least 3x here.
    assert sizes[zstandard.FLUSH_BLOCK] * 3 < sizes[zstandard.FLUSH_FRAME]


def test_block_flush_file_round_trips_all_readers(tmp_path):
    expected = [_ws_trade_frame(i) for i in range(500)]
    with RawLake(tmp_path, flush_mode=zstandard.FLUSH_BLOCK) as lake:
        for payload in expected:
            _append_ws(lake, payload)
    (f,) = tmp_path.rglob("*.jsonl.zst")
    assert [r["payload"] for r in read_raw_file(f)] == expected
    lines, truncated = _read_lines(f)
    assert not truncated
    assert [json.loads(ln)["payload"] for ln in lines] == expected
    assert _duckdb_payloads(f) == expected


def test_block_flush_crash_loses_at_most_the_final_record(tmp_path, caplog):
    expected = [_ws_trade_frame(i) for i in range(50)]
    lake = RawLake(tmp_path, flush_mode=zstandard.FLUSH_BLOCK)
    ends = []  # file size after each record: every record reaches the file
    for payload in expected:
        _append_ws(lake, payload)
        ends.append(next(tmp_path.rglob("*.zst.tmp")).stat().st_size)
    tmp = _kill_writer(lake)
    data = tmp.read_bytes()
    assert len(data) == ends[-1]
    assert _frames(data) == (1, False)  # genuinely unterminated

    # Killed between records: nothing lost, nothing to report.
    assert [r["payload"] for r in read_raw_file(tmp)] == expected
    lines, truncated = _read_lines(tmp)
    assert [json.loads(ln)["payload"] for ln in lines] == expected
    assert not truncated

    # Killed mid-write of the final record's block: only that record is lost.
    # Its compressed block is dropped whole, so the decoded bytes still end on
    # a record boundary — there is no partial line to report.
    torn = tmp_path / "torn.jsonl.zst"
    for cut in (ends[-2] + 1, (ends[-2] + ends[-1]) // 2, ends[-1] - 1):
        torn.write_bytes(data[:cut])
        assert [r["payload"] for r in read_raw_file(torn)] == expected[:-1]
        lines, truncated = _read_lines(torn)
        assert [json.loads(ln)["payload"] for ln in lines] == expected[:-1]
        assert not truncated

    # A big final record spans several blocks: tearing it mid-record decodes a
    # partial line, which both readers drop and report as a torn tail.
    big_root = tmp_path / "big"
    lake = RawLake(big_root, flush_mode=zstandard.FLUSH_BLOCK)
    small = [_ws_trade_frame(i) for i in range(2)]
    for payload in small:
        _append_ws(lake, payload)
    before_big = next(big_root.rglob("*.zst.tmp")).stat().st_size
    _append_ws(lake, {"blob": random.Random(0).randbytes(150_000).hex()})
    tmp = _kill_writer(lake)
    data = tmp.read_bytes()
    torn.write_bytes(data[: before_big + (len(data) - before_big) * 3 // 4])
    caplog.set_level(logging.WARNING)
    assert [r["payload"] for r in read_raw_file(torn)] == small
    assert "dropping partial trailing record" in caplog.text
    lines, truncated = _read_lines(torn)
    assert [json.loads(ln)["payload"] for ln in lines] == small
    assert truncated


def test_set_flush_mode_finalizes_open_writers(tmp_path):
    lake = RawLake(tmp_path, flush_mode=zstandard.FLUSH_BLOCK)
    _append_ws(lake, _ws_trade_frame(0))
    assert not lake.set_flush_mode(zstandard.FLUSH_BLOCK)  # no change: stays open
    assert list(tmp_path.rglob("*.zst.tmp"))

    assert lake.set_flush_mode(zstandard.FLUSH_FRAME)
    # The block-flushed file was closed with its frame ended: nothing left
    # open for a crash to orphan once its reader guarantee is gone.
    assert not list(tmp_path.rglob("*.zst.tmp"))
    (block_file,) = tmp_path.rglob("*.jsonl.zst")
    assert _frames(block_file.read_bytes()) == (1, True)

    for i in (1, 2):
        _append_ws(lake, _ws_trade_frame(i))
    lake.close()
    (frame_file,) = set(tmp_path.rglob("*.jsonl.zst")) - {block_file}
    frames, terminated = _frames(frame_file.read_bytes())
    assert frames >= 2 and terminated  # the new mode applied at once
    assert [r["payload"]["seq"] for f in (block_file, frame_file) for r in read_raw_file(f)] == [0, 1, 2]
    with pytest.raises(ValueError):
        lake.set_flush_mode(99)


def test_block_reader_lock_is_advertised_only_while_held(tmp_path):
    assert not block_reader_live(tmp_path)
    assert not (tmp_path / BLOCK_READER_LOCK).exists()  # probing never creates it
    held = hold_block_reader_lock(tmp_path)
    assert block_reader_live(tmp_path) and block_reader_live(tmp_path)  # probes never take it
    held.close()  # the collector exits (or dies: the kernel drops its flock)
    assert not block_reader_live(tmp_path)
    hold_block_reader_lock(tmp_path).close()  # re-acquirable after probes


def _pre_change_read_lines(path: Path, read_size: int) -> list[str]:
    """HEAD's compact._read_lines, which a collector started before
    decompress_raw_file keeps compacting with: stream_reader."""
    buf = bytearray()
    with open(path, "rb") as fh:
        reader = zstandard.ZstdDecompressor().stream_reader(fh, read_across_frames=True)
        try:
            while chunk := reader.read(read_size):
                buf += chunk
        except zstandard.ZstdError:
            pass
    lines = [ln for ln in buf.decode("utf-8").splitlines() if ln.strip()]
    if buf and not buf.endswith(b"\n"):
        lines.pop()
    return lines


@pytest.mark.parametrize("read_size", (64, 4096))
def test_ws_fallback_orphans_read_exactly_by_the_pre_change_decoder(tmp_path, read_size):
    """What the WS writer does beside a collector without the block-reader
    lock (ws_flush_mode): its SIGKILL orphans must lose nothing under that
    collector's decoder, which drops records of FLUSH_BLOCK orphans."""
    from pmkt.kalshi.ws import ws_flush_mode

    rng = random.Random(read_size)
    for trial in range(10):
        root = tmp_path / str(trial)
        lake = RawLake(root, flush_mode=ws_flush_mode(root))  # no lock under root
        assert lake.flush_mode == zstandard.FLUSH_FRAME
        expected = [_ws_trade_frame(rng.randrange(10**6)) for _ in range(rng.randint(1, 60))]
        for payload in expected:
            _append_ws(lake, payload)
        _kill_writer(lake)
        RawLake(root)  # next startup finalizes the orphan (rename only)
        (f,) = root.rglob("*.jsonl.zst")
        assert [json.loads(ln)["payload"] for ln in _pre_change_read_lines(f, read_size)] == expected


def test_orphan_recovery_finalizes_unterminated_block_flush_file(tmp_path):
    expected = [_ws_trade_frame(i) for i in range(20)]
    lake = RawLake(tmp_path, flush_mode=zstandard.FLUSH_BLOCK)
    for payload in expected:
        _append_ws(lake, payload)
    orphan = _kill_writer(lake)
    orphan_bytes = orphan.read_bytes()

    RawLake(tmp_path)  # next startup (any flush mode) recovers orphans
    assert not list(tmp_path.rglob("*.zst.tmp"))
    (f,) = tmp_path.rglob("*.jsonl.zst")
    # Renamed, never rewritten (data invariants): still an unterminated frame.
    assert f.read_bytes() == orphan_bytes
    assert _frames(orphan_bytes) == (1, False)
    assert [r["payload"] for r in read_raw_file(f)] == expected
    lines, truncated = _read_lines(f)
    assert [json.loads(ln)["payload"] for ln in lines] == expected
    assert not truncated
    # A typical small-record WS orphan also reads in DuckDB (not guaranteed
    # for every unterminated frame — see the rawlake module docstring).
    assert _duckdb_payloads(f) == expected


# -- the shared decoder: every file shape x input chunk size -------------------
# Regression for the stream_reader bug (2026-09-26): it silently dropped
# trailing complete records of an unterminated FLUSH_BLOCK frame depending on
# its read size. Both readers must return EXACTLY the complete records of every
# shape at every decoder chunk size; only a torn final record may be lost.

DECODE_SHAPES = (
    "frame_closed", "frame_torn", "block_closed", "block_unterminated", "block_torn",
)
DECODE_READ_SIZES = (64, 512, 4096, 65536)


def _random_payload(rng: random.Random, i: int) -> dict:
    # Mostly WS-shaped frames; some multi-KB pages; a few records past zstd's
    # 128 KiB block size, so tears land between blocks, inside one block, and
    # inside a multi-block record. Random hex keeps blocks from shrinking away.
    roll = rng.random()
    if roll < 0.5:
        return _ws_trade_frame(rng.randrange(10**6))
    if roll < 0.92:
        size = rng.randint(0, 600) if roll < 0.75 else rng.randint(1_000, 20_000)
    else:
        size = rng.randint(140_000, 260_000)
    return {"i": i, "blob": rng.randbytes(size // 2).hex()}


def _write_shape(root: Path, shape: str, payloads: list[dict], rng: random.Random) -> Path:
    mode = zstandard.FLUSH_FRAME if shape.startswith("frame") else zstandard.FLUSH_BLOCK
    lake = RawLake(root, flush_mode=mode)
    ends = []  # file size after each record
    for payload in payloads:
        _append_ws(lake, payload)
        ends.append(next(root.rglob("*.zst.tmp")).stat().st_size)
    if shape.endswith("closed"):
        lake.close()
    else:
        tmp = _kill_writer(lake)
        if shape.endswith("torn"):  # cut strictly inside the final record's bytes
            tmp.write_bytes(tmp.read_bytes()[: rng.randint(ends[-2] + 1, ends[-1] - 1)])
        RawLake(root)  # next startup finalizes the orphan (rename only)
    (f,) = root.rglob("*.jsonl.zst")
    return f


@pytest.mark.parametrize("read_size", DECODE_READ_SIZES)
@pytest.mark.parametrize("shape", DECODE_SHAPES)
def test_decoder_returns_exactly_the_complete_records(tmp_path, monkeypatch, shape, read_size):
    monkeypatch.setattr("pmkt.rawlake._DECODE_READ_SIZE", read_size)
    rng = random.Random(f"{shape}/{read_size}")
    torn = shape.endswith("torn")
    for trial in range(6):
        payloads = [_random_payload(rng, i) for i in range(rng.randint(2, 30))]
        f = _write_shape(tmp_path / str(trial), shape, payloads, rng)
        expected = payloads[:-1] if torn else payloads

        assert [r["payload"] for r in read_raw_file(f)] == expected
        lines, truncated = _read_lines(f)
        assert [json.loads(ln)["payload"] for ln in lines] == expected
        # truncated <=> a partial trailing line decoded (one-shot decode as the
        # oracle); never for a closed file or a between-records orphan.
        whole = zstandard.ZstdDecompressor().decompressobj(read_across_frames=True)
        assert truncated == (not whole.decompress(f.read_bytes()).endswith(b"\n"))
        if not torn:
            assert not truncated
        # DuckDB ignores the supplied chunking. Terminated frames only: DuckDB 1.5.4
        # raises "Malformed JSON" on ~1/300 unterminated FLUSH_BLOCK orphans
        # (measured 2026-09-26) — see the rawlake module docstring.
        if shape.endswith("closed") and read_size == DECODE_READ_SIZES[-1]:
            assert _duckdb_payloads(f) == expected


@pytest.mark.parametrize("read_size", DECODE_READ_SIZES)
@pytest.mark.parametrize(
    ("mode", "close", "tail", "corrupt"),
    [
        (zstandard.FLUSH_FRAME, True, b"\xff" * 64, True),  # garbage after last frame
        (zstandard.FLUSH_FRAME, True, b"\x00" * 4096, True),  # zero-filled tail
        (zstandard.FLUSH_BLOCK, True, b"\xff" * 64, True),
        (zstandard.FLUSH_BLOCK, False, b"\xff" * 64, True),  # reserved block type
        # Zeros inside an open frame parse as empty raw blocks: nothing to flag.
        (zstandard.FLUSH_BLOCK, False, b"\x00" * 4096, False),
    ],
    ids=["frame-ff", "frame-zeros", "block-closed-ff", "block-open-ff", "block-open-zeros"],
)
def test_decoder_salvages_every_record_before_corruption(
    tmp_path, monkeypatch, caplog, read_size, mode, close, tail, corrupt
):
    monkeypatch.setattr("pmkt.rawlake._DECODE_READ_SIZE", read_size)
    payloads = [_ws_trade_frame(i) for i in range(300)]  # spans several 64 KiB chunks
    lake = RawLake(tmp_path, flush_mode=mode)
    for payload in payloads:
        _append_ws(lake, payload)
    if close:
        lake.close()
        (f,) = tmp_path.rglob("*.jsonl.zst")
    else:
        f = _kill_writer(lake)
    f.write_bytes(f.read_bytes() + tail)
    caplog.set_level(logging.WARNING)

    # decompress() discards a failing call's whole output; the byte-granular
    # replay must still hand back every record decoded before the bad bytes.
    assert [r["payload"] for r in read_raw_file(f)] == payloads
    lines, truncated = _read_lines(f)
    assert [json.loads(ln)["payload"] for ln in lines] == payloads
    assert truncated == corrupt
    assert ("undecodable zstd data" in caplog.text) == corrupt
