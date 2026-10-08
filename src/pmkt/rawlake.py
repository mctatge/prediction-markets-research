"""Raw capture lake — the system of record (storage invariants).

Every API response is appended here as one JSON line BEFORE any parsing:

    {v, venue, endpoint, params, fetched_at, http_status, elapsed_ms, payload}

Layout: raw/venue=kalshi/endpoint=markets/date=2026-07-10/hour=14/part-*.jsonl.zst

Files are written through a zstd stream flushed after EVERY record, so each
complete record reaches the file immediately. The flush mode is chosen per
RawLake (``flush_mode=``):

- ``zstandard.FLUSH_FRAME`` (default — the REST collectors): each record is
  closed as its own zstd frame. Compression context resets per record, which
  is fine for large REST pages.
- ``zstandard.FLUSH_BLOCK`` (the live WS feeds): one frame stays open for the
  part file's lifetime and each record ends a zstd block, so the compression
  window carries across records. Small (~540 B) WS frames compressed only
  ~1.49x as one-frame-per-record; block flushes measured ~83% smaller files.
  The frame is ended on rotation/close.

Open files carry a .tmp suffix and are atomically renamed on rotation/close;
after a hard crash (SIGKILL, power loss) the orphaned .tmp files are finalized
(renamed, never rewritten) on the next RawLake startup. In FLUSH_BLOCK mode
that leaves an unterminated frame, which `read_raw_file` and
compact._read_lines (both via `decompress_raw_file`) decode through its last
complete block. In either mode at most the final in-flight record can be
lost — `read_raw_file` tolerates a torn tail. Raw files are never deleted;
nightly compaction to Parquet is a later, additive step.

DuckDB read_json caveat (measured 2026-09-26, DuckDB 1.5.4): it reads every
terminated file, but raises "Malformed JSON" on ~1 in 300 unterminated
FLUSH_BLOCK orphans (never a silent undercount in 340 trials). No in-repo
DuckDB reader scans ws_* raw jsonl (builders are endpoint-scoped and
compaction decodes in Python), so this only bites ad-hoc queries over the
`raw` view of a not-yet-compacted day that holds such an orphan.

Deploy skew (BLOCK_READER_LOCK): the collector daemon compacts in-process
with the decoder it STARTED with. One started before `decompress_raw_file`
decodes with stream_reader, which drops trailing records of unterminated
FLUSH_BLOCK orphans, and compact_all_pending never re-verifies the short
parquet it finalizes. A current collector holds BLOCK_READER_LOCK for its
lifetime; the WS writer block-flushes only while it is held (kalshi/ws.py),
so the two processes can restart in any order.
"""

from __future__ import annotations

from .locking import fcntl
import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO

import zstandard

from .capture_guard import CaptureGuard, CapturePaused

logger = logging.getLogger(__name__)

CAPTURE_SCHEMA_VERSION = 1

_FLUSH_MODES = (zstandard.FLUSH_FRAME, zstandard.FLUSH_BLOCK)
# Compressed bytes fed to the decoder per call — modest, so a big part file is
# never held compressed in memory as a whole (decoded bytes still accumulate).
_DECODE_READ_SIZE = 65536
# <data_dir>/<name>: flocked for its whole lifetime by a collector daemon
# whose compaction decodes raw files with decompress_raw_file (module
# docstring, "Deploy skew"). A new name if the decoder ever changes again.
BLOCK_READER_LOCK = "collector.block-reader.lock"


def hold_block_reader_lock(data_dir: Path):
    """Advertise, for this process's lifetime, that its in-process compaction
    reads unterminated FLUSH_BLOCK orphans exactly. Returns the held file;
    closing it (or process death) withdraws the claim. Blocking is fine: the
    only other locker is block_reader_live's momentary shared probe."""
    path = Path(data_dir) / BLOCK_READER_LOCK
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(path, "a")
    fcntl.flock(fh, fcntl.LOCK_EX)
    return fh


def block_reader_live(data_dir: Path) -> bool:
    """True while some live process holds BLOCK_READER_LOCK. Never creates
    the file; any probe failure answers False (the safe per-record frames)."""
    try:
        fh = open(Path(data_dir) / BLOCK_READER_LOCK, "rb")
    except OSError:
        return False
    with fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        except OSError:
            return False
        fcntl.flock(fh, fcntl.LOCK_UN)
        return False


class RawLake:
    def __init__(
        self,
        root: Path,
        *,
        capture_guard: CaptureGuard | None = None,
        flush_mode: int = zstandard.FLUSH_FRAME,
    ) -> None:
        if flush_mode not in _FLUSH_MODES:
            raise ValueError(
                f"flush_mode must be zstandard.FLUSH_FRAME or FLUSH_BLOCK, got {flush_mode!r}"
            )
        self.capture_guard = capture_guard
        self.flush_mode = flush_mode
        self.root = Path(root)
        self._lock = threading.Lock()
        # One open writer per (venue, endpoint, date, hour) partition.
        self._writers: dict[tuple[str, str, str, str], _PartWriter] = {}
        self._recover_orphans()

    def _recover_orphans(self) -> None:
        """Finalize part files orphaned by a hard crash (storage invariants: raw
        capture is the system of record — orphans must not go dark). Complete
        zstd frames (FLUSH_FRAME) or blocks of a still-open frame (FLUSH_BLOCK)
        survive in the .tmp; readers skip a torn tail.

        Live writers hold an flock on their .tmp, so recovery skips them —
        without this, any pmkt invocation would steal the running process's
        open files (observed live 2026-07-11)."""
        if not self.root.exists():
            return
        for tmp in self.root.rglob("*.jsonl.zst.tmp"):
            try:
                probe = open(tmp, "rb")
            except OSError:
                continue
            try:
                try:
                    fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue  # a live writer owns this file
                final = tmp.with_name(tmp.name[: -len(".tmp")])
                if final.exists():  # can't happen with uuid names; never clobber
                    final = final.with_name(f"{final.stem}-r{uuid.uuid4().hex[:8]}.zst")
                os.replace(tmp, final)
                logger.warning("recovered orphaned raw part file: %s", final)
            finally:
                probe.close()

    def append(
        self,
        *,
        venue: str,
        endpoint: str,
        params: dict[str, Any] | None,
        http_status: int,
        elapsed_ms: int,
        payload: Any,
        fetched_at: datetime | None = None,
    ) -> None:
        if self.capture_guard is not None:
            self.capture_guard.check()
        ts = fetched_at or datetime.now(timezone.utc)
        record = {
            "v": CAPTURE_SCHEMA_VERSION,
            "venue": venue,
            "endpoint": endpoint,
            "params": params or {},
            "fetched_at": ts.isoformat(),
            "http_status": http_status,
            "elapsed_ms": elapsed_ms,
            "payload": payload,
        }
        line = json.dumps(record, separators=(",", ":"), default=str) + "\n"
        key = (venue, endpoint, ts.strftime("%Y-%m-%d"), ts.strftime("%H"))
        with self._lock:
            writer = self._writers.get(key)
            if writer is None:
                # Hour rolled over (or first write): close stale writers for
                # this venue/endpoint and open a fresh partition file.
                for old_key in [
                    k for k in self._writers if k[0] == venue and k[1] == endpoint
                ]:
                    self._writers.pop(old_key).close()
                writer = _PartWriter(self._partition_dir(key), ts, self.flush_mode)
                self._writers[key] = writer
            writer.write(line.encode("utf-8"))

    def set_flush_mode(self, flush_mode: int) -> bool:
        """Switch the flush mode of part files; returns whether it changed.

        Open writers are finalized on a change, so the new mode applies at
        once — in particular no FLUSH_BLOCK file stays open (and could be
        orphaned by a crash) after its reader guarantee is gone."""
        if flush_mode not in _FLUSH_MODES:
            raise ValueError(
                f"flush_mode must be zstandard.FLUSH_FRAME or FLUSH_BLOCK, got {flush_mode!r}"
            )
        with self._lock:
            if flush_mode == self.flush_mode:
                return False
            self.flush_mode = flush_mode
            for writer in self._writers.values():
                writer.close()
            self._writers.clear()
        return True

    async def wait_for_capture(self) -> None:
        if self.capture_guard is not None:
            await self.capture_guard.wait_until_ready()

    async def append_when_ready(self, **record: Any) -> None:
        """Keep an in-flight REST response until it can be captured verbatim.

        Never return it to parsing/watermark code without a durable raw write.
        Preserve its original fetch time while a low-space pause is pending.
        """
        record.setdefault("fetched_at", datetime.now(timezone.utc))
        while True:
            await self.wait_for_capture()
            try:
                self.append(**record)
            except CapturePaused:
                continue  # space crossed the threshold between probe and append
            return

    def _partition_dir(self, key: tuple[str, str, str, str]) -> Path:
        venue, endpoint, date, hour = key
        return self.root / f"venue={venue}" / f"endpoint={endpoint}" / f"date={date}" / f"hour={hour}"

    def rotate_idle(self, now: datetime | None = None) -> int:
        """Finalize writers whose (date, hour) partition is no longer current.

        Rotation normally happens lazily on the next write to the same
        endpoint — but a low-frequency endpoint (3h metadata cadence, quiet WS
        control channel) can pin yesterday's last partition as a .tmp for
        hours, deferring the whole day's compaction. Call opportunistically
        (the run loop's hourly compaction guard does)."""
        ts = now or datetime.now(timezone.utc)
        current = (ts.strftime("%Y-%m-%d"), ts.strftime("%H"))
        closed = 0
        with self._lock:
            for key in [k for k in self._writers if (k[2], k[3]) != current]:
                self._writers.pop(key).close()
                closed += 1
        return closed

    def close(self) -> None:
        with self._lock:
            for writer in self._writers.values():
                writer.close()
            self._writers.clear()

    def __enter__(self) -> "RawLake":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class _PartWriter:
    def __init__(
        self,
        directory: Path,
        opened_at: datetime,
        flush_mode: int = zstandard.FLUSH_FRAME,
    ) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self._flush_mode = flush_mode
        stamp = opened_at.strftime("%Y%m%dT%H%M%S")
        # uuid fragment: same-second reopens of a partition must never collide
        # (os.replace would silently overwrite the earlier finalized part).
        unique = uuid.uuid4().hex[:8]
        self.final_path = directory / f"part-{stamp}-{os.getpid()}-{unique}.jsonl.zst"
        self.tmp_path = self.final_path.with_suffix(".zst.tmp")
        self._fh = open(self.tmp_path, "wb")
        # Mark this .tmp as live so another process's orphan recovery can't
        # rename it while it is open; released when the fd closes.
        fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self._stream = zstandard.ZstdCompressor(level=6).stream_writer(
            self._fh, closefd=False
        )

    def write(self, data: bytes) -> None:
        self._stream.write(data)
        # Flush per record so completed records always reach the file:
        # FLUSH_FRAME closes a frame per record; FLUSH_BLOCK ends a block but
        # keeps the frame (and its compression window) open until close().
        self._stream.flush(self._flush_mode)

    def close(self) -> None:
        self._stream.close()
        self._fh.flush()
        os.fsync(self._fh.fileno())
        # Rename while still holding the flock, THEN release it — closing
        # first would open a window for recovery to grab the file.
        os.replace(self.tmp_path, self.final_path)
        self._fh.close()


def decompress_raw_file(path: Path) -> tuple[bytearray, bool]:
    """Decompress one raw part file as far as it decodes: (bytes, corrupt).

    The one decoder for raw part files (read_raw_file, compact._read_lines).
    Every complete record was flushed as a complete frame (FLUSH_FRAME) or a
    complete block of an open frame (FLUSH_BLOCK), so a torn or unterminated
    tail — a recovered crash orphan — simply stops yielding output at EOF: no
    error, and the caller drops any trailing partial line. An unterminated
    FLUSH_BLOCK orphan that stopped between records decodes COMPLETELY.

    Chunked ``decompressobj``, never ``stream_reader``: stream_reader drops
    trailing complete records of an unterminated FLUSH_BLOCK frame depending
    on the read size (measured 2026-09-26, zstandard 0.25: 30/30 random
    orphans lost data at read(64), 11/30 at read(4096)), while decompressobj
    fed input chunks decodes every file shape exactly at any chunk size.
    ``corrupt`` is True only when zstd raised on undecodable bytes; the
    decoded prefix is still returned (warned here).
    """
    # bytearray, not bytes += : quadratic concatenation timed out on real
    # multi-MB captures (found live 2026-07-11 on a 37MB polymarket page).
    buf = bytearray()
    dobj = zstandard.ZstdDecompressor().decompressobj(read_across_frames=True)
    with open(path, "rb") as fh:
        while chunk := fh.read(_DECODE_READ_SIZE):
            try:
                buf += dobj.decompress(chunk)
            except zstandard.ZstdError as exc:
                bad_at = fh.tell() - len(chunk)
                logger.warning(
                    "undecodable zstd data in %s (input chunk at byte %d: %s) — "
                    "salvaging the decoded prefix",
                    path, bad_at, exc,
                )
                buf += _decode_until_error(fh, bad_at, chunk)
                return buf, True
    return buf, False


def _decode_until_error(fh: BinaryIO, bad_at: int, chunk: bytes) -> bytearray:
    """Output of the failing ``chunk`` up to the corrupt byte.

    decompress() discards everything it decoded from a call that raises, so
    replay the good prefix (whose output the caller already holds) into a
    fresh decoder, then feed the failing chunk a byte at a time: every block
    completed before the corruption is kept. Rare path — corrupt files only.
    """
    dobj = zstandard.ZstdDecompressor().decompressobj(read_across_frames=True)
    fh.seek(0)
    remaining = bad_at
    while remaining:
        piece = fh.read(min(_DECODE_READ_SIZE, remaining))
        if not piece:
            break  # file shrank during replay; keep what the replay can give
        remaining -= len(piece)
        dobj.decompress(piece)  # already decoded into the caller's buffer
    out = bytearray()
    for i in range(len(chunk)):
        try:
            out += dobj.decompress(chunk[i : i + 1])
        except zstandard.ZstdError:
            break
    return out


def read_raw_file(path: Path) -> list[dict[str, Any]]:
    """Decompress and parse one raw part file.

    Tolerates a torn or unterminated tail (recovered crash orphans — see
    decompress_raw_file) and salvages the decoded prefix of a corrupt file;
    any trailing partial line is dropped with a warning.
    """
    buf, _corrupt = decompress_raw_file(path)
    records = []
    for line in buf.decode("utf-8", errors="replace").splitlines():
        if line.strip():
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning("dropping partial trailing record in %s", path)
    return records
