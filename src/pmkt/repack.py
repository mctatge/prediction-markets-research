"""Lossless in-place re-pack of finished Kalshi WS raw part files.

The live WS feeds wrote ``raw/venue=kalshi/endpoint=ws_*/date=D/hour=H/
part-*.jsonl.zst`` one zstd frame PER RECORD (~540 B each, compressing only
~1.49x; ws_trade alone is ~148 GiB). Decompressing a finished hour file and
recompressing it as ONE frame (level 9, content checksum on) shrinks it to
~10% (benchmarked 118 MB → 12 MB in ~1.6 s). The raw lake is the system of
record (storage invariants), so the re-pack must be provably lossless and
crash-safe at every step. Run as ``uv run python -m pmkt.repack --help``.

What "lossless" means here
--------------------------
The unit of truth is the DECOMPRESSED byte stream — the JSONL text every
reader (rawlake.read_raw_file, compact._read_lines, DuckDB read_json) sees.
A re-packed file is accepted only when its decompressed bytes are
SHA-256-identical to the original's (byte and newline counts equal too). The
compressed bytes change by design; nothing else does: same directory, same
filename, same permission bits, same atime/mtime. Nothing is ever deleted —
the only unlink this tool performs is of its own temp files.

Decoding — never stream_reader
------------------------------
``ZstdDecompressor().stream_reader(fh, read_across_frames=True)`` read in
chunks DROPS trailing complete records from an unterminated FLUSH_BLOCK frame
(measured 2026-09-26, zstandard 0.25.0: 30/30 random trials lost data at
read(64), 11/30 at read(4096)). Every decode here therefore feeds input
chunks to ``decompressobj`` objects — two independent ways in the same pass:

1. primary: ONE ``decompressobj(read_across_frames=True)`` over the whole
   stream; its output is what gets re-compressed and digested;
2. cross-check: one plain ``decompressobj()`` PER FRAME, chained through
   ``unused_data``. Its ``eof`` is exact per frame, so it counts frames and
   decides TERMINATION. (The primary's ``eof`` cannot: in read_across_frames
   mode it never turns True, even after a complete terminated frame — it
   keeps waiting for a next one; measured on zstandard 0.25.0.)

Both digests must agree before anything is written into place. The temp
re-decode (step 4) runs the same decoder over the new bytes, so a decoder
that silently drops output from the ORIGINAL would be a common-mode failure
of that check — the independent per-frame decode is what catches it.

Terminated frames only
----------------------
Only files ending EXACTLY on a complete frame boundary are re-packed. An
unterminated frame (a crash orphan RawLake._recover_orphans finalized after a
SIGKILL / ``launchctl kickstart -k``), a torn frame, trailing non-frame bytes,
a decoded stream not ending in ``\\n`` or any ZstdError → the file is skipped
and never touched. Those are exactly the shapes where decoders have disagreed;
leaving their bytes alone costs little and preserves every reader's current
torn-tail salvage behaviour.

Per-file protocol (repack_file — a pure top-level function for the pool)
------------------------------------------------------------------------
1. lstat the original (size, mtime_ns, atime_ns, st_ino, st_mode); skip
   symlinks, non-regular files and hard-linked files (st_nlink > 1).
2. Stream it once: SHA-256 of the COMPRESSED bytes; both decodes; SHA-256 +
   byte count + newline count of the decompressed bytes; simultaneously
   stream-compress into a temp file in the SAME directory (one frame).
3. flush + fsync the temp, close it.
4. Verify: fully re-decode the temp (both decoders); it must be exactly one
   terminated frame whose content digest/bytes/lines equal step 2's
   (``failed-verify`` otherwise — loud).
5. new_size >= old_size → ``skipped-no-gain``.
6. Re-stat the original: size, mtime_ns and inode unchanged, else
   ``skipped-changed``.
7. chmod the temp to the original's mode; utime it to (atime_ns, mtime_ns).
8. Append an fsynced intent line (old + new digests) to
   ``<data-dir>/repack/intents.jsonl``; ``os.replace(temp, original)``;
   fsync the directory.
9. Re-decode the final path; a content mismatch is a HARD error that stops
   the run non-zero (``failed-post-replace``): storage is suspect, so once
   it is recorded queued files never start and in-flight ones stop before
   their rename (``skipped-interrupted``).

Every non-success exit before step 8 unlinks the temp and leaves the original
untouched; any exception (ENOSPC mid-write included) surfaces as ``error``,
never as success.

Crash safety
------------
The original is never opened for writing: it is only ever replaced by
rename(2) of a fully written, fsynced, re-verified sibling on the same
filesystem, so at every instant the path names either the complete original
or the complete verified replacement. A crash before step 8 leaves the
original plus a stale temp; a crash after leaves the verified replacement.
fsyncs use F_FULLFSYNC where available (plain macOS fsync does not flush the
drive cache). Temps are named ``.<name>.repack-<hex8>`` — dot-prefixed,
ending in neither ``.jsonl.zst`` nor ``.jsonl.zst.tmp`` — so no lake reader
glob (``part-*.jsonl.zst``, ``*.jsonl.zst``), compact's .tmp deferral, nor
RawLake._recover_orphans (``*.jsonl.zst.tmp``) ever sees one. The next run,
holding ``<data-dir>/repack.lock`` (so no live temps can exist), removes
stale temps matching ONLY that pattern inside the partitions it targets.

Eligibility and idempotency
---------------------------
A date partition is processed only if its UTC day is complete (strictly
before today), it holds no ``*.jsonl.zst.tmp`` (a live or crashed writer —
same deferral rule as compact.py) and, by default, it is already compacted
(``raw-parquet/.../part-0.parquet`` exists — lake readers then serve the
Parquet and the raw file is the backstop). Decompressed text is unchanged,
so compact's row-count idempotency check keeps reporting skipped-existing.
The parent alone appends one fsynced line per processed file to
``<data-dir>/repack/manifest.jsonl``; files with a ``repacked`` entry whose
new_size equals the current size are skipped on rerun, and ``--verify``
re-decodes every such file against its recorded content digest.

Every rename is recorded
------------------------
A rename whose manifest line never lands (run SIGKILLed, rebooted, or dying
between steps 8 and the parent's write) would otherwise look like a plain
single-frame file forever: skipped-no-gain on rerun, invisible to --verify.
So: (a) the step-8 intent line is durable BEFORE the rename, and a rerun
finding a file whose compressed digest equals its latest intent's new digest
finishes step 9 against the intent's ORIGINAL digests and records it as
``repacked``; (b) a checksummed single frame (every re-pack output; RawLake
never writes checksums) with neither record is recorded as
``repacked-unrecorded`` with its current digests; (c) --verify reports any
uncovered checksummed file in scope as ``unrecorded`` (not OK). Ctrl-C does
not race any of this: while a run is active SIGINT only sets a flag; pool
workers ignore it, start nothing new and rename nothing more
(``skipped-interrupted``); the parent records every in-flight result, then
exits 130. Workers also refuse to rename once their parent is gone and exit
within ~1 s of it.
"""

from __future__ import annotations

import argparse
import contextlib
from .locking import fcntl
import fnmatch
import hashlib
import json
import logging
import multiprocessing
import os
import re
import signal
import stat
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import FIRST_COMPLETED, BrokenExecutor, ProcessPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterable, Iterator

import zstandard

from .compact import PARQUET_PART_NAME

logger = logging.getLogger(__name__)

VENUE = "kalshi"
DEFAULT_ENDPOINT_GLOB = "ws_*"
DEFAULT_LEVEL = 9
DEFAULT_WORKERS = 2
LOCK_NAME = "repack.lock"
MANIFEST_DIR = "repack"
MANIFEST_NAME = "manifest.jsonl"
INTENTS_NAME = "intents.jsonl"

# Input is read in 1 MiB chunks. The per-frame cross decoder is fed 16 KiB
# slices of each chunk: after every frame it continues from unused_data, a
# copy of the rest of the slice, so big slices cost O(slice) per ~540 B
# frame (1 MiB slices ran ~3.6x slower than 16 KiB on a 200k-frame file).
_READ_CHUNK = 1 << 20
_FRAME_SLICE = 16 << 10

# ".<part file name>.repack-<8 hex>" — the ONLY names this tool ever unlinks.
_TEMP_INFIX = ".repack-"
_TEMP_RE = re.compile(r"\.part-[^/]+\.jsonl\.zst\.repack-[0-9a-f]{8}")

# Loud statuses: any of them makes the run exit non-zero.
FAILURE_STATUSES = frozenset({"failed-verify", "error", "failed-post-replace"})
# Statuses whose byte counts feed the before/after/freed summary.
_SIZED_STATUSES = frozenset({"repacked", "would-repack"})
# Manifest statuses that describe a file's CURRENT bytes: skipped on rerun
# (while the size still matches) and re-decoded by --verify.
_COVERED_STATUSES = frozenset({"repacked", "repacked-unrecorded"})
# How often a pool worker checks that its parent (the run) is still alive.
_ORPHAN_POLL_S = 1.0


class RepackLocked(RuntimeError):
    """Another repack run holds <data-dir>/repack.lock."""


class RepackInterrupted(KeyboardInterrupt):
    """Ctrl-C stopped a run after every in-flight result was recorded."""

    def __init__(self, not_started: int) -> None:
        super().__init__(f"interrupted; {not_started} task(s) not started")
        self.not_started = not_started


# ---- pool-worker guards ------------------------------------------------------
# Only the parent records results, so a worker must never rename a file the
# parent will not record: not after Ctrl-C (the parent drains and records
# what is in flight, then stops) and not once the parent is gone (a SIGKILLed
# run leaves spawn workers reparented, still draining the call queue).

_STOP_EVENT: Any = None  # multiprocessing Event, set by _run_pool on Ctrl-C or a hard stop
_OWNER_PID: int | None = None  # the run's pid; None when running in-process


def _init_worker(stop: Any, owner_pid: int) -> None:
    global _STOP_EVENT, _OWNER_PID
    # Ctrl-C belongs to the parent; a worker must not die between a rename
    # and handing its result back.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    _STOP_EVENT, _OWNER_PID = stop, owner_pid
    threading.Thread(target=_exit_when_orphaned, name="repack-orphan-watch", daemon=True).start()


def _exit_when_orphaned() -> None:
    # An orphan would otherwise block forever on the call queue. Exiting is
    # safe at any point: before a rename it leaves a stale temp (removed by
    # the next run); after one, the intent line lets the next run record it.
    while os.getppid() == _OWNER_PID:
        time.sleep(_ORPHAN_POLL_S)
    os._exit(1)


def _rename_refusal() -> str | None:
    """Why this worker must not start a task or rename now (None = go)."""
    if _STOP_EVENT is not None and _STOP_EVENT.is_set():
        return "run aborted (Ctrl-C or a hard error) — no further renames"
    if _OWNER_PID is not None and os.getppid() != _OWNER_PID:
        return f"owning run (pid {_OWNER_PID}) is gone — nothing would record this rename"
    return None


@contextlib.contextmanager
def _sigint_flag(flag: list[bool]) -> Iterator[None]:
    """While a run is active, SIGINT only appends to ``flag`` (lock-free, so
    safe in a handler): a KeyboardInterrupt raised between a result's arrival
    and its manifest line would lose that record."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.signal(signal.SIGINT, lambda signum, frame: flag.append(True))
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)


# ---- low-level helpers (module-level so tests can inject faults) ----------

def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _validate_date(date: str) -> str:
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"date must be YYYY-MM-DD, got {date!r}") from exc
    return date


def _temp_path(path: Path) -> Path:
    return path.with_name(f".{path.name}{_TEMP_INFIX}{uuid.uuid4().hex[:8]}")


def is_repack_temp(name: str) -> bool:
    """True only for this tool's own temp-file names."""
    return _TEMP_RE.fullmatch(name) is not None


def _open_temp(tmp: Path) -> BinaryIO:
    # O_EXCL: never adopt an existing file; O_NOFOLLOW: never write through
    # a symlink. 0600 until step 7 copies the original's mode.
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    return open(fd, "wb")


def _compressor(level: int) -> zstandard.ZstdCompressor:
    return zstandard.ZstdCompressor(level=level, write_checksum=True)


def _fsync(fd: int) -> None:
    """F_FULLFSYNC where the platform has it: macOS fsync(2) hands data to
    the drive but does not flush its cache, so a power cut could reorder the
    temp's data behind the rename."""
    full = getattr(fcntl, "F_FULLFSYNC", None)
    if full is not None:
        try:
            fcntl.fcntl(fd, full)
            return
        except OSError:
            pass  # not supported on this fd/filesystem — plain fsync below
    os.fsync(fd)


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        _fsync(fd)
    finally:
        os.close(fd)


def _replace_into_place(tmp: Path, path: Path) -> None:
    """Step 8: atomic same-directory rename, then make the rename durable."""
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _append_intent(log_path: str, record: dict[str, Any]) -> None:
    """Step 8's intent line: ONE O_APPEND write + fsync, so lines from
    concurrent workers never interleave and the intent is durable before the
    rename it announces (the parent pre-creates the file and fsyncs its dir)."""
    fd = os.open(log_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, (json.dumps(record, sort_keys=True) + "\n").encode())
        _fsync(fd)
    finally:
        os.close(fd)


def _has_checksum(fd: int) -> bool:
    """Whether the zstd frame at offset 0 carries a content checksum — true
    of every re-pack output, never of RawLake's writer (no write_checksum)."""
    try:
        return zstandard.get_frame_parameters(os.pread(fd, 18, 0)).has_checksum
    except zstandard.ZstdError:
        return False


def _primary_decompressobj() -> Any:
    """Decoder 1 of _decode (read_across_frames) — a seam for fault tests."""
    return zstandard.ZstdDecompressor().decompressobj(read_across_frames=True)


def _decode(fh: BinaryIO, sink: Callable[[bytes], Any] | None = None) -> dict[str, Any]:
    """Stream-decode one zstd file two independent ways (module docstring).

    Returns digests/counts of the compressed input, of the primary decode
    (read_across_frames decompressobj — its output also goes to ``sink``)
    and of the per-frame cross decode, plus the complete-frame count and
    whether the input ended exactly on a frame boundary. A ZstdError stops
    the decode and is reported in ``error`` as (status, reason): an error
    opening a new frame after at least one complete frame means unused
    trailing bytes (``skipped-unterminated``), anything else is
    ``skipped-undecodable``. Exceptions from ``sink`` propagate.
    """
    comp_sha, content_sha, cross_sha = hashlib.sha256(), hashlib.sha256(), hashlib.sha256()
    comp_bytes = content_bytes = cross_bytes = lines = frames = 0
    last = b""
    error: tuple[str, str] | None = None
    primary = _primary_decompressobj()
    dctx = zstandard.ZstdDecompressor()
    frame = dctx.decompressobj()
    frame_fed = False  # has the current per-frame decoder consumed any input?
    while error is None and (chunk := fh.read(_READ_CHUNK)):
        offset = comp_bytes
        comp_sha.update(chunk)
        comp_bytes += len(chunk)
        view = memoryview(chunk)
        at_boundary = True
        try:
            for i in range(0, len(view), _FRAME_SLICE):
                pending: bytes | memoryview = view[i : i + _FRAME_SLICE]
                while len(pending):
                    at_boundary = not frame_fed
                    frame_fed = True
                    out = frame.decompress(pending)
                    cross_sha.update(out)
                    cross_bytes += len(out)
                    if not frame.eof:
                        break
                    frames += 1
                    pending = frame.unused_data
                    frame = dctx.decompressobj()
                    frame_fed = False
        except zstandard.ZstdError as exc:
            if at_boundary and frames:
                error = ("skipped-unterminated",
                         f"unused trailing bytes after complete frame {frames} "
                         f"(chunk at offset {offset}): {exc}")
            else:
                error = ("skipped-undecodable",
                         f"per-frame decode failed after {frames} complete "
                         f"frame(s) (chunk at offset {offset}): {exc}")
            break
        try:
            out = primary.decompress(chunk)
        except zstandard.ZstdError as exc:
            error = ("skipped-undecodable",
                     f"read_across_frames decode failed (chunk at offset {offset}): {exc}")
            break
        if out:
            content_sha.update(out)
            content_bytes += len(out)
            lines += out.count(b"\n")
            last = out[-1:]
            if sink is not None:
                sink(out)
    return {
        "compressed_sha256": comp_sha.hexdigest(),
        "compressed_bytes": comp_bytes,
        "content_sha256": content_sha.hexdigest(),
        "content_bytes": content_bytes,
        "lines": lines,
        "ends_with_newline": last == b"\n",
        "cross_sha256": cross_sha.hexdigest(),
        "cross_bytes": cross_bytes,
        "frames": frames,
        "terminated": error is None and frames > 0 and not frame_fed,
        "error": error,
    }


def _decoders_agree(d: dict[str, Any]) -> bool:
    return d["content_sha256"] == d["cross_sha256"] and d["content_bytes"] == d["cross_bytes"]


def _single_frame_problems(d: dict[str, Any], want: dict[str, Any]) -> list[str]:
    """Why decode ``d`` of a re-packed file does NOT match original ``want``."""
    problems = []
    if d["error"] is not None:
        problems.append(f"decode error: {d['error'][1]}")
    if not d["terminated"]:
        problems.append("frame not terminated")
    if d["frames"] != 1:
        problems.append(f"expected exactly 1 frame, found {d['frames']}")
    if not _decoders_agree(d):
        problems.append("primary and per-frame decoders disagree")
    for key in ("content_sha256", "content_bytes", "lines"):
        if d[key] != want[key]:
            problems.append(f"{key} {d[key]!r} != original {want[key]!r}")
    return problems


def _blank_result(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "rel": task["rel"],
        "endpoint": task.get("endpoint"),
        "date": task.get("date"),
        "status": None,
        "reason": None,
        "old_size": None,
        "new_size": None,
        "old_compressed_sha256": None,
        "new_compressed_sha256": None,
        "content_sha256": None,
        "content_bytes": None,
        "lines": None,
        "old_frames": None,
        "level": task["level"],
        "zstandard_version": zstandard.__version__,
        "finished_at": None,
        "seconds": None,
    }


# ---- per-file worker -------------------------------------------------------

def repack_file(task: dict[str, Any]) -> dict[str, Any]:
    """Re-pack ONE raw part file in place — steps 1-9 of the module docstring.

    ``task``: {"path": absolute path, "rel": manifest key, "level": int,
    "dry_run": bool, optional "endpoint"/"date", "intent_log" (path of the
    intents journal) and "intent" (the latest intent line for this file with
    no covering manifest entry)}. Top-level and pure so a ProcessPoolExecutor
    can run it. Never raises for an ordinary exception: it becomes status
    ``error``. The temp is unlinked on every exit path before the rename; the
    original is never opened for writing.
    """
    started = time.monotonic()
    path = Path(task["path"])
    level = int(task["level"])
    dry_run = bool(task.get("dry_run"))
    result = _blank_result(task)

    def done(status: str, reason: str | None = None) -> dict[str, Any]:
        result["status"] = status
        result["reason"] = reason
        result["finished_at"] = _utc_now()
        result["seconds"] = round(time.monotonic() - started, 3)
        return result

    tmp: Path | None = None
    try:
        refusal = _rename_refusal()
        if refusal is not None:
            return done("skipped-interrupted", refusal)

        # 1. stat the original (never following a symlink).
        st = os.lstat(path)
        result["old_size"] = st.st_size
        if stat.S_ISLNK(st.st_mode):
            return done("skipped-symlink")
        if not stat.S_ISREG(st.st_mode):
            return done("skipped-not-regular")
        if st.st_nlink > 1:
            return done("skipped-hardlinked", f"st_nlink={st.st_nlink}")

        # 2 + 3. one streaming pass: digest, decode twice, compress into the
        # same-directory temp; then flush + fsync + close the temp.
        src_fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with open(src_fd, "rb") as src:
            fst = os.fstat(src.fileno())
            if (fst.st_ino, fst.st_dev) != (st.st_ino, st.st_dev):
                return done("skipped-changed", "path re-pointed between stat and open")
            candidate = _temp_path(path)
            out = _open_temp(candidate)  # O_EXCL: only a file this operation created ...
            tmp = candidate              # ... is ever registered for cleanup
            with out:
                cobj = _compressor(level).compressobj()

                def sink(data: bytes) -> None:
                    out.write(cobj.compress(data))

                orig = _decode(src, sink)
                result["old_compressed_sha256"] = orig["compressed_sha256"]
                result["content_sha256"] = orig["content_sha256"]
                result["content_bytes"] = orig["content_bytes"]
                result["lines"] = orig["lines"]
                result["old_frames"] = orig["frames"]
                intent = task.get("intent")
                if intent is not None and orig["compressed_sha256"] == intent.get(
                    "new_compressed_sha256"
                ):
                    # This IS the verified temp an earlier run renamed into
                    # place after writing its intent, but that run died
                    # before its manifest line: finish step 9 against the
                    # ORIGINAL's digests the intent carries, and record it.
                    for key in ("old_size", "old_compressed_sha256", "old_frames",
                                "level", "zstandard_version"):
                        result[key] = intent.get(key)
                    result["new_size"] = orig["compressed_bytes"]
                    result["new_compressed_sha256"] = orig["compressed_sha256"]
                    problems = _single_frame_problems(orig, intent)
                    if problems:
                        logger.critical("POST-REPLACE CHECK FAILED for %s (from intent): %s",
                                        path, "; ".join(problems))
                        return done("failed-post-replace", "; ".join(problems))
                    logger.warning("%s was re-packed by an interrupted run — recorded "
                                   "now from its intent line", path)
                    return done("repacked", "recorded from the intent line of an interrupted run")
                if orig["error"] is not None:
                    return done(*orig["error"])
                if orig["compressed_bytes"] != st.st_size:
                    return done("skipped-changed",
                                f"read {orig['compressed_bytes']} bytes, stat said {st.st_size}")
                if orig["content_bytes"] == 0:
                    return done("skipped-empty")
                if not orig["ends_with_newline"]:
                    return done("skipped-torn", "decoded stream does not end in a newline")
                if not orig["terminated"]:
                    return done("skipped-unterminated",
                                f"input ends inside frame {orig['frames'] + 1} (crash orphan?)")
                if not _decoders_agree(orig):
                    logger.error("decoders disagree on %s — NOT re-packed", path)
                    return done("failed-verify",
                                "read_across_frames and per-frame decodes of the original "
                                f"disagree ({orig['content_bytes']} vs {orig['cross_bytes']} bytes)")
                if (not task.get("recorded") and orig["frames"] == 1
                        and _has_checksum(src.fileno())):
                    # Already a re-pack output that neither the manifest nor
                    # an intent records (rewritten by a run that predates
                    # intents). Re-compressing would only say skipped-no-gain
                    # and keep it out of --verify: record its digests instead.
                    result["new_size"] = st.st_size
                    result["new_compressed_sha256"] = orig["compressed_sha256"]
                    logger.warning("%s is an unrecorded re-pack output — recording "
                                   "its current digests", path)
                    return done("repacked-unrecorded",
                                "already one checksummed frame with no manifest or intent "
                                "record; the pre-re-pack digests are unknown")
                out.write(cobj.flush())  # ends the single frame (+ checksum)
                out.flush()
                _fsync(out.fileno())

        # 4. verify the temp independently of anything held in memory.
        with open(tmp, "rb") as fh:
            check = _decode(fh)
        result["new_compressed_sha256"] = check["compressed_sha256"]
        result["new_size"] = check["compressed_bytes"]
        problems = _single_frame_problems(check, orig)
        if problems:
            logger.error("temp for %s failed verification: %s", path, "; ".join(problems))
            return done("failed-verify", "; ".join(problems))

        # 5. only worth a rename if it actually saves space.
        if check["compressed_bytes"] >= st.st_size:
            return done("skipped-no-gain")
        if dry_run:
            return done("would-repack")

        # 6. the original must be exactly the file the operation read.
        st2 = os.lstat(path)
        if (
            (st2.st_size, st2.st_mtime_ns, st2.st_ino, st2.st_dev)
            != (st.st_size, st.st_mtime_ns, st.st_ino, st.st_dev)
            or st2.st_nlink != 1
        ):
            return done("skipped-changed", "original changed while being re-packed")

        # 7. carry the original's permission bits and timestamps over.
        os.chmod(tmp, stat.S_IMODE(st.st_mode))
        os.utime(tmp, ns=(st.st_atime_ns, st.st_mtime_ns))

        # 8. atomic swap; from here the temp IS the original — never unlink.
        # Only while the run can still record it, and announced first: the
        # fsynced intent lets a rerun record this rename should the run die
        # before its manifest line (step 9 read-back included).
        refusal = _rename_refusal()
        if refusal is not None:
            return done("skipped-interrupted", refusal)
        if task.get("intent_log"):
            _append_intent(task["intent_log"], dict(result, status="intent", finished_at=_utc_now()))
        _replace_into_place(tmp, path)
        tmp = None

        # 9. read back what is now in place.
        with open(path, "rb") as fh:
            final = _decode(fh)
        problems = _single_frame_problems(final, orig)
        if final["compressed_sha256"] != check["compressed_sha256"]:
            problems.append("compressed bytes in place differ from the verified temp")
        if problems:
            logger.critical("POST-REPLACE CHECK FAILED for %s: %s", path, "; ".join(problems))
            return done("failed-post-replace", "; ".join(problems))
        return done("repacked")
    except Exception as exc:  # noqa: BLE001 — every failure must surface as status
        logger.error("repack of %s errored: %r", path, exc)
        tb = traceback.format_exc(limit=4)
        return done("error", f"{type(exc).__name__}: {exc}\n{tb}")
    finally:
        if tmp is not None:
            try:
                tmp.unlink(missing_ok=True)
            except OSError as exc:
                logger.error("could not remove temp %s: %r", tmp, exc)


def verify_file(task: dict[str, Any]) -> dict[str, Any]:
    """Re-decode one re-packed file against its manifest entry (``--verify``)."""
    path = Path(task["path"])
    want = task["expected"]
    result = {"rel": task["rel"], "status": None, "reason": None}
    try:
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            result.update(status="missing", reason="file does not exist")
            return result
        with open(fd, "rb") as fh:
            d = _decode(fh)
        problems = _single_frame_problems(d, want)
        result["compressed_sha256_matches"] = (
            d["compressed_sha256"] == want.get("new_compressed_sha256")
        )
        result.update(status="mismatch" if problems else "ok",
                      reason="; ".join(problems) or None)
    except Exception as exc:  # noqa: BLE001
        result.update(status="error", reason=f"{type(exc).__name__}: {exc}")
    return result


# ---- discovery -------------------------------------------------------------

def _date_selected(
    date: str, dates: set[str] | None, date_from: str | None, date_to: str | None
) -> bool:
    if dates is not None:
        return date in dates
    if date_from is not None and date < date_from:
        return False
    if date_to is not None and date > date_to:
        return False
    return True


def _targeted_partitions(
    data_dir: Path,
    endpoint_glob: str,
    dates: set[str] | None,
    date_from: str | None,
    date_to: str | None,
) -> list[tuple[str, str, Path]]:
    """(endpoint, date, date_dir) for every raw date partition the filters select."""
    out = []
    venue_dir = data_dir / "raw" / f"venue={VENUE}"
    for date_dir in sorted(venue_dir.glob(f"endpoint={endpoint_glob}/date=*")):
        if not date_dir.is_dir() or date_dir.is_symlink():
            continue
        endpoint = date_dir.parent.name.split("=", 1)[1]
        date = date_dir.name.split("=", 1)[1]
        if _date_selected(date, dates, date_from, date_to):
            out.append((endpoint, date, date_dir))
    return out


def _partition_skip_reason(
    data_dir: Path,
    endpoint: str,
    date: str,
    date_dir: Path,
    today: str,
    require_compacted: bool,
) -> str | None:
    try:
        _validate_date(date)
    except ValueError:
        return "malformed-date"
    if date >= today:
        return "not-before-today"
    if any(date_dir.rglob("*.jsonl.zst.tmp")):
        return "tmp-present"  # live or crashed writer — same deferral as compact.py
    if require_compacted:
        # The parquet root resolves exactly as cli.py / lake.raw_parquet_root.
        marker = (data_dir / "raw-parquet" / f"venue={VENUE}" / f"endpoint={endpoint}"
                  / f"date={date}" / PARQUET_PART_NAME)
        if not marker.exists():
            return "not-compacted"
    return None


def _part_files(date_dir: Path) -> list[Path]:
    files = []
    for hour_dir in sorted(date_dir.glob("hour=*")):
        if hour_dir.is_symlink() or not hour_dir.is_dir():
            continue
        files.extend(sorted(hour_dir.glob("part-*.jsonl.zst")))
    return files


def _clean_stale_temps(date_dirs: Iterable[Path], data_dir: Path, *, remove: bool) -> list[str]:
    """Stale temps of an earlier crashed run. Only names matching this tool's
    own pattern are touched; the caller holds repack.lock, so none is live."""
    found = []
    for date_dir in date_dirs:
        for hour_dir in sorted(date_dir.glob("hour=*")):
            if hour_dir.is_symlink() or not hour_dir.is_dir():
                continue
            for f in sorted(hour_dir.iterdir()):
                if not is_repack_temp(f.name) or f.is_symlink() or not f.is_file():
                    continue
                rel = os.path.relpath(f, data_dir)
                if remove:
                    f.unlink()
                    logger.warning("removed stale repack temp %s", rel)
                else:
                    logger.warning("stale repack temp %s (left in place: dry run)", rel)
                found.append(rel)
    return found


# ---- manifest --------------------------------------------------------------

def manifest_path(data_dir: Path) -> Path:
    return Path(data_dir) / MANIFEST_DIR / MANIFEST_NAME


def intents_path(data_dir: Path) -> Path:
    return Path(data_dir) / MANIFEST_DIR / INTENTS_NAME


def _load_latest(path: Path, statuses: frozenset[str]) -> dict[str, dict[str, Any]]:
    """Latest line per rel path whose status is in ``statuses`` (a torn last
    line is ignored)."""
    entries: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return entries
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("%s:%d: unparseable line ignored", path, n)
                continue
            if rec.get("status") in statuses and rec.get("rel"):
                entries[rec["rel"]] = rec
    return entries


def _load_repacked(path: Path) -> dict[str, dict[str, Any]]:
    """Latest covering manifest entry (``_COVERED_STATUSES``) per rel path."""
    return _load_latest(path, _COVERED_STATUSES)


class _LineLog:
    """Append-only JSONL written only by the parent; every line fsynced."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._fh = open(path, "a", encoding="utf-8")

    def write(self, record: dict[str, Any]) -> None:
        self._fh.write(json.dumps(record, sort_keys=True) + "\n")
        self._fh.flush()
        _fsync(self._fh.fileno())

    def close(self) -> None:
        self._fh.close()


def _acquire_lock(data_dir: Path):
    fh = open(Path(data_dir) / LOCK_NAME, "a")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        raise RepackLocked(
            f"{Path(data_dir) / LOCK_NAME} is held — another repack run is active"
        ) from None
    return fh


# ---- runners ---------------------------------------------------------------

def _run_pool(
    fn: Callable[[dict[str, Any]], dict[str, Any]],
    tasks: list[dict[str, Any]],
    workers: int,
    on_result: Callable[[dict[str, Any]], bool],
) -> int:
    """Run ``fn`` over ``tasks``; ``on_result`` returning True aborts the run:
    nothing new is submitted, queued tasks are cancelled, and pool tasks a
    worker has already taken refuse any further rename (_rename_refusal) —
    each in-flight result is still recorded. workers <= 0 runs in-process.
    Returns the number of tasks never started.

    Ctrl-C (SIGINT) is taken over while the run is active and stops it the
    same way: pool workers ignore the signal and refuse any further rename
    (_rename_refusal), every result still in flight reaches ``on_result`` —
    each may be a finished rename that must be recorded — and only then
    RepackInterrupted is raised."""

    def crashed(task: dict[str, Any], exc: BaseException) -> dict[str, Any]:
        res = _blank_result(task) if "level" in task else {"rel": task["rel"]}
        res.update(status="error", reason=f"worker failed: {type(exc).__name__}: {exc}",
                   finished_at=_utc_now())
        return res

    pending = iter(tasks)
    interrupted: list[bool] = []  # appended by the SIGINT handler
    not_started = 0
    with _sigint_flag(interrupted):
        if workers <= 0:
            for task in pending:
                if interrupted:
                    not_started += 1  # consumed from ``pending`` but never run
                    break
                try:
                    res = fn(task)
                except Exception as exc:  # noqa: BLE001
                    res = crashed(task, exc)
                if on_result(res):
                    break
        else:
            not_started = _drain_pool(fn, pending, workers, on_result, crashed, interrupted)
    not_started += sum(1 for _ in pending)
    if interrupted:
        raise RepackInterrupted(not_started)
    return not_started


def _drain_pool(
    fn: Callable[[dict[str, Any]], dict[str, Any]],
    pending: Iterator[dict[str, Any]],
    workers: int,
    on_result: Callable[[dict[str, Any]], bool],
    crashed: Callable[[dict[str, Any], BaseException], dict[str, Any]],
    interrupted: list[bool],
) -> int:
    """_run_pool's process-pool half; returns how many submitted tasks were
    cancelled before starting."""
    # Bounded submission window instead of submit-everything-then-cancel:
    # an abort stops feeding the pool (halt() stops what is already in it),
    # and a pool broken by a killed worker (BrokenProcessPool) turns every
    # in-flight task into an error.
    # spawn: every worker is a direct child, which _rename_refusal relies on.
    ctx = multiprocessing.get_context("spawn")
    stop = ctx.Event()
    aborted = False
    cancelled = 0
    in_flight: dict[Any, dict[str, Any]] = {}
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx, initializer=_init_worker,
                             initargs=(stop, os.getpid())) as pool:

        def fill() -> None:
            nonlocal aborted
            while not aborted and not interrupted and len(in_flight) < workers * 2:
                task = next(pending, None)
                if task is None:
                    return
                try:
                    in_flight[pool.submit(fn, task)] = task
                except BrokenExecutor as exc:
                    on_result(crashed(task, exc))
                    aborted = True

        def halt(why: str) -> None:
            # Ctrl-C and a hard stop alike: queued tasks are cancelled, and a
            # task a worker has already taken refuses at its next
            # _rename_refusal (task start or step 8) — nothing renames after.
            stop.set()
            for fut in in_flight:
                fut.cancel()  # succeeds only for tasks no worker has taken
            logger.warning("%s — recording %d in-flight file(s), starting no new ones",
                           why, len(in_flight))

        fill()
        while in_flight:
            # Polls so a Ctrl-C reaches the workers within ~0.5 s.
            finished, _ = wait(in_flight, timeout=0.5, return_when=FIRST_COMPLETED)
            if interrupted and not stop.is_set():
                halt("interrupted")
            for fut in finished:
                task = in_flight.pop(fut)
                if fut.cancelled():
                    cancelled += 1
                    continue
                try:
                    res = fut.result()
                except Exception as exc:  # noqa: BLE001 — e.g. BrokenProcessPool
                    res = crashed(task, exc)
                if on_result(res):
                    aborted = True
                    if not stop.is_set():
                        halt("run aborted")
            fill()
    return cancelled


def _new_summary(mode: str, data_dir: Path, level: int | None) -> dict[str, Any]:
    return {
        "mode": mode,
        "data_dir": str(data_dir),
        "level": level,
        "zstandard_version": zstandard.__version__,
        "started_at": _utc_now(),
        "finished_at": None,
        "partitions_eligible": 0,
        "partitions_skipped": [],
        "stale_temps": [],
        "already_repacked": 0,
        "not_started": 0,
        "endpoints": {},
        "totals": {"counts": {}, "bytes_before": 0, "bytes_after": 0, "freed": 0},
        "failures": [],
        "aborted": False,
        "interrupted": False,
        "ok": True,
    }


def _tally(summary: dict[str, Any], res: dict[str, Any]) -> None:
    ep = summary["endpoints"].setdefault(
        res.get("endpoint") or "?",
        {"counts": {}, "bytes_before": 0, "bytes_after": 0, "freed": 0},
    )
    for bucket in (ep, summary["totals"]):
        bucket["counts"][res["status"]] = bucket["counts"].get(res["status"], 0) + 1
        if res["status"] in _SIZED_STATUSES:
            bucket["bytes_before"] += res["old_size"]
            bucket["bytes_after"] += res["new_size"]
            bucket["freed"] = bucket["bytes_before"] - bucket["bytes_after"]
    if res["status"] in FAILURE_STATUSES:
        summary["ok"] = False
        summary["failures"].append(
            {"rel": res["rel"], "status": res["status"], "reason": res.get("reason")}
        )


def repack(
    data_dir: Path | str,
    *,
    dates: Iterable[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    endpoint_glob: str = DEFAULT_ENDPOINT_GLOB,
    level: int = DEFAULT_LEVEL,
    workers: int = DEFAULT_WORKERS,
    dry_run: bool = False,
    limit: int | None = None,
    require_compacted: bool = True,
    today: str | None = None,
) -> dict[str, Any]:
    """Re-pack every eligible Kalshi raw part file; returns the run summary
    (``summary["ok"]`` is False on any loud failure). ``today`` is
    injectable for tests only. Raises RepackLocked if a run is active."""
    data_dir = Path(data_dir)
    if not data_dir.is_dir():
        raise FileNotFoundError(f"data dir {data_dir} does not exist")
    if not 1 <= level <= 19:
        raise ValueError(f"level must be 1-19, got {level}")
    date_set = {_validate_date(d) for d in dates} if dates else None
    for d in (date_from, date_to):
        if d is not None:
            _validate_date(d)
    today = today or _utc_today()
    summary = _new_summary("dry-run" if dry_run else "repack", data_dir, level)

    lock = _acquire_lock(data_dir)
    log: _LineLog | None = None
    try:
        targeted = _targeted_partitions(data_dir, endpoint_glob, date_set, date_from, date_to)
        summary["stale_temps"] = _clean_stale_temps(
            (p[2] for p in targeted), data_dir, remove=not dry_run
        )
        done = _load_repacked(manifest_path(data_dir))
        # Intents of renames whose manifest line never landed (run killed or
        # interrupted in between) — repack_file records them (dry runs don't).
        intents = {} if dry_run else _load_latest(intents_path(data_dir), frozenset({"intent"}))
        tasks: list[dict[str, Any]] = []
        for endpoint, date, date_dir in targeted:
            reason = _partition_skip_reason(
                data_dir, endpoint, date, date_dir, today, require_compacted
            )
            if reason is not None:
                (logger.warning if reason == "tmp-present" else logger.info)(
                    "skipping partition %s/%s: %s", endpoint, date, reason
                )
                summary["partitions_skipped"].append(
                    {"endpoint": endpoint, "date": date, "reason": reason}
                )
                continue
            summary["partitions_eligible"] += 1
            for f in _part_files(date_dir):
                rel = f.relative_to(data_dir).as_posix()
                prior = done.get(rel)
                if prior is not None:
                    try:
                        size = os.lstat(f).st_size
                    except FileNotFoundError:
                        size = None
                    if size == prior.get("new_size"):
                        summary["already_repacked"] += 1
                        continue
                task = {
                    "path": os.path.abspath(f), "rel": rel, "endpoint": endpoint,
                    "date": date, "level": level, "dry_run": dry_run,
                    # Recorded once, changed since: --verify must keep
                    # flagging it, so it is never re-recorded as-is.
                    "recorded": prior is not None,
                }
                if not dry_run:
                    task["intent_log"] = str(intents_path(data_dir))
                if rel in intents:
                    task["intent"] = intents[rel]
                tasks.append(task)
        if limit is not None:
            tasks = tasks[: max(limit, 0)]

        if dry_run:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            log = _LineLog(data_dir / MANIFEST_DIR / f"dry-run-{stamp}-{uuid.uuid4().hex[:6]}.jsonl")
            summary["report"] = str(log.path)
        else:
            log = _LineLog(manifest_path(data_dir))
            summary["manifest"] = str(log.path)
            # Workers append intents with O_APPEND; the file and its directory
            # entry must be durable before the first rename relies on them.
            intents_path(data_dir).touch()
            _fsync_dir(log.path.parent)
        logger.info("%s: %d file(s) across %d eligible partition(s), level %d, %d worker(s)",
                    summary["mode"], len(tasks), summary["partitions_eligible"], level, workers)

        def on_result(res: dict[str, Any]) -> bool:
            log.write(res)
            _tally(summary, res)
            if res["status"] in _SIZED_STATUSES:
                logger.info("%s %s: %.1f MB -> %.1f MB (%.1fs)", res["status"], res["rel"],
                            res["old_size"] / 1e6, res["new_size"] / 1e6, res["seconds"] or 0)
            elif res["status"] in FAILURE_STATUSES:
                logger.error("%s %s: %s", res["status"], res["rel"], res.get("reason"))
            else:
                logger.info("%s %s%s", res["status"], res["rel"],
                            f" ({res['reason']})" if res.get("reason") else "")
            if res["status"] == "failed-post-replace":
                summary["aborted"] = True
                return True  # hard error: stop the run
            return False

        try:
            summary["not_started"] = _run_pool(repack_file, tasks, workers, on_result)
        except RepackInterrupted as exc:
            summary["not_started"] = exc.not_started
            summary["aborted"] = summary["interrupted"] = True
            logger.warning("interrupted — every file renamed so far is in the manifest; "
                           "rerun to continue")
    finally:
        if log is not None:
            log.close()
        lock.close()  # releases the flock
    summary["finished_at"] = _utc_now()
    return summary


def verify(
    data_dir: Path | str,
    *,
    dates: Iterable[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    endpoint_glob: str | None = None,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, Any]:
    """Re-decode every covered manifest entry's current file and confirm its
    content digest and line count, and flag every re-pack output in scope
    that the manifest does not cover (read-only; no lock needed)."""
    data_dir = Path(data_dir)
    date_set = {_validate_date(d) for d in dates} if dates else None
    for d in (date_from, date_to):
        if d is not None:
            _validate_date(d)
    summary = _new_summary("verify", data_dir, None)
    covered = _load_repacked(manifest_path(data_dir))
    tasks = []
    for rel, entry in sorted(covered.items()):
        parts = dict(p.split("=", 1) for p in Path(rel).parts if "=" in p)
        if not _date_selected(parts.get("date", ""), date_set, date_from, date_to):
            continue
        if endpoint_glob is not None and not fnmatch.fnmatchcase(
            parts.get("endpoint", ""), endpoint_glob
        ):
            continue
        tasks.append({"path": os.path.abspath(data_dir / rel), "rel": rel, "expected": entry})

    counts = summary["totals"]["counts"]

    def on_result(res: dict[str, Any]) -> bool:
        counts[res["status"]] = counts.get(res["status"], 0) + 1
        if res["status"] != "ok":
            summary["ok"] = False
            summary["failures"].append(res)
            logger.error("verify %s %s: %s", res["status"], res["rel"], res.get("reason"))
        return False

    # Every re-pack output is one checksummed frame; RawLake never writes a
    # checksum. One the manifest does not cover was renamed by a run that
    # died before recording it — never report OK over it.
    for _endpoint, _date, date_dir in _targeted_partitions(
        data_dir, endpoint_glob or "*", date_set, date_from, date_to
    ):
        for f in _part_files(date_dir):
            rel = f.relative_to(data_dir).as_posix()
            if rel in covered:
                continue
            try:
                fd = os.open(f, os.O_RDONLY | os.O_NOFOLLOW)
            except OSError:
                continue
            try:
                unrecorded = _has_checksum(fd)
            finally:
                os.close(fd)
            if unrecorded:
                on_result({"rel": rel, "status": "unrecorded",
                           "reason": "re-pack output with no manifest entry (run killed or "
                                     "interrupted before recording it) — rerun the re-pack "
                                     "to record it"})

    _run_pool(verify_file, tasks, workers, on_result)
    summary["files"] = len(tasks)
    summary["finished_at"] = _utc_now()
    return summary


# ---- CLI -------------------------------------------------------------------

def _print_summary(summary: dict[str, Any]) -> None:
    print(f"repack {summary['mode']}: data_dir={summary['data_dir']}")
    if summary["mode"] == "verify":
        print(f"  files checked: {summary.get('files', 0)}  {summary['totals']['counts']}")
    else:
        print(f"  partitions eligible: {summary['partitions_eligible']}, "
              f"skipped: {len(summary['partitions_skipped'])}, "
              f"already re-packed files: {summary['already_repacked']}, "
              f"stale temps: {len(summary['stale_temps'])}")
        for endpoint, ep in sorted(summary["endpoints"].items()):
            print(f"  {endpoint}: {ep['counts']}  {ep['bytes_before'] / 1e9:.3f} GB -> "
                  f"{ep['bytes_after'] / 1e9:.3f} GB (freed {ep['freed'] / 1e9:.3f} GB)")
        t = summary["totals"]
        print(f"  TOTAL: {t['counts']}  {t['bytes_before'] / 1e9:.3f} GB -> "
              f"{t['bytes_after'] / 1e9:.3f} GB (freed {t['freed'] / 1e9:.3f} GB)")
        if summary["not_started"]:
            print(f"  not started (run aborted): {summary['not_started']}")
    for f in summary["failures"]:
        first = (f.get("reason") or "").splitlines()[:1]
        print(f"  FAILED {f['status']}: {f['rel']}: {first[0] if first else ''}")
    if summary["interrupted"]:
        print("  INTERRUPTED — every file renamed so far is recorded; rerun to continue")
    print("  OK" if summary["ok"] else "  NOT OK — see failures above")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m pmkt.repack",
        description="Losslessly re-pack finished per-record-frame Kalshi raw part files "
                    "(default endpoint=ws_*) into one zstd frame each, in place.",
    )
    p.add_argument("--data-dir", type=Path, default=None,
                   help="data root (default: pmkt Config → $PMKT_DATA_DIR or ./data)")
    p.add_argument("--date", action="append", dest="dates", metavar="YYYY-MM-DD",
                   help="restrict to this UTC date (repeatable)")
    p.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD", help="first date (inclusive)")
    p.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD", help="last date (inclusive)")
    p.add_argument("--endpoint-glob", default=None,
                   help=f"endpoint name glob under venue={VENUE} (default {DEFAULT_ENDPOINT_GLOB!r}; "
                        "with --verify default: all manifest entries)")
    p.add_argument("--level", type=int, default=DEFAULT_LEVEL, help="zstd level 1-19 (default 9)")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                   help="worker processes (default 2; 0 = in-process)")
    p.add_argument("--dry-run", action="store_true",
                   help="compress + verify into temps and report projected sizes; never replaces")
    p.add_argument("--limit", type=int, default=None, help="process at most N files")
    p.add_argument("--no-require-compacted", action="store_true",
                   help="also re-pack date partitions that have no compacted part-0.parquet")
    p.add_argument("--verify", action="store_true",
                   help="re-decode every re-packed file in the manifest and check its content digest")
    p.add_argument("--json-summary", type=Path, default=None, help="also write the summary JSON here")
    args = p.parse_args(argv)
    if args.dates and (args.date_from or args.date_to):
        p.error("use --date or --from/--to, not both")
    if not 1 <= args.level <= 19:
        p.error("--level must be 1-19")
    if args.verify and args.dry_run:
        p.error("--verify and --dry-run are exclusive")
    try:
        for d in [*(args.dates or []), args.date_from, args.date_to]:
            if d is not None:
                _validate_date(d)
    except ValueError as exc:
        p.error(str(exc))
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        os.nice(10)  # background citizen next to the live collector
    except OSError:
        pass
    if args.data_dir is not None:
        data_dir = args.data_dir
    else:
        from .config import Config

        data_dir = Config().data_dir
    try:
        if args.verify:
            summary = verify(
                data_dir, dates=args.dates, date_from=args.date_from, date_to=args.date_to,
                endpoint_glob=args.endpoint_glob, workers=args.workers,
            )
        else:
            summary = repack(
                data_dir, dates=args.dates, date_from=args.date_from, date_to=args.date_to,
                endpoint_glob=args.endpoint_glob or DEFAULT_ENDPOINT_GLOB, level=args.level,
                workers=args.workers, dry_run=args.dry_run, limit=args.limit,
                require_compacted=not args.no_require_compacted,
            )
    except RepackLocked as exc:
        logger.error("%s — refusing to run", exc)
        return 2
    except RepackInterrupted:  # --verify: read-only, nothing to record
        logger.warning("verify interrupted")
        return 130
    _print_summary(summary)
    if args.json_summary is not None:
        args.json_summary.parent.mkdir(parents=True, exist_ok=True)
        args.json_summary.write_text(json.dumps(summary, indent=2, sort_keys=True))
    if summary["interrupted"]:
        return 130  # 128 + SIGINT, as a shell reports a Ctrl-C
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    # Re-enter through the package name so pool workers unpickle
    # pmkt.repack.repack_file rather than __main__.repack_file.
    from pmkt.repack import main as _main

    sys.exit(_main())
