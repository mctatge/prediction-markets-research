"""Nightly raw-lake compaction: one UTC day of JSONL.zst → Parquet per partition.

For every raw partition ``raw/venue=V/endpoint=E/date=D`` with hour part files,
compact_day() rewrites the whole day as one or more Parquet files at
``out_root/venue=V/endpoint=E/date=D/part-K.parquet`` (small partitions get
exactly part-0; partitions over _GROUP_RAW_BYTES uncompressed are split into
time-contiguous size-bounded parts so the COPY stays inside the memory
ceiling — part-0's presence is the partition's "compacted" gate and it is
finalized LAST) with columns

    (v BIGINT, venue VARCHAR, endpoint VARCHAR, params VARCHAR[json],
     fetched_at VARCHAR, http_status INTEGER, elapsed_ms BIGINT,
     payload VARCHAR[json], hour VARCHAR)

Source .zst CONTENT is NEVER modified or deleted here (storage invariants — the raw
lake is the system of record; retention is configured separately). The one outside rewrite is a lossless
``pmkt.repack`` re-pack, which may replace a finished file's compressed
container with a content-identical one (decompressed bytes SHA-256-verified);
the decoded lines are unchanged, so the row-count idempotency check below
still reports skipped-existing. ``*.jsonl.zst.tmp`` files are live or crashed
writers — recovery (RawLake startup) owns them, so a partition containing any
.tmp is DEFERRED to the next nightly run rather than compacted incompletely.

Bit-faithfulness — definition and approach
------------------------------------------
Every raw line was produced by ``json.dumps(record, separators=(",", ":"))``
(ensure_ascii=True, see rawlake.py). Python's json round-trip
``loads → dumps(separators=(",", ":"))`` is therefore BYTE-IDENTICAL to the
original line: dict insertion order is preserved, ints are arbitrary-precision,
float repr is deterministic shortest-round-trip, and ensure_ascii re-escapes
non-ASCII exactly as the writer did. The decoder verifies this per line while streaming
(``noncanonical_lines`` counts any line that fails byte-equality — value
fidelity still holds by construction of json.loads, so this is a warning
signal, not corruption).

DuckDB's ``read_json`` is deliberately NOT pointed at the raw payloads:
reading a JSON object field re-serializes it through yyjson, which un-escapes
``\\uXXXX`` sequences and may re-render numbers — value-preserving but not
byte-preserving, and read_json_auto type sniffing could corrupt big ints and
decimal strings outright. Instead, Python re-serializes ``params``/``payload``
as canonical JSON STRINGS into a staging NDJSON file, and DuckDB only ferries
those opaque strings into Parquet (a JSON string literal decodes to the exact
same text on both sides — proven by tests).

In-job verification (both must pass before the Parquet is finalized):
1. Parquet row count == total JSONL line count for the partition.
2. Full-content digest: an order-independent sum of per-row SHA-256 digests
   over ALL columns, computed once while streaming the JSONL and again while
   re-reading the written Parquet. Every byte of every column is covered.

Idempotency: an existing part-0.parquet whose part set's total row count
matches the current JSONL line count is skipped; a mismatch raises
CompactionError loudly (it means raw grew after compaction — e.g. a crashed
writer's .tmp got recovered later) and never silently overwrites.

Parquet outputs are written to a dot-prefixed temp name and os.replace()d into
place only after verification, so readers never see a partial file.
"""

from __future__ import annotations

from .locking import fcntl
import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import zstandard

from .rawlake import decompress_raw_file

logger = logging.getLogger(__name__)

PARQUET_PART_NAME = "part-0.parquet"

# Explicit staging column types — read_json must never sniff (storage invariants:
# big ints / decimal strings survive because payload+params stay VARCHAR).
_STAGING_COLUMNS = (
    "{'v': 'BIGINT', 'venue': 'VARCHAR', 'endpoint': 'VARCHAR', "
    "'params': 'VARCHAR', 'fetched_at': 'VARCHAR', 'http_status': 'INTEGER', "
    "'elapsed_ms': 'BIGINT', 'payload': 'VARCHAR', 'hour': 'VARCHAR'}"
)
_COLUMN_ORDER = (
    "v", "venue", "endpoint", "params", "fetched_at",
    "http_status", "elapsed_ms", "payload", "hour",
)
# Largest observed raw record ~1-3 MB (a 1000-market page); leave headroom.
_MAX_OBJECT_SIZE = 128 * 1024 * 1024
_DIGEST_MOD = 1 << 128
# DuckDB session guardrails — compaction shares the machine with the live
# collector loop, so it must stay a bounded background citizen (DuckDB's
# default memory_limit is 80% of RAM). Bytes-capped row groups bound the
# parquet writer's in-memory buffering: payload VARCHARs run ~0.1-3 MB, so
# the 122,880-row default row group would buffer multi-GB per group.
_DUCKDB_MEMORY_LIMIT = os.environ.get("PMKT_COMPACT_MEMORY_LIMIT", "4GB")
_DUCKDB_THREADS = int(os.environ.get("PMKT_COMPACT_THREADS", "4"))
_ROW_GROUP_SIZE_BYTES = "128MB"
# Max UNCOMPRESSED staging bytes per COPY group (one output part file each).
# One big COPY of a multi-GB partition cannot be memory-bounded: the ORDER BY
# that used to restore row-group time locality materializes every VARCHAR
# payload through the sort and OOMs at ANY practical ceiling (2026-07-19:
# 66MB-zst candlesticks killed a 4GB session; 531MB-zst trades killed 8GB —
# spill never engaged for the string heap). Size-bounded groups replace it:
# rows stream in raw (hour, part-file) order, so each group — and therefore
# each output part file — covers a contiguous time slice, and time-windowed
# consumers prune whole FILES via parquet metadata instead of row groups.
# Session working set ≈ a few × group size, safely inside the 4GB ceiling.
_GROUP_RAW_BYTES = int(
    os.environ.get("PMKT_COMPACT_GROUP_RAW_BYTES", str(512 * 1024 * 1024))
)


class CompactionError(RuntimeError):
    """Verification failure — never finalized, never overwritten silently."""


def _sql_str(path: Path | str) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _connect(out_root: Path) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB session with explicit spill + memory guardrails.

    The defaults are wrong for a long-lived daemon (incident 2026-07-12→15,
    41 failed cycles): temp_directory defaults to the RELATIVE path ".tmp"
    — resolved against the process cwd and shared with every other DuckDB
    connection in the process, so it can vanish from under a spilling query
    (IOException '.tmp/duckdb_temp_block-…: No such file or directory') —
    and memory_limit defaults to 80% of RAM, which let one multi-GB trades
    partition COPY exhaust the machine before dying (OutOfMemoryException
    at 12.7 GiB). Mirrors lake.connect(): an owned ABSOLUTE spill dir on
    the same volume as the outputs, plus a hard memory ceiling and a thread
    cap so compaction stays a background citizen next to the collector.
    preserve_insertion_order=false lets read_json → COPY stream out-of-core
    instead of buffering whole partitions; parquet row order is free here —
    the verification digest is order-independent by design and lake readers
    filter, never assume order. (It also activates ROW_GROUP_SIZE_BYTES,
    which DuckDB ignores when order preservation is on.)
    """
    tmp_dir = out_root / ".duckdb-tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    conn.execute(f"SET temp_directory = {_sql_str(tmp_dir)}")
    conn.execute(f"SET memory_limit = '{_DUCKDB_MEMORY_LIMIT}'")
    conn.execute(f"SET threads = {_DUCKDB_THREADS}")
    conn.execute("SET preserve_insertion_order = false")
    return conn


def _row_digest(fields: tuple[str, ...]) -> int:
    # Staging text is pure ASCII with control chars escaped (ensure_ascii
    # json), so \x1f can never appear inside a field — join is unambiguous.
    h = hashlib.sha256("\x1f".join(fields).encode("utf-8")).digest()
    return int.from_bytes(h[:16], "big")


def _read_lines(path: Path) -> tuple[list[str], bool]:
    """All complete JSONL lines of one raw part file, original bytes preserved.

    Decodes through rawlake.decompress_raw_file — the same decoder as
    read_raw_file, so both tolerate torn/unterminated crash-orphan tails
    identically — but keeps the raw line text so the canonical-serialization
    check can prove byte equality.

    Torn-tail detection: every complete record was flushed as either its own
    zstd frame (FLUSH_FRAME, the REST default) or a complete block of a frame
    left open until rotation/close (FLUSH_BLOCK, the live WS feeds — a crash
    orphan ends in an unterminated frame, which decodes completely and is NOT
    truncated). Either way the flushed bytes end with ``\\n``, so a decoded
    buffer NOT ending in a newline provably carries a partial trailing record
    (a record torn mid-write: the decoder returns its complete blocks, then
    stops at EOF). That partial line is dropped, exactly like read_raw_file
    does. A tear inside a small final record's compressed block can drop that
    whole block, leaving the buffer on a record boundary: that one record is
    then lost without ``truncated`` being set. Undecodable (corrupt) zstd data
    also sets ``truncated``; its decoded prefix is still returned.
    """
    buf, truncated = decompress_raw_file(path)
    lines = [ln for ln in buf.decode("utf-8", errors="replace").splitlines() if ln.strip()]
    if buf and not buf.endswith(b"\n"):
        truncated = True
        lines.pop()
        logger.warning("dropping partial trailing record in %s — salvaging", path)
    return lines, truncated


def _count_lines(files: list[Path]) -> int:
    return sum(len(_read_lines(f)[0]) for f in files)


def _partition_part_files(date_dir: Path) -> tuple[list[Path], list[Path]]:
    """(finalized part files sorted by hour+name, live/crashed .tmp files)."""
    parts = sorted(date_dir.glob("hour=*/part-*.jsonl.zst"))
    tmps = sorted(date_dir.glob("hour=*/*.jsonl.zst.tmp"))
    return parts, tmps


def _canonical(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"))


def _clean_stale_temps(out_dir: Path) -> None:
    """Remove leftovers from a previous crashed compaction run. These live in
    OUT_ROOT (which this job owns) — raw .tmp files are never touched."""
    for stale in out_dir.glob(".compact-*"):
        stale.unlink()
        logger.warning("removed stale compaction temp file %s", stale)


def _compact_partition(
    conn: duckdb.DuckDBPyConnection,
    date_dir: Path,
    out_dir: Path,
    venue: str,
    endpoint: str,
    date: str,
) -> dict[str, Any]:
    started = time.monotonic()
    part_files, tmp_files = _partition_part_files(date_dir)
    out_file = out_dir / PARQUET_PART_NAME
    report: dict[str, Any] = {
        "venue": venue,
        "endpoint": endpoint,
        "date": date,
        "files": len(part_files),
        "rows": 0,
        "noncanonical_lines": 0,
        "truncated_files": 0,
        "raw_bytes": sum(f.stat().st_size for f in part_files),
        "parquet_bytes": 0,
        "seconds": 0.0,
    }

    if tmp_files:
        # A live writer (current UTC hour just closed) or a crashed writer
        # (recovery at next collector startup owns it). Compacting around it
        # would under-count the day and trip the idempotency check later —
        # defer the whole partition to the next nightly run instead.
        logger.warning(
            "compact %s/%s %s: %d .tmp part file(s) present — deferring partition",
            venue, endpoint, date, len(tmp_files),
        )
        report["action"] = "deferred-tmp"
        report["seconds"] = round(time.monotonic() - started, 3)
        return report

    if out_file.exists():
        existing_parts = sorted(out_dir.glob("part-*.parquet"))
        expected = _count_lines(part_files)
        actual = sum(
            conn.execute(
                f"SELECT count(*) FROM read_parquet({_sql_str(p)})"
            ).fetchone()[0]
            for p in existing_parts
        )
        if actual != expected:
            raise CompactionError(
                f"{out_dir}: existing parquet part(s) have {actual} rows but raw "
                f"JSONL now has {expected} lines — raw grew after compaction "
                f"(recovered orphan?). Refusing to overwrite; investigate, then "
                f"delete the parquet part(s) to recompact."
            )
        report["action"] = "skipped-existing"
        report["rows"] = actual
        report["parquet_bytes"] = sum(p.stat().st_size for p in existing_parts)
        report["seconds"] = round(time.monotonic() - started, 3)
        return report

    out_dir.mkdir(parents=True, exist_ok=True)
    _clean_stale_temps(out_dir)
    # part-0 is renamed LAST, so its absence means the partition is unserved
    # (still read from jsonl). A crash mid-finalize can leave renamed part-K
    # (K>=1) files behind; a rerun may produce fewer groups, and a stale
    # high-K file would silently join future read_parquet lists — clear them.
    for stale in sorted(out_dir.glob("part-*.parquet")):
        stale.unlink()
        logger.warning("removed unfinalized parquet part %s", stale)
    token = uuid.uuid4().hex[:8]

    # ---- stream JSONL → canonical staging in size-bounded groups, one COPY
    # per group (see _GROUP_RAW_BYTES), computing global count + digest ------
    n_rows = 0
    digest = 0
    tmp_parts: list[Path] = []
    staging: Path | None = None
    fh = None
    writer = None
    group_rows = 0
    group_raw_bytes = 0

    def _copy_group() -> None:
        """Close the open staging group and COPY it → its tmp parquet part."""
        nonlocal staging, fh, writer, group_rows, group_raw_bytes
        writer.close()
        fh.close()
        if group_rows:
            tmp_pq = out_dir / f".compact-part-{len(tmp_parts)}-{token}.parquet"
            conn.execute(
                f"COPY (SELECT {', '.join(_COLUMN_ORDER)} FROM read_json("
                f"{_sql_str(staging)}, columns={_STAGING_COLUMNS}, "
                f"format='newline_delimited', maximum_object_size={_MAX_OBJECT_SIZE})) "
                f"TO {_sql_str(tmp_pq)} (FORMAT parquet, COMPRESSION zstd, "
                f"ROW_GROUP_SIZE_BYTES '{_ROW_GROUP_SIZE_BYTES}')"
            )
            tmp_parts.append(tmp_pq)
        staging.unlink(missing_ok=True)
        staging = None
        fh = None
        writer = None
        group_rows = 0
        group_raw_bytes = 0

    try:
        for part in part_files:
            hour = part.parent.name.split("=", 1)[1]
            lines, truncated = _read_lines(part)
            report["truncated_files"] += int(truncated)
            for i, line in enumerate(lines):
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError as exc:
                    # _read_lines already dropped any torn tail: an
                    # undecodable line here is corruption, not truncation.
                    raise CompactionError(
                        f"{part}: undecodable JSON at line {i + 1}"
                    ) from exc
                try:
                    if rec["venue"] != venue or rec["endpoint"] != endpoint:
                        raise CompactionError(
                            f"{part}: record venue/endpoint "
                            f"({rec['venue']}/{rec['endpoint']}) does not match "
                            f"partition {venue}/{endpoint}"
                        )
                    fetched_at = rec["fetched_at"]
                    if fetched_at[:10] != date or fetched_at[11:13] != hour:
                        raise CompactionError(
                            f"{part}: fetched_at {fetched_at!r} does not match "
                            f"partition date={date}/hour={hour}"
                        )
                    if _canonical(rec) != line:
                        report["noncanonical_lines"] += 1
                    params_txt = _canonical(rec["params"])
                    payload_txt = _canonical(rec["payload"])
                    staging_rec = {
                        "v": rec["v"],
                        "venue": venue,
                        "endpoint": endpoint,
                        "params": params_txt,
                        "fetched_at": fetched_at,
                        "http_status": rec["http_status"],
                        "elapsed_ms": rec["elapsed_ms"],
                        "payload": payload_txt,
                        "hour": hour,
                    }
                except KeyError as exc:
                    raise CompactionError(
                        f"{part}: line {i + 1} is missing capture field {exc}"
                    ) from exc
                if writer is None:
                    staging = out_dir / (
                        f".compact-staging-{token}-g{len(tmp_parts)}.jsonl.zst"
                    )
                    fh = open(staging, "wb")
                    writer = zstandard.ZstdCompressor(level=1).stream_writer(fh)
                encoded = (_canonical(staging_rec) + "\n").encode("utf-8")
                writer.write(encoded)
                group_raw_bytes += len(encoded)
                group_rows += 1
                digest = (
                    digest
                    + _row_digest((
                        str(rec["v"]), venue, endpoint, params_txt, fetched_at,
                        str(rec["http_status"]), str(rec["elapsed_ms"]),
                        payload_txt, hour,
                    ))
                ) % _DIGEST_MOD
                n_rows += 1
                if group_raw_bytes >= _GROUP_RAW_BYTES:
                    _copy_group()
        if writer is not None:
            _copy_group()

        if not tmp_parts:
            # 0 complete lines (e.g. an all-torn recovered orphan): write an
            # empty parquet so the partition is marked done and the lake view
            # serves it exactly once.
            tmp_pq = out_dir / f".compact-part-0-{token}.parquet"
            conn.execute(
                f"COPY (SELECT CAST(NULL AS BIGINT) v, CAST(NULL AS VARCHAR) venue, "
                f"CAST(NULL AS VARCHAR) endpoint, CAST(NULL AS VARCHAR) params, "
                f"CAST(NULL AS VARCHAR) fetched_at, CAST(NULL AS INTEGER) http_status, "
                f"CAST(NULL AS BIGINT) elapsed_ms, CAST(NULL AS VARCHAR) payload, "
                f'CAST(NULL AS VARCHAR) AS "hour" WHERE false) '
                f"TO {_sql_str(tmp_pq)} (FORMAT parquet, COMPRESSION zstd)"
            )
            tmp_parts.append(tmp_pq)

        # ---- verify: row count + full-content digest across all parts ------
        parquet_rows = 0
        parquet_digest = 0
        for tmp_pq in tmp_parts:
            parquet_rows += conn.execute(
                f"SELECT count(*) FROM read_parquet({_sql_str(tmp_pq)})"
            ).fetchone()[0]
            cur = conn.execute(
                f"SELECT {', '.join(_COLUMN_ORDER)} FROM read_parquet({_sql_str(tmp_pq)})"
            )
            # 256/batch: payloads run to multi-MB (batch candlesticks), so a
            # large fetchmany materializes GBs of Python strings at once.
            while batch := cur.fetchmany(256):
                for row in batch:
                    parquet_digest = (
                        parquet_digest
                        + _row_digest((
                            str(row[0]), row[1], row[2], row[3], row[4],
                            str(row[5]), str(row[6]), row[7], row[8],
                        ))
                    ) % _DIGEST_MOD
        if parquet_rows != n_rows:
            raise CompactionError(
                f"{out_dir}: row count mismatch — parquet part(s) {parquet_rows} "
                f"vs JSONL {n_rows} lines (partition {venue}/{endpoint}/{date})"
            )
        if parquet_digest != digest:
            raise CompactionError(
                f"{out_dir}: content digest mismatch vs source JSONL "
                f"(partition {venue}/{endpoint}/{date}) — parquet NOT finalized"
            )
    except Exception:
        for tmp_pq in tmp_parts:
            tmp_pq.unlink(missing_ok=True)
        if staging is not None:
            staging.unlink(missing_ok=True)
        raise

    # Finalize highest part first: part-0 is the partition's served/pending
    # gate everywhere (_day_pending, lake._discover_raw, health), so it must
    # appear only once every other part of the set is already in place.
    for k in range(len(tmp_parts) - 1, -1, -1):
        tmp_parts[k].replace(out_dir / f"part-{k}.parquet")
    final_parts = [out_dir / f"part-{k}.parquet" for k in range(len(tmp_parts))]
    report["action"] = "written"
    report["rows"] = n_rows
    report["parts"] = len(final_parts)
    report["parquet_bytes"] = sum(p.stat().st_size for p in final_parts)
    report["seconds"] = round(time.monotonic() - started, 3)
    logger.info(
        "compacted %s/%s %s: %d rows, %d files, %.1f MB raw → %.1f MB parquet "
        "(%d part%s, %.1fs)",
        venue, endpoint, date, n_rows, len(part_files),
        report["raw_bytes"] / 1e6, report["parquet_bytes"] / 1e6,
        len(final_parts), "" if len(final_parts) == 1 else "s", report["seconds"],
    )
    return report


def _utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _validate_date(date: str) -> None:
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"date must be YYYY-MM-DD, got {date!r}") from exc


def _date_partitions(raw_root: Path, date: str, venue: str | None) -> list[Path]:
    pattern = f"venue={venue}" if venue else "venue=*"
    return sorted(raw_root.glob(f"{pattern}/endpoint=*/date={date}"))


def compact_day(
    raw_root: Path,
    out_root: Path,
    date: str,
    venue: str | None = None,
    *,
    today: str | None = None,
) -> dict[str, Any]:
    """Compact every (venue, endpoint) partition of one COMPLETE UTC day.

    Refuses date >= today (UTC): an in-progress day would produce a parquet
    that goes stale the moment the next sweep lands. ``today`` is injectable
    for tests/validation only. Returns a report dict; raises CompactionError
    on any verification failure. Never touches source .zst files.
    """
    _validate_date(date)
    today = today or _utc_today()
    if date >= today:
        raise ValueError(
            f"refusing to compact {date}: not a complete UTC day yet (today={today})"
        )
    raw_root, out_root = Path(raw_root), Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    # Serialize compactions on this out_root: the in-loop hourly guard and a
    # manual `pmkt compact` may fire concurrently, and two writers racing the
    # same part-0.parquet temp/finalize (plus _clean_stale_temps) can corrupt
    # or delete each other's work. Blocking is fine — runs are idempotent.
    lock = open(out_root / ".compact.lock", "w")
    fcntl.flock(lock, fcntl.LOCK_EX)
    conn = _connect(out_root)
    partitions: list[dict[str, Any]] = []
    try:
        for date_dir in _date_partitions(raw_root, date, venue):
            endpoint_dir = date_dir.parent
            venue_dir = endpoint_dir.parent
            part_venue = venue_dir.name.split("=", 1)[1]
            part_endpoint = endpoint_dir.name.split("=", 1)[1]
            out_dir = out_root / venue_dir.name / endpoint_dir.name / date_dir.name
            partitions.append(
                _compact_partition(conn, date_dir, out_dir, part_venue, part_endpoint, date)
            )
    finally:
        conn.close()
        lock.close()  # releases the flock
    return {
        "date": date,
        "partitions": partitions,
        "rows": sum(p["rows"] for p in partitions),
        "written": sum(p["action"] == "written" for p in partitions),
        "skipped": sum(p["action"] == "skipped-existing" for p in partitions),
        "deferred": sum(p["action"] == "deferred-tmp" for p in partitions),
        "raw_bytes": sum(p["raw_bytes"] for p in partitions),
        "parquet_bytes": sum(p["parquet_bytes"] for p in partitions),
    }


def _day_pending(raw_root: Path, out_root: Path, date: str, venue: str | None) -> bool:
    """A day is pending if any raw partition of it lacks a finalized parquet."""
    for date_dir in _date_partitions(raw_root, date, venue):
        out_file = (
            out_root / date_dir.parent.parent.name / date_dir.parent.name
            / date_dir.name / PARQUET_PART_NAME
        )
        if not out_file.exists():
            return True
    return False


def compact_all_pending(
    raw_root: Path,
    out_root: Path,
    venue: str | None = None,
    *,
    today: str | None = None,
    store: Any | None = None,
) -> list[dict[str, Any]]:
    """Compact every complete UTC day that is not yet fully compacted.

    Discovery is purely by directory listing vs. existing parquet outputs (the
    filesystem is the truth — rebuildable with no state). Fully-compacted days
    are skipped WITHOUT re-verification (compact_day on a specific date still
    re-verifies row counts if invoked directly). ``store`` may be a HotStore;
    if given, an informational watermark is recorded under
    (venue='lake', endpoint='compact') — never used for discovery.
    """
    raw_root, out_root = Path(raw_root), Path(out_root)
    today = today or _utc_today()
    dates = sorted({
        p.name.split("=", 1)[1]
        for p in raw_root.glob(
            f"venue={venue}/endpoint=*/date=*" if venue else "venue=*/endpoint=*/date=*"
        )
    })
    reports: list[dict[str, Any]] = []
    for date in dates:
        if date >= today:
            continue  # incomplete day — tomorrow's run picks it up
        if not _day_pending(raw_root, out_root, date, venue):
            continue
        try:
            reports.append(compact_day(raw_root, out_root, date, venue, today=today))
        except CompactionError:
            # One poison day (e.g. verified parquet vs raw mismatch after a
            # late orphan recovery) needs a human; the OTHER pending days
            # don't — never let it halt compaction wholesale.
            logger.exception("compaction of %s failed — continuing with other days", date)
            reports.append({"date": date, "error": "failed", "rows": 0,
                            "written": 0, "skipped": 0, "deferred": 0})
    if store is not None and reports:
        done = [d["date"] for d in reports if d["deferred"] == 0]
        if done:
            store.set_state(
                "lake", "compact", cursor=max(done),
                watermark_ts=int(time.time()), now_ts=int(time.time()),
            )
    return reports
