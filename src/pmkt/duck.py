"""Shared DuckDB session bootstrap: explicit spill + memory guardrails.

Bare ``duckdb.connect()`` defaults are wrong for anything running in (or next
to) the long-lived collector daemon (incident 2026-07-12→15, 41 failed
compaction cycles): ``temp_directory`` defaults to the RELATIVE path ".tmp" —
resolved against the process cwd and shared by every in-process DuckDB
connection, so it can vanish from under a spilling query (IOException
'.tmp/duckdb_temp_block-…: No such file or directory') — and ``memory_limit``
defaults to 80% of RAM, which lets one heavy query exhaust the machine next
to the live collector (OutOfMemoryException at 12.7 GiB). ``connect()`` here
gives every session an owned ABSOLUTE spill dir on the data volume, a DuckDB
memory-limit setting and a thread cap. The memory limit does not cap total
process memory. ``preserve_insertion_order=false`` lets pipelines stream out-of-core
instead of buffering — any consumer that needs an order must state it with
ORDER BY (all current ones do).

Sessions sharing one spill root share its ``.duckdb-tmp/``; that is safe for
the run-loop jobs (the daemon executes them sequentially) and, unlike the
cwd-relative default, its location does not depend on later working-directory
changes. External deletion can still interrupt active queries.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

MEMORY_LIMIT = os.environ.get("PMKT_DUCKDB_MEMORY_LIMIT", "4GB")
THREADS = int(os.environ.get("PMKT_DUCKDB_THREADS", "4"))
SPILL_LIMIT = os.environ.get("PMKT_DUCKDB_SPILL_LIMIT", "1GB")


def connect(spill_root: Path) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB session with the guardrails above applied.

    ``spill_root`` must be a directory on the same volume as whatever the
    session reads/writes (the data dir for lake/model sessions); an owned
    ``.duckdb-tmp/`` under it becomes the out-of-core spill directory.
    """
    tmp_dir = Path(spill_root) / ".duckdb-tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    conn.execute("SET temp_directory = '" + str(tmp_dir).replace("'", "''") + "'")
    conn.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    conn.execute(f"SET threads = {THREADS}")
    conn.execute(f"SET max_temp_directory_size = '{SPILL_LIMIT}'")
    conn.execute("SET preserve_insertion_order = false")
    return conn
