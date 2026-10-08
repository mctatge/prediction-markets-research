"""duck.connect session guardrails (incident 2026-07-12→15: bare
duckdb.connect() defaults — cwd-relative ".tmp" spill dir, 80%-of-RAM
memory_limit — OOM'd the daemon)."""

from __future__ import annotations

import duckdb

from pmkt import duck


def _settings(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    return dict(
        conn.execute(
            "SELECT name, value FROM duckdb_settings() WHERE name IN "
            "('temp_directory', 'memory_limit', 'threads', "
            "'preserve_insertion_order')"
        ).fetchall()
    )


def test_session_guardrails_spill_dir_memory_and_threads(tmp_path, monkeypatch):
    monkeypatch.setattr(duck, "MEMORY_LIMIT", "512MiB")
    monkeypatch.setattr(duck, "THREADS", 2)
    conn = duck.connect(tmp_path)
    try:
        settings = _settings(conn)
    finally:
        conn.close()

    spill = tmp_path / ".duckdb-tmp"
    assert spill.is_dir()  # owned ABSOLUTE spill dir, created on connect
    assert settings["temp_directory"] == str(spill)
    assert settings["memory_limit"] == "512.0 MiB"  # not 80% of RAM
    assert settings["threads"] == "2"
    assert settings["preserve_insertion_order"] == "false"


def test_lake_connect_inherits_guardrails(tmp_path):
    """lake.connect() runs inside the daemon (ofi_score) — it must carry the
    memory ceiling, not just the spill dir it always had."""
    from pmkt import lake

    conn = lake.connect(tmp_path, attach_hot=False)
    try:
        settings = _settings(conn)
    finally:
        conn.close()

    assert settings["temp_directory"] == str(tmp_path / ".duckdb-tmp")
    assert settings["memory_limit"] != _default_memory_limit()
    assert settings["threads"] == str(duck.THREADS)
    assert settings["preserve_insertion_order"] == "false"


def _default_memory_limit() -> str:
    conn = duckdb.connect()
    try:
        return conn.execute(
            "SELECT current_setting('memory_limit')"
        ).fetchone()[0]
    finally:
        conn.close()
