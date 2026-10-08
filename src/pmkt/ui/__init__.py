"""Screener UI backend — FastAPI over the read-only hot store + raw lake.

Implements src/pmkt/ui/API_CONTRACT.md (frozen 2026-07-12). This process is a
READER: SQLite is opened with mode=ro and the DuckDB lake surface only ever
scans raw captures. It must never become a second writer (one collector loop
owns the hot store).
"""

from .app import create_app, serve

__all__ = ["create_app", "serve"]
