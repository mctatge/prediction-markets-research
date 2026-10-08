"""FastAPI app for the screener UI (API_CONTRACT.md v1, frozen 2026-07-12).

READ-ONLY by construction: SQLite is opened with a ``mode=ro`` URI (+
``PRAGMA query_only``) per request — this process can never write the hot
store or take its write lock (the collector loop is the one writer). The
history endpoint scans the raw lake via DuckDB in a worker thread with a
hard timeout so the event loop never blocks on an uncompacted-day scan.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..config import Config
from ..model.fees import register_fee_udf
from . import history, screener
from .model_live import register_model_udf
from .ofi_live import register_ofi_udf
from .rows import row_mid_key, shape_row

logger = logging.getLogger(__name__)

MAX_LIMIT = 200
DEFAULT_LIMIT = 100
CORS_ORIGINS = ["http://localhost:5173"]

SortName = Literal[screener.SORT_NAMES]  # type: ignore[valid-type]
StatusName = Literal[screener.STATUS_VALUES]  # type: ignore[valid-type]
VenueName = Literal[screener.VENUES]  # type: ignore[valid-type]


def _default_ui_dist() -> Path:
    # src/pmkt/ui/app.py → repo root → ui/dist (vite build output)
    return Path(__file__).resolve().parents[3] / "ui" / "dist"


def _open_ro(db_path: Path) -> sqlite3.Connection:
    """Read-only hot-store connection. mode=ro refuses every write at the
    SQLite level; query_only is belt-and-suspenders. Opens are cheap — one
    per request keeps threading trivial (handlers are sync → thread pool)."""
    uri = f"file:{quote(str(db_path), safe='/')}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    except sqlite3.OperationalError as exc:
        raise HTTPException(503, f"hot store unavailable: {exc}") from exc
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=1")
    return conn


class _SPAStaticFiles(StaticFiles):
    """Static serving with SPA fallback: unknown non-file paths (client-side
    routes) return index.html instead of 404. /api never reaches this mount
    (API routes are registered first)."""

    async def get_response(self, path: str, scope):  # type: ignore[override]
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == 404:
                return await super().get_response("index.html", scope)
            raise


def create_app(
    config: Config | None = None, *, ui_dist: Path | None = None
) -> FastAPI:
    config = config or Config()
    db_path = config.db_path
    data_dir = config.data_dir
    ui_dist = _default_ui_dist() if ui_dist is None else ui_dist
    history_cache = history.TTLCache()

    def research_report() -> dict[str, Any]:
        path = data_dir / "demo-report.json"
        if path.is_file() and path.stat().st_size <= 5_000_000:
            try:
                report = json.loads(path.read_text())
                if report.get("schema_version") == 1 and report.get("data_mode") == "synthetic":
                    return report
            except (OSError, ValueError, AttributeError):
                pass
        return {"data_mode": "local", "wallets": [], "selected_wallets": [],
                "flows": [], "decisions": [], "grading": None, "limitations": []}

    app = FastAPI(title="pmkt screener", docs_url=None, redoc_url=None)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @app.get("/api/research")
    def api_research() -> dict[str, Any]:
        """Saved synthetic research stages; never triggers collection or computation."""
        return research_report()

    # -- /api/screener -------------------------------------------------------

    @app.get("/api/screener")
    def api_screener(
        venue: VenueName | None = None,
        category: str | None = None,
        q: str | None = None,
        status: StatusName = "open",
        price_min: Annotated[float | None, Query(ge=0, le=1)] = None,
        price_max: Annotated[float | None, Query(ge=0, le=1)] = None,
        vol24h_min: Annotated[float | None, Query(ge=0)] = None,
        liquidity_min: Annotated[float | None, Query(ge=0)] = None,
        spread_max: Annotated[float | None, Query(ge=0, le=1)] = None,
        close_within_h: Annotated[float | None, Query(gt=0)] = None,
        exclude_churn: bool = True,
        sort: SortName = "vol24h",
        dir: Literal["desc", "asc"] = "desc",
        limit: Annotated[int, Query(ge=1)] = DEFAULT_LIMIT,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, Any]:
        now_ts = int(time.time())
        if not db_path.exists():
            return {"total": 0, "generated_at": now_ts, "rows": []}
        limit = min(limit, MAX_LIMIT)  # contract: clamp, don't reject
        where, args = screener.build_where(
            venue=venue,
            category=category,
            q=q,
            status=status,
            price_min=price_min,
            price_max=price_max,
            vol24h_min=vol24h_min,
            liquidity_min=liquidity_min,
            spread_max=spread_max,
            close_within_h=close_within_h,
            exclude_churn=exclude_churn,
            now_ts=now_ts,
        )
        category_filtered = category is not None
        rows_sql, rows_args = screener.build_rows_query(
            where,
            args,
            sort=sort,
            direction=dir,
            limit=limit,
            offset=offset,
            category_filtered=category_filtered,
        )
        count_sql, count_args = screener.build_count_query(
            where, args, category_filtered=category_filtered
        )
        conn = _open_ro(db_path)
        try:
            register_model_udf(conn, data_dir, now_ts)  # live model_p/edge
            register_ofi_udf(conn)  # live skilled_ofi
            register_fee_udf(conn)  # live exec_edge
            rows = [shape_row(r) for r in conn.execute(rows_sql, rows_args)]
            (total,) = conn.execute(count_sql, count_args).fetchone()
        finally:
            conn.close()
        return {"total": total, "generated_at": now_ts, "rows": rows}

    # -- /api/decisions (shadow decision ledger) ---------------------------

    @app.get("/api/decisions")
    def api_decisions(
        limit: Annotated[int, Query(ge=1)] = 200,
        graded_only: bool = False,
    ) -> dict[str, Any]:
        now_ts = int(time.time())
        limit = min(limit, 500)
        if not db_path.exists():
            return {"generated_at": now_ts, "rows": [], "summary": {
                "decisions": 0, "passed_gates": 0, "graded": 0, "last_pass_ts": None,
                "win_rate": None, "mean_ret_per_dollar": None, "equity": [],
            }}
        conn = _open_ro(db_path)
        try:
            where = "WHERE g.decision_id IS NOT NULL" if graded_only else ""
            rows = [
                {
                    "decision_id": r["decision_id"],
                    "ts": r["ts"],
                    "venue": r["venue_id"],
                    "market_id": r["venue_market_id"],
                    "title": r["title"],
                    "category": r["category"],
                    "event_key": r["event_key"],
                    "signal_venue_id": r["signal_venue_id"],
                    "signal_market_id": r["signal_market_id"],
                    "action": r["action"],
                    "side": r["g_side"] or (
                        "BUY_YES"
                        if float(r["model_p"] or 0) >= float(r["base_prob"] or 0)
                        else "BUY_NO"
                    ),
                    "ev": r["ev"],
                    "model_p": r["model_p"],
                    "bid": r["bid_prob"],
                    "ask": r["ask_prob"],
                    "spread": r["spread_prob"],
                    "skilled_ofi": r["skilled_ofi"],
                    "n_skilled_trades": r["n_skilled_trades"],
                    "ofi_age_s": r["ofi_age_s"],
                    "gates": json.loads(r["gates_json"]),
                    "passed_gates": bool(r["passed_gates"]),
                    "theta": r["theta"],
                    "graded": (
                        {
                            "won": bool(r["won"]),
                            "ret_per_dollar": r["ret_per_dollar"],
                            "ev_gap": r["ev_gap"],
                            "entry_prob": r["entry_prob"],
                        }
                        if r["g_side"] is not None else None
                    ),
                }
                for r in conn.execute(
                    f"""SELECT d.*, m.title,
                               g.side AS g_side, g.won, g.ret_per_dollar,
                               g.ev_gap, g.entry_prob
                        FROM decisions d
                        LEFT JOIN decision_grades g
                          ON g.decision_id = d.decision_id
                        LEFT JOIN markets m
                          ON m.venue_id = d.venue_id
                         AND m.venue_market_id = d.venue_market_id
                        {where}
                        ORDER BY d.ts DESC, d.decision_id DESC
                        LIMIT ?""",
                    (limit,),
                )
            ]
            (n_total, n_passed, last_ts) = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(passed_gates), 0), MAX(ts) "
                "FROM decisions"
            ).fetchone()
            graded = conn.execute(
                """SELECT d.ts, g.won, g.ret_per_dollar
                   FROM decision_grades g
                   JOIN decisions d ON d.decision_id = g.decision_id
                   ORDER BY d.ts, g.decision_id"""
            ).fetchall()
            cum = 0.0
            equity = []
            for g in graded:
                cum += float(g["ret_per_dollar"])
                equity.append({"ts": g["ts"], "cum_ret": round(cum, 4)})
            summary = {
                "decisions": n_total,
                "passed_gates": n_passed,
                "graded": len(graded),
                "last_pass_ts": last_ts,
                "win_rate": (
                    round(sum(g["won"] for g in graded) / len(graded), 4)
                    if graded else None
                ),
                "mean_ret_per_dollar": (
                    round(cum / len(graded), 4) if graded else None
                ),
                "equity": equity[-500:],
            }
        finally:
            conn.close()
        return {"generated_at": now_ts, "summary": summary, "rows": rows}

    # -- /api/market/{venue}/{market_id} --------------------------------------

    def _load_market(
        conn: sqlite3.Connection, venue: str, market_id: str
    ) -> sqlite3.Row:
        if venue not in screener.VENUES:
            raise HTTPException(404, "unknown venue")
        row = conn.execute(
            screener.build_detail_query(by_event=False), (venue, market_id)
        ).fetchone()
        if row is None:
            raise HTTPException(404, "unknown market")
        return row

    @app.get("/api/market/{venue}/{market_id}")
    def api_market(venue: str, market_id: str) -> dict[str, Any]:
        conn = _open_ro(db_path)
        try:
            register_model_udf(conn, data_dir, int(time.time()))  # live model_p/edge
            register_ofi_udf(conn)  # live skilled_ofi
            register_fee_udf(conn)  # live exec_edge
            row = _load_market(conn, venue, market_id)
            shaped = shape_row(row)
            if row["venue_event_id"] is None:
                siblings = [shaped]
            else:
                siblings = [
                    shape_row(r)
                    for r in conn.execute(
                        screener.build_detail_query(by_event=True),
                        (venue, row["venue_event_id"]),
                    )
                ]
        finally:
            conn.close()
        # contract: mid desc, nulls last, row itself included
        siblings.sort(
            key=lambda s: (row_mid_key(s) is None, -(row_mid_key(s) or 0)),
        )
        return {"row": shaped, "siblings": siblings}

    # -- /api/market/{venue}/{market_id}/history ------------------------------

    @app.get("/api/market/{venue}/{market_id}/history")
    async def api_history(
        venue: str,
        market_id: str,
        hours: Annotated[float, Query(gt=0, le=168)] = 24,
    ) -> dict[str, Any]:
        def _market_context() -> str | None:
            conn = _open_ro(db_path)
            try:
                register_model_udf(conn, data_dir, int(time.time()))  # detail query uses it
                register_ofi_udf(conn)
                register_fee_udf(conn)
                _load_market(conn, venue, market_id)  # 404 gate
                if venue != "polymarket":
                    return None
                token = conn.execute(
                    "SELECT venue_token_id FROM instruments"
                    " WHERE venue_id=? AND venue_market_id=? AND is_primary=1"
                    " AND venue_token_id IS NOT NULL LIMIT 1",
                    (venue, market_id),
                ).fetchone()
                if token is None:
                    raise LookupError  # no primary token → no history
                return token["venue_token_id"]
            finally:
                conn.close()

        loop = asyncio.get_running_loop()
        try:
            # sqlite lookups are ~µs but still I/O — keep the loop clean
            token_id = await loop.run_in_executor(None, _market_context)
        except LookupError:
            return {"points": []}

        cache_key = (venue, market_id, round(hours, 3))
        cached = history_cache.get(cache_key)
        if cached is not None:
            return {"points": cached}

        holder: dict[str, Any] = {}
        fut = loop.run_in_executor(
            None,
            functools.partial(
                history.fetch_history,
                data_dir,
                venue,
                market_id,
                hours,
                token_id=token_id,
                conn_holder=holder,
            ),
        )
        try:
            points = await asyncio.wait_for(
                asyncio.shield(fut), timeout=history.TIMEOUT_S
            )
        except asyncio.TimeoutError:
            logger.warning(
                "history scan timed out (>%ss) for %s/%s hours=%s",
                history.TIMEOUT_S, venue, market_id, hours,
            )
            conn = holder.get("conn")
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.interrupt()  # best-effort: free the worker thread
            fut.add_done_callback(lambda f: f.exception())  # swallow late error
            points = []
        except Exception:
            logger.exception("history scan failed for %s/%s", venue, market_id)
            points = []
        # cache empties too — a timed-out scan must not be retried per render
        history_cache.set(cache_key, points)
        return {"points": points}

    # -- /api/meta -------------------------------------------------------------

    @app.get("/api/meta")
    def api_meta() -> dict[str, Any]:
        data_mode = research_report()["data_mode"]
        if not db_path.exists():
            return {"venues": [], "categories": [],
                    "counts": {"open_total": 0, "open_kalshi": 0, "open_polymarket": 0},
                    "freshness": [], "data_mode": data_mode, "setup_required": True,
                    "setup_hint": "Run pmkt demo --output .demo, then pmkt --data-dir .demo ui."}
        conn = _open_ro(db_path)
        try:
            venues = [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT venue_id FROM markets ORDER BY venue_id"
                )
            ]
            categories = [
                {"venue": r[0], "category": r[1], "n": r[2]}
                for r in conn.execute(
                    f"SELECT m.venue_id, {screener.CATEGORY_EXPR} AS category,"
                    f" COUNT(*) AS n FROM markets m{screener.JOINS}"
                    f" WHERE m.status_norm='open'"
                    f" AND {screener.CATEGORY_EXPR} IS NOT NULL"
                    f" GROUP BY m.venue_id, {screener.CATEGORY_EXPR}"
                    f" ORDER BY n DESC, category LIMIT 40"
                )
            ]
            counts = {"open_total": 0, "open_kalshi": 0, "open_polymarket": 0}
            for venue_id, n in conn.execute(
                "SELECT venue_id, COUNT(*) FROM markets"
                " WHERE status_norm='open' GROUP BY venue_id"
            ):
                counts[f"open_{venue_id}"] = n
                counts["open_total"] += n
            freshness = [
                {"venue": r[0], "endpoint": r[1], "watermark_ts": r[2]}
                for r in conn.execute(
                    "SELECT venue, endpoint, MAX(watermark_ts) FROM collector_state"
                    " GROUP BY venue, endpoint ORDER BY venue, endpoint"
                )
            ]
        finally:
            conn.close()
        return {
            "venues": venues,
            "data_mode": data_mode,
            "setup_required": False,
            "categories": categories,
            "counts": counts,
            "freshness": freshness,
        }

    # -- static / SPA ----------------------------------------------------------

    if (ui_dist / "index.html").exists():
        app.mount("/", _SPAStaticFiles(directory=ui_dist, html=True), name="ui")
    else:

        @app.get("/")
        def index_placeholder(request: Request) -> dict[str, Any]:
            return {
                "service": "pmkt screener API",
                "hint": "build the frontend (ui/dist) or run vite on :5173",
                "api": ["/api/screener", "/api/market/{venue}/{market_id}",
                        "/api/market/{venue}/{market_id}/history", "/api/meta"],
            }

    return app


def serve(
    config: Config | None = None, host: str = "127.0.0.1", port: int = 8100
) -> None:
    """Start the local read-only UI server for `pmkt ui`."""
    import uvicorn

    uvicorn.run(create_app(config or Config()), host=host, port=port)
