"""Local research CLI with an offline synthetic entry point.

    uv run pmkt demo --output .demo
    uv run pmkt --data-dir .demo ui
    uv run pmkt --allow-network kalshi metadata
    uv run pmkt --allow-network polymarket prices
    uv run pmkt --allow-network --continuous run
    uv run pmkt --allow-network --continuous kalshi ws
    uv run pmkt compact --date YYYY-MM-DD
    uv run pmkt lake build-wallet-skills

Network adapters and continuous collection are experimental. Data processing
requires separately obtained inputs; the offline demo creates its own bounded
synthetic workspace. No command submits exchange orders.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import sqlite3
import sys
import time
from pathlib import Path

from .capture_guard import CaptureGuard
from .locking import fcntl
from .config import Config
from .rawlake import RawLake, hold_block_reader_lock
from .ratelimit import TokenBucket
from .state import HotStore

logger = logging.getLogger("pmkt")

# Full-phrase fallbacks for corruption surfacing through non-sqlite3 wrappers
# (in-daemon DuckDB attaches hot.sqlite and re-raises the same text without a
# sqlite_errorcode). Full phrases only, so e.g. httpx "malformed HTTP" protocol
# errors can never match.
_FATAL_DB_PHRASES = ("database disk image is malformed", "file is not a database")


def _is_fatal_db_error(exc: BaseException) -> bool:
    """Connection-level SQLite corruption that never heals without a fresh
    connection (2026-07-19 incident: a wedged 'malformed' conn failed every
    job for 46h while launchd saw a healthy process and quick_check said ok).
    Anchored on the primary result code — SQLITE_CORRUPT(11)/SQLITE_NOTADB(26)
    — so transient lock/busy (OperationalError, SQLITE_BUSY/LOCKED) and
    constraint bugs (IntegrityError) stay retryable per-cycle."""
    if isinstance(exc, sqlite3.Error):
        code = (getattr(exc, "sqlite_errorcode", 0) or 0) & 0xFF
        return code in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB)
    return any(phrase in str(exc).lower() for phrase in _FATAL_DB_PHRASES)


def _acquire_run_lock(config: Config):
    """Exactly one collector loop may write to a data dir (one SQLite writer,
    one rate budget). Returns the held lock file; exits if another loop —
    launchd job or manual run — already holds it."""
    lock_path = config.data_dir / "collector.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = open(lock_path, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.error(
            "another collector loop already holds %s — refusing to double-run "
            "(is the launchd job active? launchctl list | grep pmkt)",
            lock_path,
        )
        sys.exit(1)
    return lock


def _build(config: Config):
    """One RawLake + one HotStore serve both venues; each venue gets its own
    client and rate budget (the venues' limits are unrelated)."""
    from .kalshi.client import KalshiPublicClient
    from .polymarket.client import PolymarketClient

    signer = None
    if config.kalshi_auth_configured:
        from .kalshi.auth import KalshiSigner

        signer = KalshiSigner(config.kalshi_api_key_id, config.kalshi_private_key_path)
        logger.info(
            "kalshi auth: signing as key %s… (%.0f req/s)",
            config.kalshi_api_key_id[:8], config.rps,
        )
    else:
        logger.info("kalshi auth: none configured — unauthenticated (%.0f req/s)", config.rps)

    lake = RawLake(config.raw_dir, capture_guard=CaptureGuard(config.capture_policy))
    store = HotStore(config.db_path)
    kalshi_client = KalshiPublicClient(
        config.kalshi_base_url,
        lake,
        TokenBucket(config.rps, config.rps_burst),
        timeout_s=config.http_timeout_s,
        signer=signer,
    )
    pm_client = PolymarketClient(
        lake,
        TokenBucket(config.polymarket_rps, config.polymarket_rps * 2),
        timeout_s=config.http_timeout_s,
    )
    return lake, store, kalshi_client, pm_client


async def _run_once(config: Config, venue: str, job_name: str) -> None:
    from .kalshi import collector as kalshi_collector
    from .polymarket import collector as pm_collector

    lake, store, kalshi_client, pm_client = _build(config)
    try:
        if venue == "kalshi" and job_name == "limits":
            import json

            print(json.dumps(await kalshi_client.get_api_limits(), indent=2))
        elif venue == "kalshi":
            from .kalshi import backfill as kalshi_backfill

            jobs = {
                "metadata": kalshi_collector.collect_metadata,
                "prices": kalshi_collector.collect_prices,
                "trades": kalshi_collector.collect_trades,
                "settlements": kalshi_collector.collect_settlements,
                "backfill-markets": kalshi_backfill.backfill_markets,
                "backfill-trades": kalshi_backfill.backfill_trades,
                "backfill-candles": kalshi_backfill.backfill_candles,
            }
            await jobs[job_name](kalshi_client, store)
        else:
            from .polymarket import backfill as pm_backfill

            jobs = {
                "metadata": pm_collector.collect_metadata,
                "prices": pm_collector.collect_prices,
                "trades": pm_collector.collect_trades,
                "backfill": pm_collector.backfill_metadata,
                "backfill-trades": pm_backfill.backfill_trades,
            }
            await jobs[job_name](pm_client, store)
    finally:
        await kalshi_client.aclose()
        await pm_client.aclose()
        lake.close()
        store.close()


async def _drive_schedule(schedule: list[list], lake: RawLake, store: HotStore) -> None:
    """Run due jobs forever, sequentially (one SQLite writer). Each entry is
    [job, client, interval_s, next_due]; next_due slides after every run."""
    while True:
        now = time.monotonic()
        due = [entry for entry in schedule if entry[3] <= now]
        for entry in due:
            job, client, interval, _ = entry
            try:
                # Also pause scheduled local writers/compaction launches.
                await lake.wait_for_capture()
                await job(client, store)
            except Exception as exc:
                logger.exception("%s failed; retrying next cycle", job.__name__)
                # A fresh process clears a wedged connection — exit and
                # let launchd restart the process. If the corruption is REAL, the
                # restarted process fails the same way and launchd
                # throttling + pmkt health make it visible.
                if _is_fatal_db_error(exc):
                    logger.error(
                        "database reported corrupt — exiting for a "
                        "fresh process restart"
                    )
                    sys.exit(1)
            entry[3] = time.monotonic() + interval * random.uniform(0.95, 1.05)
        sleep_for = max(0.5, min(entry[3] for entry in schedule) - time.monotonic())
        await asyncio.sleep(sleep_for)


async def _run_loop(config: Config) -> None:
    # Loaded now, not lazily per compaction: the block-reader lock below
    # vouches for the decoder this process compacts with (rawlake.py).
    from .compact import compact_all_pending
    from .kalshi import collector as kalshi_collector
    from .polymarket import collector as pm_collector

    run_lock = _acquire_run_lock(config)  # held for process lifetime
    # Lets the WS processes block-flush (kalshi/ws.py ws_flush_mode): this
    # compaction reads its unterminated crash orphans exactly.
    block_reader = hold_block_reader_lock(config.data_dir)
    lake, store, kalshi_client, pm_client = _build(config)

    compaction_task: asyncio.Task | None = None

    async def compaction_job(client, store):
        """Opportunistic guard: compact any complete UTC day not yet done.
        Fire-and-forget in a worker thread — compaction can take minutes per
        pending day (more once WS capture lands) and must never stall the
        price cadence; it touches only raw/ + raw-parquet/, never SQLite.
        Overlap-guarded; a same-machine `pmkt compact` is additionally
        serialized by the flock inside compact_day. Partitions holding a live
        .tmp defer to the next check, so an early fire is safe."""
        nonlocal compaction_task

        # Idle writers (3h metadata cadence) can pin yesterday's partition as
        # .tmp long past midnight — finalize them so the day can compact.
        lake.rotate_idle()
        if compaction_task and not compaction_task.done():
            logger.info("compaction still running from last check — skipping")
            return

        async def _run():
            reports = await asyncio.to_thread(
                compact_all_pending, config.raw_dir, config.data_dir / "raw-parquet"
            )
            for r in reports:
                logger.info("compacted %s", r)

        compaction_task = asyncio.create_task(_run(), name="compaction")
        compaction_task.add_done_callback(
            lambda t: t.cancelled() or t.exception() and logger.error(
                "compaction task failed: %r", t.exception()
            )
        )

    from .kalshi import backfill as kalshi_backfill
    from .model.decide import (
        crosswalk_refresh,
        decide,
        decide_grade,
        resolutions_refresh,
    )
    from .model.ofi import ofi_score
    from .model.score import model_score
    from .polymarket import backfill as pm_backfill

    async def decide_job(client, store):
        """Dual-venue decide: PM client is the scheduled client; bind the
        Kalshi client so matched events log a decision on both venues."""
        return await decide(client, store, kalshi_client=kalshi_client)

    schedule = [
        # (job, client, interval_s, next_due) — kalshi metadata first so the
        # price sweep has a universe; jobs run sequentially (one SQLite writer).
        [kalshi_collector.collect_metadata, kalshi_client, config.metadata_interval_s, 0.0],
        [kalshi_collector.collect_prices, kalshi_client, config.prices_interval_s, 0.0],
        [kalshi_collector.collect_trades, kalshi_client, config.trades_interval_s, 0.0],
        # Settlement outcomes (training labels + resolution tracking).
        [kalshi_collector.collect_settlements, kalshi_client, 1800.0, 0.0],
        # Model scoring -> forecasts table + daily snapshot (client unused).
        # 15 min, not hourly: fast movers (intraday crypto/index ladders)
        # drift from a stale forecast base and pollute the edge sort.
        [model_score, None, 900.0, 0.0],
        [pm_collector.collect_metadata, pm_client, config.polymarket_metadata_interval_s, 0.0],
        [pm_collector.collect_prices, pm_client, config.polymarket_prices_interval_s, 0.0],
        [pm_collector.collect_trades, pm_client, config.polymarket_trades_interval_s, 0.0],
        # Skilled OFI: wallet-skill-weighted order-flow imbalance per PM market.
        # Runs after pm collect_trades so it sees the latest tape.
        [ofi_score, None, 900.0, 0.0],
        # Shadow decision ledger: gate -> requote fresh books -> EV -> append.
        # Dual-venue: PM markets on their CLOB books + the same signal on the
        # paired Kalshi book for confirmed cross-venue matches. Record-only
        # until PMKT_DECIDE_THETA is set. Thresholds require prospective
        # validation with observed quotes and costs. Never places orders.
        [decide_job, pm_client, 900.0, 0.0],
        # Grading chain: capture recently-closed markets' final prices (the
        # active-only metadata sweep never re-captures a market whose event
        # left enumeration — capture gap found live 2026-07-19), rebuild
        # pm_resolutions (6h, subprocess) so newly resolved markets become
        # gradeable, then grade resolved ledger rows (hourly, light
        # in-daemon join). The shadow track record accrues without manual
        # runs.
        [pm_collector.capture_closed_markets, pm_client, 21600.0, 0.0],
        [resolutions_refresh, None, 21600.0, 0.0],
        # Cross-venue matcher (subprocess, reads hot RO): keep confirmed
        # matched events current before games close, run post-settlement QA.
        [crosswalk_refresh, None, 21600.0, 0.0],
        [decide_grade, None, 3600.0, 0.0],
        [compaction_job, None, config.compact_interval_s, 0.0],
        # Historical backfills: budgeted cursor-resumable slices that chip
        # away every cycle and become near-free no-ops once complete.
        [kalshi_backfill.backfill_markets, kalshi_client, config.backfill_interval_s, 0.0],
        [kalshi_backfill.backfill_trades, kalshi_client, config.backfill_interval_s, 0.0],
        [kalshi_backfill.backfill_candles, kalshi_client, config.backfill_interval_s, 0.0],
        [pm_collector.backfill_metadata, pm_client, config.backfill_interval_s, 0.0],
        [pm_backfill.backfill_trades, pm_client, config.backfill_interval_s, 0.0],
    ]
    logger.info(
        "collector loop: kalshi meta %ds / prices %ds / trades %ds, "
        "polymarket meta %ds / prices %ds / trades %ds, compact check %ds, "
        "backfill slices %ds — Ctrl-C to stop",
        config.metadata_interval_s, config.prices_interval_s, config.trades_interval_s,
        config.polymarket_metadata_interval_s, config.polymarket_prices_interval_s,
        config.polymarket_trades_interval_s,
        config.compact_interval_s, config.backfill_interval_s,
    )
    try:
        await _drive_schedule(schedule, lake, store)
    finally:
        await kalshi_client.aclose()
        await pm_client.aclose()
        lake.close()
        store.close()
        block_reader.close()
        run_lock.close()


def _print_status(config: Config) -> None:
    if not config.db_path.exists():
        print("No local dataset. Run pmkt demo --output .demo to create synthetic research data.")
        return
    conn = sqlite3.connect(config.db_path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        print(f"data dir: {config.data_dir.resolve()}")
        for row in conn.execute(
            "SELECT venue, endpoint, cursor, watermark_ts, updated_at FROM collector_state"
        ):
            print(
                f"  state {row['venue']}/{row['endpoint']}: "
                f"watermark={row['watermark_ts']} cursor={'yes' if row['cursor'] else 'none'} "
                f"updated={row['updated_at']}"
            )
        for table in ("series", "events", "markets", "instruments"):
            (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            print(f"  {table}: {count} rows")
        for row in conn.execute(
            "SELECT venue_id, status_norm, COUNT(*) AS n FROM markets"
            " GROUP BY venue_id, status_norm ORDER BY venue_id, n DESC"
        ):
            print(f"    markets[{row['venue_id']}/{row['status_norm']}]: {row['n']}")
    finally:
        conn.close()


def _print_crosswalk(rows: list, title: str) -> None:
    """Compact review table for `pmkt crosswalk`. Grouped by sport, one line
    per contract-side with the info needed to hand-confirm a candidate."""
    print(f"cross-venue crosswalk — {title}")
    if not rows:
        print("  (none — run `pmkt lake build-crosswalk` first if the "
              "projection is missing)")
        return
    by_sport: dict[str, int] = {}
    for r in rows:
        by_sport[r["sport"]] = by_sport.get(r["sport"], 0) + 1
    print("  " + ", ".join(f"{k}:{v}" for k, v in sorted(by_sport.items()))
          + f"  ({len(rows)} contract-sides)\n")
    for r in rows:
        agree = r.get("resolution_agree") or ""
        flag = {"1": " [QA:agree]", "0": " [QA:DISAGREE]"}.get(agree, "")
        print(
            f"  {r['sport']:4s} {r['game_date']} {r['kalshi_ticker']}"
            f"  ={r['match_method']}=  pm:{r['pm_token_id'][:18]}"
            f"  ({r['pm_slug']}){flag}"
        )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # httpx logs every request at INFO (~170k lines/day at 2 req/s) — too
    # chatty for a long-running daemon log; sweeps summarize themselves.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(prog="pmkt", description="Prediction-markets collectors")
    parser.add_argument("--data-dir", help="override PMKT_DATA_DIR")
    parser.add_argument("--allow-network", action="store_true",
                        help="enable explicitly requested venue API calls (disabled by default)")
    parser.add_argument("--continuous", action="store_true",
                        help="also enable experimental continuous collection (which may schedule backfills)")
    sub = parser.add_subparsers(dest="command", required=True)

    demo_p = sub.add_parser("demo", help="bounded offline synthetic wallet-following research example")
    demo_p.add_argument("--output", type=Path,
                        help="new output directory; omitted creates a system-temp workspace")
    demo_p.add_argument("--as-of", type=int, help="synthetic decision time as unix seconds UTC")

    kalshi = sub.add_parser("kalshi", help="Kalshi collector")
    kalshi.add_argument(
        "job",
        choices=["metadata", "prices", "trades", "settlements",
                 "backfill-markets", "backfill-trades", "backfill-candles",
                 "limits", "ws", "run"],
        help="single sweep / one backfill slice, 'limits' for the auth/tier "
        "smoke test, 'ws' for realtime WebSocket capture, or 'run' for the "
        "continuous loop",
    )

    polymarket = sub.add_parser("polymarket", help="Polymarket collector (read-only)")
    polymarket.add_argument(
        "job",
        choices=["metadata", "prices", "trades", "backfill", "backfill-trades"],
        help="single sweep / one backfill slice",
    )

    sub.add_parser("run", help="continuous multi-venue collector loop")

    compact_p = sub.add_parser("compact", help="compact complete raw days to Parquet")
    compact_p.add_argument("--date", help="one UTC day YYYY-MM-DD (default: all pending)")
    compact_p.add_argument("--venue", help="restrict to one venue")

    lake_p = sub.add_parser("lake", help="DuckDB research surface")
    lake_p.add_argument(
        "action", choices=["sql", "build-trades", "build-price-snapshots",
                           "build-wallet-skills", "build-pm-trades",
                           "build-pm-resolutions", "build-crosswalk"]
    )
    lake_p.add_argument("query", nargs="?", help="SQL text for the 'sql' action")

    model_p = sub.add_parser("model", help="forecasting model (train/score/grade/ofi)")
    model_p.add_argument("action", choices=["train", "score", "grade", "ofi"])
    model_p.add_argument(
        "--rebuild", action="store_true",
        help="ofi only: rebuild the rolling trade window from raw instead of "
             "the incremental consumed-file diff",
    )

    decide_p = sub.add_parser(
        "decide",
        help="one shadow-decision pass: gate → requote books → EV → ledger "
             "(record-only unless PMKT_DECIDE_THETA is set; never trades)",
    )
    decide_p.add_argument(
        "action", nargs="?", choices=["grade"], default=None,
        help="'grade': score resolved ledger rows → shadow track record",
    )

    xw_p = sub.add_parser(
        "crosswalk",
        help="cross-venue matched-market review (Kalshi <-> Polymarket); "
             "build via `pmkt lake build-crosswalk`",
    )
    xw_p.add_argument(
        "action", nargs="?", choices=["review", "confirmed"], default="review",
        help="review = unconfirmed candidates for manual confirmation "
             "(default); confirmed = matches usable by decide",
    )

    sub.add_parser("status", help="show collector state and hot-store counts")

    sub.add_parser(
        "health",
        help="pipeline health probe (exit 0 ok / 1 warn / 2 crit) — alarms "
             "on silent deaths: stale OFI, stopped decide job, grader backlog",
    )

    ui_p = sub.add_parser("ui", help="screener UI server (read-only)")
    ui_p.add_argument("--host", default="127.0.0.1")
    ui_p.add_argument("--port", type=int, default=8100)

    args = parser.parse_args()
    if sys.platform == "win32":
        parser.error("Native Windows is unsupported; use macOS, Linux, or WSL.")
    if args.command == "demo":
        import json
        from .demo import run_demo

        if args.data_dir:
            parser.error("demo uses --output, not --data-dir; existing workspaces are never overwritten")
        try:
            print(json.dumps(run_demo(args.output, as_of_ts=args.as_of), indent=2))
        except (FileExistsError, ValueError) as exc:
            parser.error(str(exc))
        return
    network_command = args.command in ("kalshi", "polymarket", "run") or (
        args.command == "decide" and args.action != "grade"
    )
    continuous_command = args.command == "run" or (
        args.command == "kalshi" and args.job in ("run", "ws")
    )
    if network_command and not args.allow_network:
        parser.error("Venue access is disabled. Add --allow-network before the command for explicit API access.")
    if continuous_command and not args.continuous:
        parser.error("Continuous collection is experimental and can grow a large lake. Add --continuous before the command to opt in.")
    if args.data_dir:
        # Config and ws.WsConfig both read PMKT_DATA_DIR from the environment.
        import os
        os.environ["PMKT_DATA_DIR"] = args.data_dir
    config = Config()

    if args.command == "status":
        _print_status(config)
    elif args.command == "health":
        import sys as _sys

        from .health import print_health

        _sys.exit(print_health(config.data_dir))
    elif args.command == "decide":
        import asyncio as _asyncio
        import json as _json

        if args.action == "grade":
            # NOTE: HotStore comes from the module-level import — a local
            # `from .state import HotStore` here makes the name function-local
            # EVERYWHERE in main() and broke `pmkt model ofi` with
            # UnboundLocalError (2026-07-16 evening).
            from .model.decide import grade_decisions

            store = HotStore(config.db_path)
            try:
                print(_json.dumps(grade_decisions(config.data_dir, store), indent=2))
            finally:
                store.close()
        else:
            async def _decide_once() -> dict:
                from .model.decide import decide_once

                lake, store, kalshi_client, pm_client = _build(config)
                try:
                    return await decide_once(
                        pm_client, store, kalshi_client=kalshi_client,
                        data_dir=config.data_dir,
                    )
                finally:
                    await kalshi_client.aclose()
                    await pm_client.aclose()
                    lake.close()
                    store.close()

            print(_json.dumps(_asyncio.run(_decide_once()), indent=2))
    elif args.command == "crosswalk":
        from . import lake

        if args.action == "confirmed":
            rows = lake.read_venue_crosswalk(config.data_dir, confirmed=1)
            _print_crosswalk(rows, "confirmed matches (usable by decide)")
        else:
            rows = lake.read_venue_crosswalk(config.data_dir, confirmed=0)
            _print_crosswalk(rows, "unconfirmed candidates — review then confirm")
            if rows:
                path = lake.crosswalk_overrides_path(config.data_dir)
                print(
                    f"\nTo confirm a pair, append a row to {path}:\n"
                    "  kalshi_ticker,pm_token_id,confirmed,source,note,updated_ts\n"
                    "  <ticker>,<token_id>,1,manual,<why>,<unix_ts>\n"
                    "then rerun `pmkt lake build-crosswalk`."
                )
    elif args.command == "ui":
        from .ui import serve

        serve(config, host=args.host, port=args.port)
    elif args.command == "run" or (args.command == "kalshi" and args.job == "run"):
        # `kalshi run` kept as an alias: the installed processes calls it,
        # and it has run the multi-venue loop since Polymarket landed.
        try:
            asyncio.run(_run_loop(config))
        except KeyboardInterrupt:
            logger.info("stopped")
    elif args.command == "kalshi" and args.job == "ws":
        from .kalshi import ws as kalshi_ws

        try:
            asyncio.run(kalshi_ws.run())
        except KeyboardInterrupt:
            logger.info("stopped")
    elif args.command in ("kalshi", "polymarket"):
        asyncio.run(_run_once(config, args.command, args.job))
    elif args.command == "compact":
        import json as _json

        from .compact import compact_all_pending, compact_day

        out_root = config.data_dir / "raw-parquet"
        if args.date:
            print(_json.dumps(
                compact_day(config.raw_dir, out_root, args.date, args.venue), indent=2
            ))
        else:
            for report in compact_all_pending(config.raw_dir, out_root, args.venue):
                print(_json.dumps(report, indent=2))
    elif args.command == "lake":
        from . import lake

        if args.action == "sql":
            if not args.query:
                parser.error("lake sql needs a query string")
            lake.run_sql(config.data_dir, args.query).show()
        elif args.action == "build-trades":
            import json as _json

            print(_json.dumps(lake.build_trades(config.data_dir), indent=2))
        elif args.action == "build-price-snapshots":
            import json as _json

            print(_json.dumps(lake.build_price_snapshots(config.data_dir), indent=2))
        elif args.action == "build-wallet-skills":
            import json as _json

            print(_json.dumps(lake.build_wallet_skills(config.data_dir), indent=2))
        elif args.action == "build-pm-resolutions":
            import json as _json

            print(_json.dumps(lake.build_pm_resolutions(config.data_dir), indent=2))
        elif args.action == "build-crosswalk":
            import json as _json

            print(_json.dumps(lake.build_venue_crosswalk(config.data_dir), indent=2))
        else:  # build-pm-trades
            import json as _json

            print(_json.dumps(lake.build_pm_trades(config.data_dir), indent=2))
    elif args.command == "model":
        import json as _json

        if args.action == "train":
            from .model.calibrate import train

            report = train(config.data_dir)
            print(_json.dumps(report.get("metrics", report), indent=2, default=str))
        elif args.action == "score":
            from .model.score import score_once

            store = HotStore(config.db_path)
            try:
                print(_json.dumps(score_once(config.data_dir, store), indent=2))
            finally:
                store.close()
        elif args.action == "grade":
            from .model.grade import grade

            print(_json.dumps(grade(config.data_dir), indent=2, default=str))
        else:  # ofi
            from .model.ofi import ofi_score_once

            store = HotStore(config.db_path)
            try:
                print(_json.dumps(
                    ofi_score_once(config.data_dir, store, rebuild=args.rebuild),
                    indent=2,
                ))
            finally:
                store.close()


if __name__ == "__main__":
    main()
