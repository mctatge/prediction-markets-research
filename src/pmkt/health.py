"""Pipeline health check — alarms when something dies SILENTLY.

The failure mode this exists for (seen 2026-07-15): a job fails or goes
stale every cycle while the rest of the loop hums along, and nothing
surfaces it until someone greps the log days later. `pmkt health` is a
one-shot, read-only probe of the hot store + filesystem that prints one
line per check and exits non-zero on problems — cron/launchd-able, and the
Decisions/status UI reads the same JSON.

Exit codes: 0 = healthy, 1 = warnings, 2 = critical.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .state import HotStore

# check -> (warn_after_s, crit_after_s) staleness thresholds
_WATERMARKS = {
    ("kalshi", "prices"): (1800, 7200),        # 5-min cadence
    ("kalshi", "trades"): (3600, 14400),       # 15-min cadence
    ("polymarket", "prices"): (3600, 14400),   # 15-min cadence
    ("polymarket", "data_api_trades"): (3600, 14400),
}
OFI_WARN_S, OFI_CRIT_S = 2700, 14400           # 15-min cadence (3 cycles / 16)
DECIDE_WARN_S, DECIDE_CRIT_S = 2700, 14400
GRADE_BACKLOG_WARN = 200                        # resolved-but-ungraded rows
COLLECTOR_LOG_WARN_S = 1800


def _age_check(
    name: str, last_ts: int | None, warn_s: int, crit_s: int, now: int,
    detail: str = "",
) -> dict[str, Any]:
    if last_ts is None:
        return {"check": name, "status": "crit", "detail": "no data ever"}
    age = now - last_ts
    status = "ok" if age <= warn_s else ("warn" if age <= crit_s else "crit")
    return {
        "check": name, "status": status,
        "detail": f"age {age // 60}min{(' — ' + detail) if detail else ''}",
    }


def run_health(data_dir: Path, now: int | None = None) -> dict[str, Any]:
    now = int(time.time()) if now is None else now
    checks: list[dict[str, Any]] = []
    store = HotStore(Path(data_dir) / "db" / "hot.sqlite")
    try:
        # Collector watermarks
        for (venue, endpoint), (warn_s, crit_s) in _WATERMARKS.items():
            row = store.get_state(venue, endpoint)
            ts = row["updated_at"] if row else None
            checks.append(_age_check(f"{venue}/{endpoint}", ts, warn_s, crit_s, now))

        # OFI scorer freshness (the 2026-07-15 silent-death case)
        (ofi_ts,) = store.conn.execute(
            "SELECT MAX(updated_ts) FROM skilled_ofi"
        ).fetchone()
        checks.append(_age_check("ofi_scorer", ofi_ts, OFI_WARN_S, OFI_CRIT_S, now))

        # Decide job: ledger must keep appending while OFI is fresh
        (dec_ts,) = store.conn.execute("SELECT MAX(ts) FROM decisions").fetchone()
        if ofi_ts is not None and now - ofi_ts <= OFI_WARN_S:
            checks.append(_age_check(
                "decide_job", dec_ts, DECIDE_WARN_S, DECIDE_CRIT_S, now
            ))
        else:
            checks.append({
                "check": "decide_job", "status": "ok",
                "detail": "skipped (no fresh OFI to decide on)",
            })

        # Grader backlog: gate-passing decisions on resolved markets, ungraded
        res_path = Path(data_dir) / "curated" / "pm_resolutions" / "part-0.parquet"
        if res_path.exists():
            (backlog,) = store.conn.execute(
                """SELECT COUNT(*) FROM decisions d
                   JOIN markets m ON m.venue_id = d.venue_id
                    AND m.venue_market_id = d.venue_market_id
                   WHERE d.passed_gates = 1 AND m.status_norm = 'settled'
                     AND d.decision_id NOT IN
                         (SELECT decision_id FROM decision_grades)"""
            ).fetchone()
            checks.append({
                "check": "grader_backlog",
                "status": "warn" if backlog > GRADE_BACKLOG_WARN else "ok",
                "detail": f"{backlog} resolved decisions ungraded"
                          + (" — run `pmkt decide grade`" if backlog else ""),
            })
        else:
            checks.append({
                "check": "grader_backlog", "status": "warn",
                "detail": "pm_resolutions not built",
            })
    finally:
        store.close()

    # Daemon liveness: the collector log must be moving
    log = Path(data_dir) / "collector.log"
    log_ts = int(log.stat().st_mtime) if log.exists() else None
    checks.append(_age_check(
        "collector_log", log_ts, COLLECTOR_LOG_WARN_S, COLLECTOR_LOG_WARN_S * 4,
        now, detail=str(log),
    ))

    # Compaction: every complete UTC day before today should be parquet
    raw_root = Path(data_dir) / "raw"
    pq_root = Path(data_dir) / "raw-parquet"
    today = time.strftime("%Y-%m-%d", time.gmtime(now))
    lagging: set[str] = set()
    for date_dir in raw_root.glob("venue=*/endpoint=*/date=*"):
        date = date_dir.name.split("=", 1)[1]
        if date >= today:
            continue
        key = pq_root / date_dir.parent.parent.name / date_dir.parent.name / date_dir.name
        if not (key / "part-0.parquet").exists():
            lagging.add(date)
    checks.append({
        "check": "compaction",
        "status": "warn" if lagging else "ok",
        "detail": f"uncompacted complete days: {sorted(lagging)}" if lagging else "current",
    })

    worst = max(
        (c["status"] for c in checks),
        key=lambda s: {"ok": 0, "warn": 1, "crit": 2}[s],
    )
    return {
        "status": worst,
        "exit_code": {"ok": 0, "warn": 1, "crit": 2}[worst],
        "checked_at": now,
        "checks": checks,
    }


def print_health(data_dir: Path) -> int:
    report = run_health(data_dir)
    print(json.dumps(report, indent=2))
    return report["exit_code"]
