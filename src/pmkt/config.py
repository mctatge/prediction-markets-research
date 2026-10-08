"""Runtime configuration.

Environment variables override runtime settings. Credentials belong in local
.env or key files and must not be committed. Authenticated REST requests and
WebSocket capture use the configured Kalshi credentials.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from .capture_guard import CapturePolicy

# Load .env from the current working directory. Real environment variables win.
load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)

KALSHI_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


def _env_float_opt(name: str) -> float | None:
    raw = os.environ.get(name)
    return float(raw) if raw else None


@dataclass(frozen=True)
class Config:
    capture_policy: CapturePolicy = field(default_factory=CapturePolicy.from_env)

    # Data root holds the raw lake, and the SQLite hot store under db/.
    data_dir: Path = field(
        default_factory=lambda: Path(os.environ.get("PMKT_DATA_DIR", "data"))
    )
    kalshi_base_url: str = field(
        default_factory=lambda: os.environ.get("PMKT_KALSHI_BASE_URL", KALSHI_BASE_URL)
    )
    # Kalshi API credentials for KALSHI-* signing; keep the key file local.
    kalshi_api_key_id: str = field(
        default_factory=lambda: os.environ.get("KALSHI_API_KEY_ID", "")
    )
    kalshi_private_key_path: Path = field(
        default_factory=lambda: Path(
            os.environ.get("KALSHI_PRIVATE_KEY_PATH", ".secrets/kalshi-api-key.pem")
        )
    )

    # Rate limits: unauthenticated is undocumented and tight (429s observed at
    # ~4 req/s on 2026-07-10) → 2/s. Authenticated Basic tier is 200 read
    # tokens/s at 10/request = 20 reads/s → default 15/s for headroom.
    # Explicit PMKT_RPS always wins.
    requests_per_second: float | None = field(
        default_factory=lambda: _env_float_opt("PMKT_RPS")
    )
    burst: float | None = field(default_factory=lambda: _env_float_opt("PMKT_BURST"))
    http_timeout_s: float = field(
        default_factory=lambda: _env_float("PMKT_HTTP_TIMEOUT", 30.0)
    )

    # Default collection cadences, in seconds.
    metadata_interval_s: float = field(
        default_factory=lambda: _env_float("PMKT_METADATA_INTERVAL", 3600.0)
    )
    prices_interval_s: float = field(
        default_factory=lambda: _env_float("PMKT_PRICES_INTERVAL", 300.0)
    )
    trades_interval_s: float = field(
        default_factory=lambda: _env_float("PMKT_TRADES_INTERVAL", 900.0)
    )

    # Polymarket: no auth for any read. Cloudflare THROTTLES (queues) instead
    # of consistently returning 429 → conservative rate, sequential requests.
    # 2.5/s uses <1% of Gamma's documented /events budget.
    polymarket_rps: float = field(
        default_factory=lambda: _env_float("PMKT_POLYMARKET_RPS", 2.5)
    )
    # 3h, not hourly: gamma_events pages are ~313KB compressed and a full
    # enumeration is ~30MB/sweep — hourly would be ~0.7GB/day of raw metadata
    # (measured 2026-07-11). Prices don't depend on this cadence for freshness.
    polymarket_metadata_interval_s: float = field(
        default_factory=lambda: _env_float("PMKT_PM_METADATA_INTERVAL", 10800.0)
    )
    # 15 min, not 5: the sweep is ~580 requests (~4 min at 2.5/s) and
    # prices-history self-backfills across the gap, so nothing is lost.
    polymarket_prices_interval_s: float = field(
        default_factory=lambda: _env_float("PMKT_PM_PRICES_INTERVAL", 900.0)
    )
    # Wallet-attributed trade tape (Data-API /trades). 15 min matches the
    # Kalshi trades cadence; per-market crawl budgeted at 400 pages/sweep.
    polymarket_trades_interval_s: float = field(
        default_factory=lambda: _env_float("PMKT_PM_TRADES_INTERVAL", 900.0)
    )

    # Hourly opportunistic compaction check: compacts any complete UTC day
    # not yet done; near-free when there's nothing pending.
    compact_interval_s: float = field(
        default_factory=lambda: _env_float("PMKT_COMPACT_INTERVAL", 3600.0)
    )

    # Backfill slice cadence: each slice is page-budgeted (~15-25s of the
    # kalshi budget, ~16s of polymarket's), so 10 min interleaves cleanly
    # with the live sweeps. No-ops once each crawl completes.
    backfill_interval_s: float = field(
        default_factory=lambda: _env_float("PMKT_BACKFILL_INTERVAL", 600.0)
    )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "db" / "hot.sqlite"

    @property
    def kalshi_auth_configured(self) -> bool:
        return bool(self.kalshi_api_key_id) and self.kalshi_private_key_path.exists()

    @property
    def rps(self) -> float:
        if self.requests_per_second is not None:
            return self.requests_per_second
        return 15.0 if self.kalshi_auth_configured else 2.0

    @property
    def rps_burst(self) -> float:
        return self.burst if self.burst is not None else self.rps * 2
