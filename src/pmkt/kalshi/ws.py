"""Kalshi WebSocket capture — realtime public-data collector (raw-capture-first).

Connects to wss://external-api-ws.kalshi.com/trade-api/ws/v2 and captures the
public data channels (`ticker`, `trade`, `market_lifecycle_v2`) into the raw
lake. raw-capture preservation applies frame-by-frame: EVERY inbound and outbound frame
is appended to the RawLake before any parsing decision; nothing is
parse-and-discarded. Order/portfolio channels are never subscribed;
this process captures read-only market data.

Live-verified facts (2026-07-11, prod):
- Auth is REQUIRED on the handshake even for public channels: the same three
  RSA headers as REST (KALSHI-ACCESS-KEY / -TIMESTAMP / -SIGNATURE), signing
  ``timestamp + "GET" + "/trade-api/ws/v2"`` — the WS path, no query params.
- `ticker` and `trade` accept subscribe WITHOUT market_tickers = all-markets
  firehose (docs: "Market specification optional (omit to receive all …)").
  `market_lifecycle_v2` is global-only (ticker filters unsupported). Firehose
  is the default mode; PMKT_WS_FIREHOSE=0 forces per-ticker subscriptions fed
  from the SQLite hot store (opened READ-ONLY — this process never writes it).
- Server sends WS ping frames every 10s (body "heartbeat"); the websockets
  library auto-answers pongs, and the local ping_interval/ping_timeout detect a
  dead peer independently.
- Command errors arrive as {"id", "type": "error", "msg": {"code", "msg"}}.
  Code 26 = per-subscription market limit (the client halves the chunk size and
  retry), 27 = command rate limit (exponential backoff + retry). Channel
  errors 10/17/25 are terminal per the AsyncAPI spec → recycle the
  connection, which resubscribes everything.

Raw-lake conventions for WS records (documented for parsers):
- endpoint: "ws_<type>" for data frames (ws_ticker, ws_trade,
  ws_market_lifecycle_v2); "ws_control" for everything else — subscribe
  acks, errors, outbound commands, and connection lifecycle meta records.
- params.direction: "in" (server frame), "out" (outgoing command), or
  "meta" (synthetic connection-lifecycle record written by this process).
- http_status: 0 — sentinel meaning "WebSocket frame, no HTTP layer"
  (the field is kept so WS records share the REST capture schema).
- elapsed_ms: 0 (no request/response cycle to time).

WebSocket settings come from environment variables: PMKT_DATA_DIR (default
"data"), KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY_PATH (default
".secrets/kalshi-api-key.pem"), PMKT_WS_URL, PMKT_WS_FIREHOSE, PMKT_WS_CHUNK,
PMKT_WS_REFRESH_INTERVAL, PMKT_WS_DURATION (seconds; unset = run forever).

Run it: ``uv run pmkt --allow-network kalshi ws --continuous``.
A flock on <data_dir>/ws.lock
(separate from collector.lock) guarantees one WS capture per data dir.
"""

from __future__ import annotations

import asyncio
from ..locking import fcntl
import json
import logging
import os
import random
import signal
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import zstandard
from dotenv import load_dotenv
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from ..capture_guard import CaptureGuard, CapturePaused, CapturePolicy
from ..rawlake import RawLake, block_reader_live
from .auth import KalshiSigner
from .models import VENUE_ID

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)

DEFAULT_WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"

# Public market-data channels only (storage invariants: no order/portfolio
# channels — fill/user_orders/market_positions are never subscribed here).
PUBLIC_CHANNELS = ("ticker", "trade", "market_lifecycle_v2")
# Channels that accept market_tickers; market_lifecycle_v2 is global-only.
TICKERED_CHANNELS = frozenset({"ticker", "trade"})
# Frame types routed to their own raw-lake endpoint (ws_<type>).
DATA_TYPES = frozenset(PUBLIC_CHANNELS)

WS_HTTP_STATUS_SENTINEL = 0  # "WebSocket frame — no HTTP status"

ERR_ALREADY_SUBSCRIBED = 6
ERR_MARKET_TICKER_REQUIRED = 14
ERR_MARKET_LIMIT = 26
ERR_CMD_RATE_LIMIT = 27
# AsyncAPI spec: these channel errors are terminal; the client must resubscribe.
TERMINAL_ERROR_CODES = frozenset({10, 17, 25})

# Same activity rule as state.HotStore.active_market_tickers — duplicated
# here because this process opens the hot store READ-ONLY and must not import
# a writer. CAST is only a filter; stored values stay exact TEXT.
ACTIVE_TICKERS_SQL = """
    SELECT venue_market_id FROM markets
    WHERE venue_id=? AND status_norm='open'
      AND (CAST(volume_24h AS REAL) > 0 OR CAST(open_interest AS REAL) > 0)
    ORDER BY venue_market_id
"""

# A connection that survives this long resets the reconnect backoff.
STABLE_CONNECTION_S = 30.0


def _env_float_opt(name: str) -> float | None:
    raw = os.environ.get(name)
    return float(raw) if raw else None


def ws_flush_mode(data_dir: Path) -> int:
    """FLUSH_BLOCK only while a collector that reads its crash orphans runs.

    Small WS frames share one compression window per part file under
    FLUSH_BLOCK (see rawlake.py), but a SIGKILLed writer then leaves an
    unterminated frame that a collector started before
    rawlake.decompress_raw_file compacts short — silently and for good (see
    rawlake.py "Deploy skew"). Such a collector does not hold
    rawlake.BLOCK_READER_LOCK, so the WS writer falls back to per-record
    frames, which every decoder reads exactly. Re-checked every stats
    interval: the processes may restart in any order, and a fallback lasts only
    until a current collector is up (a stopped collector also means frames).
    """
    return zstandard.FLUSH_BLOCK if block_reader_live(data_dir) else zstandard.FLUSH_FRAME


@dataclass(frozen=True)
class WsConfig:
    data_dir: Path
    url: str = DEFAULT_WS_URL
    api_key_id: str = ""
    private_key_path: Path = Path(".secrets/kalshi-api-key.pem")
    channels: tuple[str, ...] = PUBLIC_CHANNELS
    capture_policy: CapturePolicy = field(default_factory=CapturePolicy.from_env)
    firehose: bool = True  # verified supported for ticker/trade; preferred
    chunk_size: int = 100  # list-mode tickers per subscribe (halved on err 26)
    refresh_interval_s: float = 3600.0  # list-mode universe refresh cadence
    reconnect_base_s: float = 1.0
    reconnect_cap_s: float = 60.0
    command_timeout_s: float = 30.0
    ping_interval_s: float = 15.0
    ping_timeout_s: float = 10.0
    stats_interval_s: float = 60.0
    duration_s: float | None = None  # None = run until stopped

    @classmethod
    def from_env(cls) -> "WsConfig":
        # PMKT_WS_CHANNELS: comma-separated subset of PUBLIC_CHANNELS. The
        # deployed processes (plist) runs "trade,market_lifecycle_v2" — the ticker
        # firehose is ~80% of the ~1.2GB/hr peak raw volume and its price
        # coverage is largely redundant with the 1-min candle sweep until the
        # live-screener UI exists. Flip it on by editing the plist env.
        raw_channels = os.environ.get("PMKT_WS_CHANNELS")
        if raw_channels is not None and not raw_channels.strip(" ,"):
            # Set-but-empty must not silently fall back to ALL channels —
            # that would turn the ticker firehose ON by accident.
            raise ValueError(
                "PMKT_WS_CHANNELS is set but empty; unset it for the default "
                f"({', '.join(PUBLIC_CHANNELS)}) or name the channels you want"
            )
        channels = tuple(
            c.strip() for c in (raw_channels or "").split(",") if c.strip()
        ) or PUBLIC_CHANNELS
        unknown = set(channels) - set(PUBLIC_CHANNELS)
        if unknown:
            raise ValueError(
                f"PMKT_WS_CHANNELS contains non-public channels {sorted(unknown)}; "
                f"allowed: {PUBLIC_CHANNELS}"  # public-channel restriction
            )
        return cls(
            data_dir=Path(os.environ.get("PMKT_DATA_DIR", "data")),
            url=os.environ.get("PMKT_WS_URL", DEFAULT_WS_URL),
            api_key_id=os.environ.get("KALSHI_API_KEY_ID", ""),
            private_key_path=Path(
                os.environ.get("KALSHI_PRIVATE_KEY_PATH", ".secrets/kalshi-api-key.pem")
            ),
            channels=channels,
            firehose=os.environ.get("PMKT_WS_FIREHOSE", "1") != "0",
            chunk_size=int(os.environ.get("PMKT_WS_CHUNK", "100")),
            refresh_interval_s=_env_float_opt("PMKT_WS_REFRESH_INTERVAL") or 3600.0,
            duration_s=_env_float_opt("PMKT_WS_DURATION"),
        )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "db" / "hot.sqlite"

    @property
    def auth_configured(self) -> bool:
        return bool(self.api_key_id) and self.private_key_path.exists()

    @property
    def sign_path(self) -> str:
        """Signing path = the WS URL path, query-less (live-verified:
        timestamp + "GET" + "/trade-api/ws/v2")."""
        return urlparse(self.url).path or "/"


class SubscribeRejected(Exception):
    def __init__(self, code: int | None, payload: dict[str, Any]) -> None:
        super().__init__(f"subscribe rejected: code={code} {str(payload)[:200]}")
        self.code = code
        self.payload = payload


class WsCapture:
    def __init__(self, config: WsConfig, lake: RawLake) -> None:
        self.config = config
        self.lake = lake
        self._signer: KalshiSigner | None = None
        if config.auth_configured:
            self._signer = KalshiSigner(config.api_key_id, config.private_key_path)
            logger.info("kalshi ws auth: signing as key %s…", config.api_key_id[:8])
        else:
            logger.warning(
                "kalshi ws auth: NOT configured (KALSHI_API_KEY_ID / key file) — "
                "prod handshake will be rejected"
            )
        self._stop = asyncio.Event()
        self._ws: Any = None
        self._cmd_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._subs: dict[int, str] = {}  # sid -> channel
        self._chunk_size = max(1, config.chunk_size)
        self._list_mode_active = False
        self._deliberate_recycle = False
        self.counts: Counter[str] = Counter()

    # -- raw capture (storage invariants: capture BEFORE parsing) -----------------

    def _capture(self, endpoint: str, params: dict[str, Any], payload: Any) -> None:
        self.lake.append(
            venue=VENUE_ID,
            endpoint=endpoint,
            params=params,
            http_status=WS_HTTP_STATUS_SENTINEL,
            elapsed_ms=0,
            payload=payload,
        )
        self.counts[endpoint] += 1

    def _capture_out(self, command: dict[str, Any]) -> None:
        self._capture(
            "ws_control",
            {"direction": "out", "cmd": command.get("cmd"), "id": command.get("id")},
            command,
        )

    def _capture_meta(self, event: str, **details: Any) -> None:
        """Synthetic connection-lifecycle record (connect/disconnect/refresh)
        — marked direction=meta so parsers can't mistake it for a venue frame.
        These make capture gaps explainable from the lake alone."""
        try:
            self._capture("ws_control", {"direction": "meta"}, {"event": event, **details})
        except CapturePaused:
            # Disk safety also covers diagnostic raw records. The guard has
            # logged the gap; never recurse into failing raw writes here.
            logger.info("ws %s metadata not captured during disk-space pause", event)

    # -- connection loop ------------------------------------------------------

    def request_stop(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            asyncio.get_running_loop().create_task(ws.close())

    async def run(self) -> None:
        """Outer loop: connect, subscribe, capture until disconnect, then
        reconnect with exponential backoff + jitter and full resubscribe."""
        attempt = 0
        watchdog = None
        if self.config.duration_s is not None:
            watchdog = asyncio.create_task(self._duration_watchdog())
        stats = asyncio.create_task(self._stats_loop())
        try:
            while not self._stop.is_set():
                guard = self.lake.capture_guard
                if guard is not None and not await guard.wait_until_ready(self._stop):
                    break
                connected_at = None
                try:
                    async with connect(
                        self.config.url,
                        additional_headers=self._auth_headers(),
                        ping_interval=self.config.ping_interval_s,
                        ping_timeout=self.config.ping_timeout_s,
                        user_agent_header="pmkt-collector/0.1 (personal research)",
                    ) as ws:
                        connected_at = time.monotonic()
                        self._capture_meta("connected", url=self.config.url)
                        logger.info("ws connected: %s", self.config.url)
                        await self._session(ws)
                except asyncio.CancelledError:
                    raise
                except CapturePaused:
                    # Exiting connect closes the socket. Do not consume more
                    # frames or reconnect until free space recovers; this is
                    # an intentional, logged capture gap (REST self-backfills).
                    logger.warning("ws disconnected for disk-space pause; capture gap begins")
                    attempt = 0
                    continue
                except InvalidStatus as exc:
                    status = exc.response.status_code
                    self._capture_meta("handshake_rejected", status=status)
                    logger.error(
                        "ws handshake rejected with HTTP %d%s",
                        status,
                        " — check KALSHI_API_KEY_ID / private key" if status in (401, 403) else "",
                    )
                except Exception as exc:
                    self._capture_meta("disconnected", error=repr(exc))
                    logger.warning("ws disconnected: %r", exc)
                else:
                    self._capture_meta("disconnected", error=None)
                    logger.info("ws closed cleanly")
                if self._stop.is_set():
                    break
                if self._deliberate_recycle:
                    self._deliberate_recycle = False
                    attempt = 0
                    await self._sleep_unless_stopped(1.0)
                    continue
                if connected_at and time.monotonic() - connected_at >= STABLE_CONNECTION_S:
                    attempt = 0  # the link was healthy; don't punish a blip
                attempt += 1
                delay = min(
                    self.config.reconnect_cap_s,
                    self.config.reconnect_base_s * 2 ** (attempt - 1),
                ) * random.uniform(0.5, 1.5)
                logger.info("reconnecting in %.1fs (attempt %d)", delay, attempt)
                await self._sleep_unless_stopped(delay)
        finally:
            stats.cancel()
            if watchdog:
                watchdog.cancel()
            self._log_stats(final=True)

    def _auth_headers(self) -> dict[str, str] | None:
        # Regenerated per attempt: the signature embeds a fresh ms timestamp.
        if self._signer is None:
            return None
        return self._signer.headers("GET", self.config.sign_path)

    async def _duration_watchdog(self) -> None:
        await asyncio.sleep(self.config.duration_s or 0)
        logger.info("duration %.0fs reached — stopping", self.config.duration_s or 0)
        self.request_stop()

    async def _sleep_unless_stopped(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    # -- per-connection session ------------------------------------------------

    async def _session(self, ws: Any) -> None:
        self._ws = ws
        self._cmd_id = 0
        self._pending.clear()
        self._subs.clear()
        self._list_mode_active = False
        subscriber = asyncio.create_task(self._subscribe_all(ws))
        refresher = asyncio.create_task(self._refresh_loop(ws))
        try:
            async for raw in ws:
                self._on_frame(ws, raw)
                if self._stop.is_set():
                    break
        finally:
            self._ws = None
            subscriber.cancel()
            refresher.cancel()
            for fut in self._pending.values():
                if not fut.done():
                    fut.cancel()
            self._pending.clear()

    def _on_frame(self, ws: Any, raw: str | bytes) -> None:
        """Route one inbound frame: capture FIRST, then react (storage invariants)."""
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        try:
            payload: Any = json.loads(text)
        except ValueError:
            payload = {"_non_json_frame": text[:10_000]}
        ftype = payload.get("type") if isinstance(payload, dict) else None
        if ftype in DATA_TYPES:
            self._capture(
                f"ws_{ftype}",
                {"direction": "in", "channel": ftype, "sid": payload.get("sid")},
                payload,
            )
            return
        self._capture(
            "ws_control",
            {"direction": "in", "type": ftype, "sid": payload.get("sid") if isinstance(payload, dict) else None},
            payload,
        )
        if not isinstance(payload, dict):
            return
        if ftype == "subscribed":
            msg = payload.get("msg") or {}
            if "sid" in msg:
                self._subs[msg["sid"]] = msg.get("channel", "?")
        cmd_id = payload.get("id")
        fut = self._pending.pop(cmd_id, None) if cmd_id is not None else None
        if fut is not None and not fut.done():
            fut.set_result(payload)
            return
        if ftype == "error":
            code = (payload.get("msg") or {}).get("code")
            if code in TERMINAL_ERROR_CODES:
                logger.warning(
                    "terminal channel error %s — recycling connection to resubscribe", code
                )
                self._deliberate_recycle = True
                asyncio.get_running_loop().create_task(ws.close())
            else:
                logger.warning("unsolicited ws error: %s", str(payload)[:200])

    # -- subscriptions ----------------------------------------------------------

    async def _send_command(self, ws: Any, cmd: str, params: dict[str, Any]) -> dict[str, Any]:
        self._cmd_id += 1
        command = {"id": self._cmd_id, "cmd": cmd, "params": params}
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[self._cmd_id] = fut
        self._capture_out(command)  # outbound frames hit the lake too
        await ws.send(json.dumps(command, separators=(",", ":")))
        try:
            return await asyncio.wait_for(fut, timeout=self.config.command_timeout_s)
        finally:
            self._pending.pop(command["id"], None)

    async def _subscribe(
        self, ws: Any, channel: str, tickers: list[str] | None
    ) -> dict[str, Any]:
        """One subscribe command; retries command-rate-limit (27) in place,
        raises SubscribeRejected for anything else."""
        params: dict[str, Any] = {"channels": [channel]}
        if tickers:
            params["market_tickers"] = list(tickers)
        backoff = 1.0
        while True:
            resp = await self._send_command(ws, "subscribe", params)
            if resp.get("type") == "subscribed":
                return resp
            code = (resp.get("msg") or {}).get("code")
            if code == ERR_CMD_RATE_LIMIT:
                delay = backoff * random.uniform(0.8, 1.2)
                logger.warning("ws error 27 (command rate limit) — retrying in %.1fs", delay)
                await asyncio.sleep(delay)
                backoff = min(backoff * 2, 30.0)
                continue
            raise SubscribeRejected(code, resp)

    async def _subscribe_all(self, ws: Any) -> None:
        """Subscribe every configured channel. Firehose (no market_tickers) is
        preferred and live-verified for ticker/trade; market_lifecycle_v2 is
        global-only. List mode chunk-subscribes the active universe."""
        try:
            universe: list[str] | None = None
            for channel in self.config.channels:
                want_list = not self.config.firehose and channel in TICKERED_CHANNELS
                try:
                    if not want_list:
                        await self._subscribe(ws, channel, None)
                        logger.info("subscribed: %s (all markets)", channel)
                        continue
                except SubscribeRejected as exc:
                    if exc.code == ERR_ALREADY_SUBSCRIBED:
                        continue
                    if exc.code != ERR_MARKET_TICKER_REQUIRED or channel not in TICKERED_CHANNELS:
                        logger.error("subscribe %s failed: %s", channel, exc)
                        continue
                    logger.warning(
                        "firehose rejected for %s (code 14) — falling back to ticker list",
                        channel,
                    )
                if universe is None:
                    universe = self._load_universe()
                if not universe:
                    logger.error(
                        "no active-market universe available (%s) — retrying %s as firehose",
                        self.config.db_path, channel,
                    )
                    try:
                        await self._subscribe(ws, channel, None)
                    except SubscribeRejected as exc:
                        logger.error("subscribe %s failed: %s", channel, exc)
                    continue
                self._list_mode_active = True
                await self._subscribe_chunked(ws, channel, universe)
            logger.info(
                "subscriptions live: %s",
                {sid: ch for sid, ch in sorted(self._subs.items())},
            )
        except (ConnectionClosed, asyncio.CancelledError):
            pass  # the session loop owns reconnect
        except Exception:
            logger.exception("subscription setup failed — recycling connection")
            await ws.close(code=1011, reason="subscribe failed")

    async def _subscribe_chunked(self, ws: Any, channel: str, tickers: list[str]) -> None:
        """List-mode subscribe in chunks. Error 26 (per-subscription market
        limit — threshold undocumented) halves the chunk size and retries, so
        the effective limit is discovered empirically and logged."""
        remaining = list(tickers)
        subscribed = 0
        while remaining:
            chunk = remaining[: self._chunk_size]
            try:
                await self._subscribe(ws, channel, chunk)
            except SubscribeRejected as exc:
                if exc.code == ERR_MARKET_LIMIT and len(chunk) > 1:
                    self._chunk_size = max(1, len(chunk) // 2)
                    logger.warning(
                        "ws error 26 (market limit) on %d tickers — halving chunk size to %d",
                        len(chunk), self._chunk_size,
                    )
                    continue  # retry the same tickers in smaller chunks
                if exc.code == ERR_ALREADY_SUBSCRIBED:
                    remaining = remaining[len(chunk):]
                    continue
                logger.error("subscribe %s chunk failed (%s) — skipping %d tickers",
                             channel, exc, len(chunk))
                remaining = remaining[len(chunk):]
                continue
            subscribed += len(chunk)
            remaining = remaining[len(chunk):]
        logger.info(
            "subscribed: %s (%d tickers, final chunk size %d)",
            channel, subscribed, self._chunk_size,
        )

    def _load_universe(self) -> list[str]:
        """Active-market tickers from the hot store, READ-ONLY (URI mode=ro —
        this process must never write to the collector's SQLite)."""
        db = self.config.db_path
        if not db.exists():
            return []
        try:
            conn = sqlite3.connect(f"file:{db.resolve()}?mode=ro", uri=True)
            try:
                rows = conn.execute(ACTIVE_TICKERS_SQL, (VENUE_ID,)).fetchall()
            finally:
                conn.close()
        except sqlite3.Error as exc:
            logger.error("hot store read failed (%s): %s", db, exc)
            return []
        return [r[0] for r in rows]

    async def _refresh_loop(self, ws: Any) -> None:
        """Hourly universe refresh, v1-simple: in list mode, recycle the
        connection — reconnect resubscribes against a fresh universe (new
        actives in, settled out). Firehose mode needs none: the venue routes
        new markets into the stream automatically."""
        await asyncio.sleep(self.config.refresh_interval_s)
        if not self._list_mode_active:
            return
        logger.info("universe refresh due — recycling connection")
        self._deliberate_recycle = True
        self._capture_meta("universe_refresh")
        await ws.close(code=1000, reason="universe refresh")

    # -- stats -------------------------------------------------------------------

    async def _stats_loop(self) -> None:
        prev: Counter[str] = Counter()
        while True:
            await asyncio.sleep(self.config.stats_interval_s)
            delta = self.counts - prev
            prev = self.counts.copy()
            per_s = {k: round(v / self.config.stats_interval_s, 1) for k, v in sorted(delta.items())}
            logger.info("frames last %.0fs: %s (totals %s)",
                        self.config.stats_interval_s, per_s or "none", dict(sorted(self.counts.items())))
            # Quiet endpoints (ws_control after startup) must not pin a stale
            # hour partition as .tmp past midnight — compaction defers on it.
            self.lake.rotate_idle()
            self._sync_flush_mode()

    def _sync_flush_mode(self) -> None:
        """Follow the collector's decoder (ws_flush_mode); a change finalizes
        the open part files so no block-flushed file outlives its reader."""
        mode = ws_flush_mode(self.config.data_dir)
        if self.lake.set_flush_mode(mode):
            block = mode == zstandard.FLUSH_BLOCK
            logger.info(
                "raw flush mode -> %s (block-reader lock %s)",
                "FLUSH_BLOCK" if block else "FLUSH_FRAME", "held" if block else "not held",
            )

    def _log_stats(self, final: bool = False) -> None:
        logger.info("%sframe counts: %s", "final " if final else "", dict(sorted(self.counts.items())))


def _acquire_ws_lock(data_dir: Path):
    """Exactly one WS capture per data dir (one raw-lake writer set, one
    subscription budget). Same flock pattern as cli._acquire_run_lock but a
    separate file — the REST collector loop and this process coexist."""
    lock_path = data_dir / "ws.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = open(lock_path, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.error(
            "another ws capture already holds %s — refusing to double-run "
            "(is the launchd job active? launchctl list | grep pmkt-ws)",
            lock_path,
        )
        sys.exit(1)
    return lock


async def run(config: WsConfig | None = None) -> WsCapture:
    """Entry point: lock the data dir, open the lake, capture until stopped.
    Returns the WsCapture (frame counters) for callers/tests."""
    config = config or WsConfig.from_env()
    lock = _acquire_ws_lock(config.data_dir)
    # FLUSH_BLOCK beside a current collector, per-record frames otherwise
    # (ws_flush_mode); REST collectors keep the per-record-frame default.
    lake = RawLake(
        config.raw_dir,
        capture_guard=CaptureGuard(config.capture_policy),
        flush_mode=ws_flush_mode(config.data_dir),
    )
    capture = WsCapture(config, lake)
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, capture.request_stop)
    except NotImplementedError:  # non-POSIX loops (tests never hit this on macOS)
        pass
    try:
        await capture.run()
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, ValueError):
                pass
        lake.close()
        lock.close()
    return capture


def main() -> None:
    if os.environ.get("PMKT_ALLOW_NETWORK") != "1" or os.environ.get("PMKT_ALLOW_CONTINUOUS") != "1":
        raise SystemExit("Use pmkt --allow-network --continuous kalshi ws for explicit experimental capture.")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # websockets logs each keepalive at DEBUG but connection churn at INFO;
    # keep the daemon log quiet the same way cli.py quiets httpx.
    logging.getLogger("websockets").setLevel(logging.WARNING)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("stopped")


if __name__ == "__main__":
    main()
