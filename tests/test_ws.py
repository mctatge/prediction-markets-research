"""WebSocket capture tests — no real network; an in-process websockets server
plays Kalshi (auth handshake, subscribe acks, error 26/27 shapes from the
AsyncAPI spec, data frames with *_dollars string prices)."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from pathlib import Path

import pytest
from websockets.asyncio.server import serve

from pmkt.kalshi.ws import PUBLIC_CHANNELS, WsCapture, WsConfig, _acquire_ws_lock
from pmkt.rawlake import RawLake, read_raw_file
from pmkt.state import HotStore

# Realistic sample frames (field shapes per docs.kalshi.com asyncapi.yaml —
# prices are fixed-point dollar STRINGS, storage invariants).
SAMPLE_FRAMES = [
    {
        "type": "ticker",
        "msg": {
            "market_ticker": "KXTEST-26JUL11-T1",
            "price_dollars": "0.4800",
            "yes_bid_dollars": "0.4700",
            "yes_ask_dollars": "0.4900",
            "volume_fp": "100.00",
            "open_interest_fp": "50.00",
            "ts": 1770000000,
        },
    },
    {
        "type": "trade",
        "msg": {
            "trade_id": "d91bc706-ee49-470d-82d8-11418bda6fed",
            "market_ticker": "KXTEST-26JUL11-T1",
            "yes_price_dollars": "0.4800",
            "no_price_dollars": "0.5200",
            "count_fp": "10.00",
            "taker_side": "yes",
            "ts": 1770000000,
        },
    },
    {
        "type": "market_lifecycle_v2",
        "msg": {
            "market_ticker": "KXTEST-26JUL11-T1",
            "event_type": "created",
            "open_ts": 1770000000,
            "close_ts": 1770086400,
        },
    },
]


class FakeKalshiWs:
    """Minimal Kalshi WS impersonator: acks subscribes with {type: subscribed,
    msg: {channel, sid}}, enforces an optional per-subscription market limit
    via error 26, can drop connections, and can emit sample data frames."""

    def __init__(
        self,
        *,
        market_limit: int | None = None,
        drop_after_acks: dict[int, int] | None = None,
        send_data_after_acks: int | None = None,
    ) -> None:
        self.market_limit = market_limit
        self.drop_after_acks = drop_after_acks or {}
        self.send_data_after_acks = send_data_after_acks
        self.connections = 0
        self.commands: list[list[dict]] = []  # per-connection inbound commands
        self.headers: list[dict] = []
        self.acked: list[tuple[str, list[str] | None]] = []  # (channel, tickers)
        self.all_subscribed: set[str] = set()
        self.rejects_26 = 0
        self._sid = 0
        self.url = ""

    async def __aenter__(self) -> "FakeKalshiWs":
        self._server = await serve(self._handler, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}/trade-api/ws/v2"
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._server.close()
        await self._server.wait_closed()

    async def _handler(self, conn) -> None:
        idx = self.connections
        self.connections += 1
        self.commands.append([])
        self.headers.append(dict(conn.request.headers.raw_items()))
        acks = 0
        sid_by_channel: dict[str, int] = {}
        async for raw in conn:
            cmd = json.loads(raw)
            self.commands[idx].append(cmd)
            if cmd.get("cmd") != "subscribe":
                continue
            channel = cmd["params"]["channels"][0]
            tickers = cmd["params"].get("market_tickers")
            if self.market_limit is not None and tickers and len(tickers) > self.market_limit:
                self.rejects_26 += 1
                await conn.send(json.dumps({
                    "id": cmd["id"],
                    "type": "error",
                    "msg": {"code": 26, "msg": "Subscription market limit exceeded"},
                }))
                continue
            self._sid += 1
            sid_by_channel[channel] = self._sid
            self.acked.append((channel, tickers))
            if tickers:
                self.all_subscribed.update(tickers)
            await conn.send(json.dumps({
                "id": cmd["id"],
                "type": "subscribed",
                "msg": {"channel": channel, "sid": self._sid},
            }))
            acks += 1
            if self.send_data_after_acks is not None and acks == self.send_data_after_acks:
                for frame in SAMPLE_FRAMES:
                    out = dict(frame)
                    out["sid"] = sid_by_channel.get(frame["type"], 0)
                    await conn.send(json.dumps(out))
            if idx in self.drop_after_acks and acks >= self.drop_after_acks[idx]:
                await conn.close(code=1001, reason="test drop")
                return


def _config(tmp_path: Path, url: str, **overrides) -> WsConfig:
    defaults = dict(
        data_dir=tmp_path / "data",
        url=url,
        reconnect_base_s=0.05,
        reconnect_cap_s=0.2,
        command_timeout_s=5.0,
        stats_interval_s=3600.0,
    )
    defaults.update(overrides)
    return WsConfig(**defaults)


async def _wait_until(predicate, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met within timeout")


async def _run_capture(config: WsConfig):
    lake = RawLake(config.raw_dir)
    capture = WsCapture(config, lake)
    task = asyncio.create_task(capture.run())
    return lake, capture, task


async def _stop_capture(capture: WsCapture, task: asyncio.Task, lake: RawLake) -> None:
    capture.request_stop()
    await asyncio.wait_for(task, timeout=5.0)
    lake.close()  # finalize .tmp part files so tests can read them back


def _records(raw_dir: Path, endpoint: str) -> list[dict]:
    parts = sorted((raw_dir).rglob(f"endpoint={endpoint}/**/*.jsonl.zst"))
    out: list[dict] = []
    for p in parts:
        out.extend(read_raw_file(p))
    return out


async def test_firehose_subscribes_and_routes_endpoints(tmp_path):
    async with FakeKalshiWs(send_data_after_acks=3) as server:
        config = _config(tmp_path, server.url)
        lake, capture, task = await _run_capture(config)
        await _wait_until(
            lambda: all(
                capture.counts[f"ws_{ch}"] >= 1 for ch in PUBLIC_CHANNELS
            )
        )
        await _stop_capture(capture, task, lake)

    # Firehose: one subscribe per channel, in order, and NO market_tickers.
    subs = [c for c in server.commands[0] if c.get("cmd") == "subscribe"]
    assert [c["params"]["channels"] for c in subs] == [[ch] for ch in PUBLIC_CHANNELS]
    assert all("market_tickers" not in c["params"] for c in subs)

    # Data frames land under their own endpoints, verbatim.
    for frame in SAMPLE_FRAMES:
        recs = _records(config.raw_dir, f"ws_{frame['type']}")
        assert len(recs) == 1
        assert recs[0]["http_status"] == 0  # WS sentinel — no HTTP layer
        assert recs[0]["params"]["direction"] == "in"
        assert recs[0]["params"]["channel"] == frame["type"]
        assert recs[0]["payload"]["msg"] == frame["msg"]
    # Prices stayed fixed-point dollar strings end to end (storage invariants).
    ticker = _records(config.raw_dir, "ws_ticker")[0]["payload"]["msg"]
    assert ticker["yes_bid_dollars"] == "0.4700"

    # Control traffic: outbound commands AND subscribe acks all captured.
    control = _records(config.raw_dir, "ws_control")
    outbound = [r for r in control if r["params"].get("direction") == "out"]
    assert [r["payload"]["cmd"] for r in outbound] == ["subscribe"] * 3
    acks = [r for r in control if r["params"].get("type") == "subscribed"]
    assert len(acks) == 3
    metas = [r["payload"]["event"] for r in control if r["params"].get("direction") == "meta"]
    assert "connected" in metas


async def test_auth_headers_signed_for_handshake(tmp_path):
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding, rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem_path = tmp_path / "test-key.pem"
    pem_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )

    async with FakeKalshiWs() as server:
        config = _config(
            tmp_path, server.url,
            api_key_id="test-key-id", private_key_path=pem_path,
        )
        lake, capture, task = await _run_capture(config)
        await _wait_until(lambda: len(server.headers) >= 1)
        await _stop_capture(capture, task, lake)

    headers = {k.upper(): v for k, v in server.headers[0].items()}
    assert headers["KALSHI-ACCESS-KEY"] == "test-key-id"
    ts = headers["KALSHI-ACCESS-TIMESTAMP"]
    assert abs(time.time() * 1000 - int(ts)) < 60_000  # unix MILLISECONDS
    # RSA-PSS/SHA-256 over timestamp + GET + WS path (no query params).
    key.public_key().verify(
        base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
        f"{ts}GET/trade-api/ws/v2".encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )


async def test_reconnect_and_full_resubscribe_after_drop(tmp_path):
    async with FakeKalshiWs(drop_after_acks={0: 3}, send_data_after_acks=3) as server:
        config = _config(tmp_path, server.url)
        lake, capture, task = await _run_capture(config)
        await _wait_until(
            lambda: server.connections >= 2
            and sum(1 for c in server.commands[-1] if c.get("cmd") == "subscribe") >= 3
        )
        await _stop_capture(capture, task, lake)

    # Both connections received the full subscribe set (full resubscribe).
    for conn_cmds in server.commands[:2]:
        subs = [c for c in conn_cmds if c.get("cmd") == "subscribe"]
        assert [c["params"]["channels"] for c in subs] == [[ch] for ch in PUBLIC_CHANNELS]
    # The drop is visible in the lake: meta records tell the gap story.
    metas = [
        r["payload"]["event"]
        for r in _records(config.raw_dir, "ws_control")
        if r["params"].get("direction") == "meta"
    ]
    assert metas.count("connected") >= 2
    assert "disconnected" in metas


async def test_error_26_halves_chunks_until_accepted(tmp_path):
    config_dir = tmp_path / "data"
    store = HotStore(config_dir / "db" / "hot.sqlite")
    store.upsert_markets(
        "kalshi",
        [
            {
                "venue_market_id": f"KXTST-26JUL11-T{i:02d}",
                "status_norm": "open",
                "volume_24h": "5",
                "open_interest": "3",
            }
            for i in range(10)
        ],
        now_ts=1_770_000_000,
    )
    store.close()

    async with FakeKalshiWs(market_limit=2) as server:
        config = _config(
            tmp_path, server.url,
            firehose=False, chunk_size=8, channels=("ticker",),
        )
        lake, capture, task = await _run_capture(config)
        await _wait_until(lambda: len(server.all_subscribed) == 10)
        await _stop_capture(capture, task, lake)

    # 8 → 26-reject → 4 → 26-reject → 2 → five accepted chunks of 2.
    assert server.rejects_26 == 2
    assert capture._chunk_size == 2  # discovered limit persists for resubscribes
    assert all(len(t) <= 2 for _, t in server.acked if t)
    assert server.all_subscribed == {f"KXTST-26JUL11-T{i:02d}" for i in range(10)}
    # Universe came from the hot store read-only path; nothing was written.
    assert (config_dir / "db" / "hot.sqlite").exists()
    # Error frames were captured, not discarded (storage invariants).
    errors = [
        r for r in _records(config.raw_dir, "ws_control")
        if r["params"].get("type") == "error"
    ]
    assert len(errors) == 2
    assert all(r["payload"]["msg"]["code"] == 26 for r in errors)


async def test_ws_lock_is_single_instance(tmp_path):
    lock = _acquire_ws_lock(tmp_path)
    assert (tmp_path / "ws.lock").exists()
    with pytest.raises(SystemExit):
        _acquire_ws_lock(tmp_path)
    lock.close()
    # Released lock can be re-acquired.
    lock2 = _acquire_ws_lock(tmp_path)
    lock2.close()


def test_default_channels_keep_trade_capture_on(tmp_path, monkeypatch):
    monkeypatch.delenv("PMKT_WS_CHANNELS", raising=False)
    monkeypatch.delenv("KALSHI_API_KEY_ID", raising=False)
    # The disk guard ported without the capture-cap branch's lifecycle-only
    # default: trade capture stays on unless PMKT_WS_CHANNELS narrows it.
    assert "trade" in PUBLIC_CHANNELS
    assert WsConfig.from_env().channels == PUBLIC_CHANNELS
    assert WsConfig(data_dir=tmp_path).channels == PUBLIC_CHANNELS
    monkeypatch.setenv("PMKT_WS_CHANNELS", "trade,market_lifecycle_v2")
    assert WsConfig.from_env().channels == ("trade", "market_lifecycle_v2")
    monkeypatch.setenv("PMKT_WS_CHANNELS", "")
    with pytest.raises(ValueError):
        WsConfig.from_env()


async def test_run_block_flushes_only_beside_a_current_collector(tmp_path, monkeypatch):
    """Deploy skew: a collector started before rawlake.decompress_raw_file
    compacts an unterminated FLUSH_BLOCK crash orphan short, for good. Such a
    collector never holds the block-reader lock, so a WS writer started
    beside it (a KeepAlive relaunch onto new code is enough) must write
    per-record frames, which that collector's decoder reads exactly."""
    import zstandard
    from pmkt.capture_guard import CaptureGuard
    from pmkt.kalshi import ws as ws_mod
    from pmkt.rawlake import hold_block_reader_lock

    seen = []

    async def no_network(self):
        seen.append(self.lake)

    monkeypatch.setattr(ws_mod.WsCapture, "run", no_network)
    config = _config(tmp_path, "ws://127.0.0.1:9/unused")
    await ws_mod.run(config)  # no collector — or one that predates the lock
    held = hold_block_reader_lock(config.data_dir)  # a current collector's loop
    try:
        await ws_mod.run(config)
    finally:
        held.close()
    assert [lake.flush_mode for lake in seen] == [zstandard.FLUSH_FRAME, zstandard.FLUSH_BLOCK]
    assert all(isinstance(lake.capture_guard, CaptureGuard) for lake in seen)


async def test_flush_mode_follows_the_collector_at_each_stats_tick(tmp_path):
    import contextlib

    import zstandard
    from pmkt.rawlake import hold_block_reader_lock

    config = _config(tmp_path, "ws://127.0.0.1:9/unused", stats_interval_s=0.02)
    held = hold_block_reader_lock(config.data_dir)
    lake = RawLake(config.raw_dir, flush_mode=zstandard.FLUSH_BLOCK)
    capture = WsCapture(config, lake)
    stats = asyncio.create_task(capture._stats_loop())
    try:
        capture._capture("ws_trade", {"direction": "in"}, SAMPLE_FRAMES[1])
        (open_block,) = config.raw_dir.rglob("*.jsonl.zst.tmp")
        held.close()  # the collector stopped, or was replaced by a pre-lock one
        await _wait_until(lambda: lake.flush_mode == zstandard.FLUSH_FRAME)
        # The open block-flushed file was finalized with its frame ended, not
        # left open for a crash to orphan under an unknown decoder.
        finalized = open_block.with_name(open_block.name[: -len(".tmp")])
        assert not open_block.exists() and finalized.exists()
        dobj = zstandard.ZstdDecompressor().decompressobj()
        dobj.decompress(finalized.read_bytes())
        assert dobj.eof and not dobj.unused_data
        held = hold_block_reader_lock(config.data_dir)  # a current collector is back
        await _wait_until(lambda: lake.flush_mode == zstandard.FLUSH_BLOCK)
    finally:
        stats.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stats
        held.close()
        lake.close()


async def test_pause_during_subscribe_is_survived_and_resubscribes(tmp_path, monkeypatch):
    """Space runs out after connect but before the first subscribe: the
    subscribe's own capture is refused (the subscriber closes the socket 1011)
    and so is the 'disconnected' meta record written on the way out.
    _capture_meta must swallow that pause — otherwise run(), i.e. the WS
    processes, crashes into a launchd restart loop for as long as disk is low."""
    from types import SimpleNamespace

    from pmkt.capture_guard import CaptureGuard, CapturePolicy

    # Probes in call order: ready check, 'connected' meta, subscribe command
    # (paused), 'disconnected' meta (paused), a paused ready check — then
    # the space is back for good.
    script = iter([100, 100, 0, 0, 0])
    monkeypatch.setattr(
        "pmkt.capture_guard.shutil.disk_usage",
        lambda _: SimpleNamespace(free=next(script, 100)),
    )
    guard = CaptureGuard(CapturePolicy(disk_path=tmp_path, min_free_bytes=100, recheck_seconds=0.01))
    async with FakeKalshiWs() as server:
        config = _config(tmp_path, server.url, channels=("market_lifecycle_v2",))
        lake = RawLake(config.raw_dir, capture_guard=guard)
        capture = WsCapture(config, lake)
        task = asyncio.create_task(capture.run())
        try:
            await _wait_until(lambda: task.done() or len(server.acked) == 1)
            assert not task.done(), task.exception()
            assert server.connections == 2
        finally:
            await _stop_capture(capture, task, lake)
    control = _records(config.raw_dir, "ws_control")
    metas = [r["payload"]["event"] for r in control if r["params"].get("direction") == "meta"]
    assert metas[:2] == ["connected", "connected"]  # the refused records left no trace
    outbound = [r for r in control if r["params"].get("direction") == "out"]
    assert len(outbound) == 1  # only the subscribe that went out after recovery


async def test_disk_pause_closes_ws_and_resubscribes_after_recovery(tmp_path, monkeypatch):
    from types import SimpleNamespace

    import zstandard
    from pmkt.capture_guard import CaptureGuard, CapturePolicy

    free = [0]
    monkeypatch.setattr("pmkt.capture_guard.shutil.disk_usage", lambda _: SimpleNamespace(free=free[0]))
    guard = CaptureGuard(CapturePolicy(disk_path=tmp_path, min_free_bytes=100, recheck_seconds=0.01))
    async with FakeKalshiWs() as server:
        # A disk pause is not a connection failure: recovery must reconnect
        # at once, never through a reconnect backoff (15-45 s here).
        config = _config(tmp_path, server.url, channels=("market_lifecycle_v2",),
                         reconnect_base_s=30.0, reconnect_cap_s=60.0)
        # Both RawLake options together, exactly as ws.run() builds the lake.
        lake = RawLake(config.raw_dir, capture_guard=guard, flush_mode=zstandard.FLUSH_BLOCK)
        capture = WsCapture(config, lake)
        task = asyncio.create_task(capture.run())
        try:
            await _wait_until(lambda: guard.paused)
            assert server.connections == 0
            free[0] = 100
            await _wait_until(lambda: len(server.acked) == 1)
            free[0] = 0
            for conn in list(server._server.connections):
                await conn.send(json.dumps(SAMPLE_FRAMES[2]))
            await _wait_until(lambda: guard.paused and not server._server.connections)
            assert server.connections == 1
            assert capture.counts["ws_market_lifecycle_v2"] == 0
            free[0] = 100
            await _wait_until(lambda: len(server.acked) == 2)
            for conn in list(server._server.connections):
                await conn.send(json.dumps(SAMPLE_FRAMES[2]))
            await _wait_until(lambda: capture.counts["ws_market_lifecycle_v2"] == 1)
            assert capture.counts["ws_trade"] == 0
            assert all(channel == "market_lifecycle_v2" for channel, _ in server.acked)
        finally:
            await _stop_capture(capture, task, lake)
    # Only the post-recovery frame was captured, and the block-flushed part
    # file reads back verbatim.
    recs = _records(config.raw_dir, "ws_market_lifecycle_v2")
    assert [r["payload"]["msg"] for r in recs] == [SAMPLE_FRAMES[2]["msg"]]
