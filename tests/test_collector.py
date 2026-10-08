"""Sliced trades-sweep semantics (the 2026-07-11 starvation incident fix),
plus the collector loop's wiring (run lock, disk-space pause, block-reader)."""

import asyncio
import contextlib
from types import SimpleNamespace

import pytest

from pmkt.kalshi import collector
from pmkt.state import HotStore

FIXED_NOW = 1_784_000_000


class FakeTradesClient:
    """Yields a fixed number of pages for every iter_trades window."""

    def __init__(self, pages_per_slice: int):
        self.pages_per_slice = pages_per_slice
        self.windows: list[tuple[int, int]] = []

    async def iter_trades(self, min_ts=None, max_ts=None, cursor=None):
        self.windows.append((min_ts, max_ts))
        for i in range(self.pages_per_slice):
            last = i == self.pages_per_slice - 1
            yield [], ("" if last else "cursor")


def make_store(tmp_path):
    return HotStore(tmp_path / "hot.sqlite")


async def test_trades_catchup_advances_watermark_per_slice(tmp_path, monkeypatch):
    monkeypatch.setattr(collector.time, "time", lambda: FIXED_NOW)
    store = make_store(tmp_path)
    behind = 3 * collector.TRADES_SLICE_S + 100
    store.set_state("kalshi", "trades", watermark_ts=FIXED_NOW - behind, now_ts=FIXED_NOW)

    client = FakeTradesClient(pages_per_slice=2)
    result = await collector.collect_trades(client, store)

    assert result["caught_up"] == 1
    assert len(client.windows) == 4  # 3 full slices + the remainder
    # Every window is bounded and overlap-padded.
    for min_ts, max_ts in client.windows:
        assert max_ts - min_ts <= collector.TRADES_SLICE_S + collector.TRADES_OVERLAP_S
    assert store.get_state("kalshi", "trades")["watermark_ts"] == FIXED_NOW
    store.close()


async def test_trades_page_budget_stops_sweep_without_losing_a_slice(tmp_path, monkeypatch):
    monkeypatch.setattr(collector.time, "time", lambda: FIXED_NOW)
    monkeypatch.setattr(collector, "TRADES_PAGE_BUDGET", 5)
    store = make_store(tmp_path)
    start = FIXED_NOW - 10 * collector.TRADES_SLICE_S
    store.set_state("kalshi", "trades", watermark_ts=start, now_ts=FIXED_NOW)

    client = FakeTradesClient(pages_per_slice=3)
    result = await collector.collect_trades(client, store)

    # Slice 1 completed (3 pages); slice 2 hit the budget mid-slice (pages 4-5)
    # and must NOT advance — the newest-first tape leaves its tail unfetched.
    assert result["caught_up"] == 0
    assert result["pages"] == 5
    assert store.get_state("kalshi", "trades")["watermark_ts"] == start + collector.TRADES_SLICE_S
    store.close()


async def test_trades_first_run_uses_bounded_lookback(tmp_path, monkeypatch):
    monkeypatch.setattr(collector.time, "time", lambda: FIXED_NOW)
    store = make_store(tmp_path)

    client = FakeTradesClient(pages_per_slice=1)
    result = await collector.collect_trades(client, store)

    assert result["caught_up"] == 1
    assert client.windows[0][1] - client.windows[0][0] <= (
        collector.TRADES_SLICE_S + collector.TRADES_OVERLAP_S
    )
    assert min(w[0] for w in client.windows) >= (
        FIXED_NOW - collector.TRADES_FIRST_RUN_LOOKBACK_S - collector.TRADES_OVERLAP_S
    )
    assert store.get_state("kalshi", "trades")["watermark_ts"] == FIXED_NOW
    store.close()


def test_run_lock_is_exclusive_per_data_dir(tmp_path):
    from pmkt.cli import _acquire_run_lock
    from pmkt.config import Config

    cfg = Config(data_dir=tmp_path)
    lock = _acquire_run_lock(cfg)
    with pytest.raises(SystemExit):
        _acquire_run_lock(cfg)  # second loop on same data dir must refuse
    lock.close()
    _acquire_run_lock(cfg).close()  # released after close


async def _wait_until(predicate, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


async def test_build_gives_the_collector_lake_the_capture_guard(tmp_path):
    from pmkt.capture_guard import CaptureGuard, CapturePolicy
    from pmkt.cli import _build
    from pmkt.config import Config

    policy = CapturePolicy(disk_path=tmp_path, min_free_bytes=123)
    # No credentials: the test must never read a real key file.
    lake, store, kalshi_client, pm_client = _build(
        Config(data_dir=tmp_path, capture_policy=policy, kalshi_api_key_id="")
    )
    try:
        assert isinstance(lake.capture_guard, CaptureGuard)
        assert lake.capture_guard.policy is policy
        # Both REST clients capture through that one guarded lake.
        assert kalshi_client._lake is lake and pm_client._lake is lake
    finally:
        await kalshi_client.aclose()
        await pm_client.aclose()
        lake.close()
        store.close()


async def test_run_loop_starts_no_due_job_while_capture_is_paused(tmp_path, monkeypatch):
    """Jobs that bypass the REST clients' own pause (compaction's large
    staging files, local builders) must not launch while disk space is low."""
    from pmkt.capture_guard import CaptureGuard, CapturePolicy
    from pmkt.cli import _drive_schedule
    from pmkt.rawlake import RawLake

    free = [0]
    monkeypatch.setattr(
        "pmkt.capture_guard.shutil.disk_usage", lambda _: SimpleNamespace(free=free[0])
    )
    guard = CaptureGuard(CapturePolicy(disk_path=tmp_path, min_free_bytes=100, recheck_seconds=0.01))
    lake = RawLake(tmp_path / "raw", capture_guard=guard)
    ran = []

    async def job(client, store):
        ran.append(client)

    loop = asyncio.create_task(_drive_schedule([[job, "client", 3600.0, 0.0]], lake, None))
    try:
        await _wait_until(lambda: guard.paused)
        await asyncio.sleep(0.05)  # several paused rechecks
        assert ran == []  # due all along, yet not started
        free[0] = 100
        await _wait_until(lambda: ran)
        assert ran == ["client"]
    finally:
        loop.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await loop
        lake.close()


async def test_run_loop_holds_the_block_reader_lock_for_its_lifetime(tmp_path, monkeypatch):
    """The WS processes block-flushes only while this lock is held (ws_flush_mode):
    only a collector whose compaction decodes FLUSH_BLOCK orphans exactly —
    this one — may hold it, and only while it runs."""
    from pmkt import cli
    from pmkt.config import Config
    from pmkt.rawlake import block_reader_live

    seen = {}

    async def drive(schedule, lake, store):
        seen["live"] = block_reader_live(tmp_path)
        seen["jobs"] = [entry[0].__name__ for entry in schedule]

    monkeypatch.setattr(cli, "_drive_schedule", drive)
    assert not block_reader_live(tmp_path)
    await cli._run_loop(Config(data_dir=tmp_path, kalshi_api_key_id=""))
    assert seen["live"] and "compaction_job" in seen["jobs"]
    assert not block_reader_live(tmp_path)  # released with the loop
