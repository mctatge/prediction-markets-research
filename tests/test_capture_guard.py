"""Synthetic disk probes and mocked APIs only; no production data or credentials."""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import respx

from pmkt.capture_guard import CaptureGuard, CapturePaused, CapturePolicy
from pmkt.kalshi.client import KalshiPublicClient
from pmkt.polymarket.client import PolymarketClient
from pmkt.ratelimit import TokenBucket
from pmkt.rawlake import RawLake, read_raw_file


async def wait_until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.005)


def test_threshold_logs_once_and_resumes_at_exact_minimum(monkeypatch, caplog, tmp_path):
    policy = CapturePolicy(disk_path=tmp_path)
    free = [59_999_999_999]
    paths = []

    def disk_usage(path):
        paths.append(path)
        return SimpleNamespace(free=free[0])

    monkeypatch.setattr('pmkt.capture_guard.shutil.disk_usage', disk_usage)
    guard = CaptureGuard(policy)
    caplog.set_level(logging.INFO)
    for _ in range(3):
        with pytest.raises(CapturePaused):
            guard.check()
    assert caplog.text.count('capture paused') == 1
    assert all(path == tmp_path for path in paths)
    free[0] = 60_000_000_000
    guard.check()
    assert not guard.paused
    assert caplog.text.count('capture resumed') == 1


def test_failed_probe_pauses_without_creating_raw_files(tmp_path, monkeypatch):
    def fail(_):
        raise OSError('synthetic probe failure')

    monkeypatch.setattr('pmkt.capture_guard.shutil.disk_usage', fail)
    guard = CaptureGuard(CapturePolicy(disk_path=tmp_path))
    with RawLake(tmp_path / 'raw', capture_guard=guard) as lake:
        with pytest.raises(CapturePaused):
            lake.append(venue='test', endpoint='test', params={}, http_status=200,
                        elapsed_ms=0, payload={'synthetic': True})
    assert not (tmp_path / 'raw').exists()


async def test_pause_is_stoppable_without_waiting_recheck_period(tmp_path, monkeypatch):
    monkeypatch.setattr('pmkt.capture_guard.shutil.disk_usage', lambda _: SimpleNamespace(free=0))
    guard = CaptureGuard(CapturePolicy(disk_path=tmp_path, recheck_seconds=3600))
    stop = asyncio.Event()
    task = asyncio.create_task(guard.wait_until_ready(stop))
    await wait_until(lambda: guard.paused)
    stop.set()
    assert await asyncio.wait_for(task, 0.5) is False


@pytest.mark.parametrize('venue', ['kalshi', 'polymarket'])
async def test_rest_pauses_before_request_and_holds_inflight_response(tmp_path, monkeypatch, venue):
    free = [0]
    monkeypatch.setattr('pmkt.capture_guard.shutil.disk_usage', lambda _: SimpleNamespace(free=free[0]))
    guard = CaptureGuard(CapturePolicy(disk_path=tmp_path, min_free_bytes=100, recheck_seconds=0.01))
    lake = RawLake(tmp_path / 'raw', capture_guard=guard)
    bucket = TokenBucket(1000, 1000)
    client = (KalshiPublicClient('https://synthetic.invalid', lake, bucket)
              if venue == 'kalshi' else PolymarketClient(lake, bucket))
    payload = {'preserve': 'synthetic in-flight response'}

    def response(_):
        # Another process consumes the remaining space while HTTP is in flight.
        free[0] = 0
        return httpx.Response(200, json=payload)

    try:
        with respx.mock(assert_all_called=False) as mock:
            route = mock.get('https://synthetic.invalid/test').mock(side_effect=response)
            request = (client._get('test', '/test') if venue == 'kalshi'
                       else client._request('test', 'GET', 'https://synthetic.invalid/test'))
            task = asyncio.create_task(request)
            await wait_until(lambda: guard.paused)
            assert route.call_count == 0
            free[0] = 100
            await wait_until(lambda: route.call_count == 1)
            await wait_until(lambda: guard.paused)
            assert not task.done()
            assert not (tmp_path / 'raw').exists()
            free[0] = 100
            assert await asyncio.wait_for(task, 2) == payload
            assert route.call_count == 1  # held response; no new request after resume
    finally:
        await client.aclose()
        lake.close()
    parts = list((tmp_path / 'raw').rglob('*.jsonl.zst'))
    assert len(parts) == 1
    assert read_raw_file(parts[0])[0]['payload'] == payload


@pytest.mark.parametrize('venue', ['kalshi', 'polymarket'])
async def test_space_lost_between_probe_and_append_keeps_the_fetched_response(tmp_path, monkeypatch, venue):
    """The ready probe passes, then append()'s own check fails (the WS writer
    took the last headroom in between). The response must still be captured
    exactly once, stamped with its original fetch time, with no re-request."""
    from pmkt import rawlake

    clock = [datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)]

    class Clock(datetime):  # the lake's only time source
        @classmethod
        def now(cls, tz=None):
            return clock[0]

    # Probes in call order: before the request, append_when_ready's wait,
    # append()'s own check (space just vanished), the retry's wait, its
    # append. Each probe moves the clock a minute on.
    script = iter([100, 100, 0, 100, 100])

    def disk_usage(_):
        clock[0] += timedelta(minutes=1)
        return SimpleNamespace(free=next(script, 100))

    monkeypatch.setattr(rawlake, 'datetime', Clock)
    monkeypatch.setattr('pmkt.capture_guard.shutil.disk_usage', disk_usage)
    guard = CaptureGuard(CapturePolicy(disk_path=tmp_path, min_free_bytes=100, recheck_seconds=0.01))
    lake = RawLake(tmp_path / 'raw', capture_guard=guard)
    bucket = TokenBucket(1000, 1000)
    client = (KalshiPublicClient('https://synthetic.invalid', lake, bucket)
              if venue == 'kalshi' else PolymarketClient(lake, bucket))
    payload = {'preserve': 'synthetic response fetched before the space ran out'}
    fetched = []

    def response(_):
        fetched.append(clock[0])
        return httpx.Response(200, json=payload)

    try:
        with respx.mock(assert_all_called=False) as mock:
            route = mock.get('https://synthetic.invalid/test').mock(side_effect=response)
            request = (client._get('test', '/test') if venue == 'kalshi'
                       else client._request('test', 'GET', 'https://synthetic.invalid/test'))
            assert await asyncio.wait_for(request, 2) == payload
            assert route.call_count == 1  # held and retried, never re-requested
    finally:
        await client.aclose()
        lake.close()
    (part,) = (tmp_path / 'raw').rglob('*.jsonl.zst')
    (record,) = read_raw_file(part)
    assert record['payload'] == payload
    assert record['fetched_at'] == fetched[0].isoformat()  # not the retry's later clock
    assert clock[0] > fetched[0] + timedelta(minutes=2)


@pytest.mark.parametrize('value', ['0', '-1', 'nan', 'inf'])
def test_invalid_guard_configuration_fails_closed(monkeypatch, value):
    monkeypatch.setenv('PMKT_CAPTURE_MIN_FREE_GB', value)
    with pytest.raises(ValueError):
        CapturePolicy.from_env()


@pytest.mark.parametrize('value', ['0', '-1', 'nan', 'inf'])
def test_invalid_recheck_interval_fails_closed(monkeypatch, value):
    monkeypatch.setenv('PMKT_CAPTURE_RECHECK_SECONDS', value)
    with pytest.raises(ValueError):
        CapturePolicy.from_env()


CAPTURE_ENV = ('PMKT_CAPTURE_MIN_FREE_GB', 'PMKT_CAPTURE_DISK_PATH', 'PMKT_CAPTURE_RECHECK_SECONDS')


def test_env_fallbacks_are_the_live_policy(monkeypatch, tmp_path):
    """Neither launchd plist sets PMKT_CAPTURE_*, so from_env's fallbacks —
    not the dataclass defaults — are what both processes actually run with."""
    from pmkt.config import Config
    from pmkt.kalshi.ws import WsConfig

    for name in CAPTURE_ENV:
        monkeypatch.delenv(name, raising=False)
    policy = CapturePolicy.from_env()
    assert policy.disk_path == Path(os.environ.get('PMKT_DATA_DIR', 'data'))
    assert policy.min_free_bytes == 60_000_000_000
    assert policy.recheck_seconds == 30.0
    assert policy == CapturePolicy()
    # The two processes' guards are built from exactly this policy.
    assert Config().capture_policy == policy
    assert WsConfig(data_dir=tmp_path).capture_policy == policy


def test_env_overrides_are_applied(monkeypatch, tmp_path):
    monkeypatch.setenv('PMKT_CAPTURE_MIN_FREE_GB', '12.5')
    monkeypatch.setenv('PMKT_CAPTURE_DISK_PATH', str(tmp_path))
    monkeypatch.setenv('PMKT_CAPTURE_RECHECK_SECONDS', '5')
    assert CapturePolicy.from_env() == CapturePolicy(
        disk_path=tmp_path, min_free_bytes=12_500_000_000, recheck_seconds=5.0,
    )


def test_new_data_directory_probes_nearest_existing_ancestor(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr('pmkt.capture_guard.shutil.disk_usage',
                        lambda path: seen.append(path) or SimpleNamespace(free=100_000_000_000))
    requested = tmp_path / 'new-volume' / 'data'
    CaptureGuard(CapturePolicy(disk_path=requested)).check()
    assert seen == [tmp_path]
    assert not requested.exists()
