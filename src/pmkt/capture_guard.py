"""Pause network capture before the Data volume runs out of headroom.

No retention/deletion policy: existing raw data is never changed. A failed
filesystem probe fails closed and is retried just like a low-space result.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CapturePolicy:
    disk_path: Path = field(default_factory=lambda: Path(os.environ.get("PMKT_DATA_DIR", "data")))
    min_free_bytes: int = 60_000_000_000  # 60 decimal GB, matching macOS disk UI
    recheck_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.min_free_bytes <= 0:
            raise ValueError("capture minimum free bytes must be positive")
        if not math.isfinite(self.recheck_seconds) or self.recheck_seconds <= 0:
            raise ValueError("capture recheck interval must be finite and positive")

    @classmethod
    def from_env(cls) -> "CapturePolicy":
        gb = float(os.environ.get("PMKT_CAPTURE_MIN_FREE_GB", "60"))
        if not math.isfinite(gb) or gb <= 0:
            raise ValueError("PMKT_CAPTURE_MIN_FREE_GB must be finite and positive")
        return cls(
            disk_path=Path(os.environ.get("PMKT_CAPTURE_DISK_PATH", os.environ.get("PMKT_DATA_DIR", "data"))),
            min_free_bytes=int(gb * 1_000_000_000),
            recheck_seconds=float(os.environ.get("PMKT_CAPTURE_RECHECK_SECONDS", "30")),
        )


class CapturePaused(RuntimeError):
    """No capture may be written until the filesystem guard recovers."""


class CaptureGuard:
    def __init__(self, policy: CapturePolicy) -> None:
        self.policy = policy
        self.paused = False

    def check(self) -> None:
        reason = None
        try:
            probe_path = self.policy.disk_path.expanduser().absolute()
            while not probe_path.exists() and probe_path != probe_path.parent:
                probe_path = probe_path.parent
            free = shutil.disk_usage(probe_path).free
        except OSError:
            reason = "free-space probe failed"
        else:
            if free < self.policy.min_free_bytes:
                reason = f"free={free} bytes below minimum={self.policy.min_free_bytes} bytes"
        if reason is not None:
            if not self.paused:
                logger.warning("capture paused on %s: %s", self.policy.disk_path, reason)
            self.paused = True
            raise CapturePaused(reason)
        if self.paused:
            logger.info("capture resumed on %s: free=%d bytes", self.policy.disk_path, free)
        self.paused = False

    async def wait_until_ready(self, stop: asyncio.Event | None = None) -> bool:
        """Wait without polling APIs; cancellation/WS stop remains responsive."""
        while stop is None or not stop.is_set():
            try:
                self.check()
            except CapturePaused:
                if stop is None:
                    await asyncio.sleep(self.policy.recheck_seconds)
                else:
                    try:
                        await asyncio.wait_for(stop.wait(), self.policy.recheck_seconds)
                    except asyncio.TimeoutError:
                        pass
            else:
                return True
        return False
