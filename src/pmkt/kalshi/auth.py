"""Kalshi Trade API v2 request signing.

RSA-PSS + SHA-256 (NOT PKCS1v15) over `timestamp + METHOD + path`, where the
path INCLUDES the /trade-api/v2 prefix and EXCLUDES query params, and the
timestamp is unix MILLISECONDS (data invariants — Kalshi auth is ms while its
data params are seconds). The private key lives in a local file, never in
the repo or logs (data invariants).
"""

from __future__ import annotations

import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


class KalshiSigner:
    def __init__(self, key_id: str, private_key_path: Path | str) -> None:
        self.key_id = key_id
        self._key = serialization.load_pem_private_key(
            Path(private_key_path).read_bytes(), password=None
        )

    def headers(self, method: str, signing_path: str) -> dict[str, str]:
        ts_ms = str(int(time.time() * 1000))
        message = f"{ts_ms}{method.upper()}{signing_path}".encode()
        signature = self._key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        }
