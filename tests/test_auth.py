import base64

import httpx
import respx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from pmkt.kalshi.auth import KalshiSigner
from pmkt.kalshi.client import KalshiPublicClient
from pmkt.ratelimit import TokenBucket
from pmkt.rawlake import RawLake

BASE = "https://external-api.kalshi.com/trade-api/v2"


def make_key(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem_path = tmp_path / "key.pem"
    pem_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return key.public_key(), pem_path


def verify(public_key, headers, method, signing_path):
    message = (headers["KALSHI-ACCESS-TIMESTAMP"] + method + signing_path).encode()
    public_key.verify(  # raises on mismatch
        base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )


def test_signer_produces_verifiable_rsa_pss_headers(tmp_path):
    public_key, pem_path = make_key(tmp_path)
    signer = KalshiSigner("my-key-id", pem_path)
    headers = signer.headers("GET", "/trade-api/v2/markets")

    assert headers["KALSHI-ACCESS-KEY"] == "my-key-id"
    ts = headers["KALSHI-ACCESS-TIMESTAMP"]
    assert ts.isdigit() and len(ts) == 13, "unix MILLISECONDS, not seconds"
    verify(public_key, headers, "GET", "/trade-api/v2/markets")


@respx.mock
async def test_client_signs_requests_with_prefixed_path_no_query(tmp_path):
    public_key, pem_path = make_key(tmp_path)
    route = respx.get(f"{BASE}/markets").mock(
        return_value=httpx.Response(200, json={"markets": [], "cursor": ""})
    )
    lake = RawLake(tmp_path / "raw")
    client = KalshiPublicClient(
        BASE, lake, TokenBucket(100, 100),
        signer=KalshiSigner("kid", pem_path),
    )
    async with client:
        async for _m, _c in client.iter_markets(status="open"):
            pass
    lake.close()

    sent = route.calls[0].request.headers
    assert sent["KALSHI-ACCESS-KEY"] == "kid"
    # Signature covers /trade-api/v2 + path, EXCLUDING query params.
    verify(
        public_key,
        {k.upper(): v for k, v in sent.items() if k.upper().startswith("KALSHI")},
        "GET",
        "/trade-api/v2/markets",
    )


@respx.mock
async def test_unsigned_when_no_signer(tmp_path):
    route = respx.get(f"{BASE}/series").mock(
        return_value=httpx.Response(200, json={"series": []})
    )
    lake = RawLake(tmp_path / "raw")
    client = KalshiPublicClient(BASE, lake, TokenBucket(100, 100))
    async with client:
        await client.get_series_catalog()
    lake.close()
    assert "KALSHI-ACCESS-KEY" not in route.calls[0].request.headers
