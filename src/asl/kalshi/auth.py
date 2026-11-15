"""Kalshi API-key request signing (RSA-PSS / SHA-256, base64).

Signed message = timestamp_ms + METHOD + path, where path is the full URL path
(e.g. "/trade-api/v2/markets" or "/trade-api/ws/v2"), without the query string.
"""

from __future__ import annotations

import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def load_private_key(path: str | Path) -> rsa.RSAPrivateKey:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"kalshi private key not found: {p}")
    key = serialization.load_pem_private_key(p.read_bytes(), password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise TypeError("kalshi key must be an RSA private key")
    return key


def sign(key: rsa.RSAPrivateKey, timestamp_ms: str, method: str, path: str) -> str:
    msg = f"{timestamp_ms}{method.upper()}{path.split('?', 1)[0]}".encode()
    sig = key.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("ascii")


def auth_headers(key: rsa.RSAPrivateKey, key_id: str, method: str, path: str) -> dict[str, str]:
    ts = str(int(time.time() * 1000))
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-SIGNATURE": sign(key, ts, method, path),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }
