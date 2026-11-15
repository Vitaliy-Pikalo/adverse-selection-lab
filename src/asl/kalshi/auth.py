"""Kalshi API-key request signing.

Signed message = timestamp_ms + METHOD + path, where path is the full URL path
(e.g. "/trade-api/v2/markets" or "/trade-api/ws/v2"), without the query string.

Kalshi supports two key types:
  - Ed25519 (the default in Kalshi's key dialog): Ed25519 signature over the message
  - RSA 2048: RSA-PSS, SHA-256, MGF1-SHA-256, salt length = digest length
The signature is base64-encoded in both cases.
"""

from __future__ import annotations

import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa

PrivateKey = rsa.RSAPrivateKey | ed25519.Ed25519PrivateKey


def load_private_key(path: str | Path) -> PrivateKey:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"kalshi private key not found: {p}")
    key = serialization.load_pem_private_key(p.read_bytes().strip() + b"\n", password=None)
    if not isinstance(key, rsa.RSAPrivateKey | ed25519.Ed25519PrivateKey):
        raise TypeError(f"unsupported kalshi key type: {type(key).__name__} (need RSA or Ed25519)")
    return key


def key_type(key: PrivateKey) -> str:
    return "ed25519" if isinstance(key, ed25519.Ed25519PrivateKey) else "rsa"


def sign(key: PrivateKey, timestamp_ms: str, method: str, path: str) -> str:
    msg = f"{timestamp_ms}{method.upper()}{path.split('?', 1)[0]}".encode()
    if isinstance(key, ed25519.Ed25519PrivateKey):
        sig = key.sign(msg)
    else:
        sig = key.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
    return base64.b64encode(sig).decode("ascii")


def auth_headers(key: PrivateKey, key_id: str, method: str, path: str) -> dict[str, str]:
    ts = str(int(time.time() * 1000))
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-SIGNATURE": sign(key, ts, method, path),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }
