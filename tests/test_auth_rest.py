import base64

import httpx
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

from asl.kalshi.auth import auth_headers, load_private_key, sign
from asl.kalshi.rest import KalshiREST


def test_signature_verifies_and_ignores_query(rsa_key_path):
    key = load_private_key(rsa_key_path)
    sig = sign(key, "1700000000000", "get", "/trade-api/v2/markets?limit=5")
    key.public_key().verify(
        base64.b64decode(sig),
        b"1700000000000GET/trade-api/v2/markets",
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )  # raises if invalid


def test_auth_headers_shape(rsa_key_path):
    h = auth_headers(load_private_key(rsa_key_path), "kid", "GET", "/trade-api/ws/v2")
    assert set(h) == {"KALSHI-ACCESS-KEY", "KALSHI-ACCESS-SIGNATURE", "KALSHI-ACCESS-TIMESTAMP"}
    assert h["KALSHI-ACCESS-KEY"] == "kid" and len(h["KALSHI-ACCESS-TIMESTAMP"]) == 13


def test_rest_paginates_signs_and_retries(rsa_key_path):
    calls = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        if len(calls) == 1:
            return httpx.Response(429)
        cursor = req.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(200, json={"markets": [{"ticker": "A"}], "cursor": "c1"})
        return httpx.Response(200, json={"markets": [{"ticker": "B"}], "cursor": ""})

    rest = KalshiREST(
        "https://x.test/trade-api/v2",
        "kid",
        str(rsa_key_path),
        min_interval_s=0,
        transport=httpx.MockTransport(handler),
    )
    import asl.kalshi.rest as mod

    orig = mod.time.sleep
    mod.time.sleep = lambda s: None
    try:
        tickers = [m["ticker"] for m in rest.iter_markets(series_ticker="KXBTC")]
    finally:
        mod.time.sleep = orig
    assert tickers == ["A", "B"]
    assert len(calls) == 3  # 429, page1, page2
    assert calls[-1].headers["KALSHI-ACCESS-KEY"] == "kid"
    assert calls[-1].url.params["series_ticker"] == "KXBTC"


def test_rest_has_no_write_methods():
    # guardrail: this project must never be able to place orders
    for name in ("post", "put", "delete", "create_order", "place_order"):
        assert not hasattr(KalshiREST, name)
