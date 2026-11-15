"""Minimal read-only Kalshi REST client (market data only — no order endpoints)."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse

import httpx

from asl.kalshi.auth import auth_headers, load_private_key


class KalshiREST:
    """Self-throttled GET client. Signs requests when credentials are given.

    Market-data endpoints are public; signing is optional but raises rate limits.
    There is deliberately no POST/DELETE method: this project never places orders.
    """

    def __init__(
        self,
        base_url: str,
        key_id: str = "",
        private_key_path: str = "",
        min_interval_s: float = 0.11,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self._base_path = urlparse(self.base_url).path
        self._key_id = key_id
        self._key = load_private_key(private_key_path) if key_id and private_key_path else None
        self._min_interval = min_interval_s
        self._last = 0.0
        self._http = httpx.Client(timeout=timeout, transport=transport)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> KalshiREST:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _throttle(self) -> None:
        wait = self._min_interval - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()

    def get(self, endpoint: str, params: dict[str, Any] | None = None, retries: int = 4) -> dict:
        endpoint = "/" + endpoint.lstrip("/")
        params = {k: v for k, v in (params or {}).items() if v is not None}
        for attempt in range(retries + 1):
            self._throttle()
            headers = {"Accept": "application/json"}
            if self._key is not None:
                headers |= auth_headers(self._key, self._key_id, "GET", self._base_path + endpoint)
            try:
                r = self._http.get(self.base_url + endpoint, params=params, headers=headers)
            except httpx.TransportError:
                if attempt == retries:
                    raise
                time.sleep(2**attempt)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                if attempt == retries:
                    r.raise_for_status()
                time.sleep(2**attempt)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError("unreachable")

    def paginate(
        self,
        endpoint: str,
        item_key: str,
        params: dict[str, Any] | None = None,
        max_pages: int = 10_000,
    ) -> Iterator[dict]:
        params = dict(params or {})
        for _ in range(max_pages):
            resp = self.get(endpoint, params)
            yield from resp.get(item_key, []) or []
            cursor = resp.get("cursor")
            if not cursor:
                return
            params["cursor"] = cursor
        raise RuntimeError(f"pagination exceeded {max_pages} pages for {endpoint}")

    # --- convenience wrappers ---

    def iter_markets(self, **params: Any) -> Iterator[dict]:
        params.setdefault("limit", 1000)
        return self.paginate("/markets", "markets", params)

    def iter_trades(
        self, ticker: str | None = None, min_ts: int | None = None, max_ts: int | None = None
    ) -> Iterator[dict]:
        p = {"ticker": ticker, "min_ts": min_ts, "max_ts": max_ts, "limit": 1000}
        return self.paginate("/markets/trades", "trades", p)

    def historical_cutoff(self) -> dict:
        return self.get("/historical/cutoff")

    def iter_historical_markets(self, **params: Any) -> Iterator[dict]:
        params.setdefault("limit", 1000)
        return self.paginate("/historical/markets", "markets", params)

    def iter_historical_trades(
        self, ticker: str | None = None, min_ts: int | None = None, max_ts: int | None = None
    ) -> Iterator[dict]:
        p = {"ticker": ticker, "min_ts": min_ts, "max_ts": max_ts, "limit": 1000}
        return self.paginate("/historical/trades", "trades", p)
