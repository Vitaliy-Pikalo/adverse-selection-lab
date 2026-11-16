"""Coinbase Exchange public ticker recorder (underlying spot reference).

Kalshi BTC/ETH contracts settle on a 60 s average of the CF Benchmarks real-time
index, which aggregates several exchanges. Coinbase is one constituent and is used
here as a high-frequency proxy for the underlying; the proxy error is a known
limitation (see DECISIONS.md).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time

import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from asl.config import COINBASE_WS
from asl.recorder.writer import RawWriter


class CoinbaseRecorder:
    def __init__(
        self,
        products: list[str],
        channels: list[str],
        writer: RawWriter,
        url: str = COINBASE_WS,
        stale_seconds: float = 30.0,
    ):
        self.products = products
        self.channels = channels
        self.writer = writer
        self.url = url
        self.stale_seconds = stale_seconds
        self.stats = {"msgs": 0, "reconnects": 0}

    async def run_once(self) -> None:
        async with connect(
            self.url, ping_interval=20, ping_timeout=20, max_size=None, open_timeout=20
        ) as ws:
            sub = {
                "type": "subscribe",
                "product_ids": self.products,
                "channels": self.channels + ["heartbeat"],
            }
            self.writer.meta("connect", url=self.url, payload=sub)
            await ws.send(json.dumps(sub))
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=self.stale_seconds)
                recv_ns = time.time_ns()
                text = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
                try:
                    data = orjson.loads(text)
                except orjson.JSONDecodeError:
                    self.writer.write("coinbase", {"unparsed": text}, recv_ns)
                    continue
                if data.get("type") == "heartbeat":
                    continue  # keeps the stale timer alive; not worth disk
                self.writer.write_raw("coinbase", text, recv_ns)
                self.stats["msgs"] += 1
                if data.get("type") == "error":
                    self.writer.meta("server_error", msg=data)

    async def run_forever(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            started = time.monotonic()
            try:
                await self.run_once()
                reason = "closed"
            except TimeoutError:
                reason = f"stale: no message for {self.stale_seconds}s"
            except ConnectionClosed as e:
                reason = f"connection_closed: {e!r}"
            except asyncio.CancelledError:
                self.writer.meta("stop")
                raise
            except Exception as e:
                reason = f"error: {e!r}"
            self.stats["reconnects"] += 1
            self.writer.meta(
                "disconnect", reason=reason, uptime_s=round(time.monotonic() - started, 1)
            )
            if time.monotonic() - started > 300:
                backoff = 1.0
            with contextlib.suppress(asyncio.TimeoutError, TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            backoff = min(backoff * 2, 60.0)
