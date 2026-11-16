"""Kalshi WebSocket recorder: orderbook_delta + trade for near-expiry crypto markets.

Behaviour
- discovers open markets in the configured series closing within `max_hours_to_close`
- one subscription per channel; markets are added/removed in place via update_subscription
- every raw message is logged before any processing (raw log = source of truth)
- a BookManager runs live only to detect seq gaps; on an orderbook gap the connection
  is dropped and rebuilt so every book restarts from a fresh snapshot
- reconnects with capped exponential backoff; every lifecycle step is a `meta` line
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import Callable
from datetime import datetime
from urllib.parse import urlparse

import httpx
import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from asl.book import SeqTracker
from asl.config import CollectorConfig, KalshiCreds
from asl.kalshi.auth import auth_headers, load_private_key
from asl.kalshi.rest import KalshiREST
from asl.recorder.writer import RawWriter

MARKET_INFO_FIELDS = (
    "ticker",
    "event_ticker",
    "series_ticker",
    "market_type",
    "strike_type",
    "floor_strike",
    "cap_strike",
    "open_time",
    "close_time",
    "expiration_time",
    "status",
    "tick_size",
    "price_level_structure",
    "rules_primary",
)


class ResyncNeeded(Exception):
    pass


def _parse_time(s: str | None) -> float | None:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def _f(v: object) -> float | None:
    try:
        return float(v) if v not in (None, "") else None  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def is_live(m: dict, min_mid: float = 0.02, max_mid: float = 0.98) -> bool:
    """Worth recording: it has traded, or it is quoted two-sided away from 0/1.

    Far out-of-the-money strikes sit at 0/1¢ with no trades; they cost subscription slots and
    disk but carry no adverse-selection information.
    """
    if (_f(m.get("volume_fp")) or _f(m.get("volume")) or 0) > 0:
        return True
    bid, ask = _f(m.get("yes_bid_dollars")), _f(m.get("yes_ask_dollars"))
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return False
    return min_mid <= (bid + ask) / 2 <= max_mid


def rank_key(m: dict, priority: set[str]) -> tuple:
    vol = _f(m.get("volume_fp")) or _f(m.get("volume")) or 0.0
    return (m.get("series_ticker") not in priority, -vol, m.get("close_time", ""))


def select_markets(
    ranked: list[dict], current: set[str], cap: int, priority: set[str]
) -> list[str]:
    """Priority series always; keep still-live current markets (avoids churn); fill by rank."""
    out: list[str] = []
    seen: set[str] = set()

    def take(t: str) -> None:
        if t not in seen and len(out) < cap:
            seen.add(t)
            out.append(t)

    for m in ranked:
        if m.get("series_ticker") in priority:
            take(m["ticker"])
    for m in ranked:
        if m["ticker"] in current:
            take(m["ticker"])
    for m in ranked:
        take(m["ticker"])
    return out


def discover_markets(
    rest: KalshiREST, cfg: CollectorConfig, now: float | None = None
) -> list[dict]:
    """Live open markets in cfg.series closing within the window, ranked (priority, volume)."""
    now = now or time.time()
    horizon = now + cfg.max_hours_to_close * 3600
    priority = set(cfg.priority_series)
    found: list[dict] = []
    for series in cfg.series:
        try:
            it = list(
                rest.iter_markets(
                    series_ticker=series,
                    status="open",
                    min_close_ts=int(now),
                    max_close_ts=int(horizon),
                )
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code != 400:
                raise
            # server rejected the close-time filters: fall back to filtering locally
            it = list(rest.iter_markets(series_ticker=series, status="open"))
        for m in it:
            ct = _parse_time(m.get("close_time"))
            if ct is None or not now < ct <= horizon:
                continue
            m.setdefault("series_ticker", series)
            if series in priority or is_live(m, cfg.min_mid, cfg.max_mid):
                found.append(m)
    found.sort(key=lambda m: rank_key(m, priority))
    return found


class KalshiRecorder:
    def __init__(
        self,
        cfg: CollectorConfig,
        creds: KalshiCreds,
        writer: RawWriter,
        rest: KalshiREST | None = None,
        ws_url: str | None = None,
        discover: Callable[[], list[dict]] | None = None,
        stale_seconds: float = 120.0,
    ):
        self.cfg = cfg
        self.creds = creds
        self.writer = writer
        self.ws_url = ws_url or creds.ws_url
        self._key = load_private_key(creds.private_key_path) if creds.private_key_path else None
        self.rest = rest
        self._discover = discover or (lambda: discover_markets(self.rest, self.cfg))
        self.stale_seconds = stale_seconds
        self.seen_info: set[str] = set()
        self.subscribed: set[str] = set()
        self.channel_sid: dict[str, int] = {}
        self._cmd_id = 0
        self._cmd_channel: dict[int, str] = {}
        self.stats = {"msgs": 0, "gaps": 0, "reconnects": 0}

    def _next_id(self) -> int:
        self._cmd_id += 1
        return self._cmd_id

    async def _markets(self) -> list[str]:
        markets = await asyncio.to_thread(self._discover)
        chosen = select_markets(
            markets, self.subscribed, self.cfg.max_markets, set(self.cfg.priority_series)
        )
        keep = set(chosen)
        markets = [m for m in markets if m["ticker"] in keep]
        for m in markets:
            if m["ticker"] not in self.seen_info:
                self.seen_info.add(m["ticker"])
                self.writer.meta(
                    "market_info", market={k: m.get(k) for k in MARKET_INFO_FIELDS if k in m}
                )
        return chosen

    async def _send(self, ws, cmd: str, params: dict, channel: str | None = None) -> None:
        cid = self._next_id()
        if channel:
            self._cmd_channel[cid] = channel
        payload = {"id": cid, "cmd": cmd, "params": params}
        self.writer.meta("send", payload=payload)
        await ws.send(json.dumps(payload))

    async def _refresh_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(self.cfg.refresh_seconds)
            try:
                want = set(await self._markets())
            except Exception as e:  # REST hiccup: keep current subscriptions
                self.writer.meta("discover_error", error=repr(e))
                continue
            add, drop = sorted(want - self.subscribed), sorted(self.subscribed - want)
            for sid in self.channel_sid.values():
                if add:
                    await self._send(
                        ws,
                        "update_subscription",
                        {"sid": sid, "market_tickers": add, "action": "add_markets"},
                    )
                if drop:
                    await self._send(
                        ws,
                        "update_subscription",
                        {"sid": sid, "market_tickers": drop, "action": "delete_markets"},
                    )
            if add or drop:
                self.writer.meta("market_set", added=add, dropped=drop, total=len(want))
            self.subscribed = want

    async def run_once(self) -> None:
        """One connection lifetime. Returns/raises when it ends."""
        tickers = await self._markets()
        headers = {}
        if self._key is not None:
            path = urlparse(self.ws_url).path
            headers = auth_headers(self._key, self.creds.key_id, "GET", path)
        tracker = SeqTracker()
        self.channel_sid.clear()
        self._cmd_channel.clear()
        async with connect(
            self.ws_url,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=60,  # high-throughput feed: a pong can queue behind data frames
            max_size=None,
            open_timeout=20,
        ) as ws:
            self.writer.meta("connect", url=self.ws_url, n_markets=len(tickers))
            self.subscribed = set(tickers)
            for ch in self.cfg.channels:
                await self._send(
                    ws, "subscribe", {"channels": [ch], "market_tickers": tickers}, channel=ch
                )
            refresher = asyncio.create_task(self._refresh_loop(ws))
            try:
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=self.stale_seconds)
                    recv_ns = time.time_ns()
                    text = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
                    try:
                        data = orjson.loads(text)
                    except orjson.JSONDecodeError:
                        self.writer.write("kalshi", {"unparsed": text}, recv_ns)
                        continue
                    self.writer.write_raw("kalshi", text, recv_ns)
                    self.stats["msgs"] += 1
                    self._on_message(data, tracker)
            finally:
                refresher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await refresher

    def _on_message(self, data: dict, tracker: SeqTracker) -> None:
        typ = data.get("type")
        if typ == "subscribed":
            ch = data["msg"].get("channel") or self._cmd_channel.get(data.get("id"), "")
            self.channel_sid[ch] = data["msg"]["sid"]
        elif typ == "error":
            self.writer.meta("server_error", msg=data)
        gap = tracker.observe(data)
        if gap:
            self.stats["gaps"] += 1
            self.writer.meta("gap", **gap)
            if gap["channel"] != "trade":
                raise ResyncNeeded(str(gap))

    async def run_forever(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            started = time.monotonic()
            try:
                await self.run_once()
                reason = "closed"
            except ResyncNeeded as e:
                reason = f"resync: {e}"
            except TimeoutError:
                reason = f"stale: no message for {self.stale_seconds}s"
            except ConnectionClosed as e:
                reason = f"connection_closed: {e!r}"
            except asyncio.CancelledError:
                self.writer.meta("stop")
                raise
            except Exception as e:  # never die silently; log and retry
                reason = f"error: {e!r}"
            self.stats["reconnects"] += 1
            self.writer.meta(
                "disconnect", reason=reason, uptime_s=round(time.monotonic() - started, 1)
            )
            if time.monotonic() - started > 300:
                backoff = 1.0  # had a healthy session
            if reason.startswith("resync"):
                backoff = 1.0  # gaps should resubscribe immediately
            with contextlib.suppress(asyncio.TimeoutError, TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            backoff = min(backoff * 2, 60.0)
