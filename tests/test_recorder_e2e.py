"""Recorder against a local fake Kalshi WS server: subscribe, record, detect gap, resync."""

import asyncio
import contextlib
import json

from websockets.asyncio.server import serve

from asl.config import CollectorConfig, KalshiCreds
from asl.recorder.kalshi_ws import KalshiRecorder, discover_markets
from asl.recorder.writer import RawWriter
from asl.replay import run


def _snap(sid, seq, t):
    return {
        "type": "orderbook_snapshot",
        "sid": sid,
        "seq": seq,
        "msg": {
            "market_ticker": t,
            "market_id": "m",
            "clock": 1,
            "yes_dollars_fp": [["0.2000", "10.00"]],
            "no_dollars_fp": [["0.7000", "4.00"]],
        },
    }


def _delta(sid, seq, t, d="1.00"):
    return {
        "type": "orderbook_delta",
        "sid": sid,
        "seq": seq,
        "msg": {
            "market_ticker": t,
            "market_id": "m",
            "clock": seq,
            "price_dollars": "0.2000",
            "delta_fp": d,
            "side": "yes",
            "ts_ms": 1,
        },
    }


async def test_recorder_records_detects_gap_and_resubscribes(tmp_path, rsa_key_path):
    connections = []
    auth_seen = []

    async def handler(ws):
        conn_no = len(connections)
        connections.append(ws)
        auth_seen.append(ws.request.headers.get("KALSHI-ACCESS-KEY"))
        sids = {}
        async for raw in ws:
            cmd = json.loads(raw)
            if cmd["cmd"] != "subscribe":
                continue
            ch = cmd["params"]["channels"][0]
            sid = len(sids) + 1
            sids[ch] = sid
            await ws.send(
                json.dumps(
                    {"id": cmd["id"], "type": "subscribed", "msg": {"channel": ch, "sid": sid}}
                )
            )
            if ch != "orderbook_delta":
                continue
            t = cmd["params"]["market_tickers"][0]
            await ws.send(json.dumps(_snap(sid, 1, t)))
            await ws.send(json.dumps(_delta(sid, 2, t)))
            if conn_no == 0:
                await ws.send(json.dumps(_delta(sid, 7, t)))  # gap -> client must reconnect
            else:
                await ws.send(json.dumps(_delta(sid, 3, t)))

    cfg = CollectorConfig(
        series=["KXBTC"],
        refresh_seconds=3600,
        channels=["orderbook_delta", "trade"],
        out_dir=tmp_path,
    )
    creds = KalshiCreds(key_id="kid", private_key_path=str(rsa_key_path))
    writer = RawWriter(tmp_path, "kalshi", flush_seconds=0)
    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        rec = KalshiRecorder(
            cfg,
            creds,
            writer,
            ws_url=f"ws://127.0.0.1:{port}/trade-api/ws/v2",
            discover=lambda: [
                {
                    "ticker": "KXBTC-T1",
                    "strike_type": "between",
                    "close_time": "2099-01-01T00:00:00Z",
                }
            ],
            stale_seconds=1.0,
        )
        stop = asyncio.Event()
        task = asyncio.create_task(rec.run_forever(stop))
        for _ in range(100):
            await asyncio.sleep(0.05)
            if len(connections) >= 2 and rec.stats["msgs"] >= 9:
                break
        stop.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    writer.close()

    assert len(connections) >= 2, "recorder should reconnect after a seq gap"
    assert auth_seen[0] == "kid"
    assert rec.stats["gaps"] >= 1
    rep = run(tmp_path, tmp_path / "derived")["kalshi"]
    assert rep["n_gaps"] >= 1 and rep["n_disconnects"] >= 1
    assert rep["n_markets_seen"] == 1
    assert rep["messages"]["orderbook_snapshot"] >= 2


def test_discover_filters_window_and_caps():
    class FakeRest:
        def iter_markets(self, **p):
            assert p["status"] == "open"
            return iter(
                [
                    {"ticker": "late", "close_time": "2026-01-01T10:00:00Z"},
                    {"ticker": "soon", "close_time": "2026-01-01T01:00:00Z"},
                    {"ticker": "out", "close_time": "2026-01-02T00:00:00Z"},
                ]
            )

    now = 1767225600  # 2026-01-01T00:00:00Z
    cfg = CollectorConfig(series=["KXBTC"], max_hours_to_close=12, max_markets=1)
    got = discover_markets(FakeRest(), cfg, now=now)
    assert [m["ticker"] for m in got] == ["soon"]
