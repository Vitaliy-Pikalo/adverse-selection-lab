"""Collector entrypoint: Kalshi + Coinbase recorders in one process.

    python -m asl.recorder.run --config configs/collector.yaml
    python -m asl.recorder.run --config configs/collector.yaml --duration 600   # 10-min smoke test

Writes data/raw/{kalshi,coinbase}_YYYYMMDDTHH.jsonl(.gz) and prints a status line
every 60 s. Ctrl+C stops cleanly (files are flushed and closed hours gzipped).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys
import time
from pathlib import Path

from asl.config import REPO_ROOT, load_collector_config, load_creds
from asl.kalshi.rest import KalshiREST
from asl.recorder.coinbase_ws import CoinbaseRecorder
from asl.recorder.kalshi_ws import KalshiRecorder
from asl.recorder.writer import RawWriter


async def _status(
    krec: KalshiRecorder, crec: CoinbaseRecorder, stop: asyncio.Event, every: float = 60.0
) -> None:
    t0 = time.monotonic()
    while not stop.is_set():
        with contextlib.suppress(asyncio.TimeoutError, TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=every)
        mins = (time.monotonic() - t0) / 60
        print(
            f"[{time.strftime('%H:%M:%S')}] up {mins:.1f}m | kalshi msgs={krec.stats['msgs']} "
            f"markets={len(krec.subscribed)} gaps={krec.stats['gaps']} "
            f"reconnects={krec.stats['reconnects']} | coinbase msgs={crec.stats['msgs']} "
            f"reconnects={crec.stats['reconnects']}",
            flush=True,
        )


async def amain(config: str, duration: float) -> int:
    cfg_path = Path(config)
    if not cfg_path.exists():
        cfg_path = REPO_ROOT / config
    cfg = load_collector_config(cfg_path)
    creds = load_creds()
    if not creds.key_id or not creds.private_key_path:
        print(
            "KALSHI_KEY_ID / KALSHI_PRIVATE_KEY_PATH missing in .env "
            "(the orderbook channel requires an authenticated connection)",
            file=sys.stderr,
        )
        return 2
    kw = RawWriter(cfg.out_dir, "kalshi", cfg.flush_seconds)
    cw = RawWriter(cfg.out_dir, "coinbase", cfg.flush_seconds)
    rest = KalshiREST(creds.rest_url, creds.key_id, creds.private_key_path)
    krec = KalshiRecorder(cfg, creds, kw, rest=rest)
    crec = CoinbaseRecorder(cfg.coinbase_products, cfg.coinbase_channels, cw)
    kw.meta("start", config=str(config), series=cfg.series, env=creds.env)
    cw.meta("start", config=str(config), products=cfg.coinbase_products)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows: Ctrl+C -> KeyboardInterrupt
            loop.add_signal_handler(sig, stop.set)

    tasks = [
        asyncio.create_task(krec.run_forever(stop)),
        asyncio.create_task(crec.run_forever(stop)),
        asyncio.create_task(_status(krec, crec, stop)),
    ]
    try:
        if duration > 0:
            with contextlib.suppress(asyncio.TimeoutError, TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=duration)
        else:
            await stop.wait()
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        kw.meta("shutdown", stats=krec.stats)
        cw.meta("shutdown", stats=crec.stats)
        kw.close()
        cw.close()
        rest.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="configs/collector.yaml")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = forever)")
    args = ap.parse_args()
    try:
        return asyncio.run(amain(args.config, args.duration))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
