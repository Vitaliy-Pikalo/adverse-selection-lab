"""Replay one paper YES bid across consecutive recorder files.

Usage: python -m asl.queue_study TICKER RECORDING [RECORDING ...] [--horizon 3]
Join the full displayed bid queue for one contract. Only unique, regular NO-taker
prints at that exact price consume queue. Cancel when the bid changes. After a
full fill, rejoin on the next valid book update. The horizon counts subsequent
book updates for this ticker. A new snapshot or continuity break censors open
observations. Receive order is used; exchange queue position is unobserved.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
from collections import Counter, deque
from pathlib import Path

from asl.book import BookManager
from asl.fixed import PRICE_SCALE, SIZE_SCALE, parse_price, parse_size


class BidStudy:
    def __init__(self, ticker: str, horizon: int):
        if horizon < 1:
            raise ValueError("horizon must be positive")
        self.ticker = ticker
        self.horizon = horizon
        self.books = BookManager()
        self.pending = deque()
        self.seen = set()
        self.count = Counter()
        self.step = 0
        self.bid = None
        self.mid = None
        self.ahead = 0
        self.remaining = 0
        self.mark_total = self.move_total = 0.0

    def cancel(self):
        if self.bid is not None:
            self.count["cancelled_orders"] += 1
        self.bid = None

    def interrupt(self):
        self.cancel()
        self.pending.clear()
        self.mid = None

    def observe_book(self):
        top = self.books.books[self.ticker].top()
        if not top.two_sided or top.crossed:
            self.interrupt()
            return
        self.step += 1
        self.mid = (top.yes_bid + top.yes_ask) / 2
        while self.pending and self.pending[0][0] <= self.step:
            _, size, price, initial_mid = self.pending.popleft()
            self.count["marked_size"] += size
            if self.mid < initial_mid:
                self.count["down_size"] += size
            if self.mid < price:
                self.count["negative_size"] += size
            self.mark_total += size * (self.mid - price)
            self.move_total += size * (self.mid - initial_mid)
        if self.bid != top.yes_bid:
            self.cancel()
            self.bid = top.yes_bid
            self.ahead = top.yes_bid_size
            self.remaining = SIZE_SCALE
            self.count["orders"] += 1

    def observe_trade(self, trade):
        trade_id = trade.get("trade_id")
        if not trade_id or trade_id in self.seen or trade.get("is_block_trade"):
            return
        self.seen.add(trade_id)
        side = trade.get("taker_outcome_side") or trade.get("taker_side")
        if self.bid is None or side != "no" or parse_price(trade["yes_price_dollars"]) != self.bid:
            return
        volume = parse_size(trade["count_fp"])
        if volume <= 0:
            return
        before = min(self.ahead, volume)
        self.ahead -= before
        size = min(self.remaining, volume - before)
        if size:
            self.pending.append((self.step + self.horizon, size, self.bid, self.mid))
            self.count["filled_size"] += size
            self.remaining -= size
            if not self.remaining:
                self.bid = None
                self.count["completed_orders"] += 1

    def process(self, record):
        raw = record["raw"]
        if record["src"] == "meta":
            if raw.get("event") in {"connect", "disconnect", "server_error"}:
                self.interrupt()
                self.books.reset()
                self.count["breaks"] += 1
            return
        if record["src"] != "kalshi":
            return
        for event in self.books.handle(raw):
            if event.kind in {"gap", "error"}:
                self.interrupt()
                self.books.reset()
                self.count["breaks"] += 1
                return
            if event.ticker != self.ticker:
                continue
            if event.kind == "invalid":
                self.interrupt()
            elif event.kind == "book":
                if raw["type"] == "orderbook_snapshot":
                    self.interrupt()
                self.observe_book()
            elif event.kind == "trade":
                self.observe_trade(event.detail)

    def report(self):
        c = self.count
        marked = c["marked_size"]
        return {
            "ticker": self.ticker,
            "horizon_book_updates": self.horizon,
            "valid_book_updates": self.step,
            "continuity_breaks": self.count["breaks"],
            "submitted_orders": c["orders"],
            "fully_filled_orders": c["completed_orders"],
            "cancelled_orders": c["cancelled_orders"],
            "open_at_end": int(self.bid is not None),
            "filled_contracts": c["filled_size"] / SIZE_SCALE,
            "marked_contracts": marked / SIZE_SCALE,
            "censored_contracts": (c["filled_size"] - marked) / SIZE_SCALE,
            "mid_moved_down_fraction": c["down_size"] / marked if marked else None,
            "negative_markout_fraction": c["negative_size"] / marked if marked else None,
            "mean_mid_move_cents": self.move_total * 100 / PRICE_SCALE / marked if marked else None,
            "mean_markout_cents": self.mark_total * 100 / PRICE_SCALE / marked if marked else None,
        }


def open_recording(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    if path.suffix == ".zst":
        import zstandard

        stream = zstandard.ZstdDecompressor().stream_reader(path.open("rb"), closefd=True)
        return io.TextIOWrapper(stream, encoding="utf-8")
    return path.open(encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ticker")
    parser.add_argument("recordings", nargs="+", type=Path, help="files in capture order")
    parser.add_argument("--horizon", type=int, default=3)
    args = parser.parse_args()
    study = BidStudy(args.ticker, args.horizon)
    for path in args.recordings:
        with open_recording(path) as records:
            for line in records:
                study.process(json.loads(line))
    print(json.dumps(study.report(), indent=2))


if __name__ == "__main__":
    main()
