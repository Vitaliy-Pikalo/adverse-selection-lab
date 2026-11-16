"""Rebuild books from raw logs and produce derived tables + a data-quality report.

    python -m asl.replay --raw data/raw --out data/derived

Outputs (one parquet per UTC hour, so reruns are incremental and diffs are small):
  derived/tob/HOUR.parquet      top of book after every book change (valid books only)
  derived/trades/HOUR.parquet   Kalshi public trades
  derived/spot/HOUR.parquet     Coinbase ticker
  derived/markets.parquet       market metadata (strike, close time, ...)
  derived/quality_report.json   gaps, reconnects, downtime, crossed books, latency
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
import zlib
from collections import Counter, defaultdict
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import zstandard

from asl.book import BookManager
from asl.fixed import parse_price, parse_size

FILE_RE = re.compile(
    r"^(?P<src>kalshi|coinbase)_(?P<hour>\d{8}T\d{2})(?:\.p(?P<part>\d+))?"
    r"\.jsonl(?P<ext>\.gz|\.zst)?$"
)


def list_files(raw_dir: Path, src: str) -> list[tuple[str, Path]]:
    """(hour, path) in write order: per hour, compressed parts (base, p2, p3...) then plain."""
    out = []
    for p in raw_dir.iterdir():
        m = FILE_RE.match(p.name)
        if m and m["src"] == src:
            part = int(m["part"] or 1)
            plain = m["ext"] is None
            out.append((m["hour"], plain, part, p))
    return [(h, p) for h, _, _, p in sorted(out)]


def _open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    if path.suffix == ".zst":
        return io.TextIOWrapper(
            zstandard.ZstdDecompressor().stream_reader(path.open("rb"), closefd=True),
            encoding="utf-8",
        )
    return path.open("r", encoding="utf-8")


def read_lines(path: Path) -> Iterator[dict]:
    """Yield records; tolerate a truncated final line or a corrupt/truncated compressed tail.

    A corrupt tail is reported via the module-level CORRUPT list so quality reports show it.
    """
    try:
        with _open_text(path) as fh:
            for line in fh:
                if not line.endswith("\n"):
                    return  # partial last write
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except (EOFError, gzip.BadGzipFile, OSError, zlib.error, zstandard.ZstdError) as e:
        CORRUPT.append({"file": path.name, "error": repr(e)[:200]})
        return


CORRUPT: list[dict] = []


class KalshiReplayer:
    def __init__(self) -> None:
        self.mgr = BookManager()
        self.q: dict = {
            "messages": Counter(),
            "gaps": [],
            "disconnects": [],
            "book_invalid": [],
            "crossed_updates": 0,
            "server_errors": [],
            "latency_ms": [],
            "first_recv_ns": None,
            "last_recv_ns": None,
        }
        self.markets: dict[str, dict] = {}
        self._down_since: int | None = None
        self.downtime_s = 0.0

    def process(self, rec: dict, tob_rows: list, trade_rows: list) -> None:
        recv_ns, src, raw = rec["recv_ns"], rec["src"], rec["raw"]
        q = self.q
        q["first_recv_ns"] = q["first_recv_ns"] or recv_ns
        q["last_recv_ns"] = recv_ns
        if src == "meta":
            ev = raw.get("event")
            if ev == "connect":
                self.mgr.reset()
            elif ev == "disconnect":
                q["disconnects"].append({"recv_ns": recv_ns, "reason": raw.get("reason")})
                self._down_since = recv_ns
            elif ev == "market_info":
                m = raw["market"]
                self.markets[m["ticker"]] = m
            elif ev == "server_error":
                q["server_errors"].append(raw.get("msg"))
            return
        typ = raw.get("type", "?")
        q["messages"][typ] += 1
        if self._down_since is not None and typ in ("orderbook_snapshot", "orderbook_delta"):
            self.downtime_s += (recv_ns - self._down_since) / 1e9
            self._down_since = None
        for ev in self.mgr.handle(raw):
            if ev.kind == "gap":
                q["gaps"].append({"recv_ns": recv_ns, **(ev.detail or {})})
            elif ev.kind == "invalid" and ev.detail:
                q["book_invalid"].append({"recv_ns": recv_ns, "ticker": ev.ticker, **ev.detail})
            elif ev.kind == "book":
                top = self.mgr.books[ev.ticker].top()
                if top.crossed:
                    q["crossed_updates"] += 1
                ts_ms = raw["msg"].get("ts_ms")
                if ts_ms and len(q["latency_ms"]) < 200_000:
                    q["latency_ms"].append(recv_ns / 1e6 - ts_ms)
                tob_rows.append(
                    {
                        "recv_ns": recv_ns,
                        "ts_ms": ts_ms,
                        "clock": top.clock,
                        "ticker": top.ticker,
                        "msg_type": typ,
                        "yes_bid": top.yes_bid,
                        "yes_bid_size": top.yes_bid_size,
                        "yes_ask": top.yes_ask,
                        "yes_ask_size": top.yes_ask_size,
                        "crossed": top.crossed,
                    }
                )
            elif ev.kind == "trade":
                t = ev.detail or {}
                trade_rows.append(
                    {
                        "recv_ns": recv_ns,
                        "ts_ms": t.get("ts_ms"),
                        "trade_id": t.get("trade_id"),
                        "ticker": t.get("market_ticker"),
                        "yes_price": parse_price(t["yes_price_dollars"]),
                        "count": parse_size(t["count_fp"]),
                        "taker_outcome_side": t.get("taker_outcome_side") or t.get("taker_side"),
                        "is_block_trade": t.get("is_block_trade", False),
                    }
                )

    def report(self) -> dict:
        q = self.q
        lat = pd.Series(q["latency_ms"], dtype=float)
        span_s = ((q["last_recv_ns"] or 0) - (q["first_recv_ns"] or 0)) / 1e9
        return {
            "span_hours": round(span_s / 3600, 3),
            "messages": dict(q["messages"]),
            "n_gaps": len(q["gaps"]),
            "gaps": q["gaps"][:50],
            "n_disconnects": len(q["disconnects"]),
            "disconnects": q["disconnects"][:50],
            "downtime_s": round(self.downtime_s, 1),
            "uptime_pct": round(100 * (1 - self.downtime_s / span_s), 3) if span_s else None,
            "n_book_invalid": len(q["book_invalid"]),
            "crossed_updates": q["crossed_updates"],
            "n_server_errors": len(q["server_errors"]),
            "n_markets_seen": len(self.markets),
            "recv_minus_exchange_ms": (
                {
                    k: round(float(lat.quantile(p)), 1)
                    for k, p in [("p01", 0.01), ("p50", 0.5), ("p99", 0.99)]
                }
                if len(lat)
                else None
            ),
        }


def spot_row(rec: dict) -> dict | None:
    raw = rec["raw"]
    if rec["src"] != "coinbase" or raw.get("type") != "ticker":
        return None

    def f(k: str) -> float | None:
        v = raw.get(k)
        return float(v) if v not in (None, "") else None

    return {
        "recv_ns": rec["recv_ns"],
        "time": raw.get("time"),
        "product": raw.get("product_id"),
        "price": f("price"),
        "best_bid": f("best_bid"),
        "best_ask": f("best_ask"),
        "last_size": f("last_size"),
        "side": raw.get("side"),
        "sequence": raw.get("sequence"),
        "trade_id": raw.get("trade_id"),
    }


def _write(rows: list, path: Path) -> None:
    if rows:
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(path, index=False)


def run(raw_dir: Path, out_dir: Path) -> dict:
    kr = KalshiReplayer()
    by_hour: dict[str, list[Path]] = defaultdict(list)
    for hour, p in list_files(raw_dir, "kalshi"):
        by_hour[hour].append(p)
    for hour in sorted(by_hour):
        tob, trades = [], []
        for p in by_hour[hour]:
            for rec in read_lines(p):
                kr.process(rec, tob, trades)
        _write(tob, out_dir / "tob" / f"{hour}.parquet")
        _write(trades, out_dir / "trades" / f"{hour}.parquet")

    spot_stats = Counter()
    cb_hours: dict[str, list[Path]] = defaultdict(list)
    for hour, p in list_files(raw_dir, "coinbase"):
        cb_hours[hour].append(p)
    for hour in sorted(cb_hours):
        rows = []
        for p in cb_hours[hour]:
            for rec in read_lines(p):
                if rec["src"] == "meta" and rec["raw"].get("event") == "disconnect":
                    spot_stats["disconnects"] += 1
                r = spot_row(rec)
                if r:
                    rows.append(r)
                    spot_stats["ticks"] += 1
        _write(rows, out_dir / "spot" / f"{hour}.parquet")

    if kr.markets:
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(list(kr.markets.values())).astype(str).to_parquet(
            out_dir / "markets.parquet", index=False
        )
    report = {"kalshi": kr.report(), "coinbase": dict(spot_stats), "corrupt_files": list(CORRUPT)}
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "quality_report.json").write_text(json.dumps(report, indent=2, default=str))
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--out", default="data/derived")
    args = ap.parse_args()
    if not Path(args.raw).is_dir():
        print(f"raw dir not found: {args.raw} (run the recorder first)", file=sys.stderr)
        return 2
    report = run(Path(args.raw), Path(args.out))
    print(
        json.dumps(
            {
                k: {kk: vv for kk, vv in v.items() if kk not in ("gaps", "disconnects")}
                for k, v in report.items()
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
