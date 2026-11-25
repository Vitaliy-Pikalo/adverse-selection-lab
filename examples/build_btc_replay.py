"""Build the public BTC example. Run with a directory containing Kalshi Eye's
2026-10-25 KXBTC15M bbo and trades parquet files (named bbo.parquet and
trades.parquet) and the path to examples/btc-real-replay.html. The HTML file
contains a replaceable JSON payload. Requires pandas and pyarrow.
"""
import argparse
import json
import math
import re
from bisect import bisect_right
from pathlib import Path

import pandas as pd

TICKER = "KXBTC15M-26OCT251200-00"
OPEN = pd.Timestamp("2026-10-25T15:45:00Z").value
SECOND = 1_000_000_000


def read_market(directory, name):
    data = pd.read_parquet(directory / (name + ".parquet"), filters=[("ticker", "=", TICKER)])
    data = data.sort_values("ts_recv", kind="stable")
    data["ns"] = data.ts_recv.astype("datetime64[ns, UTC]").astype("int64") - OPEN
    return data


def replay(directory):
    books = read_market(directory, "bbo")
    trades = read_market(directory, "trades").dropna(subset=["trade_id"]).drop_duplicates("trade_id")
    times = books.ns.tolist()
    quotes = list(zip(books.yes_bid, books.yes_ask, books.yes_bid_size, books.yes_ask_size))
    sells = trades[trades.taker_side.eq("no")]
    prints = list(zip(sells.ns, sells.yes_price, sells["size"]))
    print_times = sells.ns.tolist()

    def quote(t):
        i = bisect_right(times, t) - 1
        if i < 0 or t - times[i] > 5 * SECOND:
            return None
        b, a, bs, az = quotes[i]
        if not (0 <= b < a <= 1 and bs > 0 and az > 0):
            return None
        return [round(100*b, 4), round(100*a, 4), bs, az]

    decisions = []
    # Reserve 120 seconds for a fill at the deadline and its later midpoint.
    for sec in range(1, 780, 5):
        start = sec * SECOND
        end = start + 60 * SECOND
        q, finish = quote(start), quote(end)
        if not q or not finish or q[3] < 1:
            continue
        bid, ask, ahead, _ = q
        initial_queue = ahead
        remaining = 1.0
        fills, changes = [], []
        for t, price, qty in prints[bisect_right(print_times, start):bisect_right(print_times, end)]:
            if round(price * 100, 4) != bid or not math.isfinite(qty) or qty <= 0:
                continue
            used = min(ahead, qty)
            ahead = round(ahead - used, 4)
            size = round(min(remaining, qty - used), 4)
            remaining = round(remaining - size, 4)
            changes.append([round((t-start)/SECOND, 6), ahead, remaining])
            if size > 0:
                later = quote(t + 60 * SECOND)
                fills.append([round((t-start)/SECOND, 6), size,
                              None if later is None else round(sum(later[:2])/2 - bid, 4)])
            if remaining == 0:
                break
        mid = sum(finish[:2])/2
        decisions.append(dict(sec=sec, bid=bid, ask=ask, ahead=initial_queue, fills=fills,
                              queue=changes, size=round(1-remaining, 4), mid=mid,
                              take=round(mid-ask, 4), wait=round((1-remaining)*(mid-bid), 4)))
    mids = []
    for sec in range(901):
        q = quote(sec*SECOND)
        mids.append(None if q is None else round(sum(q[:2])/2, 4))
    return dict(ticker=TICKER, decisions=decisions, mids=mids,
                quotes=len(books), trades=len(trades))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("html", type=Path)
    args = parser.parse_args()
    payload = replay(args.directory)
    source = args.html.read_text(encoding="utf-8")
    pattern = r'(<script id="data" type="application/json">).*?(</script>)'
    updated, count = re.subn(pattern, lambda m: m[1] + json.dumps(payload, separators=(",", ":"),
                             allow_nan=False) + m[2], source, flags=re.S)
    if count != 1:
        raise ValueError("expected one data element")
    args.html.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    main()
