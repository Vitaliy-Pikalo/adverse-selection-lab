"""Historical backfill: settled Kalshi markets, their public trades, Coinbase 1-min candles.

    python -m asl.backfill --series KXBTC KXETH --days 30
    python -m asl.backfill --series KXBTC --days 2 --max-markets 50    # quick check

Resumable: trades are written one parquet per event; existing files are skipped.
Output (data/backfill/):
  cutoff.json                      Kalshi historical-cutoff timestamps at run time
  markets_<SERIES>.parquet         settled markets (strike, close time, result, volume)
  trades/<EVENT_TICKER>.parquet    all public trades for markets in that event
  spot/<PRODUCT>_1m.parquet        Coinbase 1-minute OHLCV
  backfill_log.json                counts + any endpoint failures
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pandas as pd

from asl.config import COINBASE_REST, REPO_ROOT, load_creds
from asl.fixed import parse_price, parse_size
from asl.kalshi.rest import KalshiREST

MARKET_COLS = [
    "ticker",
    "event_ticker",
    "series_ticker",
    "strike_type",
    "floor_strike",
    "cap_strike",
    "open_time",
    "close_time",
    "expiration_time",
    "result",
    "volume",
    "volume_fp",
    "open_interest",
    "open_interest_fp",
    "status",
    "settlement_value_dollars",
    "expiration_value",
]


def fetch_markets(rest: KalshiREST, series: str, t0: int, t1: int, log: dict) -> pd.DataFrame:
    rows: dict[str, dict] = {}
    sources = [
        (
            "live",
            lambda: rest.iter_markets(
                series_ticker=series, status="settled", min_close_ts=t0, max_close_ts=t1
            ),
        ),
        (
            "historical",
            lambda: rest.iter_historical_markets(
                series_ticker=series, min_close_ts=t0, max_close_ts=t1
            ),
        ),
    ]
    for name, it in sources:
        try:
            n = 0
            for m in it():
                m.setdefault("series_ticker", series)
                rows.setdefault(m["ticker"], m)
                n += 1
            log["markets"][f"{series}:{name}"] = n
        except httpx.HTTPStatusError as e:
            log["errors"].append(
                f"markets {series} {name}: {e.response.status_code} {e.response.text[:200]}"
            )
    df = pd.DataFrame(list(rows.values()))
    if df.empty:
        return df
    keep = [c for c in MARKET_COLS if c in df.columns]
    return df[keep]


def _trade_row(t: dict) -> dict:
    return {
        "trade_id": t.get("trade_id"),
        "ticker": t.get("ticker"),
        "created_time": t.get("created_time"),
        "yes_price": parse_price(t["yes_price_dollars"])
        if "yes_price_dollars" in t
        else int(t.get("yes_price", 0)) * 100,
        "count": parse_size(t["count_fp"]) if "count_fp" in t else int(t.get("count", 0)) * 100,
        "taker_outcome_side": t.get("taker_outcome_side") or t.get("taker_side"),
        "is_block_trade": bool(t.get("is_block_trade", False)),
    }


def fetch_trades(rest: KalshiREST, ticker: str, log: dict) -> list[dict]:
    rows = [_trade_row(t) for t in rest.iter_trades(ticker=ticker)]
    if not rows:  # archived markets live behind the historical endpoint
        try:
            rows = [_trade_row(t) for t in rest.iter_historical_trades(ticker=ticker)]
        except httpx.HTTPStatusError as e:
            log["errors"].append(f"historical trades {ticker}: {e.response.status_code}")
    return rows


def _volume(row: pd.Series) -> float:
    for c in ("volume_fp", "volume"):
        if c in row and pd.notna(row[c]):
            try:
                return float(row[c])
            except (TypeError, ValueError):
                pass
    return -1.0  # unknown -> fetch anyway


def fetch_coinbase_1m(
    product: str, start: datetime, end: datetime, client: httpx.Client
) -> pd.DataFrame:
    """Coinbase returns max 300 candles per call: [time, low, high, open, close, volume]."""
    out, cur = [], start
    step = timedelta(minutes=300)
    while cur < end:
        nxt = min(cur + step, end)
        for attempt in range(5):
            r = client.get(
                f"{COINBASE_REST}/products/{product}/candles",
                params={"granularity": 60, "start": cur.isoformat(), "end": nxt.isoformat()},
            )
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2**attempt)
                continue
            r.raise_for_status()
            out.extend(r.json())
            break
        time.sleep(0.15)
        cur = nxt
    df = pd.DataFrame(out, columns=["time", "low", "high", "open", "close", "volume"])
    df = df.drop_duplicates("time").sort_values("time")
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    return df.reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--series", nargs="+", default=["KXBTC", "KXETH"])
    ap.add_argument("--days", type=float, default=30)
    ap.add_argument("--end", default=None, help="ISO date (UTC); default now")
    ap.add_argument("--out", default=str(REPO_ROOT / "data" / "backfill"))
    ap.add_argument("--max-markets", type=int, default=0, help="cap per series (0 = all)")
    ap.add_argument("--products", nargs="+", default=["BTC-USD", "ETH-USD"])
    ap.add_argument("--skip-spot", action="store_true")
    args = ap.parse_args()

    end = (
        datetime.fromisoformat(args.end).replace(tzinfo=UTC) if args.end else datetime.now(tz=UTC)
    ).replace(second=0, microsecond=0)
    start = end - timedelta(days=args.days)
    t0, t1 = int(start.timestamp()), int(end.timestamp())
    out = Path(args.out)
    (out / "trades").mkdir(parents=True, exist_ok=True)
    log: dict = {
        "run_at": datetime.now(tz=UTC).isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "markets": {},
        "trades": {},
        "errors": [],
    }

    creds = load_creds()
    with KalshiREST(creds.rest_url, creds.key_id, creds.private_key_path) as rest:
        try:
            (out / "cutoff.json").write_text(json.dumps(rest.historical_cutoff(), indent=2))
        except httpx.HTTPStatusError as e:
            log["errors"].append(f"cutoff: {e.response.status_code}")

        for series in args.series:
            mk = fetch_markets(rest, series, t0, t1, log)
            print(f"{series}: {len(mk)} settled markets in window", flush=True)
            if mk.empty:
                continue
            mk.astype(str).to_parquet(out / f"markets_{series}.parquet", index=False)
            mk = mk.sort_values("close_time")
            mk = mk[mk.apply(_volume, axis=1) != 0]  # zero-volume markets have no trades
            if args.max_markets:
                mk = mk.head(args.max_markets)
            n_tr = 0
            for ev, grp in mk.groupby("event_ticker", sort=False):
                path = out / "trades" / f"{ev}.parquet"
                if path.exists():
                    continue
                rows = []
                for tk in grp["ticker"]:
                    rows.extend(fetch_trades(rest, tk, log))
                pd.DataFrame(
                    rows,
                    columns=list(_trade_row({"yes_price_dollars": "0", "count_fp": "0"}).keys()),
                ).to_parquet(path, index=False)
                n_tr += len(rows)
                print(f"  {ev}: {len(grp)} markets, {len(rows)} trades", flush=True)
            log["trades"][series] = n_tr

    if not args.skip_spot:
        (out / "spot").mkdir(exist_ok=True)
        with httpx.Client(timeout=30) as client:
            for prod in args.products:
                df = fetch_coinbase_1m(prod, start, end, client)
                df.to_parquet(out / "spot" / f"{prod}_1m.parquet", index=False)
                expected = int((end - start).total_seconds() // 60)
                log[f"spot_{prod}"] = {"rows": len(df), "expected_minutes": expected}
                print(f"{prod}: {len(df)} / {expected} 1-min candles", flush=True)

    (out / "backfill_log.json").write_text(json.dumps(log, indent=2))
    print(f"done. errors: {len(log['errors'])} (see backfill_log.json)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
