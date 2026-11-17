"""M2: maker markout labels for backfilled public trades.

For every public trade we take the maker's side (the opposite of the taker)
and measure how the contract's value moved against or for the maker:

  maker_sign = -1 if the taker bought YES (maker sold YES)
               +1 if the taker bought NO  (maker effectively bought YES)
  markout_h  = maker_sign * (V(t+h) - yes_price)     dollars per contract

V is the model fair value from the log-normal pricer (asl.pricer) using
Coinbase spot at t+h, or the settlement value (0/1) once t+h passes close.
`mo_settle` uses the settlement value directly: unbiased but noisy.
Negative markout = the maker lost = adverse selection.

  move_h = maker_sign * (V(t+h) - V(t))

splits out the part of the markout caused by the value moving after the
trade, net of any gap between the model and the trade price at t
(`maker_edge`). mo_h = maker_edge + move_h. move_h is the cleaner adverse
selection measure because it doesn't depend on the model's level being right.

The D2 hypothesis feature is `stale_move_k`: how much the model fair value
moved over the k minutes before the trade because spot moved, signed in the
taker's direction. If takers pick off stale quotes, maker markouts should get
worse as stale_move rises.

Lookahead rules (checked by tests):
  * spot at time t = open of the 1-min candle containing t (known at t)
  * sigma at minute m = std of 1-min log returns over the window ending at the
    close of minute m-1
  * features use only data at or before the trade; labels use data after it

Labels are pre-fee. Kalshi maker fees, where charged, shift every markout by
a constant and are applied in M4, not here.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from asl.fixed import PRICE_SCALE, SIZE_SCALE
from asl.pricer import prob_yes

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HORIZONS = (1, 5, 15)
STALE_LOOKBACKS = (1, 5)
VOL_WINDOW_MIN = 60

SERIES_PRODUCT = {"KXBTC": "BTC-USD", "KXETH": "ETH-USD"}


def product_for(series: str) -> str:
    for prefix, prod in SERIES_PRODUCT.items():
        if series.startswith(prefix):
            return prod
    raise ValueError(f"no spot product for series {series!r}")


# --------------------------------------------------------------------- spot


def prepare_spot(candles: pd.DataFrame, vol_window: int = VOL_WINDOW_MIN) -> pd.DataFrame:
    """Index 1-min candles by minute start; add per-minute sigma known at open.

    Missing minutes are reindexed as NaN (not forward-filled) so gaps surface
    as NaN features instead of silently reusing a stale price.
    """
    c = candles.copy()
    c["time"] = pd.to_datetime(c["time"], utc=True)
    c = c.drop_duplicates("time").set_index("time").sort_index()
    full = pd.date_range(c.index.min(), c.index.max(), freq="1min", tz="UTC")
    c = c.reindex(full)
    ret = np.log(c["close"]).diff()
    # sigma for minute m uses returns through the close of m-1.
    c["sigma"] = ret.rolling(vol_window, min_periods=vol_window // 2).std().shift(1)
    return c[["open", "close", "sigma"]]


def spot_at(spot: pd.DataFrame, ts: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """(spot, sigma) known at each timestamp: the open of its minute."""
    minute = pd.DatetimeIndex(pd.to_datetime(ts, utc=True)).floor("1min").as_unit("ns")
    idx = spot.index.as_unit("ns")
    s = pd.Series(spot["open"].to_numpy(), index=idx).reindex(minute).to_numpy(dtype=float)
    sig = pd.Series(spot["sigma"].to_numpy(), index=idx).reindex(minute).to_numpy(dtype=float)
    return s, sig


# ------------------------------------------------------------------- labels


def load_trades(backfill: Path, series: str) -> pd.DataFrame:
    files = sorted((backfill / "trades").glob(f"{series}-*.parquet"))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return df.drop_duplicates("trade_id")


def load_markets(backfill: Path, series: str) -> pd.DataFrame:
    m = pd.read_parquet(backfill / f"markets_{series}.parquet")
    for col in ("floor_strike", "cap_strike"):
        m[col] = pd.to_numeric(m[col], errors="coerce")
    m["close_time"] = pd.to_datetime(m["close_time"], utc=True)
    m["settle_yes"] = m["result"].map({"yes": 1.0, "no": 0.0})
    return m[
        [
            "ticker",
            "event_ticker",
            "strike_type",
            "floor_strike",
            "cap_strike",
            "close_time",
            "settle_yes",
        ]
    ]


def build_labels(
    trades: pd.DataFrame,
    markets: pd.DataFrame,
    spot: pd.DataFrame,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    lookbacks: tuple[int, ...] = STALE_LOOKBACKS,
) -> pd.DataFrame:
    """One row per (non-block) trade with features known at t and markout labels."""
    t = trades.loc[~trades["is_block_trade"].astype(bool)].copy()
    t = t.merge(markets, on="ticker", how="inner", validate="many_to_one")
    t["ts"] = pd.to_datetime(t["created_time"], utc=True, format="ISO8601")
    t = t[t["ts"] < t["close_time"]]  # trades after close are not on a live book
    t = t[t["settle_yes"].notna()]
    t = t.sort_values("ts").reset_index(drop=True)

    price = t["yes_price"].to_numpy() / PRICE_SCALE
    t["price"] = price
    t["qty"] = t["count"].to_numpy() / SIZE_SCALE
    taker_yes = (t["taker_outcome_side"] == "yes").to_numpy()
    t["maker_sign"] = np.where(taker_yes, -1.0, 1.0)
    t["tau_min"] = (t["close_time"] - t["ts"]).dt.total_seconds().to_numpy() / 60.0

    st = t["strike_type"].to_numpy()
    fl, cp = t["floor_strike"].to_numpy(), t["cap_strike"].to_numpy()
    s0, sig = spot_at(spot, t["ts"])
    t["spot"], t["sigma"] = s0, sig
    fv0 = prob_yes(st, fl, cp, s0, t["tau_min"].to_numpy(), sig)
    t["fv"] = fv0
    t["maker_edge"] = t["maker_sign"] * (fv0 - price)  # vs model, at trade time

    # D2 feature: model value move caused by spot over the last k minutes,
    # holding tau and sigma at their trade-time values, signed for the taker.
    taker_dir = -t["maker_sign"].to_numpy()
    for k in lookbacks:
        sk, _ = spot_at(spot, t["ts"] - pd.Timedelta(minutes=k))
        fvk = prob_yes(st, fl, cp, sk, t["tau_min"].to_numpy(), sig)
        t[f"stale_move_{k}m"] = taker_dir * (fv0 - fvk)

    settle = t["settle_yes"].to_numpy()
    t["mo_settle"] = t["maker_sign"] * (settle - price)
    for h in horizons:
        tau_h = t["tau_min"].to_numpy() - h
        sh, _ = spot_at(spot, t["ts"] + pd.Timedelta(minutes=h))
        fvh = prob_yes(st, fl, cp, sh, tau_h, sig)
        fvh = np.where(tau_h <= 0, settle, fvh)  # past close: value is known
        t[f"mo_{h}m"] = t["maker_sign"] * (fvh - price)
        # Value move only, net of any model-vs-price gap at t. This is the
        # adverse-selection part: mo_h == maker_edge + move_h.
        t[f"move_{h}m"] = t["maker_sign"] * (fvh - fv0)

    keep = [
        "trade_id",
        "ticker",
        "event_ticker",
        "ts",
        "close_time",
        "tau_min",
        "strike_type",
        "floor_strike",
        "cap_strike",
        "price",
        "qty",
        "taker_outcome_side",
        "maker_sign",
        "settle_yes",
        "spot",
        "sigma",
        "fv",
        "maker_edge",
    ]
    keep += [f"stale_move_{k}m" for k in lookbacks]
    keep += ["mo_settle"] + [f"mo_{h}m" for h in horizons] + [f"move_{h}m" for h in horizons]
    return t[keep]


# ------------------------------------------------------------------ summary


def _wmean(x: np.ndarray, w: np.ndarray) -> float:
    return float(np.sum(x * w) / np.sum(w)) if np.sum(w) > 0 else float("nan")


def block_bootstrap_ci(
    df: pd.DataFrame, col: str, block: str = "event_ticker", reps: int = 500, seed: int = 7
) -> tuple[float, float, float]:
    """Contract-weighted mean of col with a 95% CI resampling whole events.

    Trades in one event share one spot path, so they are not independent;
    resampling events keeps that correlation inside each draw.
    """
    d = df[[block, col, "qty"]].dropna()
    if d.empty:
        return (float("nan"),) * 3
    g = d.assign(wx=d[col] * d["qty"]).groupby(block)[["wx", "qty"]].sum()
    wx, w = g["wx"].to_numpy(), g["qty"].to_numpy()
    point = float(wx.sum() / w.sum())
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(g), size=(reps, len(g)))
    boots = wx[idx].sum(axis=1) / w[idx].sum(axis=1)
    lo, hi = np.percentile(boots, [2.5, 97.5])
    return point, float(lo), float(hi)


def summarize(lab: pd.DataFrame, horizons=DEFAULT_HORIZONS, n_bins: int = 5) -> dict:
    """Headline numbers in cents per contract, contract-weighted."""
    cols = (
        [f"mo_{h}m" for h in horizons]
        + ["mo_settle", "maker_edge"]
        + [f"move_{h}m" for h in horizons]
    )
    out: dict = {
        "trades": int(len(lab)),
        "contracts": float(lab["qty"].sum()),
        "events": int(lab["event_ticker"].nunique()),
        "markets": int(lab["ticker"].nunique()),
        "trades_with_spot": int(lab["fv"].notna().sum()),
        "markout_cents": {},
        "toxic_share": {},
        "by_stale_move_1m": [],
    }
    for c in cols:
        p, lo, hi = block_bootstrap_ci(lab, c)
        out["markout_cents"][c] = {"mean": 100 * p, "ci95": [100 * lo, 100 * hi]}
        v = lab[[c, "qty"]].dropna()
        out["toxic_share"][c] = _wmean((v[c] < 0).to_numpy(float), v["qty"].to_numpy())

    f = lab.dropna(subset=["stale_move_1m", "mo_5m"])
    if len(f) >= n_bins * 10:
        f = f.assign(bin=pd.qcut(f["stale_move_1m"].rank(method="first"), n_bins, labels=False))
        for b, g in f.groupby("bin"):
            row = {
                "bin": int(b),
                "trades": int(len(g)),
                "stale_move_1m_cents": [
                    100 * float(g["stale_move_1m"].min()),
                    100 * float(g["stale_move_1m"].max()),
                ],
            }
            for c in ("move_1m", "move_5m", "mo_5m", "mo_settle"):
                if c in g:
                    p, lo, hi = block_bootstrap_ci(g, c)
                    row[c] = {"mean": 100 * p, "ci95": [100 * lo, 100 * hi]}
            out["by_stale_move_1m"].append(row)
    return out


# ---------------------------------------------------------------------- CLI


def main() -> int:
    ap = argparse.ArgumentParser(description="M2: build maker markout labels from backfill.")
    ap.add_argument("--backfill", default=str(REPO_ROOT / "data" / "backfill"))
    ap.add_argument("--spot-dir", default=None, help="default: <backfill>/spot")
    ap.add_argument("--series", nargs="+", default=["KXBTC", "KXETH"])
    ap.add_argument("--out", default=str(REPO_ROOT / "data" / "derived"))
    ap.add_argument("--horizons", nargs="+", type=int, default=list(DEFAULT_HORIZONS))
    args = ap.parse_args()

    backfill = Path(args.backfill)
    spot_dir = Path(args.spot_dir) if args.spot_dir else backfill / "spot"
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    horizons = tuple(args.horizons)
    report: dict = {
        "run_at": datetime.now(tz=UTC).isoformat(),
        "backfill": str(backfill),
        "horizons_min": list(horizons),
        "vol_window_min": VOL_WINDOW_MIN,
        "series": {},
    }

    for series in args.series:
        trades = load_trades(backfill, series)
        if trades.empty:
            print(f"{series}: no trades, skipped", flush=True)
            continue
        markets = load_markets(backfill, series)
        spot = prepare_spot(pd.read_parquet(spot_dir / f"{product_for(series)}_1m.parquet"))
        lab = build_labels(trades, markets, spot, horizons)
        lab.to_parquet(out / f"markouts_{series}.parquet", index=False)
        s = summarize(lab, horizons)
        report["series"][series] = s
        mc = s["markout_cents"]
        print(
            f"{series}: {s['trades']:,} trades / {s['events']} events | maker markout "
            + " ".join(f"{k}={v['mean']:+.2f}c" for k, v in mc.items()),
            flush=True,
        )

    (out / "markouts_summary.json").write_text(json.dumps(report, indent=2))
    print(f"wrote {out / 'markouts_summary.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
