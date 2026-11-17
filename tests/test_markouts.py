import math

import numpy as np
import pandas as pd
import pytest

from asl.markouts import block_bootstrap_ci, build_labels, prepare_spot, spot_at, summarize
from asl.pricer import prob_above, prob_yes

T0 = pd.Timestamp("2026-11-01T22:00:00Z")


START = T0 - pd.Timedelta(minutes=120)


def candles(prices, start=START):
    """1-min candles where minute i opens at prices[i] and closes at prices[i+1]."""
    n = len(prices) - 1
    times = pd.date_range(start, periods=n, freq="1min", tz="UTC")
    return pd.DataFrame(
        {
            "time": times,
            "open": prices[:-1],
            "close": prices[1:],
            "low": prices[:-1],
            "high": prices[:-1],
            "volume": 1.0,
        }
    )


def market(
    ticker="M-T100",
    strike_type="greater",
    floor=100.0,
    cap=np.nan,
    close=None,
    result="yes",
    event="EV1",
):
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "event_ticker": event,
                "strike_type": strike_type,
                "floor_strike": floor,
                "cap_strike": cap,
                "close_time": close or T0 + pd.Timedelta(minutes=60),
                "settle_yes": {"yes": 1.0, "no": 0.0}[result],
            }
        ]
    )


def trade(ts, price_ticks, side, count=100, ticker="M-T100", tid="t1", block=False):
    return {
        "trade_id": tid,
        "ticker": ticker,
        "created_time": ts.isoformat().replace("+00:00", "Z"),
        "yes_price": price_ticks,
        "count": count,
        "taker_outcome_side": side,
        "is_block_trade": block,
    }


# ---------------------------------------------------------------- pricer


def test_prob_above_limits():
    assert prob_above(100, 100, 0, 0.01) == 0.0  # at expiry, S == K is not > K
    assert prob_above(101, 100, 0, 0.01) == 1.0
    assert prob_above(1e9, 100, 10, 0.01) == pytest.approx(1.0)
    assert prob_above(1e-3, 100, 10, 0.01) == pytest.approx(0.0)
    # ATM with tiny vol drag is just under 0.5
    p = prob_above(100, 100, 60, 0.001)
    assert 0.49 < p < 0.5


def test_prob_above_matches_closed_form():
    s, k, tau, sig = 100.0, 101.0, 30.0, 0.002
    sd = sig * math.sqrt(tau)
    d2 = (math.log(s / k) - 0.5 * sd * sd) / sd
    want = 0.5 * (1 + math.erf(d2 / math.sqrt(2)))
    assert prob_above(s, k, tau, sig) == pytest.approx(want, rel=1e-12)


def test_prob_yes_strike_types_are_consistent():
    s, tau, sig = 100.0, 30.0, 0.002
    g = prob_yes(np.array(["greater"]), np.array([99.0]), np.array([np.nan]), s, tau, sig)[0]
    lo = prob_yes(np.array(["less"]), np.array([np.nan]), np.array([99.0]), s, tau, sig)[0]
    assert g + lo == pytest.approx(1.0)
    b = prob_yes(np.array(["between"]), np.array([99.0]), np.array([101.0]), s, tau, sig)[0]
    assert b == pytest.approx(prob_above(s, 99, tau, sig) - prob_above(s, 101, tau, sig))
    assert 0 < b < 1


def test_prob_yes_nan_inputs_stay_nan():
    out = prob_yes(np.array(["greater"]), np.array([100.0]), np.array([np.nan]), np.nan, 10, 0.01)
    assert np.isnan(out[0])


# ------------------------------------------------------------------ spot


def test_spot_at_uses_minute_open_no_lookahead():
    sp = prepare_spot(candles(list(range(100, 230))))
    ts = pd.Series([T0 + pd.Timedelta(seconds=59)])  # late in the minute
    s, _ = spot_at(sp, ts)
    # The minute starting at T0 is index 120 -> opens at 220. Its close (221)
    # is in the future relative to ts and must not be used.
    assert s[0] == 220


def test_sigma_excludes_current_minute():
    prices = [100.0] * 100 + [200.0]  # one huge jump on the last candle's close
    sp = prepare_spot(candles(prices), vol_window=20)
    last_minute = sp.index[-1]
    assert sp.loc[last_minute, "sigma"] == pytest.approx(0.0)  # jump not yet known


def test_gaps_become_nan_not_ffill():
    c = candles([100.0 + i for i in range(130)])
    c = c.drop(index=[121])  # remove one minute
    sp = prepare_spot(c)
    s, _ = spot_at(sp, pd.Series([T0 + pd.Timedelta(minutes=1, seconds=5)]))
    assert np.isnan(s[0])


# ---------------------------------------------------------------- labels


def flat_spot(level=100.0, n=200):
    return prepare_spot(candles([level] * n))


def test_maker_sign_and_settlement_markout():
    tr = pd.DataFrame(
        [
            trade(T0 + pd.Timedelta(minutes=1), 4000, "yes", tid="a"),  # taker bought YES @ .40
            trade(T0 + pd.Timedelta(minutes=2), 4000, "no", tid="b"),  # taker bought NO, YES @ .40
        ]
    )
    lab = build_labels(tr, market(result="yes"), flat_spot()).set_index("trade_id")
    # Settled YES: the maker who sold YES at .40 lost .60; the maker long YES gained .60.
    assert lab.loc["a", "maker_sign"] == -1 and lab.loc["a", "mo_settle"] == pytest.approx(-0.60)
    assert lab.loc["b", "maker_sign"] == 1 and lab.loc["b", "mo_settle"] == pytest.approx(0.60)


def test_horizon_past_close_pins_to_settlement():
    close = T0 + pd.Timedelta(minutes=10)
    tr = pd.DataFrame([trade(close - pd.Timedelta(minutes=3), 7000, "yes")])
    lab = build_labels(tr, market(close=close, result="no"), flat_spot(), horizons=(1, 5))
    row = lab.iloc[0]
    assert row["mo_5m"] == pytest.approx(row["mo_settle"]) == pytest.approx(0.70)
    assert row["mo_1m"] != pytest.approx(row["mo_settle"])  # still model-valued


def test_spot_move_after_trade_hurts_maker_who_sold():
    # Spot jumps from 100 to 110 starting at T0+5min; strike 100 'greater'.
    prices = [100.0] * 125 + [110.0] * 80
    sp = prepare_spot(candles(prices))
    tr = pd.DataFrame([trade(T0 + pd.Timedelta(minutes=2), 5000, "yes")])
    lab = build_labels(tr, market(result="yes"), sp, horizons=(1, 5)).iloc[0]
    assert lab["mo_5m"] < lab["mo_1m"] <= 0.01  # maker short YES as spot rallies


def test_stale_move_sign():
    # Spot rallied in the minute before the trade. A taker buying YES is
    # trading with the move (positive stale_move); a taker buying NO, against it.
    prices = [100.0] * 121 + [103.0] * 60
    sp = prepare_spot(candles(prices))
    ts = T0 + pd.Timedelta(minutes=1, seconds=10)
    tr = pd.DataFrame([trade(ts, 5000, "yes", tid="y"), trade(ts, 5000, "no", tid="n")])
    lab = build_labels(tr, market(), sp).set_index("trade_id")
    assert lab.loc["y", "stale_move_1m"] > 0
    assert lab.loc["n", "stale_move_1m"] == pytest.approx(-lab.loc["y", "stale_move_1m"])


def test_block_and_post_close_trades_dropped():
    close = T0 + pd.Timedelta(minutes=30)
    tr = pd.DataFrame(
        [
            trade(T0, 5000, "yes", tid="ok"),
            trade(T0, 5000, "yes", tid="blk", block=True),
            trade(close + pd.Timedelta(seconds=1), 5000, "yes", tid="late"),
            trade(T0, 5000, "yes", tid="orphan", ticker="UNKNOWN"),
        ]
    )
    lab = build_labels(tr, market(close=close), flat_spot())
    assert list(lab["trade_id"]) == ["ok"]


def test_prices_and_sizes_use_fixed_point_scales():
    tr = pd.DataFrame([trade(T0, 2200, "yes", count=350)])
    row = build_labels(tr, market(), flat_spot()).iloc[0]
    assert row["price"] == pytest.approx(0.22) and row["qty"] == pytest.approx(3.5)


# --------------------------------------------------------------- summary


def test_block_bootstrap_weights_by_contracts_and_events():
    df = pd.DataFrame(
        {"event_ticker": ["a", "a", "b"], "x": [1.0, 1.0, -2.0], "qty": [1.0, 1.0, 2.0]}
    )
    p, lo, hi = block_bootstrap_ci(df, "x", block="event_ticker", reps=200)
    assert p == pytest.approx((1 + 1 - 4) / 4)
    assert lo <= p <= hi


def test_summarize_runs_end_to_end():
    rng = np.random.default_rng(0)
    prices = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, 400)))
    sp = prepare_spot(candles(list(prices)))
    rows = []
    for i in range(200):
        ts = T0 + pd.Timedelta(seconds=int(rng.integers(0, 50 * 60)))
        rows.append(
            trade(ts, int(rng.integers(1000, 9000)), rng.choice(["yes", "no"]), tid=f"t{i}")
        )
    lab = build_labels(pd.DataFrame(rows), market(), sp)
    s = summarize(lab)
    assert s["trades"] == 200 and len(s["by_stale_move_1m"]) == 5
    for v in s["markout_cents"].values():
        assert v["ci95"][0] <= v["mean"] <= v["ci95"][1]


def test_markout_decomposes_into_edge_plus_move():
    rng = np.random.default_rng(3)
    prices = 100 * np.exp(np.cumsum(rng.normal(0, 0.002, 300)))
    sp = prepare_spot(candles(list(prices)))
    rows = [
        trade(T0 + pd.Timedelta(minutes=i), 3000 + 100 * i, "yes" if i % 2 else "no", tid=f"t{i}")
        for i in range(40)
    ]
    lab = build_labels(pd.DataFrame(rows), market(), sp, horizons=(1, 5))
    for h in (1, 5):
        np.testing.assert_allclose(
            lab[f"mo_{h}m"], lab["maker_edge"] + lab[f"move_{h}m"], atol=1e-12
        )
