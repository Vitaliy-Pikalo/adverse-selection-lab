"""Window, pricing, and no-lookahead cases for the additive Spark labels."""

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from asl import markouts as reference  # noqa: E402
from asl.pricer import prob_yes as pandas_prob_yes  # noqa: E402
from spark.labels import build_labels  # noqa: E402
from spark.pricer import prob_yes  # noqa: E402
from spark.session import create_session  # noqa: E402
from spark.spot import prepare_spot  # noqa: E402
from tests.test_markouts import T0, candles, market, trade  # noqa: E402

RTOL = 1e-10
ATOL = 1e-12


@pytest.fixture(scope="module")
def session():
    result = create_session("asl-label-tests")
    result.sparkContext.setLogLevel("ERROR")
    yield result
    result.stop()


def spark_labels(session, trades, markets, raw_spot):
    return build_labels(
        session.createDataFrame(trades), session.createDataFrame(markets),
        prepare_spot(session.createDataFrame(raw_spot)),
    ).toPandas().sort_values("trade_id").reset_index(drop=True)


@pytest.mark.parametrize("window", [20, 60])
def test_sigma_matches_pandas_warmup_and_missing_minutes(session, window):
    prices = 100 * np.exp(np.cumsum(np.random.default_rng(12).normal(0, 0.002, 230)))
    raw = candles(prices).drop(index=[70, 72, 121])
    expected = reference.prepare_spot(raw, vol_window=window).reset_index(names="minute")
    actual = prepare_spot(session.createDataFrame(raw), vol_window=window).toPandas()
    actual = actual.sort_values("minute").reset_index(drop=True)
    actual["minute"] = pd.to_datetime(actual["minute"], utc=True)
    pd.testing.assert_frame_equal(
        actual, expected, check_dtype=False, check_exact=False, rtol=RTOL, atol=ATOL,
    )
    assert actual.sigma.isna().tolist() == expected.sigma.isna().tolist()
    assert actual.loc[window // 2, "sigma"] is None or pd.isna(actual.loc[window // 2, "sigma"])
    assert pd.notna(actual.loc[window // 2 + 1, "sigma"])


def test_pricer_strike_types_expiry_and_invalid_inputs(session):
    rows = [
        ("greater", 100.0, None, 101.0, 30.0, 0.002),
        ("less", None, 100.0, 101.0, 30.0, 0.002),
        ("between", 99.0, 101.0, 100.0, 30.0, 0.002),
        ("greater", 100.0, None, 100.0, 0.0, 0.002),
        ("greater", 100.0, None, 101.0, -1.0, None),
        ("greater", 100.0, None, 100.0, 10.0, 0.0),
        ("greater", 100.0, None, 100.0, 10.0, -1.0),
        ("greater", 100.0, None, 100.0, 10.0, None),
        ("greater", 100.0, None, 100.0, 10.0, float("nan")),
        ("greater", 100.0, None, 100.0, 10.0, float("inf")),
        ("greater", 0.0, None, 100.0, 10.0, 0.002),
        ("greater", None, None, 100.0, 10.0, 0.002),
        ("greater", 100.0, None, float("inf"), 10.0, 0.002),
        ("greater", 100.0, None, float("nan"), 10.0, 0.002),
        ("greater", 100.0, None, 0.0, 10.0, 0.002),
        ("greater", 100.0, None, 101.0, None, 0.002),
        ("unknown", 100.0, None, 100.0, 10.0, 0.002),
    ]
    raw = session.createDataFrame(
        [(i, *row) for i, row in enumerate(rows)],
        "id long, kind string, floor double, cap double, spot double, tau double, sigma double",
    )
    actual = raw.select("id", prob_yes(*(F.col(name) for name in (
        "kind", "floor", "cap", "spot", "tau", "sigma",
    ))).alias("p")).orderBy("id").toPandas().p.to_numpy(dtype=float)
    columns = list(zip(*rows, strict=True))
    expected = pandas_prob_yes(*columns)
    np.testing.assert_array_equal(np.isnan(actual), np.isnan(expected))
    np.testing.assert_allclose(actual, expected, rtol=RTOL, atol=ATOL, equal_nan=True)


def test_labels_match_pandas_for_mixed_contracts_and_missing_spot(session):
    prices = 100 * np.exp(np.cumsum(np.random.default_rng(7).normal(0, 0.002, 230)))
    raw = candles(prices).drop(index=[121, 130])
    markets = pd.concat([
        market(ticker="G", floor=99.0),
        market(ticker="L", strike_type="less", floor=np.nan, cap=101.0, result="no"),
        market(ticker="B", strike_type="between", floor=99.0, cap=101.0),
    ], ignore_index=True)
    trades = pd.DataFrame([
        trade(
            T0 + pd.Timedelta(minutes=i, microseconds=123456), 2200 + i, side,
            count=350, ticker=ticker, tid=f"{ticker}-{side}-{i}",
        )
        for ticker in ("G", "L", "B") for side in ("yes", "no")
        for i in (0, 1, 2, 5, 10, 58, 59)
    ])
    expected = reference.build_labels(trades, markets, reference.prepare_spot(raw))
    expected = expected.sort_values("trade_id").reset_index(drop=True)
    actual = spark_labels(session, trades, markets, raw)
    for name in ("ts", "close_time"):
        actual[name] = pd.to_datetime(actual[name], utc=True)
    pd.testing.assert_frame_equal(actual.isna(), expected.isna())
    pd.testing.assert_frame_equal(
        actual, expected, check_dtype=False, check_exact=False, rtol=RTOL, atol=ATOL,
    )
    assert actual.qty.eq(3.5).all()
    for horizon in (1, 5, 15):
        # Future markouts can exist when fv(t) is missing. The decomposition
        # requires the trade-time edge and subsequent value move to be defined.
        defined = actual[["maker_edge", f"move_{horizon}m"]].notna().all(axis=1)
        np.testing.assert_allclose(
            actual.loc[defined, f"mo_{horizon}m"],
            actual.loc[defined, "maker_edge"] + actual.loc[defined, f"move_{horizon}m"],
            rtol=RTOL, atol=ATOL, equal_nan=True,
        )
    assert (actual.mo_1m.notna() & actual.fv.isna()).any()


def test_future_candles_cannot_change_trade_time_features(session):
    raw = candles(100 * np.exp(np.cumsum(np.random.default_rng(3).normal(0, 0.001, 220))))
    changed = raw.copy()
    # Trade minute is 122. Its open is known, its close and later opens are not.
    changed.loc[122:, "close"] *= 2.0
    changed.loc[123:, "open"] *= 2.0
    trades = pd.DataFrame([trade(T0 + pd.Timedelta(minutes=2, seconds=30), 5000, "yes")])
    original = spark_labels(session, trades, market(), raw).iloc[0]
    mutated = spark_labels(session, trades, market(), changed).iloc[0]
    features = ["spot", "sigma", "fv", "maker_edge", "stale_move_1m", "stale_move_5m"]
    np.testing.assert_allclose(
        original[features].to_numpy(float), mutated[features].to_numpy(float),
        rtol=RTOL, atol=ATOL,
    )
    assert original.mo_5m != pytest.approx(mutated.mo_5m)


def test_horizon_at_close_pins_without_future_spot_or_sigma(session):
    # Only three candles: sigma is unavailable, and future spot is out of range.
    raw = candles([100.0, 101.0, 102.0, 103.0], start=T0)
    trades = pd.DataFrame([trade(T0 + pd.Timedelta(minutes=2), 7000, "yes")])
    actual = spark_labels(
        session, trades, market(close=T0 + pd.Timedelta(minutes=7), result="no"), raw,
    ).iloc[0]
    assert pd.isna(actual.fv) and pd.isna(actual.mo_1m)
    assert actual.mo_5m == actual.mo_15m == actual.mo_settle == pytest.approx(0.7)
    assert pd.isna(actual.move_5m)
