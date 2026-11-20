"""A routine synthetic parity gate plus opt-in, count-pinned full-data parity."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import pytest

from asl import markouts as reference
from spark.parity import ATOL, compare_output
from tests.test_markouts import T0, candles, market, trade

FULL_ROWS = {"KXBTC": 1010733, "KXETH": 110811}
FULL_FILES = {"KXBTC": 704, "KXETH": 693}


@pytest.fixture(scope="module")
def session():
    pytest.importorskip("pyspark", reason="Spark integration needs the isolated Spark environment")
    from spark.session import create_session

    result = create_session("asl-pytest-parity")
    result.sparkContext.setLogLevel("ERROR")
    yield result
    result.stop()


@pytest.fixture
def small_labels():
    """Pandas oracle for deliberate output corruptions; no Spark dependency."""
    raw = candles(100 * np.exp(np.cumsum(np.random.default_rng(5).normal(0, 0.001, 220))))
    trades = pd.DataFrame([
        trade(T0, 2200, "yes", tid="a"),
        trade(T0 + pd.Timedelta(minutes=1), 6500, "no", tid="b"),
    ])
    return reference.build_labels(trades, market(), reference.prepare_spot(raw))


@pytest.mark.parametrize("corruption", [
    "missing_row", "duplicate_in_batch", "duplicate_across_batches", "unknown_key",
    "null_key", "missing_column", "float_value", "missing_value_mask", "exact_price",
    "timestamp", "near_zero_float",
])
def test_comparator_rejects_corrupted_output(small_labels, tmp_path, corruption):
    expected = small_labels.copy()
    actual = expected.copy()
    batch_size = 10000
    if corruption == "missing_row":
        actual = actual.iloc[:1]
    elif corruption in ("duplicate_in_batch", "duplicate_across_batches"):
        actual = pd.concat([actual, actual.iloc[:1]], ignore_index=True)
        if corruption == "duplicate_across_batches":
            batch_size = 1
    elif corruption == "unknown_key":
        actual.loc[0, "trade_id"] = "unknown"
    elif corruption == "null_key":
        actual.loc[0, "trade_id"] = None
    elif corruption == "missing_column":
        actual = actual.drop(columns="move_15m")
    elif corruption == "float_value":
        actual.loc[0, "mo_5m"] += 0.001
    elif corruption == "missing_value_mask":
        actual.loc[0, "fv"] = np.nan
    elif corruption == "exact_price":
        actual.loc[0, "price"] += ATOL / 10
    elif corruption == "timestamp":
        actual.loc[0, "ts"] += pd.Timedelta(microseconds=1)
    elif corruption == "near_zero_float":
        expected.loc[0, "stale_move_1m"] = 0.0
        actual.loc[0, "stale_move_1m"] = 2 * ATOL
    path = tmp_path / "corrupted.parquet"
    actual.to_parquet(path, index=False)
    with pytest.raises(AssertionError):
        compare_output(expected, str(path), batch_size=batch_size)


def test_comparator_accepts_reordering_and_small_float_error(small_labels, tmp_path):
    actual = small_labels.iloc[::-1].reset_index(drop=True)
    actual.loc[0, "mo_5m"] += ATOL / 2
    path = tmp_path / "reordered.parquet"
    actual.to_parquet(path, index=False)
    report = compare_output(small_labels, str(path), batch_size=1)
    assert report["rows"] == 2 and report["columns"] == 27
    assert 0 < report["max_abs_error"]["mo_5m"] < ATOL


def test_empty_output_still_requires_the_complete_schema(small_labels, tmp_path):
    expected = small_labels.iloc[:0]
    path = tmp_path / "empty.parquet"
    expected.drop(columns="move_15m").to_parquet(path, index=False)
    with pytest.raises(AssertionError, match="Column schema differs"):
        compare_output(expected, str(path))


@pytest.fixture
def sample_backfill(tmp_path):
    """A generated, deterministic synthetic sample, never a research-data subset."""
    (tmp_path / "trades").mkdir()
    (tmp_path / "spot").mkdir()
    prices = 100 * np.exp(np.cumsum(np.random.default_rng(17).normal(0, 0.002, 240)))
    raw = candles(prices).drop(index=[121, 130])
    pd.concat([raw, raw.iloc[:1]]).to_parquet(
        tmp_path / "spot" / "BTC-USD_1m.parquet", index=False,
    )
    markets = pd.concat([
        market(ticker="G", floor=99.0),
        market(ticker="L", strike_type="less", floor=np.nan, cap=101.0, result="no"),
        market(ticker="B", strike_type="between", floor=99.0, cap=101.0),
    ], ignore_index=True)
    markets["result"] = markets.pop("settle_yes").map({1.0: "yes", 0.0: "no"})
    markets["close_time"] = markets["close_time"].map(lambda ts: ts.isoformat())
    markets[["floor_strike", "cap_strike"]] = markets[["floor_strike", "cap_strike"]].astype(str)
    markets = pd.concat([
        markets, markets.iloc[:1].assign(ticker="U", result=""),
    ], ignore_index=True)
    markets.to_parquet(tmp_path / "markets_KXBTC.parquet", index=False)
    rows = [
        trade(
            T0 + pd.Timedelta(minutes=i, microseconds=123456), 2200 + i, side,
            count=350, ticker=ticker, tid=f"{ticker}-{side}-{i}",
        )
        for ticker in ("G", "L", "B") for side in ("yes", "no")
        for i in (0, 1, 2, 5, 10, 44, 58, 59)
    ]
    rows.append(trade(T0 - pd.Timedelta(minutes=110), 5000, "yes", ticker="G", tid="warmup"))
    base = rows[0]
    rows += [
        dict(base), {**base, "trade_id": "block", "is_block_trade": True},
        {
            **base, "trade_id": "at_close",
            "created_time": (T0 + pd.Timedelta(minutes=60)).isoformat(),
        },
        {**base, "trade_id": "late", "created_time": (T0 + pd.Timedelta(minutes=61)).isoformat()},
        {**base, "trade_id": "orphan", "ticker": "UNKNOWN"},
        {**base, "trade_id": "unsettled", "ticker": "U"},
    ]
    pd.DataFrame(rows).to_parquet(tmp_path / "trades" / "KXBTC-SAMPLE.parquet", index=False)
    return tmp_path


def check_dataset(session, backfill: Path, series: str, rows: int, scope: str) -> dict:
    from spark.labels import load_labels

    print(f"{scope} {series}: computing pandas reference", flush=True)
    spot = reference.prepare_spot(pd.read_parquet(
        backfill / "spot" / f"{reference.product_for(series)}_1m.parquet",
    ))
    expected = reference.build_labels(
        reference.load_trades(backfill, series), reference.load_markets(backfill, series), spot,
    )
    assert len(expected) == rows, f"{scope} {series}: expected {rows} audited label rows"
    assert len(expected.columns) == 27
    print(f"{scope} {series}: computing Spark labels", flush=True)
    actual = load_labels(session, backfill, series)
    with TemporaryDirectory(prefix="asl-pytest-parity-") as temp:
        path = str(Path(temp) / "labels")
        actual.write.parquet(path)
        report = compare_output(expected, path)
    print("PARITY_RESULT " + json.dumps({"scope": scope, "series": series, **report}), flush=True)
    return report


def test_sample_pipeline_parity(session, sample_backfill):
    report = check_dataset(session, sample_backfill, "KXBTC", 49, "synthetic_sample")
    assert report["status"] == "PASS"


@pytest.mark.spark_full
@pytest.mark.parametrize("series", FULL_ROWS)
def test_full_dataset_parity(request, series):
    if not request.config.getoption("--spark-full-data"):
        pytest.skip("Pass --spark-full-data to run the audited hourly dataset")
    backfill = Path(request.config.getoption("--spark-backfill"))
    if not backfill.is_absolute():
        backfill = request.config.rootpath / backfill
    required = [
        backfill / f"markets_{series}.parquet",
        backfill / "spot" / f"{reference.product_for(series)}_1m.parquet",
    ]
    assert all(path.is_file() for path in required), f"Full backfill missing at {backfill}"
    files = list((backfill / "trades").glob(f"{series}-*.parquet"))
    assert len(files) == FULL_FILES[series], f"{series}: expected {FULL_FILES[series]} trade files"
    session = request.getfixturevalue("session")
    report = check_dataset(session, backfill, series, FULL_ROWS[series], "full_hourly_dataset")
    assert report["rows"] == FULL_ROWS[series]
