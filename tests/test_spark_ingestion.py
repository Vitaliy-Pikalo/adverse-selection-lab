"""Phase 2 boundary cases; full label parity belongs to Phase 4."""

import pandas as pd
import pytest

pytest.importorskip("pyspark")

from spark.ingestion import (  # noqa: E402
    eligible_trades,
    load_candles,
    load_markets,
    load_trades,
)
from spark.session import create_session  # noqa: E402
from spark.spot import join_spot, minute_grid  # noqa: E402
from spark.verify_ingestion import pandas_reference  # noqa: E402


@pytest.fixture(scope="module")
def session():
    result = create_session("asl-ingestion-tests")
    result.sparkContext.setLogLevel("ERROR")
    yield result
    result.stop()


@pytest.fixture
def backfill(tmp_path):
    (tmp_path / "trades").mkdir()
    (tmp_path / "spot").mkdir()
    markets = pd.DataFrame([{
        "ticker": "M", "event_ticker": "E", "strike_type": "greater",
        "floor_strike": "100", "cap_strike": "None",
        "close_time": "2026-11-01T12:03:00Z", "result": "yes",
    }, {
        "ticker": "UNKNOWN_RESULT", "event_ticker": "E", "strike_type": "greater",
        "floor_strike": "bad", "cap_strike": "None",
        "close_time": "2026-11-01T12:03:00Z", "result": "",
    }])
    markets.to_parquet(tmp_path / "markets_KXBTC.parquet", index=False)
    base = {
        "trade_id": "live", "ticker": "M", "created_time": "2026-11-01T12:00:59.999999Z",
        "yes_price": 2200, "count": 350, "taker_outcome_side": "yes", "is_block_trade": False,
    }
    rows = [
        base, dict(base),
        {**base, "trade_id": "gap", "created_time": "2026-11-01T12:01:00Z"},
        {**base, "trade_id": "block", "is_block_trade": True},
        {**base, "trade_id": "at_close", "created_time": "2026-11-01T12:03:00Z"},
        {**base, "trade_id": "post_close", "created_time": "2026-11-01T12:03:00.000001Z"},
        {**base, "trade_id": "orphan", "ticker": "MISSING"},
        {**base, "trade_id": "unsettled", "ticker": "UNKNOWN_RESULT"},
    ]
    pd.DataFrame(rows).to_parquet(tmp_path / "trades" / "KXBTC-E.parquet", index=False)
    # Missing 12:01, and closes deliberately different from opens.
    candles = pd.DataFrame({
        "time": pd.to_datetime([
            "2026-11-01T11:59:00Z", "2026-11-01T12:00:00Z", "2026-11-01T12:02:00Z",
        ], utc=True),
        "open": [99.0, 100.0, 102.0], "close": [999.0, 1000.0, 1002.0],
    })
    candles.to_parquet(tmp_path / "spot" / "BTC-USD_1m.parquet", index=False)
    return tmp_path


def test_minute_joins_match_pandas_at_boundaries_and_gaps(session, backfill):
    expected = pandas_reference(backfill, "KXBTC").sort_values("trade_id").reset_index(drop=True)
    trades = load_trades(session, backfill, "KXBTC")
    markets = load_markets(session, backfill, "KXBTC")
    candles = load_candles(session, backfill / "spot" / "BTC-USD_1m.parquet")
    spot = minute_grid(candles)
    grid = {row.minute.minute: row.open for row in spot.collect()}
    assert grid == {59: 99.0, 0: 100.0, 1: None, 2: 102.0}
    actual = join_spot(eligible_trades(trades, markets), spot).toPandas()
    actual = actual[expected.columns].sort_values("trade_id").reset_index(drop=True)
    for name in ("ts", "close_time"):
        actual[name] = pd.to_datetime(actual[name], utc=True)
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)
    assert actual.trade_id.tolist() == ["gap", "live"]
    assert actual.loc[1, "spot"] == 100.0
    assert pd.isna(actual.loc[0, "spot"])
    assert pd.isna(actual.loc[1, "spot_plus_1m"])


def test_conflicting_trade_duplicates_fail_instead_of_selecting_arbitrarily(session, backfill):
    path = backfill / "trades" / "KXBTC-E.parquet"
    rows = pd.read_parquet(path)
    rows.loc[1, "yes_price"] = 9900
    rows.to_parquet(path, index=False)
    with pytest.raises(ValueError, match="Conflicting duplicate trade_id"):
        load_trades(session, backfill, "KXBTC")


def test_market_join_rejects_even_identical_duplicate_keys(session, backfill):
    path = backfill / "markets_KXBTC.parquet"
    rows = pd.read_parquet(path)
    pd.concat([rows, rows.iloc[:1]]).to_parquet(path, index=False)
    with pytest.raises(ValueError, match="many-to-one"):
        load_markets(session, backfill, "KXBTC")


def test_conflicting_candle_duplicates_fail(session, backfill):
    path = backfill / "spot" / "BTC-USD_1m.parquet"
    rows = pd.read_parquet(path)
    repeated = rows.iloc[:1].assign(open=123.0)
    pd.concat([rows, repeated]).to_parquet(path, index=False)
    with pytest.raises(ValueError, match="Conflicting duplicate time"):
        load_candles(session, path)


def test_no_trade_files_returns_typed_empty_frame(session, backfill):
    result = load_trades(session, backfill, "KXETH")
    assert result.count() == 0
    assert result.schema["yes_price"].dataType.simpleString() == "bigint"


def test_trade_reader_excludes_unrelated_series_before_parquet_read(session, backfill):
    # A corrupt other-series file must not even be opened by this reader.
    (backfill / "trades" / "KXETH-unrelated.parquet").write_bytes(b"not parquet")
    result = load_trades(session, backfill, "KXBTC")
    assert result.count() == 7  # eight physical rows, including an identical duplicate


def test_trade_reader_preserves_top_level_glob_with_nested_directories(session, backfill):
    nested = backfill / "trades" / "nested"
    nested.mkdir()
    rows = pd.read_parquet(backfill / "trades" / "KXBTC-E.parquet").iloc[:1].copy()
    rows["trade_id"] = "nested-should-not-appear"
    rows.to_parquet(nested / "KXBTC-nested.parquet", index=False)
    result = load_trades(session, backfill, "KXBTC")
    ids = {row.trade_id for row in result.select("trade_id").collect()}
    assert "nested-should-not-appear" not in ids
