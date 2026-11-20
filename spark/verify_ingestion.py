"""Compare Phase 2 columns against pandas without writing research outputs.

This is an ingestion/join check, not full label parity or a benchmark. Spark
output is streamed from a temporary Parquet directory to bound comparison memory.
"""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from pandas.testing import assert_frame_equal

from asl import markouts as reference
from spark.ingestion import eligible_trades, load_candles, load_markets, load_trades
from spark.session import create_session
from spark.spot import DEFAULT_OFFSETS, join_spot, minute_grid, spot_column


def pandas_reference(backfill: Path, series: str) -> pd.DataFrame:
    """Use existing pandas loaders and spot lookup semantics as the oracle."""
    trades = reference.load_trades(backfill, series)
    markets = reference.load_markets(backfill, series)
    result = trades.loc[~trades["is_block_trade"].astype(bool)].merge(
        markets, on="ticker", how="inner", validate="many_to_one",
    )
    result["ts"] = pd.to_datetime(result["created_time"], utc=True, format="ISO8601")
    result = result.loc[(result["ts"] < result["close_time"]) & result["settle_yes"].notna()]
    spot = reference.prepare_spot(pd.read_parquet(
        backfill / "spot" / f"{reference.product_for(series)}_1m.parquet",
    ))
    for offset in DEFAULT_OFFSETS:
        values, _ = reference.spot_at(spot, result["ts"] + pd.Timedelta(minutes=offset))
        result[spot_column(offset)] = values
    return result.reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backfill", type=Path, default=Path("data/backfill"))
    parser.add_argument(
        "--series", nargs="+", choices=["KXBTC", "KXETH"], default=["KXBTC", "KXETH"],
    )
    args = parser.parse_args()
    session = create_session("asl-spark-ingestion-check")
    session.sparkContext.setLogLevel("ERROR")
    report = {"stage": "ingestion_and_joins_only", "series": {}}
    try:
        for series in args.series:
            expected = pandas_reference(args.backfill, series).set_index("trade_id", drop=False)
            if not expected.index.is_unique:
                raise AssertionError("Pandas reference trade_id is not unique")
            actual = eligible_trades(
                load_trades(session, args.backfill, series),
                load_markets(session, args.backfill, series),
            )
            candles = load_candles(
                session, args.backfill / "spot" / f"{reference.product_for(series)}_1m.parquet",
            )
            actual = join_spot(actual, minute_grid(candles)).select(*expected.columns)
            seen = np.zeros(len(expected), dtype=bool)
            count = 0
            missing = {spot_column(offset): 0 for offset in DEFAULT_OFFSETS}
            with TemporaryDirectory(prefix="asl-ingestion-check-") as temp:
                path = str(Path(temp) / "joined")
                actual.write.parquet(path)
                for batch in ds.dataset(path, format="parquet").to_batches(batch_size=10000):
                    got = batch.to_pandas()
                    positions = expected.index.get_indexer(got["trade_id"])
                    if (positions < 0).any():
                        raise AssertionError("Spark emitted unknown trade keys")
                    if len(np.unique(positions)) != len(positions) or seen[positions].any():
                        raise AssertionError("Spark emitted duplicate trade keys")
                    want = expected.iloc[positions].reset_index(drop=True)
                    for name in ("ts", "close_time"):
                        got[name] = pd.to_datetime(got[name], utc=True)
                    assert_frame_equal(
                        got.reset_index(drop=True), want,
                        check_dtype=False, check_exact=False, rtol=1e-12, atol=1e-12,
                    )
                    seen[positions] = True
                    count += len(got)
                    for name in missing:
                        missing[name] += int(got[name].isna().sum())
            assert seen.all(), "Spark omitted pandas trade keys"
            assert count == len(expected), "Row counts differ"
            report["series"][series] = {
                "status": "PASS", "rows": count, "columns_compared": len(expected.columns),
                "spot_missing_rows": missing,
            }
            print(json.dumps({series: report["series"][series]}), flush=True)
        print(json.dumps(report, indent=2))
    finally:
        session.stop()


if __name__ == "__main__":
    main()
