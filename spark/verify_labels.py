"""Phase 3 diagnostic: compare labels to pandas; the pytest parity gate is Phase 4."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from pandas.testing import assert_frame_equal

from asl import markouts as reference
from spark.labels import load_labels
from spark.session import create_session

# Declared before full-data execution, not fitted to observed discrepancies.
RTOL = 1e-10
ATOL = 1e-12
EXACT_COLUMNS = [
    "trade_id", "ticker", "event_ticker", "ts", "close_time", "strike_type",
    "taker_outcome_side", "maker_sign", "settle_yes", "price", "qty", "spot",
]


def compare_output(expected: pd.DataFrame, path: str) -> dict:
    """Stream Parquet batches, checking exact keys/masks and bounded float error."""
    expected = expected.set_index("trade_id", drop=False)
    if not expected.index.is_unique:
        raise AssertionError("Pandas trade IDs are not unique")
    seen = np.zeros(len(expected), dtype=bool)
    numeric = [name for name in expected.columns if name not in EXACT_COLUMNS]
    max_error = dict.fromkeys(numeric)
    rows = 0
    for batch in ds.dataset(path, format="parquet").to_batches(batch_size=10000):
        actual = batch.to_pandas()
        assert actual.columns.tolist() == expected.columns.tolist(), "Column schema differs"
        positions = expected.index.get_indexer(actual.trade_id)
        assert (positions >= 0).all(), "Unknown Spark trade IDs"
        assert len(np.unique(positions)) == len(positions), "Duplicate trade IDs in batch"
        assert not seen[positions].any(), "Duplicate trade IDs across batches"
        want = expected.iloc[positions].reset_index(drop=True)
        for name in ("ts", "close_time"):
            actual[name] = pd.to_datetime(actual[name], utc=True)
        assert_frame_equal(actual.isna(), want.isna())
        assert_frame_equal(
            actual[EXACT_COLUMNS], want[EXACT_COLUMNS], check_dtype=False, check_exact=True,
        )
        assert_frame_equal(
            actual[numeric], want[numeric], check_dtype=False,
            check_exact=False, rtol=RTOL, atol=ATOL,
        )
        for name in numeric:
            got = actual[name].to_numpy(dtype=float)
            target = want[name].to_numpy(dtype=float)
            finite = np.isfinite(got) & np.isfinite(target)
            if finite.any():
                error = float(np.abs(got[finite] - target[finite]).max())
                max_error[name] = max(max_error[name] or 0.0, error)
        seen[positions] = True
        rows += len(actual)
    assert seen.all() and rows == len(expected), "Missing Spark trade IDs"
    return {
        "status": "PASS", "rows": rows, "columns": len(expected.columns),
        "rtol": RTOL, "atol": ATOL, "max_abs_error": max_error,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backfill", type=Path, default=Path("data/backfill"))
    parser.add_argument(
        "--series", nargs="+", choices=["KXBTC", "KXETH"], default=["KXBTC", "KXETH"],
    )
    args = parser.parse_args()
    session = create_session("asl-label-diagnostic")
    session.sparkContext.setLogLevel("ERROR")
    report = {"stage": "phase_3_label_diagnostic", "series": {}}
    try:
        for series in args.series:
            print(f"{series}: computing pandas reference", flush=True)
            spot = reference.prepare_spot(pd.read_parquet(
                args.backfill / "spot" / f"{reference.product_for(series)}_1m.parquet",
            ))
            expected = reference.build_labels(
                reference.load_trades(args.backfill, series),
                reference.load_markets(args.backfill, series), spot,
            )
            print(f"{series}: computing Spark labels", flush=True)
            actual = load_labels(session, args.backfill, series)
            with TemporaryDirectory(prefix="asl-label-diagnostic-") as temp:
                path = str(Path(temp) / "labels")
                actual.write.parquet(path)
                report["series"][series] = compare_output(expected, path)
            print(json.dumps({series: report["series"][series]}), flush=True)
        print(json.dumps(report, indent=2))
    finally:
        session.stop()


if __name__ == "__main__":
    main()
