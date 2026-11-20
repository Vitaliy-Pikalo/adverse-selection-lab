"""Streaming label comparison, usable without starting or installing Spark."""

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from pandas.testing import assert_frame_equal

# Unchanged from the tolerances declared before the Phase 3 full-data run.
RTOL = 1e-10
ATOL = 1e-12
EXACT_COLUMNS = [
    "trade_id", "ticker", "event_ticker", "ts", "close_time", "strike_type",
    "taker_outcome_side", "maker_sign", "settle_yes", "price", "qty", "spot",
]


def compare_output(expected: pd.DataFrame, path: str, *, batch_size: int = 10000) -> dict:
    """Check schema, exact keys/masks, row multiplicity, and bounded float error.

    Physical row order and pandas/Spark dtype representations may differ. Both
    timestamp columns are compared as UTC instants; no timestamps are rounded.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    expected = expected.set_index("trade_id", drop=False)
    assert expected.index.notna().all(), "Pandas trade IDs contain nulls"
    assert expected.index.is_unique, "Pandas trade IDs are not unique"
    dataset = ds.dataset(path, format="parquet")
    assert dataset.schema.names == expected.columns.tolist(), "Column schema differs"
    seen = np.zeros(len(expected), dtype=bool)
    numeric = [name for name in expected.columns if name not in EXACT_COLUMNS]
    max_error = dict.fromkeys(numeric)
    rows = 0
    for batch in dataset.to_batches(batch_size=batch_size):
        actual = batch.to_pandas()
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
