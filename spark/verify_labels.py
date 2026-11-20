"""Compare labels to pandas interactively using the same comparator as pytest."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from asl import markouts as reference
from spark.labels import load_labels
from spark.parity import compare_output
from spark.session import create_session


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
