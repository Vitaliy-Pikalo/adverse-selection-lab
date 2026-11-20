"""Verify JVM execution, Python workers, and temporary Parquet round trips."""

import json
import platform
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import pyarrow
from pyspark.sql import functions as F

from spark.session import create_session


def main() -> None:
    session = create_session("asl-spark-smoke")
    session.sparkContext.setLogLevel("ERROR")
    try:
        rows = session.range(1000)
        total = rows.agg(F.sum("id").alias("total")).first()["total"]
        assert total == 499500, total
        # Exercise Python worker startup too; JVM-only expressions cannot check it.
        worker_total = session.sparkContext.parallelize(range(10), 2).map(lambda x: x * x).sum()
        assert worker_total == 285, worker_total
        with TemporaryDirectory(prefix="asl-spark-smoke-") as temp:
            path = str(Path(temp) / "roundtrip.parquet")
            rows.write.parquet(path)
            restored = session.read.parquet(path)
            assert restored.exceptAll(rows).count() == 0
            assert rows.exceptAll(restored).count() == 0
        print(json.dumps({
            "status": "PASS",
            "python": platform.python_version(),
            "spark": session.version,
            "java": session.sparkContext._jvm.java.lang.System.getProperty("java.version"),
            "pandas": pd.__version__,
            "numpy": np.__version__,
            "pyarrow": pyarrow.__version__,
            "master": session.sparkContext.master,
            "timezone": session.conf.get("spark.sql.session.timeZone"),
            "rows": 1000,
            "sum_id": total,
            "python_worker_sum_squares": worker_total,
            "parquet_roundtrip": "PASS",
        }, indent=2))
    finally:
        session.stop()


if __name__ == "__main__":
    main()
