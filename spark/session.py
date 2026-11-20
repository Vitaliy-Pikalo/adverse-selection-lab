"""A local-only Spark session for the portfolio port."""

import os
import sys

from pyspark.sql import SparkSession


def create_session(app_name: str = "asl-spark-port") -> SparkSession:
    """Use all local logical CPUs, UTC timestamps, and this Python for workers."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
