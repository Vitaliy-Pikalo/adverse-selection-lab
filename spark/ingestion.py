"""Read existing backfill Parquet and reproduce the pandas eligibility join.

Conflicting duplicate keys fail explicitly: Spark file reads do not promise the
file/row precedence used by pandas' keep-first deduplication. Identical duplicate
rows are safe to collapse. No data is collected to pandas in this module.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TRADE_SCHEMA = StructType([
    StructField("trade_id", StringType()),
    StructField("ticker", StringType()),
    StructField("created_time", StringType()),
    StructField("yes_price", LongType()),
    StructField("count", LongType()),
    StructField("taker_outcome_side", StringType()),
    StructField("is_block_trade", BooleanType()),
])
MARKET_SCHEMA = StructType([
    StructField(name, StringType()) for name in (
        "ticker", "event_ticker", "strike_type", "floor_strike", "cap_strike",
        "close_time", "result",
    )
])
CANDLE_SCHEMA = StructType([
    StructField("time", TimestampType()),
    StructField("open", DoubleType()),
    StructField("close", DoubleType()),
])


def identical_duplicates_only(frame: DataFrame, key: str) -> DataFrame:
    """Collapse identical rows; reject ambiguous keep-first selection."""
    distinct = frame.dropDuplicates()
    conflicts = distinct.groupBy(key).count().where(F.col("count") > 1).limit(1)
    if conflicts.take(1):
        raise ValueError(
            f"Conflicting duplicate {key}: pandas uses input-order keep-first; "
            "Spark ingestion requires identical duplicate records."
        )
    return distinct


def load_trades(session: SparkSession, backfill: Path, series: str) -> DataFrame:
    files = sorted((backfill / "trades").glob(f"{series}-*.parquet"))
    if not files:
        return session.createDataFrame([], TRADE_SCHEMA)
    raw = session.read.schema(TRADE_SCHEMA).parquet(*(str(path) for path in files))
    return identical_duplicates_only(raw, "trade_id")


def load_markets(session: SparkSession, backfill: Path, series: str) -> DataFrame:
    raw = session.read.schema(MARKET_SCHEMA).parquet(str(backfill / f"markets_{series}.parquet"))
    # pandas merge(validate="many_to_one") rejects all duplicate market keys,
    # even when the repeated rows are identical.
    if raw.groupBy("ticker").count().where(F.col("count") > 1).take(1):
        raise ValueError("Market ticker must be unique for the many-to-one trade join")
    return raw.select(
        "ticker", "event_ticker", "strike_type",
        F.expr("try_cast(floor_strike as double)").alias("floor_strike"),
        F.expr("try_cast(cap_strike as double)").alias("cap_strike"),
        F.to_timestamp("close_time").alias("close_time"),
        F.when(F.col("result") == "yes", 1.0)
        .when(F.col("result") == "no", 0.0).alias("settle_yes"),
    )


def eligible_trades(trades: DataFrame, markets: DataFrame) -> DataFrame:
    """Drop blocks, unmatched markets, non-live trades, and unknown settlements.

    Output is unordered. trade_id, rather than physical row order, is the parity
    key. Null ticker matching follows pandas merge semantics.
    """
    left = trades.where(~F.coalesce(F.col("is_block_trade"), F.lit(False))).alias("t")
    right = markets.alias("m")
    joined = left.join(right, F.col("t.ticker").eqNullSafe(F.col("m.ticker")), "inner")
    return (
        joined.select("t.*", *(F.col(f"m.{name}") for name in markets.columns if name != "ticker"))
        .withColumn("ts", F.to_timestamp("created_time"))
        .where((F.col("ts") < F.col("close_time")) & F.col("settle_yes").isNotNull())
    )


def load_candles(session: SparkSession, path: Path) -> DataFrame:
    raw = session.read.schema(CANDLE_SCHEMA).parquet(str(path))
    return identical_duplicates_only(raw, "time")
