"""Complete UTC minute grids and exact minute-open lookups; no forward fill."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DEFAULT_OFFSETS = (0, -1, -5, 1, 5, 15)


def spot_column(offset: int) -> str:
    if offset == 0:
        return "spot"
    direction = "minus" if offset < 0 else "plus"
    return f"spot_{direction}_{abs(offset)}m"


def minute_grid(candles: DataFrame) -> DataFrame:
    """Preserve missing candle minutes as nulls, including their close prices."""
    bounds = candles.agg(F.min("time").alias("start"), F.max("time").alias("end"))
    grid = bounds.select(F.explode(F.sequence("start", "end", F.expr("INTERVAL 1 MINUTE")))
                         .alias("minute"))
    return grid.join(candles, grid.minute == candles.time, "left").select(
        "minute", "open", "close",
    )


def prepare_spot(candles: DataFrame, vol_window: int = 60) -> DataFrame:
    """One product: sample stddev of prior minute returns, with no lookahead.

    The complete minute grid makes row offsets equal minute offsets. A missing
    close invalidates both its return and the next return, as in numpy.diff.
    This ordered window operates on the small spot table, not the trade table.
    """
    if type(vol_window) is not int or vol_window < 1:
        raise ValueError("vol_window must be a positive integer")
    order = Window.orderBy("minute")
    prior = order.rowsBetween(-vol_window, -1)
    grid = minute_grid(candles).withColumn("_log_close", F.log("close"))
    grid = grid.withColumn("_return", F.col("_log_close") - F.lag("_log_close").over(order))
    finite = F.col("_return").isNotNull() & ~F.isnan("_return") & (
        F.abs(F.col("_return")) != F.lit(float("inf"))
    )
    grid = grid.withColumn("_return", F.when(finite, F.col("_return")))
    return grid.select(
        "minute", "open", "close",
        F.when(
            F.count("_return").over(prior) >= vol_window // 2,
            F.stddev_samp("_return").over(prior),
        ).alias("sigma"),
    )


def join_spot(
    trades: DataFrame, spot: DataFrame, offsets: tuple[int, ...] = DEFAULT_OFFSETS,
) -> DataFrame:
    """Left equality joins at floor(ts + offset minutes); horizons may use future spot.

    Offsets below zero are historical features; positive offsets are future label
    inputs. Keeping distinct column names prevents accidental substitution.
    """
    if len(set(offsets)) != len(offsets) or any(type(offset) is not int for offset in offsets):
        raise ValueError("Offsets must be distinct integer minutes")
    result = trades
    for offset in offsets:
        value = spot_column(offset)
        lookup = F.broadcast(spot.select(
            F.col("minute").alias("_spot_minute"), F.col("open").alias(value),
            *([F.col("sigma")] if offset == 0 and "sigma" in spot.columns else []),
        ))
        target = F.date_trunc("minute", F.col("ts") + F.expr(f"INTERVAL {offset} MINUTES"))
        result = result.join(lookup, target == F.col("_spot_minute"), "left").drop("_spot_minute")
    return result
