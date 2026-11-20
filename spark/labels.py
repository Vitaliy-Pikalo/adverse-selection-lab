"""Additive maker labels with the same definitions as asl.markouts."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from asl.fixed import PRICE_SCALE, SIZE_SCALE
from asl.markouts import DEFAULT_HORIZONS, STALE_LOOKBACKS, product_for
from spark.ingestion import eligible_trades, load_candles, load_markets, load_trades
from spark.pricer import prob_yes
from spark.spot import join_spot, prepare_spot, spot_column


def build_labels(
    trades: DataFrame, markets: DataFrame, spot: DataFrame,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    lookbacks: tuple[int, ...] = STALE_LOOKBACKS,
) -> DataFrame:
    """Unordered rows keyed by trade_id; sigma always comes from the trade minute."""
    for values in (horizons, lookbacks):
        if len(set(values)) != len(values) or any(type(v) is not int or v <= 0 for v in values):
            raise ValueError("Horizons and lookbacks must be distinct positive integer minutes")
    offsets = (0, *(-k for k in lookbacks), *horizons)
    result = join_spot(eligible_trades(trades, markets), spot, offsets)
    result = result.withColumns({
        "price": F.col("yes_price") / PRICE_SCALE,
        "qty": F.col("count") / SIZE_SCALE,
        "maker_sign": F.when(F.col("taker_outcome_side") == "yes", -1.0).otherwise(1.0),
        "tau_min": (F.unix_micros("close_time") - F.unix_micros("ts")) / 60_000_000.0,
    })

    def fair_value(spot_name: str, tau):
        return prob_yes(
            F.col("strike_type"), F.col("floor_strike"), F.col("cap_strike"),
            F.col(spot_name), tau, F.col("sigma"),
        )

    result = result.withColumn("fv", fair_value("spot", F.col("tau_min")))
    result = result.withColumns({
        "maker_edge": F.col("maker_sign") * (F.col("fv") - F.col("price")),
        "mo_settle": F.col("maker_sign") * (F.col("settle_yes") - F.col("price")),
    })
    for k in lookbacks:
        result = result.withColumn(
            f"stale_move_{k}m",
            -F.col("maker_sign") * (F.col("fv") - fair_value(spot_column(-k), F.col("tau_min"))),
        )
    for h in horizons:
        tau_h = F.col("tau_min") - h
        result = result.withColumn(
            f"_fv_{h}m",
            F.when(tau_h <= 0, F.col("settle_yes"))
            .otherwise(fair_value(spot_column(h), tau_h)),
        ).withColumns({
            f"mo_{h}m": F.col("maker_sign") * (F.col(f"_fv_{h}m") - F.col("price")),
            f"move_{h}m": F.col("maker_sign") * (F.col(f"_fv_{h}m") - F.col("fv")),
        })
    keep = [
        "trade_id", "ticker", "event_ticker", "ts", "close_time", "tau_min", "strike_type",
        "floor_strike", "cap_strike", "price", "qty", "taker_outcome_side", "maker_sign",
        "settle_yes", "spot", "sigma", "fv", "maker_edge",
    ]
    keep += [f"stale_move_{k}m" for k in lookbacks]
    keep += ["mo_settle"] + [f"mo_{h}m" for h in horizons] + [f"move_{h}m" for h in horizons]
    return result.select(*keep)


def load_labels(
    session: SparkSession, backfill: Path, series: str,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
) -> DataFrame:
    """Read an existing local backfill; callers choose if/where to write output."""
    trades = load_trades(session, backfill, series)
    markets = load_markets(session, backfill, series)
    candles = load_candles(session, backfill / "spot" / f"{product_for(series)}_1m.parquet")
    return build_labels(trades, markets, prepare_spot(candles), horizons)
