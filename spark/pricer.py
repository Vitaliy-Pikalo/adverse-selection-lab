"""Spark expressions for the existing log-normal binary probability formula."""

import math

import numpy as np
import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from asl.pricer import SIGMA_FLOOR


@F.pandas_udf(DoubleType())
def norm_cdf(values: pd.Series) -> pd.Series:
    """Arrow batches cross Python/JVM only for the missing native erf function."""
    scaled = values.to_numpy(dtype=float) / math.sqrt(2.0)
    return pd.Series(0.5 * (1.0 + np.fromiter(
        (math.erf(value) for value in scaled), dtype=float, count=len(scaled),
    )))


def prob_above(spot: Column, strike: Column, tau: Column, sigma: Column) -> Column:
    valid = (
        spot.isNotNull() & strike.isNotNull() & ~F.isnan(spot) & ~F.isnan(strike)
        & (spot > 0) & (strike > 0)
        & (F.abs(spot) != float("inf")) & (F.abs(strike) != float("inf"))
    )
    live = (tau > 0) & ~F.isnan(tau)
    # greatest skips nulls, whereas numpy.maximum propagates missing sigma.
    sig = F.when(sigma.isNotNull() & ~F.isnan(sigma), F.greatest(sigma, F.lit(SIGMA_FLOOR)))
    sd = sig * F.sqrt(tau)
    d2 = (F.log(spot / strike) - 0.5 * sd * sd) / sd
    # Python UDFs can run outside a surrounding CASE. Guard their input so
    # expired rows and invalid strikes never evaluate unsafe log/division.
    cdf = norm_cdf(F.when(valid & live, d2))
    expired = F.when(spot > strike, 1.0).otherwise(0.0)
    return F.when(valid, F.when(live, cdf).otherwise(expired))


def prob_yes(
    strike_type: Column, floor: Column, cap: Column,
    spot: Column, tau: Column, sigma: Column,
) -> Column:
    above_floor = prob_above(spot, floor, tau, sigma)
    above_cap = prob_above(spot, cap, tau, sigma)
    raw = (
        F.when(strike_type == "greater", above_floor)
        .when(strike_type == "less", 1.0 - above_cap)
        .when(strike_type == "between", above_floor - above_cap)
    )
    # Spark least/greatest skip nulls, so preserve the missing-value mask first.
    return F.when(raw.isNotNull() & ~F.isnan(raw), F.least(F.lit(1.0), F.greatest(F.lit(0.0), raw)))
