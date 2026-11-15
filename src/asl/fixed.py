"""Exact fixed-point parsing for Kalshi price/size strings.

Kalshi sends prices as dollar strings with 4 decimals ("0.2200") and sizes as
fixed-point strings with 2 decimals ("300.00"). We store both as integers to
avoid float drift when deltas are summed over millions of messages:

  price -> ticks of $0.0001  ("0.2200" -> 2200; $1.00 == PRICE_ONE)
  size  -> hundredths of a contract ("300.00" -> 30000)
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

PRICE_SCALE = 10_000  # ticks per dollar
PRICE_ONE = PRICE_SCALE
SIZE_SCALE = 100  # hundredths of a contract


def _to_scaled(s: str | int | float, scale: int) -> int:
    try:
        d = Decimal(str(s)) * scale
    except InvalidOperation as e:
        raise ValueError(f"not a number: {s!r}") from e
    if d != d.to_integral_value():
        raise ValueError(f"{s!r} has more precision than 1/{scale}")
    return int(d)


def parse_price(s: str | int | float) -> int:
    """Dollar string -> integer ticks of $0.0001. Must lie in [0, 1]."""
    p = _to_scaled(s, PRICE_SCALE)
    if not 0 <= p <= PRICE_ONE:
        raise ValueError(f"price out of [0,1]: {s!r}")
    return p


def parse_size(s: str | int | float) -> int:
    """Fixed-point contract string (may be negative for deltas) -> hundredths."""
    return _to_scaled(s, SIZE_SCALE)


def price_to_float(p: int) -> float:
    return p / PRICE_SCALE


def size_to_float(q: int) -> float:
    return q / SIZE_SCALE
