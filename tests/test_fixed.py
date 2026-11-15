import pytest

from asl.fixed import PRICE_ONE, parse_price, parse_size


def test_price_parsing_exact():
    assert parse_price("0.2200") == 2200
    assert parse_price("1.0000") == PRICE_ONE
    assert parse_price("0.0001") == 1
    assert parse_price("0") == 0


def test_price_rejects_out_of_range_and_excess_precision():
    with pytest.raises(ValueError):
        parse_price("1.0100")
    with pytest.raises(ValueError):
        parse_price("-0.0100")
    with pytest.raises(ValueError):
        parse_price("0.12345")
    with pytest.raises(ValueError):
        parse_price("abc")


def test_size_parsing_signed():
    assert parse_size("300.00") == 30000
    assert parse_size("-54.00") == -5400
    assert parse_size("0.01") == 1


def test_no_float_drift_over_many_deltas():
    total = sum(parse_size("0.10") for _ in range(10_000))
    assert total == 100_000  # float 0.1*10000 would not be exact
