import httpx

from asl.backfill import _trade_row, fetch_markets, fetch_trades
from asl.kalshi.rest import KalshiREST


def _rest(handler):
    return KalshiREST(
        "https://x.test/trade-api/v2", min_interval_s=0, transport=httpx.MockTransport(handler)
    )


def test_trade_row_parses_current_and_legacy_fields():
    new = _trade_row(
        {
            "trade_id": "a",
            "ticker": "T",
            "created_time": "z",
            "yes_price_dollars": "0.2100",
            "count_fp": "3.00",
            "taker_outcome_side": "yes",
        }
    )
    assert new["yes_price"] == 2100 and new["count"] == 300 and new["taker_outcome_side"] == "yes"
    old = _trade_row({"trade_id": "b", "yes_price": 21, "count": 3, "taker_side": "no"})
    assert old["yes_price"] == 2100 and old["count"] == 300 and old["taker_outcome_side"] == "no"


def test_markets_merge_live_and_historical_and_survive_endpoint_failure():
    def handler(req):
        if req.url.path.endswith("/historical/markets"):
            return httpx.Response(404, text="nope")
        return httpx.Response(
            200,
            json={
                "markets": [
                    {
                        "ticker": "A",
                        "event_ticker": "E",
                        "result": "yes",
                        "close_time": "2026-11-01T01:00:00Z",
                    },
                    {
                        "ticker": "OUT",
                        "event_ticker": "E",
                        "result": "no",
                        "close_time": "2025-01-01T00:00:00Z",
                    },
                ]
            },
        )

    log = {"markets": {}, "errors": []}
    t0, t1 = 1_793_491_200, 1_793_577_600  # 2026-11-01 .. 2026-11-02 UTC
    df = fetch_markets(_rest(handler), "KXBTC", t0, t1, log)
    assert list(df["ticker"]) == ["A"]  # out-of-window row dropped locally
    assert df["series_ticker"].iloc[0] == "KXBTC"
    assert len(log["errors"]) == 1  # historical 404 recorded, not fatal


def test_historical_markets_skipped_after_cutoff_and_page_cap_is_nonfatal():
    paths = []

    def handler(req):
        paths.append(req.url.path)
        # server ignores filters and paginates forever
        return httpx.Response(
            200, json={"markets": [{"ticker": "X", "close_time": "bad"}], "cursor": "more"}
        )

    log = {"markets": {}, "errors": []}
    df = fetch_markets(_rest(handler), "KXBTC", 100, 200, log, settled_cutoff=50, max_pages=3)
    assert df.empty
    assert not any(p.endswith("/historical/markets") for p in paths)
    assert len(paths) == 3 and "exceeded" in log["errors"][0]


def test_trades_fall_back_to_historical():
    def handler(req):
        if req.url.path.endswith("/historical/trades"):
            return httpx.Response(
                200,
                json={
                    "trades": [
                        {
                            "trade_id": "h",
                            "ticker": "A",
                            "yes_price_dollars": "0.5000",
                            "count_fp": "1.00",
                            "taker_outcome_side": "no",
                        }
                    ]
                },
            )
        return httpx.Response(200, json={"trades": []})

    rows = fetch_trades(_rest(handler), "A", {"errors": []})
    assert [r["trade_id"] for r in rows] == ["h"]
