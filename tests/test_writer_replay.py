import gzip
import json

import pandas as pd

from asl.recorder.writer import RawWriter, hour_key
from asl.replay import list_files, read_lines, run

H0 = 1_763_164_800 * 10**9  # 2025-11-15T00:00:00Z
H1 = H0 + 3600 * 10**9


def k_snap(seq, ticker="KXBTC-X-B1", yes=(("0.2000", "10.00"),), no=(("0.7000", "5.00"),)):
    return {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": seq,
        "msg": {
            "market_ticker": ticker,
            "market_id": "m",
            "clock": seq,
            "yes_dollars_fp": [list(x) for x in yes],
            "no_dollars_fp": [list(x) for x in no],
        },
    }


def k_delta(seq, price="0.2000", d="5.00", side="yes", ticker="KXBTC-X-B1"):
    return {
        "type": "orderbook_delta",
        "sid": 1,
        "seq": seq,
        "msg": {
            "market_ticker": ticker,
            "market_id": "m",
            "clock": seq,
            "price_dollars": price,
            "delta_fp": d,
            "side": side,
            "ts_ms": H0 // 10**6 + seq,
        },
    }


def test_hour_rotation_never_compresses(tmp_path):
    w = RawWriter(tmp_path, "kalshi", flush_seconds=0)
    w.write("kalshi", {"a": 1}, recv_ns=H0 + 1)
    w.write("kalshi", {"a": 2}, recv_ns=H1 + 1)
    w.close()
    names = sorted(p.name for p in tmp_path.iterdir())
    assert names == [f"kalshi_{hour_key(H0)}.jsonl", f"kalshi_{hour_key(H1)}.jsonl"]
    recs = [r["raw"] for _, p in list_files(tmp_path, "kalshi") for r in read_lines(p)]
    assert recs == [{"a": 1}, {"a": 2}]


def test_restart_in_same_hour_appends_plain_file(tmp_path):
    for i in range(2):
        w = RawWriter(tmp_path, "kalshi", flush_seconds=0)
        w.write("kalshi", {"n": i}, recv_ns=H0 + i)
        w.close()
    recs = [r["raw"]["n"] for r in read_lines(tmp_path / f"kalshi_{hour_key(H0)}.jsonl")]
    assert recs == [0, 1]


def test_corrupt_gzip_tail_is_tolerated_and_reported(tmp_path):
    import asl.replay as rp

    good = gzip.compress(
        (json.dumps({"recv_ns": 1, "src": "kalshi", "raw": {"n": 1}}) + "\n").encode()
    )
    p = tmp_path / "kalshi_20251115T06.jsonl.gz"
    p.write_bytes(good + b"\x1f\x8b\x08\x00garbage-not-deflate")
    rp.CORRUPT.clear()
    assert [r["raw"]["n"] for r in read_lines(p)] == [1]
    assert rp.CORRUPT and rp.CORRUPT[0]["file"] == p.name


def test_truncated_last_line_is_skipped(tmp_path):
    p = tmp_path / "kalshi_20251115T00.jsonl"
    p.write_text(json.dumps({"recv_ns": 1, "src": "kalshi", "raw": {}}) + "\n" + '{"recv_ns": 2,')
    assert len(list(read_lines(p))) == 1


def test_end_to_end_replay_rebuilds_books_and_reports_gap(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "derived"
    w = RawWriter(raw, "kalshi", flush_seconds=0)
    t = H0

    def put(src, msg):
        nonlocal t
        t += 10**6
        w.write(src, msg, recv_ns=t)

    put("meta", {"event": "connect"})
    put(
        "meta",
        {"event": "market_info", "market": {"ticker": "KXBTC-X-B1", "strike_type": "between"}},
    )
    put("kalshi", {"type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 1}})
    put("kalshi", k_snap(1))
    put("kalshi", k_delta(2))  # yes .20 -> 15
    put("kalshi", k_delta(5, d="1.00"))  # gap (3,4 missing) -> invalid, dropped
    put("meta", {"event": "disconnect", "reason": "resync"})
    put("meta", {"event": "connect"})
    put("kalshi", {"type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 1}})
    put("kalshi", k_snap(1, yes=(("0.2100", "3.00"),)))
    put(
        "kalshi",
        {
            "type": "trade",
            "sid": 2,
            "seq": 1,
            "msg": {
                "trade_id": "t1",
                "market_ticker": "KXBTC-X-B1",
                "yes_price_dollars": "0.2100",
                "no_price_dollars": "0.7900",
                "count_fp": "2.00",
                "taker_outcome_side": "no",
                "taker_book_side": "ask",
                "is_block_trade": False,
                "ts_ms": H0 // 10**6,
            },
        },
    )
    w.close()

    rep = run(raw, out)["kalshi"]
    assert rep["n_gaps"] == 1 and rep["n_disconnects"] == 1
    tob = pd.read_parquet(next((out / "tob").glob("*.parquet")))
    assert list(tob["yes_bid_size"]) == [1000, 1500, 300]  # gapped delta not applied
    assert list(tob["yes_ask"]) == [3000, 3000, 3000]  # 1 - no bid .70
    assert tob["yes_bid"].iloc[-1] == 2100
    trades = pd.read_parquet(next((out / "trades").glob("*.parquet")))
    assert trades.iloc[0]["count"] == 200 and trades.iloc[0]["taker_outcome_side"] == "no"
    assert (out / "markets.parquet").exists()
