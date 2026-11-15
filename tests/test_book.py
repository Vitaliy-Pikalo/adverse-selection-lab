import pytest

from asl.book import BookError, BookManager, OrderBook


def snap(ticker="T", sid=1, seq=1, yes=None, no=None, clock=10):
    msg = {"market_ticker": ticker, "market_id": "x", "clock": clock}
    if yes is not None:
        msg["yes_dollars_fp"] = yes
    if no is not None:
        msg["no_dollars_fp"] = no
    return {"type": "orderbook_snapshot", "sid": sid, "seq": seq, "msg": msg}


def delta(ticker="T", sid=1, seq=2, price="0.2200", d="10.00", side="yes", clock=11):
    return {
        "type": "orderbook_delta",
        "sid": sid,
        "seq": seq,
        "msg": {
            "market_ticker": ticker,
            "market_id": "x",
            "clock": clock,
            "price_dollars": price,
            "delta_fp": d,
            "side": side,
            "ts_ms": 1_000 + seq,
        },
    }


# --- OrderBook ---


def test_snapshot_and_top_of_book_mapping():
    b = OrderBook("T")
    b.apply_snapshot(
        snap(
            yes=[["0.0800", "300.00"], ["0.2200", "333.00"]],
            no=[["0.5400", "20.00"], ["0.5600", "146.00"]],
        )["msg"]
    )
    top = b.top()
    assert top.yes_bid == 2200 and top.yes_bid_size == 33300
    # best NO bid 0.56 -> YES ask 0.44 with NO's size
    assert top.yes_ask == 4400 and top.yes_ask_size == 14600
    assert top.spread == 2200
    assert top.mid == pytest.approx(0.33)
    assert not top.crossed


def test_microprice_and_imbalance():
    b = OrderBook("T")
    b.apply_snapshot(snap(yes=[["0.4000", "300.00"]], no=[["0.5000", "100.00"]])["msg"])
    top = b.top()  # bid .40 x300, ask .50 x100
    assert top.imbalance == pytest.approx(0.5)
    # heavy bid -> microprice leans toward the ask
    assert top.microprice == pytest.approx((0.40 * 100 + 0.50 * 300) / 400)


def test_delta_add_reduce_remove():
    b = OrderBook("T")
    b.apply_snapshot(snap(yes=[["0.2200", "10.00"]])["msg"])
    b.apply_delta(delta(d="5.00")["msg"])
    assert b.yes[2200] == 1500
    b.apply_delta(delta(d="-15.00")["msg"])
    assert 2200 not in b.yes
    b.apply_delta(delta(price="0.3000", d="7.00")["msg"])
    assert b.best("yes") == (3000, 700)


def test_negative_level_invalidates():
    b = OrderBook("T")
    b.apply_snapshot(snap(yes=[["0.2200", "10.00"]])["msg"])
    with pytest.raises(BookError):
        b.apply_delta(delta(d="-11.00")["msg"])
    assert not b.valid


def test_delta_before_snapshot_ignored():
    b = OrderBook("T")
    b.apply_delta(delta()["msg"])
    assert b.yes == {} and not b.valid


def test_one_sided_book():
    b = OrderBook("T")
    b.apply_snapshot(snap(yes=[["0.0100", "5.00"]])["msg"])
    top = b.top()
    assert top.yes_ask is None and not top.two_sided and top.mid is None


def test_crossed_detection():
    b = OrderBook("T")
    b.apply_snapshot(snap(yes=[["0.6000", "1.00"]], no=[["0.5000", "1.00"]])["msg"])
    assert b.top().crossed  # yes bid .60 >= yes ask .50


def test_depth_sorted_best_first():
    b = OrderBook("T")
    b.apply_snapshot(snap(yes=[["0.1000", "1.00"], ["0.3000", "2.00"], ["0.2000", "3.00"]])["msg"])
    assert b.depth("yes", 2) == [(3000, 200), (2000, 300)]


# --- BookManager ---


def test_manager_routes_and_tracks_seq():
    m = BookManager()
    m.handle({"type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 1}})
    ev = m.handle(snap(seq=1, yes=[["0.2200", "10.00"]]))
    assert [e.kind for e in ev] == ["book"]
    ev = m.handle(delta(seq=2))
    assert [e.kind for e in ev] == ["book"]
    assert m.books["T"].yes[2200] == 2000
    assert m.gaps == []


def test_seq_gap_invalidates_all_books_on_sid_until_resnapshot():
    m = BookManager()
    m.handle({"type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 1}})
    m.handle(snap(ticker="A", seq=1, yes=[["0.2200", "10.00"]]))
    m.handle(snap(ticker="B", seq=2, yes=[["0.3000", "10.00"]]))
    ev = m.handle(delta(ticker="A", seq=5))  # 3,4 missing
    kinds = [e.kind for e in ev]
    assert "gap" in kinds
    assert not m.books["A"].valid and not m.books["B"].valid
    assert m.gaps[0]["expected"] == 3 and m.gaps[0]["got"] == 5
    # deltas are dropped until a fresh snapshot
    m.handle(delta(ticker="B", seq=6, price="0.3000", d="1.00"))
    assert m.books["B"].yes[3000] == 1000
    m.handle(snap(ticker="B", seq=7, yes=[["0.3100", "4.00"]]))
    assert m.books["B"].valid and m.books["B"].best("yes") == (3100, 400)


def test_trade_gap_does_not_invalidate_books():
    m = BookManager()
    m.handle({"type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 1}})
    m.handle({"type": "subscribed", "msg": {"channel": "trade", "sid": 2}})
    m.handle(snap(ticker="A", seq=1, yes=[["0.2200", "10.00"]]))
    t = {"type": "trade", "sid": 2, "seq": 1, "msg": {"market_ticker": "A"}}
    m.handle(t)
    ev = m.handle({**t, "seq": 9})
    assert any(e.kind == "gap" for e in ev)
    assert m.books["A"].valid


def test_reset_on_reconnect():
    m = BookManager()
    m.handle(snap(seq=1, yes=[["0.2200", "10.00"]]))
    m.reset()
    assert not m.books["T"].valid
    # new connection restarts seq at 1 without a false gap
    ev = m.handle(snap(seq=1, yes=[["0.2200", "10.00"]]))
    assert [e.kind for e in ev] == ["book"]
