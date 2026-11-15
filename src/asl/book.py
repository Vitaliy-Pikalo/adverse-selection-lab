"""Kalshi L2 order book reconstruction from orderbook_snapshot / orderbook_delta.

Kalshi binary books are bids-only on both legs:
  yes levels = resting bids to BUY YES
  no levels  = resting bids to BUY NO
A NO bid at price q is economically a YES offer at (1 - q), so
  best YES ask      = 1 - best NO bid
  size at YES ask   = size at best NO bid

Sequence numbers are per *subscription* (sid), not per market. A gap in a sid's
seq means any market on that subscription may have missed deltas, so every book
on that sid is invalidated until a fresh snapshot arrives.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from asl.fixed import PRICE_ONE, parse_price, parse_size, price_to_float


class BookError(Exception):
    """A delta that cannot be applied consistently (e.g. level goes negative)."""


@dataclass(frozen=True)
class TopOfBook:
    ticker: str
    yes_bid: int | None  # price ticks ($0.0001)
    yes_bid_size: int  # hundredths of a contract
    yes_ask: int | None
    yes_ask_size: int
    clock: int
    ts_ms: int | None

    @property
    def two_sided(self) -> bool:
        return self.yes_bid is not None and self.yes_ask is not None

    @property
    def crossed(self) -> bool:
        """bid >= ask should never persist on a matching engine; flags corruption."""
        return self.two_sided and self.yes_bid >= self.yes_ask  # type: ignore[operator]

    @property
    def spread(self) -> int | None:
        return self.yes_ask - self.yes_bid if self.two_sided else None  # type: ignore[operator]

    @property
    def mid(self) -> float | None:
        if not self.two_sided:
            return None
        return price_to_float(self.yes_bid + self.yes_ask) / 2  # type: ignore[operator]

    @property
    def imbalance(self) -> float | None:
        """(bid_size - ask_size) / (bid_size + ask_size) at the touch, in [-1, 1]."""
        tot = self.yes_bid_size + self.yes_ask_size
        if not self.two_sided or tot == 0:
            return None
        return (self.yes_bid_size - self.yes_ask_size) / tot

    @property
    def microprice(self) -> float | None:
        """Size-weighted mid: leans toward the side with LESS size (it's likelier to be hit)."""
        tot = self.yes_bid_size + self.yes_ask_size
        if not self.two_sided or tot == 0:
            return None
        b, a = price_to_float(self.yes_bid), price_to_float(self.yes_ask)  # type: ignore[arg-type]
        return (b * self.yes_ask_size + a * self.yes_bid_size) / tot


@dataclass
class OrderBook:
    ticker: str
    yes: dict[int, int] = field(default_factory=dict)  # price -> size
    no: dict[int, int] = field(default_factory=dict)
    clock: int = 0
    ts_ms: int | None = None
    valid: bool = False

    def apply_snapshot(self, msg: dict) -> None:
        self.yes = _levels(msg.get("yes_dollars_fp"))
        self.no = _levels(msg.get("no_dollars_fp"))
        self.clock = int(msg.get("clock", 0))
        self.valid = True

    def apply_delta(self, msg: dict) -> None:
        if not self.valid:
            return  # waiting for a snapshot; deltas before it are meaningless
        side = msg["side"]
        if side not in ("yes", "no"):
            raise BookError(f"{self.ticker}: unknown side {side!r}")
        levels = self.yes if side == "yes" else self.no
        price = parse_price(msg["price_dollars"])
        new = levels.get(price, 0) + parse_size(msg["delta_fp"])
        if new < 0:
            self.valid = False
            raise BookError(f"{self.ticker}: {side}@{price} would go negative ({new})")
        if new == 0:
            levels.pop(price, None)
        else:
            levels[price] = new
        self.clock = int(msg.get("clock", self.clock))
        self.ts_ms = msg.get("ts_ms", self.ts_ms)

    def best(self, side: str) -> tuple[int, int] | None:
        levels = self.yes if side == "yes" else self.no
        if not levels:
            return None
        p = max(levels)
        return p, levels[p]

    def top(self) -> TopOfBook:
        yb, nb = self.best("yes"), self.best("no")
        return TopOfBook(
            ticker=self.ticker,
            yes_bid=yb[0] if yb else None,
            yes_bid_size=yb[1] if yb else 0,
            yes_ask=PRICE_ONE - nb[0] if nb else None,
            yes_ask_size=nb[1] if nb else 0,
            clock=self.clock,
            ts_ms=self.ts_ms,
        )

    def depth(self, side: str, n: int = 5) -> list[tuple[int, int]]:
        """Top n levels of a leg, best first, as (price, size)."""
        levels = self.yes if side == "yes" else self.no
        return sorted(levels.items(), reverse=True)[:n]


def _levels(raw: list | None) -> dict[int, int]:
    out: dict[int, int] = {}
    for price, size in raw or []:
        q = parse_size(size)
        if q < 0:
            raise BookError(f"negative size in snapshot: {price} {size}")
        if q > 0:
            out[parse_price(price)] = q
    return out


@dataclass
class Event:
    kind: str  # book | trade | gap | invalid | subscribed | error | ignored
    ticker: str | None = None
    sid: int | None = None
    detail: dict | None = None


class BookManager:
    """Routes raw Kalshi WS messages to books and tracks per-sid sequence integrity."""

    def __init__(self) -> None:
        self.books: dict[str, OrderBook] = {}
        self.last_seq: dict[int, int] = {}
        self.sid_channel: dict[int, str] = {}
        self.sid_tickers: dict[int, set[str]] = {}
        self.gaps: list[dict] = []

    def reset(self) -> None:
        """Call on every (re)connect: sids and seqs are per-connection."""
        for b in self.books.values():
            b.valid = False
        self.last_seq.clear()
        self.sid_channel.clear()
        self.sid_tickers.clear()

    def _check_seq(self, sid: int, seq: int) -> dict | None:
        prev = self.last_seq.get(sid)
        self.last_seq[sid] = seq
        if prev is None or seq == prev + 1:
            return None
        return {"sid": sid, "expected": prev + 1, "got": seq, "channel": self.sid_channel.get(sid)}

    def handle(self, m: dict) -> list[Event]:
        typ = m.get("type")
        # Control replies (e.g. "ok" to update_subscription, "unsubscribed") also consume a
        # seq number on their subscription; skipping them would look like a gap.
        if typ in ("ok", "unsubscribed") and m.get("sid") is not None and "seq" in m:
            gap = self._check_seq(m["sid"], m["seq"])
            if not gap:
                return [Event("ignored", sid=m["sid"], detail={"type": typ})]
            self.gaps.append(gap)
            events = [Event("gap", sid=m["sid"], detail=gap)]
            if self.sid_channel.get(m["sid"]) != "trade":
                for t in self.sid_tickers.get(m["sid"], ()):
                    if t in self.books:
                        self.books[t].valid = False
                        events.append(Event("invalid", ticker=t, sid=m["sid"]))
            return events
        if typ == "subscribed":
            sid = m["msg"]["sid"]
            self.sid_channel[sid] = m["msg"].get("channel", "")
            return [Event("subscribed", sid=sid, detail=m["msg"])]
        if typ == "error":
            return [Event("error", detail=m.get("msg") or m.get("data") or m)]
        if typ not in ("orderbook_snapshot", "orderbook_delta", "trade"):
            return [Event("ignored", detail={"type": typ})]

        sid, msg = m.get("sid"), m["msg"]
        ticker = msg["market_ticker"]
        events: list[Event] = []
        if sid is not None and "seq" in m:
            gap = self._check_seq(sid, m["seq"])
            if gap:
                self.gaps.append(gap)
                events.append(Event("gap", sid=sid, detail=gap))
                if typ != "trade" and self.sid_channel.get(sid) != "trade":
                    for t in self.sid_tickers.get(sid, ()):
                        if t in self.books:
                            self.books[t].valid = False
                            events.append(Event("invalid", ticker=t, sid=sid))

        if typ == "trade":
            events.append(Event("trade", ticker=ticker, sid=sid, detail=msg))
            return events

        self.sid_channel.setdefault(sid, "orderbook_delta")
        self.sid_tickers.setdefault(sid, set()).add(ticker)
        book = self.books.setdefault(ticker, OrderBook(ticker))
        try:
            if typ == "orderbook_snapshot":
                book.apply_snapshot(msg)
            else:
                was_valid = book.valid
                book.apply_delta(msg)
                if not was_valid:
                    return events  # dropped: no snapshot yet / invalidated
        except BookError as e:
            return events + [Event("invalid", ticker=ticker, sid=sid, detail={"error": str(e)})]
        events.append(Event("book", ticker=ticker, sid=sid))
        return events
