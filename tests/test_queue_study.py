import gzip
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from asl.queue_study import BidStudy


def row(kind, sequence, **fields):
    trade = kind == "trade"
    return {"src": "kalshi", "raw": {"type": kind, "sid": 2 if trade else 1,
            "seq": sequence, "msg": {"market_ticker": "TEST", **fields}}}


def snapshot(seq=1):
    return row("orderbook_snapshot", seq, yes_dollars_fp=[["0.49", "2"]],
               no_dollars_fp=[["0.49", "5"]])


def trade(seq, volume, **overrides):
    fields = {"trade_id": str(seq), "yes_price_dollars": "0.49",
              "count_fp": volume, "taker_outcome_side": "no"}
    return row("trade", seq, **(fields | overrides))


def update(seq, side="no", price="0.50", delta="1"):
    return row("orderbook_delta", seq, side=side, price_dollars=price, delta_fp=delta)


class TestQueueStudy(unittest.TestCase):
    def test_fractional_fills_and_signed_marks(self):
        study = BidStudy("TEST", 2)
        records = [snapshot(), trade(1, "2"), trade(2, ".25"), trade(3, ".75"),
                   update(2), update(3)]
        for record in records:
            study.process(record)
        result = study.report()
        self.assertEqual(result["filled_contracts"], 1)
        self.assertEqual(result["marked_contracts"], 1)
        self.assertEqual(result["mean_markout_cents"], .5)
        self.assertEqual(result["mean_mid_move_cents"], -.5)
        self.assertEqual(result["mid_moved_down_fraction"], 1)
        self.assertEqual(result["negative_markout_fraction"], 0)

    def test_exclusions_and_gap(self):
        study = BidStudy("TEST", 1)
        for record in [snapshot(), trade(1, "2"), trade(2, "2", trade_id="1"),
                       trade(3, "2", is_block_trade=True), trade(4, "2", taker_outcome_side="yes"),
                       trade(5, ".5"), update(4), trade(6, "10"), snapshot(5)]:
            study.process(record)
        result = study.report()
        self.assertEqual(result["filled_contracts"], .5)
        self.assertEqual(result["censored_contracts"], .5)
        self.assertIsNone(result["mean_markout_cents"])

    def test_cli_continues_across_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            a, b = Path(tmp) / "a.jsonl", Path(tmp) / "b.jsonl.gz"
            a.write_text(json.dumps(snapshot()) + "\n", encoding="utf-8")
            with gzip.open(b, "wt", encoding="utf-8") as out:
                for record in [trade(1, "3"), update(2)]:
                    print(json.dumps(record), file=out)
            proc = subprocess.run([sys.executable, "-m", "asl.queue_study", "TEST",
                                   str(a), str(b), "--horizon", "1"],
                                  check=True, capture_output=True, text=True,
                                  env=os.environ | {"PYTHONPATH": str(
                                      Path(__file__).resolve().parents[1] / "src")})
            self.assertEqual(json.loads(proc.stdout)["marked_contracts"], 1)
