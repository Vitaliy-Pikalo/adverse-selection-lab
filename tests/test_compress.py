import os
import time

from asl.compress import run
from asl.recorder.writer import RawWriter, hour_key
from asl.replay import list_files, read_lines

H0 = 1_763_164_800 * 10**9  # 2025-11-15T00:00:00Z
H1 = H0 + 3600 * 10**9
NOW_H1 = (H1 + 30 * 60 * 10**9) / 1e9  # 00:30 into hour H1


def _write(tmp_path, ns, n):
    w = RawWriter(tmp_path, "kalshi", flush_seconds=0)
    w.write("kalshi", {"n": n}, recv_ns=ns)
    w.close()


def _age(path, seconds, now=NOW_H1):
    t = now - seconds  # relative to the simulated clock passed to run()
    os.utime(path, (t, t))


def test_compresses_only_closed_old_hours(tmp_path):
    _write(tmp_path, H0 + 1, 0)
    _write(tmp_path, H1 + 1, 1)  # current hour: must be left alone
    for p in tmp_path.glob("*.jsonl"):
        _age(p, 600)
    done = run(tmp_path, now=NOW_H1, grace_s=120)
    assert [d[1] for d in done] == [f"kalshi_{hour_key(H0)}.jsonl.zst"]
    assert (tmp_path / f"kalshi_{hour_key(H1)}.jsonl").exists()
    assert not (tmp_path / f"kalshi_{hour_key(H0)}.jsonl").exists()
    recs = [r["raw"]["n"] for _, p in list_files(tmp_path, "kalshi") for r in read_lines(p)]
    assert recs == [0, 1]


def test_grace_period_skips_recently_written(tmp_path):
    _write(tmp_path, H0 + 1, 0)  # mtime = now
    assert run(tmp_path, now=time.time() + 3600 * 24 * 365 * 0, grace_s=10**9) == []


def test_restart_after_compression_creates_part2_and_order_is_preserved(tmp_path):
    _write(tmp_path, H0 + 1, 0)
    _age(next(tmp_path.glob("*.jsonl")), 600)
    run(tmp_path, now=NOW_H1, grace_s=120)
    _write(tmp_path, H0 + 2, 1)  # late data for the same hour after compression
    _age(next(tmp_path.glob("*.jsonl")), 600)
    done = run(tmp_path, now=NOW_H1, grace_s=120)
    assert done[0][1] == f"kalshi_{hour_key(H0)}.p2.jsonl.zst"
    recs = [r["raw"]["n"] for _, p in list_files(tmp_path, "kalshi") for r in read_lines(p)]
    assert recs == [0, 1]


def test_stale_tmp_from_crash_is_removed(tmp_path):
    _write(tmp_path, H0 + 1, 0)
    _age(next(tmp_path.glob("*.jsonl")), 600)
    (tmp_path / f"kalshi_{hour_key(H0)}.jsonl.zst.tmp").write_bytes(b"partial")
    run(tmp_path, now=NOW_H1, grace_s=120)
    assert not list(tmp_path.glob("*.tmp"))
    assert [r["raw"]["n"] for r in read_lines(tmp_path / f"kalshi_{hour_key(H0)}.jsonl.zst")] == [0]
