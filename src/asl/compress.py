"""Compress closed hourly raw files to zstd, crash-safely. Run by a systemd timer.

    python -m asl.compress --raw data/raw [--level 9]

Rules
- only files for hours strictly before the current UTC hour, untouched for >= grace seconds
  (the recorder may still be flushing right after the hour boundary)
- write to <name>.zst.tmp, fsync, verify by full decompression, then atomic rename; only then
  delete the source. A crash at any point leaves either the source or a complete .zst.
- if <name>.jsonl.zst already exists (recorder restarted and re-created the same hour's
  .jsonl after it was compressed), the new data becomes <name>.p2.jsonl.zst, .p3, ...
- stale .tmp files from a crash are removed and redone
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import zstandard

PLAIN_RE = re.compile(r"^(?P<src>kalshi|coinbase)_(?P<hour>\d{8}T\d{2})\.jsonl$")
CHUNK = 4 << 20


def current_hour(now: float | None = None) -> str:
    return datetime.fromtimestamp(now or time.time(), tz=UTC).strftime("%Y%m%dT%H")


def _target(raw_dir: Path, src: str, hour: str) -> Path:
    base = raw_dir / f"{src}_{hour}.jsonl.zst"
    if not base.exists():
        return base
    n = 2
    while (raw_dir / f"{src}_{hour}.p{n}.jsonl.zst").exists():
        n += 1
    return raw_dir / f"{src}_{hour}.p{n}.jsonl.zst"


def compress_file(src_path: Path, dst: Path, level: int = 9) -> tuple[int, int]:
    tmp = dst.with_name(dst.name + ".tmp")
    cctx = zstandard.ZstdCompressor(level=level, threads=0)
    h_in = hashlib.sha256()
    with src_path.open("rb") as fi, tmp.open("wb") as fo:
        with cctx.stream_writer(fo, closefd=False) as zw:
            while chunk := fi.read(CHUNK):
                h_in.update(chunk)
                zw.write(chunk)
        fo.flush()
        os.fsync(fo.fileno())
    h_out = hashlib.sha256()
    with tmp.open("rb") as f, zstandard.ZstdDecompressor().stream_reader(f) as zr:
        while chunk := zr.read(CHUNK):
            h_out.update(chunk)
    if h_in.digest() != h_out.digest():
        tmp.unlink()
        raise RuntimeError(f"verification failed for {src_path}")
    os.replace(tmp, dst)
    return src_path.stat().st_size, dst.stat().st_size


def run(raw_dir: Path, now: float | None = None, grace_s: float = 120, level: int = 9) -> list:
    now = now or time.time()
    cur = current_hour(now)
    for t in raw_dir.glob("*.zst.tmp"):
        t.unlink()
    done = []
    for p in sorted(raw_dir.iterdir()):
        m = PLAIN_RE.match(p.name)
        if not m or m["hour"] >= cur or now - p.stat().st_mtime < grace_s:
            continue
        dst = _target(raw_dir, m["src"], m["hour"])
        n_in, n_out = compress_file(p, dst, level)
        p.unlink()
        done.append((p.name, dst.name, n_in, n_out))
    return done


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--level", type=int, default=9)
    args = ap.parse_args()
    for src, dst, n_in, n_out in run(Path(args.raw), level=args.level):
        print(
            f"{src} -> {dst}: {n_in / 1e6:.0f} MB -> {n_out / 1e6:.0f} MB "
            f"({n_in / max(n_out, 1):.1f}x)",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
