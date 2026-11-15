"""Append-only raw message log, rotated per UTC hour.

Each line: {"recv_ns": wall-clock ns, "mono_ns": monotonic ns, "src": str, "raw": <message>}
`src` is "kalshi", "coinbase" or "meta" (recorder lifecycle: connect, subscribe, gap, ...).

The current hour is written as plain .jsonl (crash-safe: at worst the last line is
truncated). When the hour rolls over, the closed file is gzipped and the .jsonl removed.
Raw logs are the source of truth: books, features and labels are all rebuilt from them.
"""

from __future__ import annotations

import gzip
import json
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path


def hour_key(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=UTC).strftime("%Y%m%dT%H")


class RawWriter:
    def __init__(self, out_dir: str | Path, src: str, flush_seconds: float = 2.0):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.src = src
        self.flush_seconds = flush_seconds
        self._fh = None
        self._hour: str | None = None
        self._path: Path | None = None
        self._last_flush = 0.0
        self.lines_written = 0
        self.gzip_closed_files()  # leftovers from a crash

    def _open(self, hour: str) -> None:
        self.close()
        self._hour = hour
        self._path = self.out_dir / f"{self.src}_{hour}.jsonl"
        self._fh = self._path.open("a", encoding="utf-8")

    def write(self, src: str, raw: object, recv_ns: int | None = None) -> None:
        recv_ns = recv_ns if recv_ns is not None else time.time_ns()
        hour = hour_key(recv_ns)
        if hour != self._hour:
            self._open(hour)
        line = json.dumps(
            {"recv_ns": recv_ns, "mono_ns": time.monotonic_ns(), "src": src, "raw": raw},
            separators=(",", ":"),
        )
        self._fh.write(line + "\n")  # type: ignore[union-attr]
        self.lines_written += 1
        now = time.monotonic()
        if now - self._last_flush >= self.flush_seconds:
            self._fh.flush()  # type: ignore[union-attr]
            self._last_flush = now

    def meta(self, event: str, **detail: object) -> None:
        self.write("meta", {"event": event, **detail})

    def close(self) -> None:
        if self._fh:
            self._fh.flush()
            self._fh.close()
            self._fh = None
            self.gzip_closed_files()

    def gzip_closed_files(self) -> None:
        """Compress every .jsonl for this src except the hour currently open."""
        for p in sorted(self.out_dir.glob(f"{self.src}_*.jsonl")):
            if self._fh is not None and p == self._path:
                continue
            gz = p.with_suffix(".jsonl.gz")
            if gz.exists():
                # a previous run already wrote this hour: append as a second gzip member
                with p.open("rb") as fi, gzip.open(gz, "ab") as fo:
                    shutil.copyfileobj(fi, fo)
            else:
                with p.open("rb") as fi, gzip.open(gz, "wb") as fo:
                    shutil.copyfileobj(fi, fo)
            p.unlink()
