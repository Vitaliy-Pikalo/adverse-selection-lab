"""Append-only raw message log, rotated per UTC hour.

Each line: {"recv_ns": wall-clock ns, "mono_ns": monotonic ns, "src": str, "raw": <message>}
`src` is "kalshi", "coinbase" or "meta" (recorder lifecycle: connect, subscribe, gap, ...).

The writer only ever appends plain .jsonl (crash-safe: at worst the last line is truncated)
and NEVER compresses. Compression of closed hours runs in a separate low-priority process
(`asl.compress`, systemd timer). History: compressing inside the recorder froze the event
loop for 5-7 min every hour (data loss + reconnects), and a SIGKILL during shutdown
compression corrupted a .gz (2026-11-15 06:00 UTC). See DECISIONS.md D11.

Raw logs are the source of truth: books, features and labels are all rebuilt from them.
"""

from __future__ import annotations

import json
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

    def _open(self, hour: str) -> None:
        self.close()
        self._hour = hour
        # appending to an existing plain file (restart within the same hour) is safe
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
        """Flush and close. Fast by design: shutdown must never wait on compression."""
        if self._fh:
            self._fh.flush()
            self._fh.close()
            self._fh = None
