"""Append L2 records to NDJSON (same bytes as ClickHouse queue) with a background buffer."""

from __future__ import annotations

import logging
import queue
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class FileL2Writer:
    def __init__(self, path: str, *, batch_max: int = 256, flush_interval_s: float = 0.2) -> None:
        self._path = Path(path)
        self._batch_max = batch_max
        self._flush_interval_s = flush_interval_s
        self._q: queue.Queue[bytes | None] = queue.Queue(maxsize=200_000)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="file-l2-writer", daemon=True)

    def start(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self._enqueue_shutdown()
        self._thread.join(timeout=10.0)

    def enqueue_payload(self, payload: bytes) -> None:
        try:
            self._q.put_nowait(payload)
        except queue.Full:
            logger.error("file l2 queue full; drop")

    def _enqueue_shutdown(self) -> None:
        while self._thread.is_alive():
            try:
                self._q.put(None, timeout=0.1)
                return
            except queue.Full:
                logger.warning("file l2 queue full during shutdown; waiting to flush")

    def _run(self) -> None:
        buf: list[bytes] = []
        last_flush = time.monotonic()
        with self._path.open("ab", buffering=0) as fh:
            while True:
                timeout = max(0.0, self._flush_interval_s - (time.monotonic() - last_flush))
                try:
                    item = self._q.get(timeout=timeout)
                except queue.Empty:
                    item = Ellipsis
                if item is None:
                    if buf:
                        fh.write(b"".join(buf))
                    break
                if item is not Ellipsis:
                    buf.append(item + b"\n")
                now = time.monotonic()
                if buf and (len(buf) >= self._batch_max or now - last_flush >= self._flush_interval_s):
                    fh.write(b"".join(buf))
                    buf.clear()
                    last_flush = now
