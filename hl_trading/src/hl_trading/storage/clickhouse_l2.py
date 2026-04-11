"""Buffered L2 raw writes to ClickHouse — hot path only enqueues orjson bytes."""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any

import orjson
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client

from hl_trading.storage.l2_serialize import l2_record_bytes

logger = logging.getLogger(__name__)


class ClickHouseL2Writer:
    def __init__(
        self,
        host: str,
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "hl",
        *,
        batch_max: int = 500,
        flush_interval_s: float = 0.25,
        queue_max: int = 200_000,
    ) -> None:
        self._batch_max = batch_max
        self._flush_interval_s = flush_interval_s
        self._q: queue.Queue[bytes | None] = queue.Queue(maxsize=queue_max)
        self._stop = threading.Event()
        self._client: Client | None = None
        self._connect_kw: dict[str, Any] = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
        }
        self._database = database
        self._thread = threading.Thread(target=self._run, name="ch-l2-writer", daemon=True)

    def start(self) -> None:
        self._client = get_client(**self._connect_kw)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self._enqueue_shutdown()
        self._thread.join(timeout=10.0)

    def enqueue_raw_message(self, ws_msg: dict[str, Any], ingest_ns: int) -> None:
        self.enqueue_payload(l2_record_bytes(ws_msg, ingest_ns))

    def enqueue_payload(self, payload: bytes) -> None:
        try:
            self._q.put_nowait(payload)
        except queue.Full:
            logger.error("clickhouse l2 queue full; drop")

    def _enqueue_shutdown(self) -> None:
        while self._thread.is_alive():
            try:
                self._q.put(None, timeout=0.1)
                return
            except queue.Full:
                logger.warning("clickhouse l2 queue full during shutdown; waiting to flush")

    def _flush(self, buf: list[tuple[int, str, int, str]]) -> None:
        if not buf or self._client is None:
            return
        try:
            self._client.insert(
                f"{self._database}.l2_book_raw",
                buf,
                column_names=["exchange_ts", "coin", "ingest_ns", "raw_json"],
            )
        except Exception:
            logger.exception("clickhouse insert failed (%s rows)", len(buf))
        buf.clear()

    def _run(self) -> None:
        buf: list[tuple[int, str, int, str]] = []
        last_flush = time.monotonic()
        while True:
            timeout = max(0.0, self._flush_interval_s - (time.monotonic() - last_flush))
            try:
                item = self._q.get(timeout=timeout)
            except queue.Empty:
                item = Ellipsis  # flush tick

            if item is None:
                self._flush(buf)
                break

            if item is not Ellipsis:
                obj = orjson.loads(item)
                raw_msg = obj["raw"]
                data = raw_msg.get("data") or {}
                coin = str(data.get("coin", ""))
                buf.append((int(obj["exchange_ts"]), coin, int(obj["ingest_ns"]), item.decode("utf-8")))

            now = time.monotonic()
            if buf and (len(buf) >= self._batch_max or now - last_flush >= self._flush_interval_s):
                self._flush(buf)
                last_flush = now
