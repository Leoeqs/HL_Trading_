"""Offline replay — rebuild L2 from recorded NDJSON with minimal overhead (no network I/O in the loop)."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import orjson

from hl_trading.book.l2 import PerpL2Book
from hl_trading.config import Settings
from hl_trading.domain import PortfolioView
from hl_trading.strategies.base import Strategy

logger = logging.getLogger(__name__)


def _parse_line(obj: dict[str, Any]) -> dict[str, Any] | None:
    """Support native ws message, recorder envelope, or ClickHouse `raw_json` cell."""
    if obj.get("channel") == "l2Book":
        return obj

    raw = obj.get("raw")
    if raw is None and "raw_json" in obj:
        inner = obj["raw_json"]
        if isinstance(inner, str):
            inner = orjson.loads(inner.encode())
        if isinstance(inner, bytes):
            inner = orjson.loads(inner)
        if isinstance(inner, dict):
            if inner.get("channel") == "l2Book":
                return inner
            raw = inner.get("raw")

    if isinstance(raw, str):
        raw = orjson.loads(raw.encode())
    if isinstance(raw, bytes):
        raw = orjson.loads(raw)
    if isinstance(raw, dict) and raw.get("channel") == "l2Book":
        return raw
    return None


def replay_file(
    path: Path,
    settings: Settings,
    strategy: Strategy,
    *,
    max_events: int | None = None,
    sleep_s: float = 0.0,
    log_every: int = 0,
) -> None:
    """
    Replay one message at a time. For fastest deterministic replay use sleep_s=0.
    `log_every` prints progress every N L2 events (0 = quiet).
    """
    portfolio = PortfolioView(
        account_address=settings.account_address.lower(),
        margin_summary={},
        positions={},
        open_orders_count=0,
        raw=None,
    )
    books: dict[str, PerpL2Book] = {}
    n = 0
    t0 = time.perf_counter()
    with path.open("rb") as fh:
        for line in fh:
            if max_events is not None and n >= max_events:
                break
            line = line.strip()
            if not line:
                continue
            obj = orjson.loads(line)
            raw_msg = _parse_line(obj)
            if raw_msg is None:
                continue
            data = raw_msg["data"]
            coin = str(data["coin"])
            book = books.setdefault(coin, PerpL2Book(coin))
            book.apply_ws_message(raw_msg)
            strategy.on_l2_book(coin, book, portfolio)
            n += 1
            if sleep_s > 0:
                time.sleep(sleep_s)
            if log_every and n % log_every == 0:
                logger.info("replayed %s events in %.3fs", n, time.perf_counter() - t0)
    elapsed = time.perf_counter() - t0
    logger.info("replay done events=%s elapsed=%.3fs evt/s=%.0f", n, elapsed, n / elapsed if elapsed else 0.0)
