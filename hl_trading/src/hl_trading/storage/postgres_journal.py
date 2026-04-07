"""Append-only order journal in PostgreSQL (async via background thread + queue)."""

from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import dataclass
from typing import Any, Protocol

import psycopg

from hl_trading.domain import LimitOrderIntent, PortfolioView

logger = logging.getLogger(__name__)


class OrderJournal(Protocol):
    def enqueue_order_record(
        self,
        intent: LimitOrderIntent,
        portfolio: PortfolioView,
        exchange_response: Any | None,
        *,
        dry_run: bool,
    ) -> None: ...

    def close(self) -> None: ...


@dataclass(slots=True)
class _OrderRow:
    cloid: str | None
    coin: str
    side: str
    size: float
    limit_px: float
    reduce_only: bool
    dry_run: bool
    account: str
    positions_json: str
    response_json: str | None


class PostgresOrderJournal:
    def __init__(self, dsn: str, queue_max: int = 50_000) -> None:
        self._dsn = dsn
        self._q: queue.Queue[_OrderRow | None] = queue.Queue(maxsize=queue_max)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="pg-order-journal", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self._q.put_nowait(None)
        except queue.Full:
            pass
        self._thread.join(timeout=5.0)

    def enqueue_order_record(
        self,
        intent: LimitOrderIntent,
        portfolio: PortfolioView,
        exchange_response: Any | None,
        *,
        dry_run: bool,
    ) -> None:
        row = _OrderRow(
            cloid=intent.client_order_id_hex,
            coin=intent.coin,
            side=intent.side,
            size=intent.size,
            limit_px=intent.limit_px,
            reduce_only=intent.reduce_only,
            dry_run=dry_run,
            account=portfolio.account_address,
            positions_json=json.dumps(portfolio.positions),
            response_json=json.dumps(exchange_response) if exchange_response is not None else None,
        )
        try:
            self._q.put_nowait(row)
        except queue.Full:
            logger.error("postgres order journal queue full; dropping record")

    def _run(self) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute("SET application_name = 'hl_trading_orders'")
            while not self._stop.is_set():
                try:
                    item = self._q.get(timeout=0.5)
                except queue.Empty:
                    continue
                if item is None:
                    break
                try:
                    conn.execute(
                        """
                        INSERT INTO orders
                        (cloid, coin, side, size, limit_px, reduce_only, dry_run, account, positions_json, exchange_response)
                        VALUES (%(cloid)s, %(coin)s, %(side)s, %(size)s, %(limit_px)s, %(reduce_only)s, %(dry_run)s,
                                %(account)s, %(positions_json)s::jsonb, %(response_json)s::jsonb)
                        """,
                        {
                            "cloid": item.cloid,
                            "coin": item.coin,
                            "side": item.side,
                            "size": item.size,
                            "limit_px": item.limit_px,
                            "reduce_only": item.reduce_only,
                            "dry_run": item.dry_run,
                            "account": item.account,
                            "positions_json": item.positions_json,
                            "response_json": item.response_json,
                        },
                    )
                    conn.commit()
                except Exception:
                    logger.exception("failed to insert order row")
                    conn.rollback()
