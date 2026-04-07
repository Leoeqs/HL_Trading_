"""Postgres: orders journal, fills ingestion, order status updates — single background writer."""

from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg

from hl_trading.domain import LimitOrderIntent, PortfolioView
from hl_trading.metrics import FILLS_INSERTED
from hl_trading.services.ws_user_parsers import map_hl_order_status_to_row_status
from hl_trading.storage.postgres_journal import OrderJournal

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _QOrderInsert:
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
    normalized_status: str
    exchange_oid: int | None
    error_message: str | None


@dataclass(slots=True)
class _QFillInsert:
    account: str
    fill: dict[str, Any]


@dataclass(slots=True)
class _QOrderStatus:
    account: str
    exchange_oid: int
    hl_status: str


_QueueItem = _QOrderInsert | _QFillInsert | _QOrderStatus | None


class PostgresStore(OrderJournal):
    def __init__(self, dsn: str, queue_max: int = 100_000) -> None:
        self._dsn = dsn
        self._q: queue.Queue[_QueueItem] = queue.Queue(maxsize=queue_max)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="postgres-store", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self._q.put_nowait(None)
        except queue.Full:
            pass
        self._thread.join(timeout=10.0)

    def enqueue_order_record(
        self,
        intent: LimitOrderIntent,
        portfolio: PortfolioView,
        exchange_response: Any | None,
        *,
        dry_run: bool,
        normalized_status: str | None,
        exchange_oid: int | None,
        error_message: str | None,
    ) -> None:
        if dry_run:
            normalized_status = normalized_status or "dry_run"
        if not dry_run and normalized_status is None:
            normalized_status = "unknown"
        row = _QOrderInsert(
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
            normalized_status=normalized_status,
            exchange_oid=exchange_oid,
            error_message=error_message,
        )
        self._put(row)

    def enqueue_fill(self, account: str, fill: dict[str, Any]) -> None:
        self._put(_QFillInsert(account=account.lower(), fill=fill))

    def enqueue_order_status(self, account: str, exchange_oid: int, hl_status: str) -> None:
        self._put(_QOrderStatus(account=account.lower(), exchange_oid=exchange_oid, hl_status=hl_status))

    def _put(self, item: _QOrderInsert | _QFillInsert | _QOrderStatus) -> None:
        try:
            self._q.put_nowait(item)
        except queue.Full:
            logger.error("postgres store queue full; dropping item")

    def _run(self) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute("SET application_name = 'hl_trading_store'")
            while not self._stop.is_set():
                try:
                    item = self._q.get(timeout=0.5)
                except queue.Empty:
                    continue
                if item is None:
                    break
                try:
                    if isinstance(item, _QOrderInsert):
                        self._do_order_insert(conn, item)
                    elif isinstance(item, _QFillInsert):
                        self._do_fill_insert(conn, item)
                    elif isinstance(item, _QOrderStatus):
                        self._do_order_status(conn, item)
                    conn.commit()
                except Exception:
                    logger.exception("postgres store transaction failed")
                    conn.rollback()

    def _do_order_insert(self, conn: psycopg.Connection, item: _QOrderInsert) -> None:
        conn.execute(
            """
            INSERT INTO orders (
                cloid, coin, side, size, limit_px, reduce_only, dry_run, account,
                positions_json, exchange_response, status, exchange_oid, error_message
            )
            VALUES (
                %(cloid)s, %(coin)s, %(side)s, %(size)s, %(limit_px)s, %(reduce_only)s, %(dry_run)s, %(account)s,
                %(positions_json)s::jsonb, %(response_json)s::jsonb, %(status)s, %(exchange_oid)s, %(error_message)s
            )
            """,
            {
                "cloid": item.cloid,
                "coin": item.coin,
                "side": item.side,
                "size": item.size,
                "limit_px": item.limit_px,
                "reduce_only": item.reduce_only,
                "dry_run": item.dry_run,
                "account": item.account.lower(),
                "positions_json": item.positions_json,
                "response_json": item.response_json,
                "status": item.normalized_status,
                "exchange_oid": item.exchange_oid,
                "error_message": item.error_message,
            },
        )

    def _do_fill_insert(self, conn: psycopg.Connection, item: _QFillInsert) -> None:
        f = item.fill
        hash_v = f.get("hash")
        if not hash_v:
            logger.warning("skip fill without hash")
            return
        hash_s = str(hash_v)
        tid = f.get("tid")
        oid = f.get("oid")
        coin = str(f.get("coin", ""))
        try:
            px = float(f.get("px", 0))
        except (TypeError, ValueError):
            px = 0.0
        try:
            sz = float(f.get("sz", 0))
        except (TypeError, ValueError):
            sz = 0.0
        try:
            closed_pnl = float(f.get("closedPnl", 0))
        except (TypeError, ValueError):
            closed_pnl = 0.0
        try:
            fee = float(f.get("fee", 0))
        except (TypeError, ValueError):
            fee = 0.0
        side = str(f.get("side", ""))
        try:
            exchange_time_ms = int(f.get("time", 0))
        except (TypeError, ValueError):
            exchange_time_ms = 0
        fee_token = str(f.get("feeToken", "")) if f.get("feeToken") is not None else None

        filled_at = datetime.fromtimestamp(exchange_time_ms / 1000.0, tz=timezone.utc)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fills (
                    account, exchange_oid, tid, hash, coin, px, sz, side, closed_pnl, fee, fee_token,
                    exchange_time_ms, filled_at, raw
                )
                VALUES (
                    %(account)s, %(oid)s, %(tid)s, %(hash)s, %(coin)s, %(px)s, %(sz)s, %(side)s, %(closed_pnl)s,
                    %(fee)s, %(fee_token)s, %(exchange_time_ms)s, %(filled_at)s, %(raw)s::jsonb
                )
                ON CONFLICT (hash) DO NOTHING
                RETURNING id
                """,
                {
                    "account": item.account,
                    "oid": int(oid) if oid is not None else None,
                    "tid": int(tid) if tid is not None else None,
                    "hash": hash_s,
                    "coin": coin,
                    "px": px,
                    "sz": sz,
                    "side": side,
                    "closed_pnl": closed_pnl,
                    "fee": fee,
                    "fee_token": fee_token,
                    "exchange_time_ms": exchange_time_ms,
                    "filled_at": filled_at,
                    "raw": json.dumps(f),
                },
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                if FILLS_INSERTED is not None:
                    FILLS_INSERTED.inc()
                if oid is not None:
                    cur.execute(
                        """
                        UPDATE fills AS f
                        SET order_id = o.id
                        FROM orders AS o
                        WHERE f.id = %(fid)s
                          AND f.order_id IS NULL
                          AND o.exchange_oid = %(oid)s
                          AND lower(o.account) = f.account
                        """,
                        {"fid": row[0], "oid": int(oid)},
                    )

    def _do_order_status(self, conn: psycopg.Connection, item: _QOrderStatus) -> None:
        row_status = map_hl_order_status_to_row_status(item.hl_status)
        conn.execute(
            """
            UPDATE orders
            SET hl_order_status = %(hl_status)s,
                status = %(row_status)s
            WHERE exchange_oid = %(oid)s AND lower(account) = %(account)s
            """,
            {
                "hl_status": item.hl_status,
                "row_status": row_status,
                "oid": item.exchange_oid,
                "account": item.account,
            },
        )
