"""Compare exchange `user_state` open orders with PostgreSQL journal (best-effort)."""

from __future__ import annotations

import logging
from typing import Any

import psycopg
from psycopg.types.json import Json

from hl_trading.adapters.hyperliquid_factory import create_info_rest_only
from hl_trading.config import Settings
from hl_trading.metrics import RECONCILE_RUNS

logger = logging.getLogger(__name__)


def run_reconcile_once(settings: Settings) -> dict[str, Any]:
    if not settings.postgres_dsn:
        raise SystemExit("POSTGRES_DSN is required for reconcile")

    info = create_info_rest_only(settings)
    raw = info.user_state(settings.account_address)
    api_orders = raw.get("openOrders") or []
    api_oids = []
    for o in api_orders:
        oid = o.get("oid")
        if oid is not None:
            api_oids.append(int(oid))

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM orders
                WHERE dry_run = false AND created_at > now() - interval '7 days'
                """
            )
            pg_recent = int(cur.fetchone()[0])

        payload: dict[str, Any] = {
            "account": settings.account_address,
            "api_open_order_count": len(api_orders),
            "api_oids_sample": api_oids[:50],
            "postgres_orders_7d_not_dry_run": pg_recent,
        }
        status = "ok"
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO reconcile_runs (status, payload) VALUES (%s, %s)",
                (status, Json(payload)),
            )
        conn.commit()

    if RECONCILE_RUNS is not None:
        RECONCILE_RUNS.labels(status=status).inc()

    logger.info("reconcile %s", payload)
    return payload
