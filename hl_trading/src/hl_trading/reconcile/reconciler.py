"""Compare exchange open orders vs Postgres rows (OIDs + status='open')."""

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
    api_oids: list[int] = []
    for o in api_orders:
        oid = o.get("oid")
        if oid is not None:
            api_oids.append(int(oid))
    api_set = set(api_oids)

    account = settings.account_address.lower()
    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT exchange_oid
                FROM orders
                WHERE lower(account) = %(account)s
                  AND dry_run = false
                  AND status = 'open'
                  AND exchange_oid IS NOT NULL
                """,
                {"account": account},
            )
            pg_open = {int(r[0]) for r in cur.fetchall() if r[0] is not None}

            cur.execute(
                """
                SELECT count(*) FROM orders
                WHERE lower(account) = %(account)s
                  AND dry_run = false
                  AND created_at > now() - interval '7 days'
                """,
                {"account": account},
            )
            pg_recent_orders = int(cur.fetchone()[0])

        missing_in_pg = sorted(api_set - pg_open)
        extra_in_pg = sorted(pg_open - api_set)
        reconcile_status = "ok" if not missing_in_pg and not extra_in_pg else "mismatch"

        payload: dict[str, Any] = {
            "account": settings.account_address,
            "api_open_order_count": len(api_orders),
            "api_open_oids": sorted(api_set),
            "postgres_open_oids": sorted(pg_open),
            "missing_in_pg": missing_in_pg,
            "extra_in_pg": extra_in_pg,
            "postgres_orders_7d_not_dry_run": pg_recent_orders,
        }
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO reconcile_runs (status, payload) VALUES (%s, %s)",
                (reconcile_status, Json(payload)),
            )
        conn.commit()

    if RECONCILE_RUNS is not None:
        RECONCILE_RUNS.labels(status=reconcile_status).inc()

    logger.info("reconcile %s", payload)
    return payload
