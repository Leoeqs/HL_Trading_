"""Recompute `pnl_daily` from `fills` for a rolling UTC calendar-day window."""

from __future__ import annotations

import logging

import psycopg

from hl_trading.config import Settings

logger = logging.getLogger(__name__)


def rollup_pnl_daily(settings: Settings, *, lookback_days: int = 30) -> int:
    """
    Delete `pnl_daily` rows for ``account`` in the lookback window, then rebuild from ``fills``.
    Returns number of daily rows inserted.
    """
    if not settings.postgres_dsn:
        raise SystemExit("POSTGRES_DSN is required")
    account = settings.account_address.lower()
    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM pnl_daily
                WHERE account = %(account)s
                  AND day >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date - %(days)s::integer
                """,
                {"account": account, "days": lookback_days},
            )
            cur.execute(
                """
                INSERT INTO pnl_daily (account, day, realized_pnl_usd, fees_usd, fill_count, updated_at)
                SELECT account,
                       (filled_at AT TIME ZONE 'UTC')::date AS day,
                       COALESCE(SUM(closed_pnl), 0)::double precision,
                       COALESCE(SUM(fee), 0)::double precision,
                       COUNT(*)::integer,
                       NOW()
                FROM fills
                WHERE account = %(account)s
                  AND (filled_at AT TIME ZONE 'UTC')::date
                      >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date - %(days)s::integer
                GROUP BY account, (filled_at AT TIME ZONE 'UTC')::date
                """,
                {"account": account, "days": lookback_days},
            )
            n = cur.rowcount
        conn.commit()
    logger.info("pnl_daily rollup rows=%s account=%s lookback_days=%s", n, account, lookback_days)
    return n
