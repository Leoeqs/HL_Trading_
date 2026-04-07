-- Apply if your DB was created from the older init (pre OID / fills v2). Run once:
--   psql "$POSTGRES_DSN" -f deploy/postgres/migrate_001_v2.sql

ALTER TABLE orders ADD COLUMN IF NOT EXISTS exchange_oid BIGINT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS hl_order_status TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS error_message TEXT;

CREATE INDEX IF NOT EXISTS idx_orders_account_exchange_oid ON orders (account, exchange_oid)
    WHERE exchange_oid IS NOT NULL;

-- If `fills` already exists with old shape, rename backup and recreate, or migrate manually.
-- Safer path for dev: dump data, drop fills, re-run init.sql fills section.
