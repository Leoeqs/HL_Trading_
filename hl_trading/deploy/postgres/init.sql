-- Postgres schema v2: orders (OIDs + status), fills (dedupe), PnL rollup, reconcile

CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    cloid TEXT,
    exchange_oid BIGINT,
    coin TEXT NOT NULL,
    side TEXT NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    limit_px DOUBLE PRECISION NOT NULL,
    reduce_only BOOLEAN NOT NULL DEFAULT FALSE,
    dry_run BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'submitted',
    hl_order_status TEXT,
    error_message TEXT,
    account TEXT NOT NULL,
    positions_json JSONB,
    exchange_response JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_account_created ON orders (account, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_account_exchange_oid ON orders (account, exchange_oid)
    WHERE exchange_oid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_open ON orders (account, exchange_oid)
    WHERE dry_run = false AND exchange_oid IS NOT NULL AND status = 'open';

CREATE TABLE IF NOT EXISTS fills (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT REFERENCES orders (id),
    account TEXT NOT NULL,
    exchange_oid BIGINT,
    tid BIGINT,
    hash TEXT NOT NULL,
    coin TEXT NOT NULL,
    px DOUBLE PRECISION NOT NULL,
    sz DOUBLE PRECISION NOT NULL,
    side TEXT,
    closed_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
    fee DOUBLE PRECISION NOT NULL DEFAULT 0,
    fee_token TEXT,
    exchange_time_ms BIGINT NOT NULL,
    filled_at TIMESTAMPTZ NOT NULL,
    raw JSONB NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_hash ON fills (hash);
CREATE INDEX IF NOT EXISTS idx_fills_account_time ON fills (account, filled_at DESC);
CREATE INDEX IF NOT EXISTS idx_fills_order ON fills (order_id);
CREATE INDEX IF NOT EXISTS idx_fills_exchange_oid ON fills (account, exchange_oid);

CREATE TABLE IF NOT EXISTS pnl_daily (
    account TEXT NOT NULL,
    day DATE NOT NULL,
    realized_pnl_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    fees_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    fill_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account, day)
);

CREATE TABLE IF NOT EXISTS pnl_snapshots (
    id BIGSERIAL PRIMARY KEY,
    account TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS reconcile_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL,
    payload JSONB NOT NULL
);
