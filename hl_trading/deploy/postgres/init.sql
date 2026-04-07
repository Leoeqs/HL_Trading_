-- Canonical order lifecycle + reconcile + optional PnL snapshots (perps-focused stack)

CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    cloid TEXT,
    coin TEXT NOT NULL,
    side TEXT NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    limit_px DOUBLE PRECISION NOT NULL,
    reduce_only BOOLEAN NOT NULL DEFAULT FALSE,
    dry_run BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'submitted',
    account TEXT NOT NULL,
    positions_json JSONB,
    exchange_response JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_account_created ON orders (account, created_at DESC);

CREATE TABLE IF NOT EXISTS fills (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT REFERENCES orders (id),
    coin TEXT,
    px DOUBLE PRECISION,
    sz DOUBLE PRECISION,
    fee DOUBLE PRECISION,
    raw JSONB,
    filled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fills_order ON fills (order_id);

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
