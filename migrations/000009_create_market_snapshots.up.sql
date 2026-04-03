CREATE TABLE IF NOT EXISTS market_snapshots (
    id                BIGSERIAL PRIMARY KEY,
    market_id         BIGINT NOT NULL REFERENCES markets(id),
    total_supply      NUMERIC(78,0) NOT NULL,
    total_borrow      NUMERIC(78,0) NOT NULL,
    supply_rate       NUMERIC(78,0) NOT NULL,
    borrow_rate       NUMERIC(78,0) NOT NULL,
    utilization_rate  NUMERIC(78,0) NOT NULL,
    tvl_usd           NUMERIC(78,0) NOT NULL DEFAULT 0,
    snapshot_time     TIMESTAMPTZ NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_snapshots_market_time ON market_snapshots(market_id, snapshot_time DESC);
