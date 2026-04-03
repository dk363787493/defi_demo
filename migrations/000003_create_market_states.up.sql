CREATE TABLE IF NOT EXISTS market_states (
    market_id             BIGINT PRIMARY KEY REFERENCES markets(id),
    total_supply          NUMERIC(78,0) NOT NULL DEFAULT 0,
    total_borrow          NUMERIC(78,0) NOT NULL DEFAULT 0,
    supply_rate           NUMERIC(78,0) NOT NULL DEFAULT 0,
    borrow_rate           NUMERIC(78,0) NOT NULL DEFAULT 0,
    liquidity_index       NUMERIC(78,0) NOT NULL DEFAULT 1000000000000000000000000000,
    borrow_index          NUMERIC(78,0) NOT NULL DEFAULT 1000000000000000000000000000,
    last_update_timestamp BIGINT NOT NULL DEFAULT 0,
    utilization_rate      NUMERIC(78,0) NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
