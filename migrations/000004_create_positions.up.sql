CREATE TABLE IF NOT EXISTS positions (
    id              BIGSERIAL PRIMARY KEY,
    chain_id        BIGINT NOT NULL,
    user_address    VARCHAR(42) NOT NULL,
    market_id       BIGINT NOT NULL REFERENCES markets(id),
    supply_balance  NUMERIC(78,0) NOT NULL DEFAULT 0,
    borrow_balance  NUMERIC(78,0) NOT NULL DEFAULT 0,
    supply_index    NUMERIC(78,0) NOT NULL DEFAULT 0,
    borrow_index    NUMERIC(78,0) NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain_id, user_address, market_id)
);

CREATE INDEX idx_positions_user ON positions(user_address);
CREATE INDEX idx_positions_market ON positions(market_id);
CREATE INDEX idx_positions_borrow ON positions(borrow_balance) WHERE borrow_balance > 0;
