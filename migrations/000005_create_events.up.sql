CREATE TABLE IF NOT EXISTS events (
    id               BIGSERIAL PRIMARY KEY,
    chain_id         BIGINT NOT NULL,
    block_number     BIGINT NOT NULL,
    tx_hash          VARCHAR(66) NOT NULL,
    log_index        INT NOT NULL,
    event_type       VARCHAR(30) NOT NULL,
    contract_address VARCHAR(42) NOT NULL,
    market_id        BIGINT REFERENCES markets(id),
    user_address     VARCHAR(42),
    amount           NUMERIC(78,0),
    data             JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain_id, tx_hash, log_index)
);

CREATE INDEX idx_events_chain_block ON events(chain_id, block_number);
CREATE INDEX idx_events_user ON events(user_address);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_market ON events(market_id);
