CREATE TABLE IF NOT EXISTS prices (
    id            BIGSERIAL PRIMARY KEY,
    asset_address VARCHAR(42) NOT NULL,
    chain_id      BIGINT NOT NULL,
    price_usd     NUMERIC(78,0) NOT NULL,
    decimals      INT NOT NULL DEFAULT 8,
    source        VARCHAR(20) NOT NULL,
    round_id      NUMERIC(78,0),
    timestamp     TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_prices_asset_time ON prices(asset_address, timestamp DESC);
CREATE INDEX idx_prices_chain ON prices(chain_id);
