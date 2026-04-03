CREATE TABLE IF NOT EXISTS markets (
    id                    BIGSERIAL PRIMARY KEY,
    chain_id              BIGINT NOT NULL REFERENCES chains(chain_id),
    asset_address         VARCHAR(42) NOT NULL,
    asset_symbol          VARCHAR(20) NOT NULL,
    asset_decimals        INT NOT NULL,
    pool_address          VARCHAR(42) NOT NULL,
    collateral_factor     NUMERIC(38,0) NOT NULL,
    liquidation_threshold NUMERIC(38,0) NOT NULL,
    liquidation_bonus     NUMERIC(38,0) NOT NULL DEFAULT 0,
    borrow_cap            NUMERIC(38,0) NOT NULL DEFAULT 0,
    supply_cap            NUMERIC(38,0) NOT NULL DEFAULT 0,
    status                VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain_id, asset_address)
);

CREATE INDEX idx_markets_chain_id ON markets(chain_id);
CREATE INDEX idx_markets_asset ON markets(asset_address);
