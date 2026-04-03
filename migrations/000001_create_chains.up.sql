CREATE TABLE IF NOT EXISTS chains (
    id            BIGSERIAL PRIMARY KEY,
    name          VARCHAR(50) NOT NULL UNIQUE,
    chain_id      BIGINT NOT NULL UNIQUE,
    rpc_urls      TEXT[] NOT NULL,
    ws_url        TEXT NOT NULL,
    block_time    INT NOT NULL DEFAULT 12,
    confirmations INT NOT NULL DEFAULT 12,
    status        VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_chains_chain_id ON chains(chain_id);
