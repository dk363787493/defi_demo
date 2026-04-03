CREATE TABLE IF NOT EXISTS sync_status (
    chain_id             BIGINT PRIMARY KEY,
    last_indexed_block   BIGINT NOT NULL DEFAULT 0,
    last_confirmed_block BIGINT NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
