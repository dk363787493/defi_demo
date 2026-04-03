CREATE TABLE IF NOT EXISTS blocks (
    chain_id     BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash   VARCHAR(66) NOT NULL,
    parent_hash  VARCHAR(66) NOT NULL,
    timestamp    TIMESTAMPTZ NOT NULL,
    is_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, block_number)
);

CREATE INDEX idx_blocks_hash ON blocks(block_hash);
