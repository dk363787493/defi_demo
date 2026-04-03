CREATE TABLE IF NOT EXISTS risk_events (
    id          BIGSERIAL PRIMARY KEY,
    rule_id     BIGINT REFERENCES risk_rules(id),
    rule_type   VARCHAR(30) NOT NULL,
    severity    VARCHAR(20) NOT NULL,
    chain_id    BIGINT,
    market_id   BIGINT,
    description TEXT NOT NULL,
    related_tx  VARCHAR(66),
    data        JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_risk_events_severity ON risk_events(severity);
CREATE INDEX idx_risk_events_time ON risk_events(created_at DESC);
CREATE INDEX idx_risk_events_rule ON risk_events(rule_type);
