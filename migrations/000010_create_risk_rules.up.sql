CREATE TABLE IF NOT EXISTS risk_rules (
    id          BIGSERIAL PRIMARY KEY,
    type        VARCHAR(30) NOT NULL,
    name        VARCHAR(100) NOT NULL,
    description TEXT,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    severity    VARCHAR(20) NOT NULL DEFAULT 'warning',
    params      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO risk_rules (type, name, description, severity, params) VALUES
('large_transaction', 'Large Transaction Alert', 'Alert when a single tx exceeds % of pool TVL', 'warning', '{"threshold_pct": "5"}'),
('high_utilization', 'High Utilization Alert', 'Alert when pool utilization exceeds threshold', 'critical', '{"threshold_pct": "95"}'),
('flash_loan', 'Flash Loan Detection', 'Detect multiple borrow+repay in same block', 'critical', '{"min_ops_per_block": "3"}'),
('protocol_health', 'Protocol Health Monitor', 'Monitor overall collateral ratio', 'warning', '{"min_collateral_ratio": "1.5"}');
