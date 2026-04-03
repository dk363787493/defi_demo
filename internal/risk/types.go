package risk

import (
	"math/big"
	"time"
)

// Severity levels for risk events.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// RuleType identifies the kind of risk rule.
type RuleType string

const (
	RuleLargeTransaction RuleType = "large_transaction"
	RuleHighUtilization  RuleType = "high_utilization"
	RulePriceDeviation   RuleType = "price_deviation"
	RuleFlashLoan        RuleType = "flash_loan"
	RuleProtocolHealth   RuleType = "protocol_health"
)

// Rule is a configurable risk detection rule.
type Rule struct {
	ID          int64    `json:"id"`
	Type        RuleType `json:"type"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Enabled     bool     `json:"enabled"`
	Severity    Severity `json:"severity"`
	Params      map[string]string `json:"params"` // rule-specific thresholds
}

// Event is a triggered risk alert.
type Event struct {
	ID          int64     `json:"id"`
	RuleID      int64     `json:"rule_id"`
	RuleType    RuleType  `json:"rule_type"`
	Severity    Severity  `json:"severity"`
	ChainID     int64     `json:"chain_id"`
	MarketID    int64     `json:"market_id,omitempty"`
	Description string    `json:"description"`
	RelatedTx   string    `json:"related_tx,omitempty"`
	Data        map[string]string `json:"data,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// ProtocolHealthSnapshot captures overall protocol metrics.
type ProtocolHealthSnapshot struct {
	TotalCollateralUSD *big.Int `json:"total_collateral_usd"`
	TotalDebtUSD       *big.Int `json:"total_debt_usd"`
	OverallCollateralRatio float64 `json:"overall_collateral_ratio"`
	BadDebtUSD         *big.Int `json:"bad_debt_usd"`
	PoolCount          int      `json:"pool_count"`
}
