package risk

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// RuleStore loads and persists risk rules.
type RuleStore interface {
	ListEnabledRules(ctx context.Context) ([]Rule, error)
}

// EventStore persists risk events.
type EventStore interface {
	SaveEvent(ctx context.Context, event *Event) error
}

// AlertSink receives risk events for downstream processing (notifications, circuit breaker).
type AlertSink interface {
	HandleAlert(ctx context.Context, event *Event)
}

// Engine evaluates risk rules against incoming protocol events.
type Engine struct {
	mu         sync.RWMutex
	rules      []Rule
	ruleStore  RuleStore
	eventStore EventStore
	alertSink  AlertSink
	logger     zerolog.Logger
}

func NewEngine(ruleStore RuleStore, eventStore EventStore, alertSink AlertSink, logger zerolog.Logger) *Engine {
	return &Engine{
		ruleStore:  ruleStore,
		eventStore: eventStore,
		alertSink:  alertSink,
		logger:     logger,
	}
}

// ReloadRules fetches enabled rules from the store.
func (e *Engine) ReloadRules(ctx context.Context) error {
	rules, err := e.ruleStore.ListEnabledRules(ctx)
	if err != nil {
		return fmt.Errorf("load rules: %w", err)
	}
	e.mu.Lock()
	e.rules = rules
	e.mu.Unlock()
	e.logger.Info().Int("count", len(rules)).Msg("risk rules reloaded")
	return nil
}

// Rules returns the current rules (snapshot).
func (e *Engine) Rules() []Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := make([]Rule, len(e.rules))
	copy(cp, e.rules)
	return cp
}

// CheckLargeTransaction evaluates if a transaction amount exceeds the configured
// threshold as a percentage of pool TVL.
func (e *Engine) CheckLargeTransaction(ctx context.Context, chainID int64, marketID int64, txHash string, amount *big.Int, poolTVL *big.Int) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, rule := range e.rules {
		if rule.Type != RuleLargeTransaction || !rule.Enabled {
			continue
		}

		thresholdPctStr, ok := rule.Params["threshold_pct"]
		if !ok {
			continue
		}
		thresholdPct, err := strconv.ParseFloat(thresholdPctStr, 64)
		if err != nil {
			continue
		}

		if poolTVL.Sign() == 0 {
			continue
		}

		// ratio = amount / poolTVL * 100
		amountF := new(big.Float).SetInt(amount)
		tvlF := new(big.Float).SetInt(poolTVL)
		ratio := new(big.Float).Quo(amountF, tvlF)
		pct, _ := ratio.Float64()
		pct *= 100

		if pct >= thresholdPct {
			e.emitEvent(ctx, &Event{
				RuleID:      rule.ID,
				RuleType:    RuleLargeTransaction,
				Severity:    rule.Severity,
				ChainID:     chainID,
				MarketID:    marketID,
				Description: fmt.Sprintf("Large transaction detected: %.2f%% of pool TVL", pct),
				RelatedTx:   txHash,
				Data:        map[string]string{"amount": amount.String(), "tvl": poolTVL.String(), "pct": fmt.Sprintf("%.2f", pct)},
				CreatedAt:   time.Now(),
			})
		}
	}
}

// CheckUtilization evaluates if pool utilization exceeds the threshold.
func (e *Engine) CheckUtilization(ctx context.Context, chainID int64, marketID int64, utilizationPct float64) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, rule := range e.rules {
		if rule.Type != RuleHighUtilization || !rule.Enabled {
			continue
		}

		thresholdStr, ok := rule.Params["threshold_pct"]
		if !ok {
			continue
		}
		threshold, err := strconv.ParseFloat(thresholdStr, 64)
		if err != nil {
			continue
		}

		if utilizationPct >= threshold {
			e.emitEvent(ctx, &Event{
				RuleID:      rule.ID,
				RuleType:    RuleHighUtilization,
				Severity:    rule.Severity,
				ChainID:     chainID,
				MarketID:    marketID,
				Description: fmt.Sprintf("High utilization: %.2f%% (threshold: %.2f%%)", utilizationPct, threshold),
				Data:        map[string]string{"utilization": fmt.Sprintf("%.2f", utilizationPct)},
				CreatedAt:   time.Now(),
			})
		}
	}
}

// CheckFlashLoan detects potential flash loan patterns:
// multiple borrow+repay events from the same user in the same block.
func (e *Engine) CheckFlashLoan(ctx context.Context, chainID int64, blockNumber uint64, userTxCounts map[string]int) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, rule := range e.rules {
		if rule.Type != RuleFlashLoan || !rule.Enabled {
			continue
		}

		minOpsStr, ok := rule.Params["min_ops_per_block"]
		if !ok {
			continue
		}
		minOps, err := strconv.Atoi(minOpsStr)
		if err != nil {
			continue
		}

		for user, count := range userTxCounts {
			if count >= minOps {
				e.emitEvent(ctx, &Event{
					RuleID:      rule.ID,
					RuleType:    RuleFlashLoan,
					Severity:    rule.Severity,
					ChainID:     chainID,
					Description: fmt.Sprintf("Possible flash loan: user %s had %d operations in block %d", user, count, blockNumber),
					Data:        map[string]string{"user": user, "block": fmt.Sprintf("%d", blockNumber), "ops": fmt.Sprintf("%d", count)},
					CreatedAt:   time.Now(),
				})
			}
		}
	}
}

func (e *Engine) emitEvent(ctx context.Context, event *Event) {
	e.logger.Warn().
		Str("rule_type", string(event.RuleType)).
		Str("severity", string(event.Severity)).
		Str("description", event.Description).
		Msg("risk event triggered")

	if err := e.eventStore.SaveEvent(ctx, event); err != nil {
		e.logger.Error().Err(err).Msg("failed to save risk event")
	}

	if e.alertSink != nil {
		e.alertSink.HandleAlert(ctx, event)
	}
}
