package risk

import (
	"context"
	"math/big"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRuleStore struct {
	rules []Rule
}

func (m *mockRuleStore) ListEnabledRules(_ context.Context) ([]Rule, error) {
	return m.rules, nil
}

type mockEventStore struct {
	events []*Event
}

func (m *mockEventStore) SaveEvent(_ context.Context, event *Event) error {
	m.events = append(m.events, event)
	return nil
}

type mockAlertSink struct {
	alerts []*Event
}

func (m *mockAlertSink) HandleAlert(_ context.Context, event *Event) {
	m.alerts = append(m.alerts, event)
}

func newTestEngine(rules []Rule) (*Engine, *mockEventStore, *mockAlertSink) {
	ruleStore := &mockRuleStore{rules: rules}
	eventStore := &mockEventStore{}
	alertSink := &mockAlertSink{}
	engine := NewEngine(ruleStore, eventStore, alertSink, zerolog.Nop())
	_ = engine.ReloadRules(context.Background())
	return engine, eventStore, alertSink
}

func TestEngine_ReloadRules(t *testing.T) {
	rules := []Rule{
		{ID: 1, Type: RuleLargeTransaction, Enabled: true, Severity: SeverityWarning},
		{ID: 2, Type: RuleHighUtilization, Enabled: true, Severity: SeverityCritical},
	}
	engine, _, _ := newTestEngine(rules)
	assert.Len(t, engine.Rules(), 2)
}

func TestEngine_CheckLargeTransaction_Triggered(t *testing.T) {
	rules := []Rule{{
		ID: 1, Type: RuleLargeTransaction, Enabled: true, Severity: SeverityWarning,
		Params: map[string]string{"threshold_pct": "5"},
	}}
	engine, eventStore, alertSink := newTestEngine(rules)

	// 100 / 1000 = 10% > 5% threshold
	amount := big.NewInt(100)
	tvl := big.NewInt(1000)
	engine.CheckLargeTransaction(context.Background(), 1, 1, "0xabc", amount, tvl)

	assert.Len(t, eventStore.events, 1)
	assert.Equal(t, RuleLargeTransaction, eventStore.events[0].RuleType)
	assert.Len(t, alertSink.alerts, 1)
}

func TestEngine_CheckLargeTransaction_NotTriggered(t *testing.T) {
	rules := []Rule{{
		ID: 1, Type: RuleLargeTransaction, Enabled: true, Severity: SeverityWarning,
		Params: map[string]string{"threshold_pct": "20"},
	}}
	engine, eventStore, _ := newTestEngine(rules)

	// 10 / 1000 = 1% < 20% threshold
	engine.CheckLargeTransaction(context.Background(), 1, 1, "0xabc", big.NewInt(10), big.NewInt(1000))
	assert.Len(t, eventStore.events, 0)
}

func TestEngine_CheckUtilization_Triggered(t *testing.T) {
	rules := []Rule{{
		ID: 2, Type: RuleHighUtilization, Enabled: true, Severity: SeverityCritical,
		Params: map[string]string{"threshold_pct": "90"},
	}}
	engine, eventStore, _ := newTestEngine(rules)

	engine.CheckUtilization(context.Background(), 1, 1, 95.0)
	require.Len(t, eventStore.events, 1)
	assert.Equal(t, SeverityCritical, eventStore.events[0].Severity)
}

func TestEngine_CheckUtilization_NotTriggered(t *testing.T) {
	rules := []Rule{{
		ID: 2, Type: RuleHighUtilization, Enabled: true, Severity: SeverityCritical,
		Params: map[string]string{"threshold_pct": "90"},
	}}
	engine, eventStore, _ := newTestEngine(rules)

	engine.CheckUtilization(context.Background(), 1, 1, 85.0)
	assert.Len(t, eventStore.events, 0)
}

func TestEngine_CheckFlashLoan(t *testing.T) {
	rules := []Rule{{
		ID: 3, Type: RuleFlashLoan, Enabled: true, Severity: SeverityCritical,
		Params: map[string]string{"min_ops_per_block": "3"},
	}}
	engine, eventStore, _ := newTestEngine(rules)

	userCounts := map[string]int{
		"0xattacker": 5, // 5 ops >= 3 threshold
		"0xnormal":   1, // 1 op < 3
	}
	engine.CheckFlashLoan(context.Background(), 1, 100, userCounts)
	require.Len(t, eventStore.events, 1)
	assert.Contains(t, eventStore.events[0].Description, "0xattacker")
}

func TestEngine_DisabledRule(t *testing.T) {
	rules := []Rule{{
		ID: 1, Type: RuleLargeTransaction, Enabled: false, Severity: SeverityWarning,
		Params: map[string]string{"threshold_pct": "1"},
	}}
	engine, eventStore, _ := newTestEngine(rules)

	engine.CheckLargeTransaction(context.Background(), 1, 1, "0x", big.NewInt(999), big.NewInt(1000))
	assert.Len(t, eventStore.events, 0)
}
