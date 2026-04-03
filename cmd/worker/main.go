package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	"github.com/zhangjinge/defi-lending-backend/internal/common/logger"
	"github.com/zhangjinge/defi-lending-backend/internal/notification"
	"github.com/zhangjinge/defi-lending-backend/internal/risk"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)
	log.Info().Msg("starting defi-lending worker")

	// Initialize notification service.
	notifStore := &stubNotifStore{}
	webhookSender := &stubWebhookSender{}
	notifSvc := notification.NewService(
		notifStore,
		[]notification.Sender{webhookSender},
		log,
		5*time.Minute, // cooldown
	)

	// Initialize risk engine with notification bridge.
	ruleStore := &stubRuleStore{}
	riskEventStore := &stubRiskEventStore{}
	alertBridge := &notificationAlertBridge{notifSvc: notifSvc, logger: log}

	riskEngine := risk.NewEngine(ruleStore, riskEventStore, alertBridge, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load rules on startup.
	if err := riskEngine.ReloadRules(ctx); err != nil {
		log.Error().Err(err).Msg("failed to load risk rules")
	}

	// Periodically reload rules (support dynamic updates).
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := riskEngine.ReloadRules(ctx); err != nil {
					log.Error().Err(err).Msg("failed to reload risk rules")
				}
			}
		}
	}()

	log.Info().Msg("worker running — risk engine and notification service active")

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info().Str("signal", sig.String()).Msg("shutting down worker")
	cancel()
	log.Info().Msg("worker stopped")
}

// notificationAlertBridge routes risk events to the notification service.
type notificationAlertBridge struct {
	notifSvc *notification.Service
	logger   zerolog.Logger
}

func (b *notificationAlertBridge) HandleAlert(ctx context.Context, event *risk.Event) {
	n := &notification.Notification{
		Type:    notification.TypeRiskAlert,
		Channel: notification.ChannelWebhook,
		Title:   "Risk Alert: " + string(event.RuleType),
		Content: event.Description,
		Data:    event.Data,
	}
	if err := b.notifSvc.Notify(ctx, n); err != nil {
		b.logger.Error().Err(err).Msg("failed to send risk alert notification")
	}
}

// Stub implementations — replaced with real PostgreSQL stores in production.
type stubNotifStore struct{ nextID int64 }

func (s *stubNotifStore) Save(_ context.Context, n *notification.Notification) error {
	s.nextID++
	n.ID = s.nextID
	return nil
}
func (s *stubNotifStore) UpdateStatus(_ context.Context, _ int64, _ notification.Status) error {
	return nil
}

type stubWebhookSender struct{}

func (s *stubWebhookSender) Channel() notification.Channel { return notification.ChannelWebhook }
func (s *stubWebhookSender) Send(_ context.Context, _ *notification.Notification) error {
	return nil
}

type stubRuleStore struct{}

func (s *stubRuleStore) ListEnabledRules(_ context.Context) ([]risk.Rule, error) {
	return []risk.Rule{
		{ID: 1, Type: risk.RuleLargeTransaction, Name: "Large Tx", Enabled: true, Severity: risk.SeverityWarning, Params: map[string]string{"threshold_pct": "5"}},
		{ID: 2, Type: risk.RuleHighUtilization, Name: "High Util", Enabled: true, Severity: risk.SeverityCritical, Params: map[string]string{"threshold_pct": "95"}},
		{ID: 3, Type: risk.RuleFlashLoan, Name: "Flash Loan", Enabled: true, Severity: risk.SeverityCritical, Params: map[string]string{"min_ops_per_block": "3"}},
	}, nil
}

type stubRiskEventStore struct{}

func (s *stubRiskEventStore) SaveEvent(_ context.Context, _ *risk.Event) error { return nil }
