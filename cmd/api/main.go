package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/zhangjinge/defi-lending-backend/internal/account"
	"github.com/zhangjinge/defi-lending-backend/internal/api"
	"github.com/zhangjinge/defi-lending-backend/internal/api/middleware"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	"github.com/zhangjinge/defi-lending-backend/internal/common/logger"
	"github.com/zhangjinge/defi-lending-backend/internal/market"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)
	log.Info().Msg("starting defi-lending api server")

	authCfg := middleware.AuthConfig{
		Secret:     os.Getenv("JWT_SECRET"),
		Issuer:     "defi-lending",
		Expiration: 24 * time.Hour,
	}
	if authCfg.Secret == "" {
		authCfg.Secret = "dev-secret-change-in-production"
		log.Warn().Msg("using default JWT secret — set JWT_SECRET in production")
	}

	// Placeholder DB implementations — replaced with real PostgreSQL in production.
	marketDB := &stubMarketDB{}
	accountDB := &stubAccountDB{}

	marketRepo := market.NewRepository(marketDB)
	marketCache := &stubCache{}
	marketSvc := market.NewService(marketRepo, marketCache)
	marketHandler := market.NewHandler(marketSvc)

	accountSvc := account.NewService(accountDB)
	poolAddr := common.HexToAddress(cfg.Chains[0].Contracts.LendingPool)
	accountHandler := account.NewHandler(accountSvc, authCfg, poolAddr)

	router := api.NewRouter(api.RouterConfig{
		MarketHandler:  marketHandler,
		AccountHandler: accountHandler,
		Logger:         log,
		RateLimitCfg:   middleware.RateLimitConfig{RequestsPerMinute: 60},
	})

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server.
	go func() {
		log.Info().Str("addr", addr).Msg("API server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("server error")
		}
	}()

	// Graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info().Str("signal", sig.String()).Msg("shutting down api server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("server shutdown error")
	}
	log.Info().Msg("api server stopped")
}

// Stub implementations for Phase 4 — replaced with real PostgreSQL store.
type stubMarketDB struct{}

func (s *stubMarketDB) ListMarkets(_ context.Context, _ int64) ([]market.MarketInfo, error) {
	return nil, nil
}
func (s *stubMarketDB) GetMarketState(_ context.Context, _ int64) (*market.MarketStateInfo, error) {
	return nil, fmt.Errorf("not found")
}
func (s *stubMarketDB) GetMarketSnapshots(_ context.Context, _ int64, _ string, _ string) ([]market.MarketSnapshot, error) {
	return nil, nil
}

type stubAccountDB struct{}

func (s *stubAccountDB) GetPositions(_ context.Context, _ string, _ int64) ([]account.PositionSummary, error) {
	return nil, nil
}
func (s *stubAccountDB) GetTxHistory(_ context.Context, _ string, _ int64, _ int, _ int) ([]account.TxHistoryEntry, error) {
	return nil, nil
}

type stubCache struct{}

func (s *stubCache) Get(_ context.Context, _ string) (string, error)                     { return "", fmt.Errorf("miss") }
func (s *stubCache) Set(_ context.Context, _ string, _ string, _ time.Duration) error { return nil }
