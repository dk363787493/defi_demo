package main

import (
	"context"
	"flag"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	"github.com/zhangjinge/defi-lending-backend/internal/common/logger"
	"github.com/zhangjinge/defi-lending-backend/internal/liquidation"
	"github.com/zhangjinge/defi-lending-backend/internal/liquidation/executor"
	"github.com/zhangjinge/defi-lending-backend/internal/liquidation/ratecomputer"
	"github.com/zhangjinge/defi-lending-backend/internal/liquidation/scanner"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)
	log.Info().Msg("starting defi-lending liquidator")

	rc := ratecomputer.New()
	sc := scanner.New()

	// Placeholder sender/signer for Phase 3 — replaced with real KMS in production.
	exec := executor.New(
		executor.Config{
			ChainID:      big.NewInt(cfg.Chains[0].ChainID),
			GasLimit:     500000,
			MaxGasPrice:  big.NewInt(100e9),
			MinProfitWei: big.NewInt(1e15),
			MaxRetries:   3,
		},
		&stubSender{},
		&stubSigner{addr: common.HexToAddress("0x0000000000000000000000000000000000000000")},
		log,
	)

	svc := liquidation.NewService(rc, sc, exec, log, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("shutting down liquidator")
		cancel()
	}()

	if err := svc.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal().Err(err).Msg("liquidator exited with error")
	}
	log.Info().Msg("liquidator stopped")
}

// Stub implementations — replaced with real chain client + KMS in production.
type stubSender struct{}

func (s *stubSender) SuggestGasPrice(_ context.Context) (*big.Int, error) {
	return big.NewInt(20e9), nil
}
func (s *stubSender) PendingNonceAt(_ context.Context, _ common.Address) (uint64, error) {
	return 0, nil
}
func (s *stubSender) SendTransaction(_ context.Context, _ *types.Transaction) error {
	return nil
}

type stubSigner struct {
	addr common.Address
}

func (s *stubSigner) SignTx(tx *types.Transaction, _ *big.Int) (*types.Transaction, error) {
	return tx, nil
}
func (s *stubSigner) Address() common.Address {
	return s.addr
}
