package liquidation

import (
	"context"
	"math/big"
	"time"

	"github.com/rs/zerolog"
	"github.com/zhangjinge/defi-lending-backend/internal/liquidation/executor"
	"github.com/zhangjinge/defi-lending-backend/internal/liquidation/ratecomputer"
	"github.com/zhangjinge/defi-lending-backend/internal/liquidation/scanner"
)

// Service orchestrates the liquidation engine: rate computation, position scanning, and execution.
type Service struct {
	rateComputer *ratecomputer.RateComputer
	scanner      *scanner.Scanner
	executor     *executor.Executor
	logger       zerolog.Logger
	scanInterval time.Duration
}

func NewService(
	rc *ratecomputer.RateComputer,
	sc *scanner.Scanner,
	exec *executor.Executor,
	logger zerolog.Logger,
	scanInterval time.Duration,
) *Service {
	return &Service{
		rateComputer: rc,
		scanner:      sc,
		executor:     exec,
		logger:       logger,
		scanInterval: scanInterval,
	}
}

// RateComputer returns the rate computer for external calibration.
func (s *Service) RateComputer() *ratecomputer.RateComputer {
	return s.rateComputer
}

// Scanner returns the position scanner for external position updates.
func (s *Service) Scanner() *scanner.Scanner {
	return s.scanner
}

// Run starts the liquidation loop. Blocks until ctx is cancelled.
func (s *Service) Run(ctx context.Context) error {
	s.logger.Info().
		Dur("scan_interval", s.scanInterval).
		Msg("liquidation engine started")

	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info().Msg("liquidation engine stopped")
			return ctx.Err()
		case <-ticker.C:
			s.scan(ctx)
		}
	}
}

func (s *Service) scan(ctx context.Context) {
	now := time.Now()

	// Refresh all position health factors with current prices and extrapolated indices.
	s.scanner.RefreshAll(s.rateComputer, now)

	// Find liquidatable positions.
	candidates := s.scanner.FindLiquidatable()
	if len(candidates) == 0 {
		return
	}

	s.logger.Info().Int("count", len(candidates)).Msg("found liquidatable positions")

	for _, pos := range candidates {
		if ctx.Err() != nil {
			return
		}

		if len(pos.Debts) == 0 || len(pos.Collaterals) == 0 {
			continue
		}

		// Pick the largest debt and largest collateral for liquidation.
		debt := pos.Debts[0]
		collateral := pos.Collaterals[0]

		// Max liquidatable = 50% of debt (Aave close factor).
		halfDebt := new(big.Int).Div(debt.BorrowBalance, big.NewInt(2))

		s.logger.Info().
			Str("borrower", pos.UserAddress.Hex()).
			Str("health_factor", pos.HealthFactor.String()).
			Str("debt_amount", halfDebt.String()).
			Msg("executing liquidation")

		_, err := s.executor.Execute(
			ctx,
			collateral.AssetAddress,
			debt.AssetAddress,
			pos.UserAddress,
			halfDebt,
		)
		if err != nil {
			s.logger.Error().Err(err).
				Str("borrower", pos.UserAddress.Hex()).
				Msg("liquidation execution failed")
		}
	}
}
