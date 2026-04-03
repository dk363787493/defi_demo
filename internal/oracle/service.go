package oracle

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog/log"
)

// PriceFetcher abstracts the price feed reading.
type PriceFetcher interface {
	FetchPrice(ctx context.Context, feed PriceFeedConfig) (*PriceData, error)
}

// PricePublisher publishes price updates to downstream consumers.
type PricePublisher interface {
	PublishPrice(ctx context.Context, price *PriceData) error
}

// PriceStore persists prices for historical queries.
type PriceStore interface {
	SavePrice(ctx context.Context, chainID int64, price *PriceData) error
}

// Service orchestrates price fetching, caching, deviation detection, and publishing.
type Service struct {
	fetcher   PriceFetcher
	cache     *PriceCache
	publisher PricePublisher
	store     PriceStore
	chainID   int64
}

func NewService(fetcher PriceFetcher, cache *PriceCache, publisher PricePublisher, store PriceStore, chainID int64) *Service {
	return &Service{
		fetcher:   fetcher,
		cache:     cache,
		publisher: publisher,
		store:     store,
		chainID:   chainID,
	}
}

// FetchAndUpdate fetches prices for all configured feeds, caches them, checks deviations,
// publishes to Kafka, and persists to DB.
func (s *Service) FetchAndUpdate(ctx context.Context, feeds []PriceFeedConfig) error {
	for _, feed := range feeds {
		price, err := s.fetcher.FetchPrice(ctx, feed)
		if err != nil {
			log.Error().Err(err).Str("asset", feed.AssetSymbol).Msg("failed to fetch price")
			continue
		}

		// Check deviation against cached price.
		if cached, err := s.cache.Get(ctx, feed.AssetAddress); err == nil {
			deviation := CalculateDeviation(cached.PriceUSD, price.PriceUSD)
			if deviation > feed.DeviationPct && feed.DeviationPct > 0 {
				log.Warn().
					Str("asset", feed.AssetSymbol).
					Float64("deviation_pct", deviation).
					Str("old_price", cached.PriceUSD.String()).
					Str("new_price", price.PriceUSD.String()).
					Msg("price deviation alert")
			}
		}

		// Check heartbeat.
		if feed.HeartbeatSec > 0 {
			staleness := time.Since(price.UpdatedAt).Seconds()
			if staleness > float64(feed.HeartbeatSec) {
				log.Warn().
					Str("asset", feed.AssetSymbol).
					Float64("staleness_sec", staleness).
					Msg("price feed heartbeat stale")
			}
		}

		// Cache with TTL.
		ttl := 30 * time.Second
		if feed.HeartbeatSec > 0 && feed.HeartbeatSec < 30 {
			ttl = time.Duration(feed.HeartbeatSec) * time.Second
		}
		if err := s.cache.Set(ctx, price, ttl); err != nil {
			log.Error().Err(err).Str("asset", feed.AssetSymbol).Msg("failed to cache price")
		}

		// Publish to Kafka.
		if err := s.publisher.PublishPrice(ctx, price); err != nil {
			log.Error().Err(err).Str("asset", feed.AssetSymbol).Msg("failed to publish price")
		}

		// Persist to DB.
		if err := s.store.SavePrice(ctx, s.chainID, price); err != nil {
			log.Error().Err(err).Str("asset", feed.AssetSymbol).Msg("failed to save price")
		}
	}
	return nil
}

// GetPrice returns the cached price for an asset.
func (s *Service) GetPrice(ctx context.Context, asset common.Address) (*PriceData, error) {
	if cached, err := s.cache.Get(ctx, asset); err == nil {
		return cached, nil
	}
	return nil, fmt.Errorf("no cached price for %s", asset.Hex())
}

// GetPrices returns cached prices for multiple assets.
func (s *Service) GetPrices(ctx context.Context, assets []common.Address) map[common.Address]*PriceData {
	return s.cache.GetAll(ctx, assets)
}

// CalculateDeviation returns the percentage deviation between old and new price.
func CalculateDeviation(oldPrice, newPrice *big.Int) float64 {
	if oldPrice.Sign() == 0 {
		return 0
	}
	oldF := new(big.Float).SetInt(oldPrice)
	newF := new(big.Float).SetInt(newPrice)
	diff := new(big.Float).Sub(newF, oldF)
	ratio := new(big.Float).Quo(diff, oldF)
	result, _ := ratio.Float64()
	return math.Abs(result * 100)
}

// RunLoop starts a polling loop that fetches prices at the given interval.
func (s *Service) RunLoop(ctx context.Context, feeds []PriceFeedConfig, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	_ = s.FetchAndUpdate(ctx, feeds)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.FetchAndUpdate(ctx, feeds)
		}
	}
}
