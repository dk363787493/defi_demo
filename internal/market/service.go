package market

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// CacheClient abstracts Redis for market data caching.
type CacheClient interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
}

// Service provides market data with Redis caching.
type Service struct {
	repo  *Repository
	cache CacheClient
}

func NewService(repo *Repository, cache CacheClient) *Service {
	return &Service{repo: repo, cache: cache}
}

func (s *Service) ListMarkets(ctx context.Context, chainID int64) ([]MarketInfo, error) {
	cacheKey := fmt.Sprintf("markets:list:%d", chainID)

	if val, err := s.cache.Get(ctx, cacheKey); err == nil {
		var markets []MarketInfo
		if json.Unmarshal([]byte(val), &markets) == nil {
			return markets, nil
		}
	}

	markets, err := s.repo.ListMarkets(ctx, chainID)
	if err != nil {
		return nil, err
	}

	if data, err := json.Marshal(markets); err == nil {
		_ = s.cache.Set(ctx, cacheKey, string(data), 30*time.Second)
	}

	return markets, nil
}

func (s *Service) GetMarketDetail(ctx context.Context, marketID int64) (*MarketStateInfo, error) {
	cacheKey := fmt.Sprintf("markets:detail:%d", marketID)

	if val, err := s.cache.Get(ctx, cacheKey); err == nil {
		var state MarketStateInfo
		if json.Unmarshal([]byte(val), &state) == nil {
			return &state, nil
		}
	}

	state, err := s.repo.GetMarketDetail(ctx, marketID)
	if err != nil {
		return nil, err
	}

	if data, err := json.Marshal(state); err == nil {
		_ = s.cache.Set(ctx, cacheKey, string(data), 10*time.Second)
	}

	return state, nil
}

func (s *Service) GetHistory(ctx context.Context, marketID int64, period string, interval string) ([]MarketSnapshot, error) {
	return s.repo.GetHistory(ctx, marketID, period, interval)
}
