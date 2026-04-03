package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// RedisClient abstracts Redis operations for testing.
type RedisClient interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
}

// PriceCache wraps Redis for price data caching.
type PriceCache struct {
	client RedisClient
}

func NewPriceCache(client RedisClient) *PriceCache {
	return &PriceCache{client: client}
}

func priceKey(asset common.Address) string {
	return fmt.Sprintf("price:%s", asset.Hex())
}

// Set caches a price with the given TTL.
func (c *PriceCache) Set(ctx context.Context, price *PriceData, ttl time.Duration) error {
	data, err := json.Marshal(price)
	if err != nil {
		return fmt.Errorf("marshal price: %w", err)
	}
	return c.client.Set(ctx, priceKey(price.AssetAddress), string(data), ttl)
}

// Get retrieves a cached price for the given asset.
func (c *PriceCache) Get(ctx context.Context, asset common.Address) (*PriceData, error) {
	val, err := c.client.Get(ctx, priceKey(asset))
	if err != nil {
		return nil, fmt.Errorf("cache miss for %s: %w", asset.Hex(), err)
	}

	var price PriceData
	if err := json.Unmarshal([]byte(val), &price); err != nil {
		return nil, fmt.Errorf("unmarshal cached price: %w", err)
	}

	return &price, nil
}

// GetAll retrieves cached prices for multiple assets. Returns only found prices.
func (c *PriceCache) GetAll(ctx context.Context, assets []common.Address) map[common.Address]*PriceData {
	result := make(map[common.Address]*PriceData)
	for _, asset := range assets {
		if p, err := c.Get(ctx, asset); err == nil {
			result[asset] = p
		}
	}
	return result
}
